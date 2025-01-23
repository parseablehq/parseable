/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 */

use std::{
    collections::HashMap,
    fs::{remove_file, OpenOptions},
    path::{Path, PathBuf},
    process,
    sync::{Arc, Mutex, RwLock},
};

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use chrono::{NaiveDateTime, Timelike, Utc};
use derive_more::{Deref, DerefMut};
use itertools::Itertools;
use parquet::{
    arrow::ArrowWriter,
    basic::Encoding,
    file::properties::{WriterProperties, WriterPropertiesBuilder},
    format::SortingColumn,
    schema::types::ColumnPath,
};
use rand::distributions::DistString;
use tracing::error;

use crate::{
    cli::Options,
    event::DEFAULT_TIMESTAMP_KEY,
    handlers::http::modal::ingest_server::INGESTOR_META,
    metrics,
    option::{Mode, CONFIG},
    storage::{StreamType, OBJECT_STORE_DATA_GRANULARITY},
    utils::{arrow::merged_reader::MergedReverseRecordReader, minute_to_slot},
};

use super::{writer::Writer, MoveDataError, StreamWriterError};

const ARROW_FILE_EXTENSION: &str = "data.arrows";

pub struct Stream<'a> {
    pub data_path: PathBuf,
    pub options: &'a Options,
    pub writer: Mutex<Writer>,
}

impl<'a> Stream<'a> {
    pub fn new(options: &'a Options, stream_name: &str) -> Self {
        Self {
            data_path: options.local_stream_data_path(stream_name),
            options,
            writer: Mutex::new(Writer::default()),
        }
    }

    fn push(
        &self,
        schema_key: &str,
        record: &RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: StreamType,
    ) -> Result<(), StreamWriterError> {
        let mut guard = self.writer.lock().unwrap();
        if self.options.mode != Mode::Query || stream_type == StreamType::Internal {
            match guard.disk.get_mut(schema_key) {
                Some(writer) => {
                    writer.write(record)?;
                }
                None => {
                    // entry is not present thus we create it
                    let file_path = self.path_by_current_time(
                        schema_key,
                        parsed_timestamp,
                        custom_partition_values,
                    );
                    std::fs::create_dir_all(&self.data_path)?;

                    let file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&file_path)?;

                    let mut writer = StreamWriter::try_new(file, &record.schema())
                        .expect("File and RecordBatch both are checked");

                    writer.write(record)?;
                    guard.disk.insert(schema_key.to_owned(), writer);
                }
            };
            guard.mem.push(schema_key, record);
        } else {
            guard.mem.push(schema_key, record);
        }

        Ok(())
    }

    pub fn path_by_current_time(
        &self,
        stream_hash: &str,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
    ) -> PathBuf {
        let mut hostname = hostname::get().unwrap().into_string().unwrap();
        if self.options.mode == Mode::Ingest {
            hostname.push_str(&INGESTOR_META.get_ingestor_id());
        }
        let filename = format!(
            "{}{stream_hash}.date={}.hour={:02}.minute={}.{}.{hostname}.{ARROW_FILE_EXTENSION}",
            Utc::now().format("%Y%m%dT%H%M"),
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            minute_to_slot(parsed_timestamp.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap(),
            custom_partition_values
                .iter()
                .sorted_by_key(|v| v.0)
                .map(|(key, value)| format!("{key}={value}"))
                .join(".")
        );
        self.data_path.join(filename)
    }

    pub fn arrow_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        let paths = dir
            .flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().is_some_and(|ext| ext.eq("arrows")))
            .sorted_by_key(|f| f.metadata().unwrap().modified().unwrap())
            .collect();

        paths
    }

    pub fn arrow_files_grouped_exclude_time(
        &self,
        exclude: NaiveDateTime,
        stream: &str,
        shutdown_signal: bool,
    ) -> HashMap<PathBuf, Vec<PathBuf>> {
        let mut grouped_arrow_file: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let mut arrow_files = self.arrow_files();

        if !shutdown_signal {
            arrow_files.retain(|path| {
                !path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with(&exclude.format("%Y%m%dT%H%M").to_string())
            });
        }

        let random_string =
            rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 15);
        for arrow_file_path in arrow_files {
            if arrow_file_path.metadata().unwrap().len() == 0 {
                error!(
                    "Invalid arrow file {:?} detected for stream {}, removing it",
                    &arrow_file_path, stream
                );
                remove_file(&arrow_file_path).unwrap();
            } else {
                let key = Self::arrow_path_to_parquet(&arrow_file_path, &random_string);
                grouped_arrow_file
                    .entry(key)
                    .or_default()
                    .push(arrow_file_path);
            }
        }
        grouped_arrow_file
    }

    pub fn parquet_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        dir.flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().is_some_and(|ext| ext.eq("parquet")))
            .collect()
    }

    fn arrow_path_to_parquet(path: &Path, random_string: &str) -> PathBuf {
        let filename = path.file_stem().unwrap().to_str().unwrap();
        let (_, filename) = filename.split_once('.').unwrap();
        assert!(filename.contains('.'), "contains the delim `.`");
        let filename_with_random_number = format!("{filename}.{random_string}.arrows");
        let mut parquet_path = path.to_owned();
        parquet_path.set_file_name(filename_with_random_number);
        parquet_path.set_extension("parquet");
        parquet_path
    }

    fn recordbatches_cloned(&self, schema: &Arc<Schema>) -> Vec<RecordBatch> {
        self.writer.lock().unwrap().mem.recordbatch_cloned(schema)
    }

    pub fn clear(&self) {
        self.writer.lock().unwrap().mem.clear();
    }

    fn unset(self) {
        let writer = self.writer.into_inner().unwrap();
        for mut writer in writer.disk.into_values() {
            _ = writer.finish();
        }
    }
}

pub fn convert_disk_files_to_parquet(
    stream: &str,
    dir: &Stream,
    time_partition: Option<String>,
    custom_partition: Option<String>,
    shutdown_signal: bool,
) -> Result<Option<Schema>, MoveDataError> {
    let mut schemas = Vec::new();

    let time = chrono::Utc::now().naive_utc();
    let staging_files = dir.arrow_files_grouped_exclude_time(time, stream, shutdown_signal);
    if staging_files.is_empty() {
        metrics::STAGING_FILES.with_label_values(&[stream]).set(0);
        metrics::STORAGE_SIZE
            .with_label_values(&["staging", stream, "arrows"])
            .set(0);
        metrics::STORAGE_SIZE
            .with_label_values(&["staging", stream, "parquet"])
            .set(0);
    }

    // warn!("staging files-\n{staging_files:?}\n");
    for (parquet_path, files) in staging_files {
        metrics::STAGING_FILES
            .with_label_values(&[stream])
            .set(files.len() as i64);

        for file in &files {
            let file_size = file.metadata().unwrap().len();
            let file_type = file.extension().unwrap().to_str().unwrap();

            metrics::STORAGE_SIZE
                .with_label_values(&["staging", stream, file_type])
                .add(file_size as i64);
        }

        let record_reader = MergedReverseRecordReader::try_new(&files);
        if record_reader.readers.is_empty() {
            continue;
        }
        let merged_schema = record_reader.merged_schema();
        let mut index_time_partition: usize = 0;
        if let Some(time_partition) = time_partition.as_ref() {
            index_time_partition = merged_schema.index_of(time_partition).unwrap();
        }
        let mut custom_partition_fields: HashMap<String, usize> = HashMap::new();
        if let Some(custom_partition) = custom_partition.as_ref() {
            for custom_partition_field in custom_partition.split(',') {
                let index = merged_schema.index_of(custom_partition_field).unwrap();
                custom_partition_fields.insert(custom_partition_field.to_string(), index);
            }
        }
        let props = parquet_writer_props(
            dir.options,
            time_partition.clone(),
            index_time_partition,
            custom_partition_fields,
        )
        .build();
        schemas.push(merged_schema.clone());
        let schema = Arc::new(merged_schema);
        let parquet_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&parquet_path)
            .map_err(|_| MoveDataError::Create)?;
        let mut writer = ArrowWriter::try_new(&parquet_file, schema.clone(), Some(props))?;
        for ref record in record_reader.merged_iter(schema, time_partition.clone()) {
            writer.write(record)?;
        }

        writer.close()?;
        if parquet_file.metadata().unwrap().len() < parquet::file::FOOTER_SIZE as u64 {
            error!(
                "Invalid parquet file {:?} detected for stream {}, removing it",
                &parquet_path, stream
            );
            remove_file(parquet_path).unwrap();
        } else {
            for file in files {
                // warn!("file-\n{file:?}\n");
                let file_size = file.metadata().unwrap().len();
                let file_type = file.extension().unwrap().to_str().unwrap();
                if remove_file(file.clone()).is_err() {
                    error!("Failed to delete file. Unstable state");
                    process::abort()
                }
                metrics::STORAGE_SIZE
                    .with_label_values(&["staging", stream, file_type])
                    .sub(file_size as i64);
            }
        }
    }

    if !schemas.is_empty() {
        Ok(Some(Schema::try_merge(schemas).unwrap()))
    } else {
        Ok(None)
    }
}

fn parquet_writer_props(
    options: &Options,
    time_partition: Option<String>,
    index_time_partition: usize,
    custom_partition_fields: HashMap<String, usize>,
) -> WriterPropertiesBuilder {
    let index_time_partition: i32 = index_time_partition as i32;
    let mut time_partition_field = DEFAULT_TIMESTAMP_KEY.to_string();
    if let Some(time_partition) = time_partition {
        time_partition_field = time_partition;
    }
    let mut sorting_column_vec: Vec<SortingColumn> = Vec::new();
    sorting_column_vec.push(SortingColumn {
        column_idx: index_time_partition,
        descending: true,
        nulls_first: true,
    });
    let mut props = WriterProperties::builder()
        .set_max_row_group_size(options.row_group_size)
        .set_compression(options.parquet_compression.into())
        .set_column_encoding(
            ColumnPath::new(vec![time_partition_field]),
            Encoding::DELTA_BINARY_PACKED,
        );

    for (field, index) in custom_partition_fields {
        let field = ColumnPath::new(vec![field]);
        let encoding = Encoding::DELTA_BYTE_ARRAY;
        props = props.set_column_encoding(field, encoding);
        let sorting_column = SortingColumn {
            column_idx: index as i32,
            descending: true,
            nulls_first: true,
        };
        sorting_column_vec.push(sorting_column);
    }
    props = props.set_sorting_columns(Some(sorting_column_vec));

    props
}

#[derive(Deref, DerefMut, Default)]
pub struct Streams(RwLock<HashMap<String, Stream<'static>>>);

impl Streams {
    // Concatenates record batches and puts them in memory store for each event.
    pub fn append_to_local(
        &self,
        stream_name: &str,
        schema_key: &str,
        record: &RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: StreamType,
    ) -> Result<(), StreamWriterError> {
        if !self.read().unwrap().contains_key(stream_name) {
            // Gets write privileges only for inserting a writer
            self.write().unwrap().insert(
                stream_name.to_owned(),
                Stream::new(&CONFIG.options, stream_name),
            );
        }

        // Updates the writer with only read privileges
        self.read()
            .unwrap()
            .get(stream_name)
            .expect("Stream exists")
            .push(
                schema_key,
                record,
                parsed_timestamp,
                custom_partition_values,
                stream_type,
            )
    }

    pub fn clear(&self, stream_name: &str) {
        if let Some(stream) = self.write().unwrap().get(stream_name) {
            stream.clear();
        }
    }

    pub fn delete_stream(&self, stream_name: &str) {
        self.write().unwrap().remove(stream_name);
    }

    pub fn unset_all(&self) {
        let mut table = self.write().unwrap();
        let map = std::mem::take(&mut *table);
        drop(table);
        for staging in map.into_values() {
            staging.unset()
        }
    }

    pub fn recordbatches_cloned(
        &self,
        stream_name: &str,
        schema: &Arc<Schema>,
    ) -> Option<Vec<RecordBatch>> {
        self.read()
            .unwrap()
            .get(stream_name)
            .map(|staging| staging.recordbatches_cloned(schema))
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use temp_dir::TempDir;

    use super::*;

    #[test]
    fn test_storage_dir_new_with_valid_stream() {
        let stream_name = "test_stream";

        let options = Options::default();
        let storage_dir = Stream::new(&options, stream_name);

        assert_eq!(
            storage_dir.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_storage_dir_with_special_characters() {
        let stream_name = "test_stream_!@#$%^&*()";

        let options = Options::default();
        let storage_dir = Stream::new(&options, stream_name);

        assert_eq!(
            storage_dir.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_storage_dir_data_path_initialization() {
        let stream_name = "example_stream";

        let options = Options::default();
        let storage_dir = Stream::new(&options, stream_name);

        assert_eq!(
            storage_dir.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_storage_dir_with_alphanumeric_stream_name() {
        let stream_name = "test123stream";

        let options = Options::default();
        let storage_dir = Stream::new(&options, stream_name);

        assert_eq!(
            storage_dir.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_arrow_files_empty_directory() {
        let temp_dir = TempDir::new().unwrap();

        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage_dir = Stream::new(&options, "test_stream");

        let files = storage_dir.arrow_files();

        assert!(files.is_empty());
    }

    #[test]
    fn generate_correct_path_with_current_time_and_valid_parameters() {
        let stream_name = "test_stream";
        let stream_hash = "abc123";
        let parsed_timestamp = NaiveDate::from_ymd_opt(2023, 10, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let mut custom_partition_values = HashMap::new();
        custom_partition_values.insert("key1".to_string(), "value1".to_string());
        custom_partition_values.insert("key2".to_string(), "value2".to_string());

        let options = Options::default();
        let storage_dir = Stream::new(&options, stream_name);

        let expected_path = storage_dir.data_path.join(format!(
            "{}{stream_hash}.date={}.hour={:02}.minute={}.key1=value1.key2=value2.{}{ARROW_FILE_EXTENSION}",
            Utc::now().format("%Y%m%dT%H%M"),
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            minute_to_slot(parsed_timestamp.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap(),
            hostname::get().unwrap().into_string().unwrap()
        ));

        let generated_path = storage_dir.path_by_current_time(
            stream_hash,
            parsed_timestamp,
            &custom_partition_values,
        );

        assert_eq!(generated_path, expected_path);
    }

    #[test]
    fn test_convert_files_with_empty_staging() -> Result<(), MoveDataError> {
        let temp_dir = TempDir::new()?;
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let stream = "test_stream".to_string();
        let storage_dir = Stream::new(&options, &stream);
        let result = convert_disk_files_to_parquet(&stream, &storage_dir, None, None, false)?;
        assert!(result.is_none());
        // Verify metrics were set to 0
        let staging_files = metrics::STAGING_FILES.with_label_values(&[&stream]).get();
        assert_eq!(staging_files, 0);
        let storage_size_arrows = metrics::STORAGE_SIZE
            .with_label_values(&["staging", &stream, "arrows"])
            .get();
        assert_eq!(storage_size_arrows, 0);
        let storage_size_parquet = metrics::STORAGE_SIZE
            .with_label_values(&["staging", &stream, "parquet"])
            .get();
        assert_eq!(storage_size_parquet, 0);
        Ok(())
    }
}
