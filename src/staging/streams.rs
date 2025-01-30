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
    utils::minute_to_slot,
};

use super::{
    reader::{MergedRecordReader, MergedReverseRecordReader},
    writer::Writer,
    StagingError,
};

const ARROW_FILE_EXTENSION: &str = "data.arrows";

pub type StreamRef<'a> = Arc<Stream<'a>>;

/// State of staging associated with a single stream of data in parseable.
pub struct Stream<'a> {
    pub stream_name: String,
    pub data_path: PathBuf,
    pub options: &'a Options,
    pub writer: Mutex<Writer>,
}

impl<'a> Stream<'a> {
    pub fn new(options: &'a Options, stream_name: impl Into<String>) -> StreamRef<'a> {
        let stream_name = stream_name.into();
        let data_path = options.local_stream_data_path(&stream_name);

        Arc::new(Self {
            stream_name,
            data_path,
            options,
            writer: Mutex::new(Writer::default()),
        })
    }

    // Concatenates record batches and puts them in memory store for each event.
    pub fn push(
        &self,
        schema_key: &str,
        record: &RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: StreamType,
    ) -> Result<(), StagingError> {
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
        }

        guard.mem.push(schema_key, record);

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
            "{}{stream_hash}.date={}.hour={:02}.minute={}.{}{hostname}.{ARROW_FILE_EXTENSION}",
            Utc::now().format("%Y%m%dT%H%M"),
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            minute_to_slot(parsed_timestamp.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap(),
            custom_partition_values
                .iter()
                .sorted_by_key(|v| v.0)
                .map(|(key, value)| format!("{key}={value}."))
                .join("")
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
                    &arrow_file_path, self.stream_name
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

    pub fn recordbatches_cloned(&self, schema: &Arc<Schema>) -> Vec<RecordBatch> {
        self.writer.lock().unwrap().mem.recordbatch_cloned(schema)
    }

    pub fn clear(&self) {
        self.writer.lock().unwrap().mem.clear();
    }

    fn flush(&self) {
        let mut disk_writers = {
            let mut writer = self.writer.lock().unwrap();
            // Flush memory
            writer.mem.clear();
            // Take schema -> disk writer mapping
            std::mem::take(&mut writer.disk)
        };

        // Flush disk
        for writer in disk_writers.values_mut() {
            _ = writer.finish();
        }
    }

    pub fn convert_disk_files_to_parquet(
        &self,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
        shutdown_signal: bool,
    ) -> Result<Option<Schema>, StagingError> {
        let mut schemas = Vec::new();

        let time = chrono::Utc::now().naive_utc();
        let staging_files = self.arrow_files_grouped_exclude_time(time, shutdown_signal);
        if staging_files.is_empty() {
            metrics::STAGING_FILES
                .with_label_values(&[&self.stream_name])
                .set(0);
            metrics::STORAGE_SIZE
                .with_label_values(&["staging", &self.stream_name, "arrows"])
                .set(0);
            metrics::STORAGE_SIZE
                .with_label_values(&["staging", &self.stream_name, "parquet"])
                .set(0);
        }

        // warn!("staging files-\n{staging_files:?}\n");
        for (parquet_path, files) in staging_files {
            metrics::STAGING_FILES
                .with_label_values(&[&self.stream_name])
                .set(files.len() as i64);

            for file in &files {
                let file_size = file.metadata().unwrap().len();
                let file_type = file.extension().unwrap().to_str().unwrap();

                metrics::STORAGE_SIZE
                    .with_label_values(&["staging", &self.stream_name, file_type])
                    .add(file_size as i64);
            }

            let record_reader = MergedReverseRecordReader::try_new(&files);
            if record_reader.readers.is_empty() {
                continue;
            }
            let merged_schema = record_reader.merged_schema();

            let props = parquet_writer_props(
                self.options,
                &merged_schema,
                time_partition,
                custom_partition,
            )
            .build();
            schemas.push(merged_schema.clone());
            let schema = Arc::new(merged_schema);
            let parquet_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&parquet_path)
                .map_err(|_| StagingError::Create)?;
            let mut writer = ArrowWriter::try_new(&parquet_file, schema.clone(), Some(props))?;
            for ref record in record_reader.merged_iter(schema, time_partition.cloned()) {
                writer.write(record)?;
            }

            writer.close()?;
            if parquet_file.metadata().unwrap().len() < parquet::file::FOOTER_SIZE as u64 {
                error!(
                    "Invalid parquet file {:?} detected for stream {}, removing it",
                    &parquet_path, &self.stream_name
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
                        .with_label_values(&["staging", &self.stream_name, file_type])
                        .sub(file_size as i64);
                }
            }
        }

        if schemas.is_empty() {
            return Ok(None);
        }

        Ok(Some(Schema::try_merge(schemas).unwrap()))
    }

    pub fn updated_schema(&self, current_schema: Schema) -> Schema {
        let staging_files = self.arrow_files();
        let record_reader = MergedRecordReader::try_new(&staging_files).unwrap();
        if record_reader.readers.is_empty() {
            return current_schema;
        }

        let schema = record_reader.merged_schema();

        Schema::try_merge(vec![schema, current_schema]).unwrap()
    }
}

fn parquet_writer_props(
    options: &Options,
    merged_schema: &Schema,
    time_partition: Option<&String>,
    custom_partition: Option<&String>,
) -> WriterPropertiesBuilder {
    // Determine time partition field
    let time_partition_field = time_partition.map_or(DEFAULT_TIMESTAMP_KEY, |tp| tp.as_str());

    // Find time partition index
    let time_partition_idx = merged_schema.index_of(time_partition_field).unwrap_or(0);

    let mut props = WriterProperties::builder()
        .set_max_row_group_size(options.row_group_size)
        .set_compression(options.parquet_compression.into())
        .set_column_encoding(
            ColumnPath::new(vec![time_partition_field.to_string()]),
            Encoding::DELTA_BINARY_PACKED,
        );

    // Create sorting columns
    let mut sorting_column_vec = vec![SortingColumn {
        column_idx: time_partition_idx as i32,
        descending: true,
        nulls_first: true,
    }];

    // Describe custom partition column encodings and sorting
    if let Some(custom_partition) = custom_partition {
        for partition in custom_partition.split(',') {
            if let Ok(idx) = merged_schema.index_of(partition) {
                let column_path = ColumnPath::new(vec![partition.to_string()]);
                props = props.set_column_encoding(column_path, Encoding::DELTA_BYTE_ARRAY);

                sorting_column_vec.push(SortingColumn {
                    column_idx: idx as i32,
                    descending: true,
                    nulls_first: true,
                });
            }
        }
    }

    // Set sorting columns
    props.set_sorting_columns(Some(sorting_column_vec))
}

#[derive(Deref, DerefMut, Default)]
pub struct Streams(RwLock<HashMap<String, StreamRef<'static>>>);

impl Streams {
    /// Try to get the handle of a stream in staging, if it doesn't exist return `None`.
    pub fn get_stream(&self, stream_name: &str) -> Option<StreamRef<'static>> {
        self.read().unwrap().get(stream_name).cloned()
    }

    /// Get the handle to a stream in staging, create one if it doesn't exist
    pub fn get_or_create_stream(&self, stream_name: &str) -> StreamRef<'static> {
        if let Some(staging) = self.get_stream(stream_name) {
            return staging;
        }

        let staging = Stream::new(&CONFIG.options, stream_name);

        // Gets write privileges only for creating the stream when it doesn't already exist.
        self.write()
            .unwrap()
            .insert(stream_name.to_owned(), staging.clone());

        staging
    }

    pub fn clear(&self, stream_name: &str) {
        if let Some(stream) = self.write().unwrap().get(stream_name) {
            stream.clear();
        }
    }

    pub fn delete_stream(&self, stream_name: &str) {
        self.write().unwrap().remove(stream_name);
    }

    pub fn flush_all(&self) {
        let streams = self.read().unwrap();

        for staging in streams.values() {
            staging.flush()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_array::{Int32Array, StringArray, TimestampMillisecondArray};
    use arrow_schema::{DataType, Field, TimeUnit};
    use chrono::{NaiveDate, TimeDelta};
    use temp_dir::TempDir;
    use tokio::time::sleep;

    use super::*;

    #[test]
    fn test_staging_new_with_valid_stream() {
        let stream_name = "test_stream";

        let options = Options::default();
        let staging = Stream::new(&options, stream_name);

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_staging_with_special_characters() {
        let stream_name = "test_stream_!@#$%^&*()";

        let options = Options::default();
        let staging = Stream::new(&options, stream_name);

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_staging_data_path_initialization() {
        let stream_name = "example_stream";

        let options = Options::default();
        let staging = Stream::new(&options, stream_name);

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_staging_with_alphanumeric_stream_name() {
        let stream_name = "test123stream";

        let options = Options::default();
        let staging = Stream::new(&options, stream_name);

        assert_eq!(
            staging.data_path,
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
        let staging = Stream::new(&options, "test_stream");

        let files = staging.arrow_files();

        assert!(files.is_empty());
    }

    #[test]
    fn generate_correct_path_with_current_time_and_no_custom_partitioning() {
        let stream_name = "test_stream";
        let stream_hash = "abc123";
        let parsed_timestamp = NaiveDate::from_ymd_opt(2023, 10, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let custom_partition_values = HashMap::new();

        let options = Options::default();
        let staging = Stream::new(&options, stream_name);

        let expected_path = staging.data_path.join(format!(
            "{}{stream_hash}.date={}.hour={:02}.minute={}.{}.{ARROW_FILE_EXTENSION}",
            Utc::now().format("%Y%m%dT%H%M"),
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            minute_to_slot(parsed_timestamp.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap(),
            hostname::get().unwrap().into_string().unwrap()
        ));

        let generated_path =
            staging.path_by_current_time(stream_hash, parsed_timestamp, &custom_partition_values);

        assert_eq!(generated_path, expected_path);
    }

    #[test]
    fn generate_correct_path_with_current_time_and_custom_partitioning() {
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
        let staging = Stream::new(&options, stream_name);

        let expected_path = staging.data_path.join(format!(
            "{}{stream_hash}.date={}.hour={:02}.minute={}.key1=value1.key2=value2.{}.{ARROW_FILE_EXTENSION}",
            Utc::now().format("%Y%m%dT%H%M"),
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            minute_to_slot(parsed_timestamp.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap(),
            hostname::get().unwrap().into_string().unwrap()
        ));

        let generated_path =
            staging.path_by_current_time(stream_hash, parsed_timestamp, &custom_partition_values);

        assert_eq!(generated_path, expected_path);
    }

    #[test]
    fn test_convert_to_parquet_with_empty_staging() -> Result<(), StagingError> {
        let temp_dir = TempDir::new()?;
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let stream = "test_stream".to_string();
        let result =
            Stream::new(&options, &stream).convert_disk_files_to_parquet(None, None, false)?;
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

    fn write_log(staging: &StreamRef, schema: &Schema, mins: i64) {
        let time: NaiveDateTime = Utc::now()
            .checked_sub_signed(TimeDelta::minutes(mins))
            .unwrap()
            .naive_utc();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        staging
            .push(
                "abc",
                &batch,
                time,
                &HashMap::new(),
                StreamType::UserDefined,
            )
            .unwrap();
        staging.flush();
    }

    #[test]
    fn different_minutes_multiple_arrow_files_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        };
        let staging = Stream::new(&options, stream_name);

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        for i in 0..3 {
            write_log(&staging, &schema, i);
        }
        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 3);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(&options, stream_name);
        let result = staging
            .convert_disk_files_to_parquet(None, None, true)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow files deleted
        assert_eq!(staging.parquet_files().len(), 3);
        assert_eq!(staging.arrow_files().len(), 0);
    }

    #[test]
    fn same_minute_multiple_arrow_files_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        };
        let staging: Arc<Stream<'_>> = Stream::new(&options, stream_name);

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        for _ in 0..3 {
            write_log(&staging, &schema, 0);
        }
        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 1);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(&options, stream_name);
        let result = staging
            .convert_disk_files_to_parquet(None, None, true)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow files deleted
        assert_eq!(staging.parquet_files().len(), 1);
        assert_eq!(staging.arrow_files().len(), 0);
    }

    #[tokio::test]
    async fn miss_current_arrow_file_when_converting_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        };
        let staging = Stream::new(&options, stream_name);

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        // 2 logs in the previous minutes
        for i in 0..2 {
            write_log(&staging, &schema, i);
        }
        sleep(Duration::from_secs(60)).await;

        write_log(&staging, &schema, 0);

        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 3);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(&options, stream_name);
        let result = staging
            .convert_disk_files_to_parquet(None, None, false)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow file left
        assert_eq!(staging.parquet_files().len(), 2);
        assert_eq!(staging.arrow_files().len(), 1);
    }
}
