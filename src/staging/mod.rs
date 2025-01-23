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

use crate::{
    cli::Options,
    event::DEFAULT_TIMESTAMP_KEY,
    handlers::http::modal::{ingest_server::INGESTOR_META, IngestorMetadata, DEFAULT_VERSION},
    metrics,
    option::{Config, Mode},
    storage::OBJECT_STORE_DATA_GRANULARITY,
    utils::{
        arrow::merged_reader::MergedReverseRecordReader, get_ingestor_id, get_url, minute_to_slot,
    },
};
use anyhow::anyhow;
use arrow_schema::{ArrowError, Schema};
use base64::Engine;
use chrono::{NaiveDateTime, Timelike, Utc};
use itertools::Itertools;
use once_cell::sync::Lazy;
use parquet::{
    arrow::ArrowWriter,
    basic::Encoding,
    errors::ParquetError,
    file::properties::{WriterProperties, WriterPropertiesBuilder},
    format::SortingColumn,
    schema::types::ColumnPath,
};
use rand::distributions::DistString;
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    process,
    sync::Arc,
};
use tracing::{error, info};
pub use writer::StreamWriterError;
use writer::WriterTable;

mod writer;

const ARROW_FILE_EXTENSION: &str = "data.arrows";

pub static STREAM_WRITERS: Lazy<WriterTable> = Lazy::new(WriterTable::default);

#[derive(Debug, thiserror::Error)]
pub enum MoveDataError {
    #[error("Unable to create recordbatch stream")]
    Arrow(#[from] ArrowError),
    #[error("Could not generate parquet file")]
    Parquet(#[from] ParquetError),
    #[error("IO Error {0}")]
    ObjectStorage(#[from] std::io::Error),
    #[error("Could not generate parquet file")]
    Create,
}

#[derive(Debug)]
pub struct Staging<'a> {
    pub data_path: PathBuf,
    pub options: &'a Options,
}

impl<'a> Staging<'a> {
    pub fn new(options: &'a Options, stream_name: &str) -> Self {
        Self {
            data_path: options.local_stream_data_path(stream_name),
            options,
        }
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

    #[allow(dead_code)]
    pub fn arrow_files_grouped_by_time(&self) -> HashMap<PathBuf, Vec<PathBuf>> {
        // hashmap <time, vec[paths]>
        let mut grouped_arrow_file: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let arrow_files = self.arrow_files();
        for arrow_file_path in arrow_files {
            let key = Self::arrow_path_to_parquet(&arrow_file_path, "");
            grouped_arrow_file
                .entry(key)
                .or_default()
                .push(arrow_file_path);
        }

        grouped_arrow_file
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
                fs::remove_file(&arrow_file_path).unwrap();
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
}

pub fn convert_disk_files_to_parquet(
    stream: &str,
    dir: &Staging,
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
        let parquet_file = fs::File::create(&parquet_path).map_err(|_| MoveDataError::Create)?;
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
            fs::remove_file(parquet_path).unwrap();
        } else {
            for file in files {
                // warn!("file-\n{file:?}\n");
                let file_size = file.metadata().unwrap().len();
                let file_type = file.extension().unwrap().to_str().unwrap();
                if fs::remove_file(file.clone()).is_err() {
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

pub fn parquet_writer_props(
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

pub fn get_ingestor_info(config: &Config) -> anyhow::Result<IngestorMetadata> {
    let path = PathBuf::from(&config.options.local_staging_path);

    // all the files should be in the staging directory root
    let entries = std::fs::read_dir(path)?;
    let url = get_url();
    let port = url.port().unwrap_or(80).to_string();
    let url = url.to_string();

    for entry in entries {
        // cause the staging directory will have only one file with ingestor in the name
        // so the JSON Parse should not error unless the file is corrupted
        let path = entry?.path();
        let flag = path
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default()
            .contains("ingestor");

        if flag {
            // get the ingestor metadata from staging
            let mut meta: JsonValue = serde_json::from_slice(&std::fs::read(path)?)?;

            // migrate the staging meta
            let obj = meta
                .as_object_mut()
                .ok_or_else(|| anyhow!("Could Not parse Ingestor Metadata Json"))?;

            if obj.get("flight_port").is_none() {
                obj.insert(
                    "flight_port".to_owned(),
                    JsonValue::String(config.options.flight_port.to_string()),
                );
            }

            let mut meta: IngestorMetadata = serde_json::from_value(meta)?;

            // compare url endpoint and port
            if meta.domain_name != url {
                info!(
                    "Domain Name was Updated. Old: {} New: {}",
                    meta.domain_name, url
                );
                meta.domain_name = url;
            }

            if meta.port != port {
                info!("Port was Updated. Old: {} New: {}", meta.port, port);
                meta.port = port;
            }

            let token = base64::prelude::BASE64_STANDARD.encode(format!(
                "{}:{}",
                config.options.username, config.options.password
            ));

            let token = format!("Basic {}", token);

            if meta.token != token {
                // TODO: Update the message to be more informative with username and password
                info!(
                    "Credentials were Updated. Old: {} New: {}",
                    meta.token, token
                );
                meta.token = token;
            }

            put_ingestor_info(config, meta.clone())?;
            return Ok(meta);
        }
    }

    let store = config.storage().get_object_store();
    let out = IngestorMetadata::new(
        port,
        url,
        DEFAULT_VERSION.to_string(),
        store.get_bucket_name(),
        &config.options.username,
        &config.options.password,
        get_ingestor_id(),
        config.options.flight_port.to_string(),
    );

    put_ingestor_info(config, out.clone())?;
    Ok(out)
}

/// Puts the ingestor info into the staging.
///
/// This function takes the ingestor info as a parameter and stores it in staging.
/// # Parameters
///
/// * `ingestor_info`: The ingestor info to be stored.
pub fn put_ingestor_info(config: &Config, info: IngestorMetadata) -> anyhow::Result<()> {
    let path = PathBuf::from(&config.options.local_staging_path);
    let file_name = format!("ingestor.{}.json", info.ingestor_id);
    let file_path = path.join(file_name);

    std::fs::write(file_path, serde_json::to_vec(&info)?)?;

    Ok(())
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
        let storage_dir = Staging::new(&options, stream_name);

        assert_eq!(
            storage_dir.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_storage_dir_with_special_characters() {
        let stream_name = "test_stream_!@#$%^&*()";

        let options = Options::default();
        let storage_dir = Staging::new(&options, stream_name);

        assert_eq!(
            storage_dir.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_storage_dir_data_path_initialization() {
        let stream_name = "example_stream";

        let options = Options::default();
        let storage_dir = Staging::new(&options, stream_name);

        assert_eq!(
            storage_dir.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_storage_dir_with_alphanumeric_stream_name() {
        let stream_name = "test123stream";

        let options = Options::default();
        let storage_dir = Staging::new(&options, stream_name);

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
        let storage_dir = Staging::new(&options, "test_stream");

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
        let storage_dir = Staging::new(&options, stream_name);

        let expected_path = storage_dir.data_path.join(format!(
            "{}{stream_hash}.date={}.hour={:02}.minute={}.key1=value1.key2=value2.{}.data.arrows",
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
}
