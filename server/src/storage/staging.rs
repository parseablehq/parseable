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
    fs,
    path::{Path, PathBuf},
    process,
    sync::Arc,
};

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    handlers::http::modal::{ingest_server::INGESTOR_META, IngestorMetadata, DEFAULT_VERSION},
    metrics,
    option::{Mode, CONFIG},
    storage::OBJECT_STORE_DATA_GRANULARITY,
    utils::{
        self, arrow::merged_reader::MergedReverseRecordReader, get_ingestor_id, get_url,
        hostname_unchecked,
    },
};
use arrow_schema::{ArrowError, Schema};
use base64::Engine;
use chrono::{NaiveDateTime, Timelike};
use parquet::{
    arrow::ArrowWriter,
    basic::Encoding,
    errors::ParquetError,
    file::properties::{WriterProperties, WriterPropertiesBuilder},
    format::SortingColumn,
    schema::types::ColumnPath,
};
use rand::distributions::DistString;

const ARROW_FILE_EXTENSION: &str = "data.arrows";
const PARQUET_FILE_EXTENSION: &str = "data.parquet";

#[derive(Debug)]
pub struct StorageDir {
    pub data_path: PathBuf,
}

impl StorageDir {
    pub fn new(stream_name: &str) -> Self {
        let data_path = CONFIG.parseable.local_stream_data_path(stream_name);

        Self { data_path }
    }

    pub fn file_time_suffix(time: NaiveDateTime, extention: &str) -> String {
        let uri = utils::date_to_prefix(time.date())
            + &utils::hour_to_prefix(time.hour())
            + &utils::minute_to_prefix(time.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap();
        let local_uri = str::replace(&uri, "/", ".");
        let hostname = hostname_unchecked();
        if CONFIG.parseable.mode == Mode::Ingest {
            let id = INGESTOR_META.get_ingestor_id();
            format!("{local_uri}{hostname}{id}.{extention}")
        } else {
            format!("{local_uri}{hostname}.{extention}")
        }
    }

    fn filename_by_time(stream_hash: &str, time: NaiveDateTime) -> String {
        format!(
            "{}.{}",
            stream_hash,
            Self::file_time_suffix(time, ARROW_FILE_EXTENSION)
        )
    }

    fn filename_by_current_time(stream_hash: &str, parsed_timestamp: NaiveDateTime) -> String {
        Self::filename_by_time(stream_hash, parsed_timestamp)
    }

    pub fn path_by_current_time(
        &self,
        stream_hash: &str,
        parsed_timestamp: NaiveDateTime,
    ) -> PathBuf {
        self.data_path.join(Self::filename_by_current_time(
            stream_hash,
            parsed_timestamp,
        ))
    }

    pub fn arrow_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        let paths: Vec<PathBuf> = dir
            .flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().map_or(false, |ext| ext.eq("arrows")))
            .collect();

        paths
    }

    #[allow(dead_code)]
    pub fn arrow_files_grouped_by_time(&self) -> HashMap<PathBuf, Vec<PathBuf>> {
        // hashmap <time, vec[paths]>
        let mut grouped_arrow_file: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let arrow_files = self.arrow_files();
        for arrow_file_path in arrow_files {
            let key = Self::arrow_path_to_parquet(&arrow_file_path);
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
    ) -> HashMap<PathBuf, Vec<PathBuf>> {
        let hot_filename = StorageDir::file_time_suffix(exclude, ARROW_FILE_EXTENSION);
        // hashmap <time, vec[paths]> but exclude where hot filename matches
        let mut grouped_arrow_file: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let mut arrow_files = self.arrow_files();

        arrow_files.retain(|path| {
            !path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(&hot_filename)
        });

        for arrow_file_path in arrow_files {
            let key = Self::arrow_path_to_parquet(&arrow_file_path);
            grouped_arrow_file
                .entry(key)
                .or_default()
                .push(arrow_file_path);
        }

        grouped_arrow_file
    }

    pub fn parquet_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        dir.flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().map_or(false, |ext| ext.eq("parquet")))
            .collect()
    }

    fn arrow_path_to_parquet(path: &Path) -> PathBuf {
        let filename = path.file_stem().unwrap().to_str().unwrap();
        let (_, filename) = filename.split_once('.').unwrap();
        let filename = filename.rsplit_once('.').expect("contains the delim `.`");
        let filename = format!("{}.{}", filename.0, filename.1);
        let random_string =
            rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 15);
        let filename_with_random_number = format!("{}.{}.{}", filename, random_string, "arrows");
        /*
                let file_stem = path.file_stem().unwrap().to_str().unwrap();
                let random_string =
                    rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 20);
                let (_, filename) = file_stem.split_once('.').unwrap();
                let filename_with_random_number = format!("{}.{}.{}", filename, random_number, "arrows");
        */

        let mut parquet_path = path.to_owned();
        parquet_path.set_file_name(filename_with_random_number);
        parquet_path.set_extension("parquet");
        parquet_path
    }
}

#[allow(unused)]
pub fn to_parquet_path(stream_name: &str, time: NaiveDateTime) -> PathBuf {
    let data_path = CONFIG.parseable.local_stream_data_path(stream_name);
    let dir = StorageDir::file_time_suffix(time, PARQUET_FILE_EXTENSION);

    data_path.join(dir)
}

pub fn convert_disk_files_to_parquet(
    stream: &str,
    dir: &StorageDir,
    time_partition: Option<String>,
) -> Result<Option<Schema>, MoveDataError> {
    let mut schemas = Vec::new();

    let time = chrono::Utc::now().naive_utc();
    let staging_files = dir.arrow_files_grouped_exclude_time(time);
    if staging_files.is_empty() {
        metrics::STAGING_FILES.with_label_values(&[stream]).set(0);
        metrics::STORAGE_SIZE
            .with_label_values(&["staging", stream, "arrows"])
            .set(0);
        metrics::STORAGE_SIZE
            .with_label_values(&["staging", stream, "parquet"])
            .set(0);
    }

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

        let record_reader = MergedReverseRecordReader::try_new(&files).unwrap();
        let merged_schema = record_reader.merged_schema();
        let mut index_time_partition: usize = 0;
        if let Some(time_partition) = time_partition.as_ref() {
            index_time_partition = merged_schema.index_of(time_partition).unwrap();
        }
        let parquet_file = fs::File::create(&parquet_path).map_err(|_| MoveDataError::Create)?;
        let props = parquet_writer_props(time_partition.clone(), index_time_partition).build();

        schemas.push(merged_schema.clone());
        let schema = Arc::new(merged_schema);
        let mut writer = ArrowWriter::try_new(parquet_file, schema.clone(), Some(props))?;

        for ref record in record_reader.merged_iter(schema) {
            writer.write(record)?;
        }

        writer.close()?;

        for file in files {
            let file_size = file.metadata().unwrap().len();
            let file_type = file.extension().unwrap().to_str().unwrap();

            if fs::remove_file(file.clone()).is_err() {
                log::error!("Failed to delete file. Unstable state");
                process::abort()
            }
            metrics::STORAGE_SIZE
                .with_label_values(&["staging", stream, file_type])
                .sub(file_size as i64);
        }
    }

    if !schemas.is_empty() {
        Ok(Some(Schema::try_merge(schemas).unwrap()))
    } else {
        Ok(None)
    }
}

pub fn parquet_writer_props(
    time_partition: Option<String>,
    index_time_partition: usize,
) -> WriterPropertiesBuilder {
    let index_time_partition: i32 = index_time_partition as i32;

    if let Some(time_partition) = time_partition {
        WriterProperties::builder()
            .set_max_row_group_size(CONFIG.parseable.row_group_size)
            .set_compression(CONFIG.parseable.parquet_compression.into())
            .set_column_encoding(
                ColumnPath::new(vec![time_partition]),
                Encoding::DELTA_BYTE_ARRAY,
            )
            .set_sorting_columns(Some(vec![SortingColumn {
                column_idx: index_time_partition,
                descending: true,
                nulls_first: true,
            }]))
    } else {
        WriterProperties::builder()
            .set_max_row_group_size(CONFIG.parseable.row_group_size)
            .set_compression(CONFIG.parseable.parquet_compression.into())
            .set_column_encoding(
                ColumnPath::new(vec![DEFAULT_TIMESTAMP_KEY.to_string()]),
                Encoding::DELTA_BINARY_PACKED,
            )
            .set_sorting_columns(Some(vec![SortingColumn {
                column_idx: index_time_partition,
                descending: true,
                nulls_first: true,
            }]))
    }
}

pub fn get_ingestor_info() -> anyhow::Result<IngestorMetadata> {
    let path = PathBuf::from(&CONFIG.parseable.local_staging_path);

    // all the files should be in the staging directory root
    let entries = std::fs::read_dir(path)?;
    let url = get_url();
    let port = url.port().expect("here port should be defined").to_string();
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
            let mut meta: IngestorMetadata = serde_json::from_slice(&std::fs::read(path)?)?;

            // compare url endpoint and port
            if meta.domain_name != url {
                log::info!(
                    "Domain Name was Updated. Old: {} New: {}",
                    meta.domain_name,
                    url
                );
                meta.domain_name = url;
            }

            if meta.port != port {
                log::info!("Port was Updated. Old: {} New: {}", meta.port, port);
                meta.port = port;
            }

            let token = base64::prelude::BASE64_STANDARD.encode(format!(
                "{}:{}",
                CONFIG.parseable.username, CONFIG.parseable.password
            ));

            let token = format!("Basic {}", token);

            if meta.token != token {
                // TODO: Update the message to be more informative with username and password
                log::info!(
                    "Credentials were Updated. Old: {} New: {}",
                    meta.token,
                    token
                );
                meta.token = token;
            }

            put_ingestor_info(meta.clone())?;
            return Ok(meta);
        }
    }

    let store = CONFIG.storage().get_object_store();
    let out = IngestorMetadata::new(
        port,
        url,
        DEFAULT_VERSION.to_string(),
        store.get_bucket_name(),
        &CONFIG.parseable.username,
        &CONFIG.parseable.password,
        get_ingestor_id(),
    );

    put_ingestor_info(out.clone())?;
    Ok(out)
}

/// Puts the ingestor info into the staging.
///
/// This function takes the ingestor info as a parameter and stores it in staging.
/// # Parameters
///
/// * `ingestor_info`: The ingestor info to be stored.
fn put_ingestor_info(info: IngestorMetadata) -> anyhow::Result<()> {
    let path = PathBuf::from(&CONFIG.parseable.local_staging_path);
    let file_name = format!("ingestor.{}.json", info.ingestor_id);
    let file_path = path.join(file_name);

    std::fs::write(file_path, serde_json::to_string(&info)?)?;

    Ok(())
}

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
