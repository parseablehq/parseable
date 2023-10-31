/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use arrow_schema::{ArrowError, Schema};
use chrono::{NaiveDateTime, Timelike, Utc};
use parquet::{
    arrow::ArrowWriter,
    basic::Encoding,
    errors::ParquetError,
    file::properties::{WriterProperties, WriterPropertiesBuilder},
    format::SortingColumn,
    schema::types::ColumnPath,
};

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    metrics,
    option::CONFIG,
    storage::OBJECT_STORE_DATA_GRANULARITY,
    utils::{self, arrow::merged_reader::MergedReverseRecordReader},
};

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
        let hostname = utils::hostname_unchecked();
        format!("{local_uri}{hostname}.{extention}")
    }

    fn filename_by_time(stream_hash: &str, time: NaiveDateTime) -> String {
        format!(
            "{}.{}",
            stream_hash,
            Self::file_time_suffix(time, ARROW_FILE_EXTENSION)
        )
    }

    fn filename_by_current_time(stream_hash: &str) -> String {
        let datetime = Utc::now();
        Self::filename_by_time(stream_hash, datetime.naive_utc())
    }

    pub fn path_by_current_time(&self, stream_hash: &str) -> PathBuf {
        self.data_path
            .join(Self::filename_by_current_time(stream_hash))
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
        let filename = path.file_name().unwrap().to_str().unwrap();
        let (_, filename) = filename.split_once('.').unwrap();
        let mut parquet_path = path.to_owned();
        parquet_path.set_file_name(filename);
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
) -> Result<Option<Schema>, MoveDataError> {
    let mut schemas = Vec::new();

    let time = chrono::Utc::now().naive_utc();
    let staging_files = dir.arrow_files_grouped_exclude_time(time);
    if staging_files.is_empty() {
        metrics::STAGING_FILES.with_label_values(&[stream]).set(0);
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

        let parquet_file = fs::File::create(&parquet_path).map_err(|_| MoveDataError::Create)?;

        let props = parquet_writer_props().build();
        let merged_schema = record_reader.merged_schema();
        schemas.push(merged_schema.clone());
        let schema = Arc::new(merged_schema);
        let mut writer = ArrowWriter::try_new(parquet_file, schema.clone(), Some(props))?;

        for ref record in record_reader.merged_iter(schema) {
            writer.write(record)?;
        }

        writer.close()?;

        for file in files {
            if fs::remove_file(file).is_err() {
                log::error!("Failed to delete file. Unstable state");
                process::abort()
            }
        }
    }

    if !schemas.is_empty() {
        Ok(Some(Schema::try_merge(schemas).unwrap()))
    } else {
        Ok(None)
    }
}

fn parquet_writer_props() -> WriterPropertiesBuilder {
    WriterProperties::builder()
        .set_max_row_group_size(CONFIG.parseable.row_group_size)
        .set_compression(CONFIG.parseable.parquet_compression.into())
        .set_column_encoding(
            ColumnPath::new(vec![DEFAULT_TIMESTAMP_KEY.to_string()]),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_sorting_columns(Some(vec![SortingColumn {
            column_idx: 0,
            descending: true,
            nulls_first: true,
        }]))
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
