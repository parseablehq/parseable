/*
 * Parseable Server (C) 2022 Parseable, Inc.
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
 */

use crate::alerts::Alerts;
use crate::metadata::{Stats, STREAM_INFO};
use crate::option::CONFIG;
use crate::query::Query;
use crate::utils;

use async_trait::async_trait;
use chrono::{DateTime, Timelike, Utc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;

use serde::Serialize;

use std::fmt::Debug;
use std::fs;
use std::iter::Iterator;
use std::path::{Path, PathBuf};

extern crate walkdir;
use walkdir::WalkDir;

/// local sync interval to move data.records to /tmp dir of that stream.
/// 60 sec is a reasonable value.
pub const LOCAL_SYNC_INTERVAL: u64 = 60;

/// duration used to configure prefix in s3 and local disk structure
/// used for storage. Defaults to 1 min.
pub const OBJECT_STORE_DATA_GRANULARITY: u32 = (LOCAL_SYNC_INTERVAL as u32) / 60;

#[async_trait]
pub trait ObjectStorage: Sync + 'static {
    fn new() -> Self;
    async fn check(&self) -> Result<(), ObjectStorageError>;
    async fn put_schema(
        &self,
        stream_name: String,
        schema: &Schema,
    ) -> Result<(), ObjectStorageError>;
    async fn create_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError>;

    async fn put_alerts(&self, stream_name: &str, alerts: Alerts)
        -> Result<(), ObjectStorageError>;
    async fn get_schema(&self, stream_name: &str) -> Result<Option<Schema>, ObjectStorageError>;
    async fn get_alerts(&self, stream_name: &str) -> Result<Alerts, ObjectStorageError>;
    async fn get_stats(&self, stream_name: &str) -> Result<Stats, ObjectStorageError>;
    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError>;
    async fn upload_file(&self, key: &str, path: &str) -> Result<(), ObjectStorageError>;
    async fn query(
        &self,
        query: &Query,
        results: &mut Vec<RecordBatch>,
    ) -> Result<(), ObjectStorageError>;
    async fn s3_sync(&self) -> Result<(), ObjectStorageError> {
        if !Path::new(&CONFIG.parseable.local_disk_path).exists() {
            return Ok(());
        }

        let streams = STREAM_INFO.list_streams();

        for stream in streams {
            // get dir
            let dir = StorageDir::new(stream.clone());

            for file in WalkDir::new(&dir.data_path)
                .min_depth(1)
                .max_depth(1)
                .into_iter()
                .filter_map(|file| file.ok())
                .map(|file| file.path().to_path_buf())
                .filter(|file| file.is_file())
                .filter(|file| {
                    let is_parquet = match file.extension() {
                        Some(ext) => ext.eq_ignore_ascii_case("parquet"),
                        None => false,
                    };

                    is_parquet
                })
            {
                let filename = file.file_name().unwrap().to_str().unwrap();
                let file_suffix = str::replacen(filename, ".", "/", 3);
                let s3_path = format!("{}/{}", stream, file_suffix);

                let _put_parquet_file = self.upload_file(&s3_path, file.to_str().unwrap()).await?;
                if let Err(e) = fs::remove_file(&file) {
                    log::error!(
                        "Error deleting parquet file in path {} due to error [{}]",
                        file.to_string_lossy(),
                        e
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
pub struct LogStream {
    pub name: String,
}

#[derive(Debug)]
pub struct StorageDir {
    pub stream_name: String,
    pub data_path: PathBuf,
}

// Storage Dir is a type which can move files form datapath to temp dir
impl StorageDir {
    pub fn new(stream_name: String) -> Self {
        let data_path = CONFIG.parseable.local_stream_data_path(&stream_name);

        Self {
            stream_name,
            data_path,
        }
    }

    fn filename_by_time(time: DateTime<Utc>) -> String {
        let uri = utils::date_to_prefix(time.date())
            + &utils::hour_to_prefix(time.hour())
            + &utils::minute_to_prefix(time.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap();
        let local_uri = str::replace(&uri, "/", ".");
        let hostname = utils::hostname_unchecked();
        format!("{}{}.data.parquet", local_uri, hostname)
    }

    fn filename_by_current_time() -> String {
        let datetime = Utc::now();
        Self::filename_by_time(datetime)
    }

    pub fn path_by_current_time(&self) -> PathBuf {
        self.data_path.join(Self::filename_by_current_time())
    }

    pub fn partial_data(&self) -> Vec<PathBuf> {
        self.data_path
            .read_dir()
            .expect("read dir failed")
            .flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().map_or(false, |ext| ext.eq("arrow")))
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStorageError {
    #[error("Bucket {0} not found")]
    NoSuchBucket(String),
    #[error("Connection Error: {0}")]
    ConnectionError(Box<dyn std::error::Error + Send + 'static>),
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("DataFusion Error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
    #[error("Unhandled Error: {0}")]
    UnhandledError(Box<dyn std::error::Error + Send + 'static>),
    #[error("Authentication Error: {0}")]
    AuthenticationError(Box<dyn std::error::Error + Send + 'static>),
}
