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
use bytes::Bytes;
use chrono::{Duration, Timelike, Utc};
use datafusion::arrow::record_batch::RecordBatch;
use serde::Serialize;

use std::fmt::Debug;
use std::fs;
use std::io;
use std::iter::Iterator;
use std::path::{Path, PathBuf};

extern crate walkdir;
use walkdir::WalkDir;

/// local sync interval to move data.parquet to /tmp dir of that stream.
/// 60 sec is a reasonable value.
pub const LOCAL_SYNC_INTERVAL: u64 = 60;

/// duration used to configure prefix in s3 and local disk structure
/// used for storage. Defaults to 1 min.
pub const OBJECT_STORE_DATA_GRANULARITY: u32 = (LOCAL_SYNC_INTERVAL as u32) / 60;

#[async_trait]
pub trait ObjectStorage: Sync + 'static {
    async fn check(&self) -> Result<(), ObjectStorageError>;
    async fn put_schema(&self, stream_name: String, body: String)
        -> Result<(), ObjectStorageError>;
    async fn create_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError>;

    async fn put_alerts(&self, stream_name: &str, alerts: Alerts)
        -> Result<(), ObjectStorageError>;
    async fn get_schema(&self, stream_name: &str) -> Result<Bytes, ObjectStorageError>;
    async fn get_alerts(&self, stream_name: &str) -> Result<Alerts, ObjectStorageError>;
    async fn get_stats(&self, stream_name: &str) -> Result<Stats, ObjectStorageError>;
    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError>;
    async fn upload_file(&self, key: &str, path: &str) -> Result<(), ObjectStorageError>;
    async fn query(
        &self,
        query: &Query,
        results: &mut Vec<RecordBatch>,
    ) -> Result<(), ObjectStorageError>;
    fn local_sync(&self) -> io::Result<()> {
        // If the local data path doesn't exist yet, return early.
        // This method will be called again after next ticker interval
        if !Path::new(&CONFIG.parseable.local_disk_path).exists() {
            return Ok(());
        }

        let streams = STREAM_INFO.list_streams();

        // entries here means all the streams present on local disk
        for stream in streams {
            let sync = StorageSync::new(&stream);

            // if data.parquet file not present, skip this stream
            if !sync.dir.parquet_path_exists() {
                continue;
            }

            if let Err(e) = sync.dir.create_temp_dir() {
                log::error!(
                    "Error creating tmp directory for {} due to error [{}]",
                    &stream,
                    e
                );
                continue;
            }

            if let Err(e) = sync.move_parquet_to_temp() {
                log::error!(
                    "Error copying parquet from stream directory in [{}] to tmp directory [{}] due to error [{}]",
                    sync.dir.data_path.to_string_lossy(),
                    sync.dir.temp_dir.to_string_lossy(),
                    e
                );
                continue;
            }
        }

        Ok(())
    }

    async fn s3_sync(&self) -> Result<(), ObjectStorageError> {
        if !Path::new(&CONFIG.parseable.local_disk_path).exists() {
            return Ok(());
        }

        let streams = STREAM_INFO.list_streams();

        for stream in streams {
            let dir = StorageDir::new(&stream);

            for file in WalkDir::new(dir.temp_dir)
                .min_depth(1)
                .max_depth(1)
                .into_iter()
                .filter_map(|file| file.ok())
                .map(|file| file.path().to_path_buf())
                .filter(|file| file.is_file())
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
    pub data_path: PathBuf,
    pub temp_dir: PathBuf,
}

impl StorageDir {
    pub fn new(stream_name: &str) -> Self {
        let data_path = CONFIG.parseable.local_stream_data_path(stream_name);
        let temp_dir = data_path.join("tmp");

        Self {
            data_path,
            temp_dir,
        }
    }

    fn create_temp_dir(&self) -> io::Result<()> {
        fs::create_dir_all(&self.temp_dir)
    }

    fn move_parquet_to_temp(&self, filename: String) -> io::Result<()> {
        fs::rename(
            self.data_path.join("data.parquet"),
            self.temp_dir.join(filename),
        )
    }

    fn parquet_path_exists(&self) -> bool {
        self.data_path.join("data.parquet").exists()
    }
}

struct StorageSync {
    pub dir: StorageDir,
    time: chrono::DateTime<Utc>,
}

impl StorageSync {
    fn new(stream_name: &str) -> Self {
        let dir = StorageDir::new(&stream_name);
        let time = Utc::now();
        Self { dir, time }
    }

    fn move_parquet_to_temp(&self) -> io::Result<()> {
        let time = self.time - Duration::minutes(OBJECT_STORE_DATA_GRANULARITY as i64);
        let uri = utils::date_to_prefix(time.date())
            + &utils::hour_to_prefix(time.hour())
            + &utils::minute_to_prefix(time.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap();
        let local_uri = str::replace(&uri, "/", ".");
        let hostname = utils::hostname_unchecked();
        let parquet_file_local = format!("{}{}.data.parquet", local_uri, hostname);
        self.dir.move_parquet_to_temp(parquet_file_local)
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
}

impl From<ObjectStorageError> for crate::error::Error {
    fn from(e: ObjectStorageError) -> Self {
        crate::error::Error::Storage(e)
    }
}
