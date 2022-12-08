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
use crate::metadata::{LOCK_EXPECT, STREAM_INFO};
use crate::option::CONFIG;
use crate::query::Query;
use crate::stats::Stats;
use crate::storage::file_link::{FileLink, FileTable};
use crate::utils;

use async_trait::async_trait;
use chrono::{NaiveDateTime, Timelike, Utc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::WriterProperties;
use lazy_static::lazy_static;
use serde::Serialize;

use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{self, File};
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use self::file_link::CacheState;

/// local sync interval to move data.records to /tmp dir of that stream.
/// 60 sec is a reasonable value.
pub const LOCAL_SYNC_INTERVAL: u64 = 60;

/// duration used to configure prefix in s3 and local disk structure
/// used for storage. Defaults to 1 min.
pub const OBJECT_STORE_DATA_GRANULARITY: u32 = (LOCAL_SYNC_INTERVAL as u32) / 60;

lazy_static! {
    pub static ref CACHED_FILES: Mutex<FileTable<FileLink>> = Mutex::new(FileTable::new());
}

impl CACHED_FILES {
    pub fn track_parquet(&self) {
        let mut table = self.lock().expect("no poisoning");
        STREAM_INFO
            .list_streams()
            .into_iter()
            .flat_map(|ref stream_name| StorageDir::new(stream_name).parquet_files().into_iter())
            .for_each(|ref path| table.upsert(path))
    }
}

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

    async fn put_alerts(
        &self,
        stream_name: &str,
        alerts: &Alerts,
    ) -> Result<(), ObjectStorageError>;
    async fn put_stats(&self, stream_name: &str, stats: &Stats) -> Result<(), ObjectStorageError>;
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

    async fn s3_sync(&self) -> Result<(), MoveDataError> {
        if !Path::new(&CONFIG.parseable.local_disk_path).exists() {
            return Ok(());
        }

        let streams = STREAM_INFO.list_streams();

        let mut stream_stats = HashMap::new();

        for stream in &streams {
            // get dir
            let dir = StorageDir::new(stream);
            // walk dir, find all .arrows files and convert to parquet

            let mut arrow_files = dir.arrow_files();
            // Do not include file which is being written to
            let hot_file = dir.path_by_current_time();
            let hot_filename = hot_file.file_name().expect("is a not none filename");

            arrow_files.retain(|file| {
                !file
                    .file_name()
                    .expect("is a not none filename")
                    .eq(hot_filename)
            });

            for file in arrow_files {
                let arrow_file = File::open(&file).map_err(|_| MoveDataError::Open)?;
                let reader = StreamReader::try_new(arrow_file, None)?;
                let schema = reader.schema();
                let records = reader.filter_map(|record| match record {
                    Ok(record) => Some(record),
                    Err(e) => {
                        log::warn!("warning from arrow stream {:?}", e);
                        None
                    }
                });

                let mut parquet_path = file.clone();
                parquet_path.set_extension("parquet");
                let mut parquet_table = CACHED_FILES.lock().unwrap();
                let parquet_file =
                    fs::File::create(&parquet_path).map_err(|_| MoveDataError::Create)?;
                parquet_table.upsert(&parquet_path);

                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(parquet_file, schema, Some(props))?;

                for ref record in records {
                    writer.write(record)?;
                }

                writer.close()?;

                fs::remove_file(file).map_err(|_| MoveDataError::Delete)?;
            }

            for file in dir.parquet_files() {
                let metadata = CACHED_FILES.lock().unwrap().get_mut(&file).metadata;
                if metadata != CacheState::Idle {
                    continue;
                }

                let filename = file
                    .file_name()
                    .expect("only parquet files are returned by iterator")
                    .to_str()
                    .expect("filename is valid string");
                let file_suffix = str::replacen(filename, ".", "/", 3);
                let s3_path = format!("{}/{}", stream, file_suffix);
                CACHED_FILES
                    .lock()
                    .unwrap()
                    .get_mut(&file)
                    .set_metadata(CacheState::Uploading);
                let _put_parquet_file = self.upload_file(&s3_path, file.to_str().unwrap()).await?;
                CACHED_FILES
                    .lock()
                    .unwrap()
                    .get_mut(&file)
                    .set_metadata(CacheState::Uploaded);

                stream_stats
                    .entry(stream)
                    .and_modify(|size| *size += file.metadata().map_or(0, |meta| meta.len()))
                    .or_insert_with(|| file.metadata().map_or(0, |meta| meta.len()));

                CACHED_FILES.lock().unwrap().remove(&file);
            }
        }

        for (stream, compressed_size) in stream_stats {
            let stats = STREAM_INFO
                .read()
                .expect(LOCK_EXPECT)
                .get(stream)
                .map(|metadata| {
                    metadata.stats.add_storage_size(compressed_size);
                    Stats::from(&metadata.stats)
                });

            if let Some(stats) = stats {
                if let Err(e) = self.put_stats(stream, &stats).await {
                    log::warn!("Error updating stats to s3 due to error [{}]", e);
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
}

impl StorageDir {
    pub fn new(stream_name: &str) -> Self {
        let data_path = CONFIG.parseable.local_stream_data_path(stream_name);

        Self { data_path }
    }

    fn filename_by_time(time: NaiveDateTime) -> String {
        let uri = utils::date_to_prefix(time.date())
            + &utils::hour_to_prefix(time.hour())
            + &utils::minute_to_prefix(time.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap();
        let local_uri = str::replace(&uri, "/", ".");
        let hostname = utils::hostname_unchecked();
        format!("{}{}.data.arrows", local_uri, hostname)
    }

    fn filename_by_current_time() -> String {
        let datetime = Utc::now();
        Self::filename_by_time(datetime.naive_utc())
    }

    pub fn path_by_current_time(&self) -> PathBuf {
        self.data_path.join(Self::filename_by_current_time())
    }

    pub fn arrow_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path
            .read_dir() else { return vec![] };

        let paths: Vec<PathBuf> = dir
            .flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().map_or(false, |ext| ext.eq("arrows")))
            .collect();

        paths
    }

    pub fn parquet_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path
            .read_dir() else { return vec![] };

        dir.flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().map_or(false, |ext| ext.eq("parquet")))
            .collect()
    }
}

pub mod file_link {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
    };

    pub trait Link {
        fn links(&self) -> usize;
        fn increase_link_count(&mut self) -> usize;
        fn decreate_link_count(&mut self) -> usize;
    }

    #[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
    pub enum CacheState {
        #[default]
        Idle,
        Uploading,
        Uploaded,
    }

    #[derive(Debug)]
    pub struct FileLink {
        link: usize,
        pub metadata: CacheState,
    }

    impl Default for FileLink {
        fn default() -> Self {
            Self {
                link: 1,
                metadata: CacheState::Idle,
            }
        }
    }

    impl FileLink {
        pub fn set_metadata(&mut self, state: CacheState) {
            self.metadata = state
        }
    }

    impl Link for FileLink {
        fn links(&self) -> usize {
            self.link
        }

        fn increase_link_count(&mut self) -> usize {
            self.link.saturating_add(1)
        }

        fn decreate_link_count(&mut self) -> usize {
            self.link.saturating_sub(1)
        }
    }

    pub struct FileTable<L: Link + Default> {
        inner: HashMap<PathBuf, L>,
    }

    impl<L: Link + Default> FileTable<L> {
        pub fn new() -> Self {
            Self {
                inner: HashMap::default(),
            }
        }

        pub fn upsert(&mut self, path: &Path) {
            if let Some(entry) = self.inner.get_mut(path) {
                entry.increase_link_count();
            } else {
                self.inner.insert(path.to_path_buf(), L::default());
            }
        }

        pub fn remove(&mut self, path: &Path) {
            let Some(link_count) = self.inner.get_mut(path).map(|entry| entry.decreate_link_count()) else { return };
            if link_count == 0 {
                let _ = std::fs::remove_file(path);
                self.inner.remove(path);
            }
        }

        pub fn get_mut(&mut self, path: &Path) -> &mut L {
            self.inner.get_mut(path).unwrap()
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MoveDataError {
    #[error("Unable to Open file after moving")]
    Open,
    #[error("Unable to create recordbatch stream")]
    Arrow(#[from] ArrowError),
    #[error("Could not generate parquet file")]
    Parquet(#[from] ParquetError),
    #[error("Object Storage Error {0}")]
    ObjectStorag(#[from] ObjectStorageError),
    #[error("Could not generate parquet file")]
    Create,
    #[error("Could not delete temp arrow file")]
    Delete,
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
