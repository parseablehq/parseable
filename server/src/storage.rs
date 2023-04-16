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
 */

use crate::metadata::STREAM_INFO;
use crate::option::CONFIG;

use crate::stats::Stats;
use crate::storage::file_link::{FileLink, FileTable};
use crate::utils;

use chrono::{Local, NaiveDateTime, Timelike, Utc};
use datafusion::arrow::error::ArrowError;
use datafusion::parquet::errors::ParquetError;
use derive_more::{Deref, DerefMut};
use once_cell::sync::Lazy;

use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

mod file_link;
mod localfs;
mod object_storage;
pub mod retention;
mod s3;
mod store_metadata;

pub use localfs::{FSConfig, LocalFS};
pub use object_storage::MergedRecordReader;
pub use object_storage::{ObjectStorage, ObjectStorageProvider};
pub use s3::{S3Config, S3};
pub use store_metadata::StorageMetadata;

use self::store_metadata::{put_staging_metadata, EnvChange};

/// local sync interval to move data.records to /tmp dir of that stream.
/// 60 sec is a reasonable value.
pub const LOCAL_SYNC_INTERVAL: u64 = 60;

/// duration used to configure prefix in objectstore and local disk structure
/// used for storage. Defaults to 1 min.
pub const OBJECT_STORE_DATA_GRANULARITY: u32 = (LOCAL_SYNC_INTERVAL as u32) / 60;

// max concurrent request allowed for datafusion object store
const MAX_OBJECT_STORE_REQUESTS: usize = 1000;

// all the supported permissions
// const PERMISSIONS_READ: &str = "readonly";
// const PERMISSIONS_WRITE: &str = "writeonly";
// const PERMISSIONS_DELETE: &str = "delete";
// const PERMISSIONS_READ_WRITE: &str = "readwrite";
const ACCESS_ALL: &str = "all";

pub const CURRENT_OBJECT_STORE_VERSION: &str = "v3";
pub const CURRENT_SCHEMA_VERSION: &str = "v3";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectStoreFormat {
    /// Version of schema registry
    pub version: String,
    /// Version for change in the way how parquet are generated/stored.
    #[serde(rename = "objectstore-format")]
    pub objectstore_format: String,
    #[serde(rename = "created-at")]
    pub created_at: String,
    pub owner: Owner,
    pub permissions: Vec<Permisssion>,
    pub stats: Stats,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Owner {
    pub id: String,
    pub group: String,
}

impl Owner {
    pub fn new(id: String, group: String) -> Self {
        Self { id, group }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Permisssion {
    pub id: String,
    pub group: String,
    pub access: Vec<String>,
}

impl Permisssion {
    pub fn new(id: String) -> Self {
        Self {
            id: id.clone(),
            group: id,
            access: vec![ACCESS_ALL.to_string()],
        }
    }
}

impl Default for ObjectStoreFormat {
    fn default() -> Self {
        Self {
            version: CURRENT_SCHEMA_VERSION.to_string(),
            objectstore_format: CURRENT_OBJECT_STORE_VERSION.to_string(),
            created_at: Local::now().to_rfc3339(),
            owner: Owner::new("".to_string(), "".to_string()),
            permissions: vec![Permisssion::new("parseable".to_string())],
            stats: Stats::default(),
        }
    }
}

impl ObjectStoreFormat {
    fn set_id(&mut self, id: String) {
        self.owner.id.clone_from(&id);
        self.owner.group = id;
    }
}

pub async fn resolve_parseable_metadata() -> Result<StorageMetadata, ObjectStorageError> {
    let staging_metadata = store_metadata::get_staging_metadata()?;
    let storage = CONFIG.storage().get_object_store();
    let remote_metadata = storage.get_metadata().await?;

    let check = store_metadata::check_metadata_conflict(staging_metadata, remote_metadata);

    const MISMATCH: &str = "Could not start the server because metadata file found in staging directory does not match one in the storage";
    let res: Result<StorageMetadata, &str> = match check {
        EnvChange::None(metadata) => Ok(metadata),
        EnvChange::StagingMismatch => Err(MISMATCH),
        EnvChange::StorageMismatch => Err(MISMATCH),
        EnvChange::NewRemote => {
            Err("Could not start the server because metadata not found in storage")
        }
        EnvChange::NewStaging(mut metadata) => {
            create_dir_all(CONFIG.staging_dir())?;
            metadata.staging = CONFIG.staging_dir().canonicalize()?;
            create_remote_metadata(&metadata).await?;
            put_staging_metadata(&metadata)?;

            Ok(metadata)
        }
        EnvChange::CreateBoth => {
            create_dir_all(CONFIG.staging_dir())?;
            let metadata = StorageMetadata::new();
            create_remote_metadata(&metadata).await?;
            put_staging_metadata(&metadata)?;

            Ok(metadata)
        }
    };

    res.map_err(|err| {
        let err = format!(
            "{}. {}",
            err,
            "Join us on Parseable Slack to report this incident : https://launchpass.com/parseable"
        );
        let err: Box<dyn std::error::Error + Send + Sync + 'static> = err.into();
        ObjectStorageError::UnhandledError(err)
    })
}

async fn create_remote_metadata(metadata: &StorageMetadata) -> Result<(), ObjectStorageError> {
    let client = CONFIG.storage().get_object_store();
    client.put_metadata(metadata).await
}

pub static CACHED_FILES: Lazy<CachedFiles> =
    Lazy::new(|| CachedFiles(Mutex::new(FileTable::new())));

#[derive(Debug, Deref, DerefMut)]
pub struct CachedFiles(Mutex<FileTable<FileLink>>);

impl CachedFiles {
    pub fn track_parquet(&self) {
        let mut table = self.lock().expect("no poisoning");
        STREAM_INFO
            .list_streams()
            .into_iter()
            .flat_map(|ref stream_name| StorageDir::new(stream_name).parquet_files().into_iter())
            .for_each(|ref path| table.upsert(path))
    }
}

#[derive(serde::Serialize)]
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

    fn file_time_suffix(time: NaiveDateTime) -> String {
        let uri = utils::date_to_prefix(time.date())
            + &utils::hour_to_prefix(time.hour())
            + &utils::minute_to_prefix(time.minute(), OBJECT_STORE_DATA_GRANULARITY).unwrap();
        let local_uri = str::replace(&uri, "/", ".");
        let hostname = utils::hostname_unchecked();
        format!("{local_uri}{hostname}.data.arrows")
    }

    fn filename_by_time(stream_hash: &str, time: NaiveDateTime) -> String {
        format!("{}.{}", stream_hash, Self::file_time_suffix(time))
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
        let Ok(dir) = self.data_path
            .read_dir() else { return vec![] };

        let paths: Vec<PathBuf> = dir
            .flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().map_or(false, |ext| ext.eq("arrows")))
            .collect();

        paths
    }

    #[allow(unused)]
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
        let hot_filename = StorageDir::file_time_suffix(exclude);
        // hashmap <time, vec[paths]> but exclude where hotfilename matches
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
        let Ok(dir) = self.data_path
            .read_dir() else { return vec![] };

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

#[derive(Debug, thiserror::Error)]
pub enum MoveDataError {
    #[error("Unable to create recordbatch stream")]
    Arrow(#[from] ArrowError),
    #[error("Could not generate parquet file")]
    Parquet(#[from] ParquetError),
    #[error("Object Storage Error {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Could not generate parquet file")]
    Create,
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStorageError {
    // no such key inside the object storage
    #[error("{0} not found")]
    NoSuchKey(String),

    // Could not connect to object storage
    #[error("Connection Error: {0}")]
    ConnectionError(Box<dyn std::error::Error + Send + Sync + 'static>),

    // IO Error when reading a file or listing path
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    // Datafusion error during a query
    #[error("DataFusion Error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error("Unhandled Error: {0}")]
    UnhandledError(Box<dyn std::error::Error + Send + Sync + 'static>),

    #[allow(dead_code)]
    #[error("Authentication Error: {0}")]
    AuthenticationError(Box<dyn std::error::Error + Send + Sync + 'static>),
}
