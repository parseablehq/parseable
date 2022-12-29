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

use crate::storage::file_link::{FileLink, FileTable};
use crate::utils;

use chrono::{Local, NaiveDateTime, Timelike, Utc};
use datafusion::arrow::error::ArrowError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::parquet::errors::ParquetError;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

mod file_link;
mod localfs;
mod object_storage;
mod s3;
mod store_metadata;

pub use localfs::{FSConfig, LocalFS};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectStoreFormat {
    pub version: String,
    #[serde(rename = "objectstore-format")]
    pub objectstore_format: String,
    #[serde(rename = "created-at")]
    pub created_at: String,
    pub owner: Owner,
    pub permissions: Vec<Permisssion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Owner {
    pub id: String,
    pub group: String,
}

impl Owner {
    pub fn new(id: String, group: String) -> Self {
        Self { id, group }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            version: "v1".to_string(),
            objectstore_format: "v1".to_string(),
            created_at: Local::now().to_rfc3339(),
            owner: Owner::new("".to_string(), "".to_string()),
            permissions: vec![Permisssion::new("parseable".to_string())],
        }
    }
}

impl ObjectStoreFormat {
    fn set_id(&mut self, id: String) {
        self.owner.id.clone_from(&id);
        self.owner.group = id;
    }
}

pub async fn resolve_parseable_metadata() -> Result<(), ObjectStorageError> {
    let staging_metadata = store_metadata::get_staging_metadata()?;
    let storage = CONFIG.storage().get_object_store();
    let remote_metadata = storage.get_metadata().await?;

    let check = store_metadata::check_metadata_conflict(
        staging_metadata.as_ref(),
        remote_metadata.as_ref(),
    );

    const MISMATCH: &str = "Could not start the server because metadata file found in staging directory does not match one in the storage";
    let err: Option<&str> = match check {
        EnvChange::None => None,
        EnvChange::StagingMismatch => Some(MISMATCH),
        EnvChange::StorageMismatch => Some(MISMATCH),
        EnvChange::NewRemote => {
            Some("Could not start the server because metadata not found in storage")
        }
        EnvChange::NewStaging => {
            let mut remote_meta = remote_metadata.expect("remote metadata exists");
            create_dir_all(CONFIG.staging_dir())?;
            remote_meta.staging = CONFIG.staging_dir().canonicalize()?;
            create_remote_metadata(&remote_meta).await?;
            put_staging_metadata(&remote_meta)?;

            None
        }
        EnvChange::CreateBoth => {
            let metadata = StorageMetadata::new();
            create_remote_metadata(&metadata).await?;
            create_dir_all(CONFIG.staging_dir())?;
            put_staging_metadata(&metadata)?;

            None
        }
    };

    if let Some(err) = err {
        let err = format!(
            "{}. {}",
            err,
            "Join us on Parseable Slack to report this incident : https://launchpass.com/parseable"
        );
        let err: Box<dyn std::error::Error + Send + Sync + 'static> = err.into();
        Err(ObjectStorageError::UnhandledError(err))
    } else {
        Ok(())
    }
}

async fn create_remote_metadata(metadata: &StorageMetadata) -> Result<(), ObjectStorageError> {
    let client = CONFIG.storage().get_object_store();
    client.put_metadata(metadata).await
}

lazy_static! {
    pub static ref CACHED_FILES: Mutex<FileTable<FileLink>> = Mutex::new(FileTable::new());
    pub static ref STORAGE_RUNTIME: Arc<RuntimeEnv> = CONFIG.storage().get_datafusion_runtime();
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

#[derive(Debug, thiserror::Error)]
pub enum MoveDataError {
    #[error("Unable to Open file after moving")]
    Open,
    #[error("Unable to create recordbatch stream")]
    Arrow(#[from] ArrowError),
    #[error("Could not generate parquet file")]
    Parquet(#[from] ParquetError),
    #[error("Object Storage Error {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Could not generate parquet file")]
    Create,
    #[error("Could not delete temp arrow file")]
    Delete,
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

    #[error("Authentication Error: {0}")]
    AuthenticationError(Box<dyn std::error::Error + Send + Sync + 'static>),
}
