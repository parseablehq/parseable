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

use crate::option::CONFIG;
use crate::stats::Stats;

use chrono::Local;

use std::fmt::Debug;
use std::fs::create_dir_all;

mod localfs;
mod object_storage;
pub mod retention;
mod s3;
pub mod staging;
mod store_metadata;

pub use localfs::{FSConfig, LocalFS};
pub use object_storage::{ObjectStorage, ObjectStorageProvider};
pub use s3::{S3Config, S3};
pub use store_metadata::StorageMetadata;

pub use self::staging::StorageDir;
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

#[derive(serde::Serialize)]
pub struct LogStream {
    pub name: String,
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
