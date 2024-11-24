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
 */

use crate::{
    catalog::snapshot::Snapshot, metadata::error::stream_info::MetadataError, stats::FullStats,
};

use chrono::Local;

use std::fmt::Debug;

mod azure_blob;
mod localfs;
mod metrics_layer;
pub(crate) mod object_storage;
pub mod retention;
mod s3;
pub mod staging;
mod store_metadata;

use self::retention::Retention;
pub use self::staging::StorageDir;
pub use azure_blob::AzureBlobConfig;
pub use localfs::FSConfig;
pub use object_storage::{ObjectStorage, ObjectStorageProvider};
pub use s3::S3Config;
pub use store_metadata::{
    put_remote_metadata, put_staging_metadata, resolve_parseable_metadata, StorageMetadata,
};

// metadata file names in a Stream prefix
pub const STREAM_METADATA_FILE_NAME: &str = ".stream.json";
pub const PARSEABLE_METADATA_FILE_NAME: &str = ".parseable.json";
pub const STREAM_ROOT_DIRECTORY: &str = ".stream";
pub const PARSEABLE_ROOT_DIRECTORY: &str = ".parseable";
pub const SCHEMA_FILE_NAME: &str = ".schema";
pub const ALERT_FILE_NAME: &str = ".alert.json";
pub const MANIFEST_FILE: &str = "manifest.json";

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

pub const CURRENT_OBJECT_STORE_VERSION: &str = "v5";
pub const CURRENT_SCHEMA_VERSION: &str = "v5";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectStoreFormat {
    /// Version of schema registry
    pub version: String,
    /// Version for change in the way how parquet are generated/stored.
    #[serde(rename = "objectstore-format")]
    pub objectstore_format: String,
    #[serde(rename = "created-at")]
    pub created_at: String,
    #[serde(rename = "first-event-at")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_event_at: Option<String>,
    pub owner: Owner,
    pub permissions: Vec<Permisssion>,
    pub stats: FullStats,
    #[serde(default)]
    pub snapshot: Snapshot,
    #[serde(default)]
    pub cache_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<Retention>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_partition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_partition_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_partition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub static_schema_flag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hot_tier_enabled: Option<bool>,
    pub stream_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StreamInfo {
    #[serde(rename = "created-at")]
    pub created_at: String,
    #[serde(rename = "first-event-at")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_event_at: Option<String>,
    #[serde(default)]
    pub cache_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_partition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_partition_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_partition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub static_schema_flag: Option<String>,
    pub stream_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum StreamType {
    #[default]
    UserDefined,
    Internal,
}

impl std::fmt::Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamType::UserDefined => write!(f, "UserDefined"),
            StreamType::Internal => write!(f, "Internal"),
        }
    }
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
            stream_type: Some(StreamType::UserDefined.to_string()),
            created_at: Local::now().to_rfc3339(),
            first_event_at: None,
            owner: Owner::new("".to_string(), "".to_string()),
            permissions: vec![Permisssion::new("parseable".to_string())],
            stats: FullStats::default(),
            snapshot: Snapshot::default(),
            cache_enabled: false,
            retention: None,
            time_partition: None,
            time_partition_limit: None,
            custom_partition: None,
            static_schema_flag: None,
            hot_tier_enabled: None,
        }
    }
}

impl ObjectStoreFormat {
    fn set_id(&mut self, id: String) {
        self.owner.id.clone_from(&id);
        self.owner.group = id;
    }
}

#[derive(serde::Serialize, PartialEq, Debug)]
pub struct LogStream {
    pub name: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStorageError {
    // no such key inside the object storage
    #[error("{0} not found")]
    NoSuchKey(String),
    #[error("Invalid Request: {0}")]
    Invalid(#[from] anyhow::Error),

    // custom
    #[error("{0}")]
    Custom(String),

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
    #[error("Error: {0}")]
    PathError(relative_path::FromPathError),
    #[error("Error: {0}")]
    MetadataError(#[from] MetadataError),

    #[allow(dead_code)]
    #[error("Authentication Error: {0}")]
    AuthenticationError(Box<dyn std::error::Error + Send + Sync + 'static>),
}
