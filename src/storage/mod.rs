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

use object_store::path::Path;
use relative_path::RelativePath;
use serde::{Deserialize, Serialize};
use tokio::task::JoinError;

use crate::{
    catalog::snapshot::Snapshot,
    event::format::LogSourceEntry,
    metadata::SchemaVersion,
    option::StandaloneWithDistributed,
    parseable::StreamNotFound,
    stats::FullStats,
    utils::json::{deserialize_string_as_true, serialize_bool_as_true},
};

use chrono::Utc;

use std::fmt::Debug;

mod azure_blob;
pub mod field_stats;
mod gcs;
mod localfs;
mod metrics_layer;
pub mod object_storage;
pub mod retention;
mod s3;
pub mod store_metadata;

use self::retention::Retention;
pub use azure_blob::AzureBlobConfig;
pub use gcs::GcsConfig;
pub use localfs::FSConfig;
pub use object_storage::{ObjectStorage, ObjectStorageProvider};
pub use s3::S3Config;
pub use store_metadata::{
    StorageMetadata, put_remote_metadata, put_staging_metadata, resolve_parseable_metadata,
};

// metadata file names in a Stream prefix
pub const STREAM_METADATA_FILE_NAME: &str = ".stream.json";
pub const PARSEABLE_METADATA_FILE_NAME: &str = ".parseable.json";
pub const STREAM_ROOT_DIRECTORY: &str = ".stream";
pub const PARSEABLE_ROOT_DIRECTORY: &str = ".parseable";
pub const SCHEMA_FILE_NAME: &str = ".schema";
pub const ALERTS_ROOT_DIRECTORY: &str = ".alerts";
pub const SETTINGS_ROOT_DIRECTORY: &str = ".settings";
pub const TARGETS_ROOT_DIRECTORY: &str = ".targets";
pub const MANIFEST_FILE: &str = "manifest.json";

// max concurrent request allowed for datafusion object store
const MAX_OBJECT_STORE_REQUESTS: usize = 1000;

// all the supported permissions
// const PERMISSIONS_READ: &str = "readonly";
// const PERMISSIONS_WRITE: &str = "writeonly";
// const PERMISSIONS_DELETE: &str = "delete";
// const PERMISSIONS_READ_WRITE: &str = "readwrite";
const ACCESS_ALL: &str = "all";

pub const CURRENT_OBJECT_STORE_VERSION: &str = "v6";
pub const CURRENT_SCHEMA_VERSION: &str = "v6";

const CONNECT_TIMEOUT_SECS: u64 = 5;
const REQUEST_TIMEOUT_SECS: u64 = 300;

pub const MIN_MULTIPART_UPLOAD_SIZE: usize = 25 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectStoreFormat {
    /// Version of schema registry
    pub version: String,
    /// Version of schema, defaults to v0 if not set
    #[serde(default)]
    pub schema_version: SchemaVersion,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<Retention>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_partition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_partition_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_partition: Option<String>,
    #[serde(
        default,    // sets to false if not configured
        deserialize_with = "deserialize_string_as_true",
        serialize_with = "serialize_bool_as_true",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub static_schema_flag: bool,
    #[serde(default)]
    pub hot_tier_enabled: bool,
    #[serde(default)]
    pub stream_type: StreamType,
    #[serde(default)]
    pub log_source: Vec<LogSourceEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamInfo {
    #[serde(rename = "created-at")]
    pub created_at: String,
    #[serde(rename = "first-event-at")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_event_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_partition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_partition_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_partition: Option<String>,
    #[serde(
        default,    // sets to false if not configured
        deserialize_with = "deserialize_string_as_true",
        serialize_with = "serialize_bool_as_true",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub static_schema_flag: bool,
    #[serde(default)]
    pub stream_type: StreamType,
    pub log_source: Vec<LogSourceEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum StreamType {
    #[default]
    UserDefined,
    Internal,
}

impl From<&str> for StreamType {
    fn from(stream_type: &str) -> Self {
        match stream_type {
            "UserDefined" => Self::UserDefined,
            "Internal" => Self::Internal,
            t => panic!("Unexpected stream type: {t}"),
        }
    }
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
            schema_version: SchemaVersion::V1, // Newly created streams should be v1
            objectstore_format: CURRENT_OBJECT_STORE_VERSION.to_string(),
            stream_type: StreamType::UserDefined,
            created_at: Utc::now().to_rfc3339(),
            first_event_at: None,
            owner: Owner::new("".to_string(), "".to_string()),
            permissions: vec![Permisssion::new("parseable".to_string())],
            stats: FullStats::default(),
            snapshot: Snapshot::default(),
            retention: None,
            time_partition: None,
            time_partition_limit: None,
            custom_partition: None,
            static_schema_flag: false,
            hot_tier_enabled: false,
            log_source: vec![LogSourceEntry::default()],
        }
    }
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

    #[error("{0}")]
    StreamNotFound(#[from] StreamNotFound),

    #[error("{0}")]
    StandaloneWithDistributed(#[from] StandaloneWithDistributed),

    #[error("JoinError: {0}")]
    JoinError(#[from] JoinError),
}

pub fn to_object_store_path(path: &RelativePath) -> Path {
    Path::from(path.as_str())
}
