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

use crate::stats::Stats;

use chrono::Local;

use std::{collections::HashMap, fmt::Debug};

mod localfs;
mod metrics_layer;
mod object_storage;
pub mod retention;
mod s3;
pub mod staging;
mod store_metadata;

pub use localfs::{FSConfig, LocalFS};
pub use object_storage::{ObjectStorage, ObjectStorageProvider};
pub use s3::{S3Config, S3};
pub use store_metadata::{
    put_remote_metadata, put_staging_metadata, resolve_parseable_metadata, StorageMetadata,
};

use self::retention::Retention;
pub use self::staging::StorageDir;

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
    #[serde(default)]
    pub retention: Retention,
    #[serde(default)]
    pub modules: HashMap<String, serde_json::Value>,
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
            retention: Retention::default(),
            modules: HashMap::default(),
        }
    }
}

impl ObjectStoreFormat {
    fn set_id(&mut self, id: String) {
        self.owner.id.clone_from(&id);
        self.owner.group = id;
    }
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
