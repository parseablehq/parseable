/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStoreExt, path::Path};
use relative_path::RelativePath;
use serde::{Deserialize, Serialize};
use tokio::task::JoinError;

use crate::{
    catalog::snapshot::Snapshot,
    event::format::LogSourceEntry,
    handlers::{DatasetTag, TelemetryType},
    hottier::StreamHotTier,
    metadata::SchemaVersion,
    metastore::{MetastoreErrorDetail, metastore_traits::MetastoreObject},
    metrics::{
        increment_bytes_scanned_in_object_store_calls_by_date,
        increment_files_scanned_in_object_store_calls_by_date,
        increment_object_store_calls_by_date,
        increment_partial_file_scans_in_object_store_calls_by_date,
    },
    option::StandaloneWithDistributed,
    parseable::{DEFAULT_TENANT, LogStream, StreamNotFound},
    stats::FullStats,
    utils::json::{deserialize_string_as_true, serialize_bool_as_true},
};

use chrono::Utc;

use std::collections::HashSet;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;

mod azure_blob;
pub mod field_stats;
mod gcs;
mod localfs;
#[cfg(test)]
pub(crate) use localfs::LocalFS;
mod metrics_layer;
pub use metrics_layer::{StorageMetricDelta, StorageMetricWindow, storage_metrics_provider_label};
pub mod object_storage;
pub mod retention;
mod s3;
pub mod store_metadata;

/// Size of the first request issued by [`get_object_ranged`], which doubles as
/// the size probe. Anything at or below this is fetched as one plain GET, with
/// no generation pinning and no retry path. Sized so that a compressed manifest
/// stays comfortably inside it; only legacy uncompressed manifests should ever
/// need ranging.
pub(crate) const RANGED_GET_PROBE_BYTES: u64 = 64 * 1024 * 1024;

/// Size of each byte range issued by [`get_object_ranged`] after the first.
pub(crate) const RANGED_GET_CHUNK_BYTES: u64 = 16 * 1024 * 1024;

/// Number of byte ranges of a single object fetched concurrently.
pub(crate) const RANGED_GET_CONCURRENCY: usize = 8;

/// How many times a ranged read restarts when the object is rewritten under it.
pub(crate) const RANGED_GET_MAX_ATTEMPTS: u32 = 3;

/// How many objects `ObjectStorage::get_objects` downloads concurrently. These
/// are small metadata objects (`stream.json` and friends) where the cost is
/// round trip latency, so overlapping them is close to a pure win.
pub(crate) const GET_OBJECTS_CONCURRENCY: usize = 32;

/// Fetch an object as a series of bounded byte ranges rather than one stream.
///
/// A single multi-hundred-MB response body is regularly killed mid transfer by
/// the peer ("peer closed connection without sending TLS close_notify"), which
/// discards the entire download. Splitting the read keeps every stream short
/// lived and lets `object_store`'s retry layer recover a single chunk instead of
/// the whole object.
///
/// The first range doubles as a size probe: the response reports the object's
/// full length, so an object smaller than [`RANGED_GET_PROBE_BYTES`] costs
/// exactly one request and needs no HEAD.
///
/// Counts each issued GET and records file/byte metrics only after a successful read.
pub(crate) async fn get_object_ranged<S: object_store::ObjectStore>(
    client: &S,
    log_path: &RelativePath,
    tenant_id: &Option<String>,
) -> Result<bytes::Bytes, ObjectStorageError> {
    let src = to_object_store_path(log_path);
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let date = Utc::now().date_naive().to_string();
    let on_request = || increment_object_store_calls_by_date("GET", &date, tenant);
    for attempt in 1..=RANGED_GET_MAX_ATTEMPTS {
        match get_object_ranged_once(client, &src, log_path, &on_request).await {
            Ok(body) => {
                increment_files_scanned_in_object_store_calls_by_date("GET", 1, &date, tenant);
                increment_bytes_scanned_in_object_store_calls_by_date(
                    "GET",
                    body.len() as u64,
                    &date,
                    tenant,
                );
                return Ok(body);
            }
            // The object was rewritten while its ranges were being read. Every
            // range fetched so far may belong to a different generation, so the
            // assembled buffer is discarded and the read restarts.
            Err(RangedReadError::ObjectChanged) if attempt < RANGED_GET_MAX_ATTEMPTS => {
                tracing::warn!(
                    path = %log_path,
                    attempt,
                    "object changed during ranged read, retrying"
                );
            }
            Err(RangedReadError::ObjectChanged) => {
                return Err(ObjectStorageError::Custom(format!(
                    "{log_path} was rewritten during every one of \
                     {RANGED_GET_MAX_ATTEMPTS} ranged read attempts"
                )));
            }
            Err(RangedReadError::Failed(err)) => return Err(err),
        }
    }
    unreachable!("loop returns on the final attempt")
}

/// Distinguishes "the object moved under us, try again" from a real failure.
/// `ObjectStorageError` boxes `object_store::Error`, so the precondition case
/// has to be carried out of the inner read separately to stay matchable.
enum RangedReadError {
    ObjectChanged,
    Failed(ObjectStorageError),
}

impl From<ObjectStorageError> for RangedReadError {
    fn from(err: ObjectStorageError) -> Self {
        Self::Failed(err)
    }
}

async fn get_object_ranged_once<S: object_store::ObjectStore>(
    client: &S,
    src: &Path,
    log_path: &RelativePath,
    on_request: &(impl Fn() + Send + Sync),
) -> Result<bytes::Bytes, RangedReadError> {
    use futures::StreamExt;

    let first = client
        .get_opts(
            src,
            object_store::GetOptions {
                range: Some(object_store::GetRange::Bounded(0..RANGED_GET_PROBE_BYTES)),
                ..Default::default()
            },
        )
        .await;
    on_request();
    let first = match first {
        Ok(first) => first,
        Err(err) => return Err(ObjectStorageError::from(err).into()),
    };

    let total = first.meta.size;
    // Pin the generation seen by the first range. Ranged reads are not atomic:
    // each range is its own request, and these objects are rewritten while
    // being read, so without this the ranges can splice two different versions
    // of the object together into a corrupt buffer.
    //
    // The ETag only, never the store's version/generation id: these objects are
    // rewritten constantly and the superseded generation is deleted, so pinning
    // that turns a concurrent rewrite into a 404 for an object which plainly
    // exists. A stale ETag returns 412, which is what the retry expects.
    let e_tag = first.meta.e_tag.clone();
    let head = match first.bytes().await {
        Ok(head) => head,
        Err(err) => return Err(ObjectStorageError::from(err).into()),
    };

    if total <= head.len() as u64 {
        return Ok(head);
    }

    // Without an ETag the follow up ranges carry no precondition, so a rewrite
    // mid read splices two generations together silently. Fail instead.
    let Some(e_tag) = e_tag else {
        return Err(ObjectStorageError::Custom(format!(
            "ranged read of {log_path} needs an ETag to pin the object, store returned none"
        ))
        .into());
    };

    // `buffered` preserves range order, so chunks append straight into the
    // output buffer and no second full size assembly copy is needed.
    let ranges: Vec<std::ops::Range<u64>> = (head.len() as u64..total)
        .step_by(RANGED_GET_CHUNK_BYTES as usize)
        .map(|start| start..(start + RANGED_GET_CHUNK_BYTES).min(total))
        .collect();

    let mut buf = Vec::with_capacity(total as usize);
    buf.extend_from_slice(&head);
    drop(head);

    let chunks = futures::stream::iter(ranges)
        .map(|range| {
            let e_tag = Some(e_tag.clone());
            async move {
                let result = client
                    .get_opts(
                        src,
                        object_store::GetOptions {
                            range: Some(object_store::GetRange::Bounded(range)),
                            if_match: e_tag,
                            ..Default::default()
                        },
                    )
                    .await;
                on_request();
                match result {
                    Ok(result) => result.bytes().await,
                    Err(err) => Err(err),
                }
            }
        })
        .buffered(RANGED_GET_CONCURRENCY);
    futures::pin_mut!(chunks);

    while let Some(chunk) = chunks.next().await {
        match chunk {
            Ok(chunk) => buf.extend_from_slice(&chunk),
            Err(err) => {
                // A stale `if_match` is answered with 412. `NotFound` counts as
                // the same case: the first range proved the object exists, so a
                // later range missing it means it was replaced mid read, not
                // that it is absent. Reporting absence here would let the
                // caller silently drop this manifest's files from the query.
                if matches!(
                    err,
                    object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. }
                ) {
                    return Err(RangedReadError::ObjectChanged);
                }
                return Err(ObjectStorageError::from(err).into());
            }
        }
    }

    // A short buffer means the ranges did not cover the object. Catch it here
    // rather than handing a truncated body to a parser downstream.
    if buf.len() as u64 != total {
        return Err(ObjectStorageError::Custom(format!(
            "ranged read of {log_path} assembled {} bytes, expected {total}",
            buf.len()
        ))
        .into());
    }

    Ok(bytes::Bytes::from(buf))
}

/// Cross-platform positional write: pwrite(2) on Unix, seek_write+loop on Windows.
/// Both APIs accept `&File`, so concurrent ranged downloads can share an Arc<File>.
#[inline(always)]
pub(crate) fn write_all_at(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut buf = buf;
        let mut offset = offset;
        while !buf.is_empty() {
            match file.seek_write(buf, offset) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        compile_error!("write_all_at: unsupported platform");
    }
}

use self::retention::Retention;
pub use azure_blob::AzureBlobConfig;
pub use gcs::GcsConfig;
pub use localfs::FSConfig;
pub use object_storage::{ObjectStorage, ObjectStorageProvider};
pub use s3::S3Config;
pub use store_metadata::{
    IngestionQuota, IngestionQuotaType, QuotaPeriod, StorageMetadata, put_remote_metadata,
    put_staging_metadata, resolve_parseable_metadata,
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
// top-level registry of streams currently being deleted; kept outside every
// stream's own prefix so a bulk prefix-delete can never sweep up a marker
// that's supposed to survive it (see is_tombstoned/tombstone_path)
pub const TOMBSTONE_ROOT_DIRECTORY: &str = ".tombstones";
// the marker itself lives one level below `{tenant}/{stream_name}/`, not as
// a leaf key directly named after the stream: list_dirs_relative on every
// backend (S3/GCS/Azure via list-with-delimiter's common_prefixes, LocalFS
// via read_dir + is_dir) only surfaces child *directories*, never leaf
// objects, so a tombstone recorded as a bare `{stream_name}` key would be
// invisible to the restart-recovery scan that discovers tombstoned streams
pub const TOMBSTONE_MARKER_FILE_NAME: &str = ".tombstone";

// max concurrent request allowed for datafusion object store, overridable per
// backend with P_MAX_OBJECT_STORE_REQUESTS.
//
// NonZero because this becomes the permit count of the `LimitStore` semaphore:
// at zero every object store request would await a permit that never arrives,
// hanging the server silently rather than failing.
pub const DEFAULT_MAX_OBJECT_STORE_REQUESTS: NonZeroUsize = NonZeroUsize::new(1000).unwrap();

// all the supported permissions
// const PERMISSIONS_READ: &str = "readonly";
// const PERMISSIONS_WRITE: &str = "writeonly";
// const PERMISSIONS_DELETE: &str = "delete";
// const PERMISSIONS_READ_WRITE: &str = "readwrite";
const ACCESS_ALL: &str = "all";

pub const CURRENT_OBJECT_STORE_VERSION: &str = "v7";
pub const CURRENT_SCHEMA_VERSION: &str = "v7";

const CONNECT_TIMEOUT_SECS: u64 = 5;
const REQUEST_TIMEOUT_SECS: u64 = 120;
const RETRY_TIMEOUT_SECS: u64 = 180;

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hot_tier: Option<StreamHotTier>,
    #[serde(default)]
    pub stream_type: StreamType,
    #[serde(default)]
    pub log_source: Vec<LogSourceEntry>,
    #[serde(default)]
    pub telemetry_type: TelemetryType,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dataset_tags: Vec<DatasetTag>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dataset_labels: Vec<String>,
    #[serde(default = "default_infer_timestamp")]
    pub infer_timestamp: bool,
}

fn default_infer_timestamp() -> bool {
    true
}

impl MetastoreObject for ObjectStoreFormat {
    fn get_object_path(&self) -> String {
        unimplemented!()
    }

    fn get_object_id(&self) -> String {
        unimplemented!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamInfo {
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_event_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_event_at: Option<String>,
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
    #[serde(default)]
    pub telemetry_type: TelemetryType,
    #[serde(default)]
    pub hot_tier_enabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dataset_tags: Vec<DatasetTag>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dataset_labels: Vec<String>,
    #[serde(default = "default_infer_timestamp")]
    pub infer_timestamp: bool,
}

impl StreamInfo {
    /// Creates a StreamInfo from LogStreamMetadata
    /// and first_event_at and latest_event_at timestamps
    pub fn from_metadata(
        metadata: &crate::metadata::LogStreamMetadata,
        first_event_at: Option<String>,
        latest_event_at: Option<String>,
    ) -> Self {
        StreamInfo {
            stream_type: metadata.stream_type,
            created_at: metadata.created_at.clone(),
            first_event_at,
            latest_event_at,
            time_partition: metadata.time_partition.clone(),
            time_partition_limit: metadata.time_partition_limit.map(|limit| limit.to_string()),
            custom_partition: metadata.custom_partition.clone(),
            static_schema_flag: metadata.static_schema_flag,
            log_source: metadata.log_source.clone(),
            telemetry_type: metadata.telemetry_type,
            hot_tier_enabled: metadata.hot_tier_enabled,
            dataset_tags: metadata.dataset_tags.clone(),
            dataset_labels: metadata.dataset_labels.clone(),
            infer_timestamp: metadata.infer_timestamp,
        }
    }
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
            hot_tier: None,
            log_source: vec![LogSourceEntry::default()],
            telemetry_type: TelemetryType::Logs,
            dataset_tags: Vec::new(),
            dataset_labels: Vec::new(),
            infer_timestamp: true,
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

    #[error("MetastoreError: {0:?}")]
    MetastoreError(Box<MetastoreErrorDetail>),

    #[error("Path traversal detected: attempted path '{attempted}' escapes root '{root}'")]
    PathTraversal {
        attempted: std::path::PathBuf,
        root: std::path::PathBuf,
    },
}

pub fn to_object_store_path(path: &RelativePath) -> Path {
    Path::from(path.as_str())
}

async fn list_dates<S: object_store::ObjectStore>(
    client: &S,
    prefix: &Path,
    strip_prefix: &str,
    tenant_id: &Option<String>,
) -> Result<Vec<String>, ObjectStorageError> {
    let resp = client.list_with_delimiter(Some(prefix)).await;
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);

    let resp = resp?;

    let common_prefixes = resp.common_prefixes;

    increment_files_scanned_in_object_store_calls_by_date(
        "LIST",
        common_prefixes.len() as u64,
        &Utc::now().date_naive().to_string(),
        tenant,
    );

    // return prefixes at the root level
    let dates: Vec<_> = common_prefixes
        .iter()
        .filter_map(|path| path.as_ref().strip_prefix(strip_prefix))
        .map(String::from)
        .collect();
    tracing::Span::current().record("dates", dates.len());

    Ok(dates)
}

async fn list_old_streams<S: object_store::ObjectStore>(
    client: &S,
    head_tenant: &str,
) -> Result<HashSet<LogStream>, ObjectStorageError> {
    let resp = client.list_with_delimiter(None).await?;
    let common_prefixes = resp.common_prefixes; // get all dirs
    increment_files_scanned_in_object_store_calls_by_date(
        "LIST",
        common_prefixes.len() as u64,
        &Utc::now().date_naive().to_string(),
        DEFAULT_TENANT,
    );
    increment_object_store_calls_by_date(
        "LIST",
        &Utc::now().date_naive().to_string(),
        DEFAULT_TENANT,
    );
    // return prefixes at the root level
    let dirs: HashSet<_> = common_prefixes
        .iter()
        .filter_map(|path| path.parts().next())
        .map(|name| name.as_ref().to_string())
        .filter(|x| x != PARSEABLE_ROOT_DIRECTORY)
        .collect();

    let stream_json_check = futures::stream::FuturesUnordered::new();

    for dir in &dirs {
        let key = format!("{dir}/{STREAM_METADATA_FILE_NAME}");
        let task = async move {
            let result = client.head(&Path::from(key)).await;
            increment_object_store_calls_by_date(
                "HEAD",
                &Utc::now().date_naive().to_string(),
                head_tenant,
            );
            result.map(|_| ())
        };
        stream_json_check.push(task);
    }
    increment_files_scanned_in_object_store_calls_by_date(
        "HEAD",
        dirs.len() as u64,
        &Utc::now().date_naive().to_string(),
        DEFAULT_TENANT,
    );
    stream_json_check.try_collect::<()>().await?;

    Ok(dirs)
}

async fn list_partitions<S: object_store::ObjectStore>(
    client: &S,
    prefix: &str,
    partition: &str,
    tenant_id: &Option<String>,
) -> Result<Vec<String>, ObjectStorageError> {
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let resp = client
        .list_with_delimiter(Some(&Path::from(prefix)))
        .await?;
    increment_files_scanned_in_object_store_calls_by_date(
        "LIST",
        resp.common_prefixes.len() as u64,
        &Utc::now().date_naive().to_string(),
        tenant,
    );
    increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);

    let partitions: Vec<String> = resp
        .common_prefixes
        .iter()
        .filter_map(|path| {
            let path_str = path.as_ref();
            if let Some(stripped) = path_str.strip_prefix(prefix) {
                // Remove trailing slash if present, otherwise use as is
                let clean_path = stripped.strip_suffix('/').unwrap_or(stripped);
                Some(clean_path.to_string())
            } else {
                None
            }
        })
        .filter(|dir| dir.starts_with(partition))
        .collect();

    Ok(partitions)
}

async fn get_objects<S: object_store::ObjectStore>(
    client: &S,
    provider: &impl ObjectStorage,
    prefix: &Path,
    filter_func: Box<dyn Fn(String) -> bool + Send>,
    tenant_id: &Option<String>,
) -> Result<Vec<bytes::Bytes>, ObjectStorageError> {
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let mut list_stream = client.list(Some(prefix));

    let mut paths = vec![];
    let mut files_scanned = 0;

    // Drain the listing first, then fetch. Awaiting each GET inside the
    // listing loop serialises every download against the one before it.
    // Note: We track each streaming list item retrieval
    while let Some(meta_result) = list_stream.next().await {
        let meta = meta_result?;

        files_scanned += 1;
        let ingestor_file = filter_func(meta.location.filename().unwrap().to_string());

        if !ingestor_file {
            continue;
        }

        paths.push(
            RelativePath::from_path(meta.location.as_ref())
                .map_err(ObjectStorageError::PathError)?
                .to_relative_path_buf(),
        );
    }

    // `buffered` rather than `buffer_unordered`: callers of this generic
    // helper may rely on results following listing order.
    let res: Vec<bytes::Bytes> = futures::stream::iter(paths)
        .map(|path| async move { provider.get_object(&path, tenant_id).await })
        .buffered(GET_OBJECTS_CONCURRENCY)
        .try_collect()
        .await?;

    // Record total files scanned
    increment_files_scanned_in_object_store_calls_by_date(
        "LIST",
        files_scanned as u64,
        &Utc::now().date_naive().to_string(),
        tenant,
    );
    increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);
    Ok(res)
}

async fn get_object<S: object_store::ObjectStore>(
    client: &S,
    path: &RelativePath,
    tenant_id: &Option<String>,
) -> Result<bytes::Bytes, ObjectStorageError> {
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let resp = client.get(&to_object_store_path(path)).await;
    increment_object_store_calls_by_date("GET", &Utc::now().date_naive().to_string(), tenant);

    let resp = resp?;
    let body = resp.bytes().await?;
    tracing::Span::current().record("bytes", body.len());
    increment_files_scanned_in_object_store_calls_by_date(
        "GET",
        1,
        &Utc::now().date_naive().to_string(),
        tenant,
    );
    increment_bytes_scanned_in_object_store_calls_by_date(
        "GET",
        body.len() as u64,
        &Utc::now().date_naive().to_string(),
        tenant,
    );
    Ok(body)
}

fn record_request<T>(
    result: Result<T, object_store::Error>,
    method: &str,
    tenant_id: &Option<String>,
) -> Result<T, ObjectStorageError> {
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    increment_object_store_calls_by_date(method, &Utc::now().date_naive().to_string(), tenant);
    let value = result?;
    increment_files_scanned_in_object_store_calls_by_date(
        method,
        1,
        &Utc::now().date_naive().to_string(),
        tenant,
    );
    Ok(value)
}

async fn list_dirs<S: object_store::ObjectStore>(
    client: &S,
    prefix: &Path,
    tenant_id: &Option<String>,
) -> Result<Vec<String>, ObjectStorageError> {
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);

    let resp = client.list_with_delimiter(Some(prefix)).await;
    increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);
    let resp = resp?;
    increment_files_scanned_in_object_store_calls_by_date(
        "LIST",
        resp.common_prefixes.len() as u64,
        &Utc::now().date_naive().to_string(),
        tenant,
    );

    Ok(resp
        .common_prefixes
        .iter()
        .filter_map(|path| path.parts().next_back())
        .map(|name| name.as_ref().to_string())
        .collect::<Vec<_>>())
}

async fn get_ingestor_meta_file_paths<S: object_store::ObjectStore>(
    client: &S,
    prefix: &Path,
    tenant_id: &Option<String>,
) -> Result<Vec<relative_path::RelativePathBuf>, ObjectStorageError> {
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let mut path_arr = vec![];
    let mut files_scanned = 0;

    let mut object_stream = client.list(Some(prefix));
    increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);

    while let Some(meta_result) = object_stream.next().await {
        let meta = meta_result?;

        files_scanned += 1;
        let flag = meta.location.filename().unwrap().starts_with("ingestor");

        if flag {
            path_arr.push(relative_path::RelativePathBuf::from(meta.location.as_ref()));
        }
    }
    // Record total files scanned
    increment_files_scanned_in_object_store_calls_by_date(
        "LIST",
        files_scanned as u64,
        &Utc::now().date_naive().to_string(),
        tenant,
    );
    Ok(path_arr)
}

async fn parallel_download<S: object_store::ObjectStore>(
    client: &S,
    path: &RelativePath,
    tenant: &str,
    partial_path: std::path::PathBuf,
    chunk: u64,
    concurrency: u64,
) -> Result<(), ObjectStorageError> {
    let date = Utc::now().date_naive().to_string();
    let src = to_object_store_path(path);

    let meta = client.head(&src).await?;
    increment_object_store_calls_by_date("HEAD", &date, tenant);
    let total = meta.size;

    if let Some(parent) = partial_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let file = tokio::fs::File::create(&partial_path).await?;
    file.set_len(total).await?;
    let std_file = Arc::new(file.into_std().await);

    let ranges: Vec<std::ops::Range<u64>> = (0..total)
        .step_by(chunk as usize)
        .map(|s| s..(s + chunk).min(total))
        .collect();
    let chunk_count = ranges.len() as u64;
    tracing::Span::current()
        .record("total_bytes", total)
        .record("chunks", chunk_count);

    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency as usize));

    futures::stream::iter(ranges)
        .map(|r| {
            let src = src.clone();
            let std_file = std_file.clone();
            let semaphore = semaphore.clone();
            async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .map_err(|e| ObjectStorageError::Custom(format!("semaphore closed: {e}")))?;
                let bytes = client.get_range(&src, r.clone()).await?;
                let offset = r.start;
                tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                    write_all_at(&std_file, &bytes, offset)
                })
                .await
                .map_err(|e| ObjectStorageError::Custom(format!("join: {e}")))??;
                Ok::<_, ObjectStorageError>(())
            }
        })
        .buffer_unordered(concurrency as usize)
        .try_collect::<Vec<_>>()
        .await?;

    increment_object_store_calls_by_date("GET", &date, tenant);
    increment_files_scanned_in_object_store_calls_by_date("GET", 1, &date, tenant);
    increment_bytes_scanned_in_object_store_calls_by_date("GET", total, &date, tenant);
    increment_partial_file_scans_in_object_store_calls_by_date("GET", chunk_count, &date, tenant);
    Ok(())
}

async fn delete_prefix<S: object_store::ObjectStore>(
    client: &S,
    key: &str,
    tenant: &str,
) -> Result<(), ObjectStorageError> {
    let object_stream = client.list(Some(&(key.into())));
    increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);

    let locations_stream = object_stream.map(|result| result.map(|obj| obj.location));

    // Use delete_stream which batches deletes via the bulk delete API
    let mut delete_results = client.delete_stream(Box::pin(locations_stream));

    let mut files_deleted: u64 = 0;
    let mut failed_deletes: u64 = 0;
    while let Some(result) = delete_results.next().await {
        match result {
            Ok(_) => files_deleted += 1,
            Err(err) => {
                failed_deletes += 1;
                tracing::error!("Failed to delete object during prefix deletion: {:?}", err);
            }
        }
    }

    let total_files = files_deleted + failed_deletes;
    tracing::Span::current()
        .record("deleted", files_deleted)
        .record("failed", failed_deletes);
    increment_files_scanned_in_object_store_calls_by_date(
        "LIST",
        total_files,
        &Utc::now().date_naive().to_string(),
        tenant,
    );
    increment_files_scanned_in_object_store_calls_by_date(
        "DELETE",
        files_deleted,
        &Utc::now().date_naive().to_string(),
        tenant,
    );

    if failed_deletes > 0 {
        return Err(ObjectStorageError::UnhandledError(
            anyhow::anyhow!("Failed to delete {failed_deletes} out of {total_files} objects under prefix '{key}'").into(),
        ));
    }

    Ok(())
}

/// Append `.partial` to the file name of a local path. Used by hot-tier
/// downloaders to write to a sibling path and atomically rename on success.
pub fn partial_path(
    write_path: &std::path::Path,
) -> Result<std::path::PathBuf, ObjectStorageError> {
    let name = write_path
        .file_name()
        .ok_or_else(|| ObjectStorageError::Custom("download write_path has no file name".into()))?;
    let mut next = std::ffi::OsString::from(name);
    next.push(".partial");
    let mut buf = write_path.to_path_buf();
    buf.set_file_name(next);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use object_store::{
        ObjectStore, ObjectStoreExt, local::LocalFileSystem, memory::InMemory, path::Path,
    };
    use relative_path::RelativePath;

    use super::{
        DEFAULT_MAX_OBJECT_STORE_REQUESTS, GcsConfig, LocalFS, ObjectStorageError,
        PARSEABLE_ROOT_DIRECTORY, STREAM_METADATA_FILE_NAME, delete_prefix,
        get_ingestor_meta_file_paths, get_object, get_object_ranged, get_objects, list_dates,
        list_dirs, list_old_streams, list_partitions, parallel_download, record_request,
    };

    #[tokio::test]
    async fn partition_listings_keep_matching_children_with_and_without_tenant_prefixes() {
        let store = InMemory::new();
        for prefix in ["stream/date=2026-11-01/", "tenant/stream/date=2026-11-01/"] {
            for suffix in [
                "hour=00/minute=01/data",
                "hour=00/other/data",
                "hour=invalid/data",
                "other/data",
                "file",
            ] {
                store
                    .put(&Path::from(format!("{prefix}{suffix}")), "data".into())
                    .await
                    .unwrap();
            }
            let hours = list_partitions(&store, prefix, "hour=", &None)
                .await
                .unwrap();
            assert_eq!(hours, ["hour=00", "hour=invalid"]);
            let prefix = format!("{prefix}hour=00/");
            let minutes = list_partitions(&store, &prefix, "minute=", &None)
                .await
                .unwrap();
            assert_eq!(minutes, ["minute=01"]);
        }
        assert!(
            list_partitions(&store, "missing/", "hour=", &None)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn old_stream_discovery_excludes_metadata_and_propagates_missing_stream_files() {
        let store = InMemory::new();
        for key in [
            format!("{PARSEABLE_ROOT_DIRECTORY}/ignored"),
            format!("one/{STREAM_METADATA_FILE_NAME}"),
            format!("two/{STREAM_METADATA_FILE_NAME}"),
        ] {
            store.put(&Path::from(key), "data".into()).await.unwrap();
        }
        let streams = list_old_streams(&store, "old-stream-test").await.unwrap();
        assert_eq!(streams, ["one".to_owned(), "two".to_owned()].into());
        store
            .put(&Path::from("missing/other"), "data".into())
            .await
            .unwrap();
        assert!(matches!(
            list_old_streams(&store, "old-stream-test").await,
            Err(ObjectStorageError::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn get_objects_filters_filenames_and_preserves_listing_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let provider = LocalFS::new(dir.path().to_path_buf());
        for key in [
            "nested/a.json",
            "nested/b.skip",
            "nested/c.json",
            "outside.json",
        ] {
            store
                .put(&Path::from(key), key.as_bytes().to_vec().into())
                .await
                .unwrap();
        }
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let accepted = seen.clone();
        let filter = Box::new(move |name: String| {
            let keep = name.ends_with(".json");
            if keep {
                accepted.lock().unwrap().push(format!("nested/{name}"));
            }
            keep
        });
        let results = get_objects(&store, &provider, &Path::from("nested"), filter, &None)
            .await
            .unwrap();
        let results: Vec<_> = results
            .into_iter()
            .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
            .collect();
        assert_eq!(results, *seen.lock().unwrap());
        assert_eq!(results.len(), 2);

        let target = dir.path().join("nested/a.json");
        let filter = Box::new(move |name: String| {
            if name != "a.json" {
                return false;
            }
            std::fs::remove_file(&target).unwrap();
            true
        });
        let result = get_objects(&store, &provider, &Path::from("nested"), filter, &None).await;
        assert!(matches!(result, Err(ObjectStorageError::NoSuchKey(_))));
    }

    #[test]
    fn record_request_counts_attempts_and_successful_files() {
        use crate::metrics::{
            TOTAL_FILES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE as FILES,
            TOTAL_OBJECT_STORE_CALLS_BY_DATE as CALLS,
        };
        let tenant = Some("storage-request-test".to_owned());
        let date = chrono::Utc::now().date_naive().to_string();
        for method in ["PUT", "HEAD", "DELETE"] {
            let labels = [method, &date, tenant.as_deref().unwrap()];
            let calls = CALLS.with_label_values(&labels);
            let files = FILES.with_label_values(&labels);
            let before = (calls.get(), files.get());
            assert_eq!(record_request(Ok(7), method, &tenant).unwrap(), 7);
            let err = object_store::Error::NotFound {
                path: "missing".into(),
                source: std::io::Error::from(std::io::ErrorKind::NotFound).into(),
            };
            assert!(matches!(
                record_request::<()>(Err(err), method, &tenant),
                Err(ObjectStorageError::NoSuchKey(path)) if path == "missing"
            ));
            assert_eq!((calls.get() - before.0, files.get() - before.1), (2, 1));
        }
    }

    #[tokio::test]
    async fn cloud_listings_preserve_prefixes_and_match_ingestor_filenames() {
        let store = InMemory::new();
        let tenant = Some("storage-listing-test".to_owned());
        for key in [
            "tenant/stream/file",
            "tenant/stream/date=2026-11-01/file",
            "tenant-other/file",
            "ingestor-one",
            "nested/ingestor-two",
            "not-ingestor",
        ] {
            store.put(&Path::from(key), "data".into()).await.unwrap();
        }
        let prefix = "tenant/stream/";
        let dates = list_dates(&store, &Path::from(prefix), prefix, &tenant)
            .await
            .unwrap();
        assert_eq!(dates, ["date=2026-11-01"]);
        let root = Path::from("/");
        let dirs = list_dirs(&store, &root, &tenant).await.unwrap();
        assert_eq!(dirs, ["nested", "tenant", "tenant-other"]);
        let nested = list_dirs(&store, &Path::from("tenant"), &tenant)
            .await
            .unwrap();
        assert_eq!(nested, ["tenant", "stream"]);
        let paths = get_ingestor_meta_file_paths(&store, &root, &tenant)
            .await
            .unwrap();
        let paths: Vec<_> = paths.iter().map(|path| path.as_str()).collect();
        assert_eq!(paths, ["ingestor-one", "nested/ingestor-two"]);
        let path = RelativePath::new("ingestor-one");
        assert_eq!(get_object(&store, path, &tenant).await.unwrap(), "data");
        assert_eq!(
            get_object_ranged(&store, path, &tenant).await.unwrap(),
            "data"
        );
        let path = RelativePath::new("missing");
        assert!(matches!(
            get_object(&store, path, &tenant).await,
            Err(ObjectStorageError::NoSuchKey(_))
        ));
        assert!(matches!(
            get_object_ranged(&store, path, &tenant).await,
            Err(ObjectStorageError::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn parallel_download_writes_all_chunks_and_truncates_existing_file() {
        let store = InMemory::new();
        let dir = tempfile::tempdir().unwrap();
        let partial = dir.path().join("nested/file.partial");
        std::fs::create_dir_all(partial.parent().unwrap()).unwrap();
        for size in [0, 1, 4, 5, 12, 13] {
            let data: Vec<u8> = (0..size).collect();
            store
                .put(&Path::from("file"), data.clone().into())
                .await
                .unwrap();
            for concurrency in [1, 3] {
                std::fs::write(&partial, [255; 32]).unwrap();
                parallel_download(
                    &store,
                    RelativePath::new("file"),
                    "download-test",
                    partial.clone(),
                    4,
                    concurrency,
                )
                .await
                .unwrap();
                assert_eq!(std::fs::read(&partial).unwrap(), data);
            }
        }
    }

    #[tokio::test]
    async fn parallel_download_propagates_store_and_file_errors() {
        let store = InMemory::new();
        let dir = tempfile::tempdir().unwrap();
        let partial = dir.path().join("file.partial");
        let result = parallel_download(
            &store,
            RelativePath::new("missing"),
            "download-error-test",
            partial.clone(),
            4,
            2,
        )
        .await;
        assert!(matches!(result, Err(ObjectStorageError::NoSuchKey(_))));
        assert!(!partial.exists());

        store.put(&Path::from("file"), "data".into()).await.unwrap();
        std::fs::write(&partial, "existing").unwrap();
        let result = parallel_download(
            &store,
            RelativePath::new("file"),
            "download-error-test",
            partial.join("child"),
            4,
            2,
        )
        .await;
        assert!(matches!(result, Err(ObjectStorageError::IoError(_))));
        assert_eq!(std::fs::read(&partial).unwrap(), b"existing");
    }

    #[tokio::test]
    async fn delete_prefix_preserves_other_prefixes_and_accepts_empty_results() {
        let store = InMemory::new();
        for key in ["stream/a", "stream/nested/b", "stream-other/a"] {
            store.put(&Path::from(key), "data".into()).await.unwrap();
        }
        delete_prefix(&store, "stream/", "delete-test")
            .await
            .unwrap();
        let remaining = store.list_with_delimiter(None).await.unwrap();
        assert_eq!(remaining.common_prefixes, [Path::from("stream-other")]);
        assert!(remaining.objects.is_empty());
        delete_prefix(&store, "stream/", "delete-test")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_prefix_reports_listing_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("blocked"), "data").unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let prefix = Path::from("blocked/child");
        assert!(list_dirs(&store, &prefix, &None).await.is_err());
        assert!(
            list_dates(&store, &prefix, "blocked/child/", &None)
                .await
                .is_err()
        );
        assert!(
            list_partitions(&store, "blocked/child/", "hour=", &None)
                .await
                .is_err()
        );
        let provider = LocalFS::new(dir.path().to_path_buf());
        assert!(
            get_objects(&store, &provider, &prefix, Box::new(|_| true), &None)
                .await
                .is_err()
        );
        assert!(
            get_ingestor_meta_file_paths(&store, &prefix, &None)
                .await
                .is_err()
        );
        let err = delete_prefix(&store, "blocked/child", "delete-error-test")
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStorageError::UnhandledError(_)));
        assert_eq!(
            err.to_string(),
            "Unhandled Error: Failed to delete 1 out of 1 objects under prefix 'blocked/child'"
        );
    }

    #[derive(Debug, Parser)]
    struct TestArgs {
        #[command(flatten)]
        gcs: GcsConfig,
    }

    fn parse(args: &[&str]) -> Result<TestArgs, clap::Error> {
        TestArgs::try_parse_from(args)
    }

    #[test]
    fn max_object_store_requests_rejects_zero() {
        // Zero would leave the LimitStore semaphore with no permits, so every
        // object store request would await forever instead of failing.
        let err = parse(&[
            "test",
            "--bucket-name",
            "b",
            "--max-object-store-requests",
            "0",
        ])
        .expect_err("zero must not parse");
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn max_object_store_requests_accepts_positive_and_defaults() {
        let parsed = parse(&[
            "test",
            "--bucket-name",
            "b",
            "--max-object-store-requests",
            "500",
        ])
        .expect("positive value must parse");
        assert_eq!(parsed.gcs.max_object_store_requests.get(), 500);

        let parsed = parse(&["test", "--bucket-name", "b"]).expect("default must parse");
        assert_eq!(
            parsed.gcs.max_object_store_requests,
            DEFAULT_MAX_OBJECT_STORE_REQUESTS
        );
    }
}
