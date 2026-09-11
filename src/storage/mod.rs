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

use object_store::path::Path;
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
    option::StandaloneWithDistributed,
    parseable::StreamNotFound,
    stats::FullStats,
    utils::json::{deserialize_string_as_true, serialize_bool_as_true},
};

use chrono::Utc;

use std::fmt::Debug;
use std::num::NonZeroUsize;

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
/// `on_request` is invoked once per issued GET so callers can keep their own
/// request accounting accurate.
pub(crate) async fn get_object_ranged<S: object_store::ObjectStore>(
    client: &S,
    src: &Path,
    log_path: &RelativePath,
    on_request: impl Fn() + Send + Sync,
) -> Result<bytes::Bytes, ObjectStorageError> {
    for attempt in 1..=RANGED_GET_MAX_ATTEMPTS {
        match get_object_ranged_once(client, src, log_path, &on_request).await {
            Ok(body) => return Ok(body),
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

    use super::{DEFAULT_MAX_OBJECT_STORE_REQUESTS, GcsConfig};

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
