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

use crate::catalog::{self, snapshot::Snapshot};
use crate::event::format::LogSource;
use crate::event::format::LogSourceEntry;
use crate::handlers::DatasetTag;
use crate::handlers::http::fetch_schema;
use crate::handlers::http::modal::ingest_server::INGESTOR_EXPECT;
use crate::handlers::http::modal::ingest_server::INGESTOR_META;
use crate::handlers::http::users::{FILTER_DIR, USERS_ROOT_DIR};
use crate::metrics::increment_parquets_stored_by_date;
use crate::metrics::increment_parquets_stored_size_by_date;
use crate::metrics::{EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_STORAGE_SIZE, STORAGE_SIZE};
use crate::option::Mode;
use crate::parseable::DEFAULT_TENANT;
use crate::parseable::{LogStream, PARSEABLE, Stream};
use crate::stats::FullStats;
use crate::storage::SETTINGS_ROOT_DIRECTORY;
use crate::storage::TARGETS_ROOT_DIRECTORY;
use crate::storage::field_stats::DATASET_STATS_STREAM_NAME;
use crate::storage::field_stats::calculate_field_stats;
use crate::storage::field_stats::extract_datetime_from_parquet_path_regex;
use crate::sync::{ACTIVE_OBJECT_STORE_SYNC_FILES, FLUSH_AND_CONVERT_RUNTIME};
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::{DashSet, mapref::entry::Entry};
use datafusion::{datasource::listing::ListingTableUrl, execution::runtime_env::RuntimeEnvBuilder};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::ListResult;
use object_store::ObjectMeta;
use object_store::buffered::BufReader;
use once_cell::sync::{Lazy, OnceCell};
use rayon::prelude::*;
use relative_path::RelativePath;
use relative_path::RelativePathBuf;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::{File, remove_file};
use std::num::NonZeroU32;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::task::JoinSet;
use tracing::{Instrument, error, info, info_span, warn};
use ulid::Ulid;

/// Staged Parquet files that were uploaded successfully but could not be
/// removed locally. These paths remain excluded from upload selection until a
/// later cleanup attempt removes the local file.
static UPLOADED_STAGED_FILES_PENDING_CLEANUP: Lazy<DashSet<PathBuf>> = Lazy::new(DashSet::new);

/// Claims a staged Parquet for upload unless its remote upload has already
/// succeeded and only local deletion remains pending.
fn claim_staged_file_for_upload(path: PathBuf) -> Option<PathBuf> {
    if UPLOADED_STAGED_FILES_PENDING_CLEANUP.contains(&path) {
        return None;
    }

    match ACTIVE_OBJECT_STORE_SYNC_FILES.entry(path.clone()) {
        Entry::Vacant(entry) => {
            entry.insert(Instant::now());
            // Close the race where deletion fails after the first pending
            // check but before this active claim is acquired.
            if UPLOADED_STAGED_FILES_PENDING_CLEANUP.contains(&path) {
                ACTIVE_OBJECT_STORE_SYNC_FILES.remove(&path);
                None
            } else {
                Some(path)
            }
        }
        Entry::Occupied(_) => None,
    }
}

use super::{
    ALERTS_ROOT_DIRECTORY, MANIFEST_FILE, ObjectStorageError, ObjectStoreFormat,
    PARSEABLE_METADATA_FILE_NAME, PARSEABLE_ROOT_DIRECTORY, SCHEMA_FILE_NAME,
    STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY, TOMBSTONE_MARKER_FILE_NAME,
    TOMBSTONE_ROOT_DIRECTORY, retention::Retention,
};

/// Context for upload operations containing stream information
pub(crate) struct UploadContext {
    stream: Arc<Stream>,
    custom_partition: Option<String>,
    schema: Arc<Schema>,
}

impl UploadContext {
    fn new(stream: Arc<Stream>) -> Self {
        let custom_partition = stream.get_custom_partition();
        let schema = stream.get_schema();

        Self {
            stream,
            custom_partition,
            schema,
        }
    }
}

/// Result of a single file upload operation
pub(crate) struct UploadResult {
    file_path: std::path::PathBuf,
    manifest_file: Option<catalog::manifest::File>,
}

struct UploadedParquetFile {
    file_path: std::path::PathBuf,
    manifest_file: catalog::manifest::File,
}

static VERIFY_UPLOAD_HEAD: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    parse_verify_upload_head(std::env::var("P_VERIFY_UPLOAD_HEAD").ok().as_deref())
});

fn parse_verify_upload_head(value: Option<&str>) -> bool {
    value.is_some_and(|v| {
        let trimmed = v.trim();
        trimmed.eq_ignore_ascii_case("true") || trimmed == "1"
    })
}

fn should_verify_upload_head() -> bool {
    *VERIFY_UPLOAD_HEAD
}

/// Handles the upload of a single parquet file
#[tracing::instrument(
    name = "object_store.upload_single_parquet",
    skip(store),
    fields(stream = %stream_name, tenant = ?tenant_id, path = %path.display(), key = %stream_relative_path)
)]
async fn upload_single_parquet_file(
    store: Arc<dyn ObjectStorage>,
    path: std::path::PathBuf,
    stream_relative_path: String,
    stream_name: String,
    tenant_id: Option<String>,
) -> Result<UploadResult, (PathBuf, ObjectStorageError)> {
    let filename = path
        .file_name()
        .expect("only parquet files are returned by iterator")
        .to_str()
        .expect("filename is valid string");

    // Get the local file size once asynchronously for metrics, manifest, and optional HEAD check
    let local_file_size = tokio::fs::metadata(&path)
        .await
        .map_err(|e| {
            (
                path.clone(),
                ObjectStorageError::Custom(format!("Failed to get local file metadata: {e}")),
            )
        })?
        .len();

    // Upload the file
    store
        .upload_multipart(
            &RelativePathBuf::from(&stream_relative_path),
            &path,
            &tenant_id,
        )
        .await
        .map_err(|e| {
            error!("Failed to upload file {filename:?} to {stream_relative_path}: {e}");
            (
                path.clone(),
                ObjectStorageError::Custom(format!("Failed to upload {filename}: {e}")),
            )
        })?;

    // Post-PUT HEAD validation is opt-in via P_VERIFY_UPLOAD_HEAD (default false),
    // as S3/object-store PUT and multipart completion are strongly consistent and durable.
    if should_verify_upload_head() {
        let upload_is_valid = validate_uploaded_parquet_file(
            &store,
            &stream_relative_path,
            local_file_size,
            &stream_name,
            &tenant_id,
        )
        .await
        .map_err(|e| (path.clone(), e))?;

        if !upload_is_valid {
            // Upload validation failed, clean up the uploaded file and return error
            let _ = store
                .delete_object(&RelativePathBuf::from(&stream_relative_path), &tenant_id)
                .await;
            error!(
                "Upload size validation failed for file {filename:?}, deleted from object storage"
            );
            return Ok(UploadResult {
                file_path: path,
                manifest_file: None, // Preserve staging file for retry; no manifest created
            });
        }
    }

    // Update storage metrics
    update_storage_metrics(
        local_file_size,
        &stream_name,
        filename,
        tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
    );

    // Create manifest entry from cached ParquetMetaData when available, otherwise
    // offload the synchronous Parquet file open + footer parse to the blocking pool.
    let absolute_path = store
        .absolute_url(RelativePath::from_path(&stream_relative_path).expect("valid relative path"))
        .to_string();

    let manifest = if let Some(metadata) = catalog::manifest::take_parquet_metadata(&path) {
        catalog::manifest::create_from_parquet_metadata(absolute_path, local_file_size, &metadata)
    } else {
        let path_for_manifest = path.clone();
        tokio::task::spawn_blocking(move || {
            catalog::create_from_parquet_file(absolute_path, &path_for_manifest)
        })
        .await
        .map_err(|e| {
            (
                path.clone(),
                ObjectStorageError::UnhandledError(Box::new(e)),
            )
        })?
        .map_err(|e| (path.clone(), ObjectStorageError::from(e)))?
    };

    Ok(UploadResult {
        file_path: path,
        manifest_file: Some(manifest),
    })
}

/// Updates storage-related metrics for an uploaded file
fn update_storage_metrics(
    compressed_size: u64,
    stream_name: &str,
    filename: &str,
    tenant_id: &str,
) {
    let mut file_date_part = filename.split('.').collect::<Vec<&str>>()[0];
    file_date_part = file_date_part.split('=').collect::<Vec<&str>>()[1];
    STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet", tenant_id])
        .add(compressed_size as i64);
    EVENTS_STORAGE_SIZE_DATE
        .with_label_values(&["data", stream_name, "parquet", file_date_part, tenant_id])
        .inc_by(compressed_size);
    LIFETIME_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet", tenant_id])
        .add(compressed_size as i64);

    // billing metrics for parquet storage
    increment_parquets_stored_by_date(file_date_part, tenant_id);
    increment_parquets_stored_size_by_date(compressed_size, file_date_part, tenant_id);
}

/// Calculates field statistics if enabled and conditions are met
async fn calculate_stats_if_enabled(
    stream_name: &str,
    path: &std::path::Path,
    schema: &Arc<Schema>,
    tenant_id: Option<String>,
) {
    if stream_name != DATASET_STATS_STREAM_NAME {
        let max_field_statistics = PARSEABLE.options.max_field_statistics;
        if let Err(err) =
            calculate_field_stats(stream_name, path, schema, max_field_statistics, &tenant_id).await
        {
            warn!(
                "Error calculating field stats for stream {stream_name} (tenant {}) from parquet file {}: {err}",
                tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
                path.display()
            );
        }
    }
}

/// Validates that a parquet file uploaded to object storage matches the staging file size
#[tracing::instrument(
    name = "object_store.validate_uploaded_parquet",
    skip(store),
    fields(stream = %stream_name, tenant = ?tenant_id, key = %stream_relative_path, expected_size)
)]
async fn validate_uploaded_parquet_file(
    store: &Arc<dyn ObjectStorage>,
    stream_relative_path: &str,
    expected_size: u64,
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<bool, ObjectStorageError> {
    // Verify the file exists and has the expected size
    match store
        .head(&RelativePathBuf::from(stream_relative_path), tenant_id)
        .await
    {
        Ok(metadata) => {
            if metadata.size != expected_size {
                warn!(
                    "Uploaded file size mismatch for stream {}: expected {}, got {}",
                    stream_name, expected_size, metadata.size
                );
                Ok(false)
            } else {
                tracing::trace!(
                    "Uploaded parquet file size validated successfully for stream {}, size: {}",
                    stream_name,
                    expected_size
                );
                Ok(true)
            }
        }
        Err(e) => {
            error!(
                "Failed to get metadata for uploaded file in stream {}: {e}",
                stream_name
            );
            Ok(false)
        }
    }
}

pub trait ObjectStorageProvider: std::fmt::Debug + Send + Sync {
    fn get_datafusion_runtime(&self) -> RuntimeEnvBuilder;
    fn construct_client(&self) -> Arc<dyn ObjectStorage>;
    fn get_object_store(&self) -> Arc<dyn ObjectStorage> {
        static STORE: OnceCell<Arc<dyn ObjectStorage>> = OnceCell::new();

        STORE.get_or_init(|| self.construct_client()).clone()
    }
    fn get_endpoint(&self) -> String;
    fn name(&self) -> &'static str;
}

/// Opaque storage precondition. Preserve both fields when returning it to storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataVersion {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VersionedMetadata {
    pub bytes: Bytes,
    pub version: MetadataVersion,
}

#[derive(Debug, Clone)]
pub enum MetadataWriteCondition {
    Create,
    Update(MetadataVersion),
}

#[derive(Debug, thiserror::Error)]
pub enum ConditionalMetadataError {
    #[error("metadata changed or already exists")]
    Conflict,
    #[error("conditional metadata operation unsupported: {0}")]
    Unsupported(String),
    #[error(transparent)]
    Storage(#[from] ObjectStorageError),
}

impl From<object_store::Error> for ConditionalMetadataError {
    fn from(error: object_store::Error) -> Self {
        match error {
            object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. } => Self::Conflict,
            object_store::Error::NotImplemented { .. }
            | object_store::Error::NotSupported { .. } => Self::Unsupported(error.to_string()),
            error => Self::Storage(error.into()),
        }
    }
}

/// A single GET binds the returned bytes to their actual storage version.
pub(crate) async fn get_cloud_metadata(
    store: &dyn object_store::ObjectStore,
    path: &RelativePath,
) -> Result<Option<VersionedMetadata>, ConditionalMetadataError> {
    use object_store::ObjectStoreExt;
    let result = match store.get(&super::to_object_store_path(path)).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let version = MetadataVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    if version.e_tag.is_none() && version.version.is_none() {
        return Err(ConditionalMetadataError::Unsupported(
            "storage did not return an object version".into(),
        ));
    }
    Ok(Some(VersionedMetadata {
        bytes: result
            .bytes()
            .await
            .map_err(ConditionalMetadataError::from)?,
        version,
    }))
}

pub(crate) async fn put_cloud_metadata(
    store: &dyn object_store::ObjectStore,
    path: &RelativePath,
    bytes: Bytes,
    condition: MetadataWriteCondition,
) -> Result<MetadataVersion, ConditionalMetadataError> {
    let mode = match condition {
        MetadataWriteCondition::Create => object_store::PutMode::Create,
        MetadataWriteCondition::Update(version) => {
            if version.e_tag.is_none() && version.version.is_none() {
                return Err(ConditionalMetadataError::Unsupported(
                    "empty update version".into(),
                ));
            }
            object_store::PutMode::Update(object_store::UpdateVersion {
                e_tag: version.e_tag,
                version: version.version,
            })
        }
    };
    // Never retry with an unconditional PUT if the provider rejects preconditions.
    let result = store
        .put_opts(
            &super::to_object_store_path(path),
            bytes.into(),
            mode.into(),
        )
        .await
        .map_err(|error| match error {
            object_store::Error::NotFound { .. } => ConditionalMetadataError::Conflict,
            error => error.into(),
        })?;
    if result.e_tag.is_none() && result.version.is_none() {
        return Err(ConditionalMetadataError::Unsupported(
            "committed write did not return an object version".into(),
        ));
    }
    Ok(MetadataVersion {
        e_tag: result.e_tag,
        version: result.version,
    })
}

#[async_trait]
pub trait ObjectStorage: Debug + Send + Sync + 'static {
    /// Paths already contain the trusted tenant prefix; tenant_id is metrics context.
    async fn get_metadata_versioned(
        &self,
        _path: &RelativePath,
        _tenant_id: &Option<String>,
    ) -> Result<Option<VersionedMetadata>, ConditionalMetadataError> {
        Err(ConditionalMetadataError::Unsupported(
            "versioned GET".into(),
        ))
    }

    async fn put_metadata_conditional(
        &self,
        _path: &RelativePath,
        _bytes: Bytes,
        _condition: MetadataWriteCondition,
        _tenant_id: &Option<String>,
    ) -> Result<MetadataVersion, ConditionalMetadataError> {
        Err(ConditionalMetadataError::Unsupported(
            "conditional PUT".into(),
        ))
    }

    /// object_store 0.13 has no conditional DELETE. Use a conditional tombstone
    /// PUT on cloud stores rather than an unsafe check-then-delete sequence.
    async fn delete_metadata_conditional(
        &self,
        _path: &RelativePath,
        _version: MetadataVersion,
        _tenant_id: &Option<String>,
    ) -> Result<(), ConditionalMetadataError> {
        Err(ConditionalMetadataError::Unsupported(
            "conditional DELETE; use a conditional tombstone PUT".into(),
        ))
    }

    async fn get_buffered_reader(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<BufReader, ObjectStorageError>;
    async fn head(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<ObjectMeta, ObjectStorageError>;
    async fn parallel_chunked_download(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
        write_path: PathBuf,
    ) -> Result<(), ObjectStorageError>;
    async fn get_object(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError>;
    /// Fetch an object as concurrent bounded byte ranges instead of one stream.
    ///
    /// Intended for the few objects that are large enough (hundreds of MB) that
    /// a single response body is likely to be severed mid transfer. For
    /// everything else prefer [`ObjectStorage::get_object`]; ranging a small
    /// object buys nothing and only widens the set of response shapes involved.
    async fn get_object_ranged(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError>;
    // TODO: make the filter function optional as we may want to get all objects
    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_fun: Box<dyn Fn(String) -> bool + Send>,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Bytes>, ObjectStorageError>;
    /// List only this stream's metadata, accepting ingestor-only streams too.
    async fn stream_exists(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<bool, ObjectStorageError> {
        let prefix = stream_metadata_prefix(stream_name, tenant_id);
        let listing = self.list_with_delimiter(Some(prefix)).await?;
        record_metadata_listing(listing.objects.len(), tenant_id);
        Ok(has_stream_metadata(&listing.objects))
    }

    /// Read each schema once, retaining filenames to compare the published
    /// schema with the union without another GET of the canonical object.
    async fn schema_objects(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<(String, Bytes)>, ObjectStorageError> {
        let listing = self
            .list_with_delimiter(Some(stream_metadata_prefix(stream_name, tenant_id)))
            .await?;
        record_metadata_listing(listing.objects.len(), tenant_id);
        let requests = listing.objects.into_iter().filter_map(|object| {
            let name = object.location.filename()?.to_owned();
            if !name.contains(".schema") {
                return None;
            }
            Some(async move {
                let path = RelativePathBuf::from(object.location.as_ref());
                self.get_object(&path, tenant_id)
                    .await
                    .map(|bytes| (name, bytes))
            })
        });
        futures::stream::iter(requests)
            .buffered(super::GET_OBJECTS_CONCURRENCY)
            .try_collect()
            .await
    }

    async fn upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError>;
    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError>;
    async fn delete_prefix(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError>;
    async fn check(&self, tenant_id: &Option<String>) -> Result<(), ObjectStorageError>;
    async fn delete_stream(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError>;
    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError>;
    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError>;
    async fn list_dirs(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError>;
    async fn list_dirs_relative(
        &self,
        relative_path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError>;

    async fn list_dates(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError>;
    /// Lists the immediate “hour=” partition directories under the given date.
    /// Only immediate child entries named `hour=HH` should be returned (no trailing slash).
    /// `HH` must be zero-padded two-digit numerals (`"hour=00"` through `"hour=23"`).
    async fn list_hours(
        &self,
        stream_name: &str,
        date: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError>;

    /// Lists the immediate “minute=” partition directories under the given date/hour.
    /// Only immediate child entries named `minute=MM` should be returned (no trailing slash).
    /// `MM` must be zero-padded two-digit numerals (`"minute=00"` through `"minute=59"`).
    async fn list_minutes(
        &self,
        stream_name: &str,
        date: &str,
        hour: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError>;
    // async fn list_manifest_files(
    //     &self,
    //     stream_name: &str,
    // ) -> Result<BTreeMap<String, Vec<String>>, ObjectStorageError>;
    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError>;
    async fn delete_object(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError>;
    async fn get_ingestor_meta_file_paths(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError>;
    async fn try_delete_node_meta(
        &self,
        node_filename: String,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError>;
    /// Returns the amount of time taken by the `ObjectStore` to perform a get
    /// call.
    async fn get_latency(&self, tenant_id: &Option<String>) -> Duration {
        // It's Ok to `unwrap` here. The hardcoded value will always Result in
        // an `Ok`.
        let path = parseable_json_path();
        let start = Instant::now();
        let _ = self.get_object(&path, tenant_id).await;
        start.elapsed()
    }

    fn query_prefixes(&self, prefixes: Vec<String>) -> Vec<ListingTableUrl>;
    fn absolute_url(&self, prefix: &RelativePath) -> object_store::path::Path;
    fn store_url(&self) -> url::Url;

    async fn check_migration_guard(&self) -> Result<(), ObjectStorageError> {
        // ObjectStorage is object safe; use its primitives directly here.
        for key in [
            crate::metastore::metastores::migration::BUSY,
            crate::metastore::metastores::migration::PAIR,
        ] {
            match self.get_object(RelativePath::new(key), &None).await {
                Ok(ref id)
                    if key.ends_with("postgres-metastore-id")
                        && std::env::var("P_ALLOW_PAIRED_BUCKET")
                            .is_ok_and(|v| v.as_bytes() == id.as_ref()) => {}
                Ok(_) => {
                    return Err(ObjectStorageError::Custom(format!(
                        "bucket has {key}: PostgreSQL pairing or offline migration; complete export before object-store startup (P_ALLOW_PAIRED_BUCKET=<pairing-id> overrides pairing only)"
                    )));
                }
                Err(ObjectStorageError::NoSuchKey(_)) => {}
                Err(ObjectStorageError::IoError(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                }
                Err(e) => {
                    return Err(ObjectStorageError::Custom(format!(
                        "cannot probe migration marker {key}: {e}; retry after restoring bucket access"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Raw inventory for offline document migration; retains keys, never bodies.
    async fn metadata_inventory(
        &self,
    ) -> Result<Vec<(String, chrono::DateTime<chrono::Utc>)>, ObjectStorageError> {
        let mut pending = vec![None];
        let mut keys = Vec::new();
        use futures::{StreamExt, TryStreamExt};
        while !pending.is_empty() {
            let round = std::mem::take(&mut pending);
            let listings: Vec<_> = futures::stream::iter(round)
                .map(|prefix| async move {
                    let listing = self.list_with_delimiter(prefix).await?;
                    let mut children = Vec::new();
                    let mut found = Vec::new();
                    for prefix in listing.common_prefixes {
                        if prefix.parts().any(|c| {
                            c.as_ref().starts_with("hour=") || c.as_ref().starts_with("minute=")
                        }) {
                            continue;
                        }
                        if prefix.filename().is_some_and(|p| p.starts_with('.')) {
                            found.extend(self.metadata_hidden_prefix(prefix).await?);
                        } else {
                            children.push(Some(prefix));
                        }
                    }
                    for object in listing
                        .objects
                        .into_iter()
                        .filter(|o| !o.location.as_ref().ends_with(".parquet"))
                    {
                        found.push((
                            decode_metadata_key(object.location.as_ref())?,
                            object.last_modified,
                        ));
                    }
                    Ok::<_, ObjectStorageError>((children, found))
                })
                .buffer_unordered(16)
                .try_collect()
                .await?;
            for (children, found) in listings {
                pending.extend(children);
                keys.extend(found);
            }
        }
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(keys)
    }

    async fn metadata_hidden_prefix(
        &self,
        prefix: object_store::path::Path,
    ) -> Result<Vec<(String, chrono::DateTime<chrono::Utc>)>, ObjectStorageError> {
        let mut pending = vec![prefix];
        let mut keys = Vec::new();
        while let Some(prefix) = pending.pop() {
            let listing = self.list_with_delimiter(Some(prefix)).await?;
            pending.extend(listing.common_prefixes);
            keys.extend(
                listing
                    .objects
                    .into_iter()
                    .map(|o| {
                        decode_metadata_key(o.location.as_ref()).map(|key| (key, o.last_modified))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(keys)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<object_store::path::Path>,
    ) -> Result<ListResult, ObjectStorageError>;

    async fn create_stream(
        &self,
        stream_name: &str,
        meta: ObjectStoreFormat,
        schema: Arc<Schema>,
        tenant_id: &Option<String>,
    ) -> Result<String, ObjectStorageError> {
        let s: Schema = schema.as_ref().clone();
        PARSEABLE
            .metastore
            .put_schema(s.clone(), stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

        PARSEABLE
            .metastore
            .put_stream_json(&meta, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

        Ok(meta.created_at)
    }

    async fn update_time_partition_limit_in_stream(
        &self,
        stream_name: &str,
        time_partition_limit: NonZeroU32,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut format: ObjectStoreFormat = serde_json::from_slice(
            &PARSEABLE
                .metastore
                .get_stream_json(stream_name, false, tenant_id, false)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?,
        )?;
        format.time_partition_limit = Some(time_partition_limit.to_string());
        PARSEABLE
            .metastore
            .put_stream_json(&format, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

        Ok(())
    }

    async fn update_custom_partition_in_stream(
        &self,
        stream_name: &str,
        custom_partition: Option<&String>,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut format: ObjectStoreFormat = serde_json::from_slice(
            &PARSEABLE
                .metastore
                .get_stream_json(stream_name, false, tenant_id, false)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?,
        )?;
        format.custom_partition = custom_partition.cloned();
        PARSEABLE
            .metastore
            .put_stream_json(&format, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

        Ok(())
    }

    async fn update_log_source_in_stream(
        &self,
        stream_name: &str,
        log_source: &[LogSourceEntry],
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut format: ObjectStoreFormat = serde_json::from_slice(
            &PARSEABLE
                .metastore
                .get_stream_json(stream_name, false, tenant_id, false)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?,
        )?;
        format.log_source = log_source.to_owned();
        PARSEABLE
            .metastore
            .put_stream_json(&format, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

        Ok(())
    }

    async fn update_dataset_tags_and_labels_in_stream(
        &self,
        stream_name: &str,
        tags: &[DatasetTag],
        labels: &[String],
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut format: ObjectStoreFormat = serde_json::from_slice(
            &PARSEABLE
                .metastore
                .get_stream_json(stream_name, false, tenant_id, false)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?,
        )?;
        format.dataset_tags = tags.to_owned();
        format.dataset_labels = labels.to_owned();
        PARSEABLE
            .metastore
            .put_stream_json(&format, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

        Ok(())
    }

    /// Updates the first event timestamp in the object store for the specified stream.
    ///
    /// This function retrieves the current object-store format for the given stream,
    /// updates the `first_event_at` field with the provided timestamp, and then
    /// stores the updated format back in the object store.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The name of the stream to update.
    /// * `first_event` - The timestamp of the first event to set.
    ///
    /// # Returns
    ///
    /// * `Result<(), ObjectStorageError>` - Returns `Ok(())` if the update is successful,
    ///   or an `ObjectStorageError` if an error occurs.
    ///
    /// # Examples
    /// ```ignore
    /// ```rust
    /// let result = object_store.update_first_event_in_stream("my_stream", "2023-01-01T00:00:00Z").await;
    /// assert!(result.is_ok());
    /// ```
    async fn update_first_event_in_stream(
        &self,
        stream_name: &str,
        first_event: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut format: ObjectStoreFormat = serde_json::from_slice(
            &PARSEABLE
                .metastore
                .get_stream_json(stream_name, false, tenant_id, false)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?,
        )?;
        format.first_event_at = Some(first_event.to_string());
        PARSEABLE
            .metastore
            .put_stream_json(&format, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

        Ok(())
    }

    async fn put_stats(
        &self,
        stream_name: &str,
        stats: &FullStats,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut stream_metadata: ObjectStoreFormat = serde_json::from_slice(
            &PARSEABLE
                .metastore
                .get_stream_json(stream_name, false, tenant_id, false)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?,
        )?;

        stream_metadata.stats = *stats;

        Ok(PARSEABLE
            .metastore
            .put_stream_json(&stream_metadata, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?)
    }

    async fn put_retention(
        &self,
        stream_name: &str,
        retention: &Retention,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut stream_metadata: ObjectStoreFormat = serde_json::from_slice(
            &PARSEABLE
                .metastore
                .get_stream_json(stream_name, false, tenant_id, false)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?,
        )?;
        stream_metadata.retention = Some(retention.clone());

        Ok(PARSEABLE
            .metastore
            .put_stream_json(&stream_metadata, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?)
    }

    async fn upsert_stream_metadata(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<ObjectStoreFormat, ObjectStorageError> {
        let stream_metadata = match PARSEABLE
            .metastore
            .get_stream_json(stream_name, false, tenant_id, false)
            .await
        {
            Ok(data) => data,
            Err(_) => {
                // get the base stream metadata
                let bytes = PARSEABLE
                    .metastore
                    .get_stream_json(stream_name, true, tenant_id, false)
                    .await
                    .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

                let mut config = serde_json::from_slice::<ObjectStoreFormat>(&bytes)
                    .expect("parseable config is valid json");

                if PARSEABLE.options.mode == Mode::Ingest {
                    config.stats = FullStats::default();
                    config.snapshot.manifest_list = vec![];
                }

                PARSEABLE
                    .metastore
                    .put_stream_json(&config, stream_name, tenant_id)
                    .await
                    .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

                bytes
            }
        };

        Ok(serde_json::from_slice(&stream_metadata).expect("parseable config is valid json"))
    }

    async fn put_snapshot(
        &self,
        stream: &str,
        snapshot: Snapshot,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut stream_meta = self.upsert_stream_metadata(stream, tenant_id).await?;
        stream_meta.snapshot = snapshot;
        Ok(PARSEABLE
            .metastore
            .put_stream_json(&stream_meta, stream, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?)
    }

    ///create stream from querier stream.json from storage
    async fn create_stream_from_querier(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError> {
        if let Ok(querier_stream_json_bytes) = PARSEABLE
            .metastore
            .get_stream_json(stream_name, true, tenant_id, false)
            .await
        {
            let querier_stream_metadata =
                serde_json::from_slice::<ObjectStoreFormat>(&querier_stream_json_bytes)?;
            let stream_metadata = ObjectStoreFormat {
                stats: FullStats::default(),
                snapshot: Snapshot::default(),
                ..querier_stream_metadata
            };
            let stream_metadata_bytes: Bytes = serde_json::to_vec(&stream_metadata)?.into();
            PARSEABLE
                .metastore
                .put_stream_json(&stream_metadata, stream_name, tenant_id)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;
            return Ok(stream_metadata_bytes);
        }

        Ok(Bytes::new())
    }

    ///create stream from ingestor stream.json from storage
    async fn create_stream_from_ingestor(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError> {
        // create only when stream name not found in memory
        if PARSEABLE.get_stream(stream_name, tenant_id).is_ok() {
            let stream_metadata_bytes = PARSEABLE
                .metastore
                .get_stream_json(stream_name, false, tenant_id, false)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;
            return Ok(stream_metadata_bytes);
        }
        let mut all_log_sources: Vec<LogSourceEntry> = Vec::new();

        if let Some(stream_metadata_obs) = PARSEABLE
            .metastore
            .get_all_stream_jsons(stream_name, Some(Mode::Ingest), tenant_id, false)
            .await
            .into_iter()
            .next()
            && !stream_metadata_obs.is_empty()
        {
            for stream_metadata_bytes in stream_metadata_obs.iter() {
                let stream_ob_metadata =
                    serde_json::from_slice::<ObjectStoreFormat>(stream_metadata_bytes)?;
                all_log_sources.extend(stream_ob_metadata.log_source.clone());
            }

            // Merge log sources
            let mut merged_log_sources: Vec<LogSourceEntry> = Vec::new();
            let mut log_source_map: HashMap<LogSource, HashSet<String>> = HashMap::new();

            for log_source_entry in all_log_sources {
                let log_source_format = log_source_entry.log_source_format;
                let fields = log_source_entry.fields;

                log_source_map
                    .entry(log_source_format)
                    .or_default()
                    .extend(fields);
            }

            for (log_source_format, fields) in log_source_map {
                merged_log_sources.push(LogSourceEntry {
                    log_source_format,
                    fields: fields.into_iter().collect(),
                });
            }

            let stream_ob_metadata =
                serde_json::from_slice::<ObjectStoreFormat>(&stream_metadata_obs[0])?;
            let stream_metadata = ObjectStoreFormat {
                stats: FullStats::default(),
                snapshot: Snapshot::default(),
                log_source: merged_log_sources,
                dataset_tags: stream_metadata_obs
                    .iter()
                    .filter_map(|bytes| serde_json::from_slice::<ObjectStoreFormat>(bytes).ok())
                    .flat_map(|meta| meta.dataset_tags)
                    .unique()
                    .collect(),
                dataset_labels: stream_metadata_obs
                    .iter()
                    .filter_map(|bytes| serde_json::from_slice::<ObjectStoreFormat>(bytes).ok())
                    .flat_map(|meta| meta.dataset_labels)
                    .unique()
                    .collect(),
                ..stream_ob_metadata
            };

            let stream_metadata_bytes: Bytes = serde_json::to_vec(&stream_metadata)?.into();
            PARSEABLE
                .metastore
                .put_stream_json(&stream_metadata, stream_name, tenant_id)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

            return Ok(stream_metadata_bytes);
        }
        Ok(Bytes::new())
    }

    /// create schema from storage
    async fn create_schema_from_metastore(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError> {
        let schema = fetch_schema(stream_name, tenant_id).await?;
        let schema_bytes = Bytes::from(serde_json::to_vec(&schema)?);
        // convert to bytes
        PARSEABLE
            .metastore
            .put_schema(schema, stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;
        Ok(schema_bytes)
    }

    async fn get_log_source_from_storage(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<LogSourceEntry>, ObjectStorageError> {
        let mut all_log_sources: Vec<LogSourceEntry> = Vec::new();
        let stream_metas = PARSEABLE
            .metastore
            .get_all_stream_jsons(stream_name, None, tenant_id, false)
            .await;
        if let Ok(stream_metas) = stream_metas {
            for stream_meta in stream_metas.iter() {
                if let Ok(stream_meta) = serde_json::from_slice::<ObjectStoreFormat>(stream_meta) {
                    // fetch unique log sources and their fields
                    all_log_sources.extend(stream_meta.log_source.clone());
                }
            }
        }

        //merge fields of same log source
        let mut merged_log_sources: Vec<LogSourceEntry> = Vec::new();
        let mut log_source_map: HashMap<LogSource, HashSet<String>> = HashMap::new();
        for log_source_entry in all_log_sources {
            let log_source_format = log_source_entry.log_source_format;
            let fields = log_source_entry.fields;

            log_source_map
                .entry(log_source_format)
                .or_default()
                .extend(fields);
        }

        for (log_source_format, fields) in log_source_map {
            merged_log_sources.push(LogSourceEntry {
                log_source_format,
                fields: fields.into_iter().collect(),
            });
        }
        Ok(merged_log_sources)
    }

    /// Retrieves both the first and latest event timestamps from storage for the specified stream.
    /// Uses directory structure traversal instead of downloading manifest files for better performance.
    ///
    /// This optimized implementation avoids downloading potentially large manifest files by leveraging
    /// the hierarchical directory structure (date=YYYY-MM-DD/hour=HH/minute=MM/) to derive timestamps.
    /// It performs efficient list operations to find the min/max date, hour, and minute combinations,
    /// then constructs the actual timestamps from this directory information.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The name of the stream
    ///
    /// # Returns
    ///
    /// * `Result<(Option<String>, Option<String>), ObjectStorageError>` - Returns tuple of
    ///   (first_event_at, latest_event_at). Each can be None if no timestamps are found.
    async fn get_first_and_latest_event_from_storage(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<(Option<String>, Option<String>), ObjectStorageError> {
        // Get all available dates for the stream
        let dates = self.list_dates(stream_name, tenant_id).await?;
        if dates.is_empty() {
            return Ok((None, None));
        }

        // Parse and sort dates to find min and max
        let mut parsed_dates: Vec<_> = dates
            .iter()
            .filter_map(|date_str| {
                // Extract date from "date=YYYY-MM-DD" format
                if let Some(date_part) = date_str.strip_prefix("date=") {
                    chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
                        .ok()
                        .map(|date| (date, date_str))
                } else {
                    None
                }
            })
            .collect();

        if parsed_dates.is_empty() {
            return Ok((None, None));
        }

        parsed_dates.sort_by_key(|(date, _)| *date);
        let min_date = &parsed_dates[0].1;
        let max_date = &parsed_dates[parsed_dates.len() - 1].1;

        // Extract timestamps for min and max dates concurrently; independent reads, no shared state
        let (first_timestamp, latest_timestamp) = tokio::try_join!(
            self.extract_timestamp_for_date(stream_name, min_date, true, tenant_id),
            self.extract_timestamp_for_date(stream_name, max_date, false, tenant_id)
        )?;

        let first_event_at = first_timestamp.map(|ts| ts.to_rfc3339());
        let latest_event_at = latest_timestamp.map(|ts| ts.to_rfc3339());

        Ok((first_event_at, latest_event_at))
    }

    /// Extract timestamp for a specific date by traversing the hour/minute structure
    async fn extract_timestamp_for_date(
        &self,
        stream_name: &str,
        date: &str,
        find_min: bool,
        tenant_id: &Option<String>,
    ) -> Result<Option<DateTime<Utc>>, ObjectStorageError> {
        // Get all hours for this date
        let hours = self.list_hours(stream_name, date, tenant_id).await?;
        if hours.is_empty() {
            return Ok(None);
        }

        // Find min/max hour and corresponding string without collecting all values
        let (target_hour_value, target_hour_str) = hours
            .iter()
            .filter_map(|hour_str| {
                hour_str.strip_prefix("hour=").and_then(|hour_part| {
                    hour_part.parse::<u32>().ok().map(|hour| (hour, hour_str))
                })
            })
            .reduce(|acc, curr| {
                if find_min {
                    if curr.0 < acc.0 { curr } else { acc }
                } else if curr.0 > acc.0 {
                    curr
                } else {
                    acc
                }
            })
            .ok_or_else(|| ObjectStorageError::Custom("No valid hours found".to_string()))?;

        // Get all minutes for the target hour
        let minutes = self
            .list_minutes(stream_name, date, target_hour_str, tenant_id)
            .await?;
        if minutes.is_empty() {
            return Ok(None);
        }

        // Find min/max minute directly without collecting all values
        let target_minute = minutes
            .iter()
            .filter_map(|minute_str| {
                minute_str
                    .strip_prefix("minute=")
                    .and_then(|minute_part| minute_part.parse::<u32>().ok())
            })
            .reduce(|acc, curr| {
                if find_min {
                    if curr < acc { curr } else { acc }
                } else if curr > acc {
                    curr
                } else {
                    acc
                }
            })
            .ok_or_else(|| ObjectStorageError::Custom("No valid minutes found".to_string()))?;

        // Extract date components and construct timestamp
        if let Some(date_part) = date.strip_prefix("date=")
            && let Ok(parsed_date) = chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
        {
            // Create timestamp from date, hour, and minute with seconds hardcoded to 00
            let naive_datetime = parsed_date
                .and_hms_opt(target_hour_value, target_minute, 0)
                .unwrap_or_else(|| parsed_date.and_hms_opt(0, 0, 0).unwrap());

            return Ok(Some(DateTime::from_naive_utc_and_offset(
                naive_datetime,
                Utc,
            )));
        }

        Ok(None)
    }

    // pick a better name
    fn get_bucket_name(&self) -> String;

    #[tracing::instrument(
        name = "object_store.upload_files_from_staging",
        skip(self),
        fields(stream = %stream_name, tenant = ?tenant_id),
        err
    )]
    async fn upload_files_from_staging(
        &self,
        stream_name: &str,
        tenant_id: Option<String>,
    ) -> Result<(), ObjectStorageError> {
        if !PARSEABLE.options.staging_dir().exists() {
            return Ok(());
        }

        info!("Starting object_store_sync for stream- {stream_name}");

        let stream = PARSEABLE.get_or_create_stream(stream_name, &tenant_id);
        let upload_context = UploadContext::new(stream);
        if PARSEABLE.options.postgres.enabled()
            && PARSEABLE
                .metastore
                .is_stream_deleting(stream_name, &tenant_id)
                .await
                .map_err(|e| ObjectStorageError::Custom(e.to_string()))?
        {
            for path in upload_context.stream.parquet_files() {
                let filename = path.file_name().unwrap().to_string_lossy();
                let key = stream_relative_path(
                    stream_name,
                    &filename,
                    &upload_context.custom_partition,
                    &tenant_id,
                );
                compensate_uploaded_parquet(self, &key, &tenant_id).await?;
                remove_uploaded_staged_file(path);
            }
            for path in upload_context.stream.schema_files() {
                remove_uploaded_staged_file(path);
            }
            return Ok(());
        }

        // Process parquet files concurrently and collect results
        let uploaded_files =
            process_parquet_files(&upload_context, stream_name, tenant_id.clone()).await?;

        // Update snapshot with collected manifest files
        if let Err(e) =
            update_snapshot_with_manifests(stream_name, &uploaded_files, &tenant_id).await
        {
            if PARSEABLE.options.postgres.enabled()
                && e.http_status() == actix_web::http::StatusCode::CONFLICT
            {
                // The marker can land between upload and manifest publication.
                // Compensate every upload in this batch, including partially
                // published manifests, before retiring its local staging files.
                for file in &uploaded_files {
                    let filename = file.file_path.file_name().unwrap().to_string_lossy();
                    let key = stream_relative_path(
                        stream_name,
                        &filename,
                        &upload_context.custom_partition,
                        &tenant_id,
                    );
                    match compensate_uploaded_parquet(self, &key, &tenant_id).await {
                        Ok(()) | Err(ObjectStorageError::NoSuchKey(_)) => {}
                        Err(cleanup) => {
                            for pending in &uploaded_files {
                                ACTIVE_OBJECT_STORE_SYNC_FILES.remove(&pending.file_path);
                            }
                            return Err(cleanup);
                        }
                    }
                }
                cleanup_uploaded_staged_files(uploaded_files).await;
                for path in upload_context.stream.schema_files() {
                    remove_uploaded_staged_file(path);
                }
                return Ok(());
            }
            return Err(e);
        }

        // Calculate stats after snapshot update so uploads are visible before stats work starts.
        calculate_stats_for_uploaded_files(
            &uploaded_files,
            stream_name,
            &upload_context,
            &tenant_id,
        )
        .await;

        cleanup_uploaded_staged_files(uploaded_files).await;

        // Process schema files
        process_schema_files(&upload_context, stream_name, &tenant_id).await?;

        Ok(())
    }
}

// Data-only compensation for an upload rejected by the metastore deletion guard.
async fn compensate_uploaded_parquet(
    storage: &(impl ObjectStorage + ?Sized),
    key: &str,
    tenant: &Option<String>,
) -> Result<(), ObjectStorageError> {
    if !key.ends_with(".parquet") {
        return Err(ObjectStorageError::Custom(
            "upload compensation requires parquet key".into(),
        ));
    }
    match storage.delete_object(RelativePath::new(key), tenant).await {
        Ok(()) | Err(ObjectStorageError::NoSuchKey(_)) => Ok(()),
        Err(ObjectStorageError::IoError(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

/// Processes parquet files concurrently and returns stats status and manifest files
#[tracing::instrument(
    name = "object_store.process_parquet_files",
    skip(upload_context),
    fields(
        stream = %stream_name,
        tenant = ?tenant_id,
        parquet_count = tracing::field::Empty,
        total_size_bytes = tracing::field::Empty,
        smallest_date = tracing::field::Empty,
        largest_date = tracing::field::Empty,
    )
)]
async fn process_parquet_files(
    upload_context: &UploadContext,
    stream_name: &str,
    tenant_id: Option<String>,
) -> Result<Vec<UploadedParquetFile>, ObjectStorageError> {
    retry_pending_staged_file_cleanup();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(100));
    let mut join_set = JoinSet::new();
    let object_store = PARSEABLE.storage().get_object_store();

    // collect all parquet files to upload
    let parquet_paths: Vec<PathBuf> = upload_context
        .stream
        .parquet_files()
        .into_par_iter()
        .filter_map(claim_staged_file_for_upload)
        .collect();

    let postgres = PARSEABLE.options.postgres.enabled();
    let retirement_paths = if postgres {
        parquet_paths.clone()
    } else {
        Vec::new()
    };

    let mut total_size: u64 = 0;
    let mut min_dt: Option<chrono::DateTime<Utc>> = None;
    let mut max_dt: Option<chrono::DateTime<Utc>> = None;
    for path in &parquet_paths {
        if let Ok(meta) = path.metadata() {
            total_size = total_size.saturating_add(meta.len());
        }
        if let Ok(dt) = extract_datetime_from_parquet_path_regex(path) {
            min_dt = Some(min_dt.map_or(dt, |cur| cur.min(dt)));
            max_dt = Some(max_dt.map_or(dt, |cur| cur.max(dt)));
        }
    }
    let span = tracing::Span::current();
    span.record("parquet_count", parquet_paths.len());
    span.record("total_size_bytes", total_size);
    if let Some(dt) = min_dt {
        span.record("smallest_date", tracing::field::display(dt));
    }
    if let Some(dt) = max_dt {
        span.record("largest_date", tracing::field::display(dt));
    }
    // Spawn upload tasks for each parquet file
    for path in parquet_paths {
        spawn_parquet_upload_task(
            &mut join_set,
            semaphore.clone(),
            object_store.clone(),
            upload_context,
            stream_name,
            path,
            tenant_id.clone(),
        )
        .await;
    }

    // Collect results from all upload tasks
    let result = collect_upload_results(join_set, postgres).await;
    if result.is_err() && postgres {
        // All tasks have settled, so no successful sibling upload can publish
        // after compensation. Keep staging retryable if deletion is unconfirmed.
        for path in &retirement_paths {
            ACTIVE_OBJECT_STORE_SYNC_FILES.remove(path);
        }
        if PARSEABLE
            .metastore
            .is_stream_deleting(stream_name, &tenant_id)
            .await
            .is_ok_and(|pending| pending)
        {
            for path in retirement_paths {
                let filename = path.file_name().unwrap().to_string_lossy();
                let key = stream_relative_path(
                    stream_name,
                    &filename,
                    &upload_context.custom_partition,
                    &tenant_id,
                );
                compensate_uploaded_parquet(object_store.as_ref(), &key, &tenant_id).await?;
                remove_uploaded_staged_file(path);
            }
            for path in upload_context.stream.schema_files() {
                remove_uploaded_staged_file(path);
            }
            return Ok(Vec::new());
        }
    }
    result
}

/// Spawns an individual parquet file upload task
async fn spawn_parquet_upload_task(
    join_set: &mut JoinSet<Result<UploadResult, (PathBuf, ObjectStorageError)>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    store: Arc<dyn ObjectStorage>,
    upload_context: &UploadContext,
    stream_name: &str,
    path: std::path::PathBuf,
    tenant_id: Option<String>,
) {
    let filename = path
        .file_name()
        .expect("only parquet files are returned by iterator")
        .to_str()
        .expect("filename is valid string");

    let stream_relative_path = stream_relative_path(
        stream_name,
        filename,
        &upload_context.custom_partition,
        &tenant_id,
    );

    let stream_name = stream_name.to_string();
    let handle = FLUSH_AND_CONVERT_RUNTIME.handle();
    join_set.spawn_on(
        async move {
            let _permit = semaphore.acquire().await.expect("semaphore is not closed");

            upload_single_parquet_file(store, path, stream_relative_path, stream_name, tenant_id)
                .await
        },
        handle,
    );
}

/// Collects results from all upload tasks
async fn collect_upload_results(
    mut join_set: JoinSet<Result<UploadResult, (PathBuf, ObjectStorageError)>>,
    drain_on_error: bool,
) -> Result<Vec<UploadedParquetFile>, ObjectStorageError> {
    let mut uploaded_files = Vec::new();
    let mut failure = None;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(upload_result)) => {
                if let Some(manifest_file) = upload_result.manifest_file {
                    uploaded_files.push(UploadedParquetFile {
                        file_path: upload_result.file_path,
                        manifest_file,
                    });
                } else {
                    // File failed in upload size validation, preserve staging file for retry
                    error!(
                        "Parquet file upload size validation failed for {:?}, preserving in staging for retry",
                        upload_result.file_path
                    );
                    ACTIVE_OBJECT_STORE_SYNC_FILES.remove(&upload_result.file_path);
                }
            }
            Ok(Err((path, e))) => {
                error!("Error uploading parquet file: {e}");
                ACTIVE_OBJECT_STORE_SYNC_FILES.remove(&path);
                if !drain_on_error {
                    return Err(e);
                }
                if failure.is_none() {
                    failure = Some(e);
                }
            }
            Err(e) => {
                error!("Task panicked: {e}");
                let error = ObjectStorageError::UnhandledError(Box::new(e));
                if !drain_on_error {
                    return Err(error);
                }
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
    }

    match failure {
        Some(error) => Err(error),
        None => Ok(uploaded_files),
    }
}

/// Updates snapshot with collected manifest files
async fn update_snapshot_with_manifests(
    stream_name: &str,
    uploaded_files: &[UploadedParquetFile],
    tenant_id: &Option<String>,
) -> Result<(), ObjectStorageError> {
    let manifest_files: Vec<_> = uploaded_files
        .iter()
        .map(|uploaded_file| uploaded_file.manifest_file.clone())
        .collect();

    if !manifest_files.is_empty() {
        catalog::update_snapshot(stream_name, manifest_files, tenant_id).await?;
    }
    Ok(())
}

/// Bounds concurrent post-upload field stats tasks across streams so burst flushes
/// cannot launch unbounded simultaneous 32K-row Parquet decoders and spike RSS.
static FIELD_STATS_CONCURRENCY_SEMAPHORE: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| {
        let permits = std::thread::available_parallelism()
            .map(|n| (n.get() / 2).clamp(2, 8))
            .unwrap_or(4);
        Arc::new(tokio::sync::Semaphore::new(permits))
    });

async fn calculate_stats_for_uploaded_files(
    uploaded_files: &[UploadedParquetFile],
    stream_name: &str,
    upload_context: &UploadContext,
    tenant_id: &Option<String>,
) {
    if !PARSEABLE.options.collect_dataset_stats {
        return;
    }

    let mut join_set = JoinSet::new();
    let handle = FLUSH_AND_CONVERT_RUNTIME.handle();

    for uploaded_file in uploaded_files {
        let stream_name = stream_name.to_string();
        let path = uploaded_file.file_path.clone();
        let schema = upload_context.schema.clone();
        let tenant_id = tenant_id.clone();
        let semaphore = FIELD_STATS_CONCURRENCY_SEMAPHORE.clone();

        join_set.spawn_on(
            async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .expect("field stats concurrency semaphore is never closed");
                calculate_stats_if_enabled(&stream_name, &path, &schema, tenant_id).await;
            },
            handle,
        );
    }

    while let Some(result) = join_set.join_next().await {
        if let Err(err) = result {
            warn!("Field stats task join failed after parquet upload: {err}");
        }
    }
}

/// Removes a file after a successful upload. A failed deletion moves the path
/// from the expiring active-upload claims into the non-expiring cleanup-pending
/// set, keeping it out of subsequent upload selections.
fn remove_uploaded_staged_file(path: PathBuf) {
    catalog::manifest::evict_parquet_metadata(&path);
    crate::storage::field_stats::evict_staged_field_stats(&path);
    match remove_file(&path) {
        Ok(()) => {
            UPLOADED_STAGED_FILES_PENDING_CLEANUP.remove(&path);
            ACTIVE_OBJECT_STORE_SYNC_FILES.remove(&path);
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            UPLOADED_STAGED_FILES_PENDING_CLEANUP.remove(&path);
            ACTIVE_OBJECT_STORE_SYNC_FILES.remove(&path);
        }
        Err(err) => {
            // Insert into the non-expiring set before releasing the active
            // claim, so upload selection never sees this path as unclaimed.
            UPLOADED_STAGED_FILES_PENDING_CLEANUP.insert(path.clone());
            ACTIVE_OBJECT_STORE_SYNC_FILES.remove(&path);
            warn!(
                "Failed to remove uploaded staged file {}; retaining it for deletion retry: {err}",
                path.display()
            );
        }
    }
}

/// Retries local deletion for files whose remote upload already succeeded.
fn retry_pending_staged_file_cleanup() {
    UPLOADED_STAGED_FILES_PENDING_CLEANUP
        .iter()
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>()
        .into_par_iter()
        .for_each(remove_uploaded_staged_file);
}

async fn cleanup_uploaded_staged_files(uploaded_files: Vec<UploadedParquetFile>) {
    uploaded_files
        .into_par_iter()
        .map(|uploaded_parquet_file| uploaded_parquet_file.file_path)
        .for_each(remove_uploaded_staged_file);

    // Use monotonic time so cleanup is immune to system clock adjustments.
    let now = Instant::now();
    ACTIVE_OBJECT_STORE_SYNC_FILES.retain(|_, tracked_instant| {
        now.duration_since(*tracked_instant) < Duration::from_secs(300)
    });
}

/// Processes schema files
async fn process_schema_files(
    upload_context: &UploadContext,
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<(), ObjectStorageError> {
    process_schema_files_with(&upload_context.stream, |schema| {
        commit_schema_to_storage(stream_name, schema, tenant_id)
    })
    .await
}

async fn process_schema_files_with<F, Fut>(
    stream: &Stream,
    commit: F,
) -> Result<(), ObjectStorageError>
where
    F: Fn(Schema) -> Fut,
    Fut: std::future::Future<Output = Result<(), ObjectStorageError>>,
{
    for path in stream.schema_files() {
        let file = File::open(&path)?;
        let schema: Schema = serde_json::from_reader(file)?;
        commit(schema).await?;

        if let Err(e) = remove_file(path) {
            warn!("Failed to remove staged file: {e}");
        }
    }
    Ok(())
}

/// Builds the stream relative path for a file
pub(crate) fn stream_relative_path(
    stream_name: &str,
    filename: &str,
    custom_partition: &Option<String>,
    tenant_id: &Option<String>,
) -> String {
    let mut file_suffix = str::replacen(filename, ".", "/", 3);

    if let Some(custom_partition_fields) = custom_partition {
        let custom_partition_list = custom_partition_fields.split(',').collect::<Vec<&str>>();
        file_suffix = str::replacen(filename, ".", "/", 3 + custom_partition_list.len());
    }
    if let Some(tenant) = tenant_id
        && !tenant.eq(DEFAULT_TENANT)
    {
        format!("{tenant}/{stream_name}/{file_suffix}")
    } else {
        format!("{stream_name}/{file_suffix}")
    }
}

/// Builds the path of a stream's parquet data directory, or of a partition
/// directory inside it such as `date=…` or `hour=…`. A tenant other than the
/// default keeps its data under `{tenant}/`, as [`stream_relative_path`]
/// uploads it. Stream metadata and manifests use their own path helpers.
pub(crate) fn stream_data_path(
    stream_name: &str,
    partitions: &[&str],
    tenant_id: &Option<String>,
) -> RelativePathBuf {
    let tenant = tenant_id
        .as_deref()
        .filter(|tenant| *tenant != DEFAULT_TENANT);
    RelativePathBuf::from_iter(
        tenant
            .into_iter()
            .chain([stream_name])
            .chain(partitions.iter().copied()),
    )
}

/// The listing prefix for the children of [`stream_data_path`]: the object
/// store path to list, and its encoded form with a trailing `/`. Object store
/// paths percent-encode characters such as non-ASCII letters, so listed
/// children must be stripped of the encoded prefix, not of the raw path.
pub(crate) fn stream_data_listing_prefix(
    stream_name: &str,
    partitions: &[&str],
    tenant_id: &Option<String>,
) -> (object_store::path::Path, String) {
    let path = object_store::path::Path::from(
        stream_data_path(stream_name, partitions, tenant_id).as_str(),
    );
    let prefix = format!("{path}/");
    (path, prefix)
}

/// Single-pass check for whether a stream's staging directory contains any
/// `.parquet` or `.schema` files without opening Parquet footers or scanning
/// the directory twice.
fn has_staged_upload_files(data_path: &Path) -> bool {
    let Ok(dir) = std::fs::read_dir(data_path) else {
        return false;
    };
    for entry in dir.flatten() {
        if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
            continue;
        }
        let file_name = entry.file_name();
        if Path::new(&file_name)
            .extension()
            .is_some_and(|ext| ext == "parquet")
            || file_name
                .to_str()
                .is_some_and(|name| name.ends_with(".schema"))
        {
            return true;
        }
    }
    false
}

pub fn sync_all_streams(joinset: &mut JoinSet<Result<(), ObjectStorageError>>) {
    let object_store = PARSEABLE.storage().get_object_store();
    let tenants = if let Some(tenants) = PARSEABLE.list_tenants() {
        tenants.into_iter().map(Some).collect()
    } else {
        vec![None]
    };
    let handle = FLUSH_AND_CONVERT_RUNTIME.handle();
    for tenant_id in tenants {
        for stream_name in PARSEABLE.streams.list(&tenant_id) {
            if let Ok(stream) = PARSEABLE.get_stream(&stream_name, &tenant_id)
                && !has_staged_upload_files(&stream.data_path)
            {
                continue;
            }
            let object_store = object_store.clone();
            let id = tenant_id.clone();
            let span = info_span!("stream_upload", stream_name = %stream_name);
            joinset.spawn_on(
                async move {
                    let start = Instant::now();
                    let result = object_store
                        .upload_files_from_staging(&stream_name, id)
                        .await;
                    if let Err(ref e) = result {
                        error!("Failed to upload files from staging for stream {stream_name}: {e}");
                    } else {
                        info!(
                            "Completed object_store_sync for stream- {stream_name} in {} ms",
                            start.elapsed().as_millis()
                        );
                    }
                    result
                }
                .instrument(span),
                handle,
            );
        }
    }
}

pub async fn commit_schema_to_storage(
    stream_name: &str,
    schema: Schema,
    tenant_id: &Option<String>,
) -> Result<(), ObjectStorageError> {
    let stream_schema = PARSEABLE
        .metastore
        .get_schema(stream_name, tenant_id)
        .await
        .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;

    let new_schema = Schema::try_merge(vec![
        schema,
        serde_json::from_slice::<Schema>(&stream_schema)?,
    ])
    .map_err(|e| ObjectStorageError::Custom(e.to_string()))?;

    let stream = PARSEABLE.get_stream(stream_name, tenant_id).ok();
    persist_schema_and_update_memory(stream.as_deref(), &new_schema, async {
        PARSEABLE
            .metastore
            .put_schema(new_schema.clone(), stream_name, tenant_id)
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))
    })
    .await
}

/// Update memory only after a successful upload, including schemas recovered
/// from staging after a crash. A failed upload leaves the staged file
/// retryable. A failed memory merge does not: the schema is already stored,
/// and a conflict would fail identically on every retry, so it is logged like
/// the per-query merge this replaces.
async fn persist_schema_and_update_memory(
    stream: Option<&Stream>,
    schema: &Schema,
    persist: impl std::future::Future<Output = Result<(), ObjectStorageError>>,
) -> Result<(), ObjectStorageError> {
    persist.await?;
    if let Some(stream) = stream
        && let Err(e) = crate::event::commit_schema_to_metadata(&stream.metadata, schema)
    {
        warn!(
            stream = stream.stream_name,
            "Stored staged schema but could not merge it into memory: {e}"
        );
    }
    Ok(())
}

#[inline(always)]
pub fn to_bytes(any: &(impl ?Sized + serde::Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}

fn has_stream_metadata(objects: &[ObjectMeta]) -> bool {
    objects.iter().any(|object| {
        object
            .location
            .filename()
            .is_some_and(|name| name.ends_with("stream.json"))
    })
}

/// Count a stream-metadata listing in the per-tenant usage counters, as the
/// `get_objects` listing it replaces on the query path did.
pub(crate) fn record_metadata_listing(files_scanned: usize, tenant_id: &Option<String>) {
    let date = Utc::now().date_naive().to_string();
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    crate::metrics::increment_files_scanned_in_object_store_calls_by_date(
        "LIST",
        files_scanned as u64,
        &date,
        tenant,
    );
    crate::metrics::increment_object_store_calls_by_date("LIST", &date, tenant);
}

fn stream_metadata_prefix(
    stream_name: &str,
    tenant_id: &Option<String>,
) -> object_store::path::Path {
    object_store::path::Path::from_iter([
        tenant_id
            .as_deref()
            .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
            .unwrap_or(""),
        stream_name,
        STREAM_ROOT_DIRECTORY,
    ])
}

pub fn schema_path(stream_name: &str, tenant_id: &Option<String>) -> RelativePathBuf {
    let tenant = tenant_id
        .as_deref()
        .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
        .unwrap_or("");
    if PARSEABLE.options.mode == Mode::Ingest {
        let id = INGESTOR_META
            .get()
            .unwrap_or_else(|| panic!("{}", INGESTOR_EXPECT))
            .get_node_id();
        let file_name = format!(".ingestor.{id}{SCHEMA_FILE_NAME}");

        RelativePathBuf::from_iter([tenant, stream_name, STREAM_ROOT_DIRECTORY, &file_name])
    } else {
        RelativePathBuf::from_iter([tenant, stream_name, STREAM_ROOT_DIRECTORY, SCHEMA_FILE_NAME])
    }
}

#[inline(always)]
pub fn stream_json_path(stream_name: &str, tenant_id: &Option<String>) -> RelativePathBuf {
    let tenant = tenant_id
        .as_deref()
        .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
        .unwrap_or("");
    if PARSEABLE.options.mode == Mode::Ingest {
        let id = INGESTOR_META
            .get()
            .unwrap_or_else(|| panic!("{}", INGESTOR_EXPECT))
            .get_node_id();
        let file_name = format!(".ingestor.{id}{STREAM_METADATA_FILE_NAME}",);
        RelativePathBuf::from_iter([tenant, stream_name, STREAM_ROOT_DIRECTORY, &file_name])
    } else {
        RelativePathBuf::from_iter([
            tenant,
            stream_name,
            STREAM_ROOT_DIRECTORY,
            STREAM_METADATA_FILE_NAME,
        ])
    }
}

/// Path to a stream's deletion marker. Deliberately lives under
/// `TOMBSTONE_ROOT_DIRECTORY`, outside the `{tenant}/{stream_name}` prefix
/// that a bulk stream delete walks, so a mid-deletion crash can never lose
/// the marker before the deletion it records has actually finished.
///
/// The marker is nested one level under `{stream_name}/`, not stored as a
/// bare key named after the stream: `list_dirs_relative` (used to discover
/// tombstoned streams on restart) only surfaces child directories on every
/// backend, so `{stream_name}` must itself resolve to a directory for that
/// scan to find it.
#[inline(always)]
pub fn tombstone_path(stream_name: &str, tenant_id: &Option<String>) -> RelativePathBuf {
    let tenant = tenant_id
        .as_deref()
        .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
        .unwrap_or("");
    RelativePathBuf::from_iter([
        TOMBSTONE_ROOT_DIRECTORY,
        tenant,
        stream_name,
        TOMBSTONE_MARKER_FILE_NAME,
    ])
}

/// Whether a stream has a deletion marker present, i.e. a deletion was
/// started (possibly by a node that has since crashed or restarted) and has
/// not yet completed.
pub async fn is_tombstoned(
    storage: &(impl ObjectStorage + ?Sized),
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<bool, ObjectStorageError> {
    match storage
        .head(&tombstone_path(stream_name, tenant_id), tenant_id)
        .await
    {
        Ok(_) => Ok(true),
        // NoSuchKey is the object-store backends' not-found; LocalFS instead
        // surfaces a plain io::Error, so both must be treated as "absent"
        // here (see ObjectStoreMetastore::is_missing_optional_dir for the
        // same not-found reconciliation across backends).
        Err(ObjectStorageError::NoSuchKey(_)) => Ok(false),
        Err(ObjectStorageError::IoError(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// Stream names with a deletion marker for the given tenant, discovered by
/// listing `TOMBSTONE_ROOT_DIRECTORY` rather than checking one name at a
/// time. Used to resume deletions left unfinished by a crashed or restarted
/// node, since a tombstoned stream whose `.stream.json` is already gone
/// would otherwise never surface via `list_streams`. See `is_tombstoned` for
/// the equivalent single-name check.
///
/// `list_dirs_relative` only proves a directory exists under the tombstone
/// root, not that it actually holds a `.tombstone` marker (e.g. a partial or
/// interrupted write could leave an empty one behind) -- each candidate is
/// re-checked with `is_tombstoned` before being returned, so a directory
/// without a real marker is silently skipped rather than misreported.
pub async fn list_tombstoned_streams(
    storage: &(impl ObjectStorage + ?Sized),
    tenant_id: &Option<String>,
) -> Result<Vec<String>, ObjectStorageError> {
    let tenant = tenant_id
        .as_deref()
        .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
        .unwrap_or("");
    let root = RelativePathBuf::from_iter([TOMBSTONE_ROOT_DIRECTORY, tenant]);
    let candidates = storage.list_dirs_relative(&root, tenant_id).await?;

    let mut confirmed = Vec::with_capacity(candidates.len());
    for stream_name in candidates {
        if is_tombstoned(storage, &stream_name, tenant_id).await? {
            confirmed.push(stream_name);
        }
    }
    Ok(confirmed)
}

/// if filter_id is an empty str it should not append it to the rel path
#[inline(always)]
pub fn filter_path(
    user_id: &str,
    stream_name: &str,
    filter_file_name: &str,
    tenant_id: &Option<String>,
) -> RelativePathBuf {
    let root = tenant_id
        .as_deref()
        .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
        .unwrap_or("");
    RelativePathBuf::from_iter([
        root,
        USERS_ROOT_DIR,
        user_id,
        FILTER_DIR,
        stream_name,
        filter_file_name,
    ])
}

/// path will be ".parseable/.parsable.json"
#[inline(always)]
pub fn parseable_json_path() -> RelativePathBuf {
    RelativePathBuf::from_iter([PARSEABLE_ROOT_DIRECTORY, PARSEABLE_METADATA_FILE_NAME])
}

/// TODO: Needs to be updated for distributed mode
#[inline(always)]
pub fn alert_json_path(alert_id: Ulid, tenant_id: &Option<String>) -> RelativePathBuf {
    if let Some(tenant_id) = tenant_id
        .as_ref()
        .filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT)
    {
        RelativePathBuf::from_iter([
            tenant_id,
            ALERTS_ROOT_DIRECTORY,
            &format!("{alert_id}.json"),
        ])
    } else {
        RelativePathBuf::from_iter([ALERTS_ROOT_DIRECTORY, &format!("{alert_id}.json")])
    }
}

/// TODO: Needs to be updated for distributed mode
#[inline(always)]
pub fn target_json_path(target_id: &Ulid, tenant_id: &Option<String>) -> RelativePathBuf {
    let root = tenant_id
        .as_deref()
        .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
        .unwrap_or("");
    RelativePathBuf::from_iter([
        root,
        SETTINGS_ROOT_DIRECTORY,
        TARGETS_ROOT_DIRECTORY,
        &format!("{target_id}.json"),
    ])
}

/// Constructs the path for storing outbound HTTP policy JSON files
/// Format: "<tenant>/settings/outbound_http_policy.json"
#[inline(always)]
pub fn outbound_http_policy_json_path(tenant_id: &Option<String>) -> RelativePathBuf {
    let root = tenant_id
        .as_deref()
        .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
        .unwrap_or("");
    RelativePathBuf::from_iter([root, SETTINGS_ROOT_DIRECTORY, "outbound_http_policy.json"])
}

/// Constructs the path for storing alert state JSON files
/// Format: ".alerts/alert_state_{alert_id}.json"
#[inline(always)]
pub fn alert_state_json_path(alert_id: Ulid, tenant_id: &Option<String>) -> RelativePathBuf {
    let root = tenant_id
        .as_deref()
        .filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT)
        .unwrap_or("");
    RelativePathBuf::from_iter([
        root,
        ALERTS_ROOT_DIRECTORY,
        &format!("alert_state_{alert_id}.json"),
    ])
}

/// Constructs the path for storing MTTR history JSON file
/// Format: ".alerts/mttr.json"
#[inline(always)]
pub fn mttr_json_path(tenant_id: &Option<String>) -> RelativePathBuf {
    if let Some(tenant) = tenant_id
        .as_ref()
        .filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT)
    {
        RelativePathBuf::from_iter([tenant, ALERTS_ROOT_DIRECTORY, "mttr.json"])
    } else {
        RelativePathBuf::from_iter([ALERTS_ROOT_DIRECTORY, "mttr.json"])
    }
}

#[inline(always)]
pub fn manifest_path(prefix: &str) -> RelativePathBuf {
    let hostname = hostname::get()
        .unwrap_or_else(|_| std::ffi::OsString::from(&Ulid::new().to_string()))
        .into_string()
        .unwrap_or_else(|_| Ulid::new().to_string())
        .matches(|c: char| c.is_alphanumeric() || c == '-' || c == '_')
        .collect::<String>();

    if PARSEABLE.options.mode == Mode::Ingest {
        let id = INGESTOR_META
            .get()
            .unwrap_or_else(|| panic!("{}", INGESTOR_EXPECT))
            .get_node_id();

        let manifest_file_name = format!("ingestor.{hostname}.{id}.{MANIFEST_FILE}");
        RelativePathBuf::from_iter([prefix, &manifest_file_name])
    } else {
        let manifest_file_name = format!("{hostname}.{MANIFEST_FILE}");
        RelativePathBuf::from_iter([prefix, &manifest_file_name])
    }
}

/// This node's own manifest file name (the final path segment), e.g. `host.manifest.json`
/// or `ingestor.host.id.manifest.json`. This is the identity used to decide manifest
/// ownership; compute it once and pass it to [`manifest_segment_matches`].
#[inline]
pub fn own_manifest_file_name() -> String {
    let own = manifest_path("").to_string();
    own.rsplit('/').next().unwrap_or_else(|| &own).to_string()
}

/// Whether a full manifest object path belongs to the writer identified by `file_name`.
///
/// Compares the FINAL path segment for exact equality. A substring (`contains`) or suffix
/// (`ends_with`) test would let one host claim another whose name is a suffix of its own —
/// e.g. `node.manifest.json` is contained in, and a suffix of, `prod-node.manifest.json` —
/// silently corrupting the wrong writer's manifest (issue #1739).
#[inline]
pub fn manifest_segment_matches(manifest_path_str: &str, file_name: &str) -> bool {
    manifest_path_str.rsplit('/').next() == Some(file_name)
}

#[cfg(test)]
mod stream_data_path_tests {
    use super::{stream_data_listing_prefix, stream_data_path, stream_relative_path};
    use crate::parseable::DEFAULT_TENANT;

    #[test]
    fn default_tenant_data_is_at_the_root() {
        // `Some("")` is what some metastore callers pass for the default tenant.
        for tenant in [None, Some(DEFAULT_TENANT.to_owned()), Some(String::new())] {
            assert_eq!(stream_data_path("app", &[], &tenant).as_str(), "app");
            assert_eq!(
                stream_data_path("app", &["date=2026-01-02"], &tenant).as_str(),
                "app/date=2026-01-02"
            );
        }
    }

    #[test]
    fn other_tenants_data_is_under_the_tenant() {
        let tenant = Some("acme".to_owned());
        assert_eq!(stream_data_path("app", &[], &tenant).as_str(), "acme/app");
        assert_eq!(
            stream_data_path("app", &["date=2026-01-02", "hour=03"], &tenant).as_str(),
            "acme/app/date=2026-01-02/hour=03"
        );
    }

    #[test]
    fn listed_children_strip_to_their_names_for_any_stream_name() {
        // Uploads and listings go through `object_store::path::Path`, which
        // percent-encodes non-ASCII stream names such as `café`.
        for (stream, tenant, stored) in [
            ("app", None, "app/"),
            ("app", Some("acme".to_owned()), "acme/app/"),
            ("café", None, "caf%C3%A9/"),
            ("café", Some("acme".to_owned()), "acme/caf%C3%A9/"),
        ] {
            let (path, prefix) = stream_data_listing_prefix(stream, &[], &tenant);
            assert_eq!(prefix, stored);
            assert_eq!(format!("{path}/"), stored);
            let uploaded = stream_relative_path(
                stream,
                "date=2026-01-02.hour=03.minute=04.host.data.parquet",
                &None,
                &tenant,
            );
            // A delimited listing returns the directory under the prefix.
            let listed = object_store::path::Path::from(uploaded.as_str())
                .parts()
                .take(path.parts().count() + 1)
                .collect::<object_store::path::Path>();
            assert_eq!(
                listed.as_ref().strip_prefix(&prefix),
                Some("date=2026-01-02"),
                "{listed} under {prefix}"
            );
        }
    }

    #[test]
    fn matches_where_parquet_files_are_uploaded() {
        let filename = "date=2026-01-02.hour=03.minute=04.host.data.parquet";
        for tenant in [
            None,
            Some(DEFAULT_TENANT.to_owned()),
            Some("acme".to_owned()),
        ] {
            let file = stream_relative_path("app", filename, &None, &tenant);
            let date_dir = stream_data_path("app", &["date=2026-01-02"], &tenant);
            assert!(
                file.starts_with(&format!("{date_dir}/")),
                "{file} is not under {date_dir}"
            );
        }
    }
}

#[cfg(test)]
mod staged_file_cleanup_tests {
    use super::{
        UPLOADED_STAGED_FILES_PENDING_CLEANUP, claim_staged_file_for_upload,
        remove_uploaded_staged_file, retry_pending_staged_file_cleanup,
    };
    use crate::sync::ACTIVE_OBJECT_STORE_SYNC_FILES;
    use std::{fs, time::Instant};
    use temp_dir::TempDir;

    #[test]
    fn deletion_failure_keeps_uploaded_file_out_of_upload_selection() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("uploaded.parquet");

        // A directory cannot be removed with remove_file, providing a
        // cross-platform deletion failure without changing permissions.
        fs::create_dir_all(&path).unwrap();
        ACTIVE_OBJECT_STORE_SYNC_FILES.insert(path.clone(), Instant::now());

        remove_uploaded_staged_file(path.clone());

        assert!(UPLOADED_STAGED_FILES_PENDING_CLEANUP.contains(&path));
        assert!(!ACTIVE_OBJECT_STORE_SYNC_FILES.contains_key(&path));
        assert!(claim_staged_file_for_upload(path.clone()).is_none());

        // Once deletion becomes possible, the retry clears the pending entry.
        fs::remove_dir(&path).unwrap();
        retry_pending_staged_file_cleanup();
        assert!(!UPLOADED_STAGED_FILES_PENDING_CLEANUP.contains(&path));
    }
}

#[cfg(test)]
mod tombstone_tests {
    use super::{ObjectStorage, is_tombstoned, list_tombstoned_streams, to_bytes, tombstone_path};
    use crate::storage::LocalFS;
    use temp_dir::TempDir;

    #[tokio::test]
    async fn no_marker_means_not_tombstoned() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFS::new(dir.path().to_path_buf());

        assert!(!is_tombstoned(&storage, "test_stream", &None).await.unwrap());
    }

    #[tokio::test]
    async fn marker_present_means_tombstoned() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFS::new(dir.path().to_path_buf());

        storage
            .put_object(&tombstone_path("test_stream", &None), to_bytes(&()), &None)
            .await
            .unwrap();

        assert!(is_tombstoned(&storage, "test_stream", &None).await.unwrap());
    }

    #[tokio::test]
    async fn no_markers_means_empty_discovery_list() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFS::new(dir.path().to_path_buf());

        assert!(
            list_tombstoned_streams(&storage, &None)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn marker_present_means_discoverable_by_listing() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFS::new(dir.path().to_path_buf());

        storage
            .put_object(&tombstone_path("test_stream", &None), to_bytes(&()), &None)
            .await
            .unwrap();

        let discovered = list_tombstoned_streams(&storage, &None).await.unwrap();
        assert_eq!(discovered, vec!["test_stream".to_string()]);
    }

    #[tokio::test]
    async fn directory_without_the_actual_marker_is_not_reported_as_tombstoned() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFS::new(dir.path().to_path_buf());

        // A directory can exist under the tombstone root without ever
        // containing the marker itself (e.g. an interrupted write) --
        // list_dirs_relative alone can't tell the difference, so
        // list_tombstoned_streams must re-verify via is_tombstoned.
        let sibling_path = tombstone_path("test_stream", &None)
            .parent()
            .unwrap()
            .join("not-the-marker");
        storage
            .put_object(&sibling_path, to_bytes(&()), &None)
            .await
            .unwrap();

        assert!(!is_tombstoned(&storage, "test_stream", &None).await.unwrap());
        assert!(
            list_tombstoned_streams(&storage, &None)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[cfg(test)]
mod manifest_ownership_tests {
    use super::manifest_segment_matches;

    #[test]
    fn matches_own_manifest_exactly() {
        assert!(manifest_segment_matches(
            "stream/date=2026-08-13/node.manifest.json",
            "node.manifest.json"
        ));
        assert!(manifest_segment_matches(
            "tenant/stream/date=2026-08-13/ingestor.host.abc123.manifest.json",
            "ingestor.host.abc123.manifest.json"
        ));
    }

    #[test]
    fn rejects_hostname_that_is_a_suffix_of_another() {
        // The core of issue #1739: `node` must NOT claim `prod-node`'s manifest. A `contains`
        // or `ends_with` test would return true here and corrupt the wrong writer's data.
        let other = "stream/date=2026-08-13/prod-node.manifest.json";
        assert!(!manifest_segment_matches(other, "node.manifest.json"));
        // Guard the assumption that makes this a real bug, not a hypothetical one.
        assert!(other.contains("node.manifest.json"));
        assert!(other.ends_with("node.manifest.json"));
    }

    #[test]
    fn rejects_a_different_writer() {
        assert!(!manifest_segment_matches(
            "stream/date=2026-08-13/host-a.manifest.json",
            "host-b.manifest.json"
        ));
    }
}

#[cfg(test)]
mod conditional_cloud_metadata_tests {
    use super::*;
    use crate::storage::metrics_layer::MetricLayer;
    use object_store::{limit::LimitStore, memory::InMemory};

    #[tokio::test]
    async fn cloud_adapter_preserves_preconditions_through_storage_wrappers() {
        let store = MetricLayer::new(LimitStore::new(InMemory::new(), 8), "s3");
        let path = RelativePath::new("tenant/users/owner/dashboards/test.json");
        assert!(get_cloud_metadata(&store, path).await.unwrap().is_none());
        let initial = put_cloud_metadata(
            &store,
            path,
            Bytes::from_static(b"initial"),
            MetadataWriteCondition::Create,
        )
        .await
        .unwrap();
        assert!(matches!(
            put_cloud_metadata(
                &store,
                path,
                Bytes::from_static(b"duplicate"),
                MetadataWriteCondition::Create
            )
            .await,
            Err(ConditionalMetadataError::Conflict)
        ));
        let read = get_cloud_metadata(&store, path).await.unwrap().unwrap();
        assert_eq!(read.bytes, Bytes::from_static(b"initial"));
        assert_eq!(read.version, initial);
        let (first, second) = tokio::join!(
            put_cloud_metadata(
                &store,
                path,
                Bytes::from_static(b"first"),
                MetadataWriteCondition::Update(initial.clone())
            ),
            put_cloud_metadata(
                &store,
                path,
                Bytes::from_static(b"second"),
                MetadataWriteCondition::Update(initial)
            ),
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert!(matches!(
            if first.is_err() { first } else { second },
            Err(ConditionalMetadataError::Conflict)
        ));
        assert!(matches!(
            put_cloud_metadata(
                &store,
                path,
                Bytes::from_static(b"unguarded"),
                MetadataWriteCondition::Update(MetadataVersion {
                    e_tag: None,
                    version: None
                })
            )
            .await,
            Err(ConditionalMetadataError::Unsupported(_))
        ));
    }
}

#[cfg(test)]
mod query_reload_tests {
    use super::*;
    use crate::{cli::Options, metadata::LogStreamMetadata, storage::LocalFS};
    use arrow_schema::{DataType, Field};
    use object_store::{ObjectStore, ObjectStoreExt};

    #[tokio::test]
    async fn cold_lookup_accepts_ingestor_only_metadata_and_is_tenant_scoped() {
        let store = object_store::memory::InMemory::new();
        let path = object_store::path::Path::from("tenant/logs/.stream/.ingestor.node.stream.json");
        store
            .put(&path, Bytes::from_static(b"{}").into())
            .await
            .unwrap();
        store
            .put(
                &object_store::path::Path::from("tenant/other/.stream/.stream.json"),
                Bytes::from_static(b"{}").into(),
            )
            .await
            .unwrap();
        let prefix = stream_metadata_prefix("logs", &Some("tenant".to_owned()));
        assert_eq!(prefix.as_ref(), "tenant/logs/.stream");
        let listing = store.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert_eq!(listing.objects.len(), 1);
        assert!(has_stream_metadata(&listing.objects));
        let prefix = stream_metadata_prefix("logs", &Some("other-tenant".to_owned()));
        assert!(!has_stream_metadata(
            &store
                .list_with_delimiter(Some(&prefix))
                .await
                .unwrap()
                .objects
        ));
        assert_eq!(
            stream_metadata_prefix("logs", &None).as_ref(),
            "logs/.stream"
        );
    }

    #[tokio::test]
    async fn localfs_cold_lookup_keeps_global_metadata_predicate() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalFS::new(dir.path().to_owned());
        assert!(!storage.stream_exists("logs", &None).await.unwrap());
        storage
            .put_object(
                &RelativePathBuf::from("logs/.stream/.ingestor.node.stream.json"),
                Bytes::from_static(b"{}"),
                &None,
            )
            .await
            .unwrap();
        assert!(!storage.stream_exists("logs", &None).await.unwrap());
        storage
            .put_object(
                &RelativePathBuf::from("logs/.stream/.stream.json"),
                Bytes::from_static(b"{}"),
                &None,
            )
            .await
            .unwrap();
        assert!(storage.stream_exists("logs", &None).await.unwrap());
        assert!(storage.list_streams().await.unwrap().contains("logs"));
    }

    #[tokio::test]
    async fn staged_schema_upload_updates_memory_without_a_query() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalFS::new(dir.path().join("remote"));
        let options = Options {
            local_staging_path: dir.path().join("staging"),
            ..Options::default()
        };
        let stream = Stream::new(
            Arc::new(options),
            "logs",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let schema = Schema::new(vec![Field::new("recovered", DataType::Utf8, true)]);
        stream.stage_schema_file(schema.clone()).unwrap();
        assert!(stream.get_schema().fields().is_empty());
        let path = RelativePathBuf::from("logs/.stream/.schema");
        process_schema_files_with(&stream, |schema| {
            let storage = &storage;
            let stream = &stream;
            let path = &path;
            async move {
                persist_schema_and_update_memory(
                    Some(stream),
                    &schema,
                    storage.put_object(path, to_bytes(&schema), &None),
                )
                .await
            }
        })
        .await
        .unwrap();
        assert_eq!(stream.get_schema().as_ref(), &schema);
        assert!(stream.schema_files().is_empty());
        assert_eq!(
            serde_json::from_slice::<Schema>(&storage.get_object(&path, &None).await.unwrap())
                .unwrap(),
            schema
        );
    }

    #[tokio::test]
    async fn conflicting_memory_merge_after_upload_does_not_wedge_staging() {
        let dir = tempfile::tempdir().unwrap();
        let options = Options {
            local_staging_path: dir.path().to_owned(),
            ..Options::default()
        };
        let stream = Stream::new(
            Arc::new(options),
            "logs",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let in_memory = Schema::new(vec![Field::new("value", DataType::Boolean, true)]);
        crate::event::commit_schema_to_metadata(&stream.metadata, &in_memory).unwrap();
        stream
            .stage_schema_file(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                true,
            )]))
            .unwrap();
        process_schema_files_with(&stream, |schema| {
            let stream = &stream;
            async move {
                persist_schema_and_update_memory(Some(stream), &schema, async { Ok(()) }).await
            }
        })
        .await
        .unwrap();
        // Stored successfully: the staged file is consumed, not retried forever.
        assert!(stream.schema_files().is_empty());
        assert_eq!(
            stream
                .get_schema()
                .field_with_name("value")
                .unwrap()
                .data_type(),
            &DataType::Boolean
        );
    }

    #[tokio::test]
    async fn failed_schema_upload_leaves_staging_and_memory_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let options = Options {
            local_staging_path: dir.path().to_owned(),
            ..Options::default()
        };
        let stream = Stream::new(
            Arc::new(options),
            "logs",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        stream
            .stage_schema_file(Schema::new(vec![Field::new(
                "recovered",
                DataType::Utf8,
                true,
            )]))
            .unwrap();
        assert!(
            process_schema_files_with(&stream, |schema| {
                let stream = &stream;
                async move {
                    persist_schema_and_update_memory(Some(stream), &schema, async {
                        Err(ObjectStorageError::Custom("upload failed".to_owned()))
                    })
                    .await
                }
            })
            .await
            .is_err()
        );
        assert!(stream.get_schema().fields().is_empty());
        assert_eq!(stream.schema_files().len(), 1);
    }
}

/// Decode exactly one object_store path encoding layer.
pub(crate) fn decode_metadata_key(key: &str) -> Result<String, ObjectStorageError> {
    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let digits = bytes
                .get(i + 1..i + 3)
                .and_then(|d| std::str::from_utf8(d).ok())
                .and_then(|d| u8::from_str_radix(d, 16).ok())
                .ok_or_else(|| {
                    ObjectStorageError::Custom("invalid percent-encoded metadata key".into())
                })?;
            out.push(digits);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|e| ObjectStorageError::Custom(format!("invalid UTF-8 metadata key: {e}")))
}

#[cfg(test)]
mod metadata_key_decode_tests {
    use super::*;
    #[tokio::test]
    async fn postgres_batches_settle_sibling_uploads_before_error_cleanup() {
        for drain in [false, true] {
            let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = completed.clone();
            let mut uploads = JoinSet::new();
            uploads.spawn(async {
                Err((
                    PathBuf::from("failed.parquet"),
                    ObjectStorageError::Custom("upload failed".into()),
                ))
            });
            uploads.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(UploadResult {
                    file_path: PathBuf::from("sibling.parquet"),
                    manifest_file: None,
                })
            });
            assert!(collect_upload_results(uploads, drain).await.is_err());
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            assert_eq!(completed.load(std::sync::atomic::Ordering::SeqCst), drain);
        }
    }
    #[tokio::test]
    async fn compensation_removes_only_parquet_and_tolerates_missing_objects() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::LocalFS::new(dir.path().to_owned());
        let key = "logs/date=2026-01-01/file.parquet";
        storage
            .put_object(RelativePath::new(key), Bytes::from_static(b"data"), &None)
            .await
            .unwrap();
        compensate_uploaded_parquet(&storage, key, &None)
            .await
            .unwrap();
        compensate_uploaded_parquet(&storage, key, &None)
            .await
            .unwrap();
        assert!(
            storage
                .get_object(RelativePath::new(key), &None)
                .await
                .is_err()
        );
        assert!(
            compensate_uploaded_parquet(&storage, "logs/.stream/.stream.json", &None)
                .await
                .is_err()
        );
    }
    #[test]
    fn invalid_encoded_keys_never_panic() {
        assert!(decode_metadata_key("%FF").is_err());
        assert!(decode_metadata_key("%é").is_err());
        assert_eq!(decode_metadata_key("caf%C3%A9").unwrap(), "café");
    }

    #[test]
    fn verify_upload_head_defaults_false_and_parses_truthy_values() {
        assert!(!parse_verify_upload_head(None));
        assert!(!parse_verify_upload_head(Some("")));
        assert!(!parse_verify_upload_head(Some("false")));
        assert!(!parse_verify_upload_head(Some("0")));
        assert!(!parse_verify_upload_head(Some("yes")));
        assert!(parse_verify_upload_head(Some("true")));
        assert!(parse_verify_upload_head(Some("TRUE")));
        assert!(parse_verify_upload_head(Some(" 1 ")));
    }

    #[test]
    fn has_staged_upload_files_single_pass() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!has_staged_upload_files(&dir.path().join("missing")));
        assert!(!has_staged_upload_files(dir.path()));

        std::fs::write(dir.path().join("batch.arrows"), b"arrow").unwrap();
        assert!(!has_staged_upload_files(dir.path()));

        let schema_path = dir.path().join(".schema");
        std::fs::write(&schema_path, b"{}").unwrap();
        assert!(has_staged_upload_files(dir.path()));

        std::fs::remove_file(&schema_path).unwrap();
        assert!(!has_staged_upload_files(dir.path()));

        std::fs::write(dir.path().join("part.parquet"), b"not-even-opened").unwrap();
        assert!(has_staged_upload_files(dir.path()));
    }
}
