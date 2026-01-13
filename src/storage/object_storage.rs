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

use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::{datasource::listing::ListingTableUrl, execution::runtime_env::RuntimeEnvBuilder};
use object_store::ListResult;
use object_store::ObjectMeta;
use object_store::buffered::BufReader;
use once_cell::sync::OnceCell;
use rayon::prelude::*;
use relative_path::RelativePath;
use relative_path::RelativePathBuf;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::{File, remove_file};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::task::JoinSet;
use tracing::info;
use tracing::{error, warn};
use ulid::Ulid;

use crate::catalog::{self, snapshot::Snapshot};
use crate::event::format::LogSource;
use crate::event::format::LogSourceEntry;
use crate::handlers::http::fetch_schema;
use crate::handlers::http::modal::ingest_server::INGESTOR_EXPECT;
use crate::handlers::http::modal::ingest_server::INGESTOR_META;
use crate::handlers::http::users::{FILTER_DIR, USERS_ROOT_DIR};
use crate::metrics::increment_parquets_stored_by_date;
use crate::metrics::increment_parquets_stored_size_by_date;
use crate::metrics::{EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_STORAGE_SIZE, STORAGE_SIZE};
use crate::option::Mode;
use crate::parseable::{LogStream, PARSEABLE, Stream};
use crate::stats::FullStats;
use crate::storage::SETTINGS_ROOT_DIRECTORY;
use crate::storage::TARGETS_ROOT_DIRECTORY;
use crate::storage::field_stats::DATASET_STATS_STREAM_NAME;
use crate::storage::field_stats::calculate_field_stats;

use super::{
    ALERTS_ROOT_DIRECTORY, MANIFEST_FILE, ObjectStorageError, ObjectStoreFormat,
    PARSEABLE_METADATA_FILE_NAME, PARSEABLE_ROOT_DIRECTORY, SCHEMA_FILE_NAME,
    STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY, retention::Retention,
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

/// Handles the upload of a single parquet file
async fn upload_single_parquet_file(
    store: Arc<dyn ObjectStorage>,
    path: std::path::PathBuf,
    stream_relative_path: String,
    stream_name: String,
    schema: Arc<Schema>,
    tenant_id: Option<String>,
) -> Result<UploadResult, ObjectStorageError> {
    let filename = path
        .file_name()
        .expect("only parquet files are returned by iterator")
        .to_str()
        .expect("filename is valid string");
    tracing::warn!("upload single file name- {filename}");

    // Get the local file size for validation
    let local_file_size = path
        .metadata()
        .map_err(|e| ObjectStorageError::Custom(format!("Failed to get local file metadata: {e}")))?
        .len();
    tracing::warn!("upload single stream_relative_path- {stream_relative_path:?}");
    tracing::warn!("upload single path- {path:?}");

    // Upload the file
    store
        .upload_multipart(&RelativePathBuf::from(&stream_relative_path), &path)
        .await
        .map_err(|e| {
            error!("Failed to upload file {filename:?} to {stream_relative_path}: {e}");
            ObjectStorageError::Custom(format!("Failed to upload {filename}: {e}"))
        })?;

    // Validate the uploaded file size matches local file
    let upload_is_valid = validate_uploaded_parquet_file(
        &store,
        &stream_relative_path,
        local_file_size,
        &stream_name,
    )
    .await?;

    if !upload_is_valid {
        // Upload validation failed, clean up the uploaded file and return error
        let _ = store
            .delete_object(&RelativePathBuf::from(&stream_relative_path))
            .await;
        error!("Upload size validation failed for file {filename:?}, deleted from object storage");
        return Ok(UploadResult {
            file_path: path,
            manifest_file: None, // Preserve staging file for retry; no manifest created
        });
    }

    // Update storage metrics
    update_storage_metrics(&path, &stream_name, filename)?;

    // Create manifest entry
    let absolute_path = store
        .absolute_url(RelativePath::from_path(&stream_relative_path).expect("valid relative path"))
        .to_string();

    let manifest = catalog::create_from_parquet_file(absolute_path, &path)?;

    // Calculate field stats if enabled
    calculate_stats_if_enabled(&stream_name, &path, &schema, tenant_id).await;

    Ok(UploadResult {
        file_path: path,
        manifest_file: Some(manifest),
    })
}

/// Updates storage-related metrics for an uploaded file
fn update_storage_metrics(
    path: &std::path::Path,
    stream_name: &str,
    filename: &str,
) -> Result<(), ObjectStorageError> {
    let mut file_date_part = filename.split('.').collect::<Vec<&str>>()[0];
    file_date_part = file_date_part.split('=').collect::<Vec<&str>>()[1];
    let compressed_size = path
        .metadata()
        .map(|m| m.len())
        .map_err(|e| ObjectStorageError::Custom(format!("metadata failed for {filename}: {e}")))?;
    STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .add(compressed_size as i64);
    EVENTS_STORAGE_SIZE_DATE
        .with_label_values(&["data", stream_name, "parquet", file_date_part])
        .inc_by(compressed_size);
    LIFETIME_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .add(compressed_size as i64);

    // billing metrics for parquet storage
    increment_parquets_stored_by_date(file_date_part);
    increment_parquets_stored_size_by_date(compressed_size, file_date_part);

    Ok(())
}

/// Calculates field statistics if enabled and conditions are met
async fn calculate_stats_if_enabled(
    stream_name: &str,
    path: &std::path::Path,
    schema: &Arc<Schema>,
    tenant_id: Option<String>,
) {
    if stream_name != DATASET_STATS_STREAM_NAME && PARSEABLE.options.collect_dataset_stats {
        let max_field_statistics = PARSEABLE.options.max_field_statistics;
        if let Err(err) =
            calculate_field_stats(stream_name, path, schema, max_field_statistics, &tenant_id).await
        {
            tracing::trace!(
                "Error calculating field stats for stream {}: {}",
                stream_name,
                err
            );
        }
    }
}

/// Validates that a parquet file uploaded to object storage matches the staging file size
async fn validate_uploaded_parquet_file(
    store: &Arc<dyn ObjectStorage>,
    stream_relative_path: &str,
    expected_size: u64,
    stream_name: &str,
) -> Result<bool, ObjectStorageError> {
    // Verify the file exists and has the expected size
    match store
        .head(&RelativePathBuf::from(stream_relative_path))
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

#[async_trait]
pub trait ObjectStorage: Debug + Send + Sync + 'static {
    async fn get_buffered_reader(
        &self,
        path: &RelativePath,
    ) -> Result<BufReader, ObjectStorageError>;
    async fn head(&self, path: &RelativePath) -> Result<ObjectMeta, ObjectStorageError>;
    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError>;
    // TODO: make the filter function optional as we may want to get all objects
    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_fun: Box<dyn Fn(String) -> bool + Send>,
    ) -> Result<Vec<Bytes>, ObjectStorageError>;
    async fn upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
    ) -> Result<(), ObjectStorageError>;
    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError>;
    async fn delete_prefix(&self, path: &RelativePath) -> Result<(), ObjectStorageError>;
    async fn check(&self) -> Result<(), ObjectStorageError>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError>;
    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError>;
    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError>;
    async fn list_dirs(&self) -> Result<Vec<String>, ObjectStorageError>;
    async fn list_dirs_relative(
        &self,
        relative_path: &RelativePath,
    ) -> Result<Vec<String>, ObjectStorageError>;

    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError>;
    /// Lists the immediate “hour=” partition directories under the given date.
    /// Only immediate child entries named `hour=HH` should be returned (no trailing slash).
    /// `HH` must be zero-padded two-digit numerals (`"hour=00"` through `"hour=23"`).
    async fn list_hours(
        &self,
        stream_name: &str,
        date: &str,
    ) -> Result<Vec<String>, ObjectStorageError>;

    /// Lists the immediate “minute=” partition directories under the given date/hour.
    /// Only immediate child entries named `minute=MM` should be returned (no trailing slash).
    /// `MM` must be zero-padded two-digit numerals (`"minute=00"` through `"minute=59"`).
    async fn list_minutes(
        &self,
        stream_name: &str,
        date: &str,
        hour: &str,
    ) -> Result<Vec<String>, ObjectStorageError>;
    // async fn list_manifest_files(
    //     &self,
    //     stream_name: &str,
    // ) -> Result<BTreeMap<String, Vec<String>>, ObjectStorageError>;
    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError>;
    async fn delete_object(&self, path: &RelativePath) -> Result<(), ObjectStorageError>;
    async fn get_ingestor_meta_file_paths(
        &self,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError>;
    async fn try_delete_node_meta(&self, node_filename: String) -> Result<(), ObjectStorageError>;
    /// Returns the amount of time taken by the `ObjectStore` to perform a get
    /// call.
    async fn get_latency(&self) -> Duration {
        // It's Ok to `unwrap` here. The hardcoded value will always Result in
        // an `Ok`.
        let path = parseable_json_path();
        let start = Instant::now();
        let _ = self.get_object(&path).await;
        start.elapsed()
    }

    fn query_prefixes(&self, prefixes: Vec<String>) -> Vec<ListingTableUrl>;
    fn absolute_url(&self, prefix: &RelativePath) -> object_store::path::Path;
    fn store_url(&self) -> url::Url;

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
                .get_stream_json(stream_name, false, tenant_id)
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
                .get_stream_json(stream_name, false, tenant_id)
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
                .get_stream_json(stream_name, false, tenant_id)
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
                .get_stream_json(stream_name, false, tenant_id)
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
                .get_stream_json(stream_name, false, tenant_id)
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
                .get_stream_json(stream_name, false, tenant_id)
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
            .get_stream_json(stream_name, false, tenant_id)
            .await
        {
            Ok(data) => data,
            Err(_) => {
                // get the base stream metadata
                let bytes = PARSEABLE
                    .metastore
                    .get_stream_json(stream_name, true, tenant_id)
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
            .get_stream_json(stream_name, true, tenant_id)
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
                .get_stream_json(stream_name, false, tenant_id)
                .await
                .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?;
            return Ok(stream_metadata_bytes);
        }
        let mut all_log_sources: Vec<LogSourceEntry> = Vec::new();

        if let Some(stream_metadata_obs) = PARSEABLE
            .metastore
            .get_all_stream_jsons(stream_name, Some(Mode::Ingest), tenant_id)
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

    ///create schema from storage
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
            .get_all_stream_jsons(stream_name, None, tenant_id)
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
    ) -> Result<(Option<String>, Option<String>), ObjectStorageError> {
        // Get all available dates for the stream
        let dates = self.list_dates(stream_name).await?;
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

        // Extract timestamps for min and max dates
        let first_timestamp = self
            .extract_timestamp_for_date(stream_name, min_date, true)
            .await?;
        let latest_timestamp = self
            .extract_timestamp_for_date(stream_name, max_date, false)
            .await?;

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
    ) -> Result<Option<DateTime<Utc>>, ObjectStorageError> {
        // Get all hours for this date
        let hours = self.list_hours(stream_name, date).await?;
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
            .list_minutes(stream_name, date, target_hour_str)
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

        // Process parquet files concurrently and collect results
        let manifest_files =
            process_parquet_files(&upload_context, stream_name, tenant_id.clone()).await?;

        // Update snapshot with collected manifest files
        update_snapshot_with_manifests(stream_name, manifest_files, &tenant_id).await?;

        // Process schema files
        process_schema_files(&upload_context, stream_name, &tenant_id).await?;

        Ok(())
    }
}

/// Processes parquet files concurrently and returns stats status and manifest files
async fn process_parquet_files(
    upload_context: &UploadContext,
    stream_name: &str,
    tenant_id: Option<String>,
) -> Result<Vec<catalog::manifest::File>, ObjectStorageError> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(100));
    let mut join_set = JoinSet::new();
    let object_store = PARSEABLE.storage().get_object_store();

    // Spawn upload tasks for each parquet file
    for path in upload_context.stream.parquet_files() {
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
    collect_upload_results(join_set).await
}

/// Spawns an individual parquet file upload task
async fn spawn_parquet_upload_task(
    join_set: &mut JoinSet<Result<UploadResult, ObjectStorageError>>,
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
    tracing::warn!("spawn parquet file name- {filename}");

    let stream_relative_path = stream_relative_path(
        stream_name,
        filename,
        &upload_context.custom_partition,
        &tenant_id,
    );
    tracing::warn!("spawn parquet stream_relative_path- {stream_relative_path}");

    let stream_name = stream_name.to_string();
    let schema = upload_context.schema.clone();

    join_set.spawn(async move {
        let _permit = semaphore.acquire().await.expect("semaphore is not closed");

        upload_single_parquet_file(
            store,
            path,
            stream_relative_path,
            stream_name,
            schema,
            tenant_id,
        )
        .await
    });
}

/// Collects results from all upload tasks
async fn collect_upload_results(
    mut join_set: JoinSet<Result<UploadResult, ObjectStorageError>>,
) -> Result<Vec<catalog::manifest::File>, ObjectStorageError> {
    let mut uploaded_files = Vec::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(upload_result)) => {
                if let Some(manifest_file) = upload_result.manifest_file {
                    uploaded_files.push((upload_result.file_path, manifest_file));
                } else {
                    // File failed in upload size validation, preserve staging file for retry
                    error!(
                        "Parquet file upload size validation failed for {:?}, preserving in staging for retry",
                        upload_result.file_path
                    );
                }
            }
            Ok(Err(e)) => {
                error!("Error uploading parquet file: {e}");
                return Err(e);
            }
            Err(e) => {
                error!("Task panicked: {e}");
                return Err(ObjectStorageError::UnhandledError(Box::new(e)));
            }
        }
    }

    let manifest_files: Vec<_> = uploaded_files
        .into_par_iter()
        .map(|(path, manifest_file)| {
            if let Err(e) = remove_file(&path) {
                warn!("Failed to remove staged file: {e}");
            }
            manifest_file
        })
        .collect();

    Ok(manifest_files)
}

/// Updates snapshot with collected manifest files
async fn update_snapshot_with_manifests(
    stream_name: &str,
    manifest_files: Vec<catalog::manifest::File>,
    tenant_id: &Option<String>,
) -> Result<(), ObjectStorageError> {
    if !manifest_files.is_empty() {
        catalog::update_snapshot(stream_name, manifest_files, tenant_id).await?;
    }
    Ok(())
}

/// Processes schema files
async fn process_schema_files(
    upload_context: &UploadContext,
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<(), ObjectStorageError> {
    for path in upload_context.stream.schema_files() {
        let file = File::open(&path)?;
        let schema: Schema = serde_json::from_reader(file)?;
        commit_schema_to_storage(stream_name, schema, tenant_id).await?;

        if let Err(e) = remove_file(path) {
            warn!("Failed to remove staged file: {e}");
        }
    }
    Ok(())
}

/// Builds the stream relative path for a file
fn stream_relative_path(
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
    if let Some(tenant) = tenant_id {
        format!("{tenant}/{stream_name}/{file_suffix}")
    } else {
        format!("{stream_name}/{file_suffix}")
    }
}

pub fn sync_all_streams(joinset: &mut JoinSet<Result<(), ObjectStorageError>>) {
    let object_store = PARSEABLE.storage().get_object_store();
    let tenants = if let Some(tenants) = PARSEABLE.list_tenants() {
        tenants.into_iter().map(|v| Some(v)).collect()
    } else {
        vec![None]
    };
    for tenant_id in tenants {
        for stream_name in PARSEABLE.streams.list(&tenant_id) {
            let object_store = object_store.clone();
            let id = tenant_id.clone();
            joinset.spawn(async move {
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
            });
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

    PARSEABLE
        .metastore
        .put_schema(new_schema, stream_name, tenant_id)
        .await
        .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))
}

#[inline(always)]
pub fn to_bytes(any: &(impl ?Sized + serde::Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}

pub fn schema_path(stream_name: &str, tenant_id: &Option<String>) -> RelativePathBuf {
    let tenant = tenant_id.as_ref().map_or("", |v| v);
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
    let tenant = tenant_id.as_ref().map_or("", |v| v);
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

/// if filter_id is an empty str it should not append it to the rel path
#[inline(always)]
pub fn filter_path(user_id: &str, stream_name: &str, filter_file_name: &str) -> RelativePathBuf {
    RelativePathBuf::from_iter([
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
    if let Some(tenant_id) = tenant_id.as_ref() {
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
pub fn target_json_path(target_id: &Ulid) -> RelativePathBuf {
    RelativePathBuf::from_iter([
        SETTINGS_ROOT_DIRECTORY,
        TARGETS_ROOT_DIRECTORY,
        &format!("{target_id}.json"),
    ])
}

/// Constructs the path for storing alert state JSON files
/// Format: ".alerts/alert_state_{alert_id}.json"
#[inline(always)]
pub fn alert_state_json_path(alert_id: Ulid) -> RelativePathBuf {
    RelativePathBuf::from_iter([
        ALERTS_ROOT_DIRECTORY,
        &format!("alert_state_{alert_id}.json"),
    ])
}

/// Constructs the path for storing MTTR history JSON file
/// Format: ".alerts/mttr.json"
#[inline(always)]
pub fn mttr_json_path(tenant_id: &Option<String>) -> RelativePathBuf {
    if let Some(tenant) = tenant_id.as_ref() {
        RelativePathBuf::from_iter([&tenant, ALERTS_ROOT_DIRECTORY, "mttr.json"])
    } else {
        RelativePathBuf::from_iter([ALERTS_ROOT_DIRECTORY, "mttr.json"])
    }
}

#[inline(always)]
pub fn manifest_path(prefix: &str, tenant_id: &Option<String>) -> RelativePathBuf {
    let tenant = tenant_id.as_ref().map_or("", |v| v);
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
        RelativePathBuf::from_iter([tenant, prefix, &manifest_file_name])
    } else {
        let manifest_file_name = format!("{hostname}.{MANIFEST_FILE}");
        RelativePathBuf::from_iter([tenant, prefix, &manifest_file_name])
    }
}
