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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::{remove_file, File};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use actix_web_prometheus::PrometheusMetrics;
use arrow_array::Array;
use arrow_array::BinaryArray;
use arrow_array::BinaryViewArray;
use arrow_array::BooleanArray;
use arrow_array::Date32Array;
use arrow_array::Float64Array;
use arrow_array::Int64Array;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_array::TimestampMillisecondArray;
use arrow_schema::DataType;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use datafusion::{datasource::listing::ListingTableUrl, execution::runtime_env::RuntimeEnvBuilder};
use futures::StreamExt;
use object_store::buffered::BufReader;
use object_store::ObjectMeta;
use once_cell::sync::OnceCell;
use relative_path::RelativePath;
use relative_path::RelativePathBuf;
use serde::Serialize;
use tokio::task::JoinSet;
use tracing::info;
use tracing::{error, warn};
use ulid::Ulid;

use crate::alerts::AlertConfig;
use crate::catalog::{self, manifest::Manifest, snapshot::Snapshot};
use crate::correlation::{CorrelationConfig, CorrelationError};
use crate::event::format::LogSource;
use crate::event::format::LogSourceEntry;
use crate::handlers::http::ingest::PostError;
use crate::handlers::http::modal::ingest_server::INGESTOR_EXPECT;
use crate::handlers::http::modal::ingest_server::INGESTOR_META;
use crate::handlers::http::modal::utils::ingest_utils::flatten_and_push_logs;
use crate::handlers::http::users::CORRELATION_DIR;
use crate::handlers::http::users::{DASHBOARDS_DIR, FILTER_DIR, USERS_ROOT_DIR};
use crate::metrics::storage::StorageMetrics;
use crate::metrics::{EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_STORAGE_SIZE, STORAGE_SIZE};
use crate::option::Mode;
use crate::parseable::LogStream;
use crate::parseable::PARSEABLE;
use crate::query::QUERY_SESSION_STATE;
use crate::stats::FullStats;
use crate::storage::StreamType;
use crate::utils::DATASET_STATS_STREAM_NAME;

use super::{
    retention::Retention, ObjectStorageError, ObjectStoreFormat, StorageMetadata,
    ALERTS_ROOT_DIRECTORY, MANIFEST_FILE, PARSEABLE_METADATA_FILE_NAME, PARSEABLE_ROOT_DIRECTORY,
    SCHEMA_FILE_NAME, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
};

pub trait ObjectStorageProvider: StorageMetrics + std::fmt::Debug + Send + Sync {
    fn get_datafusion_runtime(&self) -> RuntimeEnvBuilder;
    fn construct_client(&self) -> Arc<dyn ObjectStorage>;
    fn get_object_store(&self) -> Arc<dyn ObjectStorage> {
        static STORE: OnceCell<Arc<dyn ObjectStorage>> = OnceCell::new();

        STORE.get_or_init(|| self.construct_client()).clone()
    }
    fn get_endpoint(&self) -> String;
    fn register_store_metrics(&self, handler: &PrometheusMetrics);
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

    async fn get_all_saved_filters(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut filters: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();

        let users_dir = RelativePathBuf::from(USERS_ROOT_DIR);
        for user in self.list_dirs_relative(&users_dir).await? {
            let stream_dir = users_dir.join(&user).join("filters");
            for stream in self.list_dirs_relative(&stream_dir).await? {
                let filters_path = stream_dir.join(&stream);
                let filter_bytes = self
                    .get_objects(
                        Some(&filters_path),
                        Box::new(|file_name| file_name.ends_with(".json")),
                    )
                    .await?;
                filters
                    .entry(filters_path)
                    .or_default()
                    .extend(filter_bytes);
            }
        }

        Ok(filters)
    }

    async fn get_all_dashboards(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut dashboards: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();

        let users_dir = RelativePathBuf::from(USERS_ROOT_DIR);
        for user in self.list_dirs_relative(&users_dir).await? {
            let dashboards_path = users_dir.join(&user).join("dashboards");
            let dashboard_bytes = self
                .get_objects(
                    Some(&dashboards_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                )
                .await?;

            dashboards
                .entry(dashboards_path)
                .or_default()
                .extend(dashboard_bytes);
        }

        Ok(dashboards)
    }

    ///fetch all correlations stored in object store
    /// return the correlation file path and all correlation json bytes for each file path
    async fn get_all_correlations(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut correlations: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();

        let users_dir = RelativePathBuf::from(USERS_ROOT_DIR);
        for user in self.list_dirs_relative(&users_dir).await? {
            let correlations_path = users_dir.join(&user).join("correlations");
            let correlation_bytes = self
                .get_objects(
                    Some(&correlations_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                )
                .await?;

            correlations
                .entry(correlations_path)
                .or_default()
                .extend(correlation_bytes);
        }

        Ok(correlations)
    }

    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError>;
    async fn list_manifest_files(
        &self,
        stream_name: &str,
    ) -> Result<BTreeMap<String, Vec<String>>, ObjectStorageError>;
    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError>;
    async fn delete_object(&self, path: &RelativePath) -> Result<(), ObjectStorageError>;
    async fn get_ingestor_meta_file_paths(
        &self,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError>;
    async fn get_stream_file_paths(
        &self,
        stream_name: &str,
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

    async fn put_schema(
        &self,
        stream_name: &str,
        schema: &Schema,
    ) -> Result<(), ObjectStorageError> {
        self.put_object(&schema_path(stream_name), to_bytes(schema))
            .await?;

        Ok(())
    }

    async fn create_stream(
        &self,
        stream_name: &str,
        meta: ObjectStoreFormat,
        schema: Arc<Schema>,
    ) -> Result<String, ObjectStorageError> {
        let format_json = to_bytes(&meta);
        self.put_object(&schema_path(stream_name), to_bytes(&schema))
            .await?;

        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

        Ok(meta.created_at)
    }

    async fn update_time_partition_limit_in_stream(
        &self,
        stream_name: &str,
        time_partition_limit: NonZeroU32,
    ) -> Result<(), ObjectStorageError> {
        let mut format = self.get_object_store_format(stream_name).await?;
        format.time_partition_limit = Some(time_partition_limit.to_string());
        let format_json = to_bytes(&format);
        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

        Ok(())
    }

    async fn update_custom_partition_in_stream(
        &self,
        stream_name: &str,
        custom_partition: Option<&String>,
    ) -> Result<(), ObjectStorageError> {
        let mut format = self.get_object_store_format(stream_name).await?;
        format.custom_partition = custom_partition.cloned();
        let format_json = to_bytes(&format);
        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

        Ok(())
    }

    async fn update_log_source_in_stream(
        &self,
        stream_name: &str,
        log_source: &[LogSourceEntry],
    ) -> Result<(), ObjectStorageError> {
        let mut format = self.get_object_store_format(stream_name).await?;
        format.log_source = log_source.to_owned();
        let format_json = to_bytes(&format);
        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

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
    ) -> Result<(), ObjectStorageError> {
        let mut format = self.get_object_store_format(stream_name).await?;
        format.first_event_at = Some(first_event.to_string());
        let format_json = to_bytes(&format);
        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

        Ok(())
    }

    async fn put_alert(
        &self,
        alert_id: Ulid,
        alert: &AlertConfig,
    ) -> Result<(), ObjectStorageError> {
        self.put_object(&alert_json_path(alert_id), to_bytes(alert))
            .await
    }

    async fn put_stats(
        &self,
        stream_name: &str,
        stats: &FullStats,
    ) -> Result<(), ObjectStorageError> {
        let path = stream_json_path(stream_name);
        let stream_metadata = self.get_object(&path).await?;
        let stats = serde_json::to_value(stats).expect("stats are perfectly serializable");
        let mut stream_metadata: serde_json::Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        stream_metadata["stats"] = stats;
        self.put_object(&path, to_bytes(&stream_metadata)).await
    }

    async fn put_retention(
        &self,
        stream_name: &str,
        retention: &Retention,
    ) -> Result<(), ObjectStorageError> {
        let path = stream_json_path(stream_name);
        let stream_metadata = self.get_object(&path).await?;
        let mut stream_metadata: ObjectStoreFormat =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");
        stream_metadata.retention = Some(retention.clone());

        self.put_object(&path, to_bytes(&stream_metadata)).await
    }

    async fn put_metadata(
        &self,
        parseable_metadata: &StorageMetadata,
    ) -> Result<(), ObjectStorageError> {
        self.put_object(&parseable_json_path(), to_bytes(parseable_metadata))
            .await
    }

    async fn upsert_schema_to_storage(
        &self,
        stream_name: &str,
    ) -> Result<Schema, ObjectStorageError> {
        // try get my schema
        // if fails get the base schema
        // put the schema to storage??
        let schema_path = schema_path(stream_name);
        let byte_data = match self.get_object(&schema_path).await {
            Ok(bytes) => bytes,
            Err(_) => {
                // base schema path
                let schema_path = RelativePathBuf::from_iter([
                    stream_name,
                    STREAM_ROOT_DIRECTORY,
                    SCHEMA_FILE_NAME,
                ]);
                let data = self.get_object(&schema_path).await?;
                // schema was not found in store, so it needs to be placed
                self.put_schema(stream_name, &serde_json::from_slice(&data)?)
                    .await?;

                data
            }
        };
        Ok(serde_json::from_slice(&byte_data)?)
    }

    async fn get_schema(&self, stream_name: &str) -> Result<Schema, ObjectStorageError> {
        let schema_map = self.get_object(&schema_path(stream_name)).await?;
        Ok(serde_json::from_slice(&schema_map)?)
    }

    async fn get_alerts(&self) -> Result<Vec<AlertConfig>, ObjectStorageError> {
        let alerts_path = RelativePathBuf::from(ALERTS_ROOT_DIRECTORY);
        let alerts = self
            .get_objects(
                Some(&alerts_path),
                Box::new(|file_name| file_name.ends_with(".json")),
            )
            .await?
            .iter()
            .filter_map(|bytes| {
                serde_json::from_slice(bytes)
                    .inspect_err(|err| warn!("Expected compatible json, error = {err}"))
                    .ok()
            })
            .collect();

        Ok(alerts)
    }

    async fn upsert_stream_metadata(
        &self,
        stream_name: &str,
    ) -> Result<ObjectStoreFormat, ObjectStorageError> {
        let stream_metadata = match self.get_object(&stream_json_path(stream_name)).await {
            Ok(data) => data,
            Err(_) => {
                // get the base stream metadata
                let bytes = self
                    .get_object(&RelativePathBuf::from_iter([
                        stream_name,
                        STREAM_ROOT_DIRECTORY,
                        STREAM_METADATA_FILE_NAME,
                    ]))
                    .await?;

                let mut config = serde_json::from_slice::<ObjectStoreFormat>(&bytes)
                    .expect("parseable config is valid json");

                if PARSEABLE.options.mode == Mode::Ingest {
                    config.stats = FullStats::default();
                    config.snapshot.manifest_list = vec![];
                }

                self.put_stream_manifest(stream_name, &config).await?;
                bytes
            }
        };

        Ok(serde_json::from_slice(&stream_metadata).expect("parseable config is valid json"))
    }

    async fn put_stream_manifest(
        &self,
        stream_name: &str,
        manifest: &ObjectStoreFormat,
    ) -> Result<(), ObjectStorageError> {
        let path = stream_json_path(stream_name);
        self.put_object(&path, to_bytes(manifest)).await
    }

    async fn get_metadata(&self) -> Result<Option<StorageMetadata>, ObjectStorageError> {
        let parseable_metadata: Option<StorageMetadata> =
            match self.get_object(&parseable_json_path()).await {
                Ok(bytes) => {
                    Some(serde_json::from_slice(&bytes).expect("parseable config is valid json"))
                }
                Err(err) => {
                    if matches!(err, ObjectStorageError::NoSuchKey(_)) {
                        None
                    } else {
                        return Err(err);
                    }
                }
            };

        Ok(parseable_metadata)
    }

    // get the manifest info
    async fn get_manifest(
        &self,
        path: &RelativePath,
    ) -> Result<Option<Manifest>, ObjectStorageError> {
        let path = manifest_path(path.as_str());
        match self.get_object(&path).await {
            Ok(bytes) => {
                let manifest = serde_json::from_slice(&bytes)?;
                Ok(Some(manifest))
            }
            Err(ObjectStorageError::NoSuchKey(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn put_manifest(
        &self,
        path: &RelativePath,
        manifest: Manifest,
    ) -> Result<(), ObjectStorageError> {
        let path = manifest_path(path.as_str());
        self.put_object(&path, to_bytes(&manifest)).await
    }

    // gets the snapshot of the stream
    async fn get_object_store_format(
        &self,
        stream: &str,
    ) -> Result<ObjectStoreFormat, ObjectStorageError> {
        let path = stream_json_path(stream);
        let bytes = self.get_object(&path).await?;
        Ok(serde_json::from_slice::<ObjectStoreFormat>(&bytes).expect("snapshot is valid json"))
    }

    async fn put_snapshot(
        &self,
        stream: &str,
        snapshot: Snapshot,
    ) -> Result<(), ObjectStorageError> {
        let mut stream_meta = self.upsert_stream_metadata(stream).await?;
        stream_meta.snapshot = snapshot;
        self.put_object(&stream_json_path(stream), to_bytes(&stream_meta))
            .await
    }

    ///create stream from querier stream.json from storage
    async fn create_stream_from_querier(
        &self,
        stream_name: &str,
    ) -> Result<Bytes, ObjectStorageError> {
        let stream_path = RelativePathBuf::from_iter([
            stream_name,
            STREAM_ROOT_DIRECTORY,
            STREAM_METADATA_FILE_NAME,
        ]);

        if let Ok(querier_stream_json_bytes) = self.get_object(&stream_path).await {
            let querier_stream_metadata =
                serde_json::from_slice::<ObjectStoreFormat>(&querier_stream_json_bytes)?;
            let stream_metadata = ObjectStoreFormat {
                stats: FullStats::default(),
                snapshot: Snapshot::default(),
                ..querier_stream_metadata
            };
            let stream_metadata_bytes: Bytes = serde_json::to_vec(&stream_metadata)?.into();
            self.put_object(
                &stream_json_path(stream_name),
                stream_metadata_bytes.clone(),
            )
            .await?;
            return Ok(stream_metadata_bytes);
        }

        Ok(Bytes::new())
    }

    ///create stream from ingestor stream.json from storage
    async fn create_stream_from_ingestor(
        &self,
        stream_name: &str,
    ) -> Result<Bytes, ObjectStorageError> {
        let stream_path = RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY]);
        let mut all_log_sources: Vec<LogSourceEntry> = Vec::new();

        if let Some(stream_metadata_obs) = self
            .get_objects(
                Some(&stream_path),
                Box::new(|file_name| {
                    file_name.starts_with(".ingestor") && file_name.ends_with("stream.json")
                }),
            )
            .await
            .into_iter()
            .next()
        {
            if !stream_metadata_obs.is_empty() {
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
                self.put_object(
                    &stream_json_path(stream_name),
                    stream_metadata_bytes.clone(),
                )
                .await?;

                return Ok(stream_metadata_bytes);
            }
        }
        Ok(Bytes::new())
    }

    ///create schema from querier schema from storage
    async fn create_schema_from_querier(
        &self,
        stream_name: &str,
    ) -> Result<Bytes, ObjectStorageError> {
        let path =
            RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY, SCHEMA_FILE_NAME]);
        if let Ok(querier_schema_bytes) = self.get_object(&path).await {
            self.put_object(&schema_path(stream_name), querier_schema_bytes.clone())
                .await?;
            return Ok(querier_schema_bytes);
        }
        Ok(Bytes::new())
    }

    ///create schema from ingestor schema from storage
    async fn create_schema_from_ingestor(
        &self,
        stream_name: &str,
    ) -> Result<Bytes, ObjectStorageError> {
        let path = RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY]);
        if let Some(schema_obs) = self
            .get_objects(
                Some(&path),
                Box::new(|file_name| {
                    file_name.starts_with(".ingestor") && file_name.ends_with("schema")
                }),
            )
            .await
            .into_iter()
            .next()
        {
            let schema_ob = &schema_obs[0];
            self.put_object(&schema_path(stream_name), schema_ob.clone())
                .await?;
            return Ok(schema_ob.clone());
        }
        Ok(Bytes::new())
    }

    async fn get_stream_meta_from_storage(
        &self,
        stream_name: &str,
    ) -> Result<Vec<ObjectStoreFormat>, ObjectStorageError> {
        let mut stream_metas = vec![];
        let stream_meta_bytes = self
            .get_objects(
                Some(&RelativePathBuf::from_iter([
                    stream_name,
                    STREAM_ROOT_DIRECTORY,
                ])),
                Box::new(|file_name| file_name.ends_with("stream.json")),
            )
            .await;
        if let Ok(stream_meta_bytes) = stream_meta_bytes {
            for stream_meta in stream_meta_bytes {
                let stream_meta_ob = serde_json::from_slice::<ObjectStoreFormat>(&stream_meta)?;
                stream_metas.push(stream_meta_ob);
            }
        }

        Ok(stream_metas)
    }

    async fn get_log_source_from_storage(
        &self,
        stream_name: &str,
    ) -> Result<Vec<LogSourceEntry>, ObjectStorageError> {
        let mut all_log_sources: Vec<LogSourceEntry> = Vec::new();
        let stream_metas = self.get_stream_meta_from_storage(stream_name).await;
        if let Ok(stream_metas) = stream_metas {
            for stream_meta in stream_metas.iter() {
                // fetch unique log sources and their fields
                all_log_sources.extend(stream_meta.log_source.clone());
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

    /// Retrieves the earliest first-event-at from the storage for the specified stream.
    ///
    /// This function fetches the object-store format from all the stream.json files for the given stream from the storage,
    /// extracts the `first_event_at` timestamps, and returns the earliest `first_event_at`.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The name of the stream for which `first_event_at` is to be retrieved.
    ///
    /// # Returns
    ///
    /// * `Result<Option<String>, ObjectStorageError>` - Returns `Ok(Some(String))` with the earliest
    ///   first event timestamp if found, `Ok(None)` if no timestamps are found, or an `ObjectStorageError`
    ///   if an error occurs.
    ///
    /// # Examples
    /// ```ignore
    /// ```rust
    /// let result = get_first_event_from_storage("my_stream").await;
    /// match result {
    ///     Ok(Some(first_event)) => println!("first-event-at: {}", first_event),
    ///     Ok(None) => println!("first-event-at not found"),
    ///     Err(err) => println!("Error: {:?}", err),
    /// }
    /// ```
    async fn get_first_event_from_storage(
        &self,
        stream_name: &str,
    ) -> Result<Option<String>, ObjectStorageError> {
        let mut all_first_events = vec![];
        let stream_metas = self.get_stream_meta_from_storage(stream_name).await;
        if let Ok(stream_metas) = stream_metas {
            for stream_meta in stream_metas.iter() {
                if let Some(first_event) = &stream_meta.first_event_at {
                    let first_event = DateTime::parse_from_rfc3339(first_event).unwrap();
                    let first_event = first_event.with_timezone(&Utc);
                    all_first_events.push(first_event);
                }
            }
        }

        if all_first_events.is_empty() {
            return Ok(None);
        }
        let first_event_at = all_first_events.iter().min().unwrap().to_rfc3339();
        Ok(Some(first_event_at))
    }

    // pick a better name
    fn get_bucket_name(&self) -> String;

    async fn put_correlation(
        &self,
        correlation: &CorrelationConfig,
    ) -> Result<(), ObjectStorageError> {
        let path =
            RelativePathBuf::from_iter([CORRELATION_DIR, &format!("{}.json", correlation.id)]);
        self.put_object(&path, to_bytes(correlation)).await?;
        Ok(())
    }

    async fn get_correlations(&self) -> Result<Vec<Bytes>, CorrelationError> {
        let correlation_path = RelativePathBuf::from(CORRELATION_DIR);
        let correlation_bytes = self
            .get_objects(
                Some(&correlation_path),
                Box::new(|file_name| file_name.ends_with(".json")),
            )
            .await?;

        Ok(correlation_bytes)
    }

    async fn upload_files_from_staging(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        if !PARSEABLE.options.staging_dir().exists() {
            return Ok(());
        }
        info!("Starting object_store_sync for stream- {stream_name}");

        let stream = PARSEABLE.get_or_create_stream(stream_name);
        let custom_partition = stream.get_custom_partition();
        let schema = stream.get_schema();
        for path in stream.parquet_files() {
            if stream.get_stream_type() != StreamType::Internal
                && PARSEABLE.options.collect_dataset_stats
            {
                if let Err(err) = calculate_field_stats(stream_name, &path, &schema).await {
                    warn!(
                        "Error calculating field stats for stream {}: {}",
                        stream_name, err
                    );
                }
            }
            let filename = path
                .file_name()
                .expect("only parquet files are returned by iterator")
                .to_str()
                .expect("filename is valid string");

            let mut file_suffix = str::replacen(filename, ".", "/", 3);

            let custom_partition_clone = custom_partition.clone();
            if custom_partition_clone.is_some() {
                let custom_partition_fields = custom_partition_clone.unwrap();
                let custom_partition_list =
                    custom_partition_fields.split(',').collect::<Vec<&str>>();
                file_suffix = str::replacen(filename, ".", "/", 3 + custom_partition_list.len());
            }

            let stream_relative_path = format!("{stream_name}/{file_suffix}");

            // Try uploading the file, handle potential errors without breaking the loop
            // if let Err(e) = self.upload_multipart(key, path)
            if let Err(e) = self
                .upload_multipart(&RelativePathBuf::from(&stream_relative_path), &path)
                .await
            {
                error!("Failed to upload file {filename:?}: {e}");
                continue; // Skip to the next file
            }
            let mut file_date_part = filename.split('.').collect::<Vec<&str>>()[0];
            file_date_part = file_date_part.split('=').collect::<Vec<&str>>()[1];
            let compressed_size = path.metadata().map_or(0, |meta| meta.len());
            STORAGE_SIZE
                .with_label_values(&["data", stream_name, "parquet"])
                .add(compressed_size as i64);
            EVENTS_STORAGE_SIZE_DATE
                .with_label_values(&["data", stream_name, "parquet", file_date_part])
                .add(compressed_size as i64);
            LIFETIME_EVENTS_STORAGE_SIZE
                .with_label_values(&["data", stream_name, "parquet"])
                .add(compressed_size as i64);

            let absolute_path = self
                .absolute_url(
                    RelativePath::from_path(&stream_relative_path).expect("valid relative path"),
                )
                .to_string();
            let store = PARSEABLE.storage().get_object_store();
            let manifest = catalog::create_from_parquet_file(absolute_path.clone(), &path)?;
            catalog::update_snapshot(store, stream_name, manifest).await?;

            if let Err(e) = remove_file(path) {
                warn!("Failed to remove staged file: {e}");
            }
        }

        for path in stream.schema_files() {
            let file = File::open(&path)?;
            let schema: Schema = serde_json::from_reader(file)?;
            commit_schema_to_storage(stream_name, schema).await?;
            if let Err(e) = remove_file(path) {
                warn!("Failed to remove staged file: {e}");
            }
        }

        Ok(())
    }
}

pub fn sync_all_streams(joinset: &mut JoinSet<Result<(), ObjectStorageError>>) {
    let object_store = PARSEABLE.storage.get_object_store();
    for stream_name in PARSEABLE.streams.list() {
        let object_store = object_store.clone();
        joinset.spawn(async move {
            let start = Instant::now();
            info!("Starting object_store_sync for stream- {stream_name}");
            let result = object_store.upload_files_from_staging(&stream_name).await;
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

const MAX_CONCURRENT_FIELD_STATS: usize = 10;

#[derive(Serialize, Debug)]
struct DistinctStat {
    distinct_value: String,
    count: i64,
}

#[derive(Serialize, Debug)]
struct FieldStat {
    field_name: String,
    count: i64,
    distinct_count: i64,
    distinct_stats: Vec<DistinctStat>,
}

#[derive(Serialize, Debug)]
struct DatasetStats {
    dataset_name: String,
    field_stats: Vec<FieldStat>,
}

/// Calculates field statistics for the stream and pushes them to the internal stats dataset.
/// This function creates a new internal stream for stats if it doesn't exist.
/// It collects statistics for each field in the stream
async fn calculate_field_stats(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
) -> Result<(), PostError> {
    let log_source_entry = LogSourceEntry::new(LogSource::Json, HashSet::new());
    PARSEABLE
        .create_stream_if_not_exists(
            DATASET_STATS_STREAM_NAME,
            StreamType::Internal,
            Some(&"dataset_name".to_string()),
            vec![log_source_entry],
        )
        .await?;
    let ctx = SessionContext::new_with_state(QUERY_SESSION_STATE.clone());
    let ctx_table_name = format!("{}_{}", stream_name, parquet_path.display());
    ctx.register_parquet(
        &ctx_table_name,
        parquet_path.to_str().expect("valid path"),
        ParquetReadOptions::default(),
    )
    .await
    .map_err(|e| PostError::Invalid(e.into()))?;
    let field_stats = collect_all_field_stats(&ctx_table_name, &ctx, schema).await;
    drop(ctx);

    let stats = DatasetStats {
        dataset_name: stream_name.to_string(),
        field_stats,
    };
    if stats.field_stats.is_empty() {
        return Ok(());
    }
    let stats_value =
        serde_json::to_value(&stats).map_err(|e| ObjectStorageError::Invalid(e.into()))?;

    flatten_and_push_logs(
        stats_value,
        DATASET_STATS_STREAM_NAME,
        &LogSource::Json,
        &HashMap::new(),
    )
    .await?;
    Ok(())
}

/// Collects statistics for all fields in the stream.
/// Returns a vector of `FieldStat` for each field with non-zero count.
/// Uses `buffer_unordered` to run up to `MAX_CONCURRENT_FIELD_STATS` queries concurrently.
async fn collect_all_field_stats(
    stream_name: &str,
    ctx: &SessionContext,
    schema: &Schema,
) -> Vec<FieldStat> {
    // Collect field names into an owned Vec<String> to avoid lifetime issues
    let field_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let field_futures = field_names.into_iter().map(|field_name| {
        let ctx = ctx.clone();
        async move { calculate_single_field_stats(ctx, stream_name, &field_name).await }
    });

    futures::stream::iter(field_futures)
        .buffer_unordered(MAX_CONCURRENT_FIELD_STATS)
        .filter_map(std::future::ready)
        .collect::<Vec<_>>()
        .await
}

/// Calculates statistics for a single field in the stream.
/// Returns `None` if the count query returns 0.
async fn calculate_single_field_stats(
    ctx: SessionContext,
    stream_name: &str,
    field_name: &str,
) -> Option<FieldStat> {
    let count = query_single_i64(
        &ctx,
        &format!("select count(\"{field_name}\") as count from \"{stream_name}\""),
    )
    .await?;
    if count == 0 {
        return None;
    }

    let distinct_count = query_single_i64(
        &ctx,
        &format!(
            "select COUNT(DISTINCT \"{field_name}\") as distinct_count from \"{stream_name}\""
        ),
    )
    .await?;
    let distinct_stats = query_distinct_stats(&ctx, stream_name, field_name).await;
    Some(FieldStat {
        field_name: field_name.to_string(),
        count,
        distinct_count,
        distinct_stats,
    })
}

/// Queries a single integer value from the DataFusion context.
/// Returns `None` if the query fails or returns no rows.
/// This is used for fetching record count for a field and distinct count.
async fn query_single_i64(ctx: &SessionContext, sql: &str) -> Option<i64> {
    let df = ctx.sql(sql).await.ok()?;
    let mut stream = df.execute_stream().await.ok()?;
    let mut count = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.ok()?;
        if batch.num_rows() == 0 {
            return None;
        }
        let array = batch.column(0).as_any().downcast_ref::<Int64Array>()?;
        count += array.value(0);
    }
    Some(count)
}

/// Helper function to format an Arrow value at a given index into a string.
/// Handles null values and different data types like String, Int64, Float64, Timestamp, Date32, and Boolean.
fn format_arrow_value(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            String::from_utf8_lossy(arr.value(idx)).to_string()
        }
        DataType::BinaryView => {
            let arr = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();
            String::from_utf8_lossy(arr.value(idx)).to_string()
        }
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let timestamp = arr.value(idx);
            DateTime::from_timestamp_millis(timestamp)
                .map(|dt| dt.to_string())
                .unwrap_or_else(|| "INVALID_TIMESTAMP".to_string())
        }
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::Null => "NULL".to_string(),
        _ => {
            warn!(
                "Unsupported array type for statistics: {:?}",
                array.data_type()
            );
            "UNSUPPORTED".to_string()
        }
    }
}

/// This function is used to fetch distinct values and their counts for a field in the stream.
/// Returns a vector of `DistinctStat` containing distinct values and their counts.
/// The query groups by the field and orders by the count in descending order, limiting the results to `PARSEABLE.options.max_field_statistics`.
async fn query_distinct_stats(
    ctx: &SessionContext,
    stream_name: &str,
    field_name: &str,
) -> Vec<DistinctStat> {
    let sql = format!(
        "select count(*) as distinct_count, \"{field_name}\" from \"{stream_name}\" group by \"{field_name}\" order by distinct_count desc limit {}",
        PARSEABLE.options.max_field_statistics
    );
    let mut distinct_stats = Vec::new();
    if let Ok(df) = ctx.sql(&sql).await {
        if let Ok(batches) = df.collect().await {
            for rb in batches {
                let Some(counts) = rb.column(0).as_any().downcast_ref::<Int64Array>() else {
                    warn!("Unexpected type for count column in stats query");
                    continue;
                };
                let values = rb.column(1).as_ref();
                for i in 0..rb.num_rows() {
                    let value = format_arrow_value(values, i);
                    distinct_stats.push(DistinctStat {
                        distinct_value: value,
                        count: counts.value(i),
                    });
                }
            }
        }
    }
    distinct_stats
}

pub async fn commit_schema_to_storage(
    stream_name: &str,
    schema: Schema,
) -> Result<(), ObjectStorageError> {
    let storage = PARSEABLE.storage().get_object_store();
    let stream_schema = storage.get_schema(stream_name).await?;
    let new_schema = Schema::try_merge(vec![schema, stream_schema]).unwrap();
    storage.put_schema(stream_name, &new_schema).await
}

#[inline(always)]
pub fn to_bytes(any: &(impl ?Sized + serde::Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}

pub fn schema_path(stream_name: &str) -> RelativePathBuf {
    if PARSEABLE.options.mode == Mode::Ingest {
        let id = INGESTOR_META
            .get()
            .unwrap_or_else(|| panic!("{}", INGESTOR_EXPECT))
            .get_node_id();
        let file_name = format!(".ingestor.{id}{SCHEMA_FILE_NAME}");

        RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY, &file_name])
    } else {
        RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY, SCHEMA_FILE_NAME])
    }
}

#[inline(always)]
pub fn stream_json_path(stream_name: &str) -> RelativePathBuf {
    if PARSEABLE.options.mode == Mode::Ingest {
        let id = INGESTOR_META
            .get()
            .unwrap_or_else(|| panic!("{}", INGESTOR_EXPECT))
            .get_node_id();
        let file_name = format!(".ingestor.{id}{STREAM_METADATA_FILE_NAME}",);
        RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY, &file_name])
    } else {
        RelativePathBuf::from_iter([
            stream_name,
            STREAM_ROOT_DIRECTORY,
            STREAM_METADATA_FILE_NAME,
        ])
    }
}

/// if dashboard_id is an empty str it should not append it to the rel path
#[inline(always)]
pub fn dashboard_path(user_id: &str, dashboard_file_name: &str) -> RelativePathBuf {
    RelativePathBuf::from_iter([USERS_ROOT_DIR, user_id, DASHBOARDS_DIR, dashboard_file_name])
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
pub fn alert_json_path(alert_id: Ulid) -> RelativePathBuf {
    RelativePathBuf::from_iter([ALERTS_ROOT_DIRECTORY, &format!("{alert_id}.json")])
}

#[inline(always)]
pub fn manifest_path(prefix: &str) -> RelativePathBuf {
    match &PARSEABLE.options.mode {
        Mode::Ingest => {
            let id = INGESTOR_META
                .get()
                .unwrap_or_else(|| panic!("{}", INGESTOR_EXPECT))
                .get_node_id();
            let manifest_file_name = format!("ingestor.{id}.{MANIFEST_FILE}");
            RelativePathBuf::from_iter([prefix, &manifest_file_name])
        }
        _ => RelativePathBuf::from_iter([prefix, MANIFEST_FILE]),
    }
}
