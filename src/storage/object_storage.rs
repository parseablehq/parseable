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
use tokio::task;
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
        let mut stats_calculated = false;
        let stream = PARSEABLE.get_or_create_stream(stream_name);
        let custom_partition = stream.get_custom_partition();
        let schema = stream.get_schema();
        for path in stream.parquet_files() {
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

            // If stats collection is enabled, calculate field stats
            if stream_name != DATASET_STATS_STREAM_NAME && PARSEABLE.options.collect_dataset_stats {
                let max_field_statistics = PARSEABLE.options.max_field_statistics;
                match calculate_field_stats(stream_name, &path, &schema, max_field_statistics).await
                {
                    Ok(stats) if stats => stats_calculated = true,
                    Err(err) => warn!(
                        "Error calculating field stats for stream {}: {}",
                        stream_name, err
                    ),
                    _ => {}
                }
            }
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

        if stats_calculated {
            // perform local sync for the `pstats` dataset
            task::spawn(async move {
                if let Ok(stats_stream) = PARSEABLE.get_stream(DATASET_STATS_STREAM_NAME) {
                    if let Err(err) = stats_stream.flush_and_convert(false, false) {
                        error!("Failed in local sync for dataset stats stream: {err}");
                    }
                }
            });
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
    max_field_statistics: usize,
) -> Result<bool, PostError> {
    let field_stats = {
        let ctx = SessionContext::new_with_state(QUERY_SESSION_STATE.clone());
        let table_name = Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().expect("valid path"),
            ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| PostError::Invalid(e.into()))?;

        collect_all_field_stats(&table_name, &ctx, schema, max_field_statistics).await
    };
    let mut stats_calculated = false;
    let stats = DatasetStats {
        dataset_name: stream_name.to_string(),
        field_stats,
    };
    if stats.field_stats.is_empty() {
        return Ok(stats_calculated);
    }
    stats_calculated = true;
    let stats_value =
        serde_json::to_value(&stats).map_err(|e| ObjectStorageError::Invalid(e.into()))?;
    let log_source_entry = LogSourceEntry::new(LogSource::Json, HashSet::new());
    PARSEABLE
        .create_stream_if_not_exists(
            DATASET_STATS_STREAM_NAME,
            StreamType::Internal,
            Some(&"dataset_name".into()),
            vec![log_source_entry],
        )
        .await?;
    flatten_and_push_logs(
        stats_value,
        DATASET_STATS_STREAM_NAME,
        &LogSource::Json,
        &HashMap::new(),
    )
    .await?;
    Ok(stats_calculated)
}

/// Collects statistics for all fields in the stream.
/// Returns a vector of `FieldStat` for each field with non-zero count.
/// Uses `buffer_unordered` to run up to `MAX_CONCURRENT_FIELD_STATS` queries concurrently.
async fn collect_all_field_stats(
    stream_name: &str,
    ctx: &SessionContext,
    schema: &Schema,
    max_field_statistics: usize,
) -> Vec<FieldStat> {
    // Collect field names into an owned Vec<String> to avoid lifetime issues
    let field_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let field_futures = field_names.into_iter().map(|field_name| {
        let ctx = ctx.clone();
        async move {
            calculate_single_field_stats(ctx, stream_name, &field_name, max_field_statistics).await
        }
    });

    futures::stream::iter(field_futures)
        .buffer_unordered(MAX_CONCURRENT_FIELD_STATS)
        .filter_map(std::future::ready)
        .collect::<Vec<_>>()
        .await
}

/// This function is used to fetch distinct values and their counts for a field in the stream.
/// Returns a vector of `DistinctStat` containing distinct values and their counts.
/// The query groups by the field and orders by the count in descending order, limiting the results to `PARSEABLE.options.max_field_statistics`.
async fn calculate_single_field_stats(
    ctx: SessionContext,
    stream_name: &str,
    field_name: &str,
    max_field_statistics: usize,
) -> Option<FieldStat> {
    let mut total_count = 0;
    let mut distinct_count = 0;
    let mut distinct_stats = Vec::new();

    let combined_sql = get_stats_sql(stream_name, field_name, max_field_statistics);
    match ctx.sql(&combined_sql).await {
        Ok(df) => {
            let mut stream = match df.execute_stream().await {
                Ok(stream) => stream,
                Err(e) => {
                    warn!("Failed to execute distinct stats query: {e}");
                    return None; // Return empty if query fails
                }
            };
            while let Some(batch_result) = stream.next().await {
                let rb = match batch_result {
                    Ok(batch) => batch,
                    Err(e) => {
                        warn!("Failed to fetch batch in distinct stats query: {e}");
                        continue; // Skip this batch if there's an error
                    }
                };
                let total_count_array = rb.column(0).as_any().downcast_ref::<Int64Array>()?;
                let distinct_count_array = rb.column(1).as_any().downcast_ref::<Int64Array>()?;

                total_count = total_count_array.value(0);
                distinct_count = distinct_count_array.value(0);
                if distinct_count == 0 {
                    return None;
                }

                let field_value_array = rb.column(2).as_ref();
                let value_count_array = rb.column(3).as_any().downcast_ref::<Int64Array>()?;

                for i in 0..rb.num_rows() {
                    let value = format_arrow_value(field_value_array, i);
                    let count = value_count_array.value(i);

                    distinct_stats.push(DistinctStat {
                        distinct_value: value,
                        count,
                    });
                }
            }
        }
        Err(e) => {
            warn!("Failed to execute distinct stats query for field: {field_name}, error: {e}");
            return None;
        }
    }
    Some(FieldStat {
        field_name: field_name.to_string(),
        count: total_count,
        distinct_count,
        distinct_stats,
    })
}

fn get_stats_sql(stream_name: &str, field_name: &str, max_field_statistics: usize) -> String {
    let escaped_field_name = field_name.replace('"', "\"\"");
    let escaped_stream_name = stream_name.replace('"', "\"\"");

    format!(
        r#"
        WITH field_groups AS (
            SELECT 
                "{escaped_field_name}" as field_value,
                COUNT(*) as value_count
            FROM "{escaped_stream_name}"
            GROUP BY "{escaped_field_name}"
        ),
        field_summary AS (
            SELECT 
                field_value,
                value_count,
                SUM(value_count) OVER () as total_count,
                COUNT(*) OVER () as distinct_count,
                ROW_NUMBER() OVER (ORDER BY value_count DESC) as rn
            FROM field_groups
        )
        SELECT 
            total_count,
            distinct_count,
            field_value,
            value_count
        FROM field_summary 
        WHERE rn <= {max_field_statistics}
        ORDER BY value_count DESC
        "#
    )
}

macro_rules! try_downcast {
    ($ty:ty, $arr:expr, $body:expr) => {
        if let Some(arr) = $arr.as_any().downcast_ref::<$ty>() {
            $body(arr)
        } else {
            warn!(
                "Expected {} for {:?}, but found {:?}",
                stringify!($ty),
                $arr.data_type(),
                $arr.data_type()
            );
            "UNSUPPORTED".to_string()
        }
    };
}

/// Function to format an Arrow value at a given index into a string.
/// Handles null values and different data types by downcasting the array to the appropriate type.
fn format_arrow_value(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Utf8 => try_downcast!(StringArray, array, |arr: &StringArray| arr
            .value(idx)
            .to_string()),
        DataType::Utf8View => try_downcast!(StringViewArray, array, |arr: &StringViewArray| arr
            .value(idx)
            .to_string()),
        DataType::Binary => try_downcast!(BinaryArray, array, |arr: &BinaryArray| {
            String::from_utf8_lossy(arr.value(idx)).to_string()
        }),
        DataType::BinaryView => try_downcast!(BinaryViewArray, array, |arr: &BinaryViewArray| {
            String::from_utf8_lossy(arr.value(idx)).to_string()
        }),
        DataType::Int64 => try_downcast!(Int64Array, array, |arr: &Int64Array| arr
            .value(idx)
            .to_string()),
        DataType::Int32 => try_downcast!(
            arrow_array::Int32Array,
            array,
            |arr: &arrow_array::Int32Array| arr.value(idx).to_string()
        ),
        DataType::Int16 => try_downcast!(
            arrow_array::Int16Array,
            array,
            |arr: &arrow_array::Int16Array| arr.value(idx).to_string()
        ),
        DataType::Int8 => try_downcast!(
            arrow_array::Int8Array,
            array,
            |arr: &arrow_array::Int8Array| arr.value(idx).to_string()
        ),
        DataType::UInt64 => try_downcast!(
            arrow_array::UInt64Array,
            array,
            |arr: &arrow_array::UInt64Array| arr.value(idx).to_string()
        ),
        DataType::UInt32 => try_downcast!(
            arrow_array::UInt32Array,
            array,
            |arr: &arrow_array::UInt32Array| arr.value(idx).to_string()
        ),
        DataType::UInt16 => try_downcast!(
            arrow_array::UInt16Array,
            array,
            |arr: &arrow_array::UInt16Array| arr.value(idx).to_string()
        ),
        DataType::UInt8 => try_downcast!(
            arrow_array::UInt8Array,
            array,
            |arr: &arrow_array::UInt8Array| arr.value(idx).to_string()
        ),
        DataType::Float64 => try_downcast!(Float64Array, array, |arr: &Float64Array| arr
            .value(idx)
            .to_string()),
        DataType::Float32 => try_downcast!(
            arrow_array::Float32Array,
            array,
            |arr: &arrow_array::Float32Array| arr.value(idx).to_string()
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => try_downcast!(
            TimestampMillisecondArray,
            array,
            |arr: &TimestampMillisecondArray| {
                let timestamp = arr.value(idx);
                chrono::DateTime::from_timestamp_millis(timestamp)
                    .map(|dt| dt.to_string())
                    .unwrap_or_else(|| "INVALID_TIMESTAMP".to_string())
            }
        ),
        DataType::Date32 => try_downcast!(Date32Array, array, |arr: &Date32Array| arr
            .value(idx)
            .to_string()),
        DataType::Boolean => try_downcast!(BooleanArray, array, |arr: &BooleanArray| if arr
            .value(idx)
        {
            "true".to_string()
        } else {
            "false".to_string()
        }),
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
#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, sync::Arc};

    use arrow_array::{
        BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use temp_dir::TempDir;
    use ulid::Ulid;

    use crate::storage::object_storage::calculate_single_field_stats;

    async fn create_test_parquet_with_data() -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_data.parquet");
        let schema = Arc::new(create_test_schema());

        // Create test data with various patterns
        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let name_array = StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            Some("Alice"),
            Some("Charlie"),
            Some("Alice"),
            Some("Bob"),
            Some("David"),
            None,
            Some("Eve"),
            Some("Frank"),
        ]);
        let score_array = Float64Array::from(vec![
            Some(95.5),
            Some(87.2),
            Some(95.5),
            Some(78.9),
            Some(92.1),
            Some(88.8),
            Some(91.0),
            None,
            Some(89.5),
            Some(94.2),
        ]);
        let active_array = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            None, // null value
            Some(false),
            Some(true),
        ]);
        let timestamp_array = TimestampMillisecondArray::from(vec![
            Some(1640995200000),
            Some(1640995260000),
            Some(1640995200000),
            Some(1640995320000),
            Some(1640995380000),
            Some(1640995440000),
            Some(1640995500000),
            None,
            Some(1640995560000),
            Some(1640995620000),
        ]);
        // Field with single value (all same)
        let single_value_array = StringArray::from(vec![
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(score_array),
                Arc::new(active_array),
                Arc::new(timestamp_array),
                Arc::new(single_value_array),
            ],
        )
        .unwrap();
        let props = WriterProperties::new();
        let mut parquet_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path.clone())
            .unwrap();
        let mut writer =
            ArrowWriter::try_new(&mut parquet_file, schema.clone(), Some(props.clone())).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        (temp_dir, file_path)
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("single_value", DataType::Utf8, true),
        ])
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_multiple_values() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let random_suffix = Ulid::new().to_string();
        let ctx = SessionContext::new();
        ctx.register_parquet(
            &random_suffix,
            parquet_path.to_str().expect("valid path"),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test name field with multiple distinct values and different frequencies
        let result = calculate_single_field_stats(ctx.clone(), &random_suffix, "name", 50).await;
        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "name");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 7);
        assert_eq!(stats.distinct_stats.len(), 7);

        // Verify ordering by count (descending)
        assert!(stats.distinct_stats[0].count >= stats.distinct_stats[1].count);
        assert!(stats.distinct_stats[1].count >= stats.distinct_stats[2].count);

        // Verify specific counts
        let alice_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "Alice");
        assert!(alice_stat.is_some());
        assert_eq!(alice_stat.unwrap().count, 3);

        let bob_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "Bob");
        assert!(bob_stat.is_some());
        assert_eq!(bob_stat.unwrap().count, 2);

        let charlie_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "Charlie");
        assert!(charlie_stat.is_some());
        assert_eq!(charlie_stat.unwrap().count, 1);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_numeric_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();

        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test score field (Float64)
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "score", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "score");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 9);

        // Verify that 95.5 appears twice (should be first due to highest count)
        let highest_count_stat = &stats.distinct_stats[0];
        assert_eq!(highest_count_stat.distinct_value, "95.5");
        assert_eq!(highest_count_stat.count, 2);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_boolean_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test active field (Boolean)
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "active", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "active");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 3);
        assert_eq!(stats.distinct_stats.len(), 3);

        assert_eq!(stats.distinct_stats[0].distinct_value, "true");
        assert_eq!(stats.distinct_stats[0].count, 6);
        assert_eq!(stats.distinct_stats[1].distinct_value, "false");
        assert_eq!(stats.distinct_stats[1].count, 3);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_timestamp_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test created_at field (Timestamp)
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "created_at", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "created_at");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 9);

        // Verify that the duplicate timestamp appears twice
        let duplicate_timestamp = &stats.distinct_stats[0];
        assert_eq!(duplicate_timestamp.count, 2);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_single_value_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test field with single distinct value
        let result =
            calculate_single_field_stats(ctx.clone(), &table_name, "single_value", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "single_value");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 1);
        assert_eq!(stats.distinct_stats.len(), 1);
        assert_eq!(stats.distinct_stats[0].distinct_value, "constant");
        assert_eq!(stats.distinct_stats[0].count, 10);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_nonexistent_table() {
        let ctx = SessionContext::new();

        // Test with non-existent table
        let result =
            calculate_single_field_stats(ctx.clone(), "non_existent_table", "field", 50).await;

        // Should return None due to SQL execution failure
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_nonexistent_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test with non-existent field
        let result =
            calculate_single_field_stats(ctx.clone(), &table_name, "non_existent_field", 50).await;

        // Should return None due to SQL execution failure
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_special_characters() {
        // Create a schema with field names containing special characters
        let schema = Arc::new(Schema::new(vec![
            Field::new("field with spaces", DataType::Utf8, true),
            Field::new("field\"with\"quotes", DataType::Utf8, true),
            Field::new("field'with'apostrophes", DataType::Utf8, true),
        ]));

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("special_chars.parquet");

        use parquet::arrow::AsyncArrowWriter;
        use tokio::fs::File;

        let space_array = StringArray::from(vec![Some("value1"), Some("value2"), Some("value1")]);
        let quote_array = StringArray::from(vec![Some("quote1"), Some("quote2"), Some("quote1")]);
        let apostrophe_array = StringArray::from(vec![Some("apos1"), Some("apos2"), Some("apos1")]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(space_array),
                Arc::new(quote_array),
                Arc::new(apostrophe_array),
            ],
        )
        .unwrap();

        let file = File::create(&file_path).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            file_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test field with spaces
        let result =
            calculate_single_field_stats(ctx.clone(), &table_name, "field with spaces", 50).await;
        assert!(result.is_some());
        let stats = result.unwrap();
        assert_eq!(stats.field_name, "field with spaces");
        assert_eq!(stats.count, 3);
        assert_eq!(stats.distinct_count, 2);

        // Test field with quotes
        let result =
            calculate_single_field_stats(ctx.clone(), &table_name, "field\"with\"quotes", 50).await;
        assert!(result.is_some());
        let stats = result.unwrap();
        assert_eq!(stats.field_name, "field\"with\"quotes");
        assert_eq!(stats.count, 3);
        assert_eq!(stats.distinct_count, 2);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_empty_table() {
        // Create empty table
        let schema = Arc::new(create_test_schema());
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty_data.parquet");

        use parquet::arrow::AsyncArrowWriter;
        use tokio::fs::File;

        let file = File::create(&file_path).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema.clone(), None).unwrap();

        // Create empty batch
        let empty_batch = RecordBatch::new_empty(schema.clone());
        writer.write(&empty_batch).await.unwrap();
        writer.close().await.unwrap();

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            file_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let result = calculate_single_field_stats(ctx.clone(), &table_name, "name", 50).await;
        assert!(result.unwrap().distinct_stats.is_empty());
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_streaming_behavior() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test that the function handles streaming properly by checking
        // that all data is collected correctly across multiple batches
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "name", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        // Verify that the streaming collected all the data
        let total_distinct_count: i64 = stats.distinct_stats.iter().map(|s| s.count).sum();
        assert_eq!(total_distinct_count, stats.count);

        // Verify that distinct_stats are properly ordered by count
        for i in 1..stats.distinct_stats.len() {
            assert!(stats.distinct_stats[i - 1].count >= stats.distinct_stats[i].count);
        }
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_large_dataset() {
        // Create a larger dataset to test streaming behavior
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, true),
        ]));

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("large_data.parquet");

        use parquet::arrow::AsyncArrowWriter;
        use tokio::fs::File;

        // Create 1000 rows with 10 distinct categories
        let ids: Vec<i64> = (0..1000).collect();
        let categories: Vec<Option<&str>> = (0..1000)
            .map(|i| {
                Some(match i % 10 {
                    0 => "cat_0",
                    1 => "cat_1",
                    2 => "cat_2",
                    3 => "cat_3",
                    4 => "cat_4",
                    5 => "cat_5",
                    6 => "cat_6",
                    7 => "cat_7",
                    8 => "cat_8",
                    _ => "cat_9",
                })
            })
            .collect();

        let id_array = Int64Array::from(ids);
        let category_array = StringArray::from(categories);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(category_array)],
        )
        .unwrap();

        let file = File::create(&file_path).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            file_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let result = calculate_single_field_stats(ctx.clone(), &table_name, "category", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.count, 1000);
        assert_eq!(stats.distinct_count, 10);
        assert_eq!(stats.distinct_stats.len(), 10);

        // Each category should appear 100 times
        for distinct_stat in &stats.distinct_stats {
            assert_eq!(distinct_stat.count, 100);
        }
    }
}
