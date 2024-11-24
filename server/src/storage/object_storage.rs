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

use super::{
    retention::Retention, staging::convert_disk_files_to_parquet, LogStream, ObjectStorageError,
    ObjectStoreFormat, Permisssion, StorageDir, StorageMetadata,
};
use super::{
    ALERT_FILE_NAME, MANIFEST_FILE, PARSEABLE_METADATA_FILE_NAME, PARSEABLE_ROOT_DIRECTORY,
    SCHEMA_FILE_NAME, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
};

use crate::handlers::http::modal::ingest_server::INGESTOR_META;
use crate::handlers::http::users::{DASHBOARDS_DIR, FILTER_DIR, USERS_ROOT_DIR};
use crate::metrics::{EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_STORAGE_SIZE};
use crate::option::Mode;
use crate::{
    alerts::Alerts,
    catalog::{self, manifest::Manifest, snapshot::Snapshot},
    localcache::LocalCacheManager,
    metadata::STREAM_INFO,
    metrics::{storage::StorageMetrics, STORAGE_SIZE},
    option::CONFIG,
    stats::FullStats,
};

use actix_web_prometheus::PrometheusMetrics;
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use datafusion::{datasource::listing::ListingTableUrl, execution::runtime_env::RuntimeConfig};
use itertools::Itertools;
use relative_path::RelativePath;
use relative_path::RelativePathBuf;

use std::collections::BTreeMap;
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

pub trait ObjectStorageProvider: StorageMetrics + std::fmt::Debug {
    fn get_datafusion_runtime(&self) -> RuntimeConfig;
    fn get_object_store(&self) -> Arc<dyn ObjectStorage + Send>;
    fn get_endpoint(&self) -> String;
    fn register_store_metrics(&self, handler: &PrometheusMetrics);
}

#[async_trait]
pub trait ObjectStorage: Sync + 'static {
    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError>;
    // TODO: make the filter function optional as we may want to get all objects
    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_fun: Box<dyn Fn(String) -> bool + Send>,
    ) -> Result<Vec<Bytes>, ObjectStorageError>;
    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError>;
    async fn delete_prefix(&self, path: &RelativePath) -> Result<(), ObjectStorageError>;
    async fn check(&self) -> Result<(), ObjectStorageError>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError>;
    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError>;
    async fn list_old_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError>;
    async fn list_dirs(&self) -> Result<Vec<String>, ObjectStorageError>;
    async fn get_all_saved_filters(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError>;
    async fn get_all_dashboards(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError>;
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
    async fn try_delete_ingestor_meta(
        &self,
        ingestor_filename: String,
    ) -> Result<(), ObjectStorageError>;
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

    #[allow(clippy::too_many_arguments)]
    async fn create_stream(
        &self,
        stream_name: &str,
        time_partition: &str,
        time_partition_limit: &str,
        custom_partition: &str,
        static_schema_flag: &str,
        schema: Arc<Schema>,
        stream_type: &str,
    ) -> Result<String, ObjectStorageError> {
        let mut format = ObjectStoreFormat::default();
        format.set_id(CONFIG.parseable.username.clone());
        let permission = Permisssion::new(CONFIG.parseable.username.clone());
        format.permissions = vec![permission];
        format.created_at = Local::now().to_rfc3339();
        format.stream_type = Some(stream_type.to_string());
        if time_partition.is_empty() {
            format.time_partition = None;
        } else {
            format.time_partition = Some(time_partition.to_string());
        }
        if time_partition_limit.is_empty() {
            format.time_partition_limit = None;
        } else {
            format.time_partition_limit = Some(time_partition_limit.to_string());
        }
        if custom_partition.is_empty() {
            format.custom_partition = None;
        } else {
            format.custom_partition = Some(custom_partition.to_string());
        }
        if static_schema_flag != "true" {
            format.static_schema_flag = None;
        } else {
            format.static_schema_flag = Some(static_schema_flag.to_string());
        }
        let format_json = to_bytes(&format);
        self.put_object(&schema_path(stream_name), to_bytes(&schema))
            .await?;

        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

        Ok(format.created_at)
    }

    async fn update_time_partition_limit_in_stream(
        &self,
        stream_name: &str,
        time_partition_limit: &str,
    ) -> Result<(), ObjectStorageError> {
        let mut format = self.get_object_store_format(stream_name).await?;
        if time_partition_limit.is_empty() {
            format.time_partition_limit = None;
        } else {
            format.time_partition_limit = Some(time_partition_limit.to_string());
        }
        let format_json = to_bytes(&format);
        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

        Ok(())
    }

    async fn update_custom_partition_in_stream(
        &self,
        stream_name: &str,
        custom_partition: &str,
    ) -> Result<(), ObjectStorageError> {
        let mut format = self.get_object_store_format(stream_name).await?;
        if custom_partition.is_empty() {
            format.custom_partition = None;
        } else {
            format.custom_partition = Some(custom_partition.to_string());
        }
        let format_json = to_bytes(&format);
        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

        Ok(())
    }

    async fn put_alerts(
        &self,
        stream_name: &str,
        alerts: &Alerts,
    ) -> Result<(), ObjectStorageError> {
        self.put_object(&alert_json_path(stream_name), to_bytes(alerts))
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
        let stats =
            serde_json::to_value(retention).expect("rentention tasks are perfectly serializable");
        let mut stream_metadata: serde_json::Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        stream_metadata["retention"] = stats;

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

    async fn get_alerts(&self, stream_name: &str) -> Result<Alerts, ObjectStorageError> {
        match self.get_object(&alert_json_path(stream_name)).await {
            Ok(alerts) => {
                if let Ok(alerts) = serde_json::from_slice(&alerts) {
                    Ok(alerts)
                } else {
                    log::error!("Incompatible alerts found for stream - {stream_name}. Refer https://www.parseable.io/docs/alerts for correct alert config.");
                    Ok(Alerts::default())
                }
            }
            Err(e) => match e {
                ObjectStorageError::NoSuchKey(_) => Ok(Alerts::default()),
                e => Err(e),
            },
        }
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

                if CONFIG.parseable.mode == Mode::Ingest {
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
            Ok(bytes) => Ok(Some(
                serde_json::from_slice(&bytes).expect("manifest is valid json"),
            )),
            Err(err) => {
                if matches!(err, ObjectStorageError::NoSuchKey(_)) {
                    Ok(None)
                } else {
                    Err(err)
                }
            }
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

    async fn sync(&self, shutdown_signal: bool) -> Result<(), ObjectStorageError> {
        if !Path::new(&CONFIG.staging_dir()).exists() {
            return Ok(());
        }

        let streams = STREAM_INFO.list_streams();

        let cache_manager = LocalCacheManager::global();
        let mut cache_updates: HashMap<&String, Vec<_>> = HashMap::new();

        for stream in &streams {
            let cache_enabled = STREAM_INFO
                .get_cache_enabled(stream)
                .map_err(|err| ObjectStorageError::UnhandledError(Box::new(err)))?;
            let time_partition = STREAM_INFO
                .get_time_partition(stream)
                .map_err(|err| ObjectStorageError::UnhandledError(Box::new(err)))?;
            let custom_partition = STREAM_INFO
                .get_custom_partition(stream)
                .map_err(|err| ObjectStorageError::UnhandledError(Box::new(err)))?;
            let dir = StorageDir::new(stream);
            let schema = convert_disk_files_to_parquet(
                stream,
                &dir,
                time_partition,
                custom_partition.clone(),
                shutdown_signal,
            )
            .map_err(|err| ObjectStorageError::UnhandledError(Box::new(err)))?;

            if let Some(schema) = schema {
                let static_schema_flag = STREAM_INFO
                    .get_static_schema_flag(stream)
                    .map_err(|err| ObjectStorageError::UnhandledError(Box::new(err)))?;
                if static_schema_flag.is_none() {
                    commit_schema_to_storage(stream, schema).await?;
                }
            }

            let parquet_files = dir.parquet_files();
            for file in parquet_files {
                let filename = file
                    .file_name()
                    .expect("only parquet files are returned by iterator")
                    .to_str()
                    .expect("filename is valid string");

                let mut file_date_part = filename.split('.').collect::<Vec<&str>>()[0];
                file_date_part = file_date_part.split('=').collect::<Vec<&str>>()[1];
                let compressed_size = file.metadata().map_or(0, |meta| meta.len());
                STORAGE_SIZE
                    .with_label_values(&["data", stream, "parquet"])
                    .add(compressed_size as i64);
                EVENTS_STORAGE_SIZE_DATE
                    .with_label_values(&["data", stream, "parquet", file_date_part])
                    .add(compressed_size as i64);
                LIFETIME_EVENTS_STORAGE_SIZE
                    .with_label_values(&["data", stream, "parquet"])
                    .add(compressed_size as i64);
                let mut file_suffix = str::replacen(filename, ".", "/", 3);

                let custom_partition_clone = custom_partition.clone();
                if custom_partition_clone.is_some() {
                    let custom_partition_fields = custom_partition_clone.unwrap();
                    let custom_partition_list =
                        custom_partition_fields.split(',').collect::<Vec<&str>>();
                    file_suffix =
                        str::replacen(filename, ".", "/", 3 + custom_partition_list.len());
                }

                let stream_relative_path = format!("{stream}/{file_suffix}");

                // Try uploading the file, handle potential errors without breaking the loop
                if let Err(e) = self.upload_file(&stream_relative_path, &file).await {
                    log::error!("Failed to upload file {}: {:?}", filename, e);
                    continue; // Skip to the next file
                }

                let absolute_path = self
                    .absolute_url(RelativePath::from_path(&stream_relative_path).unwrap())
                    .to_string();
                let store = CONFIG.storage().get_object_store();
                let manifest =
                    catalog::create_from_parquet_file(absolute_path.clone(), &file).unwrap();
                catalog::update_snapshot(store, stream, manifest).await?;
                if cache_enabled && cache_manager.is_some() {
                    cache_updates
                        .entry(stream)
                        .or_default()
                        .push((absolute_path, file));
                } else {
                    let _ = fs::remove_file(file);
                }
            }
        }

        // Cache management logic
        if let Some(manager) = cache_manager {
            let cache_updates = cache_updates
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect_vec();

            tokio::spawn(async move {
                for (stream, files) in cache_updates {
                    for (storage_path, file) in files {
                        if let Err(e) = manager
                            .move_to_cache(&stream, storage_path, file.to_owned())
                            .await
                        {
                            log::error!("Failed to move file to cache: {:?}", e);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    // pick a better name
    fn get_bucket_name(&self) -> String;
}

pub async fn commit_schema_to_storage(
    stream_name: &str,
    schema: Schema,
) -> Result<(), ObjectStorageError> {
    let storage = CONFIG.storage().get_object_store();
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
    match CONFIG.parseable.mode {
        Mode::Ingest => {
            let file_name = format!(
                ".ingestor.{}{}",
                INGESTOR_META.ingestor_id.clone(),
                SCHEMA_FILE_NAME
            );

            RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY, &file_name])
        }
        Mode::All | Mode::Query => {
            RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY, SCHEMA_FILE_NAME])
        }
    }
}

#[inline(always)]
pub fn stream_json_path(stream_name: &str) -> RelativePathBuf {
    match &CONFIG.parseable.mode {
        Mode::Ingest => {
            let file_name = format!(
                ".ingestor.{}{}",
                INGESTOR_META.get_ingestor_id(),
                STREAM_METADATA_FILE_NAME
            );
            RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY, &file_name])
        }
        Mode::Query | Mode::All => RelativePathBuf::from_iter([
            stream_name,
            STREAM_ROOT_DIRECTORY,
            STREAM_METADATA_FILE_NAME,
        ]),
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
fn alert_json_path(stream_name: &str) -> RelativePathBuf {
    RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY, ALERT_FILE_NAME])
}

#[inline(always)]
pub fn manifest_path(prefix: &str) -> RelativePathBuf {
    if CONFIG.parseable.mode == Mode::Ingest {
        let manifest_file_name = format!(
            "ingestor.{}.{}",
            INGESTOR_META.get_ingestor_id(),
            MANIFEST_FILE
        );
        RelativePathBuf::from_iter([prefix, &manifest_file_name])
    } else {
        RelativePathBuf::from_iter([prefix, MANIFEST_FILE])
    }
}

#[inline(always)]
pub fn ingestor_metadata_path(id: Option<&str>) -> RelativePathBuf {
    if let Some(id) = id {
        return RelativePathBuf::from_iter([
            PARSEABLE_ROOT_DIRECTORY,
            &format!("ingestor.{}.json", id),
        ]);
    }

    RelativePathBuf::from_iter([
        PARSEABLE_ROOT_DIRECTORY,
        &format!("ingestor.{}.json", INGESTOR_META.get_ingestor_id()),
    ])
}
