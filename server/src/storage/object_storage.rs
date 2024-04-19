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
use crate::option::Mode;

use crate::{
    alerts::Alerts,
    catalog::{self, manifest::Manifest, snapshot::Snapshot},
    localcache::LocalCacheManager,
    metadata::STREAM_INFO,
    metrics::{storage::StorageMetrics, STORAGE_SIZE},
    option::CONFIG,
    stats::{self, Stats},
};

use actix_web_prometheus::PrometheusMetrics;
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{datasource::listing::ListingTableUrl, execution::runtime_env::RuntimeConfig};
use itertools::Itertools;
use relative_path::RelativePath;
use relative_path::RelativePathBuf;
use serde_json::Value;

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
    // want to make it more generic with a filter function
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
    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError>;
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

    async fn create_stream(
        &self,
        stream_name: &str,
        time_partition: &str,
        static_schema_flag: &str,
        schema: Arc<Schema>,
    ) -> Result<(), ObjectStorageError> {
        let mut format = ObjectStoreFormat::default();
        format.set_id(CONFIG.parseable.username.clone());
        let permission = Permisssion::new(CONFIG.parseable.username.clone());
        format.permissions = vec![permission];
        if time_partition.is_empty() {
            format.time_partition = None;
        } else {
            format.time_partition = Some(time_partition.to_string());
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

    async fn put_stats(&self, stream_name: &str, stats: &Stats) -> Result<(), ObjectStorageError> {
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

    async fn get_schema_on_server_start(
        &self,
        stream_name: &str,
    ) -> Result<Schema, ObjectStorageError> {
        // try get my schema
        // if fails get the base schema
        // put the schema to storage??
        let schema_path = schema_path(stream_name);
        let byte_data = match self.get_object(&schema_path).await {
            Ok(bytes) => bytes,
            Err(err) => {
                log::info!("{:?}", err);
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

    async fn get_stream_metadata(
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
                    config.stats = Stats::default();
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

    /// for future use
    async fn get_stats_for_first_time(
        &self,
        stream_name: &str,
    ) -> Result<Stats, ObjectStorageError> {
        let path = RelativePathBuf::from_iter([stream_name, STREAM_METADATA_FILE_NAME]);
        let stream_metadata = self.get_object(&path).await?;
        let stream_metadata: Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");
        let stats = &stream_metadata["stats"];

        let stats = serde_json::from_value(stats.clone()).unwrap_or_default();

        Ok(stats)
    }

    async fn get_stats(&self, stream_name: &str) -> Result<Stats, ObjectStorageError> {
        let stream_metadata = self.get_object(&stream_json_path(stream_name)).await?;
        let stream_metadata: Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        let stats = &stream_metadata["stats"];

        let stats = serde_json::from_value(stats.clone()).unwrap_or_default();

        Ok(stats)
    }

    async fn get_retention(&self, stream_name: &str) -> Result<Retention, ObjectStorageError> {
        let stream_metadata = self.get_object(&stream_json_path(stream_name)).await?;
        let stream_metadata: Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        let retention = stream_metadata
            .as_object()
            .expect("is object")
            .get("retention")
            .cloned();
        if let Some(retention) = retention {
            Ok(serde_json::from_value(retention)?)
        } else {
            Ok(Retention::default())
        }
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

    async fn stream_exists(&self, stream_name: &str) -> Result<bool, ObjectStorageError> {
        let res = self.get_object(&stream_json_path(stream_name)).await;
        match res {
            Ok(_) => Ok(true),
            Err(ObjectStorageError::NoSuchKey(_)) => Ok(false),
            Err(e) => Err(e),
        }
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
        let mut stream_meta = self.get_stream_metadata(stream).await?;
        stream_meta.snapshot = snapshot;
        self.put_object(&stream_json_path(stream), to_bytes(&stream_meta))
            .await
    }

    async fn sync(&self) -> Result<(), ObjectStorageError> {
        if !Path::new(&CONFIG.staging_dir()).exists() {
            return Ok(());
        }

        let streams = STREAM_INFO.list_streams();
        let mut stream_stats = HashMap::new();

        let cache_manager = LocalCacheManager::global();
        let mut cache_updates: HashMap<&String, Vec<_>> = HashMap::new();

        for stream in &streams {
            let cache_enabled = STREAM_INFO
                .cache_enabled(stream)
                .map_err(|err| ObjectStorageError::UnhandledError(Box::new(err)))?;
            let time_partition = STREAM_INFO
                .get_time_partition(stream)
                .map_err(|err| ObjectStorageError::UnhandledError(Box::new(err)))?;
            let dir = StorageDir::new(stream);
            let schema = convert_disk_files_to_parquet(stream, &dir, time_partition)
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
            parquet_files.iter().for_each(|file| {
                let compressed_size = file.metadata().map_or(0, |meta| meta.len());
                stream_stats
                    .entry(stream)
                    .and_modify(|size| *size += compressed_size)
                    .or_insert_with(|| compressed_size);
            });

            for file in parquet_files {
                let filename = file
                    .file_name()
                    .expect("only parquet files are returned by iterator")
                    .to_str()
                    .expect("filename is valid string");
                let file_suffix = str::replacen(filename, ".", "/", 3);
                let stream_relative_path = format!("{stream}/{file_suffix}");
                self.upload_file(&stream_relative_path, &file).await?;
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

        for (stream, compressed_size) in stream_stats {
            STORAGE_SIZE
                .with_label_values(&["data", stream, "parquet"])
                .add(compressed_size as i64);
            let stats = stats::get_current_stats(stream, "json");
            if let Some(stats) = stats {
                if let Err(e) = self.put_stats(stream, &stats).await {
                    log::warn!("Error updating stats to objectstore due to error [{}]", e);
                }
            }
        }

        if let Some(manager) = cache_manager {
            let cache_updates = cache_updates
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect_vec();

            tokio::spawn(async move {
                for (stream, files) in cache_updates {
                    for (storage_path, file) in files {
                        manager
                            .move_to_cache(&stream, storage_path, file.to_owned())
                            .await
                            .unwrap()
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
fn to_bytes(any: &(impl ?Sized + serde::Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}

#[inline(always)]
fn schema_path(stream_name: &str) -> RelativePathBuf {
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

/// path will be ".parseable/.parsable.json"
#[inline(always)]
pub fn parseable_json_path() -> RelativePathBuf {
    RelativePathBuf::from_iter([PARSEABLE_ROOT_DIRECTORY, PARSEABLE_METADATA_FILE_NAME])
}

/// TODO: Needs to be updated for distributed mode
#[inline(always)]
fn alert_json_path(stream_name: &str) -> RelativePathBuf {
    RelativePathBuf::from_iter([stream_name, ALERT_FILE_NAME])
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
