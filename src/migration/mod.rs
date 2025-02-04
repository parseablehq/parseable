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
*
*/

pub mod metadata_migration;
mod schema_migration;
mod stream_metadata_migration;

use std::{collections::HashMap, fs::OpenOptions, sync::Arc};

use arrow_schema::Schema;
use bytes::Bytes;
use relative_path::RelativePathBuf;
use serde::Serialize;
use serde_json::Value;

use crate::{
    metadata::{load_daily_metrics, update_data_type_time_partition, LogStreamMetadata},
    metrics::fetch_stats_from_storage,
    option::Mode,
    parseable::{Parseable, PARSEABLE},
    storage::{
        object_storage::{parseable_json_path, schema_path, stream_json_path},
        ObjectStorage, ObjectStorageError, ObjectStoreFormat, PARSEABLE_METADATA_FILE_NAME,
        PARSEABLE_ROOT_DIRECTORY, STREAM_ROOT_DIRECTORY,
    },
};

/// Migrate the metdata from v1 or v2 to v3
/// This is a one time migration
pub async fn run_metadata_migration(
    config: &Parseable,
    parseable_json: &Option<Bytes>,
) -> anyhow::Result<()> {
    let object_store = config.storage.get_object_store();
    let mut storage_metadata: Option<Value> = None;
    if parseable_json.is_some() {
        storage_metadata = serde_json::from_slice(parseable_json.as_ref().unwrap())
            .expect("parseable config is valid json");
    }
    let staging_metadata = get_staging_metadata(config)?;

    fn get_version(metadata: &serde_json::Value) -> Option<&str> {
        metadata
            .as_object()
            .and_then(|meta| meta.get("version"))
            .and_then(|version| version.as_str())
    }

    // if storage metadata is none do nothing
    if let Some(storage_metadata) = storage_metadata {
        match get_version(&storage_metadata) {
            Some("v1") => {
                //migrate to latest version
                //remove querier endpooint and token from storage metadata
                let mut metadata = metadata_migration::v1_v3(storage_metadata);
                metadata = metadata_migration::v3_v4(metadata);
                metadata = metadata_migration::v4_v5(metadata);
                metadata = metadata_migration::remove_querier_metadata(metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v2") => {
                //migrate to latest version
                //remove querier endpooint and token from storage metadata
                let mut metadata = metadata_migration::v2_v3(storage_metadata);
                metadata = metadata_migration::v3_v4(metadata);
                metadata = metadata_migration::v4_v5(metadata);
                metadata = metadata_migration::remove_querier_metadata(metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v3") => {
                //migrate to latest version
                //remove querier endpooint and token from storage metadata
                let mut metadata = metadata_migration::v3_v4(storage_metadata);
                metadata = metadata_migration::v4_v5(metadata);
                metadata = metadata_migration::remove_querier_metadata(metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v4") => {
                //migrate to latest version
                //remove querier endpooint and token from storage metadata
                let mut metadata = metadata_migration::v4_v5(storage_metadata);
                metadata = metadata_migration::v4_v5(metadata);
                metadata = metadata_migration::remove_querier_metadata(metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            _ => {
                //remove querier endpooint and token from storage metadata
                let metadata = metadata_migration::remove_querier_metadata(storage_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
        }
    }

    // if staging metadata is none do nothing
    if let Some(staging_metadata) = staging_metadata {
        match get_version(&staging_metadata) {
            Some("v1") => {
                let mut metadata = metadata_migration::v1_v3(staging_metadata);
                metadata = metadata_migration::v3_v4(metadata);
                put_staging_metadata(config, &metadata)?;
            }
            Some("v2") => {
                let mut metadata = metadata_migration::v2_v3(staging_metadata);
                metadata = metadata_migration::v3_v4(metadata);
                put_staging_metadata(config, &metadata)?;
            }
            Some("v3") => {
                let metadata = metadata_migration::v3_v4(staging_metadata);
                put_staging_metadata(config, &metadata)?;
            }
            _ => (),
        }
    }

    Ok(())
}

/// run the migration for all streams
pub async fn run_migration(config: &Parseable) -> anyhow::Result<()> {
    let storage = config.storage.get_object_store();
    for stream_name in storage.list_streams().await? {
        let Some(metadata) = migration_stream(&stream_name, &*storage).await? else {
            continue;
        };
        PARSEABLE.streams.set_meta(&stream_name, metadata).await;
    }

    Ok(())
}

async fn migration_stream(
    stream: &str,
    storage: &dyn ObjectStorage,
) -> anyhow::Result<Option<LogStreamMetadata>> {
    let mut arrow_schema: Schema = Schema::empty();

    //check if schema exists for the node
    //if not, create schema from querier schema from storage
    //if not present with querier, create schema from ingestor schema from storage
    let schema_path = schema_path(stream);
    let schema = if let Ok(schema) = storage.get_object(&schema_path).await {
        schema
    } else {
        let querier_schema = storage
            .create_schema_from_querier(stream)
            .await
            .unwrap_or_default();
        if !querier_schema.is_empty() {
            querier_schema
        } else {
            storage
                .create_schema_from_ingestor(stream)
                .await
                .unwrap_or_default()
        }
    };

    //check if stream.json exists for the node
    //if not, create stream.json from querier stream.json from storage
    //if not present with querier, create from ingestor stream.json from storage
    let path = stream_json_path(stream);
    let stream_metadata = if let Ok(stream_metadata) = storage.get_object(&path).await {
        stream_metadata
    } else {
        let querier_stream = storage
            .create_stream_from_querier(stream)
            .await
            .unwrap_or_default();
        if !querier_stream.is_empty() {
            querier_stream
        } else {
            storage
                .create_stream_from_ingestor(stream)
                .await
                .unwrap_or_default()
        }
    };

    let mut stream_meta_found = true;
    if stream_metadata.is_empty() {
        if PARSEABLE.options.mode != Mode::Ingest {
            return Ok(None);
        }
        stream_meta_found = false;
    }
    let mut stream_metadata_value = Value::Null;
    if stream_meta_found {
        stream_metadata_value =
            serde_json::from_slice(&stream_metadata).expect("stream.json is valid json");
        let version = stream_metadata_value
            .as_object()
            .and_then(|meta| meta.get("version"))
            .and_then(|version| version.as_str());

        match version {
            Some("v1") => {
                stream_metadata_value = stream_metadata_migration::v1_v4(stream_metadata_value);
                stream_metadata_value =
                    stream_metadata_migration::v4_v5(stream_metadata_value, stream);
                storage
                    .put_object(&path, to_bytes(&stream_metadata_value))
                    .await?;
                let schema = serde_json::from_slice(&schema).ok();
                arrow_schema = schema_migration::v1_v4(schema)?;
                storage
                    .put_object(&schema_path, to_bytes(&arrow_schema))
                    .await?;
            }
            Some("v2") => {
                stream_metadata_value = stream_metadata_migration::v2_v4(stream_metadata_value);
                stream_metadata_value =
                    stream_metadata_migration::v4_v5(stream_metadata_value, stream);
                storage
                    .put_object(&path, to_bytes(&stream_metadata_value))
                    .await?;

                let schema = serde_json::from_slice(&schema)?;
                arrow_schema = schema_migration::v2_v4(schema)?;
                storage
                    .put_object(&schema_path, to_bytes(&arrow_schema))
                    .await?;
            }
            Some("v3") => {
                stream_metadata_value = stream_metadata_migration::v3_v4(stream_metadata_value);
                stream_metadata_value =
                    stream_metadata_migration::v4_v5(stream_metadata_value, stream);
                storage
                    .put_object(&path, to_bytes(&stream_metadata_value))
                    .await?;
            }
            Some("v4") => {
                stream_metadata_value =
                    stream_metadata_migration::v4_v5(stream_metadata_value, stream);
                storage
                    .put_object(&path, to_bytes(&stream_metadata_value))
                    .await?;
            }
            _ => (),
        }
    }

    if arrow_schema.fields().is_empty() {
        arrow_schema = serde_json::from_slice(&schema)?;
    }

    // Setup logstream meta on startup
    let ObjectStoreFormat {
        schema_version,
        created_at,
        first_event_at,
        retention,
        snapshot,
        stats,
        time_partition,
        time_partition_limit,
        custom_partition,
        static_schema_flag,
        hot_tier_enabled,
        stream_type,
        log_source,
        ..
    } = serde_json::from_value(stream_metadata_value).unwrap_or_default();
    let storage = PARSEABLE.storage.get_object_store();

    // update the schema and store it back
    // NOTE: write could be saved, but the cost is cheap, given the low possibilities of being called multiple times
    update_data_type_time_partition(&mut arrow_schema, time_partition.as_ref()).await?;
    storage.put_schema(stream, &arrow_schema).await?;
    //load stats from storage
    fetch_stats_from_storage(stream, stats).await;
    load_daily_metrics(&snapshot.manifest_list, stream);

    let schema = PARSEABLE
        .streams
        .get_or_create(stream)
        .updated_schema(arrow_schema);
    let schema = HashMap::from_iter(
        schema
            .fields
            .iter()
            .map(|v| (v.name().to_owned(), v.clone())),
    );

    let metadata = LogStreamMetadata {
        schema_version,
        schema,
        retention,
        created_at,
        first_event_at,
        time_partition,
        time_partition_limit: time_partition_limit.and_then(|limit| limit.parse().ok()),
        custom_partition,
        static_schema_flag,
        hot_tier_enabled,
        stream_type,
        log_source,
    };

    Ok(Some(metadata))
}

#[inline(always)]
pub fn to_bytes(any: &(impl ?Sized + Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}

pub fn get_staging_metadata(config: &Parseable) -> anyhow::Result<Option<serde_json::Value>> {
    let path = RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME).to_path(config.staging_dir());
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => return Ok(None),
            _ => return Err(err.into()),
        },
    };
    let meta: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    Ok(Some(meta))
}

pub async fn put_remote_metadata(
    storage: &dyn ObjectStorage,
    metadata: &serde_json::Value,
) -> anyhow::Result<()> {
    let path = parseable_json_path();
    let metadata = serde_json::to_vec(metadata)?.into();
    Ok(storage.put_object(&path, metadata).await?)
}

pub fn put_staging_metadata(
    config: &Parseable,
    metadata: &serde_json::Value,
) -> anyhow::Result<()> {
    let path = config.staging_dir().join(".parseable.json");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer(&mut file, metadata)?;
    Ok(())
}

pub async fn run_file_migration(config: &Parseable) -> anyhow::Result<()> {
    let object_store = config.storage.get_object_store();

    let old_meta_file_path = RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME);

    // if this errors that means migrations is already done
    if let Err(err) = object_store.get_object(&old_meta_file_path).await {
        if matches!(err, ObjectStorageError::NoSuchKey(_)) {
            return Ok(());
        }
        return Err(err.into());
    }

    run_meta_file_migration(&object_store, old_meta_file_path).await?;
    run_stream_files_migration(&object_store).await?;

    Ok(())
}

async fn run_meta_file_migration(
    object_store: &Arc<dyn ObjectStorage>,
    old_meta_file_path: RelativePathBuf,
) -> anyhow::Result<()> {
    // get the list of all meta files
    let mut meta_files = object_store.get_ingestor_meta_file_paths().await?;
    meta_files.push(old_meta_file_path);

    for file in meta_files {
        match object_store.get_object(&file).await {
            Ok(bytes) => {
                // we can unwrap here because we know the file exists
                let new_path = RelativePathBuf::from_iter([
                    PARSEABLE_ROOT_DIRECTORY,
                    file.file_name().expect("should have a file name"),
                ]);
                object_store.put_object(&new_path, bytes).await?;
                object_store.delete_object(&file).await?;
            }
            Err(err) => {
                // if error is not a no such key error, something weird happened
                // so return the error
                if !matches!(err, ObjectStorageError::NoSuchKey(_)) {
                    return Err(err.into());
                }
            }
        }
    }

    Ok(())
}

async fn run_stream_files_migration(object_store: &Arc<dyn ObjectStorage>) -> anyhow::Result<()> {
    let streams = object_store.list_old_streams().await?;

    for stream in streams {
        let paths = object_store.get_stream_file_paths(&stream).await?;

        for path in paths {
            match object_store.get_object(&path).await {
                Ok(bytes) => {
                    let new_path = RelativePathBuf::from_iter([
                        stream.as_str(),
                        STREAM_ROOT_DIRECTORY,
                        path.file_name().expect("should have a file name"),
                    ]);
                    object_store.put_object(&new_path, bytes).await?;
                    object_store.delete_object(&path).await?;
                }
                Err(err) => {
                    if !matches!(err, ObjectStorageError::NoSuchKey(_)) {
                        return Err(err.into());
                    }
                }
            }
        }
    }

    Ok(())
}
