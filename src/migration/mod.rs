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

use std::{collections::HashMap, fs::OpenOptions};

use arrow_schema::Schema;
use bytes::Bytes;
use relative_path::RelativePathBuf;
use serde::Serialize;
use serde_json::Value;
use tracing::warn;

use crate::{
    metadata::{LogStreamMetadata, load_daily_metrics, update_data_type_time_partition},
    metrics::fetch_stats_from_storage,
    option::Mode,
    parseable::{PARSEABLE, Parseable},
    storage::{
        ObjectStorage, ObjectStoreFormat, PARSEABLE_METADATA_FILE_NAME,
        object_storage::{parseable_json_path, schema_path, stream_json_path},
    },
};

fn get_version(metadata: &serde_json::Value) -> Option<&str> {
    metadata
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str())
}

/// Migrate the metdata from v1 or v2 to v3
/// This is a one time migration
pub async fn run_metadata_migration(
    config: &Parseable,
    parseable_json: &mut Option<Bytes>,
) -> anyhow::Result<()> {
    let object_store = config.storage.get_object_store();
    let mut storage_metadata: Option<Value> = None;
    if parseable_json.is_some() {
        storage_metadata = serde_json::from_slice(parseable_json.as_ref().unwrap())
            .expect("parseable config is valid json");
    }
    let staging_metadata = get_staging_metadata(config)?;

    // if storage metadata is none do nothing
    if let Some(storage_metadata) = storage_metadata {
        match get_version(&storage_metadata) {
            Some("v1") => {
                let mut metadata = metadata_migration::v1_v3(storage_metadata);
                metadata = metadata_migration::v3_v4(metadata);
                metadata = metadata_migration::v4_v5(metadata);
                metadata = metadata_migration::v5_v6(metadata);
                metadata = metadata_migration::remove_querier_metadata(metadata);
                let _metadata: Bytes = serde_json::to_vec(&metadata)?.into();
                *parseable_json = Some(_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v2") => {
                let mut metadata = metadata_migration::v2_v3(storage_metadata);
                metadata = metadata_migration::v3_v4(metadata);
                metadata = metadata_migration::v4_v5(metadata);
                metadata = metadata_migration::v5_v6(metadata);
                metadata = metadata_migration::remove_querier_metadata(metadata);
                let _metadata: Bytes = serde_json::to_vec(&metadata)?.into();
                *parseable_json = Some(_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v3") => {
                let mut metadata = metadata_migration::v3_v4(storage_metadata);
                metadata = metadata_migration::v4_v5(metadata);
                metadata = metadata_migration::v5_v6(metadata);
                metadata = metadata_migration::remove_querier_metadata(metadata);
                let _metadata: Bytes = serde_json::to_vec(&metadata)?.into();
                *parseable_json = Some(_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v4") => {
                let mut metadata = metadata_migration::v4_v5(storage_metadata);
                metadata = metadata_migration::v5_v6(metadata);
                metadata = metadata_migration::remove_querier_metadata(metadata);
                let _metadata: Bytes = serde_json::to_vec(&metadata)?.into();
                *parseable_json = Some(_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v5") => {
                let metadata = metadata_migration::v5_v6(storage_metadata);
                let _metadata: Bytes = serde_json::to_vec(&metadata)?.into();
                *parseable_json = Some(_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            _ => {
                let metadata = metadata_migration::remove_querier_metadata(storage_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
        }
    }

    // if staging metadata is none do nothing
    if let Some(staging_metadata) = staging_metadata {
        migrate_staging(config, staging_metadata)?;
    }

    Ok(())
}

fn migrate_staging(config: &Parseable, staging_metadata: Value) -> anyhow::Result<()> {
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
        Some("v4") => {
            let metadata = metadata_migration::v4_v5(staging_metadata);
            let metadata = metadata_migration::v5_v6(metadata);
            put_staging_metadata(config, &metadata)?;
        }
        Some("v5") => {
            let metadata = metadata_migration::v5_v6(staging_metadata);
            put_staging_metadata(config, &metadata)?;
        }
        _ => (),
    }
    Ok(())
}

/// run the migration for all streams concurrently
pub async fn run_migration(config: &Parseable) -> anyhow::Result<()> {
    let storage = config.storage.get_object_store();

    // Get all stream names
    let stream_names = storage.list_streams().await?;

    // Create futures for each stream migration
    let futures = stream_names.into_iter().map(|stream_name| {
        let storage = storage.clone();
        async move {
            match migration_stream(&stream_name, &*storage).await {
                Ok(Some(metadata)) => {
                    // Apply the metadata update
                    config
                        .get_or_create_stream(&stream_name)
                        .set_metadata(metadata)
                        .await;
                    Ok(())
                }
                Ok(None) => Ok(()),
                Err(e) => {
                    // Optionally log error but continue with other streams
                    warn!("Error migrating stream {}: {:?}", stream_name, e);
                    Err(e)
                }
            }
        }
    });

    // Execute all migrations concurrently
    let results = futures::future::join_all(futures).await;

    // Check for errors
    let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();

    if errors.is_empty() {
        Ok(())
    } else {
        // Return the first error, or aggregate them if needed
        Err(anyhow::anyhow!(
            "Migration errors occurred: {} failures",
            errors.len()
        ))
    }
}

async fn migration_stream(
    stream: &str,
    storage: &dyn ObjectStorage,
) -> anyhow::Result<Option<LogStreamMetadata>> {
    let mut arrow_schema: Schema = Schema::empty();

    let schema = storage.create_schema_from_storage(stream).await?;
    let stream_metadata = fetch_or_create_stream_metadata(stream, storage).await?;

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
        stream_metadata_value =
            migrate_stream_metadata(stream_metadata_value, stream, storage, &schema).await?;
    }

    if arrow_schema.fields().is_empty() {
        arrow_schema = serde_json::from_slice(&schema)?;
    }

    let metadata =
        setup_logstream_metadata(stream, &mut arrow_schema, stream_metadata_value).await?;
    Ok(Some(metadata))
}

async fn fetch_or_create_stream_metadata(
    stream: &str,
    storage: &dyn ObjectStorage,
) -> anyhow::Result<Bytes> {
    let path = stream_json_path(stream);
    if let Ok(stream_metadata) = storage.get_object(&path).await {
        Ok(stream_metadata)
    } else {
        let querier_stream = storage
            .create_stream_from_querier(stream)
            .await
            .unwrap_or_default();
        if !querier_stream.is_empty() {
            Ok(querier_stream)
        } else {
            Ok(storage
                .create_stream_from_ingestor(stream)
                .await
                .unwrap_or_default())
        }
    }
}

async fn migrate_stream_metadata(
    mut stream_metadata_value: Value,
    stream: &str,
    storage: &dyn ObjectStorage,
    schema: &Bytes,
) -> anyhow::Result<Value> {
    let path = stream_json_path(stream);
    let schema_path = schema_path(stream);

    let version = stream_metadata_value
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str());

    match version {
        Some("v1") => {
            stream_metadata_value = stream_metadata_migration::v1_v4(stream_metadata_value);
            stream_metadata_value = stream_metadata_migration::v4_v5(stream_metadata_value, stream);
            stream_metadata_value = stream_metadata_migration::v5_v6(stream_metadata_value);

            storage
                .put_object(&path, to_bytes(&stream_metadata_value))
                .await?;
            let schema = serde_json::from_slice(schema).ok();
            let arrow_schema = schema_migration::v1_v4(schema)?;
            storage
                .put_object(&schema_path, to_bytes(&arrow_schema))
                .await?;
        }
        Some("v2") => {
            stream_metadata_value = stream_metadata_migration::v2_v4(stream_metadata_value);
            stream_metadata_value = stream_metadata_migration::v4_v5(stream_metadata_value, stream);
            stream_metadata_value = stream_metadata_migration::v5_v6(stream_metadata_value);

            storage
                .put_object(&path, to_bytes(&stream_metadata_value))
                .await?;
            let schema = serde_json::from_slice(schema)?;
            let arrow_schema = schema_migration::v2_v4(schema)?;
            storage
                .put_object(&schema_path, to_bytes(&arrow_schema))
                .await?;
        }
        Some("v3") => {
            stream_metadata_value = stream_metadata_migration::v3_v4(stream_metadata_value);
            stream_metadata_value = stream_metadata_migration::v4_v5(stream_metadata_value, stream);
            stream_metadata_value = stream_metadata_migration::v5_v6(stream_metadata_value);

            storage
                .put_object(&path, to_bytes(&stream_metadata_value))
                .await?;
        }
        Some("v4") => {
            stream_metadata_value = stream_metadata_migration::v4_v5(stream_metadata_value, stream);
            stream_metadata_value = stream_metadata_migration::v5_v6(stream_metadata_value);

            storage
                .put_object(&path, to_bytes(&stream_metadata_value))
                .await?;
        }
        Some("v5") => {
            stream_metadata_value = stream_metadata_migration::v5_v6(stream_metadata_value);
            storage
                .put_object(&path, to_bytes(&stream_metadata_value))
                .await?;
        }
        _ => {
            stream_metadata_value =
                stream_metadata_migration::rename_log_source_v6(stream_metadata_value);
            storage
                .put_object(&path, to_bytes(&stream_metadata_value))
                .await?;
        }
    }

    Ok(stream_metadata_value)
}

async fn setup_logstream_metadata(
    stream: &str,
    arrow_schema: &mut Schema,
    stream_metadata_value: Value,
) -> anyhow::Result<LogStreamMetadata> {
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

    update_data_type_time_partition(arrow_schema, time_partition.as_ref()).await?;
    storage.put_schema(stream, arrow_schema).await?;
    fetch_stats_from_storage(stream, stats).await;
    load_daily_metrics(&snapshot.manifest_list, stream);

    let schema = PARSEABLE
        .get_or_create_stream(stream)
        .updated_schema(arrow_schema.clone());
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

    Ok(metadata)
}

#[inline(always)]
pub fn to_bytes(any: &(impl ?Sized + Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}

pub fn get_staging_metadata(config: &Parseable) -> anyhow::Result<Option<serde_json::Value>> {
    let path =
        RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME).to_path(config.options.staging_dir());
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
    let path = config.options.staging_dir().join(".parseable.json");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer(&mut file, metadata)?;
    Ok(())
}
