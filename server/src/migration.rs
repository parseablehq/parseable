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

mod metadata_migration;
mod schema_migration;
mod stream_metadata_migration;

use std::{fs::OpenOptions, sync::Arc};

use bytes::Bytes;
use itertools::Itertools;
use relative_path::RelativePathBuf;
use serde::Serialize;

use crate::{
    option::Config,
    storage::{
        object_storage::{parseable_json_path, stream_json_path},
        ObjectStorage, ObjectStorageError, PARSEABLE_METADATA_FILE_NAME, PARSEABLE_ROOT_DIRECTORY,
        SCHEMA_FILE_NAME, STREAM_ROOT_DIRECTORY,
    },
};

/// Migrate the metdata from v1 or v2 to v3
/// This is a one time migration
pub async fn run_metadata_migration(config: &Config) -> anyhow::Result<()> {
    let object_store = config.storage().get_object_store();
    let storage_metadata = get_storage_metadata(&*object_store).await?;
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
                let metadata = metadata_migration::v1_v3(storage_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v2") => {
                let metadata = metadata_migration::v2_v3(storage_metadata);
                put_remote_metadata(&*object_store, &metadata).await?;
            }
            Some("v3") => {
                let mdata = metadata_migration::update_v3(storage_metadata);
                put_remote_metadata(&*object_store, &mdata).await?;
            }
            _ => (),
        }
    }

    // if staging metadata is none do nothing
    if let Some(staging_metadata) = staging_metadata {
        match get_version(&staging_metadata) {
            Some("v1") => {
                let metadata = metadata_migration::v1_v3(staging_metadata);
                put_staging_metadata(config, &metadata)?;
            }
            Some("v2") => {
                let metadata = metadata_migration::v2_v3(staging_metadata);
                put_staging_metadata(config, &metadata)?;
            }
            Some("v3") => {
                let mdata = metadata_migration::update_v3(staging_metadata);
                put_staging_metadata(config, &mdata)?;
            }
            _ => (),
        }
    }

    Ok(())
}

/// run the migration for all streams
pub async fn run_migration(config: &Config) -> anyhow::Result<()> {
    let storage = config.storage().get_object_store();
    let streams = storage.list_streams().await?;

    for stream in streams {
        migration_stream(&stream.name, &*storage).await?
    }

    Ok(())
}

async fn migration_stream(stream: &str, storage: &dyn ObjectStorage) -> anyhow::Result<()> {
    let path = stream_json_path(stream);

    let stream_metadata = storage.get_object(&path).await?;
    let stream_metadata: serde_json::Value =
        serde_json::from_slice(&stream_metadata).expect("stream.json is valid json");

    let version = stream_metadata
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str());

    match version {
        Some("v1") => {
            let new_stream_metadata = stream_metadata_migration::v1_v3(stream_metadata);
            storage
                .put_object(&path, to_bytes(&new_stream_metadata))
                .await?;

            let schema_path =
                RelativePathBuf::from_iter([stream, STREAM_ROOT_DIRECTORY, SCHEMA_FILE_NAME]);
            let schema = storage.get_object(&schema_path).await?;
            let schema = serde_json::from_slice(&schema).ok();
            let map = schema_migration::v1_v3(schema)?;
            storage.put_object(&schema_path, to_bytes(&map)).await?;
        }
        Some("v2") => {
            let new_stream_metadata = stream_metadata_migration::v2_v3(stream_metadata);
            storage
                .put_object(&path, to_bytes(&new_stream_metadata))
                .await?;

            let schema_path =
                RelativePathBuf::from_iter([stream, STREAM_ROOT_DIRECTORY, SCHEMA_FILE_NAME]);
            let schema = storage.get_object(&schema_path).await?;
            let schema = serde_json::from_slice(&schema)?;
            let map = schema_migration::v2_v3(schema)?;
            storage.put_object(&schema_path, to_bytes(&map)).await?;
        }
        _ => (),
    }

    Ok(())
}

#[inline(always)]
fn to_bytes(any: &(impl ?Sized + Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}

pub fn get_staging_metadata(config: &Config) -> anyhow::Result<Option<serde_json::Value>> {
    let path = parseable_json_path().to_path(config.staging_dir());

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

async fn get_storage_metadata(
    storage: &dyn ObjectStorage,
) -> anyhow::Result<Option<serde_json::Value>> {
    let path = parseable_json_path();
    match storage.get_object(&path).await {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).expect("parseable config is valid json"),
        )),
        Err(err) => {
            if matches!(err, ObjectStorageError::NoSuchKey(_)) {
                Ok(None)
            } else {
                Err(err.into())
            }
        }
    }
}

pub async fn put_remote_metadata(
    storage: &dyn ObjectStorage,
    metadata: &serde_json::Value,
) -> anyhow::Result<()> {
    let path = parseable_json_path();
    let metadata = serde_json::to_vec(metadata)?.into();
    Ok(storage.put_object(&path, metadata).await?)
}

pub fn put_staging_metadata(config: &Config, metadata: &serde_json::Value) -> anyhow::Result<()> {
    let path = parseable_json_path().to_path(config.staging_dir());
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer(&mut file, metadata)?;
    Ok(())
}

pub async fn run_file_migration(config: &Config) -> anyhow::Result<()> {
    let object_store = config.storage().get_object_store();

    let old_meta_file_path = RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME);

    // if this errors that means migrations is already done
    if let Err(err) = object_store.get_object(&old_meta_file_path).await {
        if matches!(err, ObjectStorageError::NoSuchKey(_)) {
            return Ok(());
        }
        return Err(err.into());
    }

    run_meta_file_migration(&object_store, old_meta_file_path).await?;
    run_stream_files_migration(object_store).await?;

    Ok(())
}

async fn run_meta_file_migration(
    object_store: &Arc<dyn ObjectStorage + Send>,
    old_meta_file_path: RelativePathBuf,
) -> anyhow::Result<()> {
    log::info!("Migrating metadata files to new location");

    // get the list of all meta files
    let mut meta_files = object_store.get_ingester_meta_file_paths().await?;
    meta_files.push(old_meta_file_path);

    for file in meta_files {
        match object_store.get_object(&file).await {
            Ok(bytes) => {
                // we can unwrap here because we know the file exists
                let new_path = RelativePathBuf::from_iter([
                    PARSEABLE_ROOT_DIRECTORY,
                    file.file_name().unwrap(),
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

async fn run_stream_files_migration(
    object_store: Arc<dyn ObjectStorage + Send>,
) -> anyhow::Result<()> {
    let streams = object_store
        .list_old_streams()
        .await?
        .into_iter()
        .map(|stream| stream.name)
        .collect_vec();

    for stream in streams {
        let paths = object_store.get_stream_file_paths(&stream).await?;

        for path in paths {
            match object_store.get_object(&path).await {
                Ok(bytes) => {
                    let new_path = RelativePathBuf::from_iter([
                        stream.as_str(),
                        STREAM_ROOT_DIRECTORY,
                        path.file_name().unwrap(),
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
