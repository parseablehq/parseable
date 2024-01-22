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

use std::fs::OpenOptions;

use bytes::Bytes;
use relative_path::RelativePathBuf;
use serde::Serialize;

use crate::{
    option::Config,
    storage::{ObjectStorage, ObjectStorageError},
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
            _ => (),
        }
    }

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
            _ => (),
        }
    }

    Ok(())
}

pub async fn run_migration(config: &Config) -> anyhow::Result<()> {
    let storage = config.storage().get_object_store();
    let streams = storage.list_streams().await?;

    for stream in streams {
        migration_stream(&stream.name, &*storage).await?
    }

    Ok(())
}

async fn migration_stream(stream: &str, storage: &dyn ObjectStorage) -> anyhow::Result<()> {
    let path = RelativePathBuf::from_iter([stream, ".stream.json"]);
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

            let schema_path = RelativePathBuf::from_iter([stream, ".schema"]);
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

            let schema_path = RelativePathBuf::from_iter([stream, ".schema"]);
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
    let path = config.staging_dir().join(".parseable.json");
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
    let path = RelativePathBuf::from_iter([".parseable.json"]);
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
    let path = RelativePathBuf::from_iter([".parseable.json"]);
    let metadata = serde_json::to_vec(metadata)?.into();
    Ok(storage.put_object(&path, metadata).await?)
}

pub fn put_staging_metadata(config: &Config, metadata: &serde_json::Value) -> anyhow::Result<()> {
    let path = config.staging_dir().join(".parseable.json");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer(&mut file, metadata)?;
    Ok(())
}
