/*
* Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::fs;

use bytes::Bytes;
use relative_path::RelativePathBuf;
use serde::Serialize;

use crate::{
    option::Config,
    storage::{encryption, ObjectStorage},
};

pub async fn run_migration(config: &Config) -> anyhow::Result<()> {
    let storage = config.storage().get_object_store();
    migration_metadata(config, &*storage).await?;
    let streams = storage.list_streams().await?;
    for stream in streams {
        migration_stream(&stream.name, &*storage).await?
    }

    Ok(())
}

// corresponds to migration of parseable metadata
async fn migration_metadata(config: &Config, storage: &dyn ObjectStorage) -> anyhow::Result<()> {
    let path = RelativePathBuf::from_iter([".parseable.json"]);
    // get bytes .. None if key does not exists
    // if parseable.json does not exists then it can mean this is a new storage
    let stream_metadata = match storage.get_object(&path).await {
        Ok(value) => value,
        Err(err) => match err {
            crate::storage::ObjectStorageError::NoSuchKey(_) => return Ok(()),
            _ => return Err(err.into()),
        },
    };

    let staging_metadata_path = path.to_path(config.staging_dir());

    // try fetching staging metadata file
    // if does not exist then this is a new staging
    let staging_metadata: Option<serde_json::Value> = match fs::read(&staging_metadata_path) {
        Ok(value) => match serde_json::from_slice(&value) {
            Ok(value) => value,
            Err(_) => {
                let bytes = encryption::decrypt(encryption::key().as_bytes(), value.into());
                serde_json::from_slice(&bytes)?
            }
        },
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => None,
            _ => return Err(err.into()),
        },
    };

    // try get unencypted otherwise try decryption
    let stream_metadata: serde_json::Value = match serde_json::from_slice(&stream_metadata) {
        Ok(value) => value,
        Err(_) => {
            let bytes = encryption::decrypt(encryption::key().as_bytes(), stream_metadata);
            serde_json::from_slice(&bytes)?
        }
    };

    let version = stream_metadata
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str());

    if version == Some("v1") {
        let meta = metadata_migration::v1_v2(stream_metadata);
        storage.encrypt_put(&path, to_bytes(&meta)).await?;
    }

    if let Some(staging_metadata) = staging_metadata {
        let version = staging_metadata
            .as_object()
            .and_then(|meta| meta.get("version"))
            .and_then(|version| version.as_str());

        if version == Some("v1") {
            let meta = metadata_migration::v1_v2(staging_metadata);
            let meta = encryption::encrypt(encryption::key().as_bytes(), to_bytes(&meta));
            fs::write(&staging_metadata_path, &meta)?;
        }
    }

    Ok(())
}

async fn migration_stream(stream: &str, storage: &dyn ObjectStorage) -> anyhow::Result<()> {
    let path = RelativePathBuf::from_iter([stream, ".stream.json"]);
    let stream_metadata = try_get_value(&path, storage).await?;

    let version = stream_metadata
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str());

    match version {
        Some("v1") => {
            let new_stream_metadata = stream_metadata_migration::v1_v4(stream_metadata);
            storage
                .encrypt_put(&path, to_bytes(&new_stream_metadata))
                .await?;
            let schema_path = RelativePathBuf::from_iter([stream, ".schema"]);
            let schema = storage.get_object(&schema_path).await?;
            let schema = serde_json::from_slice(&schema).ok();
            let map = schema_migration::v1_v4(schema)?;
            storage.encrypt_put(&schema_path, to_bytes(&map)).await?;
        }
        Some("v2") => {
            let new_stream_metadata = stream_metadata_migration::v2_v4(stream_metadata);
            storage
                .encrypt_put(&path, to_bytes(&new_stream_metadata))
                .await?;
            let schema_path = RelativePathBuf::from_iter([stream, ".schema"]);
            let schema = storage.get_object(&schema_path).await?;
            let schema = serde_json::from_slice(&schema)?;
            let map = schema_migration::v2_v4(schema)?;
            storage.encrypt_put(&schema_path, to_bytes(&map)).await?;
        }
        Some("v3") => {
            storage
                .encrypt_put(&path, to_bytes(&stream_metadata))
                .await?;
            let schema_path = RelativePathBuf::from_iter([stream, ".schema"]);
            let schema = storage.get_object(&schema_path).await?;
            storage.encrypt_put(&schema_path, schema).await?;
        }
        _ => (),
    }

    Ok(())
}

async fn try_get_value(
    path: &RelativePathBuf,
    storage: &dyn ObjectStorage,
) -> Result<serde_json::Value, anyhow::Error> {
    // try get unencrypted bytes
    let stream_metadata = storage.get_object(path).await?;
    let stream_metadata: serde_json::Value = match serde_json::from_slice(&stream_metadata) {
        Ok(value) => value,
        Err(_) => {
            let bytes = encryption::decrypt(encryption::key().as_bytes(), stream_metadata);
            serde_json::from_slice(&bytes)?
        }
    };

    Ok(stream_metadata)
}

#[inline(always)]
fn to_bytes(any: &(impl ?Sized + Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}
