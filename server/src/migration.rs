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

mod schema_migration;
mod stream_metadata_migration;

use bytes::Bytes;
use relative_path::RelativePathBuf;
use serde::Serialize;

use crate::{option::Config, storage::ObjectStorage};

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

    let maybe_v1 = stream_metadata
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str());

    if matches!(maybe_v1, Some("v1")) {
        let new_stream_metadata = stream_metadata_migration::v1_v2(stream_metadata);
        storage
            .put_object(&path, to_bytes(&new_stream_metadata))
            .await?;

        let schema_path = RelativePathBuf::from_iter([stream, ".schema"]);
        let schema = storage.get_object(&schema_path).await?;
        let schema = serde_json::from_slice(&schema).ok();
        let map = schema_migration::v1_v2(schema)?;
        storage.put_object(&schema_path, to_bytes(&map)).await?;
    }

    Ok(())
}

#[inline(always)]
fn to_bytes(any: &(impl ?Sized + Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}
