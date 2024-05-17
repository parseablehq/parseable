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

use serde_json::{json, Value};

use crate::{catalog::snapshot::CURRENT_SNAPSHOT_VERSION, storage};

pub fn v1_v4(mut stream_metadata: Value) -> Value {
    let stream_metadata_map = stream_metadata.as_object_mut().unwrap();
    let stats = stream_metadata_map.get("stats").unwrap().clone();
    let default_stats = json!({
        "lifetime_stats": {
            "events": stats.get("events").unwrap(),
            "ingestion": stats.get("ingestion").unwrap(),
            "storage": stats.get("storage").unwrap()
        },
        "current_stats": {
            "events": stats.get("events").unwrap(),
            "ingestion": stats.get("ingestion").unwrap(),
            "storage": stats.get("storage").unwrap()
        },
        "deleted_stats": {
            "events": 0,
            "ingestion": 0,
            "storage": 0
        },
        "current_date_stats": {
            "events": 0,
            "ingestion": 0,
            "storage": 0
        }
    });
    stream_metadata_map.insert("stats".to_owned(), default_stats);
    stream_metadata_map.insert(
        "version".to_owned(),
        Value::String(storage::CURRENT_SCHEMA_VERSION.into()),
    );
    stream_metadata_map.insert(
        "objectstore-format".to_owned(),
        Value::String(storage::CURRENT_OBJECT_STORE_VERSION.into()),
    );

    let snapshot = stream_metadata_map.get("snapshot").unwrap().clone();
    let version = snapshot
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str());
    if let Some("v1") = version {
        let updated_snapshot = v1_v2_snapshot_migration(snapshot);
        stream_metadata_map.insert("snapshot".to_owned(), updated_snapshot);
    }
    stream_metadata
}

pub fn v2_v4(mut stream_metadata: Value) -> Value {
    let stream_metadata_map = stream_metadata.as_object_mut().unwrap();
    let stats = stream_metadata_map.get("stats").unwrap().clone();
    let default_stats = json!({
        "lifetime_stats": {
            "events": stats.get("events").unwrap(),
            "ingestion": stats.get("ingestion").unwrap(),
            "storage": stats.get("storage").unwrap()
        },
        "current_stats": {
            "events": stats.get("events").unwrap(),
            "ingestion": stats.get("ingestion").unwrap(),
            "storage": stats.get("storage").unwrap()
        },
        "deleted_stats": {
            "events": 0,
            "ingestion": 0,
            "storage": 0
        },
        "current_date_stats": {
            "events": 0,
            "ingestion": 0,
            "storage": 0
        }
    });
    stream_metadata_map.insert("stats".to_owned(), default_stats);
    stream_metadata_map.insert(
        "version".to_owned(),
        Value::String(storage::CURRENT_SCHEMA_VERSION.into()),
    );
    stream_metadata_map.insert(
        "objectstore-format".to_owned(),
        Value::String(storage::CURRENT_OBJECT_STORE_VERSION.into()),
    );

    let snapshot = stream_metadata_map.get("snapshot").unwrap().clone();
    let version = snapshot
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str());
    if let Some("v1") = version {
        let updated_snapshot = v1_v2_snapshot_migration(snapshot);
        stream_metadata_map.insert("snapshot".to_owned(), updated_snapshot);
    }
    stream_metadata
}

pub fn v3_v4(mut stream_metadata: Value) -> Value {
    let stream_metadata_map: &mut serde_json::Map<String, Value> =
        stream_metadata.as_object_mut().unwrap();
    let stats = stream_metadata_map.get("stats").unwrap().clone();
    let default_stats = json!({
        "lifetime_stats": {
            "events": stats.get("events").unwrap(),
            "ingestion": stats.get("ingestion").unwrap(),
            "storage": stats.get("storage").unwrap()
        },
        "current_stats": {
            "events": stats.get("events").unwrap(),
            "ingestion": stats.get("ingestion").unwrap(),
            "storage": stats.get("storage").unwrap()
        },
        "deleted_stats": {
            "events": 0,
            "ingestion": 0,
            "storage": 0
        },
        "current_date_stats": {
            "events": 0,
            "ingestion": 0,
            "storage": 0
        }
    });
    stream_metadata_map.insert("stats".to_owned(), default_stats);
    stream_metadata_map.insert(
        "version".to_owned(),
        Value::String(storage::CURRENT_SCHEMA_VERSION.into()),
    );
    stream_metadata_map.insert(
        "objectstore-format".to_owned(),
        Value::String(storage::CURRENT_OBJECT_STORE_VERSION.into()),
    );

    let snapshot = stream_metadata_map.get("snapshot").unwrap().clone();
    let version = snapshot
        .as_object()
        .and_then(|meta| meta.get("version"))
        .and_then(|version| version.as_str());
    if let Some("v1") = version {
        let updated_snapshot = v1_v2_snapshot_migration(snapshot);
        stream_metadata_map.insert("snapshot".to_owned(), updated_snapshot);
    }

    stream_metadata
}

fn v1_v2_snapshot_migration(mut snapshot: Value) -> Value {
    let manifest_list = snapshot.get("manifest_list").unwrap();
    let mut new_manifest_list = Vec::new();
    for manifest in manifest_list.as_array().unwrap() {
        let manifest_map = manifest.as_object().unwrap();
        let time_lower_bound = manifest_map.get("time_lower_bound").unwrap();
        let time_upper_bound = manifest_map.get("time_upper_bound").unwrap();
        let new_manifest = json!({
            "manifest_path": manifest_map.get("manifest_path").unwrap(),
            "time_lower_bound": time_lower_bound,
            "time_upper_bound": time_upper_bound,
            "events_count": 0,
            "ingestion_size": 0,
            "storage_size": 0
        });
        new_manifest_list.push(new_manifest);
    }
    let snapshot_map: &mut serde_json::Map<String, Value> = snapshot.as_object_mut().unwrap();
    snapshot_map.insert(
        "version".to_owned(),
        Value::String(CURRENT_SNAPSHOT_VERSION.into()),
    );
    snapshot_map.insert("manifest_list".to_owned(), Value::Array(new_manifest_list));
    snapshot
}
