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

use serde_json::{json, Value};

use crate::storage;

pub fn v1_v3(mut stream_metadata: Value) -> Value {
    let default_stats = json!({
        "events": 0,
        "ingestion": 0,
        "storage": 0
    });
    let stream_metadata_map = stream_metadata.as_object_mut().unwrap();
    stream_metadata_map.entry("stats").or_insert(default_stats);
    stream_metadata_map.insert(
        "version".to_owned(),
        Value::String(storage::CURRENT_SCHEMA_VERSION.into()),
    );
    stream_metadata_map.insert(
        "objectstore-format".to_owned(),
        Value::String(storage::CURRENT_OBJECT_STORE_VERSION.into()),
    );
    stream_metadata
}

pub fn v2_v3(mut stream_metadata: Value) -> Value {
    let default_stats = json!({
        "events": 0,
        "ingestion": 0,
        "storage": 0
    });
    let stream_metadata_map = stream_metadata.as_object_mut().unwrap();
    stream_metadata_map.entry("stats").or_insert(default_stats);
    stream_metadata_map.insert(
        "version".to_owned(),
        Value::String(storage::CURRENT_SCHEMA_VERSION.into()),
    );
    stream_metadata_map.insert(
        "objectstore-format".to_owned(),
        Value::String(storage::CURRENT_OBJECT_STORE_VERSION.into()),
    );
    stream_metadata
}
