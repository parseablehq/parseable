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

use std::collections::HashMap;

use serde_json::{Value, json};

use crate::{
    catalog::snapshot::CURRENT_SNAPSHOT_VERSION, handlers::http::cluster::INTERNAL_STREAM_NAME,
    storage,
};

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
    stream_metadata_map.insert(
        "snapshot".to_owned(),
        json!({
            "version": CURRENT_SNAPSHOT_VERSION,
            "manifest_list": []
        }),
    );
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

    stream_metadata_map.insert(
        "snapshot".to_owned(),
        json!({
            "version": CURRENT_SNAPSHOT_VERSION,
            "manifest_list": []
        }),
    );
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
    if matches!(version, Some("v1")) {
        let updated_snapshot = v1_v2_snapshot_migration(snapshot);
        stream_metadata_map.insert("snapshot".to_owned(), updated_snapshot);
    }

    stream_metadata
}

pub fn v4_v5(mut stream_metadata: Value, stream_name: &str) -> Value {
    let stream_metadata_map: &mut serde_json::Map<String, Value> =
        stream_metadata.as_object_mut().unwrap();
    stream_metadata_map.insert(
        "objectstore-format".to_owned(),
        Value::String(storage::CURRENT_OBJECT_STORE_VERSION.into()),
    );
    stream_metadata_map.insert(
        "version".to_owned(),
        Value::String(storage::CURRENT_SCHEMA_VERSION.into()),
    );
    let stream_type = stream_metadata_map.get("stream_type");
    if stream_type.is_none() {
        if stream_name.eq(INTERNAL_STREAM_NAME) {
            stream_metadata_map.insert(
                "stream_type".to_owned(),
                Value::String(storage::StreamType::Internal.to_string()),
            );
        } else {
            stream_metadata_map.insert(
                "stream_type".to_owned(),
                Value::String(storage::StreamType::UserDefined.to_string()),
            );
        }
    }

    stream_metadata
}

pub fn v5_v6(mut stream_metadata: Value) -> Value {
    let stream_metadata_map = stream_metadata.as_object_mut().unwrap();

    stream_metadata_map.insert(
        "objectstore-format".to_owned(),
        Value::String(storage::CURRENT_OBJECT_STORE_VERSION.into()),
    );
    stream_metadata_map.insert(
        "version".to_owned(),
        Value::String(storage::CURRENT_SCHEMA_VERSION.into()),
    );

    // Transform or add log_source
    let log_source_entry = match stream_metadata_map.remove("log_source") {
        Some(log_source) => transform_log_source(log_source),
        None => default_log_source_entry(),
    };

    stream_metadata_map.insert("log_source".to_owned(), json!([log_source_entry]));

    stream_metadata
}

fn transform_log_source(log_source: Value) -> Value {
    if let Some(format_str) = log_source.as_str() {
        let transformed_format = map_log_source_format(format_str);
        json!({
            "log_source_format": transformed_format,
            "fields": []
        })
    } else {
        default_log_source_entry()
    }
}

fn map_log_source_format(format_str: &str) -> &str {
    match format_str {
        "Kinesis" => "kinesis",
        "OtelLogs" => "otel-logs",
        "OtelTraces" => "otel-traces",
        "OtelMetrics" => "otel-metrics",
        "Pmeta" => "pmeta",
        "Json" => "json",
        _ => "json",
    }
}

fn default_log_source_entry() -> Value {
    json!({
        "log_source_format": "json",
        "fields": []
    })
}

pub fn rename_log_source_v6(mut stream_metadata: Value) -> Value {
    let format_mapping = create_format_mapping();

    if let Some(log_sources) = stream_metadata
        .get_mut("log_source")
        .and_then(|v| v.as_array_mut())
    {
        for source in log_sources.iter_mut() {
            if let Some(format_value) = source.get_mut("log_source_format") {
                if let Some(format_str) = format_value.as_str() {
                    if let Some(new_format) = format_mapping.get(format_str) {
                        *format_value = json!(new_format);
                    }
                }
            }
        }
    }

    stream_metadata
}

fn create_format_mapping() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        ("Kinesis", "kinesis"),
        ("OtelLogs", "otel-logs"),
        ("OtelTraces", "otel-traces"),
        ("OtelMetrics", "otel-metrics"),
        ("Pmeta", "pmeta"),
        ("Json", "json"),
    ])
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
            "events_ingested": 0,
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_v5_v6_with_log_source() {
        let stream_metadata = serde_json::json!({"version":"v5","schema_version":"v0","objectstore-format":"v5","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":"OtelLogs"});
        let expected = serde_json::json!({"version":"v6","schema_version":"v0","objectstore-format":"v6","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":[{"log_source_format":"otel-logs","fields":[]}]});
        let updated_stream_metadata = super::v5_v6(stream_metadata.clone());
        assert_eq!(updated_stream_metadata, expected);
    }

    #[test]
    fn test_v5_v6_with_default_log_source() {
        let stream_metadata = serde_json::json!({"version":"v5","schema_version":"v0","objectstore-format":"v5","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":"Json"});
        let expected = serde_json::json!({"version":"v6","schema_version":"v0","objectstore-format":"v6","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":[{"log_source_format":"json","fields":[]}]});
        let updated_stream_metadata = super::v5_v6(stream_metadata.clone());
        assert_eq!(updated_stream_metadata, expected);
    }

    #[test]
    fn test_v5_v6_without_log_source() {
        let stream_metadata = serde_json::json!({"version":"v4","schema_version":"v0","objectstore-format":"v4","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined"});
        let expected = serde_json::json!({"version":"v6","schema_version":"v0","objectstore-format":"v6","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":[{"log_source_format":"json","fields":[]}]});
        let updated_stream_metadata = super::v5_v6(stream_metadata.clone());
        assert_eq!(updated_stream_metadata, expected);
    }

    #[test]
    fn test_v5_v6_unknown_log_source() {
        let stream_metadata = serde_json::json!({"version":"v5","schema_version":"v0","objectstore-format":"v5","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":"Invalid"});
        let expected = serde_json::json!({"version":"v6","schema_version":"v0","objectstore-format":"v6","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":[{"log_source_format":"json","fields":[]}]});
        let updated_stream_metadata = super::v5_v6(stream_metadata.clone());
        assert_eq!(updated_stream_metadata, expected);
    }

    #[test]
    fn test_v5_v6_invalid_log_source() {
        let stream_metadata = serde_json::json!({"version":"v5","schema_version":"v0","objectstore-format":"v5","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":{"log_source": "Invalid"}});
        let expected = serde_json::json!({"version":"v6","schema_version":"v0","objectstore-format":"v6","created-at":"2025-03-10T14:38:29.355131524-04:00","first-event-at":"2025-03-10T14:38:29.356-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":3,"ingestion":70,"storage":1969},"current_stats":{"events":3,"ingestion":70,"storage":1969},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test10/date=2025-03-10/manifest.json","time_lower_bound":"2025-03-10T00:00:00Z","time_upper_bound":"2025-03-10T23:59:59.999999999Z","events_ingested":3,"ingestion_size":70,"storage_size":1969}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":[{"log_source_format":"json","fields":[]}]});
        let updated_stream_metadata = super::v5_v6(stream_metadata.clone());
        assert_eq!(updated_stream_metadata, expected);
    }

    #[test]
    fn test_rename_log_source_v6() {
        let stream_metadata = serde_json::json!({"version":"v6","schema_version":"v1","objectstore-format":"v6","created-at":"2025-03-25T02:37:00.664625075+00:00","first-event-at":"2025-03-24T22:37:00.665-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":94,"ingestion":146530,"storage":29248},"current_stats":{"events":94,"ingestion":146530,"storage":29248},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test11/date=2025-03-25/manifest.json","time_lower_bound":"2025-03-25T00:00:00Z","time_upper_bound":"2025-03-25T23:59:59.999999999Z","events_ingested":94,"ingestion_size":146530,"storage_size":29248}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":[{"log_source_format":"OtelLogs","fields":["span_id","trace_id","time_unix_nano","severity_text","severity_number","body"]},{"log_source_format":"OtelTraces","fields":["span_status_code","flags","span_parent_span_id","span_trace_id","span_status_message","event_name","span_span_id","span_name","span_kind_description","event_time_unix_nano","span_end_time_unix_nano","span_status_description","span_start_time_unix_nano","span_kind","name"]},{"log_source_format":"OtelMetrics","fields":["metric_unit","start_time_unix_nano","time_unix_nano","metric_name","metric_description"]}]});
        let expected = serde_json::json!({"version":"v6","schema_version":"v1","objectstore-format":"v6","created-at":"2025-03-25T02:37:00.664625075+00:00","first-event-at":"2025-03-24T22:37:00.665-04:00","owner":{"id":"admin","group":"admin"},"permissions":[{"id":"admin","group":"admin","access":["all"]}],"stats":{"lifetime_stats":{"events":94,"ingestion":146530,"storage":29248},"current_stats":{"events":94,"ingestion":146530,"storage":29248},"deleted_stats":{"events":0,"ingestion":0,"storage":0}},"snapshot":{"version":"v2","manifest_list":[{"manifest_path":"home/nikhilsinha/Parseable/parseable/data/test11/date=2025-03-25/manifest.json","time_lower_bound":"2025-03-25T00:00:00Z","time_upper_bound":"2025-03-25T23:59:59.999999999Z","events_ingested":94,"ingestion_size":146530,"storage_size":29248}]},"hot_tier_enabled":false,"stream_type":"UserDefined","log_source":[{"log_source_format":"otel-logs","fields":["span_id","trace_id","time_unix_nano","severity_text","severity_number","body"]},{"log_source_format":"otel-traces","fields":["span_status_code","flags","span_parent_span_id","span_trace_id","span_status_message","event_name","span_span_id","span_name","span_kind_description","event_time_unix_nano","span_end_time_unix_nano","span_status_description","span_start_time_unix_nano","span_kind","name"]},{"log_source_format":"otel-metrics","fields":["metric_unit","start_time_unix_nano","time_unix_nano","metric_name","metric_description"]}]});
        let updated_stream_metadata = super::rename_log_source_v6(stream_metadata.clone());
        assert_eq!(updated_stream_metadata, expected);
    }
}
