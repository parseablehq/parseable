/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use anyhow::{Error as AnyError, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    handlers::TelemetryType,
    metadata::SchemaVersion,
    storage::StreamType,
    utils::arrow::{add_parseable_fields, get_field},
};

use super::{DEFAULT_TIMESTAMP_KEY, Event};

pub mod json;
pub mod known_schema;

static TIME_FIELD_NAME_PARTS: [&str; 11] = [
    "time",
    "date",
    "timestamp",
    "created",
    "received",
    "ingested",
    "collected",
    "start",
    "end",
    "ts",
    "dt",
];
type EventSchema = Vec<Arc<Field>>;

/// Normalizes a field name by replacing leading '@' with '_'.
/// Fields starting with '@' are renamed to start with '_'.
#[inline]
pub fn normalize_field_name(name: &mut String) {
    if let Some(stripped) = name.strip_prefix('@') {
        *name = format!("_{}", stripped);
    }
}

/// Source of the logs, used to perform special processing for certain sources
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum LogSource {
    // AWS Kinesis sends logs in the format of a json array
    #[serde(rename = "kinesis")]
    Kinesis,
    // OpenTelemetry sends logs according to the specification as explained here
    // https://github.com/open-telemetry/opentelemetry-proto/tree/v1.0.0/opentelemetry/proto/logs/v1
    #[serde(rename = "otel-logs")]
    OtelLogs,
    // OpenTelemetry sends traces according to the specification as explained here
    // https://github.com/open-telemetry/opentelemetry-proto/blob/v1.0.0/opentelemetry/proto/trace/v1/trace.proto
    #[serde(rename = "otel-metrics")]
    OtelMetrics,
    // OpenTelemetry sends traces according to the specification as explained here
    // https://github.com/open-telemetry/opentelemetry-proto/tree/v1.0.0/opentelemetry/proto/metrics/v1
    #[serde(rename = "otel-traces")]
    OtelTraces,
    // Internal Stream format
    #[serde(rename = "pmeta")]
    Pmeta,
    #[default]
    #[serde(rename = "json")]
    // Json object or array
    Json,
    // Custom Log Sources e.g. "syslog"
    #[serde(untagged)]
    Custom(String),
}

impl From<&str> for LogSource {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "kinesis" => LogSource::Kinesis,
            "otel-logs" => LogSource::OtelLogs,
            "otel-metrics" => LogSource::OtelMetrics,
            "otel-traces" => LogSource::OtelTraces,
            "pmeta" => LogSource::Pmeta,
            "" | "json" => LogSource::Json,
            custom => LogSource::Custom(custom.to_owned()),
        }
    }
}

impl Display for LogSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LogSource::Kinesis => "kinesis",
            LogSource::OtelLogs => "otel-logs",
            LogSource::OtelMetrics => "otel-metrics",
            LogSource::OtelTraces => "otel-traces",
            LogSource::Json => "json",
            LogSource::Pmeta => "pmeta",
            LogSource::Custom(custom) => custom,
        })
    }
}

/// Contains the format name and a list of known field names that are associated with the said format.
/// Stored on disk as part of `ObjectStoreFormat` in stream.json
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogSourceEntry {
    pub log_source_format: LogSource,
    pub fields: HashSet<String>,
}

impl LogSourceEntry {
    pub fn new(log_source_format: LogSource, fields: HashSet<String>) -> Self {
        LogSourceEntry {
            log_source_format,
            fields,
        }
    }
}

// Global Trait for event format
// This trait is implemented by all the event formats
pub trait EventFormat: Sized {
    type Data;

    fn to_data(
        self,
        schema: &HashMap<String, Arc<Field>>,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
        static_schema_flag: bool,
    ) -> Result<(Self::Data, EventSchema, bool), AnyError>;

    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, AnyError>;

    /// Returns the UTC time at ingestion
    fn get_p_timestamp(&self) -> DateTime<Utc>;

    fn into_recordbatch(
        self,
        storage_schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: bool,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
        p_custom_fields: &HashMap<String, String>,
    ) -> Result<(RecordBatch, bool), AnyError> {
        let p_timestamp = self.get_p_timestamp();
        let (data, schema, is_first) = self.to_data(
            storage_schema,
            time_partition,
            schema_version,
            static_schema_flag,
        )?;

        if get_field(&schema, DEFAULT_TIMESTAMP_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_TIMESTAMP_KEY
            ));
        };

        // prepare the record batch and new fields to be added
        let mut new_schema = Arc::new(Schema::new(schema));
        if !Self::is_schema_matching(new_schema.clone(), storage_schema, static_schema_flag) {
            return Err(anyhow!("Schema mismatch"));
        }
        new_schema =
            update_field_type_in_schema(new_schema, None, time_partition, None, schema_version);

        let rb = Self::decode(data, new_schema.clone())?;
        let rb = add_parseable_fields(rb, p_timestamp, p_custom_fields)?;

        Ok((rb, is_first))
    }

    fn is_schema_matching(
        new_schema: Arc<Schema>,
        storage_schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: bool,
    ) -> bool {
        if !static_schema_flag {
            return true;
        }
        for field in new_schema.fields() {
            let Some(storage_field) = storage_schema.get(field.name()) else {
                return false;
            };
            if field.name() != storage_field.name() {
                return false;
            }
            if field.data_type() != storage_field.data_type() {
                return false;
            }
        }
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn into_event(
        self,
        stream_name: String,
        origin_size: u64,
        storage_schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: bool,
        custom_partitions: Option<&String>,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
        stream_type: StreamType,
        p_custom_fields: &HashMap<String, String>,
        telemetry_type: TelemetryType,
    ) -> Result<Event, AnyError>;
}

pub fn get_existing_field_names(
    inferred_schema: Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
) -> HashSet<String> {
    let mut existing_field_names = HashSet::new();

    let Some(existing_schema) = existing_schema else {
        return existing_field_names;
    };
    for field in inferred_schema.fields.iter() {
        if existing_schema.contains_key(field.name()) {
            existing_field_names.insert(field.name().to_owned());
        }
    }

    existing_field_names
}

pub fn override_existing_timestamp_fields(
    existing_schema: &HashMap<String, Arc<Field>>,
    inferred_schema: Arc<Schema>,
) -> Arc<Schema> {
    let timestamp_field_names: HashSet<String> = existing_schema
        .values()
        .filter_map(|field| {
            if let DataType::Timestamp(TimeUnit::Millisecond, None) = field.data_type() {
                Some(field.name().to_owned())
            } else {
                None
            }
        })
        .collect();
    let updated_fields: Vec<Arc<Field>> = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            if timestamp_field_names.contains(field.name()) {
                Arc::new(Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    field.is_nullable(),
                ))
            } else {
                field.clone()
            }
        })
        .collect();

    Arc::new(Schema::new(updated_fields))
}

pub fn update_field_type_in_schema(
    inferred_schema: Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
    time_partition: Option<&String>,
    log_records: Option<&Vec<Value>>,
    schema_version: SchemaVersion,
) -> Arc<Schema> {
    let mut updated_schema = inferred_schema.clone();
    let existing_field_names = get_existing_field_names(inferred_schema.clone(), existing_schema);

    if let Some(existing_schema) = existing_schema {
        // overriding known timestamp fields which were inferred as string fields
        updated_schema = override_existing_timestamp_fields(existing_schema, updated_schema);
    }

    if let Some(log_records) = log_records {
        for log_record in log_records {
            updated_schema =
                override_data_type(updated_schema.clone(), log_record.clone(), schema_version);
        }
    }

    let Some(time_partition) = time_partition else {
        return updated_schema;
    };

    let new_schema: Vec<Field> = updated_schema
        .fields()
        .iter()
        .map(|field| {
            // time_partition field not present in existing schema with string type data as timestamp
            if field.name() == time_partition
                && !existing_field_names.contains(field.name())
                && field.data_type() == &DataType::Utf8
            {
                let new_data_type = DataType::Timestamp(TimeUnit::Millisecond, None);
                Field::new(field.name(), new_data_type, true)
            } else {
                Field::new(field.name(), field.data_type().clone(), true)
            }
        })
        .collect();
    Arc::new(Schema::new(new_schema))
}

// From Schema v1 onwards, convert json fields with name containig "date"/"time" and having
// a string value parseable into timestamp as timestamp type and all numbers as float64.
pub fn override_data_type(
    inferred_schema: Arc<Schema>,
    log_record: Value,
    schema_version: SchemaVersion,
) -> Arc<Schema> {
    let Value::Object(map) = log_record else {
        return inferred_schema;
    };
    let updated_schema: Vec<Field> = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            // Normalize field names - replace '@' prefix with '_'
            let mut field_name = field.name().to_string();
            normalize_field_name(&mut field_name);
            match (schema_version, map.get(field.name())) {
                // in V1 for new fields in json named "time"/"date" or such and having inferred
                // type string, that can be parsed as timestamp, use the timestamp type.
                // NOTE: support even more datetime string formats
                (SchemaVersion::V1, Some(Value::String(s)))
                    if TIME_FIELD_NAME_PARTS
                        .iter()
                        .any(|part| field_name.to_lowercase().contains(part))
                        && field.data_type() == &DataType::Utf8
                        && (DateTime::parse_from_rfc3339(s).is_ok()
                            || DateTime::parse_from_rfc2822(s).is_ok()) =>
                {
                    // Update the field's data type to Timestamp
                    Field::new(
                        field_name,
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    )
                }
                // in V1 for new fields in json with inferred type number, cast as float64.
                (SchemaVersion::V1, Some(Value::Number(_))) if field.data_type().is_numeric() => {
                    // Update the field's data type to Float64
                    Field::new(field_name, DataType::Float64, true)
                }
                // Return the original field if no update is needed
                _ => Field::new(field_name, field.data_type().clone(), true),
            }
        })
        .collect();

    Arc::new(Schema::new(updated_schema))
}

/// Returns a short suffix string for a given DataType to be used in field renaming
/// when schema conflicts occur.
pub fn get_datatype_suffix(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Null => "null",
        DataType::Boolean => "bool",
        DataType::Int8 => "int8",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Float16 => "float16",
        DataType::Float32 => "float32",
        DataType::Float64 => "float64",
        DataType::Utf8 | DataType::LargeUtf8 => "utf8",
        DataType::Binary | DataType::LargeBinary => "binary",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(TimeUnit::Second, _) => "timestamp_s",
        DataType::Timestamp(TimeUnit::Millisecond, _) => "timestamp_ms",
        DataType::Timestamp(TimeUnit::Microsecond, _) => "timestamp_us",
        DataType::Timestamp(TimeUnit::Nanosecond, _) => "timestamp_ns",
        DataType::Time32(_) | DataType::Time64(_) => "time",
        DataType::List(_) | DataType::LargeList(_) => "list",
        DataType::Struct(_) => "struct",
        DataType::Map(_, _) => "map",
        _ => "unknown",
    }
}

/// Checks if a JSON value can be successfully parsed/coerced to the target data type.
/// This is used to determine if a type mismatch is a real conflict or just
/// an inference limitation (e.g., timestamp strings are inferred as Utf8).
/// The schema_version affects how strict the type checking is (V1 is more lenient).
fn value_compatible_with_type(
    value: &Value,
    target_type: &DataType,
    schema_version: SchemaVersion,
) -> bool {
    match target_type {
        DataType::Timestamp(_, _) => {
            // Timestamps can accept strings that parse as datetime or numbers
            match value {
                Value::String(s) => {
                    chrono::DateTime::parse_from_rfc3339(s).is_ok()
                        || chrono::DateTime::parse_from_rfc2822(s).is_ok()
                        || chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
                        || chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok()
                        || chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").is_ok()
                        || chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").is_ok()
                }
                Value::Number(_) => true,
                _ => false,
            }
        }
        DataType::Float64 | DataType::Float32 | DataType::Float16 => {
            // V1 allows any numeric JSON value; non-V1 requires strict f64
            match schema_version {
                SchemaVersion::V1 => value.is_number(),
                _ => value.is_f64(),
            }
        }
        DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => value.is_i64(),
        DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => value.is_u64(),
        DataType::Boolean => value.is_boolean(),
        DataType::Utf8 | DataType::LargeUtf8 => {
            // Arrow JSON decoder only accepts actual JSON strings for Utf8 fields
            // It does NOT coerce numbers/booleans to strings
            value.is_string()
        }
        _ => false,
    }
}

/// Detects schema conflicts between the inferred schema and existing stream schema.
/// Returns a HashMap mapping original field names to new field names (with datatype suffix)
/// for fields that have conflicting types.
/// Takes JSON values to check if values are actually compatible with existing types.
pub fn detect_schema_conflicts(
    inferred_schema: &Schema,
    existing_schema: &HashMap<String, Arc<Field>>,
    values: &[Value],
    schema_version: SchemaVersion,
) -> HashMap<String, String> {
    let mut conflicts = HashMap::new();

    for field in inferred_schema.fields() {
        if let Some(existing_field) = existing_schema.get(field.name()) {
            // Check if data types are different (potential conflict)
            if existing_field.data_type() != field.data_type() && !field.data_type().is_null() {
                // Before declaring conflict, check if all values can be coerced to existing type
                let all_values_compatible = values.iter().all(|v| {
                    if let Some(field_value) = v.get(field.name()) {
                        if field_value.is_null() {
                            return true; // null is compatible with any type
                        }
                        value_compatible_with_type(
                            field_value,
                            existing_field.data_type(),
                            schema_version,
                        )
                    } else {
                        true // field not present in this value, no conflict
                    }
                });

                // Only mark as conflict if values cannot be coerced to existing type
                if !all_values_compatible {
                    let suffix = get_datatype_suffix(field.data_type());
                    let new_name = format!("{}_{}", field.name(), suffix);
                    conflicts.insert(field.name().to_string(), new_name);
                }
            }
        }
    }

    conflicts
}

/// Renames fields in JSON values according to the provided field mapping.
/// Used to resolve schema conflicts by renaming fields with conflicting types.
pub fn rename_conflicting_fields_in_json(
    values: Vec<Value>,
    field_mapping: &HashMap<String, String>,
) -> Vec<Value> {
    if field_mapping.is_empty() {
        return values;
    }

    values
        .into_iter()
        .map(|value| {
            if let Value::Object(map) = value {
                let new_map: serde_json::Map<String, Value> = map
                    .into_iter()
                    .map(|(key, val)| {
                        if let Some(new_key) = field_mapping.get(&key) {
                            (new_key.clone(), val)
                        } else {
                            (key, val)
                        }
                    })
                    .collect();
                Value::Object(new_map)
            } else {
                value
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_get_datatype_suffix() {
        assert_eq!(get_datatype_suffix(&DataType::Boolean), "bool");
        assert_eq!(get_datatype_suffix(&DataType::Int64), "int64");
        assert_eq!(get_datatype_suffix(&DataType::Float64), "float64");
        assert_eq!(get_datatype_suffix(&DataType::Utf8), "utf8");
        assert_eq!(
            get_datatype_suffix(&DataType::Timestamp(TimeUnit::Millisecond, None)),
            "timestamp_ms"
        );
        assert_eq!(
            get_datatype_suffix(&DataType::Timestamp(TimeUnit::Second, None)),
            "timestamp_s"
        );
    }

    #[test]
    fn test_detect_schema_conflicts() {
        // Create an existing schema with a field "body_timestamp" as Float64
        let mut existing_schema: HashMap<String, Arc<Field>> = HashMap::new();
        existing_schema.insert(
            "body_timestamp".to_string(),
            Arc::new(Field::new("body_timestamp", DataType::Float64, true)),
        );
        existing_schema.insert(
            "message".to_string(),
            Arc::new(Field::new("message", DataType::Utf8, true)),
        );

        // Create an inferred schema with "body_timestamp" as Timestamp(ms)
        let inferred_schema = Schema::new(vec![
            Field::new(
                "body_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("message", DataType::Utf8, true),
        ]);

        // Values that cannot be parsed as Float64
        let values = vec![json!({"body_timestamp": "2025-01-01T00:00:00Z", "message": "hello"})];

        let conflicts = detect_schema_conflicts(
            &inferred_schema,
            &existing_schema,
            &values,
            SchemaVersion::V1,
        );

        // Should detect conflict for body_timestamp (timestamp string can't be parsed as Float64)
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts.get("body_timestamp"),
            Some(&"body_timestamp_timestamp_ms".to_string())
        );
    }

    #[test]
    fn test_detect_schema_conflicts_no_conflicts() {
        let mut existing_schema: HashMap<String, Arc<Field>> = HashMap::new();
        existing_schema.insert(
            "message".to_string(),
            Arc::new(Field::new("message", DataType::Utf8, true)),
        );

        let inferred_schema = Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("new_field", DataType::Int64, true),
        ]);

        let values = vec![json!({"message": "hello", "new_field": 123})];

        let conflicts = detect_schema_conflicts(
            &inferred_schema,
            &existing_schema,
            &values,
            SchemaVersion::V1,
        );

        // No conflicts - types match for existing field
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_rename_conflicting_fields_in_json() {
        let values = vec![
            json!({"body_timestamp": "2025-01-01T00:00:00Z", "message": "hello"}),
            json!({"body_timestamp": "2025-01-02T00:00:00Z", "message": "world"}),
        ];

        let mut field_mapping = HashMap::new();
        field_mapping.insert(
            "body_timestamp".to_string(),
            "body_timestamp_timestamp_ms".to_string(),
        );

        let renamed = rename_conflicting_fields_in_json(values, &field_mapping);

        assert_eq!(renamed.len(), 2);
        assert!(renamed[0].get("body_timestamp_timestamp_ms").is_some());
        assert!(renamed[0].get("body_timestamp").is_none());
        assert!(renamed[0].get("message").is_some());
    }

    #[test]
    fn test_rename_conflicting_fields_in_json_empty_mapping() {
        let values = vec![json!({"body_timestamp": "2025-01-01T00:00:00Z"})];

        let field_mapping = HashMap::new();
        let renamed = rename_conflicting_fields_in_json(values.clone(), &field_mapping);

        // Should return values unchanged
        assert_eq!(renamed, values);
    }

    #[test]
    fn test_detect_schema_conflicts_timestamp_vs_utf8() {
        // Existing schema has body_timestamp as Timestamp
        let mut existing_schema: HashMap<String, Arc<Field>> = HashMap::new();
        existing_schema.insert(
            "body_timestamp".to_string(),
            Arc::new(Field::new(
                "body_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
        );

        // New event has body_timestamp as Utf8 with a value that can't be parsed as timestamp
        let inferred_schema = Schema::new(vec![Field::new("body_timestamp", DataType::Utf8, true)]);

        // Value that cannot be parsed as timestamp
        let values = vec![json!({"body_timestamp": "not a timestamp"})];

        let conflicts = detect_schema_conflicts(
            &inferred_schema,
            &existing_schema,
            &values,
            SchemaVersion::V1,
        );

        // Should detect conflict and rename to body_timestamp_utf8
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts.get("body_timestamp"),
            Some(&"body_timestamp_utf8".to_string())
        );
    }

    #[test]
    fn test_detect_schema_conflicts_compatible_timestamp() {
        // Existing schema has source_time as Timestamp
        let mut existing_schema: HashMap<String, Arc<Field>> = HashMap::new();
        existing_schema.insert(
            "source_time".to_string(),
            Arc::new(Field::new(
                "source_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
        );

        // New event has source_time inferred as Utf8 but the value IS a valid timestamp
        let inferred_schema = Schema::new(vec![Field::new("source_time", DataType::Utf8, true)]);

        // Value that CAN be parsed as timestamp
        let values = vec![json!({"source_time": "2026-02-13T03:16:47.582"})];

        let conflicts = detect_schema_conflicts(
            &inferred_schema,
            &existing_schema,
            &values,
            SchemaVersion::V1,
        );

        // Should NOT detect conflict because the value can be parsed as timestamp
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_detect_schema_conflicts_number_to_utf8() {
        // Existing schema has request_body as Utf8
        let mut existing_schema: HashMap<String, Arc<Field>> = HashMap::new();
        existing_schema.insert(
            "request_body".to_string(),
            Arc::new(Field::new("request_body", DataType::Utf8, true)),
        );

        // New event has request_body inferred as Float64 (number value)
        let inferred_schema =
            Schema::new(vec![Field::new("request_body", DataType::Float64, true)]);

        // Value is a number, which cannot be coerced to Utf8 by Arrow JSON decoder
        let values = vec![json!({"request_body": 200})];

        let conflicts = detect_schema_conflicts(
            &inferred_schema,
            &existing_schema,
            &values,
            SchemaVersion::V1,
        );

        // Should detect conflict because number cannot be stored in Utf8 field
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts.get("request_body"),
            Some(&"request_body_float64".to_string())
        );
    }

    #[test]
    fn test_detect_schema_conflicts_int_to_float_non_v1() {
        // Existing schema has span_kind as Float64
        let mut existing_schema: HashMap<String, Arc<Field>> = HashMap::new();
        existing_schema.insert(
            "span_kind".to_string(),
            Arc::new(Field::new("span_kind", DataType::Float64, true)),
        );

        // New event has span_kind inferred as Int64 (integer value)
        let inferred_schema = Schema::new(vec![Field::new("span_kind", DataType::Int64, true)]);

        // Value is an integer (not f64)
        let values = vec![json!({"span_kind": 1})];

        // With non-V1 schema, integers should NOT be compatible with Float64
        let conflicts = detect_schema_conflicts(
            &inferred_schema,
            &existing_schema,
            &values,
            SchemaVersion::V0,
        );

        // Should detect conflict because integer doesn't match strict Float64 validation
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts.get("span_kind"),
            Some(&"span_kind_int64".to_string())
        );
    }

    #[test]
    fn test_detect_schema_conflicts_int_to_float_v1() {
        // Existing schema has span_kind as Float64
        let mut existing_schema: HashMap<String, Arc<Field>> = HashMap::new();
        existing_schema.insert(
            "span_kind".to_string(),
            Arc::new(Field::new("span_kind", DataType::Float64, true)),
        );

        // New event has span_kind inferred as Int64 (integer value)
        let inferred_schema = Schema::new(vec![Field::new("span_kind", DataType::Int64, true)]);

        // Value is an integer
        let values = vec![json!({"span_kind": 1})];

        // With V1 schema, integers ARE compatible with Float64
        let conflicts = detect_schema_conflicts(
            &inferred_schema,
            &existing_schema,
            &values,
            SchemaVersion::V1,
        );

        // Should NOT detect conflict because V1 allows any number for Float64
        assert!(conflicts.is_empty());
    }
}
