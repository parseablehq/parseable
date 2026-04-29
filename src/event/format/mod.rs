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
use tracing::info_span;

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
        infer_timestamp: bool,
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
        infer_timestamp: bool,
    ) -> Result<(RecordBatch, bool), AnyError> {
        let _span = info_span!("into_recordbatch").entered();
        let p_timestamp = self.get_p_timestamp();
        let (data, schema, is_first) = self.to_data(
            storage_schema,
            time_partition,
            schema_version,
            static_schema_flag,
            infer_timestamp,
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
        new_schema = update_field_type_in_schema(
            new_schema,
            None,
            time_partition,
            None,
            schema_version,
            infer_timestamp,
        );

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
        tenant_id: &Option<String>,
        infer_timestamp: bool,
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
    infer_timestamp: bool,
) -> Arc<Schema> {
    let mut updated_schema = inferred_schema.clone();
    let existing_field_names = get_existing_field_names(inferred_schema.clone(), existing_schema);

    if let Some(existing_schema) = existing_schema {
        // overriding known timestamp fields which were inferred as string fields
        updated_schema = override_existing_timestamp_fields(existing_schema, updated_schema);
    }

    if let Some(log_records) = log_records {
        for log_record in log_records {
            updated_schema = override_data_type(
                updated_schema.clone(),
                log_record.clone(),
                schema_version,
                infer_timestamp,
            );
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
    infer_timestamp: bool,
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
                // in V1 for new fields in json named "time"/"date" or such and having
                // inferred type string, that can be parsed as timestamp, use the
                // timestamp type. Gated on `infer_timestamp` (default true) — settable
                // per dataset (otel-metrics) via x-p-infer-timestamp=false.
                // NOTE: support even more datetime string formats
                (SchemaVersion::V1, Some(Value::String(s)))
                    if infer_timestamp
                        && TIME_FIELD_NAME_PARTS
                            .iter()
                            .any(|part| field_name.to_lowercase().contains(part))
                        && field.data_type() == &DataType::Utf8
                        && (DateTime::parse_from_rfc3339(s).is_ok()
                            || DateTime::parse_from_rfc2822(s).is_ok()) =>
                {
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
                // Arrow JSON decoder with coerce_primitive(false) cannot decode
                // a JSON number into a Timestamp field — it expects a string.
                Value::Number(_) => false,
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

/// Per-record fallback when batch-level conflict detection misses a type
/// mismatch. This happens when a single batch contains records with
/// incompatible JSON types for the same field (e.g. record A has
/// `"escaped": "true"` and record B has `"escaped": true`). Arrow's
/// inference resolves to a single type for the batch (Utf8 wins over Bool),
/// so `detect_schema_conflicts` may see "inferred Utf8 == storage Utf8" and
/// not flag a conflict — even though record B's bool would later fail
/// `fields_mismatch`.
///
/// For each record, scan each non-null field. If the value doesn't satisfy
/// the storage type, rename that field in this specific record to
/// `<original>_<value-type-suffix>` so it routes to (or creates) a typed
/// sibling column instead of crashing the batch.
///
/// This is a no-op (and skips the per-record loop entirely) when:
///   - the batch has at most one record — for a single record arrow's
///     inferred type IS the value's type, so `detect_schema_conflicts` has
///     already handled it, or
///   - no inferred field shares both name and type with storage — meaning
///     arrow couldn't have absorbed a mixed-type record into the storage
///     type, so per-record mismatches are impossible.
pub fn rename_per_record_type_mismatches(
    values: Vec<Value>,
    inferred_schema: &Schema,
    existing_schema: &HashMap<String, Arc<Field>>,
    schema_version: SchemaVersion,
) -> Vec<Value> {
    if values.len() <= 1 || existing_schema.is_empty() {
        return values;
    }
    // Bail out unless at least one inferred field collides with storage at
    // the same type. Without that, arrow's inference can't have hidden a
    // mixed-type batch behind a matching aggregate type.
    let needs_check = inferred_schema.fields().iter().any(|f| {
        existing_schema
            .get(f.name())
            .is_some_and(|s| s.data_type() == f.data_type())
    });
    if !needs_check {
        return values;
    }

    values
        .into_iter()
        .map(|value| {
            let Value::Object(map) = value else {
                return value;
            };
            let new_map: serde_json::Map<String, Value> = map
                .into_iter()
                .map(|(key, val)| {
                    if val.is_null() {
                        return (key, val);
                    }
                    let Some(existing_field) = existing_schema.get(&key) else {
                        return (key, val);
                    };
                    if value_compatible_with_type(&val, existing_field.data_type(), schema_version)
                    {
                        return (key, val);
                    }
                    let suffix = get_datatype_suffix(&datatype_for_value(&val));
                    let new_key = format!("{key}_{suffix}");
                    (new_key, val)
                })
                .collect();
            Value::Object(new_map)
        })
        .collect()
}

/// Best-effort mapping from a JSON value to its arrow DataType. Used only to
/// pick a type-suffix for the per-record rename (e.g. "bool", "i64", "f64").
fn datatype_for_value(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Null,
        Value::Bool(_) => DataType::Boolean,
        Value::Number(n) if n.is_i64() => DataType::Int64,
        Value::Number(n) if n.is_u64() => DataType::UInt64,
        Value::Number(_) => DataType::Float64,
        Value::String(_) => DataType::Utf8,
        Value::Array(_) => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        Value::Object(_) => DataType::Struct(arrow_schema::Fields::default()),
    }
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

    #[test]
    fn override_data_type_converts_time_field_when_infer_enabled() {
        let inferred = Arc::new(Schema::new(vec![Field::new(
            "start_time",
            DataType::Utf8,
            true,
        )]));
        let log = json!({"start_time": "2025-01-01T00:00:00Z"});

        let updated = override_data_type(inferred, log, SchemaVersion::V1, true);

        assert_eq!(
            updated.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn override_data_type_keeps_utf8_when_infer_disabled() {
        let inferred = Arc::new(Schema::new(vec![Field::new(
            "start_time",
            DataType::Utf8,
            true,
        )]));
        let log = json!({"start_time": "2025-01-01T00:00:00Z"});

        let updated = override_data_type(inferred, log, SchemaVersion::V1, false);

        // With infer_timestamp=false, time-named string fields stay Utf8.
        assert_eq!(updated.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn override_data_type_still_casts_numbers_when_infer_disabled() {
        // The flag must only affect string-to-timestamp inference, not
        // V1's number-to-float64 promotion.
        let inferred = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
            true,
        )]));
        let log = json!({"count": 7});

        let updated = override_data_type(inferred, log, SchemaVersion::V1, false);

        assert_eq!(updated.field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn override_data_type_no_effect_on_v0_schema() {
        // The inference happens only on V1; V0 must be untouched regardless of the flag.
        let inferred = Arc::new(Schema::new(vec![Field::new(
            "start_time",
            DataType::Utf8,
            true,
        )]));
        let log = json!({"start_time": "2025-01-01T00:00:00Z"});

        let v0_with_flag =
            override_data_type(inferred.clone(), log.clone(), SchemaVersion::V0, true);
        assert_eq!(v0_with_flag.field(0).data_type(), &DataType::Utf8);

        let v0_no_flag = override_data_type(inferred, log, SchemaVersion::V0, false);
        assert_eq!(v0_no_flag.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn update_field_type_in_schema_respects_infer_flag() {
        let inferred = Arc::new(Schema::new(vec![Field::new(
            "received_at",
            DataType::Utf8,
            true,
        )]));
        let log_records = vec![json!({"received_at": "2025-01-01T00:00:00Z"})];

        let with_flag = update_field_type_in_schema(
            inferred.clone(),
            None,
            None,
            Some(&log_records),
            SchemaVersion::V1,
            true,
        );
        assert_eq!(
            with_flag.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );

        let without_flag = update_field_type_in_schema(
            inferred,
            None,
            None,
            Some(&log_records),
            SchemaVersion::V1,
            false,
        );
        assert_eq!(without_flag.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn override_data_type_keeps_integer_in_time_named_field_as_float64() {
        // Integer values for time-named fields are NOT inferred as timestamps —
        // {"time": 1234} could be a counter / ID / duration and silently treating
        // it as 1970-01-01 00:00:01.234 UTC would be wrong. Numbers fall through
        // to the V1 number-to-float64 arm.
        for value in [0_i64, 1, 1234, 1_735_689_600_000] {
            let inferred = Arc::new(Schema::new(vec![Field::new("time", DataType::Int64, true)]));
            let log = json!({ "time": value });
            let updated = override_data_type(inferred, log, SchemaVersion::V1, true);
            assert_eq!(
                updated.field(0).data_type(),
                &DataType::Float64,
                "value {value} should not be promoted to timestamp",
            );
        }
    }

    #[test]
    fn override_data_type_keeps_float_in_time_named_field_as_float64() {
        // Floats can never decode into Timestamp via Arrow's JSON reader; they
        // stay Float64.
        let inferred = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Float64,
            true,
        )]));
        let log = json!({"timestamp": 1735689600.5_f64});

        let updated = override_data_type(inferred, log, SchemaVersion::V1, true);
        assert_eq!(updated.field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn rename_per_record_renames_only_offending_record() {
        // Mixed-type batch: arrow infers Utf8 (string wins), so
        // detect_schema_conflicts misses the bool record. The per-record pass
        // must catch it.
        let mut storage: HashMap<String, Arc<Field>> = HashMap::new();
        storage.insert(
            "escaped".to_string(),
            Arc::new(Field::new("escaped", DataType::Utf8, true)),
        );
        // Mirrors what arrow's inference would produce for this mixed batch.
        let inferred = Schema::new(vec![Field::new("escaped", DataType::Utf8, true)]);

        let value_arr = vec![
            json!({"escaped": "true"}),
            json!({"escaped": true}),
            json!({"escaped": null}),
        ];
        let renamed =
            rename_per_record_type_mismatches(value_arr, &inferred, &storage, SchemaVersion::V1);

        assert!(renamed[0].as_object().unwrap().contains_key("escaped"));
        // bool record routes to escaped_bool
        assert!(renamed[1].as_object().unwrap().contains_key("escaped_bool"));
        assert!(!renamed[1].as_object().unwrap().contains_key("escaped"));
        // null is compatible with any type
        assert!(renamed[2].as_object().unwrap().contains_key("escaped"));
    }

    #[test]
    fn rename_per_record_skips_compatible_values() {
        // V1: any number is compatible with Float64 columns -> no rename.
        let mut storage: HashMap<String, Arc<Field>> = HashMap::new();
        storage.insert(
            "amount".to_string(),
            Arc::new(Field::new("amount", DataType::Float64, true)),
        );
        let inferred = Schema::new(vec![Field::new("amount", DataType::Float64, true)]);
        let renamed = rename_per_record_type_mismatches(
            vec![json!({"amount": 5}), json!({"amount": 2.5})],
            &inferred,
            &storage,
            SchemaVersion::V1,
        );
        for v in &renamed {
            assert!(v.as_object().unwrap().contains_key("amount"));
        }
    }

    #[test]
    fn rename_per_record_skips_unknown_fields() {
        // Fields not in storage are passed through (let arrow infer them fresh).
        let storage: HashMap<String, Arc<Field>> = HashMap::new();
        let inferred = Schema::new(vec![Field::new("new_field", DataType::Boolean, true)]);
        let renamed = rename_per_record_type_mismatches(
            vec![json!({"new_field": true}), json!({"new_field": false})],
            &inferred,
            &storage,
            SchemaVersion::V1,
        );
        assert!(renamed[0].as_object().unwrap().contains_key("new_field"));
    }

    #[test]
    fn rename_per_record_number_into_utf8_uses_int64_suffix() {
        let mut storage: HashMap<String, Arc<Field>> = HashMap::new();
        storage.insert(
            "request_body".to_string(),
            Arc::new(Field::new("request_body", DataType::Utf8, true)),
        );
        let inferred = Schema::new(vec![Field::new("request_body", DataType::Utf8, true)]);
        // Need >= 2 records for the per-record pass to run; second record's
        // string keeps inferred type at Utf8 (matches storage), unblocking
        // the precheck so the int record gets renamed.
        let renamed = rename_per_record_type_mismatches(
            vec![
                json!({"request_body": 200}),
                json!({"request_body": "hello"}),
            ],
            &inferred,
            &storage,
            SchemaVersion::V1,
        );
        assert!(
            renamed[0]
                .as_object()
                .unwrap()
                .contains_key("request_body_int64")
        );
    }

    #[test]
    fn rename_per_record_short_circuits_for_single_record() {
        // For a single record, arrow's inferred type IS the value's type, so
        // detect_schema_conflicts has already covered everything.
        let mut storage: HashMap<String, Arc<Field>> = HashMap::new();
        storage.insert(
            "escaped".to_string(),
            Arc::new(Field::new("escaped", DataType::Utf8, true)),
        );
        // Pretend inferred (incorrectly) matches storage to ensure precheck
        // would otherwise let us in — single-record gate must short-circuit.
        let inferred = Schema::new(vec![Field::new("escaped", DataType::Utf8, true)]);
        let renamed = rename_per_record_type_mismatches(
            vec![json!({"escaped": true})],
            &inferred,
            &storage,
            SchemaVersion::V1,
        );
        // No rename because the loop is skipped for single-record batches.
        assert!(renamed[0].as_object().unwrap().contains_key("escaped"));
    }

    #[test]
    fn rename_per_record_short_circuits_when_no_field_overlap_at_same_type() {
        // No inferred field shares both name AND type with storage —
        // arrow can't have absorbed a mixed-type batch, so we skip the loop.
        let mut storage: HashMap<String, Arc<Field>> = HashMap::new();
        storage.insert(
            "escaped".to_string(),
            Arc::new(Field::new("escaped", DataType::Utf8, true)),
        );
        // Inferred has a DIFFERENT type for the shared field — handled by
        // detect_schema_conflicts as a batch-level rename, not per-record.
        let inferred = Schema::new(vec![Field::new("escaped", DataType::Boolean, true)]);
        let renamed = rename_per_record_type_mismatches(
            vec![json!({"escaped": true}), json!({"escaped": false})],
            &inferred,
            &storage,
            SchemaVersion::V1,
        );
        // Per-record loop skipped; values pass through unchanged.
        assert!(renamed[0].as_object().unwrap().contains_key("escaped"));
        assert!(renamed[1].as_object().unwrap().contains_key("escaped"));
    }

    #[test]
    fn update_field_type_in_schema_explicit_time_partition_unaffected_by_flag() {
        // When the user explicitly configures a time_partition, that field is always
        // promoted to Timestamp regardless of infer_timestamp.
        let inferred = Arc::new(Schema::new(vec![Field::new(
            "ingest_ts",
            DataType::Utf8,
            true,
        )]));
        let time_partition = "ingest_ts".to_string();

        let updated = update_field_type_in_schema(
            inferred,
            None,
            Some(&time_partition),
            None,
            SchemaVersion::V1,
            false,
        );

        assert_eq!(
            updated.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }
}
