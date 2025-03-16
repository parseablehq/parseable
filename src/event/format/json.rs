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

#![allow(deprecated)]

use anyhow::anyhow;
use arrow_array::RecordBatch;
use arrow_json::reader::{infer_json_schema_from_iterator, ReaderBuilder};
use arrow_schema::{DataType, Field, Fields, Schema};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::arrow::util::bit_util::round_upto_multiple_of_64;
use itertools::Itertools;
use opentelemetry_proto::tonic::{
    logs::v1::LogsData, metrics::v1::MetricsData, trace::v1::TracesData,
};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::Arc,
};
use tracing::error;

use super::{EventFormat, LogSource};
use crate::{
    event::{get_schema_key, PartitionEvent},
    kinesis::{flatten_kinesis_logs, Message},
    metadata::SchemaVersion,
    otel::{logs::flatten_otel_logs, metrics::flatten_otel_metrics, traces::flatten_otel_traces},
    parseable::Stream,
    utils::{
        arrow::get_field,
        json::{flatten_json_body, Json},
        time::Minute,
    },
    OBJECT_STORE_DATA_GRANULARITY,
};

struct JsonPartition {
    batch: Vec<Json>,
    schema: Vec<Arc<Field>>,
    parsed_timestamp: NaiveDateTime,
}

pub struct Event {
    pub json: Value,
    pub origin_size: usize,
    pub p_timestamp: DateTime<Utc>,
    pub log_source: LogSource,
}

impl Event {
    pub fn new(json: Value, origin_size: usize, log_source: LogSource) -> Self {
        Self {
            json,
            origin_size,
            p_timestamp: Utc::now(),
            log_source,
        }
    }

    pub fn flatten_logs(
        self,
        time_partition: Option<&String>,
        time_partition_limit: Option<NonZeroU32>,
        custom_partitions: Option<&String>,
        schema_version: SchemaVersion,
    ) -> anyhow::Result<Vec<Json>> {
        let data = match self.log_source {
            LogSource::Kinesis => {
                //custom flattening required for Amazon Kinesis
                let message: Message = serde_json::from_value(self.json)?;
                flatten_kinesis_logs(message)
            }
            LogSource::OtelLogs => {
                //custom flattening required for otel logs
                let logs: LogsData = serde_json::from_value(self.json)?;
                flatten_otel_logs(&logs)
            }
            LogSource::OtelTraces => {
                //custom flattening required for otel traces
                let traces: TracesData = serde_json::from_value(self.json)?;
                flatten_otel_traces(&traces)
            }
            LogSource::OtelMetrics => {
                //custom flattening required for otel metrics
                let metrics: MetricsData = serde_json::from_value(self.json)?;
                flatten_otel_metrics(metrics)
            }
            _ => vec![self.json],
        };

        let mut logs = vec![];
        for json in data {
            let json = flatten_json_body(
                json,
                time_partition,
                time_partition_limit,
                custom_partitions,
                schema_version,
                true,
                &self.log_source,
            )?;

            // incoming event may be a single json or a json array
            // but Data (type defined above) is a vector of json values
            // hence we need to convert the incoming event to a vector of json values
            match json {
                Value::Array(arr) => {
                    for log in arr {
                        let Value::Object(json) = log else {
                            return Err(anyhow!(
                                "Expected an object or a list of objects, received: {log:?}"
                            ));
                        };
                        logs.push(json);
                    }
                }
                Value::Object(obj) => logs.push(obj),
                _ => unreachable!("flatten would have failed beforehand"),
            }
        }

        Ok(logs)
    }
}

impl EventFormat for Event {
    type Data = Json;

    // convert the incoming json to a vector of json values
    // also extract the arrow schema, tags and metadata from the incoming json
    fn to_data(
        self,
        time_partition: Option<&String>,
        time_partition_limit: Option<NonZeroU32>,
        custom_partitions: Option<&String>,
        schema_version: SchemaVersion,
    ) -> anyhow::Result<Vec<Self::Data>> {
        self.flatten_logs(
            time_partition,
            time_partition_limit,
            custom_partitions,
            schema_version,
        )
    }

    fn infer_schema(
        data: &Self::Data,
        stored_schema: &HashMap<String, Arc<Field>>,
        time_partition: Option<&String>,
        static_schema_flag: bool,
        schema_version: SchemaVersion,
    ) -> anyhow::Result<(super::EventSchema, bool)> {
        // collect all the keys from all the json objects in the request body
        let fields = collect_keys(data);

        let mut is_first = false;
        let schema = if let Some(schema) = derive_arrow_schema(stored_schema, fields) {
            schema
        } else {
            // TODO:
            let mut infer_schema =
                infer_json_schema_from_iterator([Ok(Value::Object(data.clone()))].into_iter())
                    .map_err(|err| {
                        anyhow!("Could not infer schema for this event due to err {:?}", err)
                    })?;
            let new_infer_schema = super::update_field_type_in_schema(
                Arc::new(infer_schema),
                Some(stored_schema),
                time_partition,
                Some(data),
                schema_version,
            );
            infer_schema = Schema::new(new_infer_schema.fields().clone());
            Schema::try_merge(vec![
                Schema::new(stored_schema.values().cloned().collect::<Fields>()),
                infer_schema.clone(),
            ])
            .map_err(|err| {
                anyhow!(
                    "Could not merge schema of this event with that of the existing stream. {:?}",
                    err
                )
            })?;
            is_first = true;
            infer_schema
                .fields
                .iter()
                .filter(|field| !field.data_type().is_null())
                .cloned()
                .sorted_by(|a, b| a.name().cmp(b.name()))
                .collect()
        };

        if fields_mismatch(&schema, data, schema_version, static_schema_flag) {
            return Err(anyhow!(
                "Could not process this event due to mismatch in datatype"
            ));
        }

        let schema = Self::prepare_and_validate_schema(schema, stored_schema, static_schema_flag)?;

        Ok((schema, is_first))
    }

    // Convert the Data type (defined above) to arrow record batch
    fn decode(data: &[Self::Data], schema: Arc<Schema>) -> anyhow::Result<RecordBatch> {
        let array_capacity = round_upto_multiple_of_64(data.len());
        let mut reader = ReaderBuilder::new(schema)
            .with_batch_size(array_capacity)
            .with_coerce_primitive(false)
            .build_decoder()?;

        reader.serialize(data)?;
        match reader.flush() {
            Ok(Some(recordbatch)) => Ok(recordbatch),
            Err(err) => Err(anyhow!("Failed to create recordbatch due to {:?}", err)),
            Ok(None) => unreachable!("all records are added to one rb"),
        }
    }

    /// Converts a JSON event into a Parseable Event
    fn into_event(self, stream: &Stream) -> anyhow::Result<super::Event> {
        let time_partition = stream.get_time_partition();
        let time_partition_limit = stream.get_time_partition_limit();
        let static_schema_flag = stream.get_static_schema_flag();
        let custom_partitions = stream.get_custom_partition();
        let schema_version = stream.get_schema_version();
        let stored_schema = stream.get_schema_raw();
        let stream_type = stream.get_stream_type();

        let p_timestamp = self.p_timestamp;
        let origin_size = self.origin_size;
        let data = self.to_data(
            time_partition.as_ref(),
            time_partition_limit,
            custom_partitions.as_ref(),
            schema_version,
        )?;

        let mut is_first_event = false;
        let mut json_partitions = HashMap::new();
        for json in data {
            let (schema, is_first) = Self::infer_schema(
                &json,
                &stored_schema,
                time_partition.as_ref(),
                static_schema_flag,
                schema_version,
            )?;

            is_first_event = is_first_event || is_first;
            let custom_partition_values = match custom_partitions.as_ref() {
                Some(custom_partitions) => {
                    let custom_partitions = custom_partitions.split(',').collect_vec();
                    extract_custom_partition_values(&json, &custom_partitions)
                }
                None => HashMap::new(),
            };

            let parsed_timestamp = match time_partition.as_ref() {
                Some(time_partition) => extract_and_parse_time(&json, time_partition)?,
                _ => p_timestamp.naive_utc(),
            };

            let prefix = generate_prefix(&schema, parsed_timestamp, &custom_partition_values);
            if let Some(JsonPartition { batch, .. }) = json_partitions.get_mut(&prefix) {
                batch.push(json)
            } else {
                json_partitions.insert(
                    prefix,
                    JsonPartition {
                        batch: vec![json],
                        schema,
                        parsed_timestamp,
                    },
                );
            }
        }

        let mut partitions = HashMap::new();
        for (
            prefix,
            JsonPartition {
                batch,
                schema,
                parsed_timestamp,
            },
        ) in json_partitions
        {
            let batch = Self::into_recordbatch(
                p_timestamp,
                &batch,
                &schema,
                time_partition.as_ref(),
                schema_version,
            )?;

            partitions.insert(
                prefix,
                PartitionEvent {
                    rb: batch,
                    parsed_timestamp,
                },
            );
        }

        Ok(super::Event {
            origin_format: "json",
            origin_size,
            is_first_event,
            partitions,
            stream_type,
        })
    }
}

fn generate_prefix(
    schema: &[Arc<Field>],
    parsed_timestamp: NaiveDateTime,
    custom_partition_values: &HashMap<String, String>,
) -> String {
    format!(
        "{}.{}.minute={}{}",
        get_schema_key(schema),
        parsed_timestamp.format("date=%Y-%m-%d.hour=%H"),
        Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
        custom_partition_values
            .iter()
            .sorted_by_key(|v| v.0)
            .map(|(key, value)| format!(".{key}={value}"))
            .join("")
    )
}

/// Extracts custom partition values from provided JSON object
/// e.g. `json: {"status": 400, "msg": "Hello, World!"}, custom_partition_list: ["status"]` returns `{"status" => 400}`
pub fn extract_custom_partition_values(
    json: &Json,
    custom_partition_list: &[&str],
) -> HashMap<String, String> {
    let mut custom_partition_values: HashMap<String, String> = HashMap::new();
    for custom_partition_field in custom_partition_list {
        let custom_partition_value = json.get(custom_partition_field.trim()).unwrap().to_owned();
        let custom_partition_value = match custom_partition_value {
            e @ Value::Number(_) | e @ Value::Bool(_) => e.to_string(),
            Value::String(s) => s,
            _ => "".to_string(),
        };
        custom_partition_values.insert(
            custom_partition_field.trim().to_string(),
            custom_partition_value,
        );
    }
    custom_partition_values
}

/// Returns the parsed timestamp of deignated time partition from json object
/// e.g. `json: {"timestamp": "2025-05-15T15:30:00Z"}` returns `2025-05-15T15:30:00`
fn extract_and_parse_time(json: &Json, time_partition: &str) -> anyhow::Result<NaiveDateTime> {
    let current_time = json
        .get(time_partition)
        .ok_or_else(|| anyhow!("Missing field for time partition in json: {time_partition}"))?;
    let parsed_time: DateTime<Utc> = serde_json::from_value(current_time.clone())?;

    Ok(parsed_time.naive_utc())
}

// Returns arrow schema with the fields that are present in the request body
// This schema is an input to convert the request body to arrow record batch
// Returns None if even one of the fields in the json is new and not seen before
fn derive_arrow_schema(
    schema: &HashMap<String, Arc<Field>>,
    fields: HashSet<&str>,
) -> Option<Vec<Arc<Field>>> {
    let mut res = Vec::with_capacity(fields.len());
    for field_name in fields {
        let field = schema.get(field_name)?;
        res.push(field.clone())
    }

    Some(res)
}

// Returns a list of keys that are present in the given iterable of JSON objects
// Returns None if even one of the value is not an Object
fn collect_keys(object: &Json) -> HashSet<&str> {
    object.keys().map(|k| k.as_str()).collect()
}

// Returns true when the field doesn't exist in schema or has an invalid type
fn fields_mismatch(
    schema: &[Arc<Field>],
    body: &Json,
    schema_version: SchemaVersion,
    static_schema_flag: bool,
) -> bool {
    body.iter().any(|(key, value)| {
        !value.is_null()
            && get_field(schema, key)
                .is_none_or(|field| !valid_type(field, value, schema_version, static_schema_flag))
    })
}

fn valid_type(
    field: &Field,
    value: &Value,
    schema_version: SchemaVersion,
    static_schema_flag: bool,
) -> bool {
    match field.data_type() {
        DataType::Boolean => value.is_boolean(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            validate_int(value, static_schema_flag)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => value.is_u64(),
        DataType::Float16 | DataType::Float32 => value.is_f64(),
        DataType::Float64 => validate_float(value, schema_version, static_schema_flag),
        DataType::Utf8 => value.is_string(),
        DataType::List(field) => validate_list(field, value, schema_version, static_schema_flag),
        DataType::Struct(fields) => {
            validate_struct(fields, value, schema_version, static_schema_flag)
        }
        DataType::Date32 => {
            if let Value::String(s) = value {
                return NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok();
            }
            false
        }
        DataType::Timestamp(_, _) => value.is_string() || value.is_number(),
        _ => {
            error!(
                "Unsupported datatype {:?}, value {:?}",
                field.data_type(),
                value
            );
            false
        }
    }
}

fn validate_int(value: &Value, static_schema_flag: bool) -> bool {
    // allow casting string to int for static schema
    if static_schema_flag {
        if let Value::String(s) = value {
            return s.trim().parse::<i64>().is_ok();
        }
    }
    value.is_i64()
}

fn validate_float(value: &Value, schema_version: SchemaVersion, static_schema_flag: bool) -> bool {
    // allow casting string to int for static schema
    if static_schema_flag {
        if let Value::String(s) = value.clone() {
            let trimmed = s.trim();
            return trimmed.parse::<f64>().is_ok() || trimmed.parse::<i64>().is_ok();
        }
        return value.is_number();
    }
    match schema_version {
        SchemaVersion::V1 => value.is_number(),
        _ => value.is_f64(),
    }
}

fn validate_list(
    field: &Field,
    value: &Value,
    schema_version: SchemaVersion,
    static_schema_flag: bool,
) -> bool {
    if let Value::Array(arr) = value {
        for elem in arr {
            if elem.is_null() {
                continue;
            }
            if !valid_type(field, elem, schema_version, static_schema_flag) {
                return false;
            }
        }
    }
    true
}

fn validate_struct(
    fields: &Fields,
    value: &Value,
    schema_version: SchemaVersion,
    static_schema_flag: bool,
) -> bool {
    if let Value::Object(val) = value {
        for (key, value) in val {
            let field = fields.iter().find(|f| f.name() == key);

            if let Some(field) = field {
                if value.is_null() {
                    continue;
                }
                if !valid_type(field, value, schema_version, static_schema_flag) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use arrow::datatypes::Int64Type;
    use arrow_array::{ArrayRef, Float64Array, Int64Array, ListArray, StringArray};
    use chrono::Timelike;
    use serde_json::json;

    use super::*;

    #[test]
    fn parse_time_parition_from_value() {
        let json = json!({"timestamp": "2025-05-15T15:30:00Z"});
        let parsed = extract_and_parse_time(json.as_object().unwrap(), "timestamp");

        let expected = NaiveDateTime::from_str("2025-05-15T15:30:00").unwrap();
        assert_eq!(parsed.unwrap(), expected);
    }

    #[test]
    fn time_parition_not_in_json() {
        let json = json!({"hello": "world!"});
        let parsed = extract_and_parse_time(json.as_object().unwrap(), "timestamp");

        assert!(parsed.is_err());
    }

    #[test]
    fn time_parition_not_parseable_as_datetime() {
        let json = json!({"timestamp": "not time"});
        let parsed = extract_and_parse_time(json.as_object().unwrap(), "timestamp");

        assert!(parsed.is_err());
    }

    trait TestExt {
        fn as_int64_arr(&self) -> Option<&Int64Array>;
        fn as_float64_arr(&self) -> Option<&Float64Array>;
        fn as_utf8_arr(&self) -> Option<&StringArray>;
    }

    impl TestExt for ArrayRef {
        fn as_int64_arr(&self) -> Option<&Int64Array> {
            self.as_any().downcast_ref()
        }

        fn as_float64_arr(&self) -> Option<&Float64Array> {
            self.as_any().downcast_ref()
        }

        fn as_utf8_arr(&self) -> Option<&StringArray> {
            self.as_any().downcast_ref()
        }
    }

    fn fields_to_map(iter: impl Iterator<Item = Field>) -> HashMap<String, Arc<Field>> {
        iter.map(|x| (x.name().clone(), Arc::new(x))).collect()
    }

    #[test]
    fn basic_object_into_rb() {
        let json = json!({
            "c": 4.23,
            "a": 1,
            "b": "hello",
        });

        let store_schema = HashMap::default();
        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[0], &store_schema, None, false, SchemaVersion::V0).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V0).unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 4);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from_iter_values(["hello"])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from_iter([4.23])
        );
    }

    #[test]
    fn basic_object_with_null_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
            "c": null
        });

        let store_schema = HashMap::default();
        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[0], &store_schema, None, false, SchemaVersion::V0).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V0).unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 3);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from_iter_values(["hello"])
        );
    }

    #[test]
    fn basic_object_derive_schema_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
        });

        let store_schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );
        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[0], &store_schema, None, false, SchemaVersion::V0).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V0).unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 3);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from_iter_values(["hello"])
        );
    }

    #[test]
    fn basic_object_schema_mismatch() {
        let json = json!({
            "a": 1,
            "b": 1, // type mismatch
        });

        let store_schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();

        assert!(
            Event::infer_schema(&data[0], &store_schema, None, false, SchemaVersion::V0).is_err()
        );
    }

    #[test]
    fn empty_object() {
        let json = json!({});

        let store_schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[0], &store_schema, None, false, SchemaVersion::V0).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V0).unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 1);
    }

    #[test]
    fn array_into_recordbatch_inffered_schema() {
        let json = json!([
            {
                "b": "hello",
            },
            {
                "b": "hello",
                "a": 1,
                "c": 1
            },
            {
                "a": 1,
                "b": "hello",
                "c": null
            },
        ]);

        let store_schema = HashMap::new();
        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[1], &store_schema, None, false, SchemaVersion::V0).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V0).unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 4);

        let schema = rb.schema();
        let fields = &schema.fields;

        assert_eq!(&*fields[1], &Field::new("a", DataType::Int64, true));
        assert_eq!(&*fields[2], &Field::new("b", DataType::Utf8, true));
        assert_eq!(&*fields[3], &Field::new("c", DataType::Int64, true));

        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![None, Some(1), None])
        );
    }

    #[test]
    fn arr_with_null_into_rb() {
        let json = json!([
            {
                "c": null,
                "b": "hello",
                "a": null
            },
            {
                "a": 1,
                "c": 1.22,
                "b": "hello"
            },
            {
                "b": "hello",
                "a": 1,
                "c": null
            },
        ]);

        let store_schema = HashMap::new();
        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[1], &store_schema, None, false, SchemaVersion::V0).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V0).unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 4);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![None, Some(1.22), None,])
        );
    }

    #[test]
    fn arr_with_null_derive_schema_into_rb() {
        let json = json!([
            {
                "c": null,
                "b": "hello",
                "a": null
            },
            {
                "a": 1,
                "c": 1.22,
                "b": "hello"
            },
            {
                "b": "hello",
                "a": 1,
                "c": null
            },
        ]);

        let store_schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );
        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[0], &store_schema, None, false, SchemaVersion::V0).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V0).unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 4);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![None, Some(1.22), None,])
        );
    }

    #[test]
    fn arr_schema_mismatch() {
        let json = json!(
        {
            "a": 1,
            "b": "hello",
            "c": 1
        });

        let store_schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        assert!(Event::infer_schema(
            json.as_object().unwrap(),
            &store_schema,
            None,
            false,
            SchemaVersion::V0
        )
        .is_err());
    }

    #[test]
    fn arr_obj_with_nested_type() {
        let json = json!([
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
                "c_a": [1],
            },
            {
                "a": 1,
                "b": "hello",
                "c_a": [1],
                "c_b": [2],
            },
        ]);

        let store_schema = HashMap::new();
        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V0)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[3], &store_schema, None, false, SchemaVersion::V0).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V0).unwrap();

        assert_eq!(rb.num_rows(), 4);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![Some(1), Some(1), Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello")
            ])
        );

        assert_eq!(
            rb.column_by_name("c_a")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap(),
            &ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                None,
                None,
                Some(vec![Some(1i64)]),
                Some(vec![Some(1)])
            ])
        );

        assert_eq!(
            rb.column_by_name("c_b")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap(),
            &ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                None,
                None,
                None,
                Some(vec![Some(2i64)])
            ])
        );
    }

    #[test]
    fn arr_obj_with_nested_type_v1() {
        let json = json!([
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
                "c_a": 1,
            },
            {
                "a": 1,
                "b": "hello",
                "c_a": 1,
                "c_b": 2,
            },
        ]);

        let store_schema = HashMap::new();
        let data = Event::new(json, 0 /* doesn't matter */, LogSource::Json)
            .to_data(None, None, None, SchemaVersion::V1)
            .unwrap();
        let (schema, _) =
            Event::infer_schema(&data[3], &store_schema, None, false, SchemaVersion::V1).unwrap();
        let rb =
            Event::into_recordbatch(Utc::now(), &data, &schema, None, SchemaVersion::V1).unwrap();

        assert_eq!(rb.num_rows(), 4);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![Some(1.0), Some(1.0), Some(1.0), Some(1.0)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello")
            ])
        );

        assert_eq!(
            rb.column_by_name("c_a").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![None, None, Some(1.0), Some(1.0)])
        );

        assert_eq!(
            rb.column_by_name("c_b").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![None, None, None, Some(2.0)])
        );
    }

    #[test]
    fn generate_correct_prefix_with_current_time_and_no_custom_partitioning() {
        let schema = vec![];
        let parsed_timestamp = NaiveDate::from_ymd_opt(2023, 10, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let custom_partition_values = HashMap::new();

        let expected = format!(
            "{}.date={}.hour={:02}.minute={}",
            get_schema_key(&schema),
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
        );

        let generated = generate_prefix(&schema, parsed_timestamp, &custom_partition_values);

        assert_eq!(generated, expected);
    }

    #[test]
    fn generate_correct_prefix_with_current_time_and_custom_partitioning() {
        let schema = vec![];
        let parsed_timestamp = NaiveDate::from_ymd_opt(2023, 10, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let custom_partition_values = HashMap::from_iter([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let expected = format!(
            "{}.date={}.hour={:02}.minute={}.key1=value1.key2=value2",
            get_schema_key(&schema),
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
        );

        let generated = generate_prefix(&schema, parsed_timestamp, &custom_partition_values);

        assert_eq!(generated, expected);
    }
}
