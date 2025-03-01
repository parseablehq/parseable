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
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_json::reader::{infer_json_schema_from_iterator, ReaderBuilder};
use arrow_schema::{DataType, Field, Fields, Schema};
use chrono::{DateTime, NaiveDateTime, Utc};
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
    },
};

pub struct Event {
    pub json: Value,
    pub p_timestamp: DateTime<Utc>,
}

impl Event {
    pub fn new(json: Value) -> Self {
        Self {
            json,
            p_timestamp: Utc::now(),
        }
    }
}

pub fn flatten_logs(
    json: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partitions: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
) -> anyhow::Result<Vec<Json>> {
    let data = match log_source {
        LogSource::Kinesis => {
            //custom flattening required for Amazon Kinesis
            let message: Message = serde_json::from_value(json)?;
            flatten_kinesis_logs(message)
        }
        LogSource::OtelLogs => {
            //custom flattening required for otel logs
            let logs: LogsData = serde_json::from_value(json)?;
            flatten_otel_logs(&logs)
        }
        LogSource::OtelTraces => {
            //custom flattening required for otel traces
            let traces: TracesData = serde_json::from_value(json)?;
            flatten_otel_traces(&traces)
        }
        LogSource::OtelMetrics => {
            //custom flattening required for otel metrics
            let metrics: MetricsData = serde_json::from_value(json)?;
            flatten_otel_metrics(metrics)
        }
        _ => vec![json],
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
            log_source,
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

impl EventFormat for Event {
    type Data = Vec<Json>;

    // convert the incoming json to a vector of json values
    // also extract the arrow schema, tags and metadata from the incoming json
    fn to_data(
        self,
        stored_schema: &HashMap<String, Arc<Field>>,
        time_partition: Option<&String>,
        time_partition_limit: Option<NonZeroU32>,
        custom_partitions: Option<&String>,
        schema_version: SchemaVersion,
        log_source: &LogSource,
    ) -> anyhow::Result<(Self::Data, Vec<Arc<Field>>, bool)> {
        let flattened = flatten_logs(
            self.json,
            time_partition,
            time_partition_limit,
            custom_partitions,
            schema_version,
            log_source,
        )?;

        // collect all the keys from all the json objects in the request body
        let fields = collect_keys(flattened.iter());

        let mut is_first = false;
        let schema = if let Some(schema) = derive_arrow_schema(stored_schema, fields) {
            schema
        } else {
            // TODO:
            let mut infer_schema = infer_json_schema_from_iterator(
                flattened.iter().map(|obj| Ok(Value::Object(obj.clone()))),
            )
            .map_err(|err| anyhow!("Could not infer schema for this event due to err {:?}", err))?;
            let new_infer_schema = super::update_field_type_in_schema(
                Arc::new(infer_schema),
                Some(stored_schema),
                time_partition,
                Some(&flattened),
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

        if flattened
            .iter()
            .any(|value| fields_mismatch(&schema, value, schema_version))
        {
            return Err(anyhow!(
                "Could not process this event due to mismatch in datatype"
            ));
        }

        Ok((flattened, schema, is_first))
    }

    // Convert the Data type (defined above) to arrow record batch
    fn decode(data: Self::Data, schema: Arc<Schema>) -> anyhow::Result<RecordBatch> {
        let array_capacity = round_upto_multiple_of_64(data.len());
        let mut reader = ReaderBuilder::new(schema)
            .with_batch_size(array_capacity)
            .with_coerce_primitive(false)
            .build_decoder()?;

        reader.serialize(&data)?;
        match reader.flush() {
            Ok(Some(recordbatch)) => Ok(recordbatch),
            Err(err) => Err(anyhow!("Failed to create recordbatch due to {:?}", err)),
            Ok(None) => unreachable!("all records are added to one rb"),
        }
    }

    /// Converts a JSON event into a Parseable Event
    fn into_event(
        self,
        origin_size: u64,
        stream: &Stream,
        log_source: &LogSource,
    ) -> anyhow::Result<super::Event> {
        let time_partition = stream.get_time_partition();
        let time_partition_limit = stream.get_time_partition_limit();
        let static_schema_flag = stream.get_static_schema_flag();
        let custom_partitions = stream.get_custom_partition();
        let schema_version = stream.get_schema_version();
        let storage_schema = stream.get_schema_raw();
        let stream_type = stream.get_stream_type();

        let p_timestamp = self.p_timestamp;
        let (data, schema, is_first_event) = self.to_data(
            &storage_schema,
            time_partition.as_ref(),
            time_partition_limit,
            custom_partitions.as_ref(),
            schema_version,
            log_source,
        )?;

        let mut partitions = HashMap::new();
        for json in data {
            let custom_partition_values = match custom_partitions.as_ref() {
                Some(custom_partitions) => {
                    let custom_partitions = custom_partitions.split(',').collect_vec();
                    extract_custom_partition_values(&json, &custom_partitions)
                }
                None => HashMap::new(),
            };

            let parsed_timestamp = match time_partition.as_ref() {
                Some(time_partition) => extract_and_parse_time(&json, time_partition.as_ref())?,
                _ => p_timestamp.naive_utc(),
            };

            let rb = Self::into_recordbatch(
                p_timestamp,
                vec![json],
                schema.clone(),
                &storage_schema,
                static_schema_flag,
                time_partition.as_ref(),
                schema_version,
            )?;

            let schema = rb.schema();
            let mut key = get_schema_key(&schema.fields);
            if time_partition.is_some() {
                let parsed_timestamp_to_min = parsed_timestamp.format("%Y%m%dT%H%M").to_string();
                key.push_str(&parsed_timestamp_to_min);
            }

            for (k, v) in custom_partition_values.iter().sorted_by_key(|v| v.0) {
                key.push_str(&format!("&{k}={v}"));
            }

            let entry = partitions.entry(key).or_insert(PartitionEvent {
                rb: RecordBatch::new_empty(schema.clone()),
                parsed_timestamp,
                custom_partition_values,
            });

            entry.rb = concat_batches(&schema, [&entry.rb, &rb])?;
        }

        Ok(super::Event {
            origin_format: "json",
            origin_size,
            is_first_event,
            time_partition: None,
            partitions,
            stream_type,
        })
    }
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
fn collect_keys<'a>(objects: impl Iterator<Item = &'a Json>) -> HashSet<&'a str> {
    let mut keys = HashSet::new();
    for object in objects {
        for key in object.keys() {
            keys.insert(key.as_str());
        }
    }

    keys
}

// Returns true when the field doesn't exist in schema or has an invalid type
fn fields_mismatch(schema: &[Arc<Field>], body: &Json, schema_version: SchemaVersion) -> bool {
    body.iter().any(|(key, value)| {
        !value.is_null()
            && get_field(schema, key)
                .is_none_or(|field| !valid_type(field.data_type(), value, schema_version))
    })
}

fn valid_type(data_type: &DataType, value: &Value, schema_version: SchemaVersion) -> bool {
    match data_type {
        DataType::Boolean => value.is_boolean(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => value.is_i64(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => value.is_u64(),
        DataType::Float16 | DataType::Float32 => value.is_f64(),
        // All numbers can be cast as Float64 from schema version v1
        DataType::Float64 if schema_version == SchemaVersion::V1 => value.is_number(),
        DataType::Float64 if schema_version != SchemaVersion::V1 => value.is_f64(),
        DataType::Utf8 => value.is_string(),
        DataType::List(field) => {
            let data_type = field.data_type();
            if let Value::Array(arr) = value {
                for elem in arr {
                    if elem.is_null() {
                        continue;
                    }
                    if !valid_type(data_type, elem, schema_version) {
                        return false;
                    }
                }
            }
            true
        }
        DataType::Struct(fields) => {
            if let Value::Object(val) = value {
                for (key, value) in val {
                    let field = (0..fields.len())
                        .find(|idx| fields[*idx].name() == key)
                        .map(|idx| &fields[idx]);

                    if let Some(field) = field {
                        if value.is_null() {
                            continue;
                        }
                        if !valid_type(field.data_type(), value, schema_version) {
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
        DataType::Timestamp(_, _) => value.is_string() || value.is_number(),
        _ => {
            error!("Unsupported datatype {:?}, value {:?}", data_type, value);
            unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use arrow::datatypes::Int64Type;
    use arrow_array::{ArrayRef, Float64Array, Int64Array, ListArray, StringArray};
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
        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V0,
        )
        .unwrap();

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
        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V0,
        )
        .unwrap();

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
        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V0,
        )
        .unwrap();

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

        assert!(Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .is_err());
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

        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V0,
        )
        .unwrap();

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
        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V0,
        )
        .unwrap();

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
        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V0,
        )
        .unwrap();

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
        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V0,
        )
        .unwrap();

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
        let json = json!([
            {
                "a": null,
                "b": "hello",
                "c": 1.24
            },
            {
                "a": 1,
                "b": "hello",
                "c": 1
            },
            {
                "a": 1,
                "b": "hello",
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

        assert!(Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
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
        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V0,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V0,
        )
        .unwrap();

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
        let (data, schema, _) = Event::new(json)
            .to_data(
                &store_schema,
                None,
                None,
                None,
                SchemaVersion::V1,
                &LogSource::Json,
            )
            .unwrap();
        let rb = Event::into_recordbatch(
            Utc::now(),
            data,
            schema,
            &store_schema,
            false,
            None,
            SchemaVersion::V1,
        )
        .unwrap();

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
}
