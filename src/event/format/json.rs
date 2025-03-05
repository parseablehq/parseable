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
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::arrow::util::bit_util::round_upto_multiple_of_64;
use itertools::Itertools;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tracing::error;

use super::{EventFormat, EventSchema};
use crate::{metadata::SchemaVersion, utils::arrow::get_field};

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

impl EventFormat for Event {
    type Data = Vec<Value>;

    /// Returns the time at ingestion, i.e. the `p_timestamp` value
    fn get_p_timestamp(&self) -> DateTime<Utc> {
        self.p_timestamp
    }

    fn get_partitions(
        &self,
        time_partition: Option<&String>,
        custom_partitions: Option<&String>,
    ) -> anyhow::Result<(NaiveDateTime, HashMap<String, String>)> {
        let custom_partition_values = match custom_partitions.as_ref() {
            Some(custom_partition) => {
                let custom_partitions = custom_partition.split(',').collect_vec();
                extract_custom_partition_values(&self.json, &custom_partitions)
            }
            None => HashMap::new(),
        };

        let parsed_timestamp = match time_partition {
            Some(time_partition) => extract_and_parse_time(&self.json, time_partition)?,
            _ => self.p_timestamp.naive_utc(),
        };

        Ok((parsed_timestamp, custom_partition_values))
    }

    // convert the incoming json to a vector of json values
    // also extract the arrow schema, tags and metadata from the incoming json
    fn to_data(
        self,
        schema: &HashMap<String, Arc<Field>>,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
    ) -> anyhow::Result<(Self::Data, EventSchema, bool)> {
        let stream_schema = schema;
        // incoming event may be a single json or a json array
        // but Data (type defined above) is a vector of json values
        // hence we need to convert the incoming event to a vector of json values
        let value_arr = match self.json {
            Value::Array(arr) => arr,
            value @ Value::Object(_) => vec![value],
            _ => unreachable!("flatten would have failed beforehand"),
        };

        // collect all the keys from all the json objects in the request body
        let fields =
            collect_keys(value_arr.iter()).expect("fields can be collected from array of objects");

        let mut is_first = false;
        let schema = match derive_arrow_schema(stream_schema, fields) {
            Ok(schema) => schema,
            Err(_) => {
                let mut infer_schema = infer_json_schema_from_iterator(value_arr.iter().map(Ok))
                    .map_err(|err| {
                        anyhow!("Could not infer schema for this event due to err {:?}", err)
                    })?;
                let new_infer_schema = super::update_field_type_in_schema(
                    Arc::new(infer_schema),
                    Some(stream_schema),
                    time_partition,
                    Some(&value_arr),
                    schema_version,
                );
                infer_schema = Schema::new(new_infer_schema.fields().clone());
                Schema::try_merge(vec![
                    Schema::new(stream_schema.values().cloned().collect::<Fields>()),
                    infer_schema.clone(),
                ]).map_err(|err| anyhow!("Could not merge schema of this event with that of the existing stream. {:?}", err))?;
                is_first = true;
                infer_schema
                    .fields
                    .iter()
                    .filter(|field| !field.data_type().is_null())
                    .cloned()
                    .sorted_by(|a, b| a.name().cmp(b.name()))
                    .collect()
            }
        };

        if value_arr
            .iter()
            .any(|value| fields_mismatch(&schema, value, schema_version))
        {
            return Err(anyhow!(
                "Could not process this event due to mismatch in datatype"
            ));
        }

        Ok((value_arr, schema, is_first))
    }

    // Convert the Data type (defined above) to arrow record batch
    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, anyhow::Error> {
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
}

/// Extracts custom partition values from provided JSON object
/// e.g. `json: {"status": 400, "msg": "Hello, World!"}, custom_partition_list: ["status"]` returns `{"status" => 400}`
pub fn extract_custom_partition_values(
    json: &Value,
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
fn extract_and_parse_time(
    json: &Value,
    time_partition: &str,
) -> Result<NaiveDateTime, anyhow::Error> {
    let current_time = json
        .get(time_partition)
        .ok_or_else(|| anyhow!("Missing field for time partition in json: {time_partition}"))?;
    let parsed_time: DateTime<Utc> = serde_json::from_value(current_time.clone())?;

    Ok(parsed_time.naive_utc())
}

// Returns arrow schema with the fields that are present in the request body
// This schema is an input to convert the request body to arrow record batch
fn derive_arrow_schema(
    schema: &HashMap<String, Arc<Field>>,
    fields: Vec<&str>,
) -> Result<Vec<Arc<Field>>, ()> {
    let mut res = Vec::with_capacity(fields.len());
    let fields = fields.into_iter().map(|field_name| schema.get(field_name));
    for field in fields {
        let Some(field) = field else { return Err(()) };
        res.push(field.clone())
    }
    Ok(res)
}

fn collect_keys<'a>(values: impl Iterator<Item = &'a Value>) -> Result<Vec<&'a str>, ()> {
    let mut keys = Vec::new();
    for value in values {
        if let Some(obj) = value.as_object() {
            for key in obj.keys() {
                match keys.binary_search(&key.as_str()) {
                    Ok(_) => (),
                    Err(pos) => {
                        keys.insert(pos, key.as_str());
                    }
                }
            }
        } else {
            return Err(());
        }
    }
    Ok(keys)
}

fn fields_mismatch(schema: &[Arc<Field>], body: &Value, schema_version: SchemaVersion) -> bool {
    for (name, val) in body.as_object().expect("body is of object variant") {
        if val.is_null() {
            continue;
        }
        let Some(field) = get_field(schema, name) else {
            return true;
        };
        if !valid_type(field.data_type(), val, schema_version) {
            return true;
        }
    }
    false
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

    use serde_json::json;

    use super::*;

    #[test]
    fn parse_time_parition_from_value() {
        let json = json!({"timestamp": "2025-05-15T15:30:00Z"});
        let parsed = extract_and_parse_time(&json, "timestamp");

        let expected = NaiveDateTime::from_str("2025-05-15T15:30:00").unwrap();
        assert_eq!(parsed.unwrap(), expected);
    }

    #[test]
    fn time_parition_not_in_json() {
        let json = json!({"hello": "world!"});
        let parsed = extract_and_parse_time(&json, "timestamp");

        assert!(parsed.is_err());
    }

    #[test]
    fn time_parition_not_parseable_as_datetime() {
        let json = json!({"timestamp": "not time"});
        let parsed = extract_and_parse_time(&json, "timestamp");

        assert!(parsed.is_err());
    }
}
