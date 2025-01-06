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
use datafusion::arrow::util::bit_util::round_upto_multiple_of_64;
use itertools::Itertools;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tracing::error;

use super::{EventFormat, LogSource, Metadata, Tags};
use crate::{
    metadata::SchemaVersion,
    utils::{arrow::get_field, json::flatten_json_body},
};

pub struct Event {
    pub data: Value,
    pub tags: Tags,
    pub metadata: Metadata,
}

impl EventFormat for Event {
    type Data = Vec<Value>;

    // convert the incoming json to a vector of json values
    // also extract the arrow schema, tags and metadata from the incoming json
    fn to_data(
        self,
        schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: Option<&String>,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
        log_source: &LogSource,
    ) -> Result<(Self::Data, Vec<Arc<Field>>, bool, Tags, Metadata), anyhow::Error> {
        let data = flatten_json_body(
            self.data,
            None,
            None,
            None,
            schema_version,
            false,
            log_source,
        )?;
        let stream_schema = schema;

        // incoming event may be a single json or a json array
        // but Data (type defined above) is a vector of json values
        // hence we need to convert the incoming event to a vector of json values
        let value_arr = match data {
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

        if static_schema_flag.is_none()
            && value_arr
                .iter()
                .any(|value| fields_mismatch(&schema, value, schema_version))
        {
            return Err(anyhow!(
                "Could not process this event due to mismatch in datatype"
            ));
        }

        Ok((value_arr, schema, is_first, self.tags, self.metadata))
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
