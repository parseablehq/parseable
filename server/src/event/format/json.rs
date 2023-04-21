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

#![allow(deprecated)]

use anyhow::anyhow;
use arrow_array::RecordBatch;
use arrow_json::reader::{infer_json_schema_from_iterator, Decoder, DecoderOptions};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::util::bit_util::round_upto_multiple_of_64;
use serde_json::Value;
use std::sync::Arc;

use super::EventFormat;
use crate::utils::json::flatten_json_body;

pub struct Event {
    pub data: Value,
    pub tags: String,
    pub metadata: String,
}

impl EventFormat for Event {
    type Data = Vec<Value>;

    fn to_data(
        self,
        schema: &Schema,
    ) -> Result<(Self::Data, Schema, String, String), anyhow::Error> {
        let data = flatten_json_body(self.data)?;

        let stream_schema = schema;

        let value_arr = match data {
            Value::Array(arr) => arr,
            value @ Value::Object(_) => vec![value],
            _ => unreachable!("flatten would have failed beforehand"),
        };

        let fields =
            collect_keys(value_arr.iter()).expect("fields can be collected from array of objects");

        let schema = match derive_sub_schema(stream_schema.clone(), fields) {
            Ok(schema) => schema,
            Err(_) => match infer_json_schema_from_iterator(value_arr.iter().map(Ok)) {
                Ok(mut infer_schema) => {
                    infer_schema
                        .fields
                        .sort_by(|field1, field2| Ord::cmp(field1.name(), field2.name()));

                    if let Err(err) =
                        Schema::try_merge(vec![stream_schema.clone(), infer_schema.clone()])
                    {
                        return Err(anyhow!("Could not merge schema of this event with that of the existing stream. {:?}", err));
                    }
                    infer_schema
                }
                Err(err) => {
                    return Err(anyhow!(
                        "Could not infer schema for this event due to err {:?}",
                        err
                    ))
                }
            },
        };

        if value_arr
            .iter()
            .any(|value| fields_mismatch(&schema, value))
        {
            return Err(anyhow!(
                "Could not process this event due to mismatch in datatype"
            ));
        }

        Ok((value_arr, schema, self.tags, self.metadata))
    }

    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, anyhow::Error> {
        let array_capacity = round_upto_multiple_of_64(data.len());
        let value_iter: &mut (dyn Iterator<Item = Value>) = &mut data.into_iter();

        let reader = Decoder::new(
            schema,
            DecoderOptions::new().with_batch_size(array_capacity),
        );
        match reader.next_batch(&mut value_iter.map(Ok)) {
            Ok(Some(recordbatch)) => Ok(recordbatch),
            Err(err) => Err(anyhow!("Failed to create recordbatch due to {:?}", err)),
            Ok(None) => unreachable!("all records are added to one rb"),
        }
    }
}

// invariants for this to work.
// All fields in existing schema and fields in event are sorted my name lexographically
fn derive_sub_schema(schema: arrow_schema::Schema, fields: Vec<&str>) -> Result<Schema, ()> {
    let fields = derive_subset(schema.fields, fields)?;
    Ok(Schema::new(fields))
}

fn derive_subset(superset: Vec<Field>, subset: Vec<&str>) -> Result<Vec<Field>, ()> {
    let mut superset_idx = 0;
    let mut subset_idx = 0;
    let mut subset_schema = Vec::with_capacity(subset.len());

    while superset_idx < superset.len() && subset_idx < subset.len() {
        let field = superset[superset_idx].clone();
        let key = subset[subset_idx];
        if field.name() == key {
            subset_schema.push(field);
            superset_idx += 1;
            subset_idx += 1;
        } else if field.name().as_str() < key {
            superset_idx += 1;
        } else {
            return Err(());
        }
    }

    // error if subset is not exhausted
    if subset_idx < subset.len() {
        return Err(());
    }

    Ok(subset_schema)
}

// Must be in sorted order
fn collect_keys<'a>(values: impl Iterator<Item = &'a Value>) -> Result<Vec<&'a str>, ()> {
    let mut sorted_keys = Vec::new();
    for value in values {
        if let Some(obj) = value.as_object() {
            for key in obj.keys() {
                match sorted_keys.binary_search(&key.as_str()) {
                    Ok(_) => (),
                    Err(pos) => {
                        sorted_keys.insert(pos, key.as_str());
                    }
                }
            }
        } else {
            return Err(());
        }
    }
    Ok(sorted_keys)
}

fn fields_mismatch(schema: &Schema, body: &Value) -> bool {
    for (name, val) in body.as_object().expect("body is of object variant") {
        if val.is_null() {
            continue;
        }

        let Ok(field) = schema.field_with_name(name) else { return true };
        if !valid_type(field.data_type(), val) {
            return true;
        }
    }
    false
}

fn valid_type(data_type: &DataType, value: &Value) -> bool {
    match data_type {
        DataType::Boolean => value.is_boolean(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => value.is_i64(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => value.is_u64(),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => value.is_f64(),
        DataType::Utf8 => value.is_string(),
        DataType::List(field) => {
            let data_type = field.data_type();
            if let Value::Array(arr) = value {
                for elem in arr {
                    if !valid_type(data_type, elem) {
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
                        if !valid_type(field.data_type(), value) {
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
        _ => unreachable!(),
    }
}
