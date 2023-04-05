#![allow(deprecated)]

use anyhow::anyhow;
use arrow_array::RecordBatch;
use arrow_json::reader::{infer_json_schema_from_iterator, Decoder, DecoderOptions};
use arrow_schema::{Field, Schema};
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
