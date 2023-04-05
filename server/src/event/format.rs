#![allow(dead_code)]

use std::sync::Arc;

use anyhow::{anyhow, Error as AnyError};
use arrow_array::{RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::Utc;

use crate::utils;

use super::{DEFAULT_METADATA_KEY, DEFAULT_TAGS_KEY, DEFAULT_TIMESTAMP_KEY};

pub mod json;

type Tags = String;
type Metadata = String;

pub trait EventFormat: Sized {
    type Data;
    fn to_data(self, schema: &Schema) -> Result<(Self::Data, Schema, Tags, Metadata), AnyError>;
    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, AnyError>;
    fn into_recordbatch(self, schema: &Schema) -> Result<RecordBatch, AnyError> {
        let (data, mut schema, tags, metadata) = self.to_data(schema)?;

        match tags_index(&schema) {
            Ok(_) => return Err(anyhow!("field {} is a reserved field", DEFAULT_TAGS_KEY)),
            Err(index) => {
                schema
                    .fields
                    .insert(index, Field::new(DEFAULT_TAGS_KEY, DataType::Utf8, true));
            }
        };

        match metadata_index(&schema) {
            Ok(_) => {
                return Err(anyhow!(
                    "field {} is a reserved field",
                    DEFAULT_METADATA_KEY
                ))
            }
            Err(index) => {
                schema.fields.insert(
                    index,
                    Field::new(DEFAULT_METADATA_KEY, DataType::Utf8, true),
                );
            }
        };

        match timestamp_index(&schema) {
            Ok(_) => {
                return Err(anyhow!(
                    "field {} is a reserved field",
                    DEFAULT_TIMESTAMP_KEY
                ))
            }
            Err(index) => {
                schema.fields.insert(
                    index,
                    Field::new(
                        DEFAULT_TIMESTAMP_KEY,
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    ),
                );
            }
        };

        let schema_ref = Arc::new(schema);
        let rb = Self::decode(data, Arc::clone(&schema_ref))?;
        let tags_arr = StringArray::from_iter_values(std::iter::repeat(&tags).take(rb.num_rows()));
        let metadata_arr =
            StringArray::from_iter_values(std::iter::repeat(&metadata).take(rb.num_rows()));
        let timestamp_array = get_timestamp_array(rb.num_rows());

        let rb = utils::arrow::replace_columns(
            Arc::clone(&schema_ref),
            rb,
            &[
                timestamp_index(&schema_ref).expect("timestamp field exists"),
                tags_index(&schema_ref).expect("tags field exists"),
                metadata_index(&schema_ref).expect("metadata field exists"),
            ],
            &[
                Arc::new(timestamp_array),
                Arc::new(tags_arr),
                Arc::new(metadata_arr),
            ],
        );

        Ok(rb)
    }
}

fn tags_index(schema: &Schema) -> Result<usize, usize> {
    schema
        .fields
        .binary_search_by_key(&DEFAULT_TAGS_KEY, |field| field.name())
}

fn metadata_index(schema: &Schema) -> Result<usize, usize> {
    schema
        .fields
        .binary_search_by_key(&DEFAULT_METADATA_KEY, |field| field.name())
}

fn timestamp_index(schema: &Schema) -> Result<usize, usize> {
    schema
        .fields
        .binary_search_by_key(&DEFAULT_TIMESTAMP_KEY, |field| field.name())
}

fn get_timestamp_array(size: usize) -> TimestampMillisecondArray {
    let time = Utc::now();
    TimestampMillisecondArray::from_value(time.timestamp_millis(), size)
}
