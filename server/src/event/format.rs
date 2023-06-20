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

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Error as AnyError};
use arrow_array::{RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::Utc;

use crate::utils::{self, arrow::get_field};

use super::{DEFAULT_METADATA_KEY, DEFAULT_TAGS_KEY, DEFAULT_TIMESTAMP_KEY};

pub mod json;
pub mod otel;

type Tags = String;
type Metadata = String;

/// Well known formats often have fixed schema.
/// This trait could be implemented on those avoid initial schema inference
pub trait ArrowSchema {
    /// Schema for this format/struct which is compatible with parseable
    fn arrow_schema() -> Vec<Field>;
}

pub struct RecordContext {
    pub is_first: bool,
    pub rb: RecordBatch,
}

// Global Trait for event format
// This trait is implemented by all the event formats
pub trait EventFormat: Sized {
    /// Implemented by formats
    fn decode(self) -> Result<RecordContext, AnyError>;

    fn into_recordbatch(
        self,
        extention: Option<impl RecordExt>,
    ) -> Result<RecordContext, AnyError> {
        let RecordContext { is_first, mut rb } = self.decode()?;
        let schema = rb.schema();

        if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
            return Err(anyhow!("field {} is a reserved field", DEFAULT_TAGS_KEY));
        };

        if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_METADATA_KEY
            ));
        };

        if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_TIMESTAMP_KEY
            ));
        };

        if let Some(extention) = extention {
            rb = dbg!(extention.extend_recordbatch(rb))
        }

        Ok(RecordContext { is_first, rb })
    }
}

fn get_timestamp_array(size: usize) -> TimestampMillisecondArray {
    let time = Utc::now();
    TimestampMillisecondArray::from_value(time.timestamp_millis(), size)
}

/// stratergy for schema derivation.
/// Implementation can support any combination of these  
#[derive(Debug, Clone)]
pub enum SchemaContext {
    /// Derive from global map using stream name as reference
    Derive(String),
    /// Owned map of fields. Used for testing
    #[cfg(test)]
    DeriveMap(HashMap<String, Field>),
}

impl SchemaContext {
    // Returns arrow schema with the fields that are present in the request body
    // This schema is an input to convert the request body to arrow record batch
    pub fn derive_arrow_schema(&self, fields: Vec<&str>) -> Result<Option<Schema>, anyhow::Error> {
        let schema = match self {
            SchemaContext::Derive(stream) => {
                let hash_map = crate::metadata::STREAM_INFO.read().unwrap();
                let stream_schema = &hash_map
                    .get(stream)
                    .ok_or(anyhow!("Stream does not exists"))?
                    .schema;
                Self::_derive_arrow_schema(stream_schema, fields)
            }
            #[cfg(test)]
            SchemaContext::DeriveMap(map) => Self::_derive_arrow_schema(map, fields),
        };

        Ok(schema)
    }

    fn _derive_arrow_schema(schema: &HashMap<String, Field>, fields: Vec<&str>) -> Option<Schema> {
        let mut res = Vec::with_capacity(fields.len());
        let fields = fields.into_iter().map(|field_name| schema.get(field_name));
        for field in fields {
            let Some(field) = field else { return None };
            res.push(field.clone())
        }
        Some(Schema::new(res))
    }
}

// Schema and RecordBatch transformation step
pub trait RecordExt {
    fn extend_recordbatch(&self, rb: RecordBatch) -> RecordBatch;
}

pub struct DefaultRecordExt {
    tags: Tags,
    metadata: Metadata,
}

impl DefaultRecordExt {
    pub fn new(tags: Tags, metadata: Metadata) -> Self {
        Self { tags, metadata }
    }
}

impl RecordExt for DefaultRecordExt {
    fn extend_recordbatch(&self, rb: RecordBatch) -> RecordBatch {
        let mut schema = (*rb.schema()).clone();
        // add the p_timestamp field to the event schema to the 0th index
        schema.fields.insert(
            0,
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        );

        // p_tags and p_metadata are added to the end of the schema
        let tag_index = schema.fields.len();
        let metadata_index = tag_index + 1;
        schema
            .fields
            .push(Field::new(DEFAULT_TAGS_KEY, DataType::Utf8, true));
        schema
            .fields
            .push(Field::new(DEFAULT_METADATA_KEY, DataType::Utf8, true));

        let tags_arr =
            StringArray::from_iter_values(std::iter::repeat(&self.tags).take(rb.num_rows()));
        let metadata_arr =
            StringArray::from_iter_values(std::iter::repeat(&self.metadata).take(rb.num_rows()));
        let timestamp_array = get_timestamp_array(rb.num_rows());

        // modify the record batch to add fields to respective indexes
        utils::arrow::add_columns(
            Arc::new(schema),
            rb,
            &[
                (0, Arc::new(timestamp_array)),
                (tag_index, Arc::new(tags_arr)),
                (metadata_index, Arc::new(metadata_arr)),
            ],
        )
    }
}

pub struct TimestampRecordExt;

impl RecordExt for TimestampRecordExt {
    fn extend_recordbatch(&self, rb: RecordBatch) -> RecordBatch {
        let mut schema = (*rb.schema()).clone();
        // add the p_timestamp field to the event schema to the 0th index
        schema.fields.insert(
            0,
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        );

        let timestamp_array = get_timestamp_array(rb.num_rows());

        // modify the record batch to add fields to respective indexes
        utils::arrow::add_columns(Arc::new(schema), rb, &[(0, Arc::new(timestamp_array))])
    }
}
