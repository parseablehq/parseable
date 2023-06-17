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

use crate::{
    metadata::STREAM_INFO,
    utils::{self, arrow::get_field},
};

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

// Global Trait for event format
// This trait is implemented by all the event formats
pub trait EventFormat: Sized {
    type Data;

    /// Data format given a schema context maybe derive a new schema fit to given event.
    /// In case of fixes formats this is no-op and can set first event (bool) to false.
    /// As the format schema is const we don't need to commit it to in memory.
    /// to_data is where implementors would apply flattening and validation.
    fn to_data(self, schema_context: SchemaContext)
        -> Result<(Self::Data, Schema, bool), AnyError>;

    /// Decode Self::Data to recordbatch given the schema.
    /// implementation of into_recordbatch can choose to call this method directly in the implementation
    /// avoiding to_data step ( given invariant that Self::Data is optimal for decode)
    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, AnyError>;

    // extention is used to perform operation on schema before decode and record after decode
    fn extention(&self) -> Option<Box<dyn DecodeExt>>;

    fn into_recordbatch(
        self,
        schema_provider: impl SchemaProvider,
    ) -> Result<(RecordBatch, bool), AnyError> {
        let extention = self.extention();
        let schema_context = schema_provider.get_schema_context();
        let (data, mut schema, is_first) = self.to_data(schema_context.clone())?;

        // If schema provider is a map then we should check schema is compatible
        if is_first {
            let fields = match schema_context {
                SchemaContext::Derive(stream) => STREAM_INFO.schema(&stream)?.fields.clone(),
                #[cfg(test)]
                SchemaContext::DeriveMap(map) => map.values().cloned().collect(),
                SchemaContext::Fixed(schema) => schema.fields.clone(),
            };

            if let Err(err) = Schema::try_merge(vec![Schema::new(fields), schema.clone()]) {
                return Err(anyhow!(
                    "Could not merge schema of this event with that of the existing stream. {:?}",
                    err
                ));
            }
        }

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

        let rb = if let Some(mut extention) = extention {
            extention.before_decode(&mut schema);
            // prepare the record batch and new fields to be added
            let schema_ref = Arc::new(schema);
            let rb = Self::decode(data, Arc::clone(&schema_ref))?;
            extention.after_decode(rb)
        } else {
            let schema_ref = Arc::new(schema);
            Self::decode(data, Arc::clone(&schema_ref))?
        };

        Ok((rb, is_first))
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
    /// Fixed schema for known formats
    Fixed(Arc<Schema>),
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
            SchemaContext::Fixed(schema) => Some((**schema).clone()),
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

pub trait SchemaProvider {
    fn get_schema_context(self) -> SchemaContext;
}

pub struct GlobalSchemaProvider {
    pub stream_name: String,
}

impl SchemaProvider for GlobalSchemaProvider {
    fn get_schema_context(self) -> SchemaContext {
        SchemaContext::Derive(self.stream_name)
    }
}

pub enum FormatSchemaProvider {
    OpenTelemetryTrace,
}

impl SchemaProvider for FormatSchemaProvider {
    fn get_schema_context(self) -> SchemaContext {
        let fields = match self {
            FormatSchemaProvider::OpenTelemetryTrace => otel::TracesData::arrow_schema(),
        };

        SchemaContext::Fixed(Arc::new(Schema::new(fields)))
    }
}

// Schema and RecordBatch transformation step
pub trait DecodeExt {
    fn before_decode(&mut self, schema: &mut Schema);
    fn after_decode(&self, rb: RecordBatch) -> RecordBatch;
}

struct DefaultDecoderExt {
    tag_index: usize,
    metadata_index: usize,
    tags: Tags,
    metadata: Metadata,
}

impl DefaultDecoderExt {
    fn new(tags: Tags, metadata: Metadata) -> Self {
        Self {
            tag_index: 0,
            metadata_index: 0,
            tags,
            metadata,
        }
    }
}

impl DecodeExt for DefaultDecoderExt {
    fn before_decode(&mut self, schema: &mut Schema) {
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
        self.tag_index = schema.fields.len();
        self.metadata_index = self.tag_index + 1;
        schema
            .fields
            .push(Field::new(DEFAULT_TAGS_KEY, DataType::Utf8, true));
        schema
            .fields
            .push(Field::new(DEFAULT_METADATA_KEY, DataType::Utf8, true));
    }

    fn after_decode(&self, rb: RecordBatch) -> RecordBatch {
        let tags_arr =
            StringArray::from_iter_values(std::iter::repeat(&self.tags).take(rb.num_rows()));
        let metadata_arr =
            StringArray::from_iter_values(std::iter::repeat(&self.metadata).take(rb.num_rows()));
        let timestamp_array = get_timestamp_array(rb.num_rows());

        // modify the record batch to add fields to respective indexes
        utils::arrow::replace_columns(
            Arc::clone(&rb.schema()),
            rb,
            &[0, self.tag_index, self.metadata_index],
            &[
                Arc::new(timestamp_array),
                Arc::new(tags_arr),
                Arc::new(metadata_arr),
            ],
        )
    }
}

struct TimestampDecoderExt;

impl DecodeExt for TimestampDecoderExt {
    fn before_decode(&mut self, schema: &mut Schema) {
        // add the p_timestamp field to the event schema to the 0th index
        schema.fields.insert(
            0,
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        );
    }

    fn after_decode(&self, rb: RecordBatch) -> RecordBatch {
        let timestamp_array = get_timestamp_array(rb.num_rows());

        // modify the record batch to add fields to respective indexes
        utils::arrow::replace_columns(
            Arc::clone(&rb.schema()),
            rb,
            &[0],
            &[Arc::new(timestamp_array)],
        )
    }
}
