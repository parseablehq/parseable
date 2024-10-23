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

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Error as AnyError};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::DateTime;
use serde_json::Value;

use crate::utils::{self, arrow::get_field};

use super::{DEFAULT_METADATA_KEY, DEFAULT_TAGS_KEY, DEFAULT_TIMESTAMP_KEY};

pub mod json;

type Tags = String;
type Metadata = String;
type EventSchema = Vec<Arc<Field>>;

// Global Trait for event format
// This trait is implemented by all the event formats
pub trait EventFormat: Sized {
    type Data;

    fn to_data(
        self,
        schema: HashMap<String, Arc<Field>>,
        static_schema_flag: Option<String>,
        time_partition: Option<String>,
    ) -> Result<(Self::Data, EventSchema, bool, Tags, Metadata), AnyError>;
    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, AnyError>;
    fn into_recordbatch(
        self,
        storage_schema: HashMap<String, Arc<Field>>,
        static_schema_flag: Option<String>,
        time_partition: Option<String>,
    ) -> Result<(RecordBatch, bool), AnyError> {
        let (data, mut schema, is_first, tags, metadata) = self.to_data(
            storage_schema.clone(),
            static_schema_flag.clone(),
            time_partition.clone(),
        )?;

        if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
            return Err(anyhow!("field {} is a reserved field", DEFAULT_TAGS_KEY));
        };

        if get_field(&schema, DEFAULT_METADATA_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_METADATA_KEY
            ));
        };

        if get_field(&schema, DEFAULT_TIMESTAMP_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_TIMESTAMP_KEY
            ));
        };

        // add the p_timestamp field to the event schema to the 0th index
        schema.insert(
            0,
            Arc::new(Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
        );

        // p_tags and p_metadata are added to the end of the schema
        let tags_index = schema.len();
        let metadata_index = tags_index + 1;
        schema.push(Arc::new(Field::new(DEFAULT_TAGS_KEY, DataType::Utf8, true)));
        schema.push(Arc::new(Field::new(
            DEFAULT_METADATA_KEY,
            DataType::Utf8,
            true,
        )));

        // prepare the record batch and new fields to be added
        let mut new_schema = Arc::new(Schema::new(schema));
        if !Self::is_schema_matching(new_schema.clone(), storage_schema, static_schema_flag) {
            return Err(anyhow!("Schema mismatch"));
        }
        new_schema = update_field_type_in_schema(new_schema, time_partition);
        let rb = Self::decode(data, new_schema.clone())?;
        let tags_arr = StringArray::from_iter_values(std::iter::repeat(&tags).take(rb.num_rows()));
        let metadata_arr =
            StringArray::from_iter_values(std::iter::repeat(&metadata).take(rb.num_rows()));
        // modify the record batch to add fields to respective indexes
        let rb = utils::arrow::replace_columns(
            Arc::clone(&new_schema),
            &rb,
            &[tags_index, metadata_index],
            &[Arc::new(tags_arr), Arc::new(metadata_arr)],
        );

        Ok((rb, is_first))
    }

    fn is_schema_matching(
        new_schema: Arc<Schema>,
        storage_schema: HashMap<String, Arc<Field>>,
        static_schema_flag: Option<String>,
    ) -> bool {
        if static_schema_flag.is_none() {
            return true;
        }
        for (field_name, field) in new_schema
            .fields()
            .iter()
            .map(|field| (field.name().to_owned(), field.clone()))
            .collect::<HashMap<String, Arc<Field>>>()
        {
            if let Some(storage_field) = storage_schema.get(&field_name) {
                if field_name != *storage_field.name() {
                    return false;
                }
                if field.data_type() != storage_field.data_type() {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

pub fn update_field_type_in_schema(
    schema: Arc<Schema>,
    time_partition: Option<String>,
) -> Arc<Schema> {
    if time_partition.is_none() {
        return schema;
    }
    let field_name = time_partition.unwrap();
    let new_schema: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if *field.name() == field_name {
                if field.data_type() == &DataType::Utf8 {
                    let new_data_type = DataType::Timestamp(TimeUnit::Millisecond, None);
                    Field::new(field.name().clone(), new_data_type, true)
                } else {
                    Field::new(field.name(), field.data_type().clone(), true)
                }
            } else {
                Field::new(field.name(), field.data_type().clone(), true)
            }
        })
        .collect();
    Arc::new(Schema::new(new_schema))
}

pub fn update_data_type_to_datetime(schema: Schema, value: Value) -> Schema {
    let new_schema: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.data_type() == &DataType::Utf8 {
                if let Value::Object(map) = &value {
                    if let Some(Value::String(s)) = map.get(field.name()) {
                        if DateTime::parse_from_rfc3339(s).is_ok() {
                            // Update the field's data type to Timestamp
                            return Field::new(
                                field.name().clone(),
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                true,
                            );
                        }
                    }
                }
            }
            // Return the original field if no update is needed
            Field::new(field.name(), field.data_type().clone(), true)
        })
        .collect();

    Schema::new(new_schema)
}
