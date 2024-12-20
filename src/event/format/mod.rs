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
use tracing::{debug, error};

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

        for reserved_field in [
            DEFAULT_TAGS_KEY,
            DEFAULT_METADATA_KEY,
            DEFAULT_TIMESTAMP_KEY,
        ] {
            if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
                let msg = format!("{} is a reserved field", reserved_field);
                error!("{msg}");
                return Err(anyhow!(msg));
            }
        }

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
            let msg = "Schema mismatch";
            error!("{msg}");
            return Err(anyhow!(msg));
        }
        new_schema = update_field_type_in_schema(new_schema, None, time_partition, None);
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

pub fn get_existing_fields(
    inferred_schema: Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
) -> Vec<Arc<Field>> {
    let mut existing_fields = Vec::new();

    for field in inferred_schema.fields.iter() {
        if existing_schema.map_or(false, |schema| schema.contains_key(field.name())) {
            existing_fields.push(field.clone());
        }
    }

    existing_fields
}

pub fn get_existing_timestamp_fields(
    existing_schema: &HashMap<String, Arc<Field>>,
) -> Vec<Arc<Field>> {
    let mut timestamp_fields = Vec::new();

    for field in existing_schema.values() {
        if let DataType::Timestamp(TimeUnit::Millisecond, None) = field.data_type() {
            timestamp_fields.push(field.clone());
        }
    }

    timestamp_fields
}

pub fn override_timestamp_fields(
    inferred_schema: Arc<Schema>,
    existing_timestamp_fields: &[Arc<Field>],
) -> Arc<Schema> {
    let timestamp_field_names: Vec<&str> = existing_timestamp_fields
        .iter()
        .map(|field| field.name().as_str())
        .collect();

    let updated_fields: Vec<Arc<Field>> = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            if timestamp_field_names.contains(&field.name().as_str()) {
                Arc::new(Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    field.is_nullable(),
                ))
            } else {
                field.clone()
            }
        })
        .collect();

    Arc::new(Schema::new(updated_fields))
}

pub fn update_field_type_in_schema(
    inferred_schema: Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
    time_partition: Option<String>,
    log_records: Option<&Vec<Value>>,
) -> Arc<Schema> {
    let mut updated_schema = inferred_schema.clone();

    if let Some(existing_schema) = existing_schema {
        let existing_fields = get_existing_fields(inferred_schema.clone(), Some(existing_schema));
        let existing_timestamp_fields = get_existing_timestamp_fields(existing_schema);
        // overriding known timestamp fields which were inferred as string fields
        updated_schema = override_timestamp_fields(updated_schema, &existing_timestamp_fields);
        let existing_field_names: Vec<String> = existing_fields
            .iter()
            .map(|field| field.name().clone())
            .collect();

        if let Some(log_records) = log_records {
            for log_record in log_records {
                updated_schema = Arc::new(update_data_type_to_datetime(
                    (*updated_schema).clone(),
                    log_record.clone(),
                    existing_field_names.clone(),
                ));
            }
        }
    }

    if time_partition.is_none() {
        return updated_schema;
    }
    let time_partition_field_name = time_partition.unwrap();
    let new_schema: Vec<Field> = updated_schema
        .fields()
        .iter()
        .map(|field| {
            if *field.name() == time_partition_field_name {
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

pub fn update_data_type_to_datetime(
    schema: Schema,
    value: Value,
    ignore_field_names: Vec<String>,
) -> Schema {
    let new_schema: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.data_type() == &DataType::Utf8 && !ignore_field_names.contains(field.name()) {
                if let Value::Object(map) = &value {
                    if let Some(Value::String(s)) = map.get(field.name()) {
                        if DateTime::parse_from_rfc3339(s).is_ok() {
                            debug!(
                                "Field type updated to timestamp from string: {}",
                                field.name()
                            );
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
