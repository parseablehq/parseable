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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{anyhow, Error as AnyError};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::DateTime;
use itertools::Itertools;
use serde_json::Value;

use crate::utils::{self, arrow::get_field};

use super::{DEFAULT_METADATA_KEY, DEFAULT_TAGS_KEY, DEFAULT_TIMESTAMP_KEY};

pub mod json;

// Prefixes that are most common in datetime field names
const TIME_FIELD_PREFIXS: [&str; 2] = ["date", "time"];

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
        update_field_type_in_schema(&mut new_schema, None, time_partition, None);
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

/// Returns an updated schema where certain string fields may be converted to timestamp fields based on:
/// - Matching field name in `existing_schema`.
/// - Content of `log_records` indicating date/time data.
/// - A specific `time_partition` field name.
pub fn update_field_type_in_schema(
    schema: &mut Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
    time_partition: Option<String>,
    log_records: Option<&Vec<Value>>,
) {
    if let Some(existing_schema) = existing_schema {
        override_timestamp_fields_from_existing_schema(schema, existing_schema);
    }

    if let Some(records) = log_records {
        update_schema_from_logs(schema, existing_schema, records);
    }

    if let Some(partition_field) = time_partition {
        update_time_partition_field(schema, &partition_field);
    }
}

/// Updates the schema based on log records by identifying fields containing datetime patterns.
/// Fields within the log records that match an expected substring in their names and have values
/// resembling a datetime type will be updated in the given schema.
pub fn override_timestamp_fields_from_existing_schema(
    schema: &mut Arc<Schema>,
    existing_schema: &HashMap<String, Arc<Field>>,
) {
    let timestamp_field_names: HashSet<&str> = existing_schema
        .values()
        .filter(|field| {
            matches!(
                field.data_type(),
                DataType::Timestamp(TimeUnit::Millisecond, None)
            )
        })
        .map(|field| field.name().as_str())
        .collect();

    let updated_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|field| {
            if timestamp_field_names.contains(field.name().as_str()) {
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

    *schema = Arc::new(Schema::new(updated_fields));
}

/// Updates the schema based on log records by identifying fields containing datetime patterns.
/// Fields within the log records that match an expected substring in their names and have values
/// resembling a datetime type will be added to or updated in the given schema.
fn update_schema_from_logs(
    schema: &mut Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
    log_records: &[Value],
) {
    let existing_field_names: Vec<&String> = schema
        .fields()
        .iter()
        .filter_map(|field| {
            if existing_schema.map_or(false, |s| s.contains_key(field.name())) {
                Some(field.name())
            } else {
                None
            }
        })
        .collect();

    let updated_schema = schema
        .fields()
        .iter()
        .map(|field| {
            if !existing_field_names.contains(&field.name())
                && field.data_type() == &DataType::Utf8
                && TIME_FIELD_PREFIXS.iter().any(|p| field.name().contains(p))
            {
                if log_records
                    .iter()
                    .any(|record| matches_date_time_field(field.name(), record))
                {
                    // Update the field's data type to Timestamp
                    return Field::new(
                        field.name().clone(),
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    );
                }
            }
            Field::new(field.name().clone(), field.data_type().clone(), true)
        })
        .collect_vec();
    *schema = Arc::new(Schema::new(updated_schema));
}

/// Checks if a log record contains valid date-time string for the specified field name.
fn matches_date_time_field(field_name: &String, log_record: &Value) -> bool {
    if let Value::Object(map) = log_record {
        if let Some(Value::String(s)) = map.get(field_name) {
            if DateTime::parse_from_rfc3339(s).is_ok() || DateTime::parse_from_rfc2822(s).is_ok() {
                return true;
            }
        }
    }
    false
}

/// Updates the specified time partition field to a timestamp type if it's currently a string.
fn update_time_partition_field(schema: &mut Arc<Schema>, field_name: &String) {
    let updated_schema = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == field_name && field.data_type() == &DataType::Utf8 {
                Field::new(
                    field.name().clone(),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )
            } else {
                Field::new(field.name().clone(), field.data_type().clone(), true)
            }
        })
        .collect_vec();
    *schema = Arc::new(Schema::new(updated_schema));
}
