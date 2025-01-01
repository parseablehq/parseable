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
use serde_json::Value;

use crate::{
    metadata::SchemaVersion,
    utils::{self, arrow::get_field},
};

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
        schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: Option<&String>,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
    ) -> Result<(Self::Data, EventSchema, bool, Tags, Metadata), AnyError>;

    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, AnyError>;

    fn into_recordbatch(
        self,
        storage_schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: Option<&String>,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
    ) -> Result<(RecordBatch, bool), AnyError> {
        let (data, mut schema, is_first, tags, metadata) = self.to_data(
            storage_schema,
            static_schema_flag,
            time_partition,
            schema_version,
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
        storage_schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: Option<&String>,
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

pub fn get_existing_field_names(
    inferred_schema: Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
) -> HashSet<String> {
    let mut existing_field_names = HashSet::new();

    let Some(existing_schema) = existing_schema else {
        return existing_field_names;
    };
    for field in inferred_schema.fields.iter() {
        if existing_schema.contains_key(field.name()) {
            existing_field_names.insert(field.name().to_owned());
        }
    }

    existing_field_names
}

pub fn override_existing_timestamp_fields(
    existing_schema: &HashMap<String, Arc<Field>>,
    inferred_schema: Arc<Schema>,
) -> Arc<Schema> {
    let timestamp_field_names: HashSet<String> = existing_schema
        .values()
        .filter_map(|field| {
            if let DataType::Timestamp(TimeUnit::Millisecond, None) = field.data_type() {
                Some(field.name().to_owned())
            } else {
                None
            }
        })
        .collect();
    let updated_fields: Vec<Arc<Field>> = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            if timestamp_field_names.contains(field.name()) {
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
    time_partition: Option<&String>,
    log_records: Option<&Vec<Value>>,
) -> Arc<Schema> {
    let mut updated_schema = inferred_schema.clone();
    let existing_field_names = get_existing_field_names(inferred_schema.clone(), existing_schema);

    if let Some(existing_schema) = existing_schema {
        // overriding known timestamp fields which were inferred as string fields
        updated_schema = override_existing_timestamp_fields(existing_schema, updated_schema);
    }

    if let Some(log_records) = log_records {
        for log_record in log_records {
            updated_schema = update_data_type_to_datetime(
                updated_schema.clone(),
                log_record.clone(),
                &existing_field_names,
            );
        }
    }

    let Some(time_partition) = time_partition else {
        return updated_schema;
    };

    let new_schema: Vec<Field> = updated_schema
        .fields()
        .iter()
        .map(|field| {
            // time_partition field not present in existing schema with string type data as timestamp
            if field.name() == time_partition
                && !existing_field_names.contains(field.name())
                && field.data_type() == &DataType::Utf8
            {
                let new_data_type = DataType::Timestamp(TimeUnit::Millisecond, None);
                Field::new(field.name(), new_data_type, true)
            } else {
                Field::new(field.name(), field.data_type().clone(), true)
            }
        })
        .collect();
    Arc::new(Schema::new(new_schema))
}

pub fn update_data_type_to_datetime(
    schema: Arc<Schema>,
    log_record: Value,
    ignore_field_names: &HashSet<String>,
) -> Arc<Schema> {
    let Value::Object(map) = log_record else {
        return schema;
    };
    let new_schema: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(Value::String(s)) = map.get(field.name()) {
                // for new fields in json with inferred type string, parse to check if timestamp value
                if field.data_type() == &DataType::Utf8
                    && !ignore_field_names.contains(field.name().as_str())
                    && (DateTime::parse_from_rfc3339(s).is_ok()
                        || DateTime::parse_from_rfc2822(s).is_ok())
                {
                    // Update the field's data type to Timestamp
                    return Field::new(
                        field.name().clone(),
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    );
                }
            }
            // Return the original field if no update is needed
            Field::new(field.name(), field.data_type().clone(), true)
        })
        .collect();

    Arc::new(Schema::new(new_schema))
}
