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
 */

use crate::event::DEFAULT_TIMESTAMP_KEY;
use crate::utils::arrow::get_field;
use serde::{Deserialize, Serialize};
use std::str;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StaticSchema {
    fields: Vec<SchemaFields>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaFields {
    name: String,
    data_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParsedSchema {
    pub fields: Vec<Fields>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fields {
    name: String,
    data_type: DataType,
    nullable: bool,
    dict_id: i64,
    dict_is_ordered: bool,
    metadata: HashMap<String, String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {}
pub fn convert_static_schema_to_arrow_schema(
    static_schema: StaticSchema,
    time_partition: &str,
    custom_partition: Option<&String>,
) -> Result<Arc<Schema>, StaticSchemaError> {
    let mut parsed_schema = ParsedSchema {
        fields: Vec::new(),
        metadata: HashMap::new(),
    };
    let mut time_partition_exists = false;

    if let Some(custom_partition) = custom_partition {
        let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
        let mut custom_partition_exists = HashMap::with_capacity(custom_partition_list.len());

        for partition in &custom_partition_list {
            if static_schema
                .fields
                .iter()
                .any(|field| &field.name == partition)
            {
                custom_partition_exists.insert(partition.to_string(), true);
            }
        }

        for partition in &custom_partition_list {
            if !custom_partition_exists.contains_key(*partition) {
                return Err(StaticSchemaError::MissingCustomPartition(
                    partition.to_string(),
                ));
            }
        }
    }

    let mut existing_field_names: HashSet<String> = HashSet::new();

    for mut field in static_schema.fields {
        validate_field_names(&field.name, &mut existing_field_names)?;
        if !time_partition.is_empty() && field.name == time_partition {
            time_partition_exists = true;
            field.data_type = "datetime".to_string();
        }

        let parsed_field = Fields {
            name: field.name.clone(),

            data_type: {
                match field.data_type.as_str() {
                    "int" => DataType::Int64,
                    "double" | "float" => DataType::Float64,
                    "boolean" => DataType::Boolean,
                    "string" => DataType::Utf8,
                    "datetime" => DataType::Timestamp(TimeUnit::Millisecond, None),
                    "date" => DataType::Date32,
                    "string_list" => {
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                    }
                    "int_list" => {
                        DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
                    }
                    "double_list" | "float_list" => {
                        DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                    }
                    "boolean_list" => {
                        DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)))
                    }
                    _ => {
                        return Err(StaticSchemaError::UnrecognizedDataType(
                            field.data_type.clone(),
                        ));
                    }
                }
            },
            nullable: default_nullable(),
            dict_id: default_dict_id(),
            dict_is_ordered: default_dict_is_ordered(),
            metadata: HashMap::new(),
        };

        parsed_schema.fields.push(parsed_field);
    }
    if !time_partition.is_empty() && !time_partition_exists {
        return Err(StaticSchemaError::MissingTimePartition(
            time_partition.to_string(),
        ));
    }
    add_parseable_fields_to_static_schema(parsed_schema)
}

fn add_parseable_fields_to_static_schema(
    parsed_schema: ParsedSchema,
) -> Result<Arc<Schema>, StaticSchemaError> {
    let mut schema: Vec<Arc<Field>> = Vec::new();
    for field in parsed_schema.fields.iter() {
        let field = Field::new(field.name.clone(), field.data_type.clone(), field.nullable);
        schema.push(Arc::new(field));
    }

    if get_field(&schema, DEFAULT_TIMESTAMP_KEY).is_some() {
        return Err(StaticSchemaError::ReservedKey(DEFAULT_TIMESTAMP_KEY));
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

    // prepare the record batch and new fields to be added
    let schema = Arc::new(Schema::new(schema));
    Ok(schema)
}

fn default_nullable() -> bool {
    true
}
fn default_dict_id() -> i64 {
    0
}
fn default_dict_is_ordered() -> bool {
    false
}

fn validate_field_names(
    field_name: &str,
    existing_fields: &mut HashSet<String>,
) -> Result<(), StaticSchemaError> {
    if field_name.is_empty() {
        return Err(StaticSchemaError::EmptyFieldName);
    }

    if !existing_fields.insert(field_name.to_string()) {
        return Err(StaticSchemaError::DuplicateField(field_name.to_string()));
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum StaticSchemaError {
    #[error(
        "custom partition field {0} does not exist in the schema for the static schema logstream"
    )]
    MissingCustomPartition(String),

    #[error(
        "time partition field {0} does not exist in the schema for the static schema logstream"
    )]
    MissingTimePartition(String),

    #[error("field {0:?} is a reserved field")]
    ReservedKey(&'static str),

    #[error("field name cannot be empty")]
    EmptyFieldName,

    #[error("duplicate field name: {0}")]
    DuplicateField(String),

    #[error("unrecognized data type: {0}")]
    UnrecognizedDataType(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_field_names() {
        let mut existing_field_names: HashSet<String> = HashSet::new();
        assert!(validate_field_names("", &mut existing_field_names).is_err());
    }

    #[test]
    fn duplicate_field_names() {
        let mut existing_field_names: HashSet<String> = HashSet::new();
        let _ = validate_field_names("test_field", &mut existing_field_names);
        assert!(validate_field_names("test_field", &mut existing_field_names).is_err());
    }

    #[test]
    fn unrecognized_data_type() {
        let static_schema = StaticSchema {
            fields: vec![SchemaFields {
                name: "test_field".to_string(),
                data_type: "unknown_type".to_string(),
            }],
        };

        let result = convert_static_schema_to_arrow_schema(static_schema, "", None);

        assert!(result.is_err());
        match result.unwrap_err() {
            StaticSchemaError::UnrecognizedDataType(data_type) => {
                assert_eq!(data_type, "unknown_type");
            }
            _ => panic!("Expected UnrecognizedDataType error"),
        }
    }
}
