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
use anyhow::{anyhow, Error as AnyError};
use serde::{Deserialize, Serialize};
use std::str;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::{collections::HashMap, sync::Arc};
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
) -> Result<Arc<Schema>, AnyError> {
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
                return Err(anyhow!("custom partition field {partition} does not exist in the schema for the static schema logstream"));
            }
        }
    }
    for mut field in static_schema.fields {
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
                    _ => DataType::Null,
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
        return Err(anyhow! {
            format!(
                "time partition field {time_partition} does not exist in the schema for the static schema logstream"
            ),
        });
    }
    add_parseable_fields_to_static_schema(parsed_schema)
}

fn add_parseable_fields_to_static_schema(
    parsed_schema: ParsedSchema,
) -> Result<Arc<Schema>, AnyError> {
    let mut schema: Vec<Arc<Field>> = Vec::new();
    for field in parsed_schema.fields.iter() {
        let field = Field::new(field.name.clone(), field.data_type.clone(), field.nullable);
        schema.push(Arc::new(field));
    }

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
