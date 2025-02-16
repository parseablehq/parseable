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

impl StaticSchema {
    pub fn convert_to_arrow_schema(
        self,
        time_partition: &str,
        custom_partition: Option<&String>,
    ) -> Result<Arc<Schema>, AnyError> {
        let mut fields = Vec::new();
        let mut time_partition_exists = false;

        if let Some(custom_partition) = custom_partition {
            for partition in custom_partition.split(',') {
                if !self.fields.iter().any(|field| field.name == partition) {
                    return Err(anyhow!("custom partition field {partition} does not exist in the schema for the static schema logstream"));
                }
            }
        }
        for mut field in self.fields {
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

            fields.push(parsed_field);
        }

        if !time_partition.is_empty() && !time_partition_exists {
            return Err(anyhow!("time partition field {time_partition} does not exist in the schema for the static schema logstream"));
        }

        let mut schema: Vec<Arc<Field>> = Vec::new();
        for field in fields {
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
        Ok(Arc::new(Schema::new(schema)))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaFields {
    name: String,
    data_type: String,
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

fn default_nullable() -> bool {
    true
}
fn default_dict_id() -> i64 {
    0
}
fn default_dict_is_ordered() -> bool {
    false
}
