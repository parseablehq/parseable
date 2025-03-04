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

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::event::DEFAULT_TIMESTAMP_KEY;

const DEFAULT_NULLABLE: bool = true;

type FieldName = String;
type FieldType = String;

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

    #[error("field {0:?} is duplicated")]
    DuplicateField(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SchemaFields {
    name: FieldName,
    data_type: FieldType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StaticSchema {
    fields: Vec<SchemaFields>,
}

impl StaticSchema {
    pub fn convert_to_arrow_schema(
        self,
        time_partition: &str,
        custom_partition: Option<&String>,
    ) -> Result<Arc<Schema>, StaticSchemaError> {
        let mut schema: Vec<Arc<Field>> = Vec::new();

        // Convert to hashmap for easy access and operation
        let mut fields: HashMap<FieldName, FieldType> = HashMap::new();

        for field in self.fields {
            if field.name.is_empty() {
                return Err(StaticSchemaError::EmptyFieldName);
            }

            if fields.contains_key(&field.name) {
                return Err(StaticSchemaError::DuplicateField(field.name.to_owned()));
            }

            fields.insert(field.name, field.data_type);
        }

        // Ensures all custom partitions are present in schema
        if let Some(custom_partition) = custom_partition {
            for partition in custom_partition.split(',') {
                if !fields.contains_key(partition) {
                    return Err(StaticSchemaError::MissingCustomPartition(
                        partition.to_owned(),
                    ));
                }
            }
        }

        // Ensures default timestamp is not mentioned(as it is only inserted by parseable)
        if fields.contains_key(DEFAULT_TIMESTAMP_KEY) {
            return Err(StaticSchemaError::ReservedKey(DEFAULT_TIMESTAMP_KEY));
        }

        // If time partitioning is enabled, mutate the datatype to be datetime
        if !time_partition.is_empty() {
            let Some(field_type) = fields.get_mut(time_partition) else {
                return Err(StaticSchemaError::MissingTimePartition(
                    time_partition.to_owned(),
                ));
            };
            *field_type = "datetime".to_string();
        }

        for (field_name, field_type) in fields {
            let data_type = match field_type.as_str() {
                "int" => DataType::Int64,
                "double" | "float" => DataType::Float64,
                "boolean" => DataType::Boolean,
                "string" => DataType::Utf8,
                "datetime" => DataType::Timestamp(TimeUnit::Millisecond, None),
                "string_list" => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                "int_list" => DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                "double_list" | "float_list" => {
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                }
                "boolean_list" => {
                    DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)))
                }
                _ => DataType::Null,
            };
            let field = Field::new(&field_name, data_type, DEFAULT_NULLABLE);
            schema.push(Arc::new(field));
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

        // prepare the record batch and new fields to be added
        Ok(Arc::new(Schema::new(schema)))
    }
}
