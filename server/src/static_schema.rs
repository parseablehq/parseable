use crate::event::{DEFAULT_METADATA_KEY, DEFAULT_TAGS_KEY, DEFAULT_TIMESTAMP_KEY};
use crate::utils::arrow::get_field;
use anyhow::{anyhow, Error as AnyError};
use serde::{Deserialize, Serialize};
use std::str;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::{collections::HashMap, sync::Arc};
#[derive(Serialize, Deserialize, Debug)]
pub struct StaticSchema {
    fields: Vec<SchemaFields>,
}

#[derive(Serialize, Deserialize, Debug)]

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
) -> Result<Arc<Schema>, AnyError> {
    let mut parsed_schema = ParsedSchema {
        fields: Vec::new(),
        metadata: HashMap::new(),
    };
    for field in static_schema.fields.iter() {
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
    let schema = add_parseable_fields_to_static_schema(parsed_schema);
    if schema.is_err() {
        return Err(schema.err().unwrap());
    }
    Ok(schema.unwrap())
}

fn add_parseable_fields_to_static_schema(
    parsed_schema: ParsedSchema,
) -> Result<Arc<Schema>, AnyError> {
    let mut schema: Vec<Arc<Field>> = Vec::new();
    for field in parsed_schema.fields.iter() {
        let field = Field::new(field.name.clone(), field.data_type.clone(), field.nullable);
        schema.push(Arc::new(field));
    }
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
    schema.push(Arc::new(Field::new(DEFAULT_TAGS_KEY, DataType::Utf8, true)));
    schema.push(Arc::new(Field::new(
        DEFAULT_METADATA_KEY,
        DataType::Utf8,
        true,
    )));

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
