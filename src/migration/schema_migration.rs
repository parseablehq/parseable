/*
* Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema};
use serde_json::Value;

pub(super) fn v1_v4(schema: Option<Value>) -> anyhow::Result<Schema> {
    if let Some(schema) = schema {
        value_to_schema(schema)
    } else {
        Ok(Schema::empty())
    }
}

pub(super) fn v2_v4(schemas: HashMap<String, Value>) -> anyhow::Result<Schema> {
    let mut derived_schemas = Vec::new();

    for value in schemas.into_values() {
        let schema = value_to_schema(value)?;
        derived_schemas.push(schema);
    }

    let schema = Schema::try_merge(derived_schemas)?;
    let mut fields: Vec<_> = schema.fields.iter().cloned().collect();
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Ok(Schema::new(fields))
}

fn value_to_schema(schema: Value) -> Result<Schema, anyhow::Error> {
    let fields = schema
        .as_object()
        .expect("schema is an object")
        .get("fields")
        .expect("fields exists")
        .as_array()
        .expect("fields is an array");

    let mut new_fields = Vec::new();

    for field in fields {
        let field = field.as_object().unwrap();
        let field_name: String =
            serde_json::from_value(field.get("name").unwrap().clone()).unwrap();
        let field_dt: DataType =
            serde_json::from_value(field.get("data_type").unwrap().clone()).unwrap();
        new_fields.push(Field::new(field_name, field_dt, true));
    }
    new_fields.sort_by(|a, b| a.name().cmp(b.name()));

    Ok(Schema::new(new_fields))
}
