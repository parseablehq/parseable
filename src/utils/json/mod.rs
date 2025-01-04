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

use std::num::NonZeroU32;

use serde_json;
use serde_json::Value;

use crate::metadata::SchemaVersion;

pub mod flatten;

pub fn flatten_json_body(
    body: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    validation_required: bool,
    log_source: &str,
) -> Result<Value, anyhow::Error> {
    let mut nested_value = if schema_version == SchemaVersion::V1 && !log_source.contains("otel") {
        flatten::generic_flattening(body)?
    } else {
        body
    };

    flatten::flatten(
        &mut nested_value,
        "_",
        time_partition,
        time_partition_limit,
        custom_partition,
        validation_required,
    )?;

    Ok(nested_value)
}

pub fn convert_array_to_object(
    body: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &str,
) -> Result<Vec<Value>, anyhow::Error> {
    let data = flatten_json_body(
        body,
        time_partition,
        time_partition_limit,
        custom_partition,
        schema_version,
        true,
        log_source,
    )?;
    let value_arr = match data {
        Value::Array(arr) => arr,
        value @ Value::Object(_) => vec![value],
        _ => unreachable!("flatten would have failed beforehand"),
    };
    Ok(value_arr)
}

pub fn convert_to_string(value: &Value) -> Value {
    match value {
        Value::Null => Value::String("null".to_owned()),
        Value::Bool(b) => Value::String(b.to_string()),
        Value::Number(n) => Value::String(n.to_string()),
        Value::String(s) => Value::String(s.to_owned()),
        Value::Array(v) => {
            let new_vec = v.iter().map(convert_to_string).collect();
            Value::Array(new_vec)
        }
        Value::Object(map) => {
            let new_map = map
                .iter()
                .map(|(k, v)| (k.clone(), convert_to_string(v)))
                .collect();
            Value::Object(new_map)
        }
    }
}
