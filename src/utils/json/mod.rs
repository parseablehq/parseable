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

pub mod flatten;

pub fn flatten_json_body(
    body: &Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    validation_required: bool,
) -> Result<Value, anyhow::Error> {
    let mut nested_value = flatten::convert_to_array(flatten::flatten_json(body))?;

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
    body: &Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
) -> Result<Vec<Value>, anyhow::Error> {
    let data = flatten_json_body(
        body,
        time_partition,
        time_partition_limit,
        custom_partition,
        true,
    )?;
    let value_arr = match data {
        Value::Array(arr) => arr,
        value @ Value::Object(_) => vec![value],
        _ => unreachable!("flatten would have failed beforehand"),
    };
    Ok(value_arr)
}
