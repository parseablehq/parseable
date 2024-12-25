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

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use serde_json::map::Map;
use serde_json::value::Value;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum JsonFlattenError {
    #[error("Cannot flatten this JSON")]
    CannotFlatten,
    #[error("Ingestion failed as field {0} is not part of the log")]
    FieldNotPartOfLog(String),
    #[error("Ingestion failed as field {0} is empty or 'null'")]
    FieldEmptyOrNull(String),
    #[error("Ingestion failed as field {0} has a floating point value")]
    FieldIsFloatingPoint(String),
    #[error("Ingestion failed as field {0} is not a string")]
    FieldNotString(String),
    #[error("Field {0} is not in the correct datetime format")]
    InvalidDatetimeFormat(String),
    #[error("Field {0} value is more than {1} days old")]
    TimestampTooOld(String, i64),
    #[error("Expected object in array of objects")]
    ExpectedObjectInArray,
    #[error("Found non-object element while flattening array of objects")]
    NonObjectInArray,
}

pub fn flatten(
    nested_value: &mut Value,
    separator: &str,
    time_partition: Option<&String>,
    time_partition_limit: Option<&String>,
    custom_partition: Option<&String>,
    validation_required: bool,
) -> Result<(), JsonFlattenError> {
    match nested_value {
        Value::Object(nested_dict) => {
            if validation_required {
                validate_time_partition(nested_dict, time_partition, time_partition_limit)?;
                validate_custom_partition(nested_dict, custom_partition)?;
            }
            let mut map = Map::new();
            flatten_object(&mut map, None, nested_dict, separator)?;
            *nested_dict = map;
        }
        Value::Array(arr) => {
            for nested_value in arr {
                flatten(
                    nested_value,
                    separator,
                    time_partition,
                    time_partition_limit,
                    custom_partition,
                    validation_required,
                )?;
            }
        }
        _ => return Err(JsonFlattenError::CannotFlatten),
    }

    Ok(())
}

pub fn validate_custom_partition(
    value: &Map<String, Value>,
    custom_partition: Option<&String>,
) -> Result<(), JsonFlattenError> {
    let Some(custom_partition) = custom_partition else {
        return Ok(());
    };
    let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();

    for field in custom_partition_list {
        let trimmed_field = field.trim();
        let Some(field_value) = value.get(trimmed_field) else {
            return Err(JsonFlattenError::FieldNotPartOfLog(
                trimmed_field.to_owned(),
            ));
        };

        fn is_null_or_empty(value: &Value) -> bool {
            match value {
                Value::Null => true,
                Value::Object(o) if o.is_empty() => true,
                Value::Array(a) if a.is_empty() => true,
                Value::String(s) if s.is_empty() => true,
                _ => false,
            }
        }

        if is_null_or_empty(field_value) {
            return Err(JsonFlattenError::FieldEmptyOrNull(trimmed_field.to_owned()));
        }

        if field_value.is_f64() {
            return Err(JsonFlattenError::FieldIsFloatingPoint(
                trimmed_field.to_owned(),
            ));
        }
    }

    Ok(())
}

pub fn validate_time_partition(
    value: &Map<String, Value>,
    time_partition: Option<&String>,
    time_partition_limit: Option<&String>,
) -> Result<(), JsonFlattenError> {
    let Some(partition_key) = time_partition else {
        return Ok(());
    };

    let limit_days = time_partition_limit
        .and_then(|limit| limit.parse::<i64>().ok())
        .unwrap_or(30);

    let Some(timestamp_value) = value.get(partition_key) else {
        return Err(JsonFlattenError::FieldNotPartOfLog(
            partition_key.to_owned(),
        ));
    };

    let Value::String(timestamp_str) = timestamp_value else {
        return Err(JsonFlattenError::FieldNotString(partition_key.to_owned()));
    };
    let Ok(parsed_timestamp) = timestamp_str.parse::<DateTime<Utc>>() else {
        return Err(JsonFlattenError::InvalidDatetimeFormat(
            partition_key.to_owned(),
        ));
    };
    let cutoff_date = Utc::now().naive_utc() - Duration::days(limit_days);
    if parsed_timestamp.naive_utc() >= cutoff_date {
        Ok(())
    } else {
        Err(JsonFlattenError::TimestampTooOld(
            partition_key.to_owned(),
            limit_days,
        ))
    }
}

pub fn flatten_with_parent_prefix(
    nested_value: &mut Value,
    prefix: &str,
    separator: &str,
) -> Result<(), JsonFlattenError> {
    let Value::Object(nested_obj) = nested_value else {
        return Err(JsonFlattenError::NonObjectInArray);
    };

    let mut map = Map::new();
    flatten_object(&mut map, Some(prefix), nested_obj, separator)?;
    *nested_obj = map;

    Ok(())
}

pub fn flatten_object(
    output_map: &mut Map<String, Value>,
    parent_key: Option<&str>,
    nested_map: &mut Map<String, Value>,
    separator: &str,
) -> Result<(), JsonFlattenError> {
    for (key, mut value) in nested_map {
        let new_key = match parent_key {
            Some(parent) => format!("{parent}{separator}{key}"),
            None => key.to_string(),
        };

        match &mut value {
            Value::Object(obj) => {
                flatten_object(output_map, Some(&new_key), obj, separator)?;
            }
            Value::Array(arr) if arr.iter().any(Value::is_object) => {
                flatten_array_objects(output_map, &new_key, arr, separator)?;
            }
            _ => {
                output_map.insert(new_key, std::mem::take(value));
            }
        }
    }
    Ok(())
}

pub fn flatten_array_objects(
    output_map: &mut Map<String, Value>,
    parent_key: &str,
    arr: &mut [Value],
    separator: &str,
) -> Result<(), JsonFlattenError> {
    let mut columns: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    for (index, value) in arr.iter_mut().enumerate() {
        match value {
            Value::Object(nested_object) => {
                let mut output_map = Map::new();
                flatten_object(&mut output_map, Some(parent_key), nested_object, separator)?;
                for (key, value) in output_map {
                    let column = columns
                        .entry(key)
                        .or_insert_with(|| vec![Value::Null; index]);
                    column.push(value);
                }
            }
            Value::Null => {
                for column in columns.values_mut() {
                    column.push(Value::Null);
                }
            }
            _ => return Err(JsonFlattenError::NonObjectInArray),
        }

        // Ensure all columns are extended with null values if they weren't updated in this iteration
        let max_len = index + 1;
        for column in columns.values_mut() {
            while column.len() < max_len {
                column.push(Value::Null);
            }
        }
    }

    // Update the main map with new keys and their corresponding arrays
    for (key, values) in columns {
        output_map.insert(key, Value::Array(values));
    }

    Ok(())
}

pub fn flatten_json(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(arr) => arr.iter().flat_map(flatten_json).collect(),
        Value::Object(map) => map
            .iter()
            .fold(vec![Map::new()], |results, (key, val)| match val {
                Value::Array(arr) => arr
                    .iter()
                    .flat_map(flatten_json)
                    .flat_map(|flattened_item| {
                        results.iter().map(move |result| {
                            let mut new_obj = result.clone();
                            new_obj.insert(key.clone(), flattened_item.clone());
                            new_obj
                        })
                    })
                    .collect(),
                Value::Object(_) => flatten_json(val)
                    .iter()
                    .flat_map(|nested_result| {
                        results.iter().map(move |result| {
                            let mut new_obj = result.clone();
                            new_obj.insert(key.clone(), nested_result.clone());
                            new_obj
                        })
                    })
                    .collect(),
                _ => results
                    .into_iter()
                    .map(|mut result| {
                        result.insert(key.clone(), val.clone());
                        result
                    })
                    .collect(),
            })
            .into_iter()
            .map(Value::Object)
            .collect(),
        _ => vec![value.clone()],
    }
}

pub fn convert_to_array(flattened: Vec<Value>) -> Result<Value, JsonFlattenError> {
    let mut result = Vec::new();
    for item in flattened {
        let mut map = Map::new();
        let Some(item) = item.as_object() else {
            return Err(JsonFlattenError::ExpectedObjectInArray);
        };
        for (key, value) in item {
            map.insert(key.clone(), value.clone());
        }
        result.push(Value::Object(map));
    }
    Ok(Value::Array(result))
}

#[cfg(test)]
mod tests {
    use crate::utils::json::flatten::flatten_array_objects;

    use super::flatten;
    use serde_json::{json, Map, Value};

    #[test]
    fn flatten_single_key_string() {
        let mut obj = json!({"key": "value"});
        let expected = obj.clone();
        flatten(&mut obj, "_", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_single_key_int() {
        let mut obj = json!({"key": 1});
        let expected = obj.clone();
        flatten(&mut obj, "_", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_multiple_key_value() {
        let mut obj = json!({"key1": 1, "key2": "value2"});
        let expected = obj.clone();
        flatten(&mut obj, "_", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_nested_single_key_value() {
        let mut obj = json!({"key": "value", "nested_key": {"key":"value"}});
        let expected = json!({"key": "value", "nested_key.key": "value"});
        flatten(&mut obj, ".", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_multiple_key_value() {
        let mut obj = json!({"key": "value", "nested_key": {"key1":"value1", "key2": "value2"}});
        let expected =
            json!({"key": "value", "nested_key.key1": "value1", "nested_key.key2": "value2"});
        flatten(&mut obj, ".", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_key_value_with_array() {
        let mut obj = json!({"key": "value", "nested_key": {"key1":[1,2,3]}});
        let expected = json!({"key": "value", "nested_key.key1": [1,2,3]});
        flatten(&mut obj, ".", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array() {
        let mut obj = json!({"key": [{"a": "value0"}, {"a": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"]});
        flatten(&mut obj, ".", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nulls() {
        let mut obj = json!({"key": [{"a": "value0"}, {"a": "value1", "b": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"], "key.b": [null, "value1"]});
        flatten(&mut obj, ".", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nulls_reversed() {
        let mut obj = json!({"key": [{"a": "value0", "b": "value0"}, {"a": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"], "key.b": ["value0", null]});
        flatten(&mut obj, ".", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nested_obj() {
        let mut obj = json!({"key": [{"a": {"p": 0}, "b": "value0"}, {"b": "value1"}]});
        let expected = json!({"key.a.p": [0, null], "key.b": ["value0", "value1"]});
        flatten(&mut obj, ".", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nested_obj_array() {
        let mut obj = json!({"key": [{"a": [{"p": "value0", "q": "value0"}, {"p": "value1", "q": null}], "b": "value0"}, {"b": "value1"}]});
        let expected = json!({"key.a.p": [["value0", "value1"], null], "key.a.q": [["value0", null], null], "key.b": ["value0", "value1"]});
        flatten(&mut obj, ".", None, None, None, false).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_mixed_object() {
        let mut obj = json!({"a": 42, "arr": ["1", {"key": "2"}, {"key": {"nested": "3"}}]});
        assert!(flatten(&mut obj, ".", None, None, None, false).is_err());
    }

    #[test]
    fn flatten_array_nulls_at_start() {
        let Value::Array(mut arr) = json!([
            null,
            {"p": 2, "q": 2},
            {"q": 3},
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", &mut arr, ".").unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("key.p").unwrap(), &json!([null, 2, null]));
        assert_eq!(map.get("key.q").unwrap(), &json!([null, 2, 3]));
    }

    #[test]
    fn flatten_array_objects_nulls_at_end() {
        let Value::Array(mut arr) = json!([{"a": 1, "b": 1}, {"a": 2}, null]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", &mut arr, ".").unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("key.a").unwrap(), &json!([1, 2, null]));
        assert_eq!(map.get("key.b").unwrap(), &json!([1, null, null]));
    }

    #[test]
    fn flatten_array_objects_nulls_in_middle() {
        let Value::Array(mut arr) = json!([{"a": 1, "b": 1}, null, {"a": 3, "c": 3}]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", &mut arr, ".").unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.a").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.b").unwrap(), &json!([1, null, null]));
        assert_eq!(map.get("key.c").unwrap(), &json!([null, null, 3]));
    }

    #[test]
    fn flatten_array_test() {
        let Value::Array(mut arr) = json!([
            {"p": 1, "q": 1},
            {"r": 2, "q": 2},
            {"p": 3, "r": 3}
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", &mut arr, ".").unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.p").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.q").unwrap(), &json!([1, 2, null]));
        assert_eq!(map.get("key.r").unwrap(), &json!([null, 2, 3]));
    }

    #[test]
    fn flatten_array_nested_test() {
        let Value::Array(mut arr) = json!([
            {"p": 1, "q": [{"x": 1}, {"x": 2}]},
            {"r": 2, "q": [{"x": 1}]},
            {"p": 3, "r": 3}
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", &mut arr, ".").unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.p").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.q.x").unwrap(), &json!([[1, 2], [1], null]));
        assert_eq!(map.get("key.r").unwrap(), &json!([null, 2, 3]));
    }
}
