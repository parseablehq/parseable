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
use std::num::NonZeroU32;

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
    #[error("Ingestion failed as field {0} is an object")]
    FieldIsObject(String),
    #[error("Ingestion failed as field {0} is an array")]
    FieldIsArray(String),
    #[error("Ingestion failed as field {0} contains a period in the value")]
    FieldContainsPeriod(String),
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
    #[error("JSON hierarchy exceeds maximum depth of {0} levels")]
    InvalidHierarchy(String),
}

// Recursively flattens JSON objects and arrays, e.g. with the separator `.`, starting from the TOP
// `{"key": "value", "nested_key": {"key":"value"}}` becomes `{"key": "value", "nested_key.key": "value"}`
pub fn flatten(
    nested_value: &mut Value,
    separator: &str,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
) -> Result<(), JsonFlattenError> {
    match nested_value {
        Value::Object(nested_dict) => {
            validate_time_partition(nested_dict, time_partition, time_partition_limit)?;
            validate_custom_partition(nested_dict, custom_partition)?;
            let mut map = Map::new();
            flatten_object(&mut map, None, nested_dict, separator)?;
            *nested_dict = map;
        }
        Value::Array(arr) => {
            for nested_value in arr {
                // Recursively flatten each element, ONLY in the TOP array
                flatten(
                    nested_value,
                    separator,
                    time_partition,
                    time_partition_limit,
                    custom_partition,
                )?;
            }
        }
        _ => return Err(JsonFlattenError::CannotFlatten),
    }

    Ok(())
}

// Validates the presence and content of custom partition fields, that it is
// not null, empty, an object , an array, or contain a `.` when serialized
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

        // The field should not be null, empty, an object, an array or contain a `.` in the value
        match field_value {
            Value::Null => {
                return Err(JsonFlattenError::FieldEmptyOrNull(trimmed_field.to_owned()))
            }
            Value::String(s) if s.is_empty() => {
                return Err(JsonFlattenError::FieldEmptyOrNull(trimmed_field.to_owned()))
            }
            Value::Object(_) => {
                return Err(JsonFlattenError::FieldIsObject(trimmed_field.to_owned()))
            }
            Value::Array(_) => {
                return Err(JsonFlattenError::FieldIsArray(trimmed_field.to_owned()))
            }
            Value::String(s) if s.contains('.') => {
                return Err(JsonFlattenError::FieldContainsPeriod(
                    trimmed_field.to_owned(),
                ))
            }
            Value::Number(n) if n.is_f64() => {
                return Err(JsonFlattenError::FieldContainsPeriod(
                    trimmed_field.to_owned(),
                ))
            }
            _ => {}
        }
    }

    Ok(())
}

// Validates time partitioning constraints, checking if a timestamp is a string
// that can be parsed as datetime within the configured time limit
pub fn validate_time_partition(
    value: &Map<String, Value>,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
) -> Result<(), JsonFlattenError> {
    let Some(partition_key) = time_partition else {
        return Ok(());
    };

    let limit_days = time_partition_limit.map_or(30, |days| days.get() as i64);

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

// Flattens starting from only object types at TOP, e.g. with the parent_key `root` and separator `_`
// `{ "a": { "b": 1, c: { "d": 2 } } }` becomes `{"root_a_b":1,"root_a_c_d":2}`
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

// Flattens a nested JSON Object/Map into another target Map
fn flatten_object(
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

// Flattens a nested JSON Array into the parent Map
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

#[allow(clippy::too_many_arguments)]
pub struct FlattenContext<'a> {
    pub current_level: usize,
    pub separator: &'a str,
    pub time_partition: Option<&'a String>,
    pub time_partition_limit: Option<NonZeroU32>,
    pub custom_partition: Option<&'a String>,
    pub flatten_depth_limit: usize,
}

/// Recursively flattens a JSON value.
/// - If the value is an array, it flattens all elements of the array.
/// - If the value is an object, it flattens all nested objects and arrays.
/// - If the JSON value is heavily nested (with more than 4 levels of hierarchy), returns error
/// - Otherwise, it returns the value itself in a vector.
///
/// Examples:
/// 1. `{"a": 1}` ~> `[{"a": 1}]`
/// 2. `[{"a": 1}, {"b": 2}]` ~> `[{"a": 1}, {"b": 2}]`
/// 3. `[{"a": [{"b": 1}, {"c": 2}]}]` ~> `[{"a": {"b": 1)}}, {"a": {"c": 2)}}]`
/// 4. `{"a": [{"b": 1}, {"c": 2}], "d": {"e": 4}}` ~> `[{"a": {"b":1}, "d": {"e":4}}, {"a": {"c":2}, "d": {"e":4}}]`
/// 5. `{"a":{"b":{"c":{"d":{"e":["a","b"]}}}}}` ~> returns error - heavily nested, cannot flatten this JSON
pub fn generic_flattening(
    value: &mut Value,
    context: &FlattenContext,
    parent_key: Option<&str>,
) -> Result<Vec<Value>, JsonFlattenError> {
    if context.current_level > context.flatten_depth_limit {
        return Err(JsonFlattenError::InvalidHierarchy(
            context.flatten_depth_limit.to_string(),
        ));
    }

    match value {
        Value::Array(arr) => process_json_array(arr, context, parent_key),
        Value::Object(map) => process_json_object(map, context, parent_key),
        _ => Ok(vec![value.clone()]),
    }
}

fn process_json_array(
    arr: &mut [Value],
    context: &FlattenContext,
    parent_key: Option<&str>,
) -> Result<Vec<Value>, JsonFlattenError> {
    if let Some(parent) = parent_key {
        return Ok(arr
            .iter_mut()
            .map(|flattened_item| {
                let mut map = Map::new();
                map.insert(parent.to_string(), flattened_item.clone());
                Value::Object(map)
            })
            .collect());
    }
    Ok(arr
        .iter_mut()
        .flat_map(|flattened_item| {
            generic_flattening(flattened_item, context, parent_key).unwrap_or_default()
        })
        .collect())
}

fn process_json_object(
    map: &mut Map<String, Value>,
    context: &FlattenContext,
    parent_key: Option<&str>,
) -> Result<Vec<Value>, JsonFlattenError> {
    if context.current_level == 1 {
        validate_time_partition(map, context.time_partition, context.time_partition_limit)?;
        validate_custom_partition(map, context.custom_partition)?;
    }

    let mut results = vec![Map::new()];

    for (key, val) in map.iter_mut() {
        let new_key = create_nested_key(parent_key, key, context.separator);
        let new_results = match val {
            Value::Array(arr) => process_array_value(arr, &new_key, &results, context.separator),
            Value::Object(_) => process_object_value(val, &new_key, &results, context),
            _ => Ok(create_results_with_value(&results, &new_key, val)),
        }?;

        if !new_results.is_empty() {
            results = new_results;
        }
    }

    Ok(results.into_iter().map(Value::Object).collect())
}

fn create_nested_key(parent_key: Option<&str>, key: &str, separator: &str) -> String {
    match parent_key {
        Some(parent) => format!("{parent}{separator}{key}"),
        None => key.to_string(),
    }
}

fn process_array_value(
    arr: &mut [Value],
    new_key: &str,
    results: &[Map<String, Value>],
    separator: &str,
) -> Result<Vec<Map<String, Value>>, JsonFlattenError> {
    let mut new_results = Vec::new();

    for item in arr.iter_mut() {
        match item {
            Value::Object(obj) => {
                new_results.extend(flatten_nested_object(obj, new_key, results, separator));
            }
            _ => {
                new_results.extend(create_results_with_value(results, new_key, item));
            }
        }
    }

    Ok(new_results)
}

fn flatten_nested_object(
    obj: &mut Map<String, Value>,
    base_key: &str,
    results: &[Map<String, Value>],
    separator: &str,
) -> Vec<Map<String, Value>> {
    let mut temp_results = Vec::new();

    for (k, v) in obj {
        let nested_key = format!("{base_key}{separator}{k}");
        match v {
            Value::Array(nested_arr) => {
                for arr_item in nested_arr {
                    temp_results.extend(create_results_with_value(results, &nested_key, arr_item));
                }
            }
            _ => {
                temp_results.extend(create_results_with_value(results, &nested_key, v));
            }
        }
    }

    temp_results
}

fn create_results_with_value(
    results: &[Map<String, Value>],
    key: &str,
    value: &Value,
) -> Vec<Map<String, Value>> {
    results
        .iter()
        .map(|result| {
            let mut new_obj = result.clone();
            new_obj.insert(key.to_string(), value.clone());
            new_obj
        })
        .collect()
}

fn process_object_value(
    val: &mut Value,
    new_key: &str,
    results: &[Map<String, Value>],
    context: &FlattenContext,
) -> Result<Vec<Map<String, Value>>, JsonFlattenError> {
    let nested_context = FlattenContext {
        current_level: context.current_level + 1,
        ..*context
    };

    let nested_results = generic_flattening(val, &nested_context, Some(new_key))?;

    Ok(nested_results
        .into_iter()
        .flat_map(|nested| {
            if let Value::Object(obj) = nested {
                results
                    .iter()
                    .map(|result| {
                        let mut new_obj = result.clone();
                        new_obj.extend(obj.clone());
                        new_obj
                    })
                    .collect()
            } else {
                vec![]
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::utils::json::flatten::{flatten_array_objects, generic_flattening, FlattenContext};

    use super::{flatten, JsonFlattenError};
    use serde_json::{json, Map, Value};

    #[test]
    fn flatten_single_key_string() {
        let mut obj = json!({"key": "value"});
        let expected = obj.clone();
        flatten(&mut obj, "_", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_single_key_int() {
        let mut obj = json!({"key": 1});
        let expected = obj.clone();
        flatten(&mut obj, "_", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_multiple_key_value() {
        let mut obj = json!({"key1": 1, "key2": "value2"});
        let expected = obj.clone();
        flatten(&mut obj, "_", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_nested_single_key_value() {
        let mut obj = json!({"key": "value", "nested_key": {"key":"value"}});
        let expected = json!({"key": "value", "nested_key.key": "value"});
        flatten(&mut obj, ".", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_multiple_key_value() {
        let mut obj = json!({"key": "value", "nested_key": {"key1":"value1", "key2": "value2"}});
        let expected =
            json!({"key": "value", "nested_key.key1": "value1", "nested_key.key2": "value2"});
        flatten(&mut obj, ".", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_key_value_with_array() {
        let mut obj = json!({"key": "value", "nested_key": {"key1":[1,2,3]}});
        let expected = json!({"key": "value", "nested_key.key1": [1,2,3]});
        flatten(&mut obj, ".", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array() {
        let mut obj = json!({"key": [{"a": "value0"}, {"a": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"]});
        flatten(&mut obj, ".", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nulls() {
        let mut obj = json!({"key": [{"a": "value0"}, {"a": "value1", "b": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"], "key.b": [null, "value1"]});
        flatten(&mut obj, ".", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nulls_reversed() {
        let mut obj = json!({"key": [{"a": "value0", "b": "value0"}, {"a": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"], "key.b": ["value0", null]});
        flatten(&mut obj, ".", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nested_obj() {
        let mut obj = json!({"key": [{"a": {"p": 0}, "b": "value0"}, {"b": "value1"}]});
        let expected = json!({"key.a.p": [0, null], "key.b": ["value0", "value1"]});
        flatten(&mut obj, ".", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nested_obj_array() {
        let mut obj = json!({"key": [{"a": [{"p": "value0", "q": "value0"}, {"p": "value1", "q": null}], "b": "value0"}, {"b": "value1"}]});
        let expected = json!({"key.a.p": [["value0", "value1"], null], "key.a.q": [["value0", null], null], "key.b": ["value0", "value1"]});
        flatten(&mut obj, ".", None, None, None).unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_mixed_object() {
        let mut obj = json!({"a": 42, "arr": ["1", {"key": "2"}, {"key": {"nested": "3"}}]});
        assert!(flatten(&mut obj, ".", None, None, None).is_err());
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

    #[test]
    fn acceptable_value_custom_parition_test() {
        let mut value = json!({
            "a": 1,
        });
        assert!(flatten(&mut value, "_", None, None, Some(&"a".to_string())).is_ok());

        let mut value = json!({
            "a": true,
        });
        assert!(flatten(&mut value, "_", None, None, Some(&"a".to_string())).is_ok());

        let mut value = json!({
            "a": "yes",
        });
        assert!(flatten(&mut value, "_", None, None, Some(&"a".to_string())).is_ok());

        let mut value = json!({
            "a": -1,
        });
        assert!(flatten(&mut value, "_", None, None, Some(&"a".to_string())).is_ok());
    }

    #[test]
    fn unacceptable_value_custom_partition_test() {
        let mut value = json!({
            "a": null,
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string())).unwrap_err(),
            JsonFlattenError::FieldEmptyOrNull(_)
        );

        let mut value = json!({
            "a": "",
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string())).unwrap_err(),
            JsonFlattenError::FieldEmptyOrNull(_)
        );

        let mut value = json!({
            "a": {"b": 1},
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string())).unwrap_err(),
            JsonFlattenError::FieldIsObject(_)
        );

        let mut value = json!({
            "a": ["b", "c"],
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string())).unwrap_err(),
            JsonFlattenError::FieldIsArray(_)
        );

        let mut value = json!({
            "a": "b.c",
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string())).unwrap_err(),
            JsonFlattenError::FieldContainsPeriod(_)
        );

        let mut value = json!({
            "a": 1.0,
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string())).unwrap_err(),
            JsonFlattenError::FieldContainsPeriod(_)
        );
    }
    #[test]
    fn unacceptable_levels_of_nested_json() {
        let mut value = json!({"a":{"b":{"c":{"d":{"e":["a","b"]}}}}});
        let context = FlattenContext {
            current_level: 1,
            separator: "_",
            time_partition: None,
            time_partition_limit: None,
            custom_partition: None,
            flatten_depth_limit: 3,
        };
        assert!(generic_flattening(&mut value, &context, None).is_err());
    }

    #[test]
    fn acceptable_levels_of_nested_json() {
        let mut value = json!({"a":{"b":{"e":["a","b"]}}});
        let context = FlattenContext {
            current_level: 1,
            separator: "_",
            time_partition: None,
            time_partition_limit: None,
            custom_partition: None,
            flatten_depth_limit: 3,
        };
        assert!(generic_flattening(&mut value, &context, None).is_ok());
    }
    #[test]
    fn flatten_json() {
        let mut value = json!({"a":{"b":{"e":["a","b"]}}});
        let expected = vec![json!({"a_b_e":"a"}), json!({"a_b_e":"b"})];
        let context = FlattenContext {
            current_level: 1,
            separator: "_",
            time_partition: None,
            time_partition_limit: None,
            custom_partition: None,
            flatten_depth_limit: 3,
        };
        assert_eq!(
            generic_flattening(&mut value, &context, None).unwrap(),
            expected
        );
    }
}
