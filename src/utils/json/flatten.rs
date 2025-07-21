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

use crate::parseable::PARSEABLE;

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
}

// Recursively flattens JSON objects and arrays, e.g. with the separator `.`, starting from the TOP
// `{"key": "value", "nested_key": {"key":"value"}}` becomes `{"key": "value", "nested_key.key": "value"}`
pub fn flatten(
    nested_value: &mut Value,
    separator: &str,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
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
                // Recursively flatten each element, ONLY in the TOP array
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
                return Err(JsonFlattenError::FieldEmptyOrNull(trimmed_field.to_owned()));
            }
            Value::String(s) if s.is_empty() => {
                return Err(JsonFlattenError::FieldEmptyOrNull(trimmed_field.to_owned()));
            }
            Value::Object(_) => {
                return Err(JsonFlattenError::FieldIsObject(trimmed_field.to_owned()));
            }
            Value::Array(_) => {
                return Err(JsonFlattenError::FieldIsArray(trimmed_field.to_owned()));
            }
            Value::String(s) if s.contains('.') => {
                return Err(JsonFlattenError::FieldContainsPeriod(
                    trimmed_field.to_owned(),
                ));
            }
            Value::Number(n) if n.is_f64() => {
                return Err(JsonFlattenError::FieldContainsPeriod(
                    trimmed_field.to_owned(),
                ));
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
pub fn generic_flattening(value: &Value) -> Result<Vec<Value>, JsonFlattenError> {
    match value {
        Value::Array(arr) => Ok(arr
            .iter()
            .flat_map(|flatten_item| generic_flattening(flatten_item).unwrap_or_default())
            .collect()),
        Value::Object(map) => {
            let results = map
                .iter()
                .fold(vec![Map::new()], |results, (key, val)| match val {
                    Value::Array(arr) => {
                        if arr.is_empty() {
                            // Insert empty array for this key in all current results
                            results
                                .into_iter()
                                .map(|mut result| {
                                    result.insert(key.clone(), Value::Array(vec![]));
                                    result
                                })
                                .collect()
                        } else {
                            arr.iter()
                                .flat_map(|flatten_item| {
                                    generic_flattening(flatten_item).unwrap_or_default()
                                })
                                .flat_map(|flattened_item| {
                                    results.iter().map(move |result| {
                                        let mut new_obj = result.clone();
                                        new_obj.insert(key.clone(), flattened_item.clone());
                                        new_obj
                                    })
                                })
                                .collect()
                        }
                    }
                    Value::Object(_) => generic_flattening(val)
                        .unwrap_or_default()
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
                });

            Ok(results.into_iter().map(Value::Object).collect())
        }
        _ => Ok(vec![value.clone()]),
    }
}

/// recursively checks the level of nesting for the serde Value
/// if Value has more than configured `P_MAX_FLATTEN_LEVEL` levels of hierarchy, returns true
/// example - if `P_MAX_FLATTEN_LEVEL` is 4, and the JSON is
/// 1. `{"a":{"b":{"c":{"d":{"e":["a","b"]}}}}}` ~> returns true
/// 2. `{"a": [{"b": 1}, {"c": 2}], "d": {"e": 4}}` ~> returns false
pub fn has_more_than_max_allowed_levels(value: &Value, current_level: usize) -> bool {
    if current_level > PARSEABLE.options.event_flatten_level {
        return true;
    }
    match value {
        Value::Array(arr) => arr
            .iter()
            .any(|item| has_more_than_max_allowed_levels(item, current_level)),
        Value::Object(map) => map
            .values()
            .any(|val| has_more_than_max_allowed_levels(val, current_level + 1)),
        _ => false,
    }
}

// Converts a Vector of values into a `Value::Array`, as long as all of them are objects
pub fn convert_to_array(flattened: Vec<Value>) -> Result<Value, JsonFlattenError> {
    if flattened.iter().any(|item| !item.is_object()) {
        return Err(JsonFlattenError::ExpectedObjectInArray);
    }

    Ok(Value::Array(flattened))
}

#[cfg(test)]
mod tests {
    use crate::utils::json::flatten::{flatten_array_objects, generic_flattening};

    use super::{JsonFlattenError, flatten};
    use serde_json::{Map, Value, json};

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

    #[test]
    fn acceptable_value_custom_parition_test() {
        let mut value = json!({
            "a": 1,
        });
        assert!(flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).is_ok());

        let mut value = json!({
            "a": true,
        });
        assert!(flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).is_ok());

        let mut value = json!({
            "a": "yes",
        });
        assert!(flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).is_ok());

        let mut value = json!({
            "a": -1,
        });
        assert!(flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).is_ok());
    }

    #[test]
    fn unacceptable_value_custom_partition_test() {
        let mut value = json!({
            "a": null,
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).unwrap_err(),
            JsonFlattenError::FieldEmptyOrNull(_)
        );

        let mut value = json!({
            "a": "",
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).unwrap_err(),
            JsonFlattenError::FieldEmptyOrNull(_)
        );

        let mut value = json!({
            "a": {"b": 1},
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).unwrap_err(),
            JsonFlattenError::FieldIsObject(_)
        );

        let mut value = json!({
            "a": ["b", "c"],
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).unwrap_err(),
            JsonFlattenError::FieldIsArray(_)
        );

        let mut value = json!({
            "a": "b.c",
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).unwrap_err(),
            JsonFlattenError::FieldContainsPeriod(_)
        );

        let mut value = json!({
            "a": 1.0,
        });
        matches!(
            flatten(&mut value, "_", None, None, Some(&"a".to_string()), true).unwrap_err(),
            JsonFlattenError::FieldContainsPeriod(_)
        );
    }

    #[test]
    fn flatten_json() {
        let value = json!({"a":{"b":{"e":["a","b"]}}});
        let expected = vec![json!({"a":{"b":{"e":"a"}}}), json!({"a":{"b":{"e":"b"}}})];
        assert_eq!(generic_flattening(&value).unwrap(), expected);
    }
}
