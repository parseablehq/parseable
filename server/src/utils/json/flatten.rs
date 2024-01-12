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

use anyhow::anyhow;
use itertools::Itertools;
use serde_json::map::Map;
use serde_json::value::Value;

pub fn flatten(nested_value: Value, separator: &str) -> Result<Value, anyhow::Error> {
    match nested_value {
        Value::Object(nested_dict) => {
            let mut map = Map::new();
            flatten_object(&mut map, None, nested_dict, separator)?;
            Ok(Value::Object(map))
        }
        Value::Array(mut arr) => {
            for _value in &mut arr {
                let value = std::mem::replace(_value, Value::Null);
                let mut map = Map::new();
                let Value::Object(obj) = value else {
                    return Err(anyhow!("Expected object in array of objects"));
                };
                flatten_object(&mut map, None, obj, separator)?;
                *_value = Value::Object(map);
            }
            Ok(Value::Array(arr))
        }
        _ => Err(anyhow!("Cannot flatten this JSON")),
    }
}

pub fn flatten_with_parent_prefix(
    nested_value: Value,
    prefix: &str,
    separator: &str,
) -> Result<Value, anyhow::Error> {
    let mut map = Map::new();
    if let Value::Object(nested_dict) = nested_value {
        flatten_object(&mut map, Some(prefix), nested_dict, separator)?;
    } else {
        return Err(anyhow!("Must be an object"));
    }
    Ok(Value::Object(map))
}

pub fn flatten_object(
    map: &mut Map<String, Value>,
    parent_key: Option<&str>,
    nested_dict: Map<String, Value>,
    separator: &str,
) -> Result<(), anyhow::Error> {
    for (key, value) in nested_dict.into_iter() {
        let new_key = parent_key.map_or_else(
            || key.clone(),
            |parent_key| format!("{parent_key}{separator}{key}"),
        );
        match value {
            Value::Object(obj) => flatten_object(map, Some(&new_key), obj, separator)?,
            Value::Array(arr) => {
                // if value is object then decompose this list into lists
                if arr.iter().any(|value| value.is_object()) {
                    flatten_array_objects(map, &new_key, arr, separator)?;
                } else {
                    map.insert(new_key, Value::Array(arr));
                }
            }
            x => {
                map.insert(new_key, x);
            }
        }
    }
    Ok(())
}

pub fn flatten_array_objects(
    map: &mut Map<String, Value>,
    parent_key: &str,
    arr: Vec<Value>,
    separator: &str,
) -> Result<(), anyhow::Error> {
    let mut columns: Vec<(String, Vec<Value>)> = Vec::new();
    let mut len = 0;
    for value in arr {
        if let Value::Object(object) = value {
            let mut flattened_object = Map::new();
            flatten_object(&mut flattened_object, None, object, separator)?;
            let mut col_index = 0;
            for (key, value) in flattened_object.into_iter().sorted_by(|a, b| a.0.cmp(&b.0)) {
                loop {
                    if let Some((column_name, column)) = columns.get_mut(col_index) {
                        match (*column_name).cmp(&key) {
                            std::cmp::Ordering::Less => {
                                column.push(Value::Null);
                                col_index += 1;
                                continue;
                            }
                            std::cmp::Ordering::Equal => column.push(value),
                            std::cmp::Ordering::Greater => {
                                let mut list = vec![Value::Null; len];
                                list.push(value);
                                columns.insert(col_index, (key, list));
                            }
                        }
                    } else {
                        let mut list = vec![Value::Null; len];
                        list.push(value);
                        columns.push((key, list));
                    }
                    col_index += 1;
                    break;
                }
            }
            for (_, column) in &mut columns[col_index..] {
                column.push(Value::Null)
            }
        } else if value.is_null() {
            for (_, column) in &mut columns {
                column.push(Value::Null)
            }
        } else {
            return Err(anyhow!(
                "Found non object element while flattening array of object(s)",
            ));
        }
        len += 1;
    }

    for (key, arr) in columns {
        let new_key = format!("{parent_key}{separator}{key}");
        map.insert(new_key, Value::Array(arr));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::utils::json::flatten::flatten_array_objects;

    use super::flatten;
    use serde_json::{json, Map, Value};

    #[test]
    fn flatten_single_key_string() {
        let obj = json!({"key": "value"});
        assert_eq!(obj.clone(), flatten(obj, "_").unwrap());
    }

    #[test]
    fn flatten_single_key_int() {
        let obj = json!({"key": 1});
        assert_eq!(obj.clone(), flatten(obj, "_").unwrap());
    }

    #[test]
    fn flatten_multiple_key_value() {
        let obj = json!({"key1": 1, "key2": "value2"});
        assert_eq!(obj.clone(), flatten(obj, "_").unwrap());
    }

    #[test]
    fn flatten_nested_single_key_value() {
        let obj = json!({"key": "value", "nested_key": {"key":"value"}});
        assert_eq!(
            json!({"key": "value", "nested_key.key": "value"}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn nested_multiple_key_value() {
        let obj = json!({"key": "value", "nested_key": {"key1":"value1", "key2": "value2"}});
        assert_eq!(
            json!({"key": "value", "nested_key.key1": "value1", "nested_key.key2": "value2"}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn nested_key_value_with_array() {
        let obj = json!({"key": "value", "nested_key": {"key1":[1,2,3]}});
        assert_eq!(
            json!({"key": "value", "nested_key.key1": [1,2,3]}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn nested_obj_array() {
        let obj = json!({"key": [{"a": "value0"}, {"a": "value1"}]});
        assert_eq!(
            json!({"key.a": ["value0", "value1"]}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn nested_obj_array_nulls() {
        let obj = json!({"key": [{"a": "value0"}, {"a": "value1", "b": "value1"}]});
        assert_eq!(
            json!({"key.a": ["value0", "value1"], "key.b": [null, "value1"]}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn nested_obj_array_nulls_reversed() {
        let obj = json!({"key": [{"a": "value0", "b": "value0"}, {"a": "value1"}]});
        assert_eq!(
            json!({"key.a": ["value0", "value1"], "key.b": ["value0", null]}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn nested_obj_array_nested_obj() {
        let obj = json!({"key": [{"a": {"p": 0}, "b": "value0"}, {"b": "value1"}]});
        assert_eq!(
            json!({"key.a.p": [0, null], "key.b": ["value0", "value1"]}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn nested_obj_array_nested_obj_array() {
        let obj = json!({"key": [{"a": [{"p": "value0", "q": "value0"}, {"p": "value1", "q": null}], "b": "value0"}, {"b": "value1"}]});
        assert_eq!(
            json!({"key.a.p": [["value0", "value1"], null], "key.a.q": [["value0", null], null], "key.b": ["value0", "value1"]}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn flatten_mixed_object() {
        let obj = json!({"a": 42, "arr": ["1", {"key": "2"}, {"key": {"nested": "3"}}]});
        assert!(flatten(obj, ".").is_err());
    }

    #[test]
    fn flatten_array_nulls_at_start() {
        let Value::Array(arr) = json!([
            null,
            {"p": 2, "q": 2},
            {"q": 3},
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", arr, ".").unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("key.p").unwrap(), &json!([null, 2, null]));
        assert_eq!(map.get("key.q").unwrap(), &json!([null, 2, 3]));
    }

    #[test]
    fn flatten_array_objects_nulls_at_end() {
        let Value::Array(arr) = json!([{"a": 1, "b": 1}, {"a": 2}, null]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", arr, ".").unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("key.a").unwrap(), &json!([1, 2, null]));
        assert_eq!(map.get("key.b").unwrap(), &json!([1, null, null]));
    }

    #[test]
    fn flatten_array_objects_nulls_in_middle() {
        let Value::Array(arr) = json!([{"a": 1, "b": 1}, null, {"a": 3, "c": 3}]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", arr, ".").unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.a").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.b").unwrap(), &json!([1, null, null]));
        assert_eq!(map.get("key.c").unwrap(), &json!([null, null, 3]));
    }

    #[test]
    fn flatten_array_test() {
        let Value::Array(arr) = json!([
            {"p": 1, "q": 1},
            {"r": 2, "q": 2},
            {"p": 3, "r": 3}
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", arr, ".").unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.p").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.q").unwrap(), &json!([1, 2, null]));
        assert_eq!(map.get("key.r").unwrap(), &json!([null, 2, 3]));
    }

    #[test]
    fn flatten_array_nested_test() {
        let Value::Array(arr) = json!([
            {"p": 1, "q": [{"x": 1}, {"x": 2}]},
            {"r": 2, "q": [{"x": 1}]},
            {"p": 3, "r": 3}
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(&mut map, "key", arr, ".").unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.p").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.q.x").unwrap(), &json!([[1, 2], [1], null]));
        assert_eq!(map.get("key.r").unwrap(), &json!([null, 2, 3]));
    }
}
