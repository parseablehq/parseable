/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use serde_json::map::Map;
use serde_json::value::Value;

pub fn flatten(nested_value: Value, separator: &str) -> Result<Value, ()> {
    let mut map = Map::new();
    if let Value::Object(nested_dict) = nested_value {
        flatten_object(&mut map, None, nested_dict, separator)?;
    } else {
        return Err(());
    }
    Ok(Value::Object(map))
}

pub fn flatten_with_parent_prefix(
    nested_value: Value,
    prefix: &str,
    separator: &str,
) -> Result<Value, ()> {
    let mut map = Map::new();
    if let Value::Object(nested_dict) = nested_value {
        flatten_object(&mut map, Some(prefix), nested_dict, separator)?;
    } else {
        return Err(());
    }
    Ok(Value::Object(map))
}

pub fn flatten_object(
    map: &mut Map<String, Value>,
    parent_key: Option<&str>,
    nested_dict: Map<String, Value>,
    separator: &str,
) -> Result<(), ()> {
    for (key, value) in nested_dict.into_iter() {
        let new_key = parent_key.map_or_else(
            || key.clone(),
            |parent_key| format!("{}{}{}", parent_key, separator, key),
        );
        match value {
            Value::Object(obj) => flatten_object(map, Some(&new_key), obj, separator)?,
            Value::Array(arr) => {
                let mut new_arr = Vec::with_capacity(arr.len());
                for elem in arr {
                    if let Value::Object(object) = elem {
                        let mut map = Map::new();
                        flatten_object_disallow_array(&mut map, None, object, separator)?;
                        new_arr.push(Value::Object(map))
                    } else {
                        new_arr.push(elem)
                    }
                }
                map.insert(new_key, Value::Array(new_arr));
            }
            x => {
                map.insert(new_key, x);
            }
        }
    }
    Ok(())
}

fn flatten_object_disallow_array(
    map: &mut Map<String, Value>,
    parent_key: Option<&str>,
    object: Map<String, Value>,
    separator: &str,
) -> Result<(), ()> {
    for (key, value) in object.into_iter() {
        let new_key = parent_key.map_or_else(
            || key.clone(),
            |parent_key| format!("{}{}{}", parent_key, separator, key),
        );
        match value {
            Value::Object(obj) => {
                flatten_object_disallow_array(map, Some(&new_key), obj, separator)?;
            }
            Value::Array(_) => return Err(()),
            x => {
                map.insert(new_key, x);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::flatten;
    use serde_json::json;

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
    fn nested_array() {
        let obj = json!({"key": ["value1", "value2"]});
        assert_eq!(obj.clone(), flatten(obj, ".").unwrap());
    }

    #[test]
    fn nested_obj_array() {
        let obj = json!({"key": ["value1", {"key": "value2"}]});
        assert_eq!(obj.clone(), flatten(obj, ".").unwrap());
    }

    #[test]
    fn flatten_mixed_object() {
        let obj = json!({"a": 42, "arr": ["1", {"key": "2"}, {"key": {"nested": "3"}}]});
        assert_eq!(
            json!({"a": 42, "arr": ["1", {"key": "2"}, {"key.nested": "3"}]}),
            flatten(obj, ".").unwrap()
        );
    }

    #[test]
    fn err_flatten_mixed_object_with_array_within_object() {
        let obj = json!({"a": 42, "arr": ["1", {"key": [1,2,4]}]});
        assert!(flatten(obj, ".").is_err());
    }
}
