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

use chrono::DateTime;
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as OtelValue, AnyValue, ArrayValue, KeyValue, KeyValueList,
};
use serde_json::{Map, Value};

/// Prefixes of attribute keys that should be preserved as individual fields in flattened output.
/// Other attributes will be collected in a separate JSON object under `other_attributes`.
const KNOWN_ATTRIBUTES_PREFIX: [&str; 6] = ["http", "url", "service", "os", "host", "telemetry"];
pub const OTHER_ATTRIBUTES_KEY: &str = "other_attributes";
// Value can be one of types - String, Bool, Int, Double, ArrayValue, AnyValue, KeyValueList, Byte
pub fn collect_json_from_value(key: &String, value: OtelValue) -> Map<String, Value> {
    let mut value_json: Map<String, Value> = Map::new();
    match value {
        OtelValue::StringValue(str_val) => {
            value_json.insert(key.to_string(), Value::String(str_val));
        }
        OtelValue::BoolValue(bool_val) => {
            value_json.insert(key.to_string(), Value::Bool(bool_val));
        }
        OtelValue::IntValue(int_val) => {
            value_json.insert(key.to_string(), Value::String(int_val.to_string()));
        }
        OtelValue::DoubleValue(double_val) => {
            if let Some(number) = serde_json::Number::from_f64(double_val) {
                value_json.insert(key.to_string(), Value::Number(number));
            }
        }
        OtelValue::ArrayValue(array_val) => {
            let json_array_value = collect_json_from_array_value(&array_val);
            // Convert the array to a JSON string
            let json_array_string = match serde_json::to_string(&json_array_value) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("failed to serialise array value: {e}");
                    String::default()
                }
            };
            // Insert the array into the result map
            value_json.insert(key.to_string(), Value::String(json_array_string));
        }
        OtelValue::KvlistValue(kv_list_val) => {
            // Create a JSON object to store the key-value list
            let kv_object = collect_json_from_key_value_list(kv_list_val);
            for (key, value) in kv_object.iter() {
                value_json.insert(key.clone(), value.clone());
            }
        }
        OtelValue::BytesValue(bytes_val) => {
            value_json.insert(
                key.to_string(),
                Value::String(String::from_utf8_lossy(&bytes_val).to_string()),
            );
        }
    }

    value_json
}

/// Recursively converts an ArrayValue into a JSON Value
/// This handles nested array values and key-value lists by recursively
/// converting them to JSON
fn collect_json_from_array_value(array_value: &ArrayValue) -> Value {
    let mut json_array = Vec::new();
    for value in &array_value.values {
        if let Some(val) = &value.value {
            match val {
                OtelValue::StringValue(s) => json_array.push(Value::String(s.clone())),
                OtelValue::BoolValue(b) => json_array.push(Value::Bool(*b)),
                OtelValue::IntValue(i) => {
                    json_array.push(Value::Number(serde_json::Number::from(*i)))
                }
                OtelValue::DoubleValue(d) => {
                    if let Some(n) = serde_json::Number::from_f64(*d) {
                        json_array.push(Value::Number(n));
                    }
                }
                OtelValue::BytesValue(b) => {
                    json_array.push(Value::String(String::from_utf8_lossy(b).to_string()));
                }
                OtelValue::ArrayValue(arr) => {
                    // Recursively collect JSON from nested array values
                    let nested_json = collect_json_from_array_value(arr);
                    json_array.push(nested_json);
                }
                OtelValue::KvlistValue(kv_list) => {
                    // Recursively collect JSON from nested key-value lists
                    let nested_json = collect_json_from_key_value_list(kv_list.clone());
                    json_array.push(Value::Object(nested_json));
                }
            }
        }
    }
    Value::Array(json_array)
}

/// Recursively converts an OpenTelemetry KeyValueList into a JSON Map
/// The function iterates through the key-value pairs in the list
/// and collects their JSON representations into a single Map
fn collect_json_from_key_value_list(key_value_list: KeyValueList) -> Map<String, Value> {
    let mut kv_list_json: Map<String, Value> = Map::new();
    for key_value in key_value_list.values {
        if let Some(val) = key_value.value {
            if let Some(val) = val.value {
                let json_value = collect_json_from_value(&key_value.key, val);
                kv_list_json.extend(json_value);
            } else {
                tracing::warn!("Key '{}' has no value in key-value list", key_value.key);
            }
        }
    }
    kv_list_json
}

pub fn collect_json_from_anyvalue(key: &String, value: AnyValue) -> Map<String, Value> {
    collect_json_from_value(key, value.value.unwrap())
}

//traverse through Value by calling function ollect_json_from_any_value
pub fn collect_json_from_values(values: &Option<AnyValue>, key: &String) -> Map<String, Value> {
    let mut value_json: Map<String, Value> = Map::new();

    for value in values.iter() {
        value_json = collect_json_from_anyvalue(key, value.clone());
    }

    value_json
}

pub fn value_to_string(value: serde_json::Value) -> String {
    match value.clone() {
        e @ Value::Number(_) | e @ Value::Bool(_) => e.to_string(),
        Value::String(s) => s,
        _ => "".to_string(),
    }
}

pub fn flatten_attributes(
    attributes: &Vec<KeyValue>,
    other_attributes_json: &mut Map<String, Value>,
) -> Map<String, Value> {
    let mut attributes_json: Map<String, Value> = Map::new();
    for attribute in attributes {
        let key = &attribute.key;
        let value = &attribute.value;
        let value_json = collect_json_from_values(value, &key.to_string());
        for (attr_key, attr_val) in &value_json {
            if KNOWN_ATTRIBUTES_PREFIX
                .iter()
                .any(|prefix| attr_key.starts_with(prefix))
            {
                attributes_json.insert(attr_key.clone(), attr_val.clone());
            } else {
                other_attributes_json.insert(attr_key.clone(), attr_val.clone());
            }
        }
    }
    attributes_json
}

pub fn insert_if_some<T: ToString>(map: &mut Map<String, Value>, key: &str, option: &Option<T>) {
    if let Some(value) = option {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
}

pub fn insert_number_if_some(map: &mut Map<String, Value>, key: &str, option: &Option<f64>) {
    if let Some(value) = option {
        if let Some(number) = serde_json::Number::from_f64(*value) {
            map.insert(key.to_string(), Value::Number(number));
        }
    }
}

pub fn insert_bool_if_some(map: &mut Map<String, Value>, key: &str, option: &Option<bool>) {
    if let Some(value) = option {
        map.insert(key.to_string(), Value::Bool(*value));
    }
}

pub fn insert_attributes(
    map: &mut Map<String, Value>,
    attributes: &Vec<KeyValue>,
    other_attributes_json: &mut Map<String, Value>,
) {
    let attributes_json = flatten_attributes(attributes, other_attributes_json);
    for (key, value) in attributes_json {
        map.insert(key, value);
    }
}

pub fn convert_epoch_nano_to_timestamp(epoch_ns: i64) -> String {
    let dt = DateTime::from_timestamp_nanos(epoch_ns).naive_utc();
    dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

/// fetch `other_attributes` from array of JSON objects
/// merge them with the provided attributes
/// and return the merged array of JSON object
pub fn merge_attributes_in_json(
    attributes: Map<String, Value>,
    vec_json: &mut Vec<Map<String, Value>>,
) {
    if attributes.is_empty() {
        return;
    }

    for json in vec_json {
        let merged_attributes = match json.get(OTHER_ATTRIBUTES_KEY) {
            Some(Value::String(attrs_str)) => {
                merge_with_existing_attributes(&attributes, attrs_str)
            }
            Some(Value::Object(existing_attrs)) => {
                merge_with_existing_object(&attributes, existing_attrs)
            }
            _ => serialize_attributes(&attributes),
        };

        if let Some(merged_str) = merged_attributes {
            json.insert(OTHER_ATTRIBUTES_KEY.to_string(), Value::String(merged_str));
        }
    }
}

/// Merge attributes with an existing JSON string of attributes
fn merge_with_existing_attributes(
    attributes: &Map<String, Value>,
    attrs_str: &str,
) -> Option<String> {
    match serde_json::from_str::<Map<String, Value>>(attrs_str) {
        Ok(mut existing_attrs) => {
            for (key, value) in attributes {
                existing_attrs.insert(key.clone(), value.clone());
            }
            serde_json::to_string(&existing_attrs).ok()
        }
        Err(e) => {
            tracing::warn!("failed to deserialize existing attributes: {e}");
            None
        }
    }
}

/// Merge attributes with an existing JSON object of attributes
fn merge_with_existing_object(
    attributes: &Map<String, Value>,
    existing_attrs: &Map<String, Value>,
) -> Option<String> {
    let mut merged_attrs = existing_attrs.clone();
    for (key, value) in attributes {
        merged_attrs.insert(key.clone(), value.clone());
    }
    serde_json::to_string(&merged_attrs).ok()
}

/// Serialize attributes into a JSON string
fn serialize_attributes(attributes: &Map<String, Value>) -> Option<String> {
    serde_json::to_string(attributes).ok()
}

/// fetch `other_attributes` from array of JSON objects
/// and merge them into a single map
/// and return the merged map
pub fn fetch_attributes_from_json(json_arr: &Vec<Map<String, Value>>) -> Map<String, Value> {
    let mut merged_attributes = Map::new();

    for json in json_arr {
        if let Some(Value::String(attrs_str)) = json.get(OTHER_ATTRIBUTES_KEY) {
            if let Ok(attrs) = serde_json::from_str::<Map<String, Value>>(attrs_str) {
                for (key, value) in attrs {
                    merged_attributes.insert(key, value);
                }
            }
        }
    }
    merged_attributes
}

/// convert attributes map to a string
/// and return the string
/// if serialisation fails, return an empty string
pub fn fetch_attributes_string(attributes: &Map<String, Value>) -> String {
    match serde_json::to_string(attributes) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("failed to serialise OTEL other_attributes: {e}");
            String::default()
        }
    }
}

/// add `other_attributes` to the JSON object
/// if `other_attributes` is not empty
/// and return the JSON object
pub fn add_other_attributes_if_not_empty(
    json: &mut Map<String, Value>,
    other_attributes: &Map<String, Value>,
) {
    if !other_attributes.is_empty() {
        let attrs_str = fetch_attributes_string(other_attributes);
        json.insert(OTHER_ATTRIBUTES_KEY.to_string(), Value::String(attrs_str));
    }
}
