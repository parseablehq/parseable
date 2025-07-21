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
    AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value as OtelValue,
};
use serde_json::{Map, Value};

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

pub fn flatten_attributes(attributes: &[KeyValue]) -> Map<String, Value> {
    let mut attributes_json: Map<String, Value> = Map::new();
    for attribute in attributes {
        let key = &attribute.key;
        let value = &attribute.value;
        let value_json = collect_json_from_values(value, &key.to_string());
        for (attr_key, attr_val) in &value_json {
            attributes_json.insert(attr_key.clone(), attr_val.clone());
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

pub fn insert_attributes(map: &mut Map<String, Value>, attributes: &[KeyValue]) {
    let attributes_json = flatten_attributes(attributes);
    for (key, value) in attributes_json {
        map.insert(key, value);
    }
}

pub fn convert_epoch_nano_to_timestamp(epoch_ns: i64) -> String {
    let dt = DateTime::from_timestamp_nanos(epoch_ns).naive_utc();
    dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}
