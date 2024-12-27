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

use opentelemetry_proto::tonic::common::v1::{any_value::Value as OtelValue, AnyValue, KeyValue};
use serde_json::Value;
use std::collections::BTreeMap;
// Value can be one of types - String, Bool, Int, Double, ArrayValue, AnyValue, KeyValueList, Byte
pub fn collect_json_from_value(key: &String, value: OtelValue) -> BTreeMap<String, Value> {
    let mut value_json: BTreeMap<String, Value> = BTreeMap::new();
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
            let values = &array_val.values;
            for value in values {
                let array_value_json = collect_json_from_anyvalue(key, value.clone());
                for key in array_value_json.keys() {
                    value_json.insert(
                        format!(
                            "{}_{}",
                            key.to_owned(),
                            value_to_string(array_value_json[key].to_owned())
                        ),
                        array_value_json[key].to_owned(),
                    );
                }
            }
        }
        OtelValue::KvlistValue(kv_list_val) => {
            for key_value in kv_list_val.values {
                let value = key_value.value;
                if value.is_some() {
                    let value = value.unwrap();
                    let key_value_json = collect_json_from_anyvalue(key, value.clone());

                    for key in key_value_json.keys() {
                        value_json.insert(
                            format!(
                                "{}_{}_{}",
                                key.to_owned(),
                                key_value.key,
                                value_to_string(key_value_json[key].to_owned())
                            ),
                            key_value_json[key].to_owned(),
                        );
                    }
                }
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

pub fn collect_json_from_anyvalue(key: &String, value: AnyValue) -> BTreeMap<String, Value> {
    collect_json_from_value(key, value.value.unwrap())
}

//traverse through Value by calling function ollect_json_from_any_value
pub fn collect_json_from_values(
    values: &Option<AnyValue>,
    key: &String,
) -> BTreeMap<String, Value> {
    let mut value_json: BTreeMap<String, Value> = BTreeMap::new();

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

pub fn flatten_attributes(attributes: &Vec<KeyValue>) -> BTreeMap<String, Value> {
    let mut attributes_json: BTreeMap<String, Value> = BTreeMap::new();
    for attribute in attributes {
        let key = &attribute.key;
        let value = &attribute.value;
        let value_json = collect_json_from_values(value, &key.to_string());
        for key in value_json.keys() {
            attributes_json.insert(key.to_owned(), value_json[key].to_owned());
        }
    }
    attributes_json
}

pub fn insert_if_some<T: ToString>(
    map: &mut BTreeMap<String, Value>,
    key: &str,
    option: &Option<T>,
) {
    if let Some(value) = option {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
}

pub fn insert_number_if_some(map: &mut BTreeMap<String, Value>, key: &str, option: &Option<f64>) {
    if let Some(value) = option {
        if let Some(number) = serde_json::Number::from_f64(*value) {
            map.insert(key.to_string(), Value::Number(number));
        }
    }
}

pub fn insert_bool_if_some(map: &mut BTreeMap<String, Value>, key: &str, option: &Option<bool>) {
    if let Some(value) = option {
        map.insert(key.to_string(), Value::Bool(*value));
    }
}

pub fn insert_attributes(map: &mut BTreeMap<String, Value>, attributes: &Vec<KeyValue>) {
    let attributes_json = flatten_attributes(attributes);
    for (key, value) in attributes_json {
        map.insert(key, value);
    }
}
