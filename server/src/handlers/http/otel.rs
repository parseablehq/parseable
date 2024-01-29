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

use bytes::Bytes;
use serde_json::Value;
mod common;
mod log;
mod proto;
mod resource;
use std::collections::BTreeMap;

fn collect_json_from_any_value(
    key: &String,
    value: super::otel::proto::common::v1::Value,
) -> BTreeMap<String, Value> {
    let mut value_json: BTreeMap<String, Value> = BTreeMap::new();
    if value.str_val.is_some() {
        value_json.insert(
            key.to_string(),
            Value::String(value.str_val.as_ref().unwrap().to_owned()),
        );
    }
    if value.bool_val.is_some() {
        value_json.insert(
            key.to_string(),
            Value::String(value.bool_val.as_ref().unwrap().to_owned()),
        );
    }
    if value.int_val.is_some() {
        value_json.insert(
            key.to_string(),
            Value::String(value.int_val.as_ref().unwrap().to_owned()),
        );
    }
    if value.double_val.is_some() {
        value_json.insert(
            key.to_string(),
            Value::String(value.double_val.as_ref().unwrap().to_owned()),
        );
    }
    if value.array_val.is_some() {
        let array_val = value.array_val.as_ref().unwrap();
        let values = &array_val.values;

        for value in values {
            let value = &value.value;
            value_json = collect_json_from_any_value(key, value.clone());
        }
    }
    if value.kv_list_val.is_some() {
        let kv_list_val = value.kv_list_val.unwrap();
        for key_value in kv_list_val.values {
            let value = key_value.value;
            value_json = collect_json_from_values(&value, key);
        }
    }
    if value.bytes_val.is_some() {
        value_json.insert(
            key.to_string(),
            Value::String(value.bytes_val.as_ref().unwrap().to_owned()),
        );
    }

    value_json
}

fn collect_json_from_values(
    values: &Option<super::otel::proto::common::v1::Value>,
    key: &String,
) -> BTreeMap<String, Value> {
    let mut value_json: BTreeMap<String, Value> = BTreeMap::new();

    for value in values.iter() {
        value_json = collect_json_from_any_value(key, value.clone());
    }

    value_json
}

pub fn flatten_otel_logs(body: Bytes) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let body_str = std::str::from_utf8(&body).unwrap();

    let message: log::LogsData = serde_json::from_str(body_str).unwrap();

    for records in message.resource_logs.iter() {
        for record in records.iter() {
            let mut otel_json: BTreeMap<String, Value> = BTreeMap::new();
            for resource in record.resource.iter() {
                let attributes = &resource.attributes;
                for attributes in attributes.iter() {
                    for attribute in attributes {
                        let key = &attribute.key;
                        let value = &attribute.value;
                        let value_json =
                            collect_json_from_values(value, &format!("resource_{}", key));
                        for key in value_json.keys() {
                            otel_json.insert(key.to_owned(), value_json[key].to_owned());
                        }
                    }
                }
                if resource.dropped_attributes_count.is_some() {
                    otel_json.insert(
                        "resource_dropped_attributes_count".to_string(),
                        Value::String(resource.dropped_attributes_count.unwrap().to_string()),
                    );
                }
            }

            for scope_logs in record.scope_logs.iter() {
                for scope_log in scope_logs.iter() {
                    for instrumentation_scope in scope_log.scope.iter() {
                        if instrumentation_scope.name.is_some() {
                            otel_json.insert(
                                "instrumentation_scope_name".to_string(),
                                Value::String(
                                    instrumentation_scope.name.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        if instrumentation_scope.version.is_some() {
                            otel_json.insert(
                                "instrumentation_scope_version".to_string(),
                                Value::String(
                                    instrumentation_scope.version.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        let attributes = &instrumentation_scope.attributes;
                        for attributes in attributes.iter() {
                            for attribute in attributes {
                                let key = &attribute.key;
                                let value = &attribute.value;
                                let value_json = collect_json_from_values(
                                    value,
                                    &format!("instrumentation_scope_{}", key),
                                );
                                for key in value_json.keys() {
                                    otel_json.insert(key.to_owned(), value_json[key].to_owned());
                                }
                            }
                        }
                        if instrumentation_scope.dropped_attributes_count.is_some() {
                            otel_json.insert(
                                "instrumentation_scope_dropped_attributes_count".to_string(),
                                Value::String(
                                    instrumentation_scope
                                        .dropped_attributes_count
                                        .unwrap()
                                        .to_string(),
                                ),
                            );
                        }
                    }

                    for log_record in scope_log.log_records.iter() {
                        let mut log_record_json: BTreeMap<String, Value> = BTreeMap::new();
                        if log_record.time_unix_nano.is_some() {
                            log_record_json.insert(
                                "time_unix_nano".to_string(),
                                Value::String(
                                    log_record.time_unix_nano.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        if log_record.observed_time_unix_nano.is_some() {
                            log_record_json.insert(
                                "observed_time_unix_nano".to_string(),
                                Value::String(
                                    log_record
                                        .observed_time_unix_nano
                                        .as_ref()
                                        .unwrap()
                                        .to_string(),
                                ),
                            );
                        }
                        if log_record.severity_number.is_some() {
                            log_record_json.insert(
                                "severity_number".to_string(),
                                Value::String(
                                    log_record.severity_number.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        if log_record.severity_text.is_some() {
                            log_record_json.insert(
                                "severity_text".to_string(),
                                Value::String(
                                    log_record.severity_text.as_ref().unwrap().to_string(),
                                ),
                            );
                        }

                        if log_record.name.is_some() {
                            log_record_json.insert(
                                "log_record_name".to_string(),
                                Value::String(log_record.name.as_ref().unwrap().to_string()),
                            );
                        }

                        if log_record.body.is_some() {
                            let body = &log_record.body;
                            let body_json = collect_json_from_values(body, &"body".to_string());
                            for key in body_json.keys() {
                                log_record_json.insert(key.to_owned(), body_json[key].to_owned());
                            }
                        }

                        for attributes in log_record.attributes.iter() {
                            for attribute in attributes {
                                let key = &attribute.key;
                                let value = &attribute.value;
                                let value_json =
                                    collect_json_from_values(value, &format!("log_record_{}", key));
                                for key in value_json.keys() {
                                    log_record_json
                                        .insert(key.to_owned(), value_json[key].to_owned());
                                }
                            }
                        }

                        if log_record.dropped_attributes_count.is_some() {
                            log_record_json.insert(
                                "log_record_dropped_attributes_count".to_string(),
                                Value::String(
                                    log_record.dropped_attributes_count.unwrap().to_string(),
                                ),
                            );
                        }

                        if log_record.flags.is_some() {
                            log_record_json.insert(
                                "flags".to_string(),
                                Value::String(log_record.flags.unwrap().to_string()),
                            );
                        }

                        if log_record.span_id.is_some() {
                            log_record_json.insert(
                                "span_id".to_string(),
                                Value::String(log_record.span_id.as_ref().unwrap().to_string()),
                            );
                        }

                        if log_record.trace_id.is_some() {
                            log_record_json.insert(
                                "trace_id".to_string(),
                                Value::String(log_record.trace_id.as_ref().unwrap().to_string()),
                            );
                        }
                        for key in log_record_json.keys() {
                            otel_json.insert(key.to_owned(), log_record_json[key].to_owned());
                        }
                        vec_otel_json.push(otel_json.clone());
                    }

                    if scope_log.schema_url.is_some() {
                        otel_json.insert(
                            "scope_log_schema_url".to_string(),
                            Value::String(scope_log.schema_url.as_ref().unwrap().to_string()),
                        );
                    }
                }
            }
            if record.schema_url.is_some() {
                otel_json.insert(
                    "resource_schema_url".to_string(),
                    Value::String(record.schema_url.as_ref().unwrap().to_string()),
                );
            }
        }
    }
    vec_otel_json
}
