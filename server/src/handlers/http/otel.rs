use bytes::Bytes;
use serde_json::Value;
mod common;
mod log;
mod resource;
use std::collections::BTreeMap;

pub fn flatten_otel_logs(body: Bytes) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let body_str = std::str::from_utf8(&body).unwrap();

    let message: log::LogsData = serde_json::from_str(body_str).unwrap();

    if let Some(records) = message.resource_logs {
        let mut otel_json: BTreeMap<String, Value> = BTreeMap::new();
        for record in records.iter() {
            for resource in record.resource.iter() {
                let attributes = &resource.attributes;
                for attributes in attributes.iter() {
                    for attribute in attributes {
                        let key = &attribute.key;
                        let value = &attribute.value;
                        for value in value.iter() {
                            if value.str_val.is_some() {
                                otel_json.insert(
                                    key.to_string(),
                                    Value::String(value.str_val.as_ref().unwrap().to_owned()),
                                );
                            }
                            if value.bool_val.is_some() {
                                otel_json.insert(
                                    key.to_string(),
                                    Value::String(value.bool_val.as_ref().unwrap().to_owned()),
                                );
                            }
                            if value.int_val.is_some() {
                                otel_json.insert(
                                    key.to_string(),
                                    Value::String(value.int_val.as_ref().unwrap().to_owned()),
                                );
                            }
                            if value.double_val.is_some() {
                                otel_json.insert(
                                    key.to_string(),
                                    Value::String(value.double_val.as_ref().unwrap().to_owned()),
                                );
                            }
                            // if value.array_val.is_some(){
                            //     attributes_json.insert(key.to_owned(), value.array_val.unwrap());
                            // }
                            // if value.kv_list_val.is_some(){
                            //     attributes_json.insert(key.to_owned(), value.kv_list_val.unwrap());
                            // }
                            if value.bytes_val.is_some() {
                                otel_json.insert(
                                    key.to_string(),
                                    Value::String(value.bytes_val.as_ref().unwrap().to_owned()),
                                );
                            }
                        }
                    }
                }
                if resource.dropped_attributes_count.is_some() {
                    let dropped_attributes_count = &resource.dropped_attributes_count;
                    otel_json.insert(
                        "dropped_attributes_count".to_string(),
                        Value::String(dropped_attributes_count.unwrap().to_string()),
                    );
                }
            }

            for scope_logs in record.scope_logs.iter() {
                for scope_log in scope_logs.iter() {
                    for log_record in scope_log.log_records.iter() {
                        if log_record.time_unix_nano.is_some() {
                            let time_unix_nano = &log_record.time_unix_nano;
                            otel_json.insert(
                                "time_unix_nano".to_string(),
                                Value::String(time_unix_nano.as_ref().unwrap().to_string()),
                            );
                        }
                        if log_record.observed_time_unix_nano.is_some() {
                            let observed_time_unix_nano = &log_record.observed_time_unix_nano;
                            otel_json.insert(
                                "observed_time_unix_nano".to_string(),
                                Value::String(
                                    observed_time_unix_nano.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        if log_record.severity_number.is_some() {
                            let severity_number = &log_record.severity_number;
                            otel_json.insert(
                                "severity_number".to_string(),
                                Value::String(severity_number.as_ref().unwrap().to_string()),
                            );
                        }
                        if log_record.severity_text.is_some() {
                            let severity_text = &log_record.severity_text;
                            otel_json.insert(
                                "severity_text".to_string(),
                                Value::String(severity_text.as_ref().unwrap().to_string()),
                            );
                        }

                        if log_record.name.is_some() {
                            let name = &log_record.name;
                            otel_json.insert(
                                "name".to_string(),
                                Value::String(name.as_ref().unwrap().to_string()),
                            );
                        }
                        if log_record.dropped_attributes_count.is_some() {
                            let dropped_attributes_count = &log_record.dropped_attributes_count;
                            otel_json.insert(
                                "dropped_attributes_count".to_string(),
                                Value::String(dropped_attributes_count.unwrap().to_string()),
                            );
                        }

                        if log_record.flags.is_some() {
                            let flags = &log_record.flags;
                            otel_json.insert(
                                "flags".to_string(),
                                Value::String(flags.unwrap().to_string()),
                            );
                        }

                        if log_record.span_id.is_some() {
                            let span_id = &log_record.span_id;
                            otel_json.insert(
                                "span_id".to_string(),
                                Value::String(span_id.as_ref().unwrap().to_string()),
                            );
                        }

                        if log_record.trace_id.is_some() {
                            let trace_id = &log_record.trace_id;
                            otel_json.insert(
                                "trace_id".to_string(),
                                Value::String(trace_id.as_ref().unwrap().to_string()),
                            );
                        }

                        if log_record.body.is_some() {
                            let body = &log_record.body;
                            for value in body.iter() {
                                if value.str_val.is_some() {
                                    otel_json.insert(
                                        "body".to_string(),
                                        Value::String(value.str_val.as_ref().unwrap().to_owned()),
                                    );
                                }
                                if value.bool_val.is_some() {
                                    otel_json.insert(
                                        "body".to_string(),
                                        Value::String(value.bool_val.as_ref().unwrap().to_owned()),
                                    );
                                }
                                if value.int_val.is_some() {
                                    otel_json.insert(
                                        "body".to_string(),
                                        Value::String(value.int_val.as_ref().unwrap().to_owned()),
                                    );
                                }
                                if value.double_val.is_some() {
                                    otel_json.insert(
                                        "body".to_string(),
                                        Value::String(
                                            value.double_val.as_ref().unwrap().to_owned(),
                                        ),
                                    );
                                }
                                // if value.array_val.is_some(){
                                //     attributes_json.insert(key.to_owned(), value.array_val.unwrap());
                                // }
                                // if value.kv_list_val.is_some(){
                                //     attributes_json.insert(key.to_owned(), value.kv_list_val.unwrap());
                                // }
                                if value.bytes_val.is_some() {
                                    otel_json.insert(
                                        "body".to_string(),
                                        Value::String(value.bytes_val.as_ref().unwrap().to_owned()),
                                    );
                                }
                            }
                        }

                        let attributes = &log_record.attributes;
                        for attributes in attributes.iter() {
                            for attribute in attributes {
                                let key = &attribute.key;
                                let value = &attribute.value;
                                for value in value.iter() {
                                    if value.str_val.is_some() {
                                        otel_json.insert(
                                            key.to_string(),
                                            Value::String(
                                                value.str_val.as_ref().unwrap().to_owned(),
                                            ),
                                        );
                                    }
                                    if value.bool_val.is_some() {
                                        otel_json.insert(
                                            key.to_string(),
                                            Value::String(
                                                value.bool_val.as_ref().unwrap().to_owned(),
                                            ),
                                        );
                                    }
                                    if value.int_val.is_some() {
                                        otel_json.insert(
                                            key.to_string(),
                                            Value::String(
                                                value.int_val.as_ref().unwrap().to_owned(),
                                            ),
                                        );
                                    }
                                    if value.double_val.is_some() {
                                        otel_json.insert(
                                            key.to_string(),
                                            Value::String(
                                                value.double_val.as_ref().unwrap().to_owned(),
                                            ),
                                        );
                                    }
                                    // if value.array_val.is_some(){
                                    //     attributes_json.insert(key.to_owned(), value.array_val.unwrap());
                                    // }
                                    // if value.kv_list_val.is_some(){
                                    //     attributes_json.insert(key.to_owned(), value.kv_list_val.unwrap());
                                    // }
                                    if value.bytes_val.is_some() {
                                        otel_json.insert(
                                            key.to_string(),
                                            Value::String(
                                                value.bytes_val.as_ref().unwrap().to_owned(),
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        vec_otel_json.push(otel_json);
    }

    vec_otel_json
}
