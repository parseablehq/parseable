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

use crate::handlers::http::otel::proto::logs::v1::LogRecord;
use crate::handlers::http::otel::proto::logs::v1::LogRecordFlags;
use crate::handlers::http::otel::proto::logs::v1::LogsData;
use crate::handlers::http::otel::proto::logs::v1::SeverityNumber;
use bytes::Bytes;
use serde_json::Value;

use super::otel::collect_json_from_values;
use super::otel::flatten_attributes;

//flatten log record and all its attributes to create one record for each log record
pub fn flatten_log_record(log_record: &LogRecord) -> BTreeMap<String, Value> {
    let mut log_record_json: BTreeMap<String, Value> = BTreeMap::new();
    if log_record.time_unix_nano.is_some() {
        log_record_json.insert(
            "time_unix_nano".to_string(),
            Value::String(log_record.time_unix_nano.as_ref().unwrap().to_string()),
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
        let severity_number: i32 = log_record.severity_number.unwrap();
        log_record_json.insert(
            "severity_number".to_string(),
            Value::Number(serde_json::Number::from(severity_number)),
        );
        if log_record.severity_text.is_none() {
            log_record_json.insert(
                "severity_text".to_string(),
                Value::String(SeverityNumber::as_str_name(severity_number).to_string()),
            );
        }
    }
    if log_record.severity_text.is_some() {
        log_record_json.insert(
            "severity_text".to_string(),
            Value::String(log_record.severity_text.as_ref().unwrap().to_string()),
        );
    }

    if log_record.body.is_some() {
        let body = &log_record.body;
        let body_json = collect_json_from_values(body, &"body".to_string());
        for key in body_json.keys() {
            log_record_json.insert(key.to_owned(), body_json[key].to_owned());
        }
    }

    if let Some(attributes) = log_record.attributes.as_ref() {
        let attributes_json = flatten_attributes(attributes);
        for key in attributes_json.keys() {
            log_record_json.insert(key.to_owned(), attributes_json[key].to_owned());
        }
    }

    if log_record.dropped_attributes_count.is_some() {
        log_record_json.insert(
            "log_record_dropped_attributes_count".to_string(),
            Value::Number(serde_json::Number::from(
                log_record.dropped_attributes_count.unwrap(),
            )),
        );
    }

    if log_record.flags.is_some() {
        let flags: u32 = log_record.flags.unwrap();
        log_record_json.insert(
            "flags_number".to_string(),
            Value::Number(serde_json::Number::from(flags)),
        );
        log_record_json.insert(
            "flags_string".to_string(),
            Value::String(LogRecordFlags::as_str_name(flags).to_string()),
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

    log_record_json
}

//flatten otel logs
pub fn flatten_otel_logs(body: &Bytes) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let body_str = std::str::from_utf8(body).unwrap();
    let message: LogsData = serde_json::from_str(body_str).unwrap();

    if let Some(records) = message.resource_logs.as_ref() {
        let mut vec_resource_logs_json: Vec<BTreeMap<String, Value>> = Vec::new();
        for record in records.iter() {
            let mut resource_log_json: BTreeMap<String, Value> = BTreeMap::new();

            if let Some(resource) = record.resource.as_ref() {
                //flatten resource attributes to create multiple key value pairs (headers) for each log record
                if let Some(attributes) = resource.attributes.as_ref() {
                    let attributes_json = flatten_attributes(attributes);
                    for key in attributes_json.keys() {
                        resource_log_json.insert(key.to_owned(), attributes_json[key].to_owned());
                    }
                }

                if resource.dropped_attributes_count.is_some() {
                    resource_log_json.insert(
                        "resource_dropped_attributes_count".to_string(),
                        Value::Number(serde_json::Number::from(
                            resource.dropped_attributes_count.unwrap(),
                        )),
                    );
                }
            }

            if let Some(scope_logs) = record.scope_logs.as_ref() {
                let mut vec_scope_log_json: Vec<BTreeMap<String, Value>> = Vec::new();
                //create flattened record for each scope log
                for scope_log in scope_logs.iter() {
                    let mut scope_log_json: BTreeMap<String, Value> = BTreeMap::new();
                    if scope_log.scope.is_some() {
                        let instrumentation_scope = scope_log.scope.as_ref().unwrap();
                        if instrumentation_scope.name.is_some() {
                            scope_log_json.insert(
                                "scope_name".to_string(),
                                Value::String(
                                    instrumentation_scope.name.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        if instrumentation_scope.version.is_some() {
                            scope_log_json.insert(
                                "scope_version".to_string(),
                                Value::String(
                                    instrumentation_scope.version.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        //flatten instrumentation scope attributes to create multiple key value pairs (headers) for each log record
                        if let Some(attributes) = instrumentation_scope.attributes.as_ref() {
                            let attributes_json = flatten_attributes(attributes);
                            for key in attributes_json.keys() {
                                scope_log_json
                                    .insert(key.to_owned(), attributes_json[key].to_owned());
                            }
                        }

                        if instrumentation_scope.dropped_attributes_count.is_some() {
                            scope_log_json.insert(
                                "scope_dropped_attributes_count".to_string(),
                                Value::Number(serde_json::Number::from(
                                    instrumentation_scope.dropped_attributes_count.unwrap(),
                                )),
                            );
                        }
                    }
                    if scope_log.schema_url.is_some() {
                        scope_log_json.insert(
                            "scope_log_schema_url".to_string(),
                            Value::String(scope_log.schema_url.as_ref().unwrap().to_string()),
                        );
                    }

                    //create flattened record for each log record
                    for log_record in scope_log.log_records.iter() {
                        let log_record_json = flatten_log_record(log_record);

                        for key in log_record_json.keys() {
                            scope_log_json.insert(key.to_owned(), log_record_json[key].to_owned());
                        }
                        vec_scope_log_json.push(scope_log_json.clone());
                    }
                }
                for scope_log_json in vec_scope_log_json.iter() {
                    vec_resource_logs_json.push(scope_log_json.clone());
                }
            }
            if record.schema_url.is_some() {
                resource_log_json.insert(
                    "schema_url".to_string(),
                    Value::String(record.schema_url.as_ref().unwrap().to_string()),
                );
            }

            for resource_logs_json in vec_resource_logs_json.iter_mut() {
                for key in resource_log_json.keys() {
                    resource_logs_json.insert(key.to_owned(), resource_log_json[key].to_owned());
                }
            }
        }

        for resource_logs_json in vec_resource_logs_json.iter() {
            vec_otel_json.push(resource_logs_json.clone());
        }
    }
    vec_otel_json
}
