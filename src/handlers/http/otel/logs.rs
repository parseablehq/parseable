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

use super::collect_json_from_values;
use super::insert_attributes;
use super::insert_if_some;
use super::insert_number_if_some;
use super::proto::logs::v1::ScopeLogs;

/// otel log event has severity number
/// there is a mapping of severity number to severity text provided in proto
/// this function fetches the severity text from the severity number
/// and adds it to the flattened json
fn flatten_severity(severity_number: &Option<i32>) -> BTreeMap<String, Value> {
    let mut severity_json: BTreeMap<String, Value> = BTreeMap::new();
    insert_number_if_some(
        &mut severity_json,
        "severity_number",
        &severity_number.map(|f| f as f64),
    );
    if let Some(severity_number) = severity_number {
        insert_if_some(
            &mut severity_json,
            "severity_text",
            &Some(SeverityNumber::as_str_name(*severity_number)),
        );
    }
    severity_json
}

/// otel log event has flags
/// there is a mapping of flags to flags text provided in proto
/// this function fetches the flags text from the flags
/// and adds it to the flattened json
fn flatten_flags(flags: &Option<u32>) -> BTreeMap<String, Value> {
    let mut flags_json: BTreeMap<String, Value> = BTreeMap::new();
    insert_number_if_some(&mut flags_json, "flags_number", &flags.map(|f| f as f64));
    if let Some(flags) = flags {
        insert_if_some(
            &mut flags_json,
            "flags_string",
            &Some(LogRecordFlags::as_str_name(*flags)),
        );
    }
    flags_json
}

/// this function flattens the `LogRecord` object
/// and returns a `BTreeMap` of the flattened json
/// this function is called recursively for each log record object in the otel logs
pub fn flatten_log_record(log_record: &LogRecord) -> BTreeMap<String, Value> {
    let mut log_record_json: BTreeMap<String, Value> = BTreeMap::new();
    insert_if_some(
        &mut log_record_json,
        "time_unix_nano",
        &log_record.time_unix_nano,
    );
    insert_if_some(
        &mut log_record_json,
        "observed_time_unix_nano",
        &log_record.observed_time_unix_nano,
    );

    log_record_json.extend(flatten_severity(&log_record.severity_number));

    if log_record.body.is_some() {
        let body = &log_record.body;
        let body_json = collect_json_from_values(body, &"body".to_string());
        for key in body_json.keys() {
            log_record_json.insert(key.to_owned(), body_json[key].to_owned());
        }
    }
    insert_attributes(&mut log_record_json, &log_record.attributes);
    insert_number_if_some(
        &mut log_record_json,
        "log_record_dropped_attributes_count",
        &log_record.dropped_attributes_count.map(|f| f as f64),
    );

    log_record_json.extend(flatten_flags(&log_record.flags));
    insert_if_some(&mut log_record_json, "span_id", &log_record.span_id);
    insert_if_some(&mut log_record_json, "trace_id", &log_record.trace_id);

    log_record_json
}

/// this function flattens the `ScopeLogs` object
/// and returns a `Vec` of `BTreeMap` of the flattened json
fn flatten_scope_log(scope_log: &ScopeLogs) -> Vec<BTreeMap<String, Value>> {
    let mut vec_scope_log_json = Vec::new();
    let mut scope_log_json = BTreeMap::new();

    if let Some(scope) = &scope_log.scope {
        insert_if_some(&mut scope_log_json, "scope_name", &scope.name);
        insert_if_some(&mut scope_log_json, "scope_version", &scope.version);
        insert_attributes(&mut scope_log_json, &scope.attributes);
        insert_number_if_some(
            &mut scope_log_json,
            "scope_dropped_attributes_count",
            &scope.dropped_attributes_count.map(|f| f as f64),
        );
    }

    if let Some(schema_url) = &scope_log.schema_url {
        scope_log_json.insert(
            "scope_log_schema_url".to_string(),
            Value::String(schema_url.clone()),
        );
    }

    for log_record in &scope_log.log_records {
        let log_record_json = flatten_log_record(log_record);
        let mut combined_json = scope_log_json.clone();
        combined_json.extend(log_record_json);
        vec_scope_log_json.push(combined_json);
    }

    vec_scope_log_json
}

/// this function performs the custom flattening of the otel logs
/// and returns a `Vec` of `BTreeMap` of the flattened json
pub fn flatten_otel_logs(body: &Bytes) -> Vec<BTreeMap<String, Value>> {
    let body_str = std::str::from_utf8(body).unwrap();
    let message: LogsData = serde_json::from_str(body_str).unwrap();
    let mut vec_otel_json = Vec::new();

    if let Some(records) = &message.resource_logs {
        for record in records {
            let mut resource_log_json = BTreeMap::new();

            if let Some(resource) = &record.resource {
                insert_attributes(&mut resource_log_json, &resource.attributes);
                insert_number_if_some(
                    &mut resource_log_json,
                    "resource_dropped_attributes_count",
                    &resource.dropped_attributes_count.map(|f| f as f64),
                );
            }

            let mut vec_resource_logs_json = Vec::new();
            if let Some(scope_logs) = &record.scope_logs {
                for scope_log in scope_logs {
                    vec_resource_logs_json.extend(flatten_scope_log(scope_log));
                }
            }

            insert_if_some(&mut resource_log_json, "schema_url", &record.schema_url);

            for resource_logs_json in &mut vec_resource_logs_json {
                resource_logs_json.extend(resource_log_json.clone());
            }

            vec_otel_json.extend(vec_resource_logs_json);
        }
    }

    vec_otel_json
}
