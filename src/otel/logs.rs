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
use super::otel_utils::collect_json_from_values;
use super::otel_utils::convert_epoch_nano_to_timestamp;
use super::otel_utils::insert_attributes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::logs::v1::LogsData;
use opentelemetry_proto::tonic::logs::v1::ScopeLogs;
use opentelemetry_proto::tonic::logs::v1::SeverityNumber;
use serde_json::Map;
use serde_json::Value;

pub const OTEL_LOG_KNOWN_FIELD_LIST: [&str; 16] = [
    "scope_name",
    "scope_version",
    "scope_log_schema_url",
    "scope_dropped_attributes_count",
    "resource_dropped_attributes_count",
    "schema_url",
    "time_unix_nano",
    "observed_time_unix_nano",
    "severity_number",
    "severity_text",
    "body",
    "flags",
    "log_record_dropped_attributes_count",
    "span_id",
    "trace_id",
    "event_name",
];
/// otel log event has severity number
/// there is a mapping of severity number to severity text provided in proto
/// this function fetches the severity text from the severity number
/// and adds it to the flattened json
fn flatten_severity(severity_number: i32) -> Map<String, Value> {
    let mut severity_json: Map<String, Value> = Map::new();
    severity_json.insert(
        "severity_number".to_string(),
        Value::Number(severity_number.into()),
    );
    let severity = SeverityNumber::try_from(severity_number).unwrap();
    severity_json.insert(
        "severity_text".to_string(),
        Value::String(severity.as_str_name().to_string()),
    );
    severity_json
}

/// this function flattens the `LogRecord` object
/// and returns a `Map` of the flattened json
/// this function is called recursively for each log record object in the otel logs
pub fn flatten_log_record(log_record: &LogRecord) -> Map<String, Value> {
    let mut log_record_json: Map<String, Value> = Map::new();
    log_record_json.insert(
        "time_unix_nano".to_string(),
        Value::String(convert_epoch_nano_to_timestamp(
            log_record.time_unix_nano as i64,
        )),
    );
    log_record_json.insert(
        "observed_time_unix_nano".to_string(),
        Value::String(convert_epoch_nano_to_timestamp(
            log_record.observed_time_unix_nano as i64,
        )),
    );

    log_record_json.extend(flatten_severity(log_record.severity_number));

    if log_record.body.is_some() {
        let body = &log_record.body;
        let body_json = collect_json_from_values(body, &"body".to_string());
        for key in body_json.keys() {
            log_record_json.insert(key.to_owned(), body_json[key].to_owned());
        }
    }
    insert_attributes(&mut log_record_json, &log_record.attributes);
    log_record_json.insert(
        "log_record_dropped_attributes_count".to_string(),
        Value::Number(log_record.dropped_attributes_count.into()),
    );

    log_record_json.insert(
        "flags".to_string(),
        Value::Number((log_record.flags).into()),
    );
    log_record_json.insert(
        "span_id".to_string(),
        Value::String(hex::encode(&log_record.span_id)),
    );
    log_record_json.insert(
        "trace_id".to_string(),
        Value::String(hex::encode(&log_record.trace_id)),
    );

    log_record_json
}

/// this function flattens the `ScopeLogs` object
/// and returns a `Vec` of `Map` of the flattened json
fn flatten_scope_log(scope_log: &ScopeLogs) -> Vec<Map<String, Value>> {
    let mut vec_scope_log_json = Vec::new();
    let mut scope_log_json = Map::new();
    if let Some(scope) = &scope_log.scope {
        scope_log_json.insert("scope_name".to_string(), Value::String(scope.name.clone()));
        scope_log_json.insert(
            "scope_version".to_string(),
            Value::String(scope.version.clone()),
        );
        insert_attributes(&mut scope_log_json, &scope.attributes);
        scope_log_json.insert(
            "scope_dropped_attributes_count".to_string(),
            Value::Number(scope.dropped_attributes_count.into()),
        );
    }
    scope_log_json.insert(
        "scope_log_schema_url".to_string(),
        Value::String(scope_log.schema_url.clone()),
    );

    for log_record in &scope_log.log_records {
        let log_record_json = flatten_log_record(log_record);
        let mut combined_json = scope_log_json.clone();
        combined_json.extend(log_record_json);
        vec_scope_log_json.push(combined_json);
    }

    vec_scope_log_json
}

/// Common function to process resource logs and merge resource-level fields
fn process_resource_logs<T>(
    resource_logs: &[T],
    get_resource: fn(&T) -> Option<&opentelemetry_proto::tonic::resource::v1::Resource>,
    get_scope_logs: fn(&T) -> &[ScopeLogs],
    get_schema_url: fn(&T) -> &str,
) -> Vec<Value>
where
    T: std::fmt::Debug,
{
    let mut vec_otel_json = Vec::new();

    for resource_log in resource_logs {
        let mut resource_log_json = Map::new();

        // Process resource attributes if present
        if let Some(resource) = get_resource(resource_log) {
            insert_attributes(&mut resource_log_json, &resource.attributes);
            resource_log_json.insert(
                "resource_dropped_attributes_count".to_string(),
                Value::Number(resource.dropped_attributes_count.into()),
            );
        }

        let mut vec_resource_logs_json = Vec::new();
        let scope_logs = get_scope_logs(resource_log);

        for scope_log in scope_logs {
            vec_resource_logs_json.extend(flatten_scope_log(scope_log));
        }

        resource_log_json.insert(
            "schema_url".to_string(),
            Value::String(get_schema_url(resource_log).to_string()),
        );

        for resource_logs_json in &mut vec_resource_logs_json {
            resource_logs_json.extend(resource_log_json.clone());
            vec_otel_json.push(Value::Object(resource_logs_json.clone()));
        }
    }

    vec_otel_json
}

pub fn flatten_otel_protobuf(message: &ExportLogsServiceRequest) -> Vec<Value> {
    process_resource_logs(
        &message.resource_logs,
        |record| record.resource.as_ref(),
        |record| &record.scope_logs,
        |record| &record.schema_url,
    )
}

/// this function performs the custom flattening of the otel logs
/// and returns a `Vec` of `Value::Object` of the flattened json
pub fn flatten_otel_logs(message: &LogsData) -> Vec<Value> {
    process_resource_logs(
        &message.resource_logs,
        |record| record.resource.as_ref(),
        |record| &record.scope_logs,
        |record| &record.schema_url,
    )
}
