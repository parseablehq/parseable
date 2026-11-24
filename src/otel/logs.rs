/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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
use super::otel_utils::attribute_capacity;
use super::otel_utils::collect_json_from_values;
use super::otel_utils::convert_epoch_nano_to_timestamp;
use super::otel_utils::insert_attributes;
use super::sink::RowSink;
use crate::metrics::increment_logs_collected_by_date;
use crate::utils::json::flatten::{
    FlattenBudget, FlattenCost, JsonFlattenError, expand_generic_flattening, flatten_pipeline_cost,
    string_json_len,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::logs::v1::LogsData;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::logs::v1::SeverityNumber;
use opentelemetry_proto::tonic::resource::v1::Resource;
use serde_json::Map;
use serde_json::Value;

pub const OTEL_LOG_KNOWN_FIELD_LIST: [&str; 17] = [
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
    "p_log_category",
];
/// Derive whichever OTel severity field is absent from the other.
fn normalize_severity(severity_number: i32, severity_text: &str) -> (i32, String) {
    let severity_text_is_unspecified =
        severity_text.trim().is_empty() || severity_text.trim().eq_ignore_ascii_case("UNSPECIFIED");
    let severity_number = if severity_number == 0 {
        severity_number_from_text(severity_text).unwrap_or(severity_number)
    } else {
        severity_number
    };

    let severity_text = if severity_text_is_unspecified {
        SeverityNumber::try_from(severity_number)
            .ok()
            .map(|severity| {
                let name = severity.as_str_name();
                name.strip_prefix("SEVERITY_NUMBER_")
                    .unwrap_or(name)
                    .to_string()
            })
            .unwrap_or_else(|| "UNSPECIFIED".to_string())
    } else {
        severity_text.to_string()
    };

    (severity_number, severity_text)
}

/// Convert standard OTel severity names (and the common WARNING alias) to their
/// lowest severity number in the corresponding range.
fn severity_number_from_text(severity_text: &str) -> Option<i32> {
    let severity_text = severity_text.trim().to_ascii_uppercase();
    let severity_text = severity_text
        .strip_prefix("SEVERITY_NUMBER_")
        .unwrap_or(&severity_text);

    match severity_text {
        "TRACE" => Some(1),
        "TRACE2" => Some(2),
        "TRACE3" => Some(3),
        "TRACE4" => Some(4),
        "DEBUG" => Some(5),
        "DEBUG2" => Some(6),
        "DEBUG3" => Some(7),
        "DEBUG4" => Some(8),
        "INFO" => Some(9),
        "INFO2" => Some(10),
        "INFO3" => Some(11),
        "INFO4" => Some(12),
        "WARN" | "WARNING" => Some(13),
        "WARN2" => Some(14),
        "WARN3" => Some(15),
        "WARN4" => Some(16),
        "ERROR" => Some(17),
        "ERROR2" => Some(18),
        "ERROR3" => Some(19),
        "ERROR4" => Some(20),
        "FATAL" => Some(21),
        "FATAL2" => Some(22),
        "FATAL3" => Some(23),
        "FATAL4" => Some(24),
        _ => None,
    }
}

/// Add the normalized severity fields to the flattened JSON.
fn flatten_severity(severity_number: i32, severity_text: &str) -> Map<String, Value> {
    let mut severity_json: Map<String, Value> = Map::new();
    severity_json.insert(
        "severity_number".to_string(),
        Value::Number(severity_number.into()),
    );
    severity_json.insert(
        "severity_text".to_string(),
        Value::String(severity_text.to_string()),
    );
    severity_json
}

/// Maps OTel severity_number (0–24) to a log category.
/// See https://opentelemetry.io/docs/specs/otel/logs/data-model/#severity-fields
fn category_from_severity(severity_number: i32) -> Option<&'static str> {
    match severity_number {
        1..=4 => Some("TRACE"),
        5..=8 => Some("DEBUG"),
        9..=12 => Some("INFO"),
        13..=16 => Some("WARN"),
        17..=20 => Some("ERROR"),
        21..=24 => Some("FATAL"),
        _ => None, // 0 (Unspecified) or out of range
    }
}

/// Fallback: case-insensitive partial match on the body string.
/// Categories are ordered from most severe to least severe so the highest severity are checked first.
const LOG_CATEGORIES: &[(&str, &str)] = &[
    ("critical", "FATAL"),
    ("fatal", "FATAL"),
    ("error", "ERROR"),
    ("warning", "WARN"),
    ("warn", "WARN"),
    ("info", "INFO"),
    ("debug", "DEBUG"),
    ("trace", "TRACE"),
    ("verbose", "TRACE"),
];

fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn category_from_body(body_str: &str) -> &'static str {
    LOG_CATEGORIES
        .iter()
        .find(|(pattern, _)| contains_ignore_ascii_case(body_str, pattern))
        .map_or("UNSPECIFIED", |(_, label)| *label)
}

/// this function flattens the `LogRecord` object
/// and returns a `Map` of the flattened json
/// this function is called recursively for each log record object in the otel logs
/// The generic flattening of all JSON object bodies of the record is one event for `budget`
pub fn flatten_log_record(
    log_record: &LogRecord,
    budget: &mut FlattenBudget,
) -> Result<Map<String, Value>, JsonFlattenError> {
    // Fixed fields plus the record attributes; body fields can add more, but
    // one growth is cheaper than the four an empty map needs for these
    let mut log_record_json: Map<String, Value> =
        Map::with_capacity(16 + attribute_capacity(&log_record.attributes));
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

    let (severity_number, severity_text) =
        normalize_severity(log_record.severity_number, &log_record.severity_text);
    log_record_json.extend(flatten_severity(severity_number, &severity_text));

    // Primary: derive category from the normalized severity_number
    let mut log_category = category_from_severity(severity_number);

    if log_record.body.is_some() {
        let body = &log_record.body;
        let body_json = collect_json_from_values(body, "body");
        // Values that are strings which can be parsed as JSON objects get their fields extracted
        let parsed_bodies: Vec<Option<Value>> = body_json
            .values()
            .map(|value| match value {
                Value::String(s) => serde_json::from_str::<Value>(s)
                    .ok()
                    .filter(Value::is_object),
                _ => None,
            })
            .collect();
        // The rows of every parsed body are merged into this record, but they are all built
        // first, so charge all of them before building any
        let limits = budget.limits();
        let mut body_cost = FlattenCost::default();
        for (key, parsed) in body_json.keys().zip(&parsed_bodies) {
            let Some(parsed @ Value::Object(fields)) = parsed else {
                continue;
            };
            let cost = flatten_pipeline_cost(parsed, limits)?;
            // Every row holds each root field of the body once, and each of them gets a
            // copy of the body key, as `{key}_{field}`. Structurally flattening the record
            // later (in `push_logs`) copies the key into the name of every field below a
            // root field too, as `{key}_{field}_{name}`. Charge the larger of the two.
            let prefix = string_json_len(key).map(|quoted| quoted - 2 + "_".len());
            let generic = prefix
                .and_then(|prefix| prefix.checked_mul(fields.len()))
                .and_then(|bytes| bytes.checked_mul(cost.rows))
                .and_then(|bytes| bytes.checked_add(cost.generic_bytes));
            let structural = prefix
                .and_then(|prefix| prefix.checked_mul(cost.structural_fields))
                .and_then(|bytes| bytes.checked_add(cost.structural_bytes));
            let bytes = generic
                .zip(structural)
                .map(|(generic, structural)| generic.max(structural))
                .ok_or(JsonFlattenError::BytesPerRequestLimitExceeded(
                    limits.max_bytes_per_request,
                ))?;
            body_cost = body_cost.add(
                FlattenCost {
                    rows: cost.rows,
                    bytes,
                },
                limits,
            )?;
        }
        budget.charge_event(body_cost)?;

        for ((key, value), parsed) in body_json.iter().zip(&parsed_bodies) {
            // Always insert the original body field as is
            log_record_json.insert(key.clone(), value.clone());

            if let Some(parsed) = parsed {
                for flattened_value in expand_generic_flattening(parsed)? {
                    if let Value::Object(flattened_obj) = flattened_value {
                        for (inner_key, inner_value) in flattened_obj {
                            let prefixed_key = format!("{key}_{inner_key}");
                            log_record_json.insert(prefixed_key, inner_value);
                        }
                    }
                }
            }
        }

        // Fallback: scan body only when severity_number is unset
        if log_category.is_none() {
            let body_str: String = body_json
                .values()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect::<Vec<_>>()
                .join(" ");
            log_category = Some(category_from_body(&body_str));
        }
    }

    insert_attributes(&mut log_record_json, &log_record.attributes);

    // Insert after attributes so a client-sent "p_log_category" cannot override
    log_record_json.insert(
        "p_log_category".to_string(),
        Value::String(log_category.unwrap_or("UNSPECIFIED").to_string()),
    );
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

    Ok(log_record_json)
}

/// Fields every row of one `ScopeLogs` gets, before the record's own fields
pub fn scope_log_fields(
    scope: Option<&InstrumentationScope>,
    schema_url: &str,
) -> Map<String, Value> {
    let mut scope_log_json =
        Map::with_capacity(5 + scope.map_or(0, |scope| attribute_capacity(&scope.attributes)));
    if let Some(scope) = scope {
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
        Value::String(schema_url.to_string()),
    );
    scope_log_json
}

/// Fields every row of one `ResourceLogs` gets, after the record's own fields
pub fn resource_log_fields(resource: Option<&Resource>, schema_url: &str) -> Map<String, Value> {
    let mut resource_log_json = Map::with_capacity(
        4 + resource.map_or(0, |resource| attribute_capacity(&resource.attributes)),
    );
    if let Some(resource) = resource {
        insert_attributes(&mut resource_log_json, &resource.attributes);
        resource_log_json.insert(
            "resource_dropped_attributes_count".to_string(),
            Value::Number(resource.dropped_attributes_count.into()),
        );
    }
    resource_log_json.insert(
        "schema_url".to_string(),
        Value::String(schema_url.to_string()),
    );
    resource_log_json
}

/// The row of one log record: the scope fields, the record's own fields over them,
/// then the resource fields over both
pub fn log_record_row(
    log_record: &LogRecord,
    scope_fields: &Map<String, Value>,
    resource_fields: &Map<String, Value>,
    budget: &mut FlattenBudget,
) -> Result<Map<String, Value>, JsonFlattenError> {
    let log_record_json = flatten_log_record(log_record, budget)?;
    let mut row = scope_fields.clone();
    row.extend(log_record_json);
    row.extend(resource_fields.clone());
    Ok(row)
}

/// Hands the row of every log record of `resource_logs` to `sink`, one at a time.
/// The JSON bodies of every record are charged to `budget`, which all records of the
/// request share, before their rows are built.
pub fn flatten_logs_into<S: RowSink>(
    resource_logs: &[ResourceLogs],
    budget: &mut FlattenBudget,
    sink: &mut S,
) -> Result<(), S::Error> {
    for resource_log in resource_logs {
        let resource_fields =
            resource_log_fields(resource_log.resource.as_ref(), &resource_log.schema_url);
        for scope_log in &resource_log.scope_logs {
            let scope_fields = scope_log_fields(scope_log.scope.as_ref(), &scope_log.schema_url);
            for log_record in &scope_log.log_records {
                sink.row(log_record_row(
                    log_record,
                    &scope_fields,
                    &resource_fields,
                    budget,
                )?)?;
                sink.record_end();
            }
        }
    }
    Ok(())
}

/// All rows of `resource_logs` at once. Counts the records for `tenant_id` once
/// every one was converted. Ingestion streams the rows instead, see `flatten_logs_into`.
fn collect_resource_logs(
    resource_logs: &[ResourceLogs],
    tenant_id: &str,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    let mut rows = Vec::new();
    flatten_logs_into(resource_logs, budget, &mut rows)?;
    let date = chrono::Utc::now().date_naive().to_string();
    increment_logs_collected_by_date(rows.len() as u64, &date, tenant_id);
    Ok(rows)
}

/// Same as `flatten_otel_logs`, for the protobuf request
pub fn flatten_otel_protobuf(
    message: &ExportLogsServiceRequest,
    tenant_id: &str,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    collect_resource_logs(&message.resource_logs, tenant_id, budget)
}

/// this function performs the custom flattening of the otel logs
/// and returns a `Vec` of `Value::Object` of the flattened json
/// Every record is charged to `budget`, which all records of the request share, before the
/// rows of its bodies are built. Fails once a record would go over its limits, dropping the
/// records converted before it
pub fn flatten_otel_logs(
    message: &LogsData,
    tenant_id: &str,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    collect_resource_logs(&message.resource_logs, tenant_id, budget)
}

#[cfg(test)]
mod tests {
    use super::{flatten_log_record, flatten_otel_logs, normalize_severity};
    use crate::utils::json::flatten::{
        FlattenBudget, FlattenCost, FlattenLimits, JsonFlattenError, generic_flattening_with_limits,
    };
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, KeyValueList, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
    use serde_json::json;

    /// Resident memory of this process in bytes
    #[cfg(target_os = "linux")]
    fn resident_bytes() -> usize {
        let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
        let pages: usize = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
        pages * 4096
    }

    // The scope map is cloned into every record's row, capacity included. Sized
    // by the raw attribute count, 200k valueless scope attributes (two bytes
    // each on the wire) made every one of 10k rows carry a map for 200k
    // entries: a body of half a megabyte held about 3 GiB.
    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "measures the resident memory of the process; run it alone"]
    fn valueless_scope_attributes_do_not_inflate_every_row() {
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;

        let logs = LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        attributes: vec![KeyValue::default(); 200_000],
                        ..Default::default()
                    }),
                    log_records: (0..10_000u64)
                        .map(|i| LogRecord {
                            time_unix_nano: 1_700_000_000_000_000_000 + i,
                            observed_time_unix_nano: 1_700_000_000_000_000_000 + i,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let before = resident_bytes();
        let rows = flatten_otel_logs(
            &logs,
            "tenant",
            &mut FlattenBudget::new(crate::utils::json::flatten::GENEROUS_LIMITS),
        )
        .unwrap();
        let grown = resident_bytes().saturating_sub(before);
        println!(
            "{} rows hold {} MiB of resident memory",
            rows.len(),
            grown / (1024 * 1024)
        );
        assert_eq!(rows.len(), 10_000);
        assert!(
            grown < 256 * 1024 * 1024,
            "{} MiB for 10k rows without attributes",
            grown / (1024 * 1024)
        );
    }

    fn flatten_record(
        number: i32,
        text: &str,
        body: &str,
    ) -> serde_json::Map<String, serde_json::Value> {
        flatten_log_record(
            &LogRecord {
                severity_number: number,
                severity_text: text.to_string(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(body.to_string())),
                }),
                ..Default::default()
            },
            &mut FlattenBudget::new(FlattenLimits::DEFAULT),
        )
        .unwrap()
    }

    #[test]
    fn text_only_error_record_is_categorized_with_a_neutral_body() {
        let record = flatten_record(0, "ERROR", "request completed");
        assert_eq!(record["severity_number"], json!(17));
        assert_eq!(record["severity_text"], json!("ERROR"));
        assert_eq!(record["p_log_category"], json!("ERROR"));
    }

    #[test]
    fn invalid_positive_and_negative_severity_numbers_do_not_panic() {
        for number in [25, i32::MAX, -1, i32::MIN] {
            let record = flatten_record(number, "", "request completed");
            assert_eq!(record["severity_number"], json!(number));
            assert_eq!(record["severity_text"], json!("UNSPECIFIED"));
            assert_eq!(record["p_log_category"], json!("UNSPECIFIED"));

            let record = flatten_record(number, "custom", "ERROR: request failed");
            assert_eq!(record["severity_text"], json!("custom"));
            assert_eq!(record["p_log_category"], json!("ERROR"));
        }
    }

    #[test]
    fn explicit_number_wins_over_contradictory_text_and_body_for_category() {
        let record = flatten_record(9, "ERROR", "fatal request failure");
        assert_eq!(record["severity_number"], json!(9));
        assert_eq!(record["severity_text"], json!("ERROR"));
        assert_eq!(record["p_log_category"], json!("INFO"));

        let record = flatten_record(0, "INFO", "fatal request failure");
        assert_eq!(record["severity_number"], json!(9));
        assert_eq!(record["p_log_category"], json!("INFO"));
    }

    #[test]
    fn unspecified_severity_retains_body_fallback() {
        for text in ["", "UNSPECIFIED", " unspecified "] {
            let record = flatten_record(0, text, "Error: request failed");
            assert_eq!(record["severity_number"], json!(0));
            assert_eq!(record["severity_text"], json!("UNSPECIFIED"));
            assert_eq!(record["p_log_category"], json!("ERROR"));
        }
    }

    #[test]
    fn derives_severity_text_from_number_when_missing() {
        assert_eq!(normalize_severity(17, ""), (17, "ERROR".to_string()));
        assert_eq!(
            normalize_severity(17, "UNSPECIFIED"),
            (17, "ERROR".to_string())
        );
    }

    #[test]
    fn derives_severity_number_from_text_when_unspecified() {
        assert_eq!(
            normalize_severity(0, "warning"),
            (13, "warning".to_string())
        );
        assert_eq!(
            normalize_severity(0, "unspecified"),
            (0, "UNSPECIFIED".to_string())
        );
        assert_eq!(
            normalize_severity(0, "SEVERITY_NUMBER_FATAL4"),
            (24, "SEVERITY_NUMBER_FATAL4".to_string())
        );
    }

    #[test]
    fn leaves_both_fields_unspecified_when_neither_is_present() {
        assert_eq!(normalize_severity(0, ""), (0, "UNSPECIFIED".to_string()));
    }

    #[test]
    fn preserves_provided_severity_fields() {
        assert_eq!(normalize_severity(18, "error"), (18, "error".to_string()));
    }

    fn limits(rows_per_event: usize, rows_per_request: usize) -> FlattenBudget {
        FlattenBudget::new(FlattenLimits {
            max_rows_per_event: rows_per_event,
            max_rows_per_request: rows_per_request,
            max_bytes_per_request: 1 << 20,
        })
    }

    fn string_body(body: &str) -> Option<AnyValue> {
        Some(AnyValue {
            value: Some(any_value::Value::StringValue(body.to_string())),
        })
    }

    fn record(body: Option<AnyValue>) -> LogRecord {
        LogRecord {
            severity_number: 9,
            body,
            ..Default::default()
        }
    }

    // Two resources, the first with two scopes, holding one record each
    fn logs(body: &str) -> LogsData {
        let scope = || ScopeLogs {
            log_records: vec![record(string_body(body))],
            ..Default::default()
        };
        LogsData {
            resource_logs: vec![
                ResourceLogs {
                    scope_logs: vec![scope(), scope()],
                    ..Default::default()
                },
                ResourceLogs {
                    scope_logs: vec![scope()],
                    ..Default::default()
                },
            ],
        }
    }

    const SIX_ROW_BODY: &str = r#"{"a": [1, 2, 3], "b": [true, false]}"#;

    #[test]
    fn json_body_rows_are_merged_into_the_record_and_charged() {
        let mut budget = limits(6, 6);
        let flattened =
            flatten_log_record(&record(string_body(SIX_ROW_BODY)), &mut budget).unwrap();
        // The last row wins, as before
        assert_eq!(flattened["body"], json!(SIX_ROW_BODY));
        assert_eq!(flattened["body_a"], json!(3));
        assert_eq!(flattened["body_b"], json!(false));
        assert_eq!(budget.used().rows, 6);

        assert!(matches!(
            flatten_log_record(&record(string_body(SIX_ROW_BODY)), &mut limits(5, 6)),
            Err(JsonFlattenError::RowsPerEventLimitExceeded(5))
        ));
    }

    #[test]
    fn all_json_bodies_of_a_record_are_one_event() {
        let kv = |key: &str, body: &str| KeyValue {
            key: key.to_string(),
            value: string_body(body),
            ..Default::default()
        };
        let body = Some(AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![
                    kv("x", r#"{"a": [1, 2, 3]}"#),
                    kv("plain", "not json"),
                    kv("y", r#"{"b": [1, 2, 3]}"#),
                ],
            })),
        });
        let mut budget = limits(6, 6);
        let flattened = flatten_log_record(&record(body.clone()), &mut budget).unwrap();
        assert_eq!(flattened["x_a"], json!(3));
        assert_eq!(flattened["y_b"], json!(3));
        assert_eq!(flattened["plain"], json!("not json"));
        assert_eq!(budget.used().rows, 6);
        // Each body fits on its own, but not both together
        assert!(matches!(
            flatten_log_record(&record(body), &mut limits(5, 6)),
            Err(JsonFlattenError::RowsPerEventLimitExceeded(5))
        ));
    }

    #[test]
    fn bodies_that_are_not_json_objects_are_kept_and_not_charged() {
        for body in ["not json", "[1, 2, 3]", "12", r#"{"a": [1, 2"#] {
            let mut budget = limits(1, 1);
            let flattened = flatten_log_record(&record(string_body(body)), &mut budget).unwrap();
            assert_eq!(flattened["body"], json!(body));
            assert_eq!(budget.used().rows, 0, "body: {body}");
        }
        let mut budget = limits(1, 1);
        flatten_log_record(&record(None), &mut budget).unwrap();
        assert_eq!(budget.used().rows, 0);
    }

    #[test]
    fn records_of_all_scopes_and_resources_share_the_request_budget() {
        let mut budget = limits(6, 18);
        assert_eq!(
            flatten_otel_logs(&logs(SIX_ROW_BODY), "tenant", &mut budget)
                .unwrap()
                .len(),
            3
        );
        assert_eq!(budget.used().rows, 18);
        // The third record, in the second resource, goes over the request limit
        assert!(matches!(
            flatten_otel_logs(&logs(SIX_ROW_BODY), "tenant", &mut limits(6, 17)),
            Err(JsonFlattenError::RowsPerRequestLimitExceeded(17))
        ));
        // The second record, in the second scope of the first resource, goes over it too
        assert!(matches!(
            flatten_otel_logs(&logs(SIX_ROW_BODY), "tenant", &mut limits(6, 11)),
            Err(JsonFlattenError::RowsPerRequestLimitExceeded(11))
        ));
    }

    // Logs collected for `tenant` over all dates, so that a test running across midnight (UTC)
    // still counts what it collected. Each test uses a tenant of its own, as the counter is
    // global.
    fn logs_collected(tenant: &str) -> u64 {
        use prometheus::core::Collector;

        crate::metrics::TOTAL_LOGS_COLLECTED_BY_DATE
            .collect()
            .iter()
            .flat_map(|family| family.get_metric())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.get_name() == "tenant_id" && label.get_value() == tenant)
            })
            .map(|metric| metric.get_counter().get_value() as u64)
            .sum()
    }

    #[test]
    fn logs_collected_metric_counts_only_converted_requests() {
        let tenant = "flatten-limit-metric-tenant";
        let before = logs_collected(tenant);
        // Fails on the third record, after the scopes of the first resource were converted
        assert!(flatten_otel_logs(&logs(SIX_ROW_BODY), tenant, &mut limits(6, 17)).is_err());
        assert_eq!(logs_collected(tenant), before);

        flatten_otel_logs(&logs(SIX_ROW_BODY), tenant, &mut limits(6, 18)).unwrap();
        assert_eq!(logs_collected(tenant), before + 3);
    }

    fn byte_limit(bytes: usize) -> FlattenBudget {
        FlattenBudget::new(FlattenLimits {
            max_rows_per_event: 1000,
            max_rows_per_request: 10_000,
            max_bytes_per_request: bytes,
        })
    }

    fn kv_body(entries: &[(&str, &str)]) -> Option<AnyValue> {
        Some(AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: entries
                    .iter()
                    .map(|(key, body)| KeyValue {
                        key: key.to_string(),
                        value: string_body(body),
                        ..Default::default()
                    })
                    .collect(),
            })),
        })
    }

    // Compact JSON bytes of the rows of `body`, with their fields renamed as they are merged
    // into the record
    fn prefixed_rows_len(key: &str, body: &str) -> usize {
        let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
        generic_flattening_with_limits(&parsed, FlattenLimits::DEFAULT)
            .unwrap()
            .iter()
            .map(|row| {
                let prefixed: serde_json::Map<String, serde_json::Value> = row
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(field, value)| (format!("{key}_{field}"), value.clone()))
                    .collect();
                serde_json::to_vec(&prefixed).unwrap().len()
            })
            .sum()
    }

    // One row of 32 short scalar fields, about 300 bytes before the key is copied into them
    fn wide_body() -> String {
        let fields: serde_json::Map<String, serde_json::Value> =
            (0..32).map(|i| (format!("f{i}"), json!(i))).collect();
        serde_json::Value::Object(fields).to_string()
    }

    #[test]
    fn long_body_key_copied_into_every_field_is_charged() {
        let body = wide_body();
        let long_key = "k".repeat(128);
        // 32 copies of the key alone are over the limit, the parsed body is not
        let mut budget = byte_limit(4096);
        assert!(matches!(
            flatten_log_record(
                &record(kv_body(&[(long_key.as_str(), body.as_str())])),
                &mut budget
            ),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(4096))
        ));
        assert_eq!(budget.used(), FlattenCost::default());

        let flattened =
            flatten_log_record(&record(kv_body(&[("k", body.as_str())])), &mut budget).unwrap();
        assert_eq!(flattened["k_f31"], json!(31));
        assert_eq!(budget.used().rows, 1);
        assert_eq!(budget.used().bytes, prefixed_rows_len("k", &body));
    }

    #[test]
    fn body_key_prefixes_are_charged_exactly() {
        // Two rows of two fields each, under a key with escapes
        let key = "k\"\t\u{1}";
        let body = r#"{"a": [1, 2], "b": "x"}"#;
        let other = r#"{"c": {"d": [true, null]}}"#;
        let body_record = || record(kv_body(&[(key, body), ("plain", "text"), ("o", other)]));
        let expected = prefixed_rows_len(key, body) + prefixed_rows_len("o", other);

        let mut budget = byte_limit(expected);
        let flattened = flatten_log_record(&body_record(), &mut budget).unwrap();
        assert_eq!(flattened[&format!("{key}_a")], json!(2));
        assert_eq!(flattened["o_c"], json!({"d": null}));
        assert_eq!(
            budget.used(),
            FlattenCost {
                rows: 4,
                bytes: expected
            }
        );

        let mut budget = byte_limit(expected - 1);
        assert!(matches!(
            flatten_log_record(&body_record(), &mut budget),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(limit)) if limit == expected - 1
        ));
        assert_eq!(budget.used(), FlattenCost::default());
    }

    #[test]
    fn body_key_prefixes_add_up_over_records() {
        let body = wide_body();
        let key = "k".repeat(16);
        let per_record = prefixed_rows_len(&key, &body);
        let mut budget = byte_limit(2 * per_record - 1);
        flatten_log_record(
            &record(kv_body(&[(key.as_str(), body.as_str())])),
            &mut budget,
        )
        .unwrap();
        assert!(matches!(
            flatten_log_record(
                &record(kv_body(&[(key.as_str(), body.as_str())])),
                &mut budget
            ),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(_))
        ));
        assert_eq!(budget.used().bytes, per_record);
    }

    #[test]
    fn logs_collected_metric_is_unchanged_when_a_body_key_is_over_the_budget() {
        let tenant = "flatten-limit-prefix-metric-tenant";
        let body = wide_body();
        let long_key = "k".repeat(128);
        let scope = |key: &str| ScopeLogs {
            log_records: vec![record(kv_body(&[(key, body.as_str())]))],
            ..Default::default()
        };
        // The first resource fits, the long key in the second one does not
        let logs = LogsData {
            resource_logs: vec![
                ResourceLogs {
                    scope_logs: vec![scope("k")],
                    ..Default::default()
                },
                ResourceLogs {
                    scope_logs: vec![scope(&long_key)],
                    ..Default::default()
                },
            ],
        };

        let before = logs_collected(tenant);
        assert!(matches!(
            flatten_otel_logs(&logs, tenant, &mut byte_limit(4096)),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(4096))
        ));
        assert_eq!(logs_collected(tenant), before);

        // Without the second resource, the request is converted and counted
        let fitting = LogsData {
            resource_logs: logs.resource_logs[..1].to_vec(),
        };
        flatten_otel_logs(&fitting, tenant, &mut byte_limit(4096)).unwrap();
        assert_eq!(logs_collected(tenant), before + 1);
    }

    // Compact JSON bytes of the rows of `body` merged into the record under `key`, once
    // `push_logs` structurally flattens them
    fn structural_prefixed_rows_len(key: &str, body: &str) -> usize {
        let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
        generic_flattening_with_limits(&parsed, FlattenLimits::DEFAULT)
            .unwrap()
            .iter()
            .map(|row| {
                let mut prefixed = serde_json::Value::Object(
                    row.as_object()
                        .unwrap()
                        .iter()
                        .map(|(field, value)| (format!("{key}_{field}"), value.clone()))
                        .collect(),
                );
                crate::utils::json::flatten::flatten(&mut prefixed, "_", None, None, None, false)
                    .unwrap();
                serde_json::to_vec(&prefixed).unwrap().len()
            })
            .sum()
    }

    // 4 rows of the 32 fields of `wide_body` below a parent
    fn nested_body() -> String {
        format!(r#"{{"p": {}, "r": [0, 1, 2, 3]}}"#, wide_body())
    }

    #[test]
    fn body_key_copied_into_nested_field_names_is_charged() {
        let body = nested_body();
        let long_key = "k".repeat(128);
        // The generic rows and their root field prefixes fit, but structurally flattening
        // the record names all 32 fields below `p` after the long key
        assert!(prefixed_rows_len(&long_key, &body) <= 4096);
        let mut budget = byte_limit(4096);
        assert!(matches!(
            flatten_log_record(
                &record(kv_body(&[(long_key.as_str(), body.as_str())])),
                &mut budget
            ),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(4096))
        ));
        assert_eq!(budget.used(), FlattenCost::default());

        // With a short key, the structurally flattened rows are the larger, and charged
        let expected = structural_prefixed_rows_len("k", &body);
        assert!(expected > prefixed_rows_len("k", &body));
        let flattened =
            flatten_log_record(&record(kv_body(&[("k", body.as_str())])), &mut budget).unwrap();
        assert_eq!(flattened["k_p"]["f31"], json!(31));
        assert_eq!(
            budget.used(),
            FlattenCost {
                rows: 4,
                bytes: expected
            }
        );
    }

    // Release-profile micro-benchmarks. Run with a filter, for example:
    //   cargo test --release --lib -- --ignored --exact otel::logs::tests::bench_flatten_otel_protobuf_logs --nocapture
    // They never touch the PARSEABLE global: every timestamp is set, so the rows
    // are deterministic, and the flatten budget is built from FlattenLimits::DEFAULT.
    mod benches {
        use crate::otel::bench;
        use crate::utils::json::flatten::{FlattenBudget, FlattenLimits};
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::{
            AnyValue, ArrayValue, InstrumentationScope, KeyValue, any_value,
        };
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use opentelemetry_proto::tonic::resource::v1::Resource;

        const RECORDS_PER_SCOPE: usize = 250;
        const ITERS: usize = 20;

        fn string_kv(key: &str, value: impl Into<String>) -> KeyValue {
            KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(value.into())),
                }),
                ..Default::default()
            }
        }

        fn int_kv(key: &str, value: i64) -> KeyValue {
            KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(value)),
                }),
                ..Default::default()
            }
        }

        fn bool_kv(key: &str, value: bool) -> KeyValue {
            KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::BoolValue(value)),
                }),
                ..Default::default()
            }
        }

        fn array_kv(key: &str) -> KeyValue {
            KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::ArrayValue(ArrayValue {
                        values: vec![
                            AnyValue {
                                value: Some(any_value::Value::StringValue("keep-alive".into())),
                            },
                            AnyValue {
                                value: Some(any_value::Value::StringValue("gzip".into())),
                            },
                            AnyValue {
                                value: Some(any_value::Value::IntValue(3)),
                            },
                        ],
                    })),
                }),
                ..Default::default()
            }
        }

        fn record(i: usize) -> LogRecord {
            // Every tenth body is a JSON object, which the log flattener parses and expands
            let body = if i.is_multiple_of(10) {
                format!(
                    r#"{{"event":"checkout","user":{{"id":{i}}},"items":[{{"sku":"a","qty":1}},{{"sku":"b","qty":2}}],"ok":true}}"#
                )
            } else {
                format!("request {i} completed in {} ms", i % 500)
            };
            LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000 + i as u64 * 1_000_000,
                observed_time_unix_nano: 1_700_000_000_000_000_100 + i as u64 * 1_000_000,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(body)),
                }),
                attributes: vec![
                    string_kv("http.method", "GET"),
                    int_kv("http.status_code", (i % 500) as i64),
                    string_kv("http.target", format!("/api/v1/items/{i}")),
                    string_kv("net.host.name", "checkout.example.com"),
                    string_kv("user_agent.original", "Mozilla/5.0 (compatible; bench)"),
                    bool_kv("cache.hit", i.is_multiple_of(3)),
                    string_kv("http.request.header.x-request-id", format!("req-{i:016x}")),
                    array_kv("http.request.header.accept-encoding"),
                ],
                dropped_attributes_count: 0,
                flags: 1,
                trace_id: vec![0xab; 16],
                span_id: vec![0xcd; 8],
                event_name: String::new(),
            }
        }

        fn request() -> ExportLogsServiceRequest {
            let resource_logs = (0..4)
                .map(|resource_index| ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![
                            string_kv("service.name", "checkout"),
                            string_kv("service.version", "1.42.0"),
                            string_kv("deployment.environment", "production"),
                            string_kv("host.name", format!("checkout-{resource_index}")),
                            int_kv("process.pid", 4242 + resource_index as i64),
                        ],
                        dropped_attributes_count: 0,
                        entity_refs: vec![],
                    }),
                    scope_logs: (0..2)
                        .map(|scope_index| ScopeLogs {
                            scope: Some(InstrumentationScope {
                                name: "io.opentelemetry.contrib.bench".to_string(),
                                version: "0.31.0".to_string(),
                                attributes: vec![
                                    string_kv("scope.kind", "server"),
                                    string_kv(
                                        "scope.extra",
                                        format!("{resource_index}-{scope_index}"),
                                    ),
                                    int_kv("scope.ordinal", scope_index),
                                ],
                                dropped_attributes_count: 0,
                            }),
                            log_records: (0..RECORDS_PER_SCOPE)
                                .map(|record_index| {
                                    record(
                                        resource_index as usize * 1000
                                            + scope_index as usize * 100
                                            + record_index,
                                    )
                                })
                                .collect(),
                            schema_url: "https://opentelemetry.io/schemas/1.24.0".to_string(),
                        })
                        .collect(),
                    schema_url: "https://opentelemetry.io/schemas/1.24.0".to_string(),
                })
                .collect();
            ExportLogsServiceRequest { resource_logs }
        }

        /// What ingestion does with a decoded request before staging: the rows
        /// handed over in chunks (collected here)
        fn flatten(request: &ExportLogsServiceRequest) -> Vec<serde_json::Value> {
            let mut budget = FlattenBudget::new(FlattenLimits::DEFAULT);
            bench::chunked(|chunker| {
                super::super::flatten_logs_into(&request.resource_logs, &mut budget, chunker)
            })
        }

        #[test]
        #[ignore = "performance benchmark"]
        fn bench_flatten_otel_protobuf_logs() {
            let request = request();
            bench::run("bench_flatten_otel_protobuf_logs", ITERS, || {
                flatten(&request)
            });
        }

        /// The rows of the benchmark request, pinned so that a refactor changing
        /// them fails a normal `cargo test --lib` run. The digest is of
        /// `serde_json::to_vec` of the rows; it was first recorded from the
        /// unoptimised flatteners.
        #[test]
        fn benchmark_request_rows_are_stable() {
            let rows = flatten(&request());
            let bytes = serde_json::to_vec(&rows).unwrap();
            use std::hash::Hasher;
            let mut hasher = rustc_hash::FxHasher::default();
            hasher.write(&bytes);
            assert_eq!(rows.len(), 2000);
            assert_eq!(
                (bytes.len(), hasher.finish()),
                (2_288_617, 0xa4ce_c43f_538f_d2cd),
                "flattened rows of the benchmark request changed"
            );
        }

        /// Cost of one flattened row clone, the per-record work the row
        /// assembly in `process_resource_logs` repeats.
        #[test]
        #[ignore = "performance benchmark"]
        fn bench_row_clone() {
            let rows = flatten(&request());
            let row = rows[0].as_object().expect("object row");
            let mut sum = 0usize;
            bench::time_calls("bench_row_clone", 200_000, |_| {
                let clone = row.clone();
                sum += clone.len();
            });
            assert!(sum > 0);
        }
    }
}
