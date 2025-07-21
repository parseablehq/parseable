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
use opentelemetry_proto::tonic::trace::v1::ScopeSpans;
use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_proto::tonic::trace::v1::Status;
use opentelemetry_proto::tonic::trace::v1::TracesData;
use opentelemetry_proto::tonic::trace::v1::span::Event;
use opentelemetry_proto::tonic::trace::v1::span::Link;
use serde_json::{Map, Value};

use super::otel_utils::convert_epoch_nano_to_timestamp;
use super::otel_utils::insert_attributes;

pub const OTEL_TRACES_KNOWN_FIELD_LIST: [&str; 30] = [
    "scope_name",
    "scope_version",
    "scope_schema_url",
    "scope_dropped_attributes_count",
    "resource_schema_url",
    "resource_dropped_attributes_count",
    "span_trace_id",
    "span_span_id",
    "span_name",
    "span_parent_span_id",
    "name",
    "span_kind",
    "span_kind_description",
    "span_start_time_unix_nano",
    "span_end_time_unix_nano",
    "event_name",
    "event_time_unix_nano",
    "event_dropped_attributes_count",
    "link_span_id",
    "link_trace_id",
    "link_dropped_attributes_count",
    "span_dropped_events_count",
    "span_dropped_links_count",
    "span_dropped_attributes_count",
    "span_trace_state",
    "span_flags",
    "span_flags_description",
    "span_status_code",
    "span_status_description",
    "span_status_message",
];
/// this function flattens the `ScopeSpans` object
/// and returns a `Vec` of `Map` of the flattened json
fn flatten_scope_span(scope_span: &ScopeSpans) -> Vec<Map<String, Value>> {
    let mut vec_scope_span_json = Vec::new();
    let mut scope_span_json = Map::new();
    for span in &scope_span.spans {
        let span_record_json = flatten_span_record(span);
        vec_scope_span_json.extend(span_record_json);
    }

    if let Some(scope) = &scope_span.scope {
        scope_span_json.insert("scope_name".to_string(), Value::String(scope.name.clone()));
        scope_span_json.insert(
            "scope_version".to_string(),
            Value::String(scope.version.clone()),
        );
        insert_attributes(&mut scope_span_json, &scope.attributes);
        scope_span_json.insert(
            "scope_dropped_attributes_count".to_string(),
            Value::Number(scope.dropped_attributes_count.into()),
        );

        for span_json in &mut vec_scope_span_json {
            for (key, value) in &scope_span_json {
                span_json.insert(key.clone(), value.clone());
            }
        }
    }

    for span_json in &mut vec_scope_span_json {
        span_json.insert(
            "scope_schema_url".to_string(),
            Value::String(scope_span.schema_url.clone()),
        );
    }

    vec_scope_span_json
}

/// this function performs the custom flattening of the otel traces event
/// and returns a `Vec` of `Value::Object` of the flattened json
pub fn flatten_otel_traces(message: &TracesData) -> Vec<Value> {
    let mut vec_otel_json = Vec::new();

    for record in &message.resource_spans {
        let mut resource_span_json = Map::new();
        if let Some(resource) = &record.resource {
            insert_attributes(&mut resource_span_json, &resource.attributes);
            resource_span_json.insert(
                "resource_dropped_attributes_count".to_string(),
                Value::Number(resource.dropped_attributes_count.into()),
            );
        }

        let mut vec_resource_spans_json = Vec::new();
        for scope_span in &record.scope_spans {
            let scope_span_json = flatten_scope_span(scope_span);
            vec_resource_spans_json.extend(scope_span_json);
        }

        resource_span_json.insert(
            "resource_schema_url".to_string(),
            Value::String(record.schema_url.clone()),
        );

        for resource_spans_json in &mut vec_resource_spans_json {
            for (key, value) in &resource_span_json {
                resource_spans_json.insert(key.clone(), value.clone());
            }

            vec_otel_json.push(Value::Object(resource_spans_json.clone()));
        }
    }

    vec_otel_json
}

/// otel traces has json array of events
/// this function flattens the `Event` object
/// and returns a `Vec` of `Map` of the flattened json
fn flatten_events(events: &[Event]) -> Vec<Map<String, Value>> {
    events
        .iter()
        .map(|event| {
            let mut event_json = Map::new();
            event_json.insert(
                "event_time_unix_nano".to_string(),
                Value::String(
                    convert_epoch_nano_to_timestamp(event.time_unix_nano as i64).to_string(),
                ),
            );
            event_json.insert("event_name".to_string(), Value::String(event.name.clone()));
            insert_attributes(&mut event_json, &event.attributes);
            event_json.insert(
                "event_dropped_attributes_count".to_string(),
                Value::Number(event.dropped_attributes_count.into()),
            );
            event_json
        })
        .collect()
}

/// otel traces has json array of links
/// this function flattens the `Link` object
/// and returns a `Vec` of `Map` of the flattened json
fn flatten_links(links: &[Link]) -> Vec<Map<String, Value>> {
    links
        .iter()
        .map(|link| {
            let mut link_json = Map::new();
            link_json.insert(
                "link_span_id".to_string(),
                Value::String(hex::encode(&link.span_id)),
            );
            link_json.insert(
                "link_trace_id".to_string(),
                Value::String(hex::encode(&link.trace_id)),
            );

            insert_attributes(&mut link_json, &link.attributes);
            link_json.insert(
                "link_dropped_attributes_count".to_string(),
                Value::Number(link.dropped_attributes_count.into()),
            );
            link_json
        })
        .collect()
}

/// otel trace event has status
/// there is a mapping of status code to status description provided in proto
/// this function fetches the status description from the status code
/// and adds it to the flattened json
fn flatten_status(status: &Status) -> Map<String, Value> {
    let mut status_json = Map::new();
    status_json.insert(
        "span_status_message".to_string(),
        Value::String(status.message.clone()),
    );
    status_json.insert(
        "span_status_code".to_string(),
        Value::Number(status.code.into()),
    );
    let description = match status.code {
        0 => "STATUS_CODE_UNSET",
        1 => "STATUS_CODE_OK",
        2 => "STATUS_CODE_ERROR",
        _ => "",
    };
    status_json.insert(
        "span_status_description".to_string(),
        Value::String(description.to_string()),
    );

    status_json
}

/// otel log event has flags
/// there is a mapping of flags to flags description provided in proto
/// this function fetches the flags description from the flags
/// and adds it to the flattened json
fn flatten_flags(flags: u32) -> Map<String, Value> {
    let mut flags_json = Map::new();
    flags_json.insert("span_flags".to_string(), Value::Number(flags.into()));
    let description = match flags {
        0 => "SPAN_FLAGS_DO_NOT_USE",
        255 => "SPAN_FLAGS_TRACE_FLAGS_MASK",
        256 => "SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK",
        512 => "SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK",
        _ => "",
    };
    flags_json.insert(
        "span_flags_description".to_string(),
        Value::String(description.to_string()),
    );

    flags_json
}

/// otel span event has kind
/// there is a mapping of kind to kind description provided in proto
/// this function fetches the kind description from the kind
/// and adds it to the flattened json
fn flatten_kind(kind: i32) -> Map<String, Value> {
    let mut kind_json = Map::new();
    kind_json.insert("span_kind".to_string(), Value::Number(kind.into()));
    let description = match kind {
        0 => "SPAN_KIND_UNSPECIFIED",
        1 => "SPAN_KIND_INTERNAL",
        2 => "SPAN_KIND_SERVER",
        3 => "SPAN_KIND_CLIENT",
        4 => "SPAN_KIND_PRODUCER",
        5 => "SPAN_KIND_CONSUMER",
        _ => "",
    };
    kind_json.insert(
        "span_kind_description".to_string(),
        Value::String(description.to_string()),
    );

    kind_json
}

/// this function flattens the `Span` object
/// and returns a `Vec` of `Map` of the flattened json
/// this function is called recursively for each span record object in the otel traces event
fn flatten_span_record(span_record: &Span) -> Vec<Map<String, Value>> {
    let mut span_records_json = Vec::new();
    let mut span_record_json = Map::new();
    span_record_json.insert(
        "span_trace_id".to_string(),
        Value::String(hex::encode(&span_record.trace_id)),
    );
    span_record_json.insert(
        "span_span_id".to_string(),
        Value::String(hex::encode(&span_record.span_id)),
    );
    span_record_json.insert(
        "span_trace_state".to_string(),
        Value::String(span_record.trace_state.clone()),
    );
    span_record_json.insert(
        "span_parent_span_id".to_string(),
        Value::String(hex::encode(&span_record.parent_span_id)),
    );
    span_record_json.extend(flatten_flags(span_record.flags));
    span_record_json.insert(
        "span_name".to_string(),
        Value::String(span_record.name.clone()),
    );
    span_record_json.extend(flatten_kind(span_record.kind));
    span_record_json.insert(
        "span_start_time_unix_nano".to_string(),
        Value::String(convert_epoch_nano_to_timestamp(
            span_record.start_time_unix_nano as i64,
        )),
    );
    span_record_json.insert(
        "span_end_time_unix_nano".to_string(),
        Value::String(convert_epoch_nano_to_timestamp(
            span_record.end_time_unix_nano as i64,
        )),
    );
    insert_attributes(&mut span_record_json, &span_record.attributes);
    span_record_json.insert(
        "span_dropped_attributes_count".to_string(),
        Value::Number(span_record.dropped_attributes_count.into()),
    );
    let events_json = flatten_events(&span_record.events);
    span_records_json.extend(events_json);
    span_record_json.insert(
        "span_dropped_events_count".to_string(),
        Value::Number(span_record.dropped_events_count.into()),
    );
    let links_json = flatten_links(&span_record.links);
    span_records_json.extend(links_json);

    span_record_json.insert(
        "span_dropped_links_count".to_string(),
        Value::Number(span_record.dropped_links_count.into()),
    );
    if let Some(status) = &span_record.status {
        span_record_json.extend(flatten_status(status));
    }
    // if span_record.events is null, code should still flatten other elements in the span record - this is handled in the if block
    // else block handles the flattening the span record that includes events and links records in each span record
    if span_records_json.is_empty() {
        span_records_json = vec![span_record_json];
    } else {
        for span_json in &mut span_records_json {
            for (key, value) in &span_record_json {
                span_json.insert(key.clone(), value.clone());
            }
        }
    }
    span_records_json
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, TracesData};

    /// Helper function to create a sample trace ID
    fn sample_trace_id() -> Vec<u8> {
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    /// Helper function to create a sample span ID
    fn sample_span_id() -> Vec<u8> {
        vec![1, 2, 3, 4, 5, 6, 7, 8]
    }

    /// Helper function to create sample attributes
    fn sample_attributes() -> Vec<KeyValue> {
        vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "test-service".to_string(),
                        ),
                    ),
                }),
            },
            KeyValue {
                key: "http.method".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "GET".to_string(),
                        ),
                    ),
                }),
            },
        ]
    }

    #[test]
    fn test_flatten_status_code_mapping() {
        // Test that status codes are correctly mapped to descriptions
        let test_cases = vec![
            (0, "STATUS_CODE_UNSET"),
            (1, "STATUS_CODE_OK"),
            (2, "STATUS_CODE_ERROR"),
            (999, ""), // Unknown status code should return empty string
        ];

        for (code, expected_description) in test_cases {
            let status = Status {
                message: "Test message".to_string(),
                code,
            };

            let result = flatten_status(&status);

            assert_eq!(
                result.get("span_status_code").unwrap(),
                &Value::Number(code.into()),
                "Status code should be preserved"
            );
            assert_eq!(
                result.get("span_status_description").unwrap(),
                &Value::String(expected_description.to_string()),
                "Status description should match expected value for code {}",
                code
            );
            assert_eq!(
                result.get("span_status_message").unwrap(),
                &Value::String("Test message".to_string()),
                "Status message should be preserved"
            );
        }
    }

    #[test]
    fn test_flatten_span_kind_mapping() {
        // Test that span kinds are correctly mapped to descriptions
        let test_cases = vec![
            (0, "SPAN_KIND_UNSPECIFIED"),
            (1, "SPAN_KIND_INTERNAL"),
            (2, "SPAN_KIND_SERVER"),
            (3, "SPAN_KIND_CLIENT"),
            (4, "SPAN_KIND_PRODUCER"),
            (5, "SPAN_KIND_CONSUMER"),
            (999, ""), // Unknown kind should return empty string
        ];

        for (kind, expected_description) in test_cases {
            let result = flatten_kind(kind);

            assert_eq!(
                result.get("span_kind").unwrap(),
                &Value::Number(kind.into()),
                "Span kind should be preserved"
            );
            assert_eq!(
                result.get("span_kind_description").unwrap(),
                &Value::String(expected_description.to_string()),
                "Span kind description should match expected value for kind {}",
                kind
            );
        }
    }

    #[test]
    fn test_flatten_flags_mapping() {
        // Test that flags are correctly mapped to descriptions
        let test_cases = vec![
            (0, "SPAN_FLAGS_DO_NOT_USE"),
            (255, "SPAN_FLAGS_TRACE_FLAGS_MASK"),
            (256, "SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK"),
            (512, "SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK"),
            (999, ""), // Unknown flag should return empty string
        ];

        for (flags, expected_description) in test_cases {
            let result = flatten_flags(flags);

            assert_eq!(
                result.get("span_flags").unwrap(),
                &Value::Number(flags.into()),
                "Span flags should be preserved"
            );
            assert_eq!(
                result.get("span_flags_description").unwrap(),
                &Value::String(expected_description.to_string()),
                "Span flags description should match expected value for flags {}",
                flags
            );
        }
    }

    #[test]
    fn test_flatten_events_structure() {
        // Test that events are properly flattened with all expected fields
        let events = vec![
            Event {
                time_unix_nano: 1640995200000000000,
                name: "request.start".to_string(),
                attributes: sample_attributes(),
                dropped_attributes_count: 2,
            },
            Event {
                time_unix_nano: 1640995201000000000,
                name: "request.end".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            },
        ];

        let result = flatten_events(&events);

        assert_eq!(result.len(), 2, "Should have two flattened events");

        // Check first event
        let first_event = &result[0];
        assert!(
            first_event.contains_key("event_time_unix_nano"),
            "Should contain timestamp"
        );
        assert_eq!(
            first_event.get("event_name").unwrap(),
            &Value::String("request.start".to_string()),
            "Event name should be preserved"
        );
        assert_eq!(
            first_event.get("event_dropped_attributes_count").unwrap(),
            &Value::Number(2.into()),
            "Dropped attributes count should be preserved"
        );
        assert!(
            first_event.contains_key("service.name"),
            "Should contain flattened attributes"
        );

        // Check second event
        let second_event = &result[1];
        assert_eq!(
            second_event.get("event_name").unwrap(),
            &Value::String("request.end".to_string()),
            "Second event name should be preserved"
        );
        assert_eq!(
            second_event.get("event_dropped_attributes_count").unwrap(),
            &Value::Number(0.into()),
            "Second event dropped count should be zero"
        );
    }

    #[test]
    fn test_flatten_links_structure() {
        // Test that links are properly flattened with all expected fields
        let links = vec![Link {
            trace_id: sample_trace_id(),
            span_id: sample_span_id(),
            trace_state: "state1".to_string(),
            attributes: sample_attributes(),
            dropped_attributes_count: 1,
            flags: 0,
        }];

        let result = flatten_links(&links);

        assert_eq!(result.len(), 1, "Should have one flattened link");

        let link = &result[0];
        assert_eq!(
            link.get("link_trace_id").unwrap(),
            &Value::String("0102030405060708090a0b0c0d0e0f10".to_string()),
            "Trace ID should be hex encoded"
        );
        assert_eq!(
            link.get("link_span_id").unwrap(),
            &Value::String("0102030405060708".to_string()),
            "Span ID should be hex encoded"
        );
        assert_eq!(
            link.get("link_dropped_attributes_count").unwrap(),
            &Value::Number(1.into()),
            "Dropped attributes count should be preserved"
        );
        assert!(
            link.contains_key("service.name"),
            "Should contain flattened attributes"
        );
    }

    #[test]
    fn test_flatten_span_record_with_events_and_links() {
        // Test that span records with events and links are properly flattened
        let span = Span {
            trace_id: sample_trace_id(),
            span_id: sample_span_id(),
            trace_state: "test-state".to_string(),
            parent_span_id: vec![],
            flags: 0,
            name: "test-span".to_string(),
            kind: 2, // SERVER
            start_time_unix_nano: 1640995200000000000,
            end_time_unix_nano: 1640995201000000000,
            attributes: sample_attributes(),
            dropped_attributes_count: 0,
            events: vec![Event {
                time_unix_nano: 1640995200500000000,
                name: "middleware".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }],
            dropped_events_count: 0,
            links: vec![Link {
                trace_id: sample_trace_id(),
                span_id: sample_span_id(),
                trace_state: "".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
            }],
            dropped_links_count: 0,
            status: Some(Status {
                message: "OK".to_string(),
                code: 1,
            }),
        };

        let result = flatten_span_record(&span);

        // Should have 2 records: one for event and one for link
        assert_eq!(result.len(), 2, "Should have records for event and link");

        // Both records should contain span-level information
        for record in &result {
            assert_eq!(
                record.get("span_name").unwrap(),
                &Value::String("test-span".to_string()),
                "All records should contain span name"
            );
            assert_eq!(
                record.get("span_kind").unwrap(),
                &Value::Number(2.into()),
                "All records should contain span kind"
            );
            assert_eq!(
                record.get("span_kind_description").unwrap(),
                &Value::String("SPAN_KIND_SERVER".to_string()),
                "All records should contain span kind description"
            );
            assert!(
                record.contains_key("span_trace_id"),
                "Should contain trace ID"
            );
            assert!(
                record.contains_key("span_span_id"),
                "Should contain span ID"
            );
            assert!(
                record.contains_key("span_start_time_unix_nano"),
                "Should contain start time"
            );
            assert!(
                record.contains_key("span_end_time_unix_nano"),
                "Should contain end time"
            );
            assert!(
                record.contains_key("service.name"),
                "Should contain span attributes"
            );
            assert!(
                record.contains_key("span_status_code"),
                "Should contain status"
            );
        }

        // One record should be an event, one should be a link
        let has_event = result.iter().any(|r| r.contains_key("event_name"));
        let has_link = result.iter().any(|r| r.contains_key("link_trace_id"));
        assert!(has_event, "Should have at least one event record");
        assert!(has_link, "Should have at least one link record");
    }

    #[test]
    fn test_flatten_span_record_without_events_and_links() {
        // Test that span records without events/links still produce a record
        let span = Span {
            trace_id: sample_trace_id(),
            span_id: sample_span_id(),
            trace_state: "".to_string(),
            parent_span_id: vec![],
            flags: 0,
            name: "simple-span".to_string(),
            kind: 1, // INTERNAL
            start_time_unix_nano: 1640995200000000000,
            end_time_unix_nano: 1640995201000000000,
            attributes: vec![],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: None,
        };

        let result = flatten_span_record(&span);

        assert_eq!(
            result.len(),
            1,
            "Should have exactly one record for span without events/links"
        );

        let record = &result[0];
        assert_eq!(
            record.get("span_name").unwrap(),
            &Value::String("simple-span".to_string()),
            "Should contain span name"
        );
        assert!(
            !record.contains_key("event_name"),
            "Should not contain event fields"
        );
        assert!(
            !record.contains_key("link_trace_id"),
            "Should not contain link fields"
        );
        assert!(
            !record.contains_key("span_status_code"),
            "Should not contain status when none provided"
        );
    }

    #[test]
    fn test_hex_encoding_consistency() {
        // Test that trace and span IDs are consistently hex encoded
        let trace_id = vec![0xFF, 0xAB, 0xCD, 0xEF];
        let span_id = vec![0x12, 0x34, 0x56, 0x78];

        let span = Span {
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
            trace_state: "".to_string(),
            parent_span_id: vec![0x87, 0x65, 0x43, 0x21],
            flags: 0,
            name: "hex-test".to_string(),
            kind: 0,
            start_time_unix_nano: 0,
            end_time_unix_nano: 0,
            attributes: vec![],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![Link {
                trace_id: trace_id.clone(),
                span_id: span_id.clone(),
                trace_state: "".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
            }],
            dropped_links_count: 0,
            status: None,
        };

        let result = flatten_span_record(&span);

        for record in &result {
            if let Some(Value::String(hex_trace_id)) = record.get("span_trace_id") {
                assert_eq!(hex_trace_id, "ffabcdef", "Trace ID should be lowercase hex");
            }
            if let Some(Value::String(hex_span_id)) = record.get("span_span_id") {
                assert_eq!(hex_span_id, "12345678", "Span ID should be lowercase hex");
            }
            if let Some(Value::String(hex_parent_span_id)) = record.get("span_parent_span_id") {
                assert_eq!(
                    hex_parent_span_id, "87654321",
                    "Parent span ID should be lowercase hex"
                );
            }
            if let Some(Value::String(link_trace_id)) = record.get("link_trace_id") {
                assert_eq!(
                    link_trace_id, "ffabcdef",
                    "Link trace ID should be lowercase hex"
                );
            }
        }
    }

    #[test]
    fn test_flatten_otel_traces_complete_structure() {
        // Test the complete flattening of a TracesData structure
        let traces_data = TracesData {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "deployment.environment".to_string(),
                        value: Some(AnyValue {
                            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("production".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                        name: "test-tracer".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    spans: vec![Span {
                        trace_id: sample_trace_id(),
                        span_id: sample_span_id(),
                        trace_state: "".to_string(),
                        parent_span_id: vec![],
                        flags: 0,
                        name: "integration-test-span".to_string(),
                        kind: 3, // CLIENT
                        start_time_unix_nano: 1640995200000000000,
                        end_time_unix_nano: 1640995201000000000,
                        attributes: sample_attributes(),
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: "Success".to_string(),
                            code: 1,
                        }),
                    }],
                    schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
                }],
                schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
            }],
        };

        let result = flatten_otel_traces(&traces_data);

        assert_eq!(result.len(), 1, "Should have one flattened record");

        let record = result[0].as_object().unwrap();

        // Verify resource-level fields
        assert_eq!(
            record.get("deployment.environment").unwrap(),
            &Value::String("production".to_string()),
            "Should contain resource attributes"
        );
        assert_eq!(
            record.get("resource_schema_url").unwrap(),
            &Value::String("https://opentelemetry.io/schemas/1.21.0".to_string()),
            "Should contain resource schema URL"
        );

        // Verify scope-level fields
        assert_eq!(
            record.get("scope_name").unwrap(),
            &Value::String("test-tracer".to_string()),
            "Should contain scope name"
        );
        assert_eq!(
            record.get("scope_version").unwrap(),
            &Value::String("1.0.0".to_string()),
            "Should contain scope version"
        );
        assert_eq!(
            record.get("scope_schema_url").unwrap(),
            &Value::String("https://opentelemetry.io/schemas/1.21.0".to_string()),
            "Should contain scope schema URL"
        );

        // Verify span-level fields
        assert_eq!(
            record.get("span_name").unwrap(),
            &Value::String("integration-test-span".to_string()),
            "Should contain span name"
        );
        assert_eq!(
            record.get("span_kind_description").unwrap(),
            &Value::String("SPAN_KIND_CLIENT".to_string()),
            "Should contain span kind description"
        );
        assert_eq!(
            record.get("service.name").unwrap(),
            &Value::String("test-service".to_string()),
            "Should contain span attributes"
        );
        assert_eq!(
            record.get("span_status_description").unwrap(),
            &Value::String("STATUS_CODE_OK".to_string()),
            "Should contain status description"
        );
    }

    #[test]
    fn test_known_field_list_completeness() {
        // Test that the OTEL_TRACES_KNOWN_FIELD_LIST contains all expected fields
        let expected_fields = [
            "scope_name",
            "scope_version",
            "scope_schema_url",
            "scope_dropped_attributes_count",
            "resource_schema_url",
            "resource_dropped_attributes_count",
            "span_trace_id",
            "span_span_id",
            "span_name",
            "span_parent_span_id",
            "name",
            "span_kind",
            "span_kind_description",
            "span_start_time_unix_nano",
            "span_end_time_unix_nano",
            "event_name",
            "event_time_unix_nano",
            "event_dropped_attributes_count",
            "link_span_id",
            "link_trace_id",
            "link_dropped_attributes_count",
            "span_dropped_events_count",
            "span_dropped_links_count",
            "span_dropped_attributes_count",
            "span_trace_state",
            "span_flags",
            "span_flags_description",
            "span_status_code",
            "span_status_description",
            "span_status_message",
        ];

        assert_eq!(
            OTEL_TRACES_KNOWN_FIELD_LIST.len(),
            expected_fields.len(),
            "Known field list should have correct length"
        );

        for field in &expected_fields {
            assert!(
                OTEL_TRACES_KNOWN_FIELD_LIST.contains(field),
                "Known field list should contain {}",
                field
            );
        }
    }
}
