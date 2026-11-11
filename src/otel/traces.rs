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
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::trace::v1::ScopeSpans;
use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_proto::tonic::trace::v1::Status;
use opentelemetry_proto::tonic::trace::v1::TracesData;
use opentelemetry_proto::tonic::trace::v1::span::Event;
use opentelemetry_proto::tonic::trace::v1::span::Link;
use serde_json::{Map, Value};

use crate::metrics::increment_traces_collected_by_date;
use crate::otel::otel_utils::flatten_attributes;

use super::otel_utils::convert_epoch_nano_to_timestamp;
use super::otel_utils::insert_attributes;

pub const OTEL_TRACES_KNOWN_FIELD_LIST: [&str; 32] = [
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
    "span_duration_ns",
    "event_name",
    "event_time_unix_nano",
    "event_duration_ns",
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
fn flatten_scope_span(scope_span: &ScopeSpans, tenant_id: &str) -> Vec<Map<String, Value>> {
    let mut vec_scope_span_json = Vec::new();
    let mut scope_span_json = Map::new();
    for span in &scope_span.spans {
        let span_record_json = flatten_span_record(span);
        vec_scope_span_json.extend(span_record_json);
    }

    let date = chrono::Utc::now().date_naive().to_string();
    increment_traces_collected_by_date(scope_span.spans.len() as u64, &date, tenant_id);

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

/// Common function to process resource spans and merge resource-level fields
fn process_resource_spans<T>(
    resource_spans: &[T],
    get_resource: fn(&T) -> Option<&opentelemetry_proto::tonic::resource::v1::Resource>,
    get_scope_spans: fn(&T) -> &[ScopeSpans],
    get_schema_url: fn(&T) -> &str,
    tenant_id: &str,
) -> Vec<Value>
where
    T: std::fmt::Debug,
{
    let mut vec_otel_json = Vec::new();

    for resource_span in resource_spans {
        let mut resource_span_json = Map::new();

        // Process resource attributes if present
        if let Some(resource) = get_resource(resource_span) {
            insert_attributes(&mut resource_span_json, &resource.attributes);
            resource_span_json.insert(
                "resource_dropped_attributes_count".to_string(),
                Value::Number(resource.dropped_attributes_count.into()),
            );
        }

        // Process scope spans
        let mut vec_resource_spans_json = Vec::new();
        for scope_span in get_scope_spans(resource_span) {
            let scope_span_json = flatten_scope_span(scope_span, tenant_id);
            vec_resource_spans_json.extend(scope_span_json);
        }

        // Add resource schema URL
        resource_span_json.insert(
            "resource_schema_url".to_string(),
            Value::String(get_schema_url(resource_span).to_string()),
        );

        // Merge resource-level fields into each span record
        for resource_spans_json in &mut vec_resource_spans_json {
            resource_spans_json.extend(resource_span_json.clone());
            vec_otel_json.push(Value::Object(resource_spans_json.clone()));
        }
    }

    vec_otel_json
}

/// Flattens OpenTelemetry traces from protobuf format
pub fn flatten_otel_traces_protobuf(
    message: &ExportTraceServiceRequest,
    tenant_id: &str,
) -> Vec<Value> {
    process_resource_spans(
        &message.resource_spans,
        |rs| rs.resource.as_ref(),
        |rs| &rs.scope_spans,
        |rs| &rs.schema_url,
        tenant_id,
    )
}

/// this function performs the custom flattening of the otel traces event
/// and returns a `Vec` of `Value::Object` of the flattened json
pub fn flatten_otel_traces(message: &TracesData, tenant_id: &str) -> Vec<Value> {
    process_resource_spans(
        &message.resource_spans,
        |rs| rs.resource.as_ref(),
        |rs| &rs.scope_spans,
        |rs| &rs.schema_url,
        tenant_id,
    )
}

/// otel traces has json array of events
/// this function flattens the `Event` object
/// and returns a `Vec` of `Map` of the flattened json
fn flatten_events(events: &[Event], span_start_time_unix_nano: u64) -> Vec<Map<String, Value>> {
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
            // add epoch value of event time in json
            event_json.insert(
                "event_time_unix_nano_epoch".to_string(),
                Value::Number(serde_json::Number::from(event.time_unix_nano)),
            );
            event_json.insert("event_name".to_string(), Value::String(event.name.clone()));

            // Calculate event duration in nanoseconds from span start
            let duration_nanos = event
                .time_unix_nano
                .saturating_sub(span_start_time_unix_nano);
            event_json.insert(
                "event_duration_ns".to_string(),
                Value::Number(serde_json::Number::from(duration_nanos)),
            );

            insert_events_attributes(&mut event_json, &event.attributes);
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
            let mut link_json = Map::from_iter([
                ("link_span_id".into(), hex::encode(&link.span_id).into()),
                ("link_trace_id".into(), hex::encode(&link.trace_id).into()),
            ]);

            insert_links_attributes(&mut link_json, &link.attributes);
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
    let description = match status.code {
        0 => "UNSET",
        1 => "OK",
        2 => "ERROR",
        _ => "",
    };
    Map::from_iter([
        ("span_status_message".into(), status.message.clone().into()),
        ("span_status_code".into(), status.code.into()),
        ("span_status_description".into(), description.into()),
    ])
}

/// otel log event has flags
/// there is a mapping of flags to flags description provided in proto
/// this function fetches the flags description from the flags
/// and adds it to the flattened json
fn flatten_flags(flags: u32) -> Map<String, Value> {
    let description = match flags {
        0 => "DO_NOT_USE",
        255 => "TRACE_FLAGS_MASK",
        256 => "CONTEXT_HAS_IS_REMOTE_MASK",
        512 => "CONTEXT_IS_REMOTE_MASK",
        _ => "",
    };
    Map::from_iter([
        ("span_flags".into(), flags.into()),
        ("span_flags_description".into(), description.into()),
    ])
}

/// otel span event has kind
/// there is a mapping of kind to kind description provided in proto
/// this function fetches the kind description from the kind
/// and adds it to the flattened json
fn flatten_kind(kind: i32) -> Map<String, Value> {
    let description = match kind {
        0 => "UNSPECIFIED",
        1 => "INTERNAL",
        2 => "SERVER",
        3 => "CLIENT",
        4 => "PRODUCER",
        5 => "CONSUMER",
        _ => "",
    };
    Map::from_iter([
        ("span_kind".into(), kind.into()),
        ("span_kind_description".into(), description.into()),
    ])
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
    // add epoch value of start time in json
    span_record_json.insert(
        "span_start_time_unix_nano_epoch".to_string(),
        Value::Number(serde_json::Number::from(span_record.start_time_unix_nano)),
    );
    span_record_json.insert(
        "span_end_time_unix_nano".to_string(),
        Value::String(convert_epoch_nano_to_timestamp(
            span_record.end_time_unix_nano as i64,
        )),
    );
    // add epoch value of end time in json
    span_record_json.insert(
        "span_end_time_unix_nano_epoch".to_string(),
        Value::Number(serde_json::Number::from(span_record.end_time_unix_nano)),
    );

    // Calculate span duration in nanoseconds
    let duration_nanos = span_record
        .end_time_unix_nano
        .saturating_sub(span_record.start_time_unix_nano);
    span_record_json.insert(
        "span_duration_ns".to_string(),
        Value::Number(serde_json::Number::from(duration_nanos)),
    );

    insert_attributes(&mut span_record_json, &span_record.attributes);
    span_record_json.insert(
        "span_dropped_attributes_count".to_string(),
        Value::Number(span_record.dropped_attributes_count.into()),
    );
    let events_json = flatten_events(&span_record.events, span_record.start_time_unix_nano);
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

pub fn insert_events_attributes(map: &mut Map<String, Value>, attributes: &[KeyValue]) {
    let attributes_json = flatten_attributes(attributes);
    for (key, value) in attributes_json {
        map.insert(format!("event_{}", key), value);
    }
}

pub fn insert_links_attributes(map: &mut Map<String, Value>, attributes: &[KeyValue]) {
    let attributes_json = flatten_attributes(attributes);
    for (key, value) in attributes_json {
        map.insert(format!("link_{}", key), value);
    }
}

#[cfg(test)]
mod tests {
    use crate::parseable::DEFAULT_TENANT;

    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, EntityRef, KeyValue};
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
                key_strindex: 0,
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
                key_strindex: 0,
            },
        ]
    }

    #[test]
    fn test_flatten_status_code_mapping() {
        // Test that status codes are correctly mapped to descriptions
        let test_cases = vec![
            (0, "UNSET"),
            (1, "OK"),
            (2, "ERROR"),
            (999, ""), // Unknown status code should return empty string
        ];

        for (code, expected_description) in test_cases {
            let status = Status {
                message: "Test message".to_string(),
                code,
            };

            let result = flatten_status(&status);

            assert_eq!(result["span_status_code"], serde_json::json!(code));
            assert_eq!(
                result["span_status_description"],
                serde_json::json!(expected_description)
            );
            assert_eq!(
                result["span_status_message"],
                serde_json::json!("Test message")
            );
        }
    }

    #[test]
    fn test_flatten_span_kind_mapping() {
        // Test that span kinds are correctly mapped to descriptions
        let test_cases = vec![
            (0, "UNSPECIFIED"),
            (1, "INTERNAL"),
            (2, "SERVER"),
            (3, "CLIENT"),
            (4, "PRODUCER"),
            (5, "CONSUMER"),
            (999, ""), // Unknown kind should return empty string
        ];

        for (kind, expected_description) in test_cases {
            let result = flatten_kind(kind);

            assert_eq!(result["span_kind"], serde_json::json!(kind));
            assert_eq!(
                result["span_kind_description"],
                serde_json::json!(expected_description)
            );
        }
    }

    #[test]
    fn test_flatten_flags_mapping() {
        // Test that flags are correctly mapped to descriptions
        let test_cases = vec![
            (0, "DO_NOT_USE"),
            (255, "TRACE_FLAGS_MASK"),
            (256, "CONTEXT_HAS_IS_REMOTE_MASK"),
            (512, "CONTEXT_IS_REMOTE_MASK"),
            (999, ""), // Unknown flag should return empty string
        ];

        for (flags, expected_description) in test_cases {
            let result = flatten_flags(flags);

            assert_eq!(result["span_flags"], serde_json::json!(flags));
            assert_eq!(
                result["span_flags_description"],
                serde_json::json!(expected_description)
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
                ..Default::default()
            },
        ];

        let result = flatten_events(&events, 1640995200000000000);

        assert_eq!(result.len(), 2, "Should have two flattened events");

        // Check first event
        let first_event = &result[0];
        assert!(first_event.contains_key("event_time_unix_nano"));
        assert_eq!(
            first_event["event_name"],
            serde_json::json!("request.start")
        );
        assert_eq!(
            first_event["event_dropped_attributes_count"],
            serde_json::json!(2)
        );
        assert!(first_event.contains_key("event_service.name"));

        // Check second event
        let second_event = &result[1];
        assert_eq!(second_event["event_name"], serde_json::json!("request.end"));
        assert_eq!(
            second_event["event_dropped_attributes_count"],
            serde_json::json!(0)
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
            link["link_trace_id"],
            serde_json::json!("0102030405060708090a0b0c0d0e0f10")
        );
        assert_eq!(link["link_span_id"], serde_json::json!("0102030405060708"));
        assert_eq!(link["link_dropped_attributes_count"], serde_json::json!(1));
        assert!(link.contains_key("link_service.name"));
    }

    #[test]
    fn test_flatten_span_record_with_events_and_links() {
        // Test that span records with events and links are properly flattened
        let span = Span {
            trace_id: sample_trace_id(),
            span_id: sample_span_id(),
            trace_state: "test-state".to_string(),
            name: "test-span".to_string(),
            kind: 2, // SERVER
            start_time_unix_nano: 1640995200000000000,
            end_time_unix_nano: 1640995201000000000,
            attributes: sample_attributes(),
            events: vec![Event {
                time_unix_nano: 1640995200500000000,
                name: "middleware".to_string(),
                ..Default::default()
            }],
            links: vec![Link {
                trace_id: sample_trace_id(),
                span_id: sample_span_id(),
                ..Default::default()
            }],
            status: Some(Status {
                message: "OK".to_string(),
                code: 1,
            }),
            ..Default::default()
        };

        let result = flatten_span_record(&span);

        // Should have 2 records: one for event and one for link
        assert_eq!(result.len(), 2, "Should have records for event and link");

        // Both records should contain span-level information
        for record in &result {
            assert_eq!(record["span_name"], serde_json::json!("test-span"));
            assert_eq!(record["span_kind"], serde_json::json!(2));
            assert_eq!(record["span_kind_description"], serde_json::json!("SERVER"));
            assert!(record.contains_key("span_trace_id"));
            assert!(record.contains_key("span_span_id"));
            assert!(record.contains_key("span_start_time_unix_nano"));
            assert!(record.contains_key("span_end_time_unix_nano"));
            assert!(record.contains_key("service.name"));
            assert!(record.contains_key("span_status_code"));
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
            name: "simple-span".to_string(),
            kind: 1, // INTERNAL
            start_time_unix_nano: 1640995200000000000,
            end_time_unix_nano: 1640995201000000000,
            ..Default::default()
        };

        let result = flatten_span_record(&span);

        assert_eq!(
            result.len(),
            1,
            "Should have exactly one record for span without events/links"
        );

        let record = &result[0];
        assert_eq!(record["span_name"], serde_json::json!("simple-span"));
        assert!(!record.contains_key("event_name"));
        assert!(!record.contains_key("link_trace_id"));
        assert!(!record.contains_key("span_status_code"));
    }

    #[test]
    fn test_hex_encoding_consistency() {
        // Test that trace and span IDs are consistently hex encoded
        let trace_id = vec![0xFF, 0xAB, 0xCD, 0xEF];
        let span_id = vec![0x12, 0x34, 0x56, 0x78];

        let span = Span {
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
            parent_span_id: vec![0x87, 0x65, 0x43, 0x21],
            name: "hex-test".to_string(),
            links: vec![Link {
                trace_id: trace_id.clone(),
                span_id: span_id.clone(),
                ..Default::default()
            }],
            ..Default::default()
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
                            key_strindex: 0,
                        }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![
                        EntityRef{
                            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
                            r#type: "service".to_string(),
                            id_keys: vec!["service.name".to_string()],
                            description_keys: vec!["service.name".to_string()],
                        }
                    ]
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                        name: "test-tracer".to_string(),
                        version: "1.0.0".to_string(),
..Default::default()
}),
                    spans: vec![Span {
                        trace_id: sample_trace_id(),
                        span_id: sample_span_id(),
                        name: "integration-test-span".to_string(),
                        kind: 3, // CLIENT
                        start_time_unix_nano: 1640995200000000000,
                        end_time_unix_nano: 1640995201000000000,
                        attributes: sample_attributes(),
                        status: Some(Status {
                            message: "Success".to_string(),
                            code: 1,
                        }),
..Default::default()
}],
                    schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
                }],
                schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
            }],
        };

        let result = flatten_otel_traces(&traces_data, DEFAULT_TENANT);

        assert_eq!(result.len(), 1, "Should have one flattened record");

        let record = result[0].as_object().unwrap();

        // Verify resource-level fields
        assert_eq!(
            record["deployment.environment"],
            serde_json::json!("production")
        );
        assert_eq!(
            record["resource_schema_url"],
            serde_json::json!("https://opentelemetry.io/schemas/1.21.0")
        );

        // Verify scope-level fields
        assert_eq!(record["scope_name"], serde_json::json!("test-tracer"));
        assert_eq!(record["scope_version"], serde_json::json!("1.0.0"));
        assert_eq!(
            record["scope_schema_url"],
            serde_json::json!("https://opentelemetry.io/schemas/1.21.0")
        );

        // Verify span-level fields
        assert_eq!(
            record["span_name"],
            serde_json::json!("integration-test-span")
        );
        assert_eq!(record["span_kind_description"], serde_json::json!("CLIENT"));
        assert_eq!(record["service.name"], serde_json::json!("test-service"));
        assert_eq!(record["span_status_description"], serde_json::json!("OK"));
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
            "event_duration_ns",
            "link_span_id",
            "link_trace_id",
            "link_dropped_attributes_count",
            "span_dropped_events_count",
            "span_dropped_links_count",
            "span_dropped_attributes_count",
            "span_duration_ns",
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

    #[test]
    fn test_flatten_events_empty_slice() {
        // flatten_events on an empty slice must return an empty Vec —
        // the caller (flatten_span_record) relies on this to detect
        // "no events" and fall back to a single span-only record.
        let result = flatten_events(&[], 1640995200000000000);
        assert!(
            result.is_empty(),
            "flatten_events on an empty slice should return an empty Vec"
        );
    }

    #[test]
    fn test_flatten_links_empty_slice() {
        // flatten_links on an empty slice must return an empty Vec —
        // the caller (flatten_span_record) relies on this to detect
        // "no links" and fall back to a single span-only record.
        let result = flatten_links(&[]);
        assert!(
            result.is_empty(),
            "flatten_links on an empty slice should return an empty Vec"
        );
    }

    #[test]
    fn test_flatten_span_record_events_only_no_links() {
        // A span with events but no links should produce one record per event,
        // each enriched with all span-level fields.
        let span = Span {
            trace_id: sample_trace_id(),
            span_id: sample_span_id(),
            name: "events-only-span".to_string(),
            kind: 1,
            start_time_unix_nano: 1640995200000000000,
            end_time_unix_nano: 1640995201000000000,
            events: vec![
                Event {
                    time_unix_nano: 1640995200100000000,
                    name: "evt-a".to_string(),
                    ..Default::default()
                },
                Event {
                    time_unix_nano: 1640995200200000000,
                    name: "evt-b".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let result = flatten_span_record(&span);

        assert_eq!(result.len(), 2, "One record per event when links are empty");
        assert_eq!(result[0]["event_name"], serde_json::json!("evt-a"));
        assert_eq!(result[1]["event_name"], serde_json::json!("evt-b"));
        // Each event record must also carry the span-level fields.
        for record in &result {
            assert_eq!(record["span_name"], serde_json::json!("events-only-span"));
        }
    }

    #[test]
    fn test_flatten_span_record_links_only_no_events() {
        // A span with links but no events should produce one record per link,
        // each enriched with all span-level fields.
        let span = Span {
            trace_id: sample_trace_id(),
            span_id: sample_span_id(),
            name: "links-only-span".to_string(),
            kind: 3,
            start_time_unix_nano: 1640995200000000000,
            end_time_unix_nano: 1640995201000000000,
            links: vec![Link {
                trace_id: sample_trace_id(),
                span_id: sample_span_id(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let result = flatten_span_record(&span);

        assert_eq!(result.len(), 1, "One record per link when events are empty");
        assert!(result[0].contains_key("link_trace_id"));
        assert_eq!(result[0]["span_name"], serde_json::json!("links-only-span"));
    }
}
