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
use opentelemetry_proto::tonic::trace::v1::span::Event;
use opentelemetry_proto::tonic::trace::v1::span::Link;
use opentelemetry_proto::tonic::trace::v1::ScopeSpans;
use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_proto::tonic::trace::v1::Status;
use opentelemetry_proto::tonic::trace::v1::TracesData;
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
