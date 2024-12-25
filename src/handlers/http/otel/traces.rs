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

use super::insert_attributes;
use super::insert_if_some;
use super::insert_number_if_some;
use super::proto::trace::v1::span::Event;
use super::proto::trace::v1::span::Link;
use super::proto::trace::v1::ScopeSpans;
use super::proto::trace::v1::Span;
use super::proto::trace::v1::Status;
use super::proto::trace::v1::TracesData;
use bytes::Bytes;

use serde_json::Value;
use std::collections::BTreeMap;

/// this function flattens the `ScopeSpans` object
/// and returns a `Vec` of `BTreeMap` of the flattened json
fn flatten_scope_span(scope_span: &ScopeSpans) -> Vec<BTreeMap<String, Value>> {
    let mut vec_scope_span_json = Vec::new();
    let mut scope_span_json = BTreeMap::new();

    if let Some(spans) = &scope_span.spans {
        for span in spans {
            let span_record_json = flatten_span_record(span);
            vec_scope_span_json.extend(span_record_json);
        }
    }

    if let Some(scope) = &scope_span.scope {
        insert_if_some(&mut scope_span_json, "scope_name", &scope.name);
        insert_if_some(&mut scope_span_json, "scope_version", &scope.version);
        insert_attributes(&mut scope_span_json, &scope.attributes);
        insert_number_if_some(
            &mut scope_span_json,
            "scope_dropped_attributes_count",
            &scope.dropped_attributes_count.map(|f| f as f64),
        );

        for span_json in &mut vec_scope_span_json {
            for (key, value) in &scope_span_json {
                span_json.insert(key.clone(), value.clone());
            }
        }
    }

    if let Some(schema_url) = &scope_span.schema_url {
        for span_json in &mut vec_scope_span_json {
            span_json.insert("schema_url".to_string(), Value::String(schema_url.clone()));
        }
    }

    vec_scope_span_json
}

/// this function performs the custom flattening of the otel traces event
/// and returns a `Vec` of `BTreeMap` of the flattened json
pub fn flatten_otel_traces(body: &Bytes) -> Vec<BTreeMap<String, Value>> {
    let body_str = std::str::from_utf8(body).unwrap();
    let message: TracesData = serde_json::from_str(body_str).unwrap();
    let mut vec_otel_json = Vec::new();

    if let Some(records) = &message.resource_spans {
        for record in records {
            let mut resource_span_json = BTreeMap::new();

            if let Some(resource) = &record.resource {
                insert_attributes(&mut resource_span_json, &resource.attributes);
                insert_number_if_some(
                    &mut resource_span_json,
                    "resource_dropped_attributes_count",
                    &resource.dropped_attributes_count.map(|f| f as f64),
                );
            }

            let mut vec_resource_spans_json = Vec::new();
            if let Some(scope_spans) = &record.scope_spans {
                for scope_span in scope_spans {
                    let scope_span_json = flatten_scope_span(scope_span);
                    vec_resource_spans_json.extend(scope_span_json);
                }
            }

            insert_if_some(
                &mut resource_span_json,
                "resource_span_schema_url",
                &record.schema_url,
            );

            for resource_spans_json in &mut vec_resource_spans_json {
                for (key, value) in &resource_span_json {
                    resource_spans_json.insert(key.clone(), value.clone());
                }
            }

            vec_otel_json.extend(vec_resource_spans_json);
        }
    }

    vec_otel_json
}

/// otel traces has json array of events
/// this function flattens the `Event` object
/// and returns a `Vec` of `BTreeMap` of the flattened json
fn flatten_events(events: &[Event]) -> Vec<BTreeMap<String, Value>> {
    events
        .iter()
        .map(|event| {
            let mut event_json = BTreeMap::new();
            insert_if_some(
                &mut event_json,
                "event_time_unix_nano",
                &event.time_unix_nano,
            );
            insert_if_some(&mut event_json, "event_name", &event.name);
            insert_attributes(&mut event_json, &event.attributes);
            insert_number_if_some(
                &mut event_json,
                "event_dropped_attributes_count",
                &event.dropped_attributes_count.map(|f| f as f64),
            );
            event_json
        })
        .collect()
}

/// otel traces has json array of links
/// this function flattens the `Link` object
/// and returns a `Vec` of `BTreeMap` of the flattened json
fn flatten_links(links: &[Link]) -> Vec<BTreeMap<String, Value>> {
    links
        .iter()
        .map(|link| {
            let mut link_json = BTreeMap::new();
            insert_if_some(&mut link_json, "link_trace_id", &link.trace_id);
            insert_if_some(&mut link_json, "link_span_id", &link.span_id);
            insert_attributes(&mut link_json, &link.attributes);
            insert_number_if_some(
                &mut link_json,
                "link_dropped_attributes_count",
                &link.dropped_attributes_count.map(|f| f as f64),
            );
            link_json
        })
        .collect()
}

/// otel trace event has status
/// there is a mapping of status code to status description provided in proto
/// this function fetches the status description from the status code
/// and adds it to the flattened json
fn flatten_status(status: &Status) -> BTreeMap<String, Value> {
    let mut status_json = BTreeMap::new();
    insert_if_some(&mut status_json, "span_status_message", &status.message);
    insert_number_if_some(
        &mut status_json,
        "span_status_code",
        &status.code.map(|f| f as f64),
    );

    if let Some(code) = status.code {
        let description = match code {
            0 => "STATUS_CODE_UNSET",
            1 => "STATUS_CODE_OK",
            2 => "STATUS_CODE_ERROR",
            _ => "",
        };
        status_json.insert(
            "span_status_description".to_string(),
            Value::String(description.to_string()),
        );
    }

    status_json
}

/// otel log event has flags
/// there is a mapping of flags to flags description provided in proto
/// this function fetches the flags description from the flags
/// and adds it to the flattened json
fn flatten_flags(flags: &Option<u32>) -> BTreeMap<String, Value> {
    let mut flags_json = BTreeMap::new();
    insert_number_if_some(&mut flags_json, "span_flags", &flags.map(|f| f as f64));

    if let Some(flag) = flags {
        let description = match flag {
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
    }

    flags_json
}

/// otel span event has kind
/// there is a mapping of kind to kind description provided in proto
/// this function fetches the kind description from the kind
/// and adds it to the flattened json
fn flatten_kind(kind: &Option<i32>) -> BTreeMap<String, Value> {
    let mut kind_json = BTreeMap::new();
    insert_number_if_some(&mut kind_json, "span_kind", &kind.map(|k| k as f64));

    if let Some(kind) = kind {
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
    }

    kind_json
}

/// this function flattens the `Span` object
/// and returns a `Vec` of `BTreeMap` of the flattened json
/// this function is called recursively for each span record object in the otel traces event
fn flatten_span_record(span_record: &Span) -> Vec<BTreeMap<String, Value>> {
    let mut span_records_json = Vec::new();

    let mut span_record_json = BTreeMap::new();
    insert_if_some(
        &mut span_record_json,
        "span_trace_id",
        &span_record.trace_id,
    );
    insert_if_some(&mut span_record_json, "span_span_id", &span_record.span_id);
    insert_if_some(
        &mut span_record_json,
        "span_trace_state",
        &span_record.trace_state,
    );
    insert_if_some(
        &mut span_record_json,
        "span_parent_span_id",
        &span_record.parent_span_id,
    );
    span_record_json.extend(flatten_flags(&span_record.flags));
    insert_if_some(&mut span_record_json, "span_name", &span_record.name);
    span_record_json.extend(flatten_kind(&span_record.kind));
    insert_if_some(
        &mut span_record_json,
        "span_start_time_unix_nano",
        &span_record.start_time_unix_nano,
    );
    insert_if_some(
        &mut span_record_json,
        "span_end_time_unix_nano",
        &span_record.end_time_unix_nano,
    );
    insert_attributes(&mut span_record_json, &span_record.attributes);
    insert_if_some(
        &mut span_record_json,
        "span_dropped_attributes_count",
        &span_record.dropped_attributes_count,
    );
    if let Some(events) = &span_record.events {
        span_records_json.extend(flatten_events(events));
    }
    insert_number_if_some(
        &mut span_record_json,
        "span_dropped_events_count",
        &span_record.dropped_events_count.map(|f| f as f64),
    );
    if let Some(links) = &span_record.links {
        span_records_json.extend(flatten_links(links));
    }

    insert_number_if_some(
        &mut span_record_json,
        "span_dropped_links_count",
        &span_record.dropped_links_count.map(|f| f as f64),
    );

    if let Some(status) = &span_record.status {
        span_record_json.extend(flatten_status(status));
    }

    for span_json in &mut span_records_json {
        for (key, value) in &span_record_json {
            span_json.insert(key.clone(), value.clone());
        }
    }

    span_records_json
}
