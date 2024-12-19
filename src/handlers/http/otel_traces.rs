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
use std::collections::BTreeMap;

use super::otel::flatten_attributes;
use super::otel::proto::trace::v1::span::Event;
use super::otel::proto::trace::v1::span::Link;
use super::otel::proto::trace::v1::Span;
use super::otel::proto::trace::v1::Status;
use super::otel::proto::trace::v1::TracesData;

//flatten otel traces
pub fn flatten_otel_traces(body: &Bytes) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let body_str = std::str::from_utf8(body).unwrap();
    let message: TracesData = serde_json::from_str(body_str).unwrap();

    if let Some(records) = message.resource_spans.as_ref() {
        let mut vec_resource_spans_json: Vec<BTreeMap<String, Value>> = Vec::new();
        for record in records.iter() {
            let mut resource_span_json: BTreeMap<String, Value> = BTreeMap::new();
            if let Some(resource) = record.resource.as_ref() {
                //flatten resource attributes to create multiple key value pairs (headers) for each trace record
                if let Some(attributes) = resource.attributes.as_ref() {
                    let attributes_json = flatten_attributes(attributes);
                    for key in attributes_json.keys() {
                        resource_span_json.insert(key.to_owned(), attributes_json[key].to_owned());
                    }
                }

                if resource.dropped_attributes_count.is_some() {
                    resource_span_json.insert(
                        "resource_dropped_attributes_count".to_string(),
                        Value::Number(serde_json::Number::from(
                            resource.dropped_attributes_count.unwrap(),
                        )),
                    );
                }
            }

            if let Some(scope_spans) = record.scope_spans.as_ref() {
                let mut vec_scope_span_json: Vec<BTreeMap<String, Value>> = Vec::new();
                //create flattened record for each scope span
                for scope_span in scope_spans.iter() {
                    let mut scope_span_json = BTreeMap::new();
                    if let Some(spans) = scope_span.spans.as_ref() {
                        //create flattened record for each span record
                        for span in spans.iter() {
                            let span_record_json = flatten_span_record(span);

                            for span_json in span_record_json.iter() {
                                vec_scope_span_json.push(span_json.to_owned());
                            }
                        }
                    }
                    if scope_span.scope.is_some() {
                        let instrumentation_scope = scope_span.scope.as_ref().unwrap();
                        if instrumentation_scope.name.is_some() {
                            scope_span_json.insert(
                                "scope_name".to_string(),
                                Value::String(
                                    instrumentation_scope.name.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        if instrumentation_scope.version.is_some() {
                            scope_span_json.insert(
                                "scope_version".to_string(),
                                Value::String(
                                    instrumentation_scope.version.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                        //flatten instrumentation scope attributes to create multiple key value pairs (headers) for each span record
                        if let Some(attributes) = instrumentation_scope.attributes.as_ref() {
                            let attributes_json = flatten_attributes(attributes);
                            for key in attributes_json.keys() {
                                scope_span_json
                                    .insert(key.to_owned(), attributes_json[key].to_owned());
                            }
                        }

                        if instrumentation_scope.dropped_attributes_count.is_some() {
                            scope_span_json.insert(
                                "scope_dropped_attributes_count".to_string(),
                                Value::Number(serde_json::Number::from(
                                    instrumentation_scope.dropped_attributes_count.unwrap(),
                                )),
                            );
                        }
                        for span_json in vec_scope_span_json.iter_mut() {
                            for key in scope_span_json.keys() {
                                span_json.insert(key.to_owned(), scope_span_json[key].to_owned());
                            }
                        }
                    }
                    if scope_span.schema_url.is_some() {
                        for scope_span_json in vec_scope_span_json.iter_mut() {
                            scope_span_json.insert(
                                "schema_url".to_string(),
                                Value::String(scope_span.schema_url.as_ref().unwrap().to_string()),
                            );
                        }
                    }
                }
                for scope_span_json in vec_scope_span_json.iter() {
                    vec_resource_spans_json.push(scope_span_json.clone());
                }
            }

            if record.schema_url.is_some() {
                resource_span_json.insert(
                    "resource_span_schema_url".to_string(),
                    Value::String(record.schema_url.as_ref().unwrap().to_string()),
                );
            }

            for resource_spans_json in vec_resource_spans_json.iter_mut() {
                for key in resource_span_json.keys() {
                    resource_spans_json.insert(key.to_owned(), resource_span_json[key].to_owned());
                }
            }
        }
        for resource_spans_json in vec_resource_spans_json.iter() {
            vec_otel_json.push(resource_spans_json.clone());
        }
    }

    vec_otel_json
}

//flatten events records and all its attributes to create one record for each event
fn flatten_events(events: &[Event]) -> Vec<BTreeMap<String, Value>> {
    let mut events_json: Vec<BTreeMap<String, Value>> = Vec::new();
    for event in events.iter() {
        let mut event_json = BTreeMap::new();
        if event.time_unix_nano.is_some() {
            event_json.insert(
                "event_time_unix_nano".to_string(),
                Value::Number(serde_json::Number::from(event.time_unix_nano.unwrap())),
            );
        }

        if event.name.is_some() {
            event_json.insert(
                "event_name".to_string(),
                Value::String(event.name.as_ref().unwrap().to_string()),
            );
        }

        if let Some(attributes) = event.attributes.as_ref() {
            let attributes_json = flatten_attributes(attributes);
            for key in attributes_json.keys() {
                event_json.insert(
                    format!("event_{}", key.to_owned()),
                    attributes_json[key].to_owned(),
                );
            }
        }

        if event.dropped_attributes_count.is_some() {
            event_json.insert(
                "event_dropped_attributes_count".to_string(),
                Value::Number(serde_json::Number::from(
                    event.dropped_attributes_count.unwrap(),
                )),
            );
        }

        events_json.push(event_json);
    }
    events_json
}

//flatten links and all its attributes to create one record for each link
fn flatten_links(links: &[Link]) -> Vec<BTreeMap<String, Value>> {
    let mut links_json = Vec::new();
    for link in links.iter() {
        let mut link_json = BTreeMap::new();
        if link.trace_id.is_some() {
            link_json.insert(
                "link_trace_id".to_string(),
                Value::String(link.trace_id.as_ref().unwrap().to_string()),
            );
        }

        if link.span_id.is_some() {
            link_json.insert(
                "link_span_id".to_string(),
                Value::String(link.span_id.as_ref().unwrap().to_string()),
            );
        }

        if let Some(attributes) = link.attributes.as_ref() {
            let attributes_json = flatten_attributes(attributes);
            for key in attributes_json.keys() {
                link_json.insert(
                    format!("link_{}", key.to_owned()),
                    attributes_json[key].to_owned(),
                );
            }
        }

        if link.dropped_attributes_count.is_some() {
            link_json.insert(
                "link_dropped_attributes_count".to_string(),
                Value::Number(serde_json::Number::from(
                    link.dropped_attributes_count.unwrap(),
                )),
            );
        }

        links_json.push(link_json);
    }
    links_json
}

//flatten span records and all its attributes to create one record for each span
fn flatten_span_record(span_record: &Span) -> Vec<BTreeMap<String, Value>> {
    let mut span_records_json: Vec<BTreeMap<String, Value>> = Vec::new();

    if let Some(events) = span_record.events.as_ref() {
        let events_json = flatten_events(events);
        for event_json in events_json.iter() {
            span_records_json.push(event_json.to_owned());
        }
    }

    let mut span_record_json = BTreeMap::new();

    if span_record.trace_id.is_some() {
        span_record_json.insert(
            "span_trace_id".to_string(),
            Value::String(span_record.trace_id.as_ref().unwrap().to_string()),
        );
    }

    if span_record.span_id.is_some() {
        span_record_json.insert(
            "span_span_id".to_string(),
            Value::String(span_record.span_id.as_ref().unwrap().to_string()),
        );
    }

    if span_record.trace_state.is_some() {
        span_record_json.insert(
            "span_trace_state".to_string(),
            Value::String(span_record.trace_state.as_ref().unwrap().to_string()),
        );
    }

    if span_record.parent_span_id.is_some() {
        span_record_json.insert(
            "span_parent_span_id".to_string(),
            Value::String(span_record.parent_span_id.as_ref().unwrap().to_string()),
        );
    }

    let flags_json = flatten_flags(&span_record.flags);
    for key in flags_json.keys() {
        span_record_json.insert(key.to_owned(), flags_json[key].to_owned());
    }

    if span_record.name.is_some() {
        span_record_json.insert(
            "span_name".to_string(),
            Value::String(span_record.name.as_ref().unwrap().to_string()),
        );
    }

    let kind_json = flatten_kind(&span_record.kind);
    for key in kind_json.keys() {
        span_record_json.insert(key.to_owned(), kind_json[key].to_owned());
    }

    if span_record.start_time_unix_nano.is_some() {
        span_record_json.insert(
            "span_start_time_unix_nano".to_string(),
            Value::String(
                span_record
                    .start_time_unix_nano
                    .as_ref()
                    .unwrap()
                    .to_string(),
            ),
        );
    }

    if span_record.end_time_unix_nano.is_some() {
        span_record_json.insert(
            "span_end_time_unix_nano".to_string(),
            Value::String(span_record.end_time_unix_nano.as_ref().unwrap().to_string()),
        );
    }

    if let Some(attributes) = span_record.attributes.as_ref() {
        let attributes_json = flatten_attributes(attributes);
        for key in attributes_json.keys() {
            span_record_json.insert(key.to_owned(), attributes_json[key].to_owned());
        }
    }

    if span_record.dropped_attributes_count.is_some() {
        span_record_json.insert(
            "span_dropped_attributes_count".to_string(),
            Value::Number(serde_json::Number::from(
                span_record.dropped_attributes_count.unwrap(),
            )),
        );
    }

    if let Some(links) = span_record.links.as_ref() {
        let links_json = flatten_links(links);
        for link_json in links_json.iter() {
            span_records_json.push(link_json.to_owned());
        }
    }

    if span_record.dropped_links_count.is_some() {
        span_record_json.insert(
            "span_dropped_links_count".to_string(),
            Value::Number(serde_json::Number::from(
                span_record.dropped_links_count.unwrap(),
            )),
        );
    }

    if let Some(status) = span_record.status.as_ref() {
        let status_json = flatten_status(status);
        for key in status_json.keys() {
            span_record_json.insert(key.to_owned(), status_json[key].to_owned());
        }
    }

    for span_json in span_records_json.iter_mut() {
        for key in span_record_json.keys() {
            span_json.insert(key.to_owned(), span_record_json[key].to_owned());
        }
    }
    span_records_json
}

//flatten status object
fn flatten_status(status: &Status) -> BTreeMap<String, Value> {
    let mut status_json = BTreeMap::new();
    if status.message.is_some() {
        status_json.insert(
            "span_status_message".to_string(),
            Value::String(status.message.as_ref().unwrap().to_string()),
        );
    }

    if status.code.is_some() {
        status_json.insert(
            "span_status_code".to_string(),
            Value::Number(serde_json::Number::from(status.code.unwrap())),
        );

        match status.code {
            Some(0) => {
                status_json.insert(
                    "span_status_description".to_string(),
                    Value::String("STATUS_CODE_UNSET".to_string()),
                );
            }
            Some(1) => {
                status_json.insert(
                    "span_status_description".to_string(),
                    Value::String("STATUS_CODE_OK".to_string()),
                );
            }
            Some(2) => {
                status_json.insert(
                    "span_status_description".to_string(),
                    Value::String("STATUS_CODE_ERROR".to_string()),
                );
            }
            _ => {}
        }
    }
    status_json
}

//flatten flags object
fn flatten_flags(flags: &Option<u32>) -> BTreeMap<String, Value> {
    let mut flags_json = BTreeMap::new();
    if flags.is_some() {
        flags_json.insert(
            "span_flags".to_string(),
            Value::Number(serde_json::Number::from(flags.unwrap())),
        );

        match flags {
            Some(0) => {
                flags_json.insert(
                    "span_flags_description".to_string(),
                    Value::String("SPAN_FLAGS_DO_NOT_USE".to_string()),
                );
            }
            Some(255) => {
                flags_json.insert(
                    "span_flags_description".to_string(),
                    Value::String("SPAN_FLAGS_TRACE_FLAGS_MASK".to_string()),
                );
            }
            Some(256) => {
                flags_json.insert(
                    "span_flags_description".to_string(),
                    Value::String("SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK".to_string()),
                );
            }
            Some(512) => {
                flags_json.insert(
                    "span_flags_description".to_string(),
                    Value::String("SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK".to_string()),
                );
            }
            _ => {}
        }
    }

    flags_json
}

//flatten kind object
fn flatten_kind(kind: &Option<i32>) -> BTreeMap<String, Value> {
    let mut kind_json = BTreeMap::new();
    if kind.is_some() {
        kind_json.insert(
            "span_kind".to_string(),
            Value::Number(serde_json::Number::from(kind.unwrap())),
        );

        match kind {
            Some(0) => {
                kind_json.insert(
                    "span_kind_description".to_string(),
                    Value::String("SPAN_KIND_UNSPECIFIED".to_string()),
                );
            }
            Some(1) => {
                kind_json.insert(
                    "span_kind_description".to_string(),
                    Value::String("SPAN_KIND_INTERNAL".to_string()),
                );
            }
            Some(2) => {
                kind_json.insert(
                    "span_kind_description".to_string(),
                    Value::String("SPAN_KIND_SERVER".to_string()),
                );
            }
            Some(3) => {
                kind_json.insert(
                    "span_kind_description".to_string(),
                    Value::String("SPAN_KIND_CLIENT".to_string()),
                );
            }
            Some(4) => {
                kind_json.insert(
                    "span_kind_description".to_string(),
                    Value::String("SPAN_KIND_PRODUCER".to_string()),
                );
            }
            Some(5) => {
                kind_json.insert(
                    "span_kind_description".to_string(),
                    Value::String("SPAN_KIND_CONSUMER".to_string()),
                );
            }
            _ => {}
        }
    }
    kind_json
}
