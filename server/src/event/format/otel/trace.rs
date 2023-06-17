use std::collections::BTreeMap;

use super::proto::{common, resource, trace};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SpanData {
    resource: Option<Resource>,
    scope: Option<Scope>,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    trace_state: String,
    parent_span_id: [u8; 8],
    name: String,
    // replace with SpanKind
    kind: i32,
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    attributes: BTreeMap<String, String>,
    dropped_attributes_count: u32,
    events: Vec<Event>,
    dropped_events_count: u32,
    links: Vec<Link>,
    dropped_links_count: u32,
    status: Option<Status>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    attributes: BTreeMap<String, String>,
    dropped_attributes_count: u32,
}

impl From<resource::Resource> for Resource {
    fn from(value: resource::Resource) -> Self {
        let resource::Resource {
            attributes,
            dropped_attributes_count,
        } = value;
        Resource {
            attributes: BTreeMap::from_iter(attributes),
            dropped_attributes_count,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Scope {
    pub name: String,
    pub version: String,
    pub attributes: BTreeMap<String, String>,
    pub dropped_attributes_count: u32,
}

impl From<common::InstrumentationScope> for Scope {
    fn from(value: common::InstrumentationScope) -> Self {
        let common::InstrumentationScope {
            name,
            version,
            attributes,
            dropped_attributes_count,
        } = value;
        Scope {
            name,
            version,
            attributes: BTreeMap::from_iter(attributes),
            dropped_attributes_count,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Event {
    time_unix_nano: u64,
    name: String,
    attributes: BTreeMap<String, String>,
    dropped_attributes_count: u32,
}

impl From<trace::span::Event> for Event {
    fn from(value: trace::span::Event) -> Self {
        let trace::span::Event {
            time_unix_nano,
            name,
            attributes,
            dropped_attributes_count,
        } = value;
        Event {
            time_unix_nano,
            name,
            attributes: BTreeMap::from_iter(attributes),
            dropped_attributes_count,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Link {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_state: String,
    pub attributes: BTreeMap<String, String>,
    pub dropped_attributes_count: u32,
}

impl From<trace::span::Link> for Link {
    fn from(value: trace::span::Link) -> Self {
        let trace::span::Link {
            trace_id,
            span_id,
            trace_state,
            attributes,
            dropped_attributes_count,
        } = value;
        let trace_id: [u8; 16] = trace_id[0..16]
            .try_into()
            .expect("trace id is 16 bytes in otel format ");
        let span_id: [u8; 8] = span_id[0..8]
            .try_into()
            .expect("span id is 8 bytes in otel format");
        Link {
            trace_id,
            span_id,
            trace_state,
            attributes: BTreeMap::from_iter(attributes),
            dropped_attributes_count,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Status {
    pub message: String,
    pub code: i32,
}

impl From<trace::Status> for Status {
    fn from(value: trace::Status) -> Self {
        Status {
            message: value.message,
            code: value.code,
        }
    }
}

impl From<super::proto::trace::TracesData> for Vec<SpanData> {
    fn from(value: super::proto::trace::TracesData) -> Self {
        let mut res = Vec::new();
        for resource_spans in value.resource_spans {
            let resource = resource_spans.resource.map(Resource::from);
            for scope_spans in resource_spans.scope_spans {
                let scope = scope_spans.scope.map(Scope::from);
                for span in scope_spans.spans {
                    let span = SpanData {
                        resource: resource.clone(),
                        scope: scope.clone(),
                        trace_id: span.trace_id[0..16]
                            .try_into()
                            .expect("trace_id is 16 bytes by spec"),
                        span_id: span.span_id[0..8]
                            .try_into()
                            .expect("span_id is 16 bytes by spec"),
                        trace_state: span.trace_state,
                        parent_span_id: span
                            .parent_span_id
                            .get(0..8)
                            .unwrap_or(&[0; 8])
                            .try_into()
                            .unwrap(),
                        name: span.name,
                        kind: span.kind,
                        start_time_unix_nano: span.start_time_unix_nano,
                        end_time_unix_nano: span.end_time_unix_nano,
                        attributes: BTreeMap::from_iter(span.attributes),
                        dropped_attributes_count: span.dropped_attributes_count,
                        events: span.events.into_iter().map(Into::into).collect(),
                        dropped_events_count: span.dropped_events_count,
                        links: span.links.into_iter().map(Into::into).collect(),
                        dropped_links_count: span.dropped_links_count,
                        status: span.status.map(Into::into),
                    };

                    res.push(span)
                }
            }
        }
        res
    }
}
