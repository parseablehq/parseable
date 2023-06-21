use itertools::Itertools;

use super::common::{attributes_to_string, id_to_string, KeyValue, Resource, Scope};
use super::proto::trace;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SpanData {
    resource_attributes: String,
    scope_name: String,
    scope_version: String,
    scope_attributes: String,
    trace_id: String,
    span_id: String,
    trace_state: String,
    parent_span_id: String,
    name: String,
    // replace with SpanKind
    kind: i32,
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    attributes: String,
    events: Vec<Event>,
    links: Vec<Link>,
    status_message: String,
    status_code: i32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Event {
    time_unix_nano: u64,
    name: String,
    attributes: String,
}

impl From<trace::span::Event> for Event {
    fn from(value: trace::span::Event) -> Self {
        let trace::span::Event {
            time_unix_nano,
            name,
            attributes,
            ..
        } = value;
        Event {
            time_unix_nano,
            name,
            attributes: attributes_to_string(&attributes.into_iter().map(Into::into).collect_vec()),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Link {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: String,
    pub attributes: String,
}

impl From<trace::span::Link> for Link {
    fn from(value: trace::span::Link) -> Self {
        let trace::span::Link {
            trace_id,
            span_id,
            trace_state,
            attributes,
            ..
        } = value;
        Link {
            trace_id: id_to_string(&trace_id),
            span_id: id_to_string(&span_id),
            trace_state,
            attributes: attributes_to_string(&attributes.into_iter().map(Into::into).collect_vec()),
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
                        resource_attributes: resource
                            .as_ref()
                            .map(|x| attributes_to_string(&x.attributes))
                            .unwrap_or_default(),
                        scope_name: scope
                            .as_ref()
                            .map(|ref x| x.name.clone())
                            .unwrap_or_default(),
                        scope_version: scope
                            .as_ref()
                            .map(|ref x| x.version.clone())
                            .unwrap_or_default(),
                        scope_attributes: scope
                            .as_ref()
                            .map(|x| attributes_to_string(&x.attributes))
                            .unwrap_or_default(),
                        trace_id: id_to_string(&span.trace_id),
                        span_id: id_to_string(&span.span_id),
                        trace_state: span.trace_state,
                        parent_span_id: id_to_string(&span.parent_span_id),
                        name: span.name,
                        kind: span.kind,
                        start_time_unix_nano: span.start_time_unix_nano,
                        end_time_unix_nano: span.end_time_unix_nano,
                        attributes: attributes_to_string(
                            &span
                                .attributes
                                .into_iter()
                                .map(|x| Into::<KeyValue>::into(x))
                                .collect_vec(),
                        ),
                        events: span.events.into_iter().map(Into::into).collect(),
                        links: span.links.into_iter().map(Into::into).collect(),
                        status_code: span
                            .status
                            .as_ref()
                            .map(|status| status.code)
                            .unwrap_or_default(),
                        status_message: span
                            .status
                            .map(|status| status.message)
                            .unwrap_or_default(),
                    };

                    res.push(span)
                }
            }
        }
        res
    }
}
