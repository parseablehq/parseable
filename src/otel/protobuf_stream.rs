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

//! Decodes an OTLP/HTTP protobuf export request one leaf record (`LogRecord`,
//! `Span`, `Metric`) at a time, instead of decoding the whole request first.
//!
//! The request, resource and scope levels are messages of their own here, which
//! prost drives exactly as it drives the generated ones: the same keys, wire type
//! checks, recursion limit, unknown-field skipping and error messages (each error
//! gets the `Message.field` path the generated code would push). Their fields are
//! merged as prost merges them, so a singular field that occurs more than once
//! (`resource`, `scope`, `schema_url`) ends up as whole-request decoding leaves it,
//! whatever the field order. The repeated children of a level are not decoded
//! where they occur: their bytes are kept (`Bytes`, so not copied) with the decode
//! context prost would use for them, and decoded once the level is read whole, one
//! at a time. A leaf record is decoded by `prost::encoding::message::merge`, as
//! whole-request decoding decodes it, once its decoded size is known to be under
//! a cap ([`max_record_bytes`]).

use bytes::{Buf, Bytes};
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::metrics::v1::Metric;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::encoding::{
    DecodeContext, WireType, check_wire_type, decode_varint, message, skip_field, string,
};
use prost::{DecodeError, Message};

use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::Map;

use super::decoded_size::{Leaf, leaf_decoded_size};
use super::expansion::{LogsSizer, MetricsSizer, RequestSize, TracesSizer};
use super::logs::{log_record_row, resource_log_fields, scope_log_fields};
use super::metrics::{ResourceMetricFields, metric_rows_into};
use super::sink::RowSink;
use super::traces::{SpanParents, resource_span_fields, scope_span_fields, span_rows};
use crate::utils::json::flatten::FlattenBudget;

/// Most memory one leaf record may take once decoded, for a payload limit of
/// `max_payload` bytes: 16 bytes per payload byte. Realistic records decode to
/// 2.4-4.2 bytes per byte, exponential histograms with small buckets to 12.5; a
/// span of 2 million empty events decodes to 32 bytes per byte.
pub fn max_record_bytes(max_payload: usize) -> usize {
    max_payload.saturating_mul(16)
}

/// Why a body could not be read, or what the consumer of its records refused
#[derive(Debug)]
pub enum StreamError<E> {
    /// The body is not a valid request; the error prost gives for it
    Decode(DecodeError),
    /// A leaf record would take more than `limit` bytes once decoded
    RecordTooLarge { leaf: Leaf, limit: usize },
    /// The consumer refused a record
    Consumer(E),
}

impl<E> StreamError<E> {
    /// The same error, with a refusal of the consumer mapped by `f`
    pub fn map_consumer<F>(self, f: impl FnOnce(E) -> F) -> StreamError<F> {
        match self {
            Self::Decode(e) => StreamError::Decode(e),
            Self::RecordTooLarge { leaf, limit } => StreamError::RecordTooLarge { leaf, limit },
            Self::Consumer(e) => StreamError::Consumer(f(e)),
        }
    }
}

/// What a request is made of, in the order its records are read
pub enum Item<L> {
    /// A resource level starts: its `resource` and `schema_url`
    Resource(Option<Resource>, String),
    /// A scope level of the current resource starts
    Scope(Option<InstrumentationScope>, String),
    /// A leaf record of the current scope
    Record(L),
    /// The current resource level ends
    ResourceEnd,
}

/// The names prost's error messages use for the levels of one signal
pub struct Names {
    request: &'static str,
    resources: &'static str,
    resource_level: &'static str,
    scopes: &'static str,
    scope_level: &'static str,
    records: &'static str,
}

pub const LOGS: Names = Names {
    request: "ExportLogsServiceRequest",
    resources: "resource_logs",
    resource_level: "ResourceLogs",
    scopes: "scope_logs",
    scope_level: "ScopeLogs",
    records: "log_records",
};

pub const TRACES: Names = Names {
    request: "ExportTraceServiceRequest",
    resources: "resource_spans",
    resource_level: "ResourceSpans",
    scopes: "scope_spans",
    scope_level: "ScopeSpans",
    records: "spans",
};

pub const METRICS: Names = Names {
    request: "ExportMetricsServiceRequest",
    resources: "resource_metrics",
    resource_level: "ResourceMetrics",
    scopes: "scope_metrics",
    scope_level: "ScopeMetrics",
    records: "metrics",
};

/// A leaf record type and how it is measured
pub trait LeafRecord: Message + Default {
    const LEAF: Leaf;
}

impl LeafRecord for LogRecord {
    const LEAF: Leaf = Leaf::LogRecord;
}

impl LeafRecord for Span {
    const LEAF: Leaf = Leaf::Span;
}

impl LeafRecord for Metric {
    const LEAF: Leaf = Leaf::Metric;
}

/// A length-delimited child kept for later: where it starts in the body (at its
/// length prefix) and the context prost would decode it with there. It is decoded
/// from the rest of the body, as prost decodes it: a broken length inside it can
/// run past its end before decoding fails, and fails the way it fails in place.
struct Child {
    start: usize,
    prefix: usize,
    len: usize,
    ctx: DecodeContext,
}

impl Child {
    /// Steps over the child at the front of `buf` (the end of a body of `body_len`
    /// bytes), failing like prost's `message::merge_repeated` on its framing
    fn take(
        wire_type: WireType,
        buf: &mut impl Buf,
        body_len: usize,
        ctx: DecodeContext,
    ) -> Result<Self, DecodeError> {
        check_wire_type(WireType::LengthDelimited, wire_type)?;
        let start = body_len - buf.remaining();
        let mut peek = buf.chunk();
        let contiguous = peek.len();
        let len = decode_varint(&mut peek)?;
        let prefix = contiguous - peek.len();
        if len > (buf.remaining() - prefix) as u64 {
            return Err(DecodeError::new("buffer underflow"));
        }
        buf.advance(prefix + len as usize);
        Ok(Self {
            start,
            prefix,
            len: len as usize,
            ctx,
        })
    }

    /// Decodes the child into `msg`, as prost decodes it where it occurs
    fn merge<M: Message>(self, body: &Bytes, msg: &mut M) -> Result<(), DecodeError> {
        message::merge(
            WireType::LengthDelimited,
            msg,
            &mut body.slice(self.start..),
            self.ctx,
        )
    }

    /// The encoding of the child, without its length prefix
    fn body<'b>(&self, body: &'b [u8]) -> &'b [u8] {
        &body[self.start + self.prefix..self.start + self.prefix + self.len]
    }
}

fn push(mut error: DecodeError, message: &'static str, field: &'static str) -> DecodeError {
    error.push(message, field);
    error
}

/// The request level: the resource levels, kept
struct RequestLevel {
    names: &'static Names,
    body_len: usize,
    children: Vec<Child>,
}

impl Message for RequestLevel {
    fn encode_raw(&self, _buf: &mut impl bytes::BufMut) {
        unreachable!("only decoded")
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        if tag == 1 {
            let child = Child::take(wire_type, buf, self.body_len, ctx)
                .map_err(|e| push(e, self.names.request, self.names.resources))?;
            self.children.push(child);
            Ok(())
        } else {
            skip_field(wire_type, tag, buf, ctx)
        }
    }

    fn encoded_len(&self) -> usize {
        0
    }

    fn clear(&mut self) {
        self.children.clear();
    }
}

/// A resource or scope level: its singular fields, merged, and its children, kept
struct Level<P> {
    body_len: usize,
    message: &'static str,
    parent_field: &'static str,
    children_field: &'static str,
    parent: Option<P>,
    schema_url: String,
    children: Vec<Child>,
}

impl<P> Level<P> {
    fn new(
        body_len: usize,
        message: &'static str,
        parent_field: &'static str,
        children_field: &'static str,
    ) -> Self {
        Self {
            body_len,
            message,
            parent_field,
            children_field,
            parent: None,
            schema_url: String::new(),
            children: Vec::new(),
        }
    }
}

impl<P: Message + Default> Message for Level<P> {
    fn encode_raw(&self, _buf: &mut impl bytes::BufMut) {
        unreachable!("only decoded")
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        match tag {
            1 => message::merge(
                wire_type,
                self.parent.get_or_insert_with(Default::default),
                buf,
                ctx,
            )
            .map_err(|e| push(e, self.message, self.parent_field)),
            2 => {
                let child = Child::take(wire_type, buf, self.body_len, ctx)
                    .map_err(|e| push(e, self.message, self.children_field))?;
                self.children.push(child);
                Ok(())
            }
            3 => string::merge(wire_type, &mut self.schema_url, buf, ctx)
                .map_err(|e| push(e, self.message, "schema_url")),
            _ => skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        0
    }

    fn clear(&mut self) {
        self.parent = None;
        self.schema_url.clear();
        self.children.clear();
    }
}

/// Reads the request `body` of the signal `names` belongs to and hands its items to
/// `consume` in order, one leaf record at a time. Records whose decoded size is over
/// `max_record` are refused before they are decoded.
pub fn read_request<L: LeafRecord, E>(
    body: Bytes,
    names: &'static Names,
    max_record: usize,
    mut consume: impl FnMut(Item<L>) -> Result<(), E>,
) -> Result<(), StreamError<E>> {
    let body_len = body.len();
    let mut request = RequestLevel {
        names,
        body_len,
        children: Vec::new(),
    };
    request.merge(body.clone()).map_err(StreamError::Decode)?;
    for resource_child in request.children {
        let mut resource =
            Level::<Resource>::new(body_len, names.resource_level, "resource", names.scopes);
        resource_child
            .merge(&body, &mut resource)
            .map_err(|e| StreamError::Decode(push(e, names.request, names.resources)))?;
        consume(Item::Resource(resource.parent, resource.schema_url))
            .map_err(StreamError::Consumer)?;
        for scope_child in resource.children {
            let in_resource = |e| {
                let e = push(e, names.resource_level, names.scopes);
                StreamError::Decode(push(e, names.request, names.resources))
            };
            let mut scope = Level::<InstrumentationScope>::new(
                body_len,
                names.scope_level,
                "scope",
                names.records,
            );
            scope_child.merge(&body, &mut scope).map_err(in_resource)?;
            consume(Item::Scope(scope.parent, scope.schema_url)).map_err(StreamError::Consumer)?;
            for record_child in scope.children {
                if leaf_decoded_size(record_child.body(&body), L::LEAF) > max_record {
                    return Err(StreamError::RecordTooLarge {
                        leaf: L::LEAF,
                        limit: max_record,
                    });
                }
                let mut record = L::default();
                record_child
                    .merge(&body, &mut record)
                    .map_err(|e| in_resource(push(e, names.scope_level, names.records)))?;
                consume(Item::Record(record)).map_err(StreamError::Consumer)?;
            }
        }
        consume(Item::ResourceEnd).map_err(StreamError::Consumer)?;
    }
    Ok(())
}

/// Reads only the request level of `body`: the framing of its resource levels and
/// its unknown fields. Needs no limit, so a body that is not a request of the signal
/// at all can be refused before anything else is looked at. On an error, reports
/// the one whole-request decoding gives (`first_error`, here without a cap on the
/// records it decodes on the way, one at a time).
pub fn check_request_level(body: Bytes, signal: Signal) -> Result<(), StreamError<Infallible>> {
    let names = signal.names();
    let mut request = RequestLevel {
        names,
        body_len: body.len(),
        children: Vec::new(),
    };
    match request.merge(body.clone()) {
        Ok(()) => Ok(()),
        Err(error) => Err(match signal {
            Signal::Logs => first_error::<LogRecord>(body, names, usize::MAX),
            Signal::Traces => first_error::<Span>(body, names, usize::MAX),
            Signal::Metrics => first_error::<Metric>(body, names, usize::MAX),
        }
        .unwrap_or(StreamError::Decode(error))),
    }
}

/// The signal of a protobuf body
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Logs,
    Metrics,
    Traces,
}

impl Signal {
    fn names(self) -> &'static Names {
        match self {
            Self::Logs => &LOGS,
            Self::Metrics => &METRICS,
            Self::Traces => &TRACES,
        }
    }
}

/// Rows, row bytes, records and data points of the request `body`, read one record
/// at a time (the sizing pass of the expansion cap, see `expansion`)
pub fn size_request(
    body: Bytes,
    signal: Signal,
    max_record: usize,
    flatten_exemplars: bool,
) -> Result<RequestSize, StreamError<Infallible>> {
    size_in_levels(body.clone(), signal, max_record, flatten_exemplars).map_err(|error| {
        match error {
            // Report the break whole-request decoding meets first
            StreamError::Decode(_) => match signal {
                Signal::Logs => first_error::<LogRecord>(body, &LOGS, max_record),
                Signal::Traces => first_error::<Span>(body, &TRACES, max_record),
                Signal::Metrics => first_error::<Metric>(body, &METRICS, max_record),
            }
            .unwrap_or(error),
            error => error,
        }
    })
}

fn size_in_levels(
    body: Bytes,
    signal: Signal,
    max_record: usize,
    flatten_exemplars: bool,
) -> Result<RequestSize, StreamError<Infallible>> {
    match signal {
        Signal::Logs => {
            let mut sizer = LogsSizer::default();
            read_request::<LogRecord, Infallible>(body, &LOGS, max_record, |item| {
                match item {
                    Item::Resource(resource, url) => sizer.resource(resource.as_ref(), &url),
                    Item::Scope(scope, url) => sizer.scope(scope.as_ref(), &url),
                    Item::Record(record) => sizer.record(&record),
                    Item::ResourceEnd => {}
                }
                Ok(())
            })?;
            Ok(sizer.finish())
        }
        Signal::Traces => {
            let mut sizer = TracesSizer::default();
            read_request::<Span, Infallible>(body, &TRACES, max_record, |item| {
                match item {
                    Item::Resource(resource, url) => sizer.resource(resource.as_ref(), &url),
                    Item::Scope(scope, url) => sizer.scope(scope.as_ref(), &url),
                    Item::Record(span) => sizer.span(&span),
                    Item::ResourceEnd => {}
                }
                Ok(())
            })?;
            Ok(sizer.finish())
        }
        Signal::Metrics => {
            let mut sizer = MetricsSizer::new(flatten_exemplars);
            read_request::<Metric, Infallible>(body, &METRICS, max_record, |item| {
                match item {
                    Item::Resource(resource, url) => sizer.resource(resource.as_ref(), &url),
                    Item::Scope(scope, url) => sizer.scope(scope.as_ref(), &url),
                    Item::Record(metric) => sizer.metric(&metric),
                    Item::ResourceEnd => sizer.resource_end(),
                }
                Ok(())
            })?;
            Ok(sizer.finish())
        }
    }
}

/// Hands the rows of the request `body` to `sink`, decoding one record at a time:
/// the same rows, in the same order, as decoding the whole request and flattening it
pub fn flatten_request<S: RowSink + ?Sized>(
    body: Bytes,
    signal: Signal,
    max_record: usize,
    budget: &mut FlattenBudget,
    flatten_exemplars: bool,
    sink: &mut S,
) -> Result<(), StreamError<S::Error>> {
    match signal {
        Signal::Logs => {
            let (mut resource_fields, mut scope_fields) = (Map::new(), Map::new());
            read_request::<LogRecord, S::Error>(body, &LOGS, max_record, |item| {
                match item {
                    Item::Resource(resource, url) => {
                        resource_fields = resource_log_fields(resource.as_ref(), &url)
                    }
                    Item::Scope(scope, url) => {
                        scope_fields = scope_log_fields(scope.as_ref(), &url)
                    }
                    Item::Record(record) => {
                        sink.row(log_record_row(
                            &record,
                            &scope_fields,
                            &resource_fields,
                            budget,
                        )?)?;
                        sink.record_end();
                    }
                    Item::ResourceEnd => {}
                }
                Ok(())
            })
        }
        Signal::Traces => {
            let (mut resource_fields, mut scope_fields) = (Map::new(), Map::new());
            let mut scope_url = String::new();
            read_request::<Span, S::Error>(body, &TRACES, max_record, |item| {
                match item {
                    Item::Resource(resource, url) => {
                        resource_fields = resource_span_fields(resource.as_ref(), &url)
                    }
                    Item::Scope(scope, url) => {
                        scope_fields = scope_span_fields(scope.as_ref());
                        scope_url = url;
                    }
                    Item::Record(span) => span_rows(
                        &span,
                        &SpanParents {
                            scope_fields: &scope_fields,
                            scope_schema_url: &scope_url,
                            resource_fields: &resource_fields,
                        },
                        sink,
                    )?,
                    Item::ResourceEnd => {}
                }
                Ok(())
            })
        }
        Signal::Metrics => {
            let mut resource: Option<ResourceMetricFields> = None;
            let mut envelope = Map::new();
            let mut latest: Option<String> = None;
            read_request::<Metric, S::Error>(body, &METRICS, max_record, |item| {
                match item {
                    Item::Resource(r, url) => {
                        resource = Some(ResourceMetricFields::new(r.as_ref(), &url));
                        latest = None;
                    }
                    Item::Scope(scope, url) => {
                        envelope = resource
                            .as_ref()
                            .expect("a scope is inside a resource")
                            .scope_envelope(scope.as_ref(), &url);
                    }
                    Item::Record(metric) => {
                        metric_rows_into(&metric, &envelope, flatten_exemplars, &mut latest, sink)?
                    }
                    Item::ResourceEnd => {
                        if let Some(target) = resource
                            .take()
                            .and_then(|resource| resource.target_info(latest.take()))
                        {
                            sink.row(target)?;
                        }
                    }
                }
                Ok(())
            })
        }
    }
}

/// One level of `first_error`'s walk: decodes everything in wire order, as prost
/// does, keeping nothing but one record at a time
struct WireOrder<'a, L> {
    names: &'static Names,
    depth: u8,
    max_record: usize,
    oversized: &'a AtomicBool,
    leaf: PhantomData<fn() -> L>,
}

impl<L: LeafRecord> WireOrder<'_, L> {
    fn child(&self) -> WireOrder<'_, L> {
        WireOrder {
            names: self.names,
            depth: self.depth + 1,
            max_record: self.max_record,
            oversized: self.oversized,
            leaf: PhantomData,
        }
    }
}

impl<L: LeafRecord> Message for WireOrder<'_, L> {
    fn encode_raw(&self, _buf: &mut impl bytes::BufMut) {
        unreachable!("only decoded")
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let names = self.names;
        match (self.depth, tag) {
            (0, 1) => message::merge(wire_type, &mut self.child(), buf, ctx)
                .map_err(|e| push(e, names.request, names.resources)),
            (1, 1) => message::merge(wire_type, &mut Resource::default(), buf, ctx)
                .map_err(|e| push(e, names.resource_level, "resource")),
            (1, 2) => message::merge(wire_type, &mut self.child(), buf, ctx)
                .map_err(|e| push(e, names.resource_level, names.scopes)),
            (2, 1) => message::merge(wire_type, &mut InstrumentationScope::default(), buf, ctx)
                .map_err(|e| push(e, names.scope_level, "scope")),
            (2, 2) => {
                // Measured where the framing allows, then decoded in place
                let mut peek = buf.chunk();
                if wire_type == WireType::LengthDelimited
                    && let Ok(len) = decode_varint(&mut peek)
                    && len <= peek.len() as u64
                    && leaf_decoded_size(&peek[..len as usize], L::LEAF) > self.max_record
                {
                    self.oversized.store(true, Ordering::Relaxed);
                    return Err(DecodeError::new("record over the decoded size cap"));
                }
                message::merge(wire_type, &mut L::default(), buf, ctx)
                    .map_err(|e| push(e, names.scope_level, names.records))
            }
            (1 | 2, 3) => {
                let level = if self.depth == 1 {
                    names.resource_level
                } else {
                    names.scope_level
                };
                string::merge(wire_type, &mut String::new(), buf, ctx)
                    .map_err(|e| push(e, level, "schema_url"))
            }
            _ => skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        0
    }

    fn clear(&mut self) {}
}

/// The error whole-request decoding gives for `body`, found by decoding it in wire
/// order one record at a time (`read_request` reads each level whole before its
/// children, so where a body is broken in more than one place it can meet another
/// break first). `None` when the body decodes.
pub fn first_error<L: LeafRecord>(
    body: Bytes,
    names: &'static Names,
    max_record: usize,
) -> Option<StreamError<Infallible>> {
    let oversized = AtomicBool::new(false);
    let mut walk = WireOrder::<L> {
        names,
        depth: 0,
        max_record,
        oversized: &oversized,
        leaf: PhantomData,
    };
    walk.merge(body).err().map(|error| {
        if oversized.load(Ordering::Relaxed) {
            StreamError::RecordTooLarge {
                leaf: L::LEAF,
                limit: max_record,
            }
        } else {
            StreamError::Decode(error)
        }
    })
}
