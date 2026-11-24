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
//! The request, resource and scope levels are walked as prost walks them: the same
//! keys, wire type checks, recursion limit, unknown-field skipping and error
//! messages (each error gets the `Message.field` path the generated code would
//! push). A resource or scope level is read twice over its own bytes: first its
//! singular fields (`resource`, `scope`, `schema_url`) are merged as prost merges
//! them, so a field that occurs more than once ends up as whole-request decoding
//! leaves it whatever the field order, while its children are only stepped over;
//! then its children are visited in order. A child is decoded from the rest of the
//! body with the decode context prost would use for it where it occurs, so it fails
//! exactly as it fails in place.
//!
//! A request is read in two walks. [`outline`] walks only the levels above the
//! records and counts the records without decoding one, so a body whose framing
//! is broken there is refused before anything is staged. [`flatten_request`] then
//! walks it again and decodes every leaf record once, with
//! `prost::encoding::message::merge` as whole-request decoding decodes it, hands
//! its rows to the sink and drops it. A record whose decoded size could be over
//! [`max_record_bytes`] is measured first, and refused without being decoded when
//! it is over.

use std::convert::Infallible;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::{Buf, Bytes};
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::metrics::v1::Metric;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::encoding::{
    DecodeContext, WireType, check_wire_type, decode_key, decode_varint, message, skip_field,
    string,
};
use prost::{DecodeError, Message};
use serde_json::Map;

use super::decoded_size::{Leaf, leaf_decoded_size, metric_points, needs_measuring};
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

impl StreamError<Infallible> {
    /// The same error, as one of a consumer of any type
    pub fn widen<F>(self) -> StreamError<F> {
        self.map_consumer(|never| match never {})
    }
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

/// A length-delimited child: where it starts in the body (at its length prefix)
/// and the context prost would decode it with there. It is decoded from the rest
/// of the body, as prost decodes it: a broken length inside it can run past its end
/// before decoding fails, and fails the way it fails in place.
#[derive(Clone)]
struct Child {
    start: usize,
    prefix: usize,
    len: usize,
    ctx: DecodeContext,
}

impl Child {
    /// Steps over the child at the front of `buf`, which ends at `end` in the body,
    /// failing like prost's `message::merge_repeated` on its framing
    fn take(
        wire_type: WireType,
        buf: &mut impl Buf,
        end: usize,
        ctx: DecodeContext,
    ) -> Result<Self, DecodeError> {
        check_wire_type(WireType::LengthDelimited, wire_type)?;
        let start = end - buf.remaining();
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
    fn merge<M: Message>(&self, body: &Bytes, msg: &mut M) -> Result<(), DecodeError> {
        message::merge(
            WireType::LengthDelimited,
            msg,
            &mut body.slice(self.start..),
            self.ctx.clone(),
        )
    }

    /// Where the encoding of the child, without its length prefix, is in the body
    fn content(&self) -> Range<usize> {
        self.start + self.prefix..self.start + self.prefix + self.len
    }

    /// The encoding of the child, without its length prefix
    fn body<'b>(&self, body: &'b [u8]) -> &'b [u8] {
        &body[self.content()]
    }
}

fn push(mut error: DecodeError, message: &'static str, field: &'static str) -> DecodeError {
    error.push(message, field);
    error
}

/// The singular fields of a resource or scope level, merged as prost merges them.
/// Its children (field 2) are only stepped over, which checks their framing.
struct Fields<P> {
    message: &'static str,
    parent_field: &'static str,
    children_field: &'static str,
    parent: Option<P>,
    schema_url: String,
    /// The context prost decodes the fields of the level with, once it has one
    ctx: Option<DecodeContext>,
}

impl<P> Fields<P> {
    fn new(
        message: &'static str,
        parent_field: &'static str,
        children_field: &'static str,
    ) -> Self {
        Self {
            message,
            parent_field,
            children_field,
            parent: None,
            schema_url: String::new(),
            ctx: None,
        }
    }
}

impl<P: Message + Default> Message for Fields<P> {
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
        if self.ctx.is_none() {
            self.ctx = Some(ctx.clone());
        }
        match tag {
            1 => message::merge(
                wire_type,
                self.parent.get_or_insert_with(Default::default),
                buf,
                ctx,
            )
            .map_err(|e| push(e, self.message, self.parent_field)),
            2 => {
                // Only the framing; `Child::take` needs no position for that
                let end = buf.remaining();
                Child::take(wire_type, buf, end, ctx)
                    .map(drop)
                    .map_err(|e| push(e, self.message, self.children_field))
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
    }
}

/// Hands every child (fields numbered `tag`) of the level whose fields are
/// `body[range]` to `visit`, in order, stepping over its other fields as prost
/// does. `ctx` is the context prost decodes the fields of the level with; `path`
/// adds the field of the children to a framing error.
fn children<E>(
    body: &Bytes,
    range: Range<usize>,
    ctx: &DecodeContext,
    tag: u32,
    path: impl Fn(DecodeError) -> DecodeError,
    visit: &mut dyn FnMut(Child) -> Result<(), StreamError<E>>,
) -> Result<(), StreamError<E>> {
    let end = range.end;
    let mut buf = &body[range];
    while buf.has_remaining() {
        let (field, wire_type) = decode_key(&mut buf).map_err(StreamError::Decode)?;
        if field == tag {
            let child = Child::take(wire_type, &mut buf, end, ctx.clone())
                .map_err(|e| StreamError::Decode(path(e)))?;
            visit(child)?;
        } else {
            skip_field(wire_type, field, &mut buf, ctx.clone()).map_err(StreamError::Decode)?;
        }
    }
    Ok(())
}

/// What a request is made of, in the order of the body
enum Item {
    /// A resource level starts: its `resource` and `schema_url`
    Resource(Option<Resource>, String),
    /// A scope level of the current resource starts
    Scope(Option<InstrumentationScope>, String),
    /// A leaf record of the current scope, not decoded
    Record(Child),
    /// The current resource level ends
    ResourceEnd,
}

/// Walks the levels of the request `body` above its leaf records and hands their
/// items to `consume`, in order. Fails with prost's error where one of those levels
/// is broken (a record is only stepped over, so its framing is checked but not its
/// content), or with the first refusal of `consume`.
fn walk<E>(
    body: &Bytes,
    names: &'static Names,
    consume: &mut dyn FnMut(Item) -> Result<(), StreamError<E>>,
) -> Result<(), StreamError<E>> {
    let in_request = |e| push(e, names.request, names.resources);
    let in_resource = |e| in_request(push(e, names.resource_level, names.scopes));
    let in_scope = |e| in_resource(push(e, names.scope_level, names.records));
    // `Message::merge` decodes the fields of a request with the default context
    let top = DecodeContext::default();
    children(body, 0..body.len(), &top, 1, in_request, &mut |resource| {
        let mut level = Fields::<Resource>::new(names.resource_level, "resource", names.scopes);
        resource
            .merge(body, &mut level)
            .map_err(|e| StreamError::Decode(in_request(e)))?;
        consume(Item::Resource(
            level.parent.take(),
            std::mem::take(&mut level.schema_url),
        ))?;
        if let Some(ctx) = &level.ctx {
            children(
                body,
                resource.content(),
                ctx,
                2,
                in_resource,
                &mut |scope| {
                    let mut level = Fields::<InstrumentationScope>::new(
                        names.scope_level,
                        "scope",
                        names.records,
                    );
                    scope
                        .merge(body, &mut level)
                        .map_err(|e| StreamError::Decode(in_resource(e)))?;
                    consume(Item::Scope(
                        level.parent.take(),
                        std::mem::take(&mut level.schema_url),
                    ))?;
                    match &level.ctx {
                        Some(ctx) => {
                            children(body, scope.content(), ctx, 2, in_scope, &mut |record| {
                                consume(Item::Record(record))
                            })
                        }
                        None => Ok(()),
                    }
                },
            )?;
        }
        consume(Item::ResourceEnd)
    })
}

/// What [`outline`] found in a request
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Outline {
    /// Leaf records (log records, spans or metrics) of the request
    pub records: u64,
}

/// Reads the levels of the request `body` above its leaf records, without decoding
/// a record, and counts the records. Needs no limit, so a body that is not a
/// request of the signal at all is refused before anything else is looked at.
/// The error is the first break met in those levels; [`refusal`] turns it into
/// the error whole-request decoding gives.
pub fn outline(body: &Bytes, signal: Signal) -> Result<Outline, DecodeError> {
    let mut outline = Outline::default();
    walk::<Infallible>(body, signal.names(), &mut |item| {
        if let Item::Record(_) = item {
            outline.records += 1;
        }
        Ok(())
    })
    .map_err(|error| match error {
        StreamError::Decode(error) => error,
        StreamError::RecordTooLarge { .. } => unreachable!("no record is measured"),
        StreamError::Consumer(never) => match never {},
    })?;
    Ok(outline)
}

/// The error to refuse a body with that [`outline`] found broken (`error`): the one
/// whole-request decoding gives, found by decoding the body in wire order one record
/// at a time (`first_error`), or a record over the cap met before the break.
/// `max_record` is asked for only once a record is met, so a body that is not a
/// request is refused without it.
pub fn refusal(
    body: &Bytes,
    signal: Signal,
    error: DecodeError,
    max_record: impl FnOnce() -> usize,
) -> StreamError<Infallible> {
    let found = match first_error(body, signal, None) {
        FirstError::NeedsCap => first_error(body, signal, Some(max_record())),
        found => found,
    };
    match found {
        FirstError::Found(found) => found,
        // Not reached: a body broken above its records does not decode
        FirstError::None | FirstError::NeedsCap => StreamError::Decode(error),
    }
}

/// Decodes the leaf record `child` of `body`, refusing it before it is decoded if
/// it would take more than `max_record` bytes
fn decode_leaf<L: LeafRecord, E>(
    body: &Bytes,
    child: &Child,
    names: &'static Names,
    max_record: usize,
) -> Result<L, StreamError<E>> {
    let encoded = child.body(body);
    if needs_measuring(encoded.len(), L::LEAF, max_record)
        && leaf_decoded_size(encoded, L::LEAF) > max_record
    {
        return Err(StreamError::RecordTooLarge {
            leaf: L::LEAF,
            limit: max_record,
        });
    }
    let mut record = L::default();
    child.merge(body, &mut record).map_err(|e| {
        let e = push(e, names.scope_level, names.records);
        let e = push(e, names.resource_level, names.scopes);
        StreamError::Decode(push(e, names.request, names.resources))
    })?;
    Ok(record)
}

/// Hands the rows of the request `body` to `sink`, decoding one record at a time:
/// the same rows, in the same order, as decoding the whole request and flattening
/// it. `body` must have passed [`outline`]; then the only errors are those of a
/// record (it does not decode, or is over `max_record`) and refusals of `sink`,
/// and the rows of every record before it were handed over.
pub fn flatten_request<S: RowSink + ?Sized>(
    body: &Bytes,
    signal: Signal,
    max_record: usize,
    budget: &mut FlattenBudget,
    flatten_exemplars: bool,
    sink: &mut S,
) -> Result<(), StreamError<S::Error>> {
    let names = signal.names();
    match signal {
        Signal::Logs => {
            let (mut resource_fields, mut scope_fields) = (Map::new(), Map::new());
            walk(body, names, &mut |item| {
                match item {
                    Item::Resource(resource, url) => {
                        resource_fields = resource_log_fields(resource.as_ref(), &url)
                    }
                    Item::Scope(scope, url) => {
                        scope_fields = scope_log_fields(scope.as_ref(), &url)
                    }
                    Item::Record(child) => {
                        let record: LogRecord = decode_leaf(body, &child, names, max_record)?;
                        let row = log_record_row(&record, &scope_fields, &resource_fields, budget)
                            .map_err(|e| StreamError::Consumer(e.into()))?;
                        sink.row(row).map_err(StreamError::Consumer)?;
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
            walk(body, names, &mut |item| {
                match item {
                    Item::Resource(resource, url) => {
                        resource_fields = resource_span_fields(resource.as_ref(), &url)
                    }
                    Item::Scope(scope, url) => {
                        scope_fields = scope_span_fields(scope.as_ref());
                        scope_url = url;
                    }
                    Item::Record(child) => {
                        let span: Span = decode_leaf(body, &child, names, max_record)?;
                        span_rows(
                            &span,
                            &SpanParents {
                                scope_fields: &scope_fields,
                                scope_schema_url: &scope_url,
                                resource_fields: &resource_fields,
                            },
                            sink,
                        )
                        .map_err(StreamError::Consumer)?
                    }
                    Item::ResourceEnd => {}
                }
                Ok(())
            })
        }
        Signal::Metrics => {
            let mut resource: Option<ResourceMetricFields> = None;
            let mut envelope = Map::new();
            let mut latest: Option<String> = None;
            walk(body, names, &mut |item| {
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
                    Item::Record(child) => {
                        let metric: Metric = decode_leaf(body, &child, names, max_record)?;
                        metric_rows_into(&metric, &envelope, flatten_exemplars, &mut latest, sink)
                            .map_err(StreamError::Consumer)?
                    }
                    Item::ResourceEnd => {
                        if let Some(target) = resource
                            .take()
                            .and_then(|resource| resource.target_info(latest.take()))
                        {
                            sink.row(target).map_err(StreamError::Consumer)?;
                        }
                    }
                }
                Ok(())
            })
        }
    }
}

/// Data points of the metrics request `body`, counted from the wire without
/// decoding a metric (see `metric_points`). For the rejected count of a partial
/// success, so only read after a failure. `body` must have passed [`outline`].
pub fn count_points(body: &Bytes) -> u64 {
    let mut points = 0u64;
    let _ = walk::<Infallible>(body, &METRICS, &mut |item| {
        if let Item::Record(child) = item {
            points += metric_points(child.body(body));
        }
        Ok(())
    });
    points
}

/// One level of `first_error`'s walk: decodes everything in wire order, as prost
/// does, keeping nothing but one record at a time
struct WireOrder<'a> {
    names: &'static Names,
    leaf: Leaf,
    depth: u8,
    /// `None` until the cap on one record is known: the walk stops at the first record
    max_record: Option<usize>,
    stopped: &'a AtomicBool,
}

impl WireOrder<'_> {
    fn child(&self) -> WireOrder<'_> {
        WireOrder {
            names: self.names,
            leaf: self.leaf,
            depth: self.depth + 1,
            max_record: self.max_record,
            stopped: self.stopped,
        }
    }

    fn merge_leaf(
        &self,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        match self.leaf {
            Leaf::LogRecord => message::merge(wire_type, &mut LogRecord::default(), buf, ctx),
            Leaf::Span => message::merge(wire_type, &mut Span::default(), buf, ctx),
            Leaf::Metric => message::merge(wire_type, &mut Metric::default(), buf, ctx),
        }
    }
}

impl Message for WireOrder<'_> {
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
                let Some(max_record) = self.max_record else {
                    self.stopped.store(true, Ordering::Relaxed);
                    return Err(DecodeError::new("stopped at the first record"));
                };
                // Measured where the framing allows, then decoded in place
                let mut peek = buf.chunk();
                if wire_type == WireType::LengthDelimited
                    && let Ok(len) = decode_varint(&mut peek)
                    && len <= peek.len() as u64
                    && needs_measuring(len as usize, self.leaf, max_record)
                    && leaf_decoded_size(&peek[..len as usize], self.leaf) > max_record
                {
                    self.stopped.store(true, Ordering::Relaxed);
                    return Err(DecodeError::new("record over the decoded size cap"));
                }
                self.merge_leaf(wire_type, buf, ctx)
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

/// What `first_error` found
enum FirstError {
    /// The error whole-request decoding gives, or a record over the cap before it
    Found(StreamError<Infallible>),
    /// The walk met a record and needs the cap on one record to go on
    NeedsCap,
    /// The body decodes
    None,
}

/// The error whole-request decoding gives for `body`, found by decoding it in wire
/// order one record at a time (`walk` reads each level whole before its children,
/// so where a body is broken in more than one place it can meet another break first)
fn first_error(body: &Bytes, signal: Signal, max_record: Option<usize>) -> FirstError {
    let stopped = AtomicBool::new(false);
    let leaf = match signal {
        Signal::Logs => Leaf::LogRecord,
        Signal::Traces => Leaf::Span,
        Signal::Metrics => Leaf::Metric,
    };
    let mut walk = WireOrder {
        names: signal.names(),
        leaf,
        depth: 0,
        max_record,
        stopped: &stopped,
    };
    match walk.merge(body.clone()) {
        Ok(()) => FirstError::None,
        Err(_) if stopped.load(Ordering::Relaxed) => match max_record {
            None => FirstError::NeedsCap,
            Some(limit) => FirstError::Found(StreamError::RecordTooLarge { leaf, limit }),
        },
        Err(error) => FirstError::Found(StreamError::Decode(error)),
    }
}
