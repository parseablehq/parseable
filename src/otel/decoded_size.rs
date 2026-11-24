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

//! The memory an OTLP export request takes once decoded, estimated before it is decoded.
//!
//! Decoding allocates far more than the body holds for small messages: an empty span is
//! two bytes of protobuf and a `Span` of a few hundred bytes, an empty attribute two bytes
//! and a `KeyValue` of 64. A body inside the payload limit could ask for gigabytes. The
//! estimate walks the body against the OTLP schema without allocating, adding up the heap
//! memory the decoded request holds: every `Vec` of messages or numbers, at the capacity
//! that pushing its elements one by one grows it to, as prost does, and every string and
//! bytes field, which prost allocates at its length but at least 8 bytes (a message field
//! held inline, not in a `Vec`, is part of its parent's struct). For a body as prost
//! encodes it, that is exactly what decoding retains, allocator overhead aside; while
//! decoding, a `Vec` that grows holds its old and new buffers at once for a moment.
//!
//! The same walk runs over the `Value` tree of an OTLP JSON body before it is mapped to
//! the typed messages, so both encodings of a request are refused alike. A JSON body has
//! already been parsed by then; the walk bounds the typed copy made from it. It counts
//! the text of a hex or base64 field, more than the bytes it decodes to.

use std::mem::size_of;

use opentelemetry_proto::tonic::common::v1::{AnyValue, EntityRef, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, SummaryDataPoint, summary_data_point::ValueAtQuantile,
};
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span,
    span::{Event, Link},
};

/// The leaf records the request is decoded one at a time in (see `protobuf_stream`)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leaf {
    LogRecord,
    Span,
    Metric,
}

/// Estimated bytes one leaf record takes once prost decodes `body` (its encoding,
/// without a length prefix) on its own: its struct and everything its fields allocate.
/// For a record prost encoded this equals what decoding retains; a record that repeats
/// a singular message field can retain up to twice its estimate (prost merges the
/// occurrences into one `Vec` that grows past the sum of their capacities). A record
/// whose framing breaks is counted up to the break, where decoding it fails too.
pub fn leaf_decoded_size(body: &[u8], leaf: Leaf) -> usize {
    let (msg, own) = match leaf {
        Leaf::LogRecord => (Msg::LogRecord, size_of::<LogRecord>()),
        Leaf::Span => (Msg::Span, size_of::<Span>()),
        Leaf::Metric => (Msg::Metric, size_of::<Metric>()),
    };
    let mut size = own;
    // A leaf is three messages deep in its request
    let _ = scan(body, msg, 3, &mut size);
    size
}

/// Messages of the OTLP schema whose fields allocate when decoded. Leaf records are
/// sized on their own (`leaf_decoded_size`); the levels above them are walked by
/// prost in `protobuf_stream`, so their variants are only reached from each other.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Msg {
    LogsRequest,
    ResourceLogs,
    ScopeLogs,
    LogRecord,
    TracesRequest,
    ResourceSpans,
    ScopeSpans,
    Span,
    Event,
    Link,
    Status,
    MetricsRequest,
    ResourceMetrics,
    ScopeMetrics,
    Metric,
    /// `Gauge`, `Sum`: their data points
    NumberPoints,
    Histogram,
    ExponentialHistogram,
    Summary,
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    Buckets,
    SummaryDataPoint,
    ValueAtQuantile,
    Exemplar,
    Resource,
    EntityRef,
    Scope,
    KeyValue,
    AnyValue,
    /// `ArrayValue`, `KeyValueList`: their values
    ArrayValue,
    KeyValueList,
}

/// What a field allocates when decoded
#[derive(Debug, Clone, Copy)]
enum Kind {
    /// A message: its struct if it is held in a `Vec`, and what its fields allocate
    Message(Msg),
    /// A string or bytes: its bytes, and a `String` if it is held in a `Vec`
    Bytes,
    /// A repeated 8-byte number, a `u64` or `f64` slot each
    Numbers(Encoding),
}

/// Wire encoding of the elements of a repeated number field
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Varint,
    Fixed64,
}

#[derive(Debug, Clone, Copy)]
struct Field {
    kind: Kind,
    /// For a repeated field, which of the `SLOTS` counters of its message counts its
    /// elements; every repeated field of a message has its own
    slot: Option<usize>,
}

/// Repeated fields a message has at most (`HistogramDataPoint` has 4)
const SLOTS: usize = 4;

const fn one(kind: Kind) -> Option<Field> {
    Some(Field { kind, slot: None })
}

const fn many(slot: usize, kind: Kind) -> Option<Field> {
    Some(Field {
        kind,
        slot: Some(slot),
    })
}

impl Kind {
    /// Bytes of one element of a `Vec` of this kind
    fn element_size(self) -> usize {
        match self {
            Self::Message(msg) => msg.size(),
            Self::Bytes => size_of::<String>(),
            Self::Numbers(_) => size_of::<u64>(),
        }
    }
}

/// Bytes a `Vec` of `len` elements of `element_size` bytes holds once they were pushed one
/// by one: the standard library starts at 4 elements (8 for 1-byte elements, 1 for
/// elements over 1 KiB) and doubles
fn vec_capacity_bytes(len: usize, element_size: usize) -> usize {
    if len == 0 || element_size == 0 {
        return 0;
    }
    let first = match element_size {
        1 => 8,
        2..=1024 => 4,
        _ => 1,
    };
    len.checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(first)
        .saturating_mul(element_size)
}

/// Bytes prost allocates for a string or bytes field of `len` bytes
fn bytes_capacity(len: usize) -> usize {
    vec_capacity_bytes(len.min(1), 1).max(len)
}

/// Counts the elements of the repeated fields of one message, and adds the `Vec`s that
/// hold them once the message is done
#[derive(Default)]
struct Vecs {
    lens: [usize; SLOTS],
    element_sizes: [usize; SLOTS],
}

impl Vecs {
    fn push(&mut self, slot: usize, kind: Kind, elements: usize) {
        self.lens[slot] = self.lens[slot].saturating_add(elements);
        self.element_sizes[slot] = kind.element_size();
    }

    fn add_to(&self, size: &mut usize) {
        for (&len, &element_size) in self.lens.iter().zip(&self.element_sizes) {
            add(size, vec_capacity_bytes(len, element_size));
        }
    }
}

use Kind::{Bytes, Message as M, Numbers};

impl Msg {
    /// Bytes of the struct a message held in a `Vec` adds
    fn size(self) -> usize {
        match self {
            Self::LogsRequest | Self::TracesRequest | Self::MetricsRequest => 0,
            Self::ResourceLogs => size_of::<ResourceLogs>(),
            Self::ScopeLogs => size_of::<ScopeLogs>(),
            Self::LogRecord => size_of::<LogRecord>(),
            Self::ResourceSpans => size_of::<ResourceSpans>(),
            Self::ScopeSpans => size_of::<ScopeSpans>(),
            Self::Span => size_of::<Span>(),
            Self::Event => size_of::<Event>(),
            Self::Link => size_of::<Link>(),
            Self::ResourceMetrics => size_of::<ResourceMetrics>(),
            Self::ScopeMetrics => size_of::<ScopeMetrics>(),
            Self::Metric => size_of::<Metric>(),
            Self::NumberDataPoint => size_of::<NumberDataPoint>(),
            Self::HistogramDataPoint => size_of::<HistogramDataPoint>(),
            Self::ExponentialHistogramDataPoint => size_of::<ExponentialHistogramDataPoint>(),
            Self::SummaryDataPoint => size_of::<SummaryDataPoint>(),
            Self::ValueAtQuantile => size_of::<ValueAtQuantile>(),
            Self::Exemplar => size_of::<Exemplar>(),
            Self::EntityRef => size_of::<EntityRef>(),
            Self::KeyValue => size_of::<KeyValue>(),
            Self::AnyValue => size_of::<AnyValue>(),
            // Only ever held inline in their parent
            Self::Status
            | Self::NumberPoints
            | Self::Histogram
            | Self::ExponentialHistogram
            | Self::Summary
            | Self::Buckets
            | Self::Resource
            | Self::Scope
            | Self::ArrayValue
            | Self::KeyValueList => 0,
        }
    }

    /// The field with protobuf field number `number`, `None` for a field that allocates
    /// nothing (a number, a bool, an enum) or that the schema does not have
    fn field(self, number: u64) -> Option<Field> {
        match (self, number) {
            (Self::LogsRequest, 1) => many(0, M(Self::ResourceLogs)),
            (Self::ResourceLogs, 1) => one(M(Self::Resource)),
            (Self::ResourceLogs, 2) => many(0, M(Self::ScopeLogs)),
            (Self::ScopeLogs, 1) => one(M(Self::Scope)),
            (Self::ScopeLogs, 2) => many(0, M(Self::LogRecord)),
            (Self::LogRecord, 3 | 9 | 10 | 12) => one(Bytes),
            (Self::LogRecord, 5) => one(M(Self::AnyValue)),
            (Self::LogRecord, 6) => many(0, M(Self::KeyValue)),

            (Self::TracesRequest, 1) => many(0, M(Self::ResourceSpans)),
            (Self::ResourceSpans, 1) => one(M(Self::Resource)),
            (Self::ResourceSpans, 2) => many(0, M(Self::ScopeSpans)),
            (Self::ScopeSpans, 1) => one(M(Self::Scope)),
            (Self::ScopeSpans, 2) => many(0, M(Self::Span)),
            (Self::Span, 1..=5) => one(Bytes),
            (Self::Span, 9) => many(0, M(Self::KeyValue)),
            (Self::Span, 11) => many(1, M(Self::Event)),
            (Self::Span, 13) => many(2, M(Self::Link)),
            (Self::Span, 15) => one(M(Self::Status)),
            (Self::Event, 2) => one(Bytes),
            (Self::Event, 3) => many(0, M(Self::KeyValue)),
            (Self::Link, 1..=3) => one(Bytes),
            (Self::Link, 4) => many(0, M(Self::KeyValue)),
            (Self::Status, 2) => one(Bytes),

            (Self::MetricsRequest, 1) => many(0, M(Self::ResourceMetrics)),
            (Self::ResourceMetrics, 1) => one(M(Self::Resource)),
            (Self::ResourceMetrics, 2) => many(0, M(Self::ScopeMetrics)),
            (Self::ScopeMetrics, 1) => one(M(Self::Scope)),
            (Self::ScopeMetrics, 2) => many(0, M(Self::Metric)),
            (Self::Metric, 1..=3) => one(Bytes),
            (Self::Metric, 5 | 7) => one(M(Self::NumberPoints)),
            (Self::Metric, 9) => one(M(Self::Histogram)),
            (Self::Metric, 10) => one(M(Self::ExponentialHistogram)),
            (Self::Metric, 11) => one(M(Self::Summary)),
            (Self::Metric, 12) => many(0, M(Self::KeyValue)),
            (Self::NumberPoints, 1) => many(0, M(Self::NumberDataPoint)),
            (Self::Histogram, 1) => many(0, M(Self::HistogramDataPoint)),
            (Self::ExponentialHistogram, 1) => many(0, M(Self::ExponentialHistogramDataPoint)),
            (Self::Summary, 1) => many(0, M(Self::SummaryDataPoint)),
            (Self::NumberDataPoint, 7) => many(0, M(Self::KeyValue)),
            (Self::NumberDataPoint, 5) => many(1, M(Self::Exemplar)),
            (Self::HistogramDataPoint, 9) => many(0, M(Self::KeyValue)),
            (Self::HistogramDataPoint, 6) => many(1, Numbers(Encoding::Fixed64)),
            (Self::HistogramDataPoint, 7) => many(2, Numbers(Encoding::Fixed64)),
            (Self::HistogramDataPoint, 8) => many(3, M(Self::Exemplar)),
            (Self::ExponentialHistogramDataPoint, 1) => many(0, M(Self::KeyValue)),
            (Self::ExponentialHistogramDataPoint, 8 | 9) => one(M(Self::Buckets)),
            (Self::ExponentialHistogramDataPoint, 11) => many(1, M(Self::Exemplar)),
            (Self::Buckets, 2) => many(0, Numbers(Encoding::Varint)),
            (Self::SummaryDataPoint, 7) => many(0, M(Self::KeyValue)),
            (Self::SummaryDataPoint, 6) => many(1, M(Self::ValueAtQuantile)),
            (Self::Exemplar, 7) => many(0, M(Self::KeyValue)),
            (Self::Exemplar, 4 | 5) => one(Bytes),

            (
                Self::ResourceLogs
                | Self::ScopeLogs
                | Self::ResourceSpans
                | Self::ScopeSpans
                | Self::ResourceMetrics
                | Self::ScopeMetrics,
                3,
            ) => one(Bytes),
            (Self::Resource, 1) => many(0, M(Self::KeyValue)),
            (Self::Resource, 3) => many(1, M(Self::EntityRef)),
            (Self::EntityRef, 1 | 2) => one(Bytes),
            (Self::EntityRef, 3) => many(0, Bytes),
            (Self::EntityRef, 4) => many(1, Bytes),
            (Self::Scope, 1 | 2) => one(Bytes),
            (Self::Scope, 3) => many(0, M(Self::KeyValue)),
            (Self::KeyValue, 1) => one(Bytes),
            (Self::KeyValue, 2) => one(M(Self::AnyValue)),
            (Self::AnyValue, 1 | 7) => one(Bytes),
            (Self::AnyValue, 5) => one(M(Self::ArrayValue)),
            (Self::AnyValue, 6) => one(M(Self::KeyValueList)),
            (Self::ArrayValue, 1) => many(0, M(Self::AnyValue)),
            (Self::KeyValueList, 1) => many(0, M(Self::KeyValue)),
            _ => None,
        }
    }
}

/// Deeper than prost decodes: a body nested this deep fails to decode before reaching it
const MAX_DEPTH: u32 = 128;

/// The framing of the body is broken, or nested deeper than decoding allows
struct Malformed;

fn varint(buf: &mut &[u8]) -> Result<u64, Malformed> {
    let mut value = 0u64;
    for (index, &byte) in buf.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte < 0x80 {
            *buf = &buf[index + 1..];
            return Ok(value);
        }
    }
    Err(Malformed)
}

fn take<'a>(buf: &mut &'a [u8], len: u64) -> Result<&'a [u8], Malformed> {
    let len = usize::try_from(len).map_err(|_| Malformed)?;
    if len > buf.len() {
        return Err(Malformed);
    }
    let (taken, rest) = buf.split_at(len);
    *buf = rest;
    Ok(taken)
}

const VARINT: u64 = 0;
const FIXED64: u64 = 1;
const LEN: u64 = 2;
const START_GROUP: u64 = 3;
const END_GROUP: u64 = 4;
const FIXED32: u64 = 5;

/// Skips a field of wire type `wire`, the way decoding skips a field it does not know
fn skip(buf: &mut &[u8], wire: u64, number: u64, depth: u32) -> Result<(), Malformed> {
    match wire {
        VARINT => varint(buf).map(drop),
        FIXED64 => take(buf, 8).map(drop),
        LEN => {
            let len = varint(buf)?;
            take(buf, len).map(drop)
        }
        FIXED32 => take(buf, 4).map(drop),
        START_GROUP => {
            if depth >= MAX_DEPTH {
                return Err(Malformed);
            }
            loop {
                let (inner, inner_wire) = key(buf)?;
                if inner_wire == END_GROUP {
                    return if inner == number {
                        Ok(())
                    } else {
                        Err(Malformed)
                    };
                }
                skip(buf, inner_wire, inner, depth + 1)?;
            }
        }
        _ => Err(Malformed),
    }
}

fn key(buf: &mut &[u8]) -> Result<(u64, u64), Malformed> {
    let key = varint(buf)?;
    if key > u64::from(u32::MAX) || key >> 3 == 0 {
        return Err(Malformed);
    }
    Ok((key >> 3, key & 7))
}

fn add(size: &mut usize, bytes: usize) {
    *size = size.saturating_add(bytes);
}

/// Adds what decoding `buf` as a `msg` allocates to `size`, up to where its framing breaks
fn scan(mut buf: &[u8], msg: Msg, depth: u32, size: &mut usize) -> Result<(), Malformed> {
    let mut vecs = Vecs::default();
    // The `Vec`s of a message cut short count too: decoding allocated them before failing
    let result = scan_fields(&mut buf, msg, depth, size, &mut vecs);
    vecs.add_to(size);
    result
}

fn scan_fields(
    buf: &mut &[u8],
    msg: Msg,
    depth: u32,
    size: &mut usize,
    vecs: &mut Vecs,
) -> Result<(), Malformed> {
    while !buf.is_empty() {
        let (number, wire) = key(buf)?;
        match (msg.field(number), wire) {
            (Some(Field { kind, slot }), LEN) => {
                let len = varint(buf)?;
                let body = take(buf, len)?;
                let elements = match kind {
                    Kind::Message(child) => {
                        if depth >= MAX_DEPTH {
                            return Err(Malformed);
                        }
                        scan(body, child, depth + 1, size)?;
                        1
                    }
                    Kind::Bytes => {
                        add(size, bytes_capacity(body.len()));
                        1
                    }
                    // Packed
                    Kind::Numbers(Encoding::Varint) => {
                        body.iter().filter(|&&byte| byte < 0x80).count()
                    }
                    Kind::Numbers(Encoding::Fixed64) => body.len() / size_of::<u64>(),
                };
                if let Some(slot) = slot {
                    vecs.push(slot, kind, elements);
                }
            }
            // One unpacked element
            (
                Some(Field {
                    kind: kind @ Kind::Numbers(encoding),
                    slot: Some(slot),
                }),
                VARINT | FIXED64,
            ) => {
                skip(buf, wire, number, depth)?;
                if (encoding == Encoding::Varint) == (wire == VARINT) {
                    vecs.push(slot, kind, 1);
                }
            }
            // A field that allocates nothing, a field the schema does not have, or a field
            // of the wrong wire type, which fails decoding
            _ => skip(buf, wire, number, depth)?,
        }
    }
    Ok(())
}
