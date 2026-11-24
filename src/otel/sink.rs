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

//! Rows of an OTLP request, handed over one at a time.
//!
//! The flatteners build one row per log record, span event or link (or span), and
//! metric data point, and hand each to a [`RowSink`] as soon as it is built. A
//! [`Chunker`] collects them into chunks limited by rows and by an estimate of the
//! memory the rows hold ([`row_bytes`]), and passes every full chunk on before the
//! next is built. So the rows of a request are never all held at once.

use std::collections::HashSet;
use std::mem::size_of;

use serde_json::{Map, Value};

use crate::utils::json::flatten::JsonFlattenError;

/// Receives the rows of one request, in order, as they are built
pub trait RowSink {
    type Error: From<JsonFlattenError>;

    /// Takes one finished row
    fn row(&mut self, row: Map<String, Value>) -> Result<(), Self::Error>;

    /// Takes one finished row that is one metric data point
    fn point(&mut self, row: Map<String, Value>) -> Result<(), Self::Error> {
        self.row(row)
    }

    /// Every row of one leaf record (a log record, a span or a metric) was handed over
    fn record_end(&mut self) {}
}

/// Collects every row, for callers that want them all at once (tests, benchmarks)
impl RowSink for Vec<Value> {
    type Error = JsonFlattenError;

    fn row(&mut self, row: Map<String, Value>) -> Result<(), Self::Error> {
        self.push(Value::Object(row));
        Ok(())
    }
}

/// `size_of::<Value>()`: every value in a map or array takes this much in place
pub const VALUE_BYTES: usize = size_of::<Value>();

/// One entry of a row map (`serde_json::Map` is an `IndexMap`): its bucket holds the
/// hash, the key `String` and the `Value` (104 B), the hash table one index and one
/// control byte, and both grow by doubling, so on average about a third more
/// capacity is held than used
pub const ENTRY_BYTES: usize = 128;

/// What the system allocator adds to one heap block (header and rounding), counted
/// so that rows of many tiny strings are not underestimated
pub const HEAP_BLOCK_BYTES: usize = 16;

/// Estimated bytes of the heap block of a string with this capacity
#[inline]
pub fn heap_bytes(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity + HEAP_BLOCK_BYTES
    }
}

/// Estimate of the memory one row holds, including the cells of the column tables
/// that structural flattening later builds from its arrays of objects (one `Value`
/// per distinct key and element, see `flatten_array_objects`). It tracks the real
/// `Value` memory, not the length of the row as JSON: a field `"a":0` is 6 bytes of
/// JSON and about 150 bytes of memory.
pub fn row_bytes(row: &Map<String, Value>) -> usize {
    VALUE_BYTES + map_bytes(row)
}

fn map_bytes(map: &Map<String, Value>) -> usize {
    let mut bytes = 0;
    for (key, value) in map {
        bytes += ENTRY_BYTES + heap_bytes(key.capacity()) + value_heap_bytes(value);
    }
    bytes
}

/// Heap bytes owned by `value` (its own `VALUE_BYTES` are counted by its container)
fn value_heap_bytes(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(s) => heap_bytes(s.capacity()),
        Value::Object(map) => map_bytes(map),
        Value::Array(values) => array_bytes(values),
    }
}

fn array_bytes(values: &Vec<Value>) -> usize {
    let mut bytes = heap_bytes(values.capacity() * VALUE_BYTES);
    let mut objects = false;
    for value in values {
        objects |= value.is_object();
        bytes += value_heap_bytes(value);
    }
    if objects {
        // Structural flattening turns the elements into one column per distinct key,
        // with a cell for every element
        let keys: HashSet<&str> = values
            .iter()
            .filter_map(Value::as_object)
            .flat_map(|object| object.keys().map(String::as_str))
            .collect();
        bytes += keys.len() * (ENTRY_BYTES + values.len() * VALUE_BYTES);
    }
    bytes
}

/// Limits of one chunk of rows
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLimits {
    /// Rows of one chunk
    pub rows: usize,
    /// Estimated memory of the rows of one chunk, see [`row_bytes`]
    pub bytes: usize,
}

/// Default chunk limits, see `REPORT.md` of the change that introduced them for the
/// measurements. A chunk is full at 8192 rows (the default batch size of the
/// OpenTelemetry collector) or 16 MiB of estimated row memory, whichever comes first.
/// Rows of realistic requests hold 1-10 KiB each, so the memory limit usually decides.
pub const CHUNK_LIMITS: ChunkLimits = ChunkLimits {
    rows: 8192,
    bytes: 16 * 1024 * 1024,
};

/// What a [`Chunker`] passed on
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Staged {
    /// Chunks passed on
    pub chunks: u64,
    /// Rows passed on
    pub rows: u64,
    /// Leaf records all of whose rows were passed on
    pub records: u64,
    /// Data point rows passed on
    pub points: u64,
}

/// Row bytes per payload byte one request may expand to: the rows of a request
/// may hold at most `EXPANSION_FACTOR` times `P_MAX_EVENT_PAYLOAD_SIZE` of
/// estimated memory ([`row_bytes`]) in all, counted while they are staged.
///
/// Every row carries its own copy of the resource, scope, span and metric fields,
/// so a small request can expand to a very large number of row bytes: 15 KB of
/// resource attributes copied into each of 5 million two-byte log records is tens
/// of GiB. Chunking bounds the memory this takes at once, not what is written to
/// staging. The most measured for a request shaped like collector traffic is
/// below 500 (short log records or bare gauge points under 30 Kubernetes resource
/// attributes; ordinary k8s logs, spans and metrics expand 40-110x, see
/// `realistic_tests`), so 1024 leaves about 2x headroom, and stops the requests
/// that copy kilobytes of shared fields into millions of records of a few bytes,
/// at 10 GiB of rows for the default 10 MiB payload limit.
pub const EXPANSION_FACTOR: u64 = 1024;

/// Estimated bytes of rows one request may expand to, for a payload limit of
/// `max_payload` bytes
pub fn max_expanded_bytes(max_payload: usize) -> u64 {
    (max_payload as u64).saturating_mul(EXPANSION_FACTOR)
}

/// Collects rows into chunks and passes every full chunk to `stage`, with the
/// estimated bytes of its rows, dropping it before the next row is taken. Call
/// [`Chunker::finish`] to pass on the rest.
pub struct Chunker<F> {
    limits: ChunkLimits,
    stage: F,
    rows: Vec<Value>,
    bytes: usize,
    /// Records and data points whose rows are all in `rows`, not yet passed on
    pending_records: u64,
    pending_points: u64,
    staged: Staged,
}

impl<F, E> Chunker<F>
where
    F: FnMut(Vec<Value>, usize) -> Result<(), E>,
    E: From<JsonFlattenError>,
{
    pub fn new(limits: ChunkLimits, stage: F) -> Self {
        Self {
            limits,
            stage,
            rows: Vec::new(),
            bytes: 0,
            pending_records: 0,
            pending_points: 0,
            staged: Staged::default(),
        }
    }

    /// What was passed on so far
    pub fn staged(&self) -> Staged {
        self.staged
    }

    fn flush(&mut self) -> Result<(), E> {
        let rows = std::mem::take(&mut self.rows);
        let count = rows.len() as u64;
        let bytes = std::mem::take(&mut self.bytes);
        (self.stage)(rows, bytes)?;
        self.staged.chunks += 1;
        self.staged.rows += count;
        self.staged.records += std::mem::take(&mut self.pending_records);
        self.staged.points += std::mem::take(&mut self.pending_points);
        Ok(())
    }

    /// Passes on the rows taken since the last full chunk, if any
    pub fn finish(&mut self) -> Result<(), E> {
        if !self.rows.is_empty() {
            self.flush()?;
        }
        Ok(())
    }
}

impl<F, E> RowSink for Chunker<F>
where
    F: FnMut(Vec<Value>, usize) -> Result<(), E>,
    E: From<JsonFlattenError>,
{
    type Error = E;

    fn row(&mut self, row: Map<String, Value>) -> Result<(), E> {
        self.bytes += row_bytes(&row);
        self.rows.push(Value::Object(row));
        if self.rows.len() >= self.limits.rows || self.bytes >= self.limits.bytes {
            self.flush()?;
        }
        Ok(())
    }

    fn point(&mut self, row: Map<String, Value>) -> Result<(), E> {
        // Counted with the chunk it is in, which `row` may already have passed on
        let flushed_before = self.staged.chunks;
        self.row(row)?;
        if self.staged.chunks == flushed_before {
            self.pending_points += 1;
        } else {
            self.staged.points += 1;
        }
        Ok(())
    }

    fn record_end(&mut self) {
        if self.rows.is_empty() {
            // Its last row (if it had any) filled a chunk that was passed on
            self.staged.records += 1;
        } else {
            self.pending_records += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn object(value: Value) -> Map<String, Value> {
        match value {
            Value::Object(map) => map,
            _ => unreachable!(),
        }
    }

    #[test]
    fn value_is_72_bytes() {
        assert_eq!(VALUE_BYTES, 72);
    }

    #[test]
    fn chunks_are_cut_by_rows_and_by_bytes() {
        let mut chunks: Vec<usize> = Vec::new();
        let mut chunker = Chunker::new(
            ChunkLimits {
                rows: 3,
                bytes: usize::MAX,
            },
            |rows, _| {
                chunks.push(rows.len());
                Ok::<_, JsonFlattenError>(())
            },
        );
        for i in 0..7 {
            chunker.row(object(json!({ "i": i }))).unwrap();
            chunker.record_end();
        }
        chunker.finish().unwrap();
        let staged = chunker.staged();
        drop(chunker);
        assert_eq!(chunks, [3, 3, 1]);
        assert_eq!(staged.rows, 7);
        assert_eq!(staged.records, 7);
        assert_eq!(staged.chunks, 3);

        let row = object(json!({ "a": "x" }));
        let one = row_bytes(&row);
        let mut chunks: Vec<usize> = Vec::new();
        let mut chunker = Chunker::new(
            ChunkLimits {
                rows: usize::MAX,
                bytes: 2 * one,
            },
            |rows, _| {
                chunks.push(rows.len());
                Ok::<_, JsonFlattenError>(())
            },
        );
        for _ in 0..5 {
            chunker.row(row.clone()).unwrap();
        }
        chunker.finish().unwrap();
        drop(chunker);
        assert_eq!(chunks, [2, 2, 1]);
    }

    #[test]
    fn a_record_counts_once_all_its_rows_are_passed_on() {
        let mut fail_at = 2;
        let mut chunker = Chunker::new(
            ChunkLimits {
                rows: 2,
                bytes: usize::MAX,
            },
            |_, _| {
                fail_at -= 1;
                if fail_at == 0 {
                    Err(JsonFlattenError::CannotFlatten)
                } else {
                    Ok(())
                }
            },
        );
        // Record 1: rows 1-3 (second chunk fails); record 0: row 0
        chunker.point(object(json!({ "r": 0 }))).unwrap();
        chunker.record_end();
        chunker.point(object(json!({ "r": 1 }))).unwrap();
        assert_eq!(chunker.staged().records, 1);
        assert_eq!(chunker.staged().points, 2);
        chunker.point(object(json!({ "r": 2 }))).unwrap();
        assert!(chunker.point(object(json!({ "r": 3 }))).is_err());
        assert_eq!(chunker.staged().records, 1);
        assert_eq!(chunker.staged().points, 2);
        assert_eq!(chunker.staged().rows, 2);
    }

    #[test]
    fn records_without_rows_count_when_finished() {
        let mut chunker = Chunker::new(CHUNK_LIMITS, |_, _| Ok::<_, JsonFlattenError>(()));
        chunker.record_end();
        chunker.finish().unwrap();
        let staged = chunker.staged();
        assert_eq!(staged.records, 1);
        assert_eq!(staged.chunks, 0);
    }

    #[test]
    fn row_bytes_counts_the_column_table_of_arrays_of_objects() {
        let objects: Vec<Value> = (0..100).map(|i| json!({ format!("k{i}"): 1 })).collect();
        let row = object(json!({ "exemplars": objects }));
        // 100 distinct keys x 100 elements
        assert!(row_bytes(&row) > 100 * 100 * VALUE_BYTES);
        let same: Vec<Value> = (0..100).map(|_| json!({ "k": 1 })).collect();
        let row = object(json!({ "exemplars": same }));
        assert!(row_bytes(&row) < 100 * 3 * ENTRY_BYTES);
    }
}
