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

//! Regression tests for what two reviews of chunked OTLP ingestion found: the
//! memory of rows whose shared maps were sized by repeated attribute keys, and
//! the memory of walking a body of millions of empty resource or scope levels.
//! Measured with the test allocator (layout bytes, calling thread). Each test
//! prints `REGRESSION <name>: ...`. None touches `PARSEABLE`, so they may be run
//! with the name filter `otel::regression_tests`.

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{Gauge, Metric, NumberDataPoint, metric};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use prost::Message as _;
use serde_json::{Map, Value};

use super::protobuf_stream::{Signal, count_points, flatten_request, max_record_bytes, outline};
use super::sink::{CHUNK_LIMITS, Chunker, RowSink, row_bytes};
use crate::test_alloc::measure;
use crate::utils::json::flatten::{FlattenBudget, FlattenLimits, JsonFlattenError};

const MIB: usize = 1024 * 1024;

/// What a request may make the server hold while its rows are built and staged,
/// as `memory_tests` states it: two chunks of rows and a little more
const PEAK_BOUND: usize = 2 * CHUNK_LIMITS.bytes + 8 * MIB;

/// `P_MAX_EVENT_PAYLOAD_SIZE` default
const PAYLOAD: usize = 10 * MIB;

fn mb(bytes: impl TryInto<i128>) -> f64 {
    bytes.try_into().ok().unwrap() as f64 / MIB as f64
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

/// 128 attributes that all have the key `k`: one field, counted as 128 by
/// `attribute_capacity`
fn repeated_keys() -> Vec<KeyValue> {
    (0..128).map(|_| kv("k", "v")).collect()
}

fn scope(attributes: Vec<KeyValue>) -> Option<InstrumentationScope> {
    Some(InstrumentationScope {
        name: "scope".into(),
        version: "1".into(),
        attributes,
        ..Default::default()
    })
}

fn resource(attributes: Vec<KeyValue>) -> Option<Resource> {
    Some(Resource {
        attributes,
        ..Default::default()
    })
}

/// Keeps the last row it is handed, and nothing else
#[derive(Default)]
struct Last(Option<Map<String, Value>>);

impl RowSink for Last {
    type Error = JsonFlattenError;

    fn row(&mut self, row: Map<String, Value>) -> Result<(), Self::Error> {
        self.0 = Some(row);
        Ok(())
    }
}

/// Builds one row into a `Last`
type BuildRow = Box<dyn Fn(&mut Last)>;

/// Bytes the row built by `build` holds once built, and its `row_bytes`
fn held_and_estimated(build: impl FnOnce(&mut Last)) -> (isize, usize) {
    let mut last = Last::default();
    let ((), allocated) = measure(|| build(&mut last));
    let row = last.0.expect("a row");
    (allocated.retained, row_bytes(&row))
}

/// Review finding 1: resource, scope, span and metric maps are sized from the
/// attribute count, which counts repeated keys, and `IndexMap::clone` keeps the
/// capacity, so a row cloned from such a map held about 10x what `row_bytes`
/// counts (a scope of 128 attributes with one key: 25,917 B against 2,613 B).
/// Every shared map now holds no more than its entries need before it is cloned.
///
/// The same holds for a log record's own fields. Span event, span link and metric
/// data point rows are still sized from their own attribute count, repeats
/// included: their slack is bounded by the record's wire bytes and was left
/// alone. They are held to what they measured when this was written (at
/// `98ae3903`: 3.09, 3.25 and 5.03 held/estimated) so that it does not grow.
#[test]
fn rows_copied_from_maps_with_repeated_keys_hold_what_row_bytes_counts() {
    let record = LogRecord {
        time_unix_nano: 1_700_000_000_000_000_000,
        observed_time_unix_nano: 1_700_000_000_000_000_000,
        ..Default::default()
    };
    let span = Span {
        trace_id: vec![1; 16],
        span_id: vec![2; 8],
        start_time_unix_nano: 1_700_000_000_000_000_000,
        end_time_unix_nano: 1_700_000_000_000_000_001,
        ..Default::default()
    };
    let span_with_event = Span {
        events: vec![Event {
            time_unix_nano: 1_700_000_000_000_000_000,
            name: "e".into(),
            ..Default::default()
        }],
        ..span.clone()
    };
    let gauge = |metadata: Vec<KeyValue>, attributes: Vec<KeyValue>| Metric {
        name: "m".into(),
        metadata,
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes,
                time_unix_nano: 1_700_000_000_000_000_000,
                ..Default::default()
            }],
        })),
        ..Default::default()
    };
    let with_identity = |mut attributes: Vec<KeyValue>| {
        attributes.push(kv("service.name", "api"));
        attributes
    };

    let mut cases: Vec<(&str, BuildRow)> = Vec::new();
    for (level, resource_attributes, scope_attributes) in [
        ("resource", repeated_keys(), vec![]),
        ("scope", vec![], repeated_keys()),
    ] {
        let (ra, sa) = (resource_attributes.clone(), scope_attributes.clone());
        let record = record.clone();
        cases.push((
            if level == "resource" {
                "logs, resource"
            } else {
                "logs, scope"
            },
            Box::new(move |sink: &mut Last| {
                let resource_fields =
                    super::logs::resource_log_fields(resource(ra.clone()).as_ref(), "");
                let scope_fields = super::logs::scope_log_fields(scope(sa.clone()).as_ref(), "");
                let row = super::logs::log_record_row(
                    &record,
                    &scope_fields,
                    &resource_fields,
                    &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                )
                .unwrap();
                drop((resource_fields, scope_fields));
                sink.row(row).unwrap();
            }),
        ));
    }
    // The record's own map is the row (or becomes it), so it must not keep the
    // room its repeated keys were counted for either
    let repeated_record = LogRecord {
        attributes: repeated_keys(),
        ..record.clone()
    };
    cases.push((
        "logs, record",
        Box::new(move |sink: &mut Last| {
            let resource_fields = super::logs::resource_log_fields(
                resource(vec![kv("service.name", "api")]).as_ref(),
                "",
            );
            let scope_fields = super::logs::scope_log_fields(scope(vec![]).as_ref(), "");
            let row = super::logs::log_record_row(
                &repeated_record,
                &scope_fields,
                &resource_fields,
                &mut FlattenBudget::new(FlattenLimits::DEFAULT),
            )
            .unwrap();
            drop((resource_fields, scope_fields));
            sink.row(row).unwrap();
        }),
    ));
    for (name, resource_attributes, scope_attributes, span) in [
        ("traces, resource", repeated_keys(), vec![], span.clone()),
        ("traces, scope", vec![], repeated_keys(), span.clone()),
        (
            "traces, span",
            vec![],
            vec![],
            Span {
                attributes: repeated_keys(),
                ..span.clone()
            },
        ),
        (
            "traces, span with an event",
            vec![],
            vec![],
            Span {
                attributes: repeated_keys(),
                ..span_with_event.clone()
            },
        ),
        (
            "traces, event",
            vec![kv("service.name", "api")],
            vec![],
            Span {
                attributes: vec![kv("http.method", "GET")],
                events: vec![Event {
                    attributes: repeated_keys(),
                    ..span_with_event.events[0].clone()
                }],
                ..span.clone()
            },
        ),
        (
            "traces, link",
            vec![kv("service.name", "api")],
            vec![],
            Span {
                attributes: vec![kv("http.method", "GET")],
                links: vec![Link {
                    trace_id: vec![3; 16],
                    span_id: vec![4; 8],
                    attributes: repeated_keys(),
                    ..Default::default()
                }],
                ..span.clone()
            },
        ),
    ] {
        cases.push((
            name,
            Box::new(move |sink: &mut Last| {
                let resource_fields = super::traces::resource_span_fields(
                    resource(resource_attributes.clone()).as_ref(),
                    "",
                );
                let scope_fields =
                    super::traces::scope_span_fields(scope(scope_attributes.clone()).as_ref());
                super::traces::span_rows(
                    &span,
                    &super::traces::SpanParents {
                        scope_fields: &scope_fields,
                        scope_schema_url: "",
                        resource_fields: &resource_fields,
                    },
                    sink,
                )
                .unwrap();
            }),
        ));
    }
    for (name, resource_attributes, scope_attributes, metadata, point, target_info) in [
        (
            "metrics, resource",
            with_identity(repeated_keys()),
            vec![],
            vec![],
            vec![],
            false,
        ),
        (
            "metrics, resource, target_info row",
            with_identity(repeated_keys()),
            vec![],
            vec![],
            vec![],
            true,
        ),
        (
            "metrics, scope",
            with_identity(vec![]),
            repeated_keys(),
            vec![],
            vec![],
            false,
        ),
        (
            "metrics, metric metadata",
            with_identity(vec![]),
            vec![],
            repeated_keys(),
            vec![],
            false,
        ),
        (
            "metrics, data point",
            with_identity(vec![]),
            vec![],
            vec![],
            repeated_keys(),
            false,
        ),
    ] {
        let metric = gauge(metadata, point);
        cases.push((
            name,
            Box::new(move |sink: &mut Last| {
                let fields = super::metrics::ResourceMetricFields::new(
                    resource(resource_attributes.clone()).as_ref(),
                    "",
                );
                let envelope = fields.scope_envelope(scope(scope_attributes.clone()).as_ref(), "");
                let mut latest = None;
                super::metrics::metric_rows_into(&metric, &envelope, false, &mut latest, sink)
                    .unwrap();
                if target_info {
                    sink.row(fields.target_info(latest).expect("a target_info row"))
                        .unwrap();
                }
            }),
        ));
    }

    let mut failed = Vec::new();
    for (name, build) in &cases {
        let (held, estimated) = held_and_estimated(build);
        let ratio = held as f64 / estimated as f64;
        println!(
            "REGRESSION repeated keys, {name}: row holds {held} B, row_bytes {estimated} B, held/estimated {ratio:.2}"
        );
        let bound = match *name {
            "traces, event" => 3.2,
            "traces, link" => 3.35,
            "metrics, data point" => 5.15,
            _ => 2.0,
        };
        if ratio > bound {
            failed.push(*name);
        }
    }

    // The reviewer's request: 65,536 records under that scope, 1.25 MiB of protobuf
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                scope: scope(repeated_keys()),
                log_records: vec![record; 1 << 16],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let (rows, allocated) = measure(|| {
        let mut rows = 0usize;
        let mut chunker = Chunker::new(CHUNK_LIMITS, |chunk: Vec<Value>, _bytes: usize| {
            rows += chunk.len();
            Ok::<_, JsonFlattenError>(())
        });
        super::logs::flatten_logs_into(
            &request.resource_logs,
            &mut FlattenBudget::new(FlattenLimits::DEFAULT),
            &mut chunker,
        )
        .and_then(|()| chunker.finish())
        .unwrap();
        drop(chunker);
        rows
    });
    println!(
        "REGRESSION repeated keys, request: protobuf {} KiB, {rows} rows, peak {:.1} MiB, bound {:.0} MiB",
        request.encoded_len() / 1024,
        mb(allocated.peak),
        mb(PEAK_BOUND)
    );
    assert!(
        failed.is_empty(),
        "rows hold more than their bound over row_bytes: {failed:?}"
    );
    assert_eq!(rows, 1 << 16);
    assert!(
        allocated.peak <= PEAK_BOUND,
        "peak {:.1} MiB",
        mb(allocated.peak)
    );
}

/// A body of `count` two-byte empty entries of field 1 (`resource_logs`,
/// `resource_metrics`, `resource_spans`): every one an empty resource level
fn empty_resources(count: usize) -> Bytes {
    Bytes::from([0x0a, 0x00].repeat(count))
}

/// One resource level holding `count` two-byte empty scope levels (field 2)
fn empty_scopes_in_one_resource(count: usize) -> Bytes {
    let scopes = [0x12, 0x00].repeat(count);
    let mut body = vec![0x0a];
    prost::encoding::encode_varint(scopes.len() as u64, &mut body);
    body.extend_from_slice(&scopes);
    Bytes::from(body)
}

/// Security review finding 1: the outer levels of a protobuf body were indexed
/// (32 bytes per entry) before the first was read, so a 10 MiB body of 5.2 million
/// empty two-byte `resource_logs` entries held 384 MiB while it was answered 200.
/// The levels are walked as they are read: what reading a body holds for them does
/// not grow with how many there are, for empty resources and for empty scopes of
/// one resource, in every pass over the body.
#[test]
fn outer_levels_are_read_in_memory_independent_of_their_count() {
    let max_record = max_record_bytes(PAYLOAD);
    for (shape, body) in [
        ("empty resources", empty_resources(PAYLOAD / 2)),
        (
            "empty scopes of one resource",
            empty_scopes_in_one_resource((PAYLOAD - 8) / 2),
        ),
    ] {
        assert!(body.len() <= PAYLOAD);
        for (name, signal) in [
            ("logs", Signal::Logs),
            ("metrics", Signal::Metrics),
            ("traces", Signal::Traces),
        ] {
            let (read, outlined) = measure(|| outline(&body, signal).expect("the body decodes"));
            let (rows, flattened) = measure(|| {
                let mut rows = 0usize;
                let mut chunker = Chunker::new(CHUNK_LIMITS, |chunk: Vec<Value>, _bytes: usize| {
                    rows += chunk.len();
                    Ok::<_, JsonFlattenError>(())
                });
                flatten_request(
                    &body,
                    signal,
                    max_record,
                    &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                    false,
                    &mut chunker,
                )
                .map_err(|_| "flatten failed")
                .and_then(|()| chunker.finish().map_err(|_| "staging failed"))
                .unwrap();
                drop(chunker);
                rows
            });
            let (points, counted) = measure(|| count_points(&body));
            println!(
                "REGRESSION outer levels, {shape}, {name}: body {:.1} MiB, {} records, {rows} rows, {points} points; peak: outline {} B, flatten_request {} B, count_points {} B",
                mb(body.len()),
                read.records,
                outlined.peak,
                flattened.peak,
                counted.peak
            );
            assert_eq!((read.records, rows, points), (0, 0, 0));
            for (pass, peak) in [
                ("outline", outlined.peak),
                ("flatten_request", flattened.peak),
                ("count_points", counted.peak),
            ] {
                assert!(peak < 64 * 1024, "{shape}, {name}: {pass} held {peak} B");
            }
        }
    }
}
