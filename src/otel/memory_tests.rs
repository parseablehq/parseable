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

//! Memory one OTLP request makes the server hold while its rows are built and
//! handed to staging, measured with the test allocator (`test_alloc`, layout
//! bytes on the calling thread). The decoded request is built before the
//! measurement starts. Each test prints `MEMORY <name>: ...` with its numbers.
//! None of them touches `PARSEABLE`, so they may be run with a name filter.

use crate::otel::sink::{CHUNK_LIMITS, Chunker, row_bytes};
use crate::test_alloc::{Allocated, measure};
use crate::utils::json::flatten::{FlattenBudget, FlattenLimits, JsonFlattenError};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use serde_json::Value;

const MIB: usize = 1024 * 1024;

/// What a request may make the server hold while its rows are built and staged:
/// one chunk of rows (whose real size stays within 2x of its estimate), the row
/// that filled it and the chunk's `Vec`. It does not grow with the rows of the
/// request.
const PEAK_BOUND: usize = 2 * CHUNK_LIMITS.bytes + 8 * MIB;

fn mb(bytes: impl TryInto<i128>) -> f64 {
    bytes.try_into().ok().unwrap() as f64 / MIB as f64
}

fn kv(key: impl Into<String>, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

/// Takes a chunk of rows, as staging does
type Stage<'a> = dyn FnMut(Vec<Value>, usize) -> Result<(), JsonFlattenError> + 'a;

/// Rows handed over, and the most rows a chunk held
#[derive(Debug, Default)]
struct Handed {
    rows: usize,
    largest_chunk: usize,
}

/// Hands the rows of a request to a consumer that drops every chunk once it is
/// full, as staging does, and reports what that allocated
fn hand_over<E: std::fmt::Debug>(
    name: &str,
    flatten: impl FnOnce(&mut Stage) -> Result<(), E>,
) -> (Result<Handed, String>, Allocated) {
    let (result, allocated) = measure(|| {
        let mut handed = Handed::default();
        let mut stage = |rows: Vec<Value>, _bytes: usize| {
            handed.rows += rows.len();
            handed.largest_chunk = handed.largest_chunk.max(rows.len());
            Ok(())
        };
        flatten(&mut stage).map_err(|e| format!("{e:?}"))?;
        Ok(handed)
    });
    println!(
        "MEMORY {name}: {:?}, peak {:.1} MiB, bound {:.0} MiB",
        result,
        mb(allocated.peak),
        mb(PEAK_BOUND)
    );
    (result, allocated)
}

fn traces(
    request: &ExportTraceServiceRequest,
) -> impl FnOnce(&mut Stage) -> Result<(), JsonFlattenError> + '_ {
    move |stage| {
        let mut chunker = Chunker::new(CHUNK_LIMITS, stage);
        crate::otel::traces::flatten_traces_into(&request.resource_spans, &mut chunker)?;
        chunker.finish()
    }
}

fn metrics(
    request: &ExportMetricsServiceRequest,
) -> impl FnOnce(&mut Stage) -> Result<(), JsonFlattenError> + '_ {
    move |stage| {
        let mut chunker = Chunker::new(CHUNK_LIMITS, stage);
        crate::otel::metrics::flatten_metrics_into(&request.resource_metrics, false, &mut chunker)?;
        chunker.finish()
    }
}

fn logs<'a>(
    request: &'a ExportLogsServiceRequest,
    budget: &'a mut FlattenBudget,
) -> impl FnOnce(&mut Stage) -> Result<(), JsonFlattenError> + 'a {
    move |stage| {
        let mut chunker = Chunker::new(CHUNK_LIMITS, stage);
        crate::otel::logs::flatten_logs_into(&request.resource_logs, budget, &mut chunker)?;
        chunker.finish()
    }
}

fn one_span(span: Span) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn one_metric(metric: Metric) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![metric],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Review finding 1: every event row of a span was built before the first was
/// handed over. 131,072 empty events (256 KiB of protobuf) held about 600 MiB.
#[test]
fn events_of_one_span_are_handed_over_in_bounded_memory() {
    let n = 1 << 17;
    let request = one_span(Span {
        start_time_unix_nano: 1,
        end_time_unix_nano: 2,
        events: vec![
            Event {
                time_unix_nano: 1,
                ..Default::default()
            };
            n
        ],
        ..Default::default()
    });
    let (result, allocated) = hand_over("events of one span", traces(&request));
    let handed = result.unwrap();
    assert_eq!(handed.rows, n);
    assert!(handed.largest_chunk < n);
    assert!(allocated.peak < PEAK_BOUND, "peak {} B", allocated.peak);
}

/// Same for the links of one span
#[test]
fn links_of_one_span_are_handed_over_in_bounded_memory() {
    let n = 1 << 16;
    let request = one_span(Span {
        start_time_unix_nano: 1,
        end_time_unix_nano: 2,
        links: vec![Link::default(); n],
        ..Default::default()
    });
    let (result, allocated) = hand_over("links of one span", traces(&request));
    assert_eq!(result.unwrap().rows, n);
    assert!(allocated.peak < PEAK_BOUND, "peak {} B", allocated.peak);
}

/// Same for the data points of one gauge
#[test]
fn data_points_of_one_metric_are_handed_over_in_bounded_memory() {
    let n = 1 << 17;
    let request = one_metric(Metric {
        name: "g".into(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![
                NumberDataPoint {
                    time_unix_nano: 1,
                    start_time_unix_nano: 1,
                    ..Default::default()
                };
                n
            ],
        })),
        ..Default::default()
    });
    let (result, allocated) = hand_over("points of one gauge", metrics(&request));
    assert_eq!(result.unwrap().rows, n);
    assert!(allocated.peak < PEAK_BOUND, "peak {} B", allocated.peak);
}

/// 1000 tiny resource attributes copied into every row of 2000 records: a 13 KB
/// body whose rows held about 400 MiB at once (review finding 3)
#[test]
fn tiny_copied_fields_are_handed_over_in_bounded_memory() {
    let attributes: Vec<KeyValue> = (0..1000)
        .map(|i| kv(format!("k{i:03}"), any_value::Value::IntValue(0)))
        .collect();
    let rows = 2000;
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: (0..rows)
                    .map(|i| LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
                        observed_time_unix_nano: 1_700_000_000_000_000_000,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut budget = FlattenBudget::new(FlattenLimits::DEFAULT);
    let (result, allocated) = hand_over("tiny copied fields", logs(&request, &mut budget));
    assert_eq!(result.unwrap().rows, rows);
    assert!(allocated.peak < PEAK_BOUND, "peak {} B", allocated.peak);
}

/// Review finding 7: a log body that is JSON text was parsed before anything was
/// charged. A 10 MiB body `{"a":[0,0,...]}` peaked at 586 MiB. It would parse into
/// more than one chunk's worth of memory, so it is kept as its text.
#[test]
fn json_text_log_body_is_not_parsed_into_unbounded_memory() {
    let mut text = String::with_capacity(10 * MIB);
    text.push_str(r#"{"a":["#);
    while text.len() < 10 * MIB - 4096 {
        text.push_str("0,");
    }
    text.push_str("0]}");
    let body_len = text.len();
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    observed_time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(text)),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut budget = FlattenBudget::new(FlattenLimits::DEFAULT);
    let (result, allocated) = hand_over("10 MiB JSON text body", logs(&request, &mut budget));
    assert_eq!(result.unwrap().rows, 1, "the record is kept, its body as text");
    // The row holds copies of the body text, nothing parsed from it
    assert!(
        allocated.peak < PEAK_BOUND + 3 * body_len,
        "peak {} B",
        allocated.peak
    );
}

/// Review finding 4: an array of objects with distinct keys becomes a column per
/// key with a cell for every object: 3,600 exemplars held 890 MiB once their row
/// was structurally flattened
#[test]
fn exemplar_column_tables_are_bounded() {
    let exemplars: Vec<Exemplar> = (0..3_600)
        .map(|i| Exemplar {
            filtered_attributes: vec![kv(format!("k{i}"), any_value::Value::IntValue(1))],
            time_unix_nano: 1,
            ..Default::default()
        })
        .collect();
    let request = one_metric(Metric {
        name: "g".into(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                time_unix_nano: 1,
                start_time_unix_nano: 1,
                exemplars,
                ..Default::default()
            }],
        })),
        ..Default::default()
    });
    let mut rows = Vec::new();
    crate::otel::metrics::flatten_metrics_into(&request.resource_metrics, false, &mut rows)
        .unwrap();
    let mut row = rows.pop().unwrap();
    let estimate = row_bytes(row.as_object().unwrap());
    let mut budget = FlattenBudget::new(FlattenLimits::DEFAULT);
    let (result, allocated) = measure(|| {
        crate::utils::json::flatten::flatten(&mut row, "_", None, None, None, false, &mut budget)
    });
    println!(
        "MEMORY exemplar table: {:?}, estimate {:.1} MiB, peak {:.1} MiB",
        result.as_ref().map_err(ToString::to_string),
        mb(estimate),
        mb(allocated.peak)
    );
    // The chunk estimate counts the table, so such a row fills a chunk on its own
    assert!(estimate > 3_600 * 3_600 * 72);
    assert!(result.is_err());
    assert!(allocated.peak < PEAK_BOUND, "peak {} B", allocated.peak);
}

/// `count` attributes without a value: two bytes each on the wire, skipped when rows
/// are built
fn valueless(count: usize) -> Vec<KeyValue> {
    vec![KeyValue::default(); count]
}

const VALUELESS: usize = 50_000;

/// Maps sized by the raw attribute count of the request held room for every
/// attribute without a value: 50,000 of them (100 KB of protobuf) made a row map
/// reserve about 5.5 MB
#[test]
fn valueless_attributes_do_not_size_row_maps() {
    let point = NumberDataPoint {
        attributes: valueless(VALUELESS),
        time_unix_nano: 1,
        start_time_unix_nano: 1,
        exemplars: vec![Exemplar {
            filtered_attributes: valueless(VALUELESS),
            time_unix_nano: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    let metric = |data| Metric {
        name: "m".into(),
        metadata: valueless(VALUELESS),
        data: Some(data),
        ..Default::default()
    };
    for (name, metric) in [
        (
            "gauge point",
            metric(metric::Data::Gauge(Gauge {
                data_points: vec![point.clone()],
            })),
        ),
        (
            "metric without points",
            metric(metric::Data::Gauge(Gauge::default())),
        ),
    ] {
        let (rows, allocated) =
            measure(|| crate::otel::metrics::flatten_metrics_record(&metric, false));
        println!(
            "MEMORY valueless attributes, {name}: {} rows, retained {} B, peak {} B",
            rows.len(),
            allocated.retained,
            allocated.peak
        );
        assert_eq!(rows.len(), 1);
        assert!(
            allocated.peak < 256 * 1024,
            "{name}: peak {} B",
            allocated.peak
        );
    }
}

/// Same for the maps built while a row is: event and link attributes, and the
/// entries of key-value lists and arrays
#[test]
fn valueless_attributes_do_not_size_temporary_maps() {
    let list = |values: Vec<KeyValue>| {
        vec![kv(
            "list",
            any_value::Value::KvlistValue(opentelemetry_proto::tonic::common::v1::KeyValueList {
                values,
            }),
        )]
    };
    let array = vec![kv(
        "array",
        any_value::Value::ArrayValue(opentelemetry_proto::tonic::common::v1::ArrayValue {
            values: vec![AnyValue::default(); VALUELESS],
        }),
    )];
    let span = Span {
        start_time_unix_nano: 1,
        end_time_unix_nano: 2,
        attributes: [list(valueless(VALUELESS)), array].concat(),
        events: vec![Event {
            time_unix_nano: 1,
            attributes: valueless(VALUELESS),
            ..Default::default()
        }],
        links: vec![Link {
            attributes: valueless(VALUELESS),
            ..Default::default()
        }],
        ..Default::default()
    };
    let request = one_span(span);
    let (rows, allocated) = measure(|| {
        let mut rows = Vec::new();
        crate::otel::traces::flatten_traces_into(&request.resource_spans, &mut rows).unwrap();
        rows
    });
    println!(
        "MEMORY valueless attributes, span: {} rows, peak {} B",
        rows.len(),
        allocated.peak
    );
    assert_eq!(rows.len(), 2);
    // Converting an attribute clones its decoded value first: the 50,000 entries of
    // the list and of the array, as decoded, are held once more
    let clones = VALUELESS * (std::mem::size_of::<KeyValue>() + std::mem::size_of::<AnyValue>());
    assert!(
        allocated.peak < clones + 256 * 1024,
        "peak {} B, clones {clones} B",
        allocated.peak
    );
}

/// The chunk limit is an estimate of the memory rows hold (`row_bytes`); it must
/// track what they really allocate, for realistic rows and for rows of many tiny
/// fields alike (review finding 3: JSON length was 18.5x off)
#[test]
fn row_estimate_tracks_allocated_memory() {
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;

    let realistic: Vec<KeyValue> = [
        ("service.name", "checkout"),
        ("service.namespace", "shop"),
        ("service.instance.id", "checkout-7d9f8b6c4-x2x9q"),
        ("k8s.namespace.name", "production"),
        ("k8s.pod.name", "checkout-7d9f8b6c4-x2x9q"),
        ("k8s.pod.uid", "6b1a3c1e-2f0e-4d5a-9a43-77b8c2d1e0f1"),
        ("k8s.deployment.name", "checkout"),
        ("k8s.node.name", "ip-10-0-12-34.ec2.internal"),
        ("k8s.cluster.name", "prod-eu-west-1"),
        ("host.name", "ip-10-0-12-34.ec2.internal"),
        ("cloud.provider", "aws"),
        ("cloud.region", "eu-west-1"),
        ("container.name", "checkout"),
        ("container.image.name", "registry.example.com/shop/checkout"),
        ("container.image.tag", "1.42.3"),
        ("telemetry.sdk.name", "opentelemetry"),
        ("telemetry.sdk.language", "java"),
        ("telemetry.sdk.version", "1.38.0"),
    ]
    .into_iter()
    .map(|(k, v)| kv(k, any_value::Value::StringValue(v.into())))
    .collect();
    let tiny: Vec<KeyValue> = (0..1000)
        .map(|i| kv(format!("k{i:03}"), any_value::Value::IntValue(0)))
        .collect();
    for (name, resource, record_attributes) in
        [("realistic", realistic, 6), ("tiny fields", tiny, 0)]
    {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: resource,
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "io.opentelemetry.logback".into(),
                        version: "2.6.0".into(),
                        ..Default::default()
                    }),
                    log_records: (0..500u64)
                        .map(|i| LogRecord {
                            time_unix_nano: 1_700_000_000_000_000_000 + i,
                            observed_time_unix_nano: 1_700_000_000_000_000_000 + i,
                            severity_number: 9,
                            severity_text: "INFO".into(),
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(format!(
                                    "GET /api/v1/orders/{i} 200 in 12ms"
                                ))),
                            }),
                            attributes: (0..record_attributes)
                                .map(|a| {
                                    kv(format!("attr.{a}"), any_value::Value::IntValue(i as i64))
                                })
                                .collect(),
                            trace_id: vec![7; 16],
                            span_id: vec![3; 8],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let (rows, allocated) = measure(|| {
            let mut rows = Vec::new();
            crate::otel::logs::flatten_logs_into(
                &request.resource_logs,
                &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                &mut rows,
            )
            .unwrap();
            rows
        });
        let estimate: usize = rows
            .iter()
            .map(|row| row_bytes(row.as_object().unwrap()))
            .sum();
        let ratio = allocated.retained as f64 / estimate as f64;
        println!(
            "MEMORY row estimate, {name}: {} rows, allocated {} B, estimated {} B, allocated/estimated {:.2}",
            rows.len(),
            allocated.retained,
            estimate,
            ratio,
        );
        assert!((0.5..=2.0).contains(&ratio), "{name}: {ratio:.2}");
    }
}

/// Resident and peak resident memory of this process in bytes, from
/// `/proc/self/statm` and `VmHWM`
#[cfg(target_os = "linux")]
fn resident() -> (usize, usize) {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let pages: usize = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let hwm: usize = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|kb| kb.trim().trim_end_matches(" kB").trim().parse().ok())
        .unwrap();
    (pages * 4096, hwm * 1024)
}

/// Resident memory of decoding a request and handing its rows over in chunks.
/// Release build, run alone:
/// `cargo test --release --lib -- --ignored --exact otel::memory_tests::resident_memory_of_requests --nocapture`
#[test]
#[cfg(target_os = "linux")]
#[ignore = "measures the resident memory of the process; run it alone"]
fn resident_memory_of_requests() {
    use prost::Message;

    fn measure_case(name: &str, signal: &str, body: &[u8]) {
        // VmHWM is reset to the current resident size
        std::fs::write("/proc/self/clear_refs", "5").unwrap();
        let (before, _) = resident();
        let mut rows = 0usize;
        let mut stage = |chunk: Vec<Value>, _bytes: usize| {
            rows += chunk.len();
            Ok::<_, JsonFlattenError>(())
        };
        let mut chunker = Chunker::new(CHUNK_LIMITS, &mut stage);
        let (decoded, after_decode) = match signal {
            "logs" => {
                let request = ExportLogsServiceRequest::decode(body).unwrap();
                let decoded = resident().0;
                crate::otel::logs::flatten_logs_into(
                    &request.resource_logs,
                    &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                    &mut chunker,
                )
                .unwrap();
                (decoded, resident().0)
            }
            "traces" => {
                let request = ExportTraceServiceRequest::decode(body).unwrap();
                let decoded = resident().0;
                crate::otel::traces::flatten_traces_into(&request.resource_spans, &mut chunker)
                    .unwrap();
                (decoded, resident().0)
            }
            _ => {
                let request = ExportMetricsServiceRequest::decode(body).unwrap();
                let decoded = resident().0;
                crate::otel::metrics::flatten_metrics_into(
                    &request.resource_metrics,
                    false,
                    &mut chunker,
                )
                .unwrap();
                (decoded, resident().0)
            }
        };
        chunker.finish().unwrap();
        drop(chunker);
        let (_, hwm) = resident();
        println!(
            "RESIDENT {name}: body {:.1} MiB, {rows} rows, decoded +{:.0} MiB, peak +{:.0} MiB over the {:.0} MiB before (decode included), {:.0} MiB at the end",
            mb(body.len()),
            mb(decoded.saturating_sub(before)),
            mb(hwm.saturating_sub(before)),
            mb(before),
            mb(after_decode)
        );
    }

    // One case per process, so that memory freed by an earlier case does not hide
    // what a later one takes: `RESIDENT_CASE` is `events`, `tiny`, or
    // `<signal>:<path of a protobuf body>` (see `dump_realistic_bodies`)
    let case = std::env::var("RESIDENT_CASE").expect("RESIDENT_CASE");
    match case.split_once(':') {
        // The two worst cases of the review: 2,097,152 empty events of one span
        // (4 MiB), and 1000 tiny resource attributes copied into 5000 rows
        None if case == "events" => {
            let body = one_span(Span {
                events: vec![Event::default(); 1 << 21],
                ..Default::default()
            })
            .encode_to_vec();
            measure_case("2,097,152 empty events of one span", "traces", &body);
        }
        None if case == "tiny" => {
            let body = ExportLogsServiceRequest {
                resource_logs: vec![ResourceLogs {
                    resource: Some(Resource {
                        attributes: (0..1000)
                            .map(|i| kv(format!("k{i:03}"), any_value::Value::IntValue(0)))
                            .collect(),
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: (0..5000u64)
                            .map(|i| LogRecord {
                                time_unix_nano: 1_700_000_000_000_000_000 + i,
                                observed_time_unix_nano: 1_700_000_000_000_000_000,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec();
            measure_case(
                "1000 tiny resource attributes x 5000 records",
                "logs",
                &body,
            );
        }
        Some((signal, path)) => {
            let body = std::fs::read(path).unwrap();
            measure_case(path, signal, &body);
        }
        None => panic!("unknown RESIDENT_CASE {case}"),
    }
}
