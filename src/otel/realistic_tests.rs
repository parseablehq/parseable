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

//! Requests shaped like real collector traffic, at the default payload limit:
//! what they expand to per body byte (the numbers behind `EXPANSION_FACTOR`),
//! and that the expansion cap accepts every one of them. Nothing here touches
//! `PARSEABLE`. `OTLP_DUMP_DIR=<dir>` makes the ignored `dump_realistic_bodies`
//! write the bodies for end-to-end checks against a server.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, exemplar,
    exponential_histogram_data_point::Buckets, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span};
use prost::Message;

use super::expansion::{RequestSize, logs_size, max_expanded_bytes, metrics_size, traces_size};

/// `P_MAX_EVENT_PAYLOAD_SIZE` default
const PAYLOAD: usize = 10 * 1024 * 1024;

fn kv(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

fn s(key: &str, value: &str) -> KeyValue {
    kv(key, any_value::Value::StringValue(value.into()))
}

/// What the Kubernetes attributes processor and an SDK add to a resource
fn k8s_resource() -> Option<Resource> {
    Some(Resource {
        attributes: vec![
            s("service.name", "checkout"),
            s("service.namespace", "shop"),
            s("service.version", "1.42.0"),
            s(
                "service.instance.id",
                "6f1c0a52-8a4e-4c21-9a07-3b3c4e9b0d51",
            ),
            s("deployment.environment.name", "production"),
            s("telemetry.sdk.name", "opentelemetry"),
            s("telemetry.sdk.language", "java"),
            s("telemetry.sdk.version", "1.44.1"),
            s(
                "telemetry.distro.name",
                "opentelemetry-java-instrumentation",
            ),
            s("telemetry.distro.version", "2.10.0"),
            s("host.name", "ip-10-0-12-34.eu-west-1.compute.internal"),
            s("host.arch", "amd64"),
            s("os.type", "linux"),
            s("os.description", "Linux 6.1.112-124.190.amzn2023.x86_64"),
            s("process.runtime.name", "OpenJDK Runtime Environment"),
            s("process.runtime.version", "21.0.5+11-LTS"),
            s("process.pid", "1"),
            s(
                "container.id",
                "3c5a2d0f9b8e7a6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c",
            ),
            s("container.name", "checkout"),
            s("container.image.name", "registry.example.com/shop/checkout"),
            s("container.image.tag", "1.42.0"),
            s("k8s.cluster.name", "prod-eu-west-1"),
            s("k8s.namespace.name", "shop"),
            s("k8s.node.name", "ip-10-0-12-34.eu-west-1.compute.internal"),
            s("k8s.pod.name", "checkout-7d9c8b6f5-x2x9q"),
            s("k8s.pod.uid", "0b6e2c1a-4f3d-4e5b-8a9c-7d6e5f4a3b2c"),
            s("k8s.deployment.name", "checkout"),
            s("k8s.replicaset.name", "checkout-7d9c8b6f5"),
            s("cloud.provider", "aws"),
            s("cloud.region", "eu-west-1"),
        ],
        ..Default::default()
    })
}

fn minimal_resource() -> Option<Resource> {
    Some(Resource {
        attributes: vec![s("service.name", "checkout")],
        ..Default::default()
    })
}

fn scope(name: &str) -> Option<InstrumentationScope> {
    Some(InstrumentationScope {
        name: name.into(),
        version: "2.10.0".into(),
        ..Default::default()
    })
}

const T0: u64 = 1_700_000_000_000_000_000;

fn logs(resource: Option<Resource>, records: Vec<LogRecord>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource,
            scope_logs: vec![ScopeLogs {
                scope: scope("com.example.checkout"),
                log_records: records,
                schema_url: String::new(),
            }],
            schema_url: "https://opentelemetry.io/schemas/1.24.0".into(),
        }],
    }
}

fn app_log(i: usize) -> LogRecord {
    LogRecord {
        time_unix_nano: T0 + i as u64,
        observed_time_unix_nano: T0 + 1 + i as u64,
        severity_number: 9,
        severity_text: "INFO".into(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(format!(
                "GET /api/v1/items/{i} completed with 200 in 12 ms"
            ))),
        }),
        attributes: vec![
            s("code.function", "handle"),
            s("thread.name", "http-nio-8080-exec-7"),
            kv("http.response.status_code", any_value::Value::IntValue(200)),
        ],
        trace_id: vec![0xab; 16],
        span_id: vec![0xcd; 8],
        flags: 1,
        ..Default::default()
    }
}

/// The smallest realistic log record: a time, a severity and a two-byte body
fn short_log(i: usize) -> LogRecord {
    LogRecord {
        time_unix_nano: T0 + i as u64,
        severity_number: 9,
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue("ok".into())),
        }),
        ..Default::default()
    }
}

fn traces(resource: Option<Resource>, spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource,
            scope_spans: vec![ScopeSpans {
                scope: scope("io.opentelemetry.tomcat-10.0"),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn server_span(i: usize) -> Span {
    Span {
        trace_id: vec![0xab; 16],
        span_id: (i as u64).to_be_bytes().to_vec(),
        parent_span_id: vec![0xef; 8],
        name: "GET /api/v1/items/{id}".into(),
        kind: 2,
        start_time_unix_nano: T0 + i as u64,
        end_time_unix_nano: T0 + 12_000_000 + i as u64,
        attributes: vec![
            s("http.request.method", "GET"),
            s("url.path", &format!("/api/v1/items/{i}")),
            kv("http.response.status_code", any_value::Value::IntValue(200)),
            s("server.address", "checkout.shop.svc.cluster.local"),
        ],
        status: Some(Status::default()),
        ..Default::default()
    }
}

/// The smallest realistic span: ids, a name and its times
fn bare_span(i: usize) -> Span {
    Span {
        trace_id: vec![0xab; 16],
        span_id: (i as u64).to_be_bytes().to_vec(),
        name: "query".into(),
        start_time_unix_nano: T0 + i as u64,
        end_time_unix_nano: T0 + 1000 + i as u64,
        ..Default::default()
    }
}

/// A span with an exception event and a link, as instrumentations record them
fn span_with_event(i: usize) -> Span {
    Span {
        events: vec![span::Event {
            time_unix_nano: T0 + 5 + i as u64,
            name: "exception".into(),
            attributes: vec![
                s("exception.type", "java.io.IOException"),
                s("exception.message", "Connection reset by peer"),
            ],
            ..Default::default()
        }],
        links: vec![span::Link {
            trace_id: vec![0x12; 16],
            span_id: vec![0x34; 8],
            ..Default::default()
        }],
        ..server_span(i)
    }
}

fn metrics(resource: Option<Resource>, metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource,
            scope_metrics: vec![ScopeMetrics {
                scope: scope("io.opentelemetry.runtime-telemetry-java17"),
                metrics,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn sum_points(points: usize) -> Vec<Metric> {
    vec![Metric {
        name: "jvm.memory.used".into(),
        description: "Measure of memory used.".into(),
        unit: "By".into(),
        data: Some(metric::Data::Sum(Sum {
            data_points: (0..points)
                .map(|i| NumberDataPoint {
                    attributes: vec![
                        s("jvm.memory.pool.name", "G1 Eden Space"),
                        s("jvm.memory.type", "heap"),
                    ],
                    start_time_unix_nano: T0,
                    time_unix_nano: T0 + 60_000_000_000 + i as u64,
                    value: Some(number_data_point::Value::AsInt(123_456_789 + i as i64)),
                    ..Default::default()
                })
                .collect(),
            aggregation_temporality: 2,
            is_monotonic: false,
        })),
        ..Default::default()
    }]
}

/// The smallest realistic data point: a time and a value
fn bare_gauge_points(points: usize) -> Vec<Metric> {
    vec![Metric {
        name: "up".into(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: (0..points)
                .map(|i| NumberDataPoint {
                    time_unix_nano: T0 + i as u64,
                    value: Some(number_data_point::Value::AsDouble(1.0)),
                    ..Default::default()
                })
                .collect(),
        })),
        ..Default::default()
    }]
}

/// Explicit-bucket histograms with the SDK's 16 default bounds and one exemplar
fn histogram_points(points: usize) -> Vec<Metric> {
    vec![Metric {
        name: "http.server.request.duration".into(),
        unit: "s".into(),
        data: Some(metric::Data::Histogram(Histogram {
            data_points: (0..points)
                .map(|i| HistogramDataPoint {
                    attributes: vec![
                        s("http.request.method", "GET"),
                        s("http.route", "/api/v1/items/{id}"),
                        kv("http.response.status_code", any_value::Value::IntValue(200)),
                    ],
                    start_time_unix_nano: T0,
                    time_unix_nano: T0 + 60_000_000_000 + i as u64,
                    count: 1000,
                    sum: Some(12.5),
                    bucket_counts: (0..16).map(|b| b * 3).collect(),
                    explicit_bounds: vec![
                        0.0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0,
                        7.5, 10.0,
                    ],
                    exemplars: vec![Exemplar {
                        time_unix_nano: T0 + i as u64,
                        trace_id: vec![0xab; 16],
                        span_id: vec![0xcd; 8],
                        value: Some(exemplar::Value::AsDouble(0.012)),
                        ..Default::default()
                    }],
                    min: Some(0.001),
                    max: Some(1.2),
                    ..Default::default()
                })
                .collect(),
            aggregation_temporality: 2,
        })),
        ..Default::default()
    }]
}

/// Exponential histograms with 160 small buckets on each side (the SDK maximum)
fn exp_histogram_points(points: usize) -> Vec<Metric> {
    vec![Metric {
        name: "http.server.duration".into(),
        data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points: (0..points)
                .map(|i| ExponentialHistogramDataPoint {
                    attributes: vec![s("http.route", "/api/v1/items")],
                    start_time_unix_nano: T0,
                    time_unix_nano: T0 + i as u64,
                    count: 1000,
                    sum: Some(12.5),
                    scale: 3,
                    positive: Some(Buckets {
                        offset: -10,
                        bucket_counts: (0..160).map(|b| b % 100).collect(),
                    }),
                    negative: Some(Buckets {
                        offset: -10,
                        bucket_counts: (0..160).map(|b| b % 7).collect(),
                    }),
                    ..Default::default()
                })
                .collect(),
            aggregation_temporality: 2,
        })),
        ..Default::default()
    }]
}

/// One realistic request, built to fill most of the payload limit
pub(crate) struct Shape {
    pub name: &'static str,
    pub signal: &'static str,
    pub body: Vec<u8>,
    pub size: RequestSize,
}

/// Records of `one` bytes each that fit in `PAYLOAD`, with room for framing
fn fitting(one: usize) -> usize {
    (PAYLOAD - 64 * 1024) / one
}

fn shape<M: Message>(
    name: &'static str,
    signal: &'static str,
    build: impl Fn(usize) -> M,
    size: impl Fn(&M) -> RequestSize,
) -> Shape {
    let probe = build(1000).encoded_len();
    let mut n = fitting(probe.div_ceil(1000));
    let mut request = build(n);
    // Records can grow with their index
    while request.encoded_len() > PAYLOAD - 16 * 1024 {
        n = n * 99 / 100;
        request = build(n);
    }
    let body = request.encode_to_vec();
    let size = size(&request);
    Shape {
        name,
        signal,
        body,
        size,
    }
}

pub(crate) fn shapes() -> Vec<Shape> {
    let logs_size = |r: &ExportLogsServiceRequest| logs_size(&r.resource_logs);
    let traces_size = |r: &ExportTraceServiceRequest| traces_size(&r.resource_spans);
    let metrics_size = |r: &ExportMetricsServiceRequest| metrics_size(&r.resource_metrics, false);
    vec![
        shape(
            "logs, 30 k8s resource attributes",
            "logs",
            |n| logs(k8s_resource(), (0..n).map(app_log).collect()),
            logs_size,
        ),
        shape(
            "logs, service.name only",
            "logs",
            |n| logs(minimal_resource(), (0..n).map(app_log).collect()),
            logs_size,
        ),
        shape(
            "logs, short records (time, severity, body \"ok\")",
            "logs",
            |n| logs(None, (0..n).map(short_log).collect()),
            logs_size,
        ),
        shape(
            "logs, short records, 30 k8s resource attributes",
            "logs",
            |n| logs(k8s_resource(), (0..n).map(short_log).collect()),
            logs_size,
        ),
        shape(
            "traces, 30 k8s resource attributes",
            "traces",
            |n| traces(k8s_resource(), (0..n).map(server_span).collect()),
            traces_size,
        ),
        shape(
            "traces, service.name only",
            "traces",
            |n| traces(minimal_resource(), (0..n).map(server_span).collect()),
            traces_size,
        ),
        shape(
            "traces, bare spans (ids, name, times)",
            "traces",
            |n| traces(minimal_resource(), (0..n).map(bare_span).collect()),
            traces_size,
        ),
        shape(
            "traces, bare spans, 30 k8s resource attributes",
            "traces",
            |n| traces(k8s_resource(), (0..n).map(bare_span).collect()),
            traces_size,
        ),
        shape(
            "traces, an event and a link per span, k8s",
            "traces",
            |n| traces(k8s_resource(), (0..n).map(span_with_event).collect()),
            traces_size,
        ),
        shape(
            "metrics, sum points, 30 k8s resource attributes",
            "metrics",
            |n| metrics(k8s_resource(), sum_points(n)),
            metrics_size,
        ),
        shape(
            "metrics, sum points, service.name only",
            "metrics",
            |n| metrics(minimal_resource(), sum_points(n)),
            metrics_size,
        ),
        shape(
            "metrics, bare gauge points (time, value)",
            "metrics",
            |n| metrics(minimal_resource(), bare_gauge_points(n)),
            metrics_size,
        ),
        shape(
            "metrics, bare gauge points, 30 k8s resource attributes",
            "metrics",
            |n| metrics(k8s_resource(), bare_gauge_points(n)),
            metrics_size,
        ),
        shape(
            "metrics, histograms with an exemplar, k8s",
            "metrics",
            |n| metrics(k8s_resource(), histogram_points(n)),
            metrics_size,
        ),
        shape(
            "metrics, exponential histograms, 160 buckets",
            "metrics",
            |n| metrics(minimal_resource(), exp_histogram_points(n)),
            metrics_size,
        ),
    ]
}

/// Every realistic shape at the payload limit is under the expansion cap
#[test]
fn realistic_requests_at_the_payload_limit_are_under_the_expansion_cap() {
    let cap = max_expanded_bytes(PAYLOAD);
    let mut worst: f64 = 0.0;
    for shape in shapes() {
        let ratio = shape.size.bytes as f64 / shape.body.len() as f64;
        worst = worst.max(ratio);
        println!(
            "EXPANSION {}: body {:.2} MiB, {} records, {} rows, {:.0} row bytes per row, {:.1} row bytes per body byte ({:.0}% of the cap)",
            shape.name,
            shape.body.len() as f64 / (1024.0 * 1024.0),
            shape.size.records,
            shape.size.rows,
            shape.size.bytes as f64 / shape.size.rows as f64,
            ratio,
            100.0 * shape.size.bytes as f64 / cap as f64
        );
        assert!(shape.size.bytes <= cap, "{} is refused", shape.name);
    }
    println!(
        "EXPANSION worst {worst:.1} row bytes per body byte, cap {} per body byte",
        cap / PAYLOAD as u64
    );
}

/// Copied fields that no collector sends: a 15 KB resource over empty records of
/// two bytes each, which expand to tens of GiB of rows, are refused
#[test]
fn copied_fields_over_tiny_records_are_over_the_expansion_cap() {
    let resource = Some(Resource {
        attributes: (0..500)
            .map(|i| s(&format!("attribute.{i:03}"), "value-0123456789"))
            .collect(),
        ..Default::default()
    });
    // Half a MiB of protobuf
    let records = 1 << 18;
    let request = logs(resource, vec![LogRecord::default(); records]);
    let size = logs_size(&request.resource_logs);
    println!(
        "EXPANSION copied resource: {} records, {:.1} GiB of rows",
        size.records,
        size.bytes as f64 / (1u64 << 30) as f64
    );
    assert!(request.encoded_len() <= PAYLOAD);
    assert!(size.bytes > max_expanded_bytes(PAYLOAD));
}

/// Writes the body of every shape to `OTLP_DUMP_DIR`, named after its signal
/// and position, for end-to-end checks against a server
#[test]
#[ignore = "writes request bodies for an end-to-end check"]
fn dump_realistic_bodies() {
    let dir = std::env::var("OTLP_DUMP_DIR").expect("OTLP_DUMP_DIR");
    let mut index = String::new();
    for (i, shape) in shapes().into_iter().enumerate() {
        let file = format!("{i:02}-{}.pb", shape.signal);
        std::fs::write(format!("{dir}/{file}"), &shape.body).unwrap();
        index.push_str(&format!(
            "{file}\t{}\t{}\t{}\t{}\n",
            shape.signal, shape.size.records, shape.size.points, shape.name
        ));
    }
    // Requests of 100 records, as an SDK or a collector with a small batch sends them
    let small: [(&str, Vec<u8>); 3] = [
        (
            "logs",
            logs(k8s_resource(), (0..100).map(app_log).collect()).encode_to_vec(),
        ),
        (
            "traces",
            traces(k8s_resource(), (0..100).map(server_span).collect()).encode_to_vec(),
        ),
        (
            "metrics",
            metrics(k8s_resource(), sum_points(100)).encode_to_vec(),
        ),
    ];
    for (signal, body) in small {
        std::fs::write(format!("{dir}/small-{signal}.pb"), body).unwrap();
    }
    std::fs::write(format!("{dir}/index.tsv"), index).unwrap();
}
