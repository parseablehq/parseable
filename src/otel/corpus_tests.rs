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

//! Seeded corpus of generated OTLP requests, pinning the rows the flatteners
//! make for every signal. A change to the flatteners that is meant to be a pure
//! speedup must leave every digest here unchanged: the rows, their values and
//! the order of the keys inside each row (which decides column order).
//!
//! The generator aims at what the benchmark requests do not reach: attribute
//! keys that collide with fixed row fields and with each other across resource,
//! scope, record, event, link and data point; repeated keys inside one list;
//! attributes without a value; every `AnyValue` kind, nested; empty and missing
//! scopes and resources; log bodies of every kind including JSON text; every
//! metric type with exemplars and metadata; spans with many events and links.
//! Every timestamp is set, so rows do not depend on the clock.
//!
//! Conventions follow the DST harness: SplitMix64, `DST_SEED` and `DST_ITERS`
//! override the seed and the request count (the pinned digests are checked
//! only for the defaults), and the seed is printed. None of these tests touches
//! `PARSEABLE`, so they may be run with the filter `otel::corpus_tests`.

use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
    SummaryDataPoint, exemplar, exponential_histogram_data_point, metric, number_data_point,
    summary_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use serde_json::Value;

use crate::utils::json::flatten::{FlattenBudget, FlattenLimits};

const DEFAULT_SEED: u64 = 0x0071_9c0d_5eed_2026;
const DEFAULT_ITERS: usize = 3000;

/// SplitMix64, as the DST harness uses
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `0..n`
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// True with probability `percent` in 100
    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    /// A nonzero timestamp, so the flatteners never read the clock
    fn timestamp(&mut self) -> u64 {
        match self.below(4) {
            0 => 1,
            1 => 1_700_000_000_000_000_000 + self.below(1_000_000_000) as u64,
            2 => u64::MAX - self.below(1000) as u64,
            _ => 1 + self.next() % (i64::MAX as u64),
        }
    }

    fn id(&mut self) -> Vec<u8> {
        let len = *self.pick(&[0usize, 8, 16, 3]);
        (0..len).map(|_| self.next() as u8).collect()
    }
}

/// Keys every signal can collide with: its own fixed row fields, generic keys
/// shared across levels, and keys a flattened JSON body or key-value list makes
fn key_pool() -> Vec<&'static str> {
    let mut keys: Vec<&'static str> = vec![
        "k",
        "a",
        "a.b",
        "service.name",
        "host.name",
        "body",
        "body_a",
        "body_k",
        "p_timestamp",
        "p_log_category",
        "",
        "ключ",
        "le",
        "quantile",
    ];
    keys.extend(super::logs::OTEL_LOG_KNOWN_FIELD_LIST);
    keys.extend(super::traces::OTEL_TRACES_KNOWN_FIELD_LIST);
    keys.extend(super::metrics::OTEL_METRICS_KNOWN_FIELD_LIST);
    keys
}

const TEXTS: [&str; 14] = [
    "",
    "v",
    "INFO",
    "warn",
    "Error",
    "UNSPECIFIED",
    "fatal: disk full",
    "ünïcödé ✓",
    "{\"a\":1,\"b\":{\"c\":[1,2,{\"d\":null}]},\"k\":\"x\"}",
    "{\"severity_text\":\"x\",\"body\":true,\"nested\":{\"deep\":{\"deeper\":[[1],[2]]}}}",
    "{}",
    "[1,2,3]",
    "{\"a\":",
    "null",
];

const DOUBLES: [f64; 12] = [
    0.0,
    -0.0,
    1.5,
    -2.25,
    1e20,
    1e21,
    1e-7,
    123456789.125,
    f64::MAX,
    f64::MIN_POSITIVE,
    f64::NAN,
    f64::INFINITY,
];

struct Gen {
    rng: Rng,
    keys: Vec<&'static str>,
}

impl Gen {
    fn key(&mut self) -> String {
        // A small pool for most keys, so lists repeat keys and levels collide
        if self.rng.chance(85) {
            let index = self.rng.below(self.keys.len().min(24));
            self.keys[index].to_string()
        } else {
            self.rng.pick(&self.keys).to_string()
        }
    }

    fn text(&mut self) -> String {
        if self.rng.chance(80) {
            self.rng.pick(&TEXTS).to_string()
        } else {
            format!("s{}", self.rng.next() % 1000)
        }
    }

    fn any_value(&mut self, depth: usize) -> AnyValue {
        AnyValue {
            value: self.value(depth),
        }
    }

    fn value(&mut self, depth: usize) -> Option<any_value::Value> {
        let kinds = if depth == 0 { 6 } else { 8 };
        Some(match self.rng.below(kinds + 1) {
            0 => return None,
            1 => any_value::Value::StringValue(self.text()),
            2 => any_value::Value::BoolValue(self.rng.chance(50)),
            3 => {
                let random = self.rng.next() as i64;
                any_value::Value::IntValue(*self.rng.pick(&[0, -1, 42, i64::MIN, i64::MAX, random]))
            }
            4 => any_value::Value::DoubleValue(*self.rng.pick(&DOUBLES)),
            5 => {
                let len = self.rng.below(6);
                let mut bytes: Vec<u8> = (0..len).map(|_| self.rng.next() as u8).collect();
                if self.rng.chance(30) {
                    bytes.extend_from_slice(b"ok");
                }
                any_value::Value::BytesValue(bytes)
            }
            6 => any_value::Value::StringValue(self.rng.pick(&TEXTS[8..]).to_string()),
            7 => any_value::Value::ArrayValue(ArrayValue {
                values: (0..self.rng.below(5))
                    .map(|_| self.any_value(depth - 1))
                    .collect(),
            }),
            _ => any_value::Value::KvlistValue(KeyValueList {
                values: self.attributes_up_to(4, depth - 1),
            }),
        })
    }

    fn attribute(&mut self, depth: usize) -> KeyValue {
        KeyValue {
            key: self.key(),
            value: if self.rng.chance(10) {
                None
            } else {
                Some(self.any_value(depth))
            },
            ..Default::default()
        }
    }

    fn attributes_up_to(&mut self, max: usize, depth: usize) -> Vec<KeyValue> {
        let len = self.rng.below(max + 1);
        let mut attributes: Vec<KeyValue> = (0..len).map(|_| self.attribute(depth)).collect();
        // Sometimes one key many times over
        if self.rng.chance(5) {
            let key = self.key();
            for _ in 0..self.rng.below(40) {
                let mut attribute = self.attribute(depth);
                attribute.key = key.clone();
                attributes.push(attribute);
            }
        }
        attributes
    }

    fn attributes(&mut self) -> Vec<KeyValue> {
        self.attributes_up_to(8, 3)
    }

    fn resource(&mut self) -> Option<Resource> {
        match self.rng.below(5) {
            0 => None,
            1 => Some(Resource::default()),
            _ => Some(Resource {
                attributes: self.attributes(),
                dropped_attributes_count: self.rng.below(3) as u32,
                ..Default::default()
            }),
        }
    }

    fn scope(&mut self) -> Option<InstrumentationScope> {
        match self.rng.below(5) {
            0 => None,
            1 => Some(InstrumentationScope::default()),
            _ => Some(InstrumentationScope {
                name: self.text(),
                version: self.text(),
                attributes: self.attributes(),
                dropped_attributes_count: self.rng.below(3) as u32,
            }),
        }
    }

    fn schema_url(&mut self) -> String {
        if self.rng.chance(50) {
            String::new()
        } else {
            "https://opentelemetry.io/schemas/1.21.0".into()
        }
    }

    fn count(&mut self, max: usize) -> usize {
        self.rng.below(max + 1)
    }

    fn log_record(&mut self) -> LogRecord {
        LogRecord {
            time_unix_nano: self.rng.timestamp(),
            observed_time_unix_nano: self.rng.timestamp(),
            severity_number: self.rng.below(28) as i32 - 1,
            severity_text: self.text(),
            body: match self.rng.below(6) {
                0 => None,
                1 => Some(AnyValue {
                    value: Some(any_value::Value::StringValue(
                        self.rng.pick(&TEXTS[8..]).to_string(),
                    )),
                }),
                _ => Some(self.any_value(3)),
            },
            attributes: self.attributes(),
            dropped_attributes_count: self.rng.below(3) as u32,
            flags: *self.rng.pick(&[0u32, 1, 255, 256, 7]),
            trace_id: self.rng.id(),
            span_id: self.rng.id(),
            event_name: self.text(),
        }
    }

    fn logs(&mut self) -> Vec<ResourceLogs> {
        (0..self.count(3))
            .map(|_| ResourceLogs {
                resource: self.resource(),
                scope_logs: (0..self.count(3))
                    .map(|_| ScopeLogs {
                        scope: self.scope(),
                        log_records: (0..self.count(4)).map(|_| self.log_record()).collect(),
                        schema_url: self.schema_url(),
                    })
                    .collect(),
                schema_url: self.schema_url(),
            })
            .collect()
    }

    fn event(&mut self) -> Event {
        Event {
            time_unix_nano: self.rng.timestamp(),
            name: self.text(),
            attributes: self.attributes_up_to(4, 2),
            dropped_attributes_count: self.rng.below(3) as u32,
        }
    }

    fn link(&mut self) -> Link {
        Link {
            trace_id: self.rng.id(),
            span_id: self.rng.id(),
            trace_state: self.text(),
            attributes: self.attributes_up_to(4, 2),
            dropped_attributes_count: self.rng.below(3) as u32,
            flags: *self.rng.pick(&[0u32, 1, 256, 512, 769]),
        }
    }

    fn span(&mut self) -> Span {
        // Most spans have a few events and links; some have many
        let (events, links) = if self.rng.chance(5) {
            (20 + self.count(60), 10 + self.count(30))
        } else {
            (self.count(3), self.count(2))
        };
        Span {
            trace_id: self.rng.id(),
            span_id: self.rng.id(),
            trace_state: self.text(),
            parent_span_id: self.rng.id(),
            flags: *self.rng.pick(&[0u32, 255, 256, 512, 1, 769]),
            name: self.text(),
            kind: self.rng.below(8) as i32 - 1,
            start_time_unix_nano: self.rng.timestamp(),
            end_time_unix_nano: self.rng.timestamp(),
            attributes: self.attributes(),
            dropped_attributes_count: self.rng.below(3) as u32,
            events: (0..events).map(|_| self.event()).collect(),
            dropped_events_count: self.rng.below(3) as u32,
            links: (0..links).map(|_| self.link()).collect(),
            dropped_links_count: self.rng.below(3) as u32,
            status: if self.rng.chance(30) {
                None
            } else {
                Some(Status {
                    message: self.text(),
                    code: self.rng.below(5) as i32 - 1,
                })
            },
        }
    }

    fn traces(&mut self) -> Vec<ResourceSpans> {
        (0..self.count(3))
            .map(|_| ResourceSpans {
                resource: self.resource(),
                scope_spans: (0..self.count(3))
                    .map(|_| ScopeSpans {
                        scope: self.scope(),
                        spans: (0..self.count(3)).map(|_| self.span()).collect(),
                        schema_url: self.schema_url(),
                    })
                    .collect(),
                schema_url: self.schema_url(),
            })
            .collect()
    }

    fn double(&mut self) -> f64 {
        *self.rng.pick(&DOUBLES)
    }

    fn option_double(&mut self) -> Option<f64> {
        self.rng.chance(70).then(|| self.double())
    }

    fn exemplars(&mut self) -> Vec<Exemplar> {
        (0..self.count(3))
            .map(|_| Exemplar {
                filtered_attributes: self.attributes_up_to(4, 2),
                time_unix_nano: self.rng.timestamp(),
                span_id: self.rng.id(),
                trace_id: self.rng.id(),
                value: match self.rng.below(3) {
                    0 => None,
                    1 => Some(exemplar::Value::AsInt(self.rng.next() as i64)),
                    _ => Some(exemplar::Value::AsDouble(self.double())),
                },
            })
            .collect()
    }

    fn number_points(&mut self) -> Vec<NumberDataPoint> {
        (0..self.count(4))
            .map(|_| NumberDataPoint {
                attributes: self.attributes(),
                start_time_unix_nano: self.rng.timestamp(),
                time_unix_nano: self.rng.timestamp(),
                exemplars: self.exemplars(),
                flags: self.rng.below(3) as u32,
                value: match self.rng.below(3) {
                    0 => None,
                    1 => Some(number_data_point::Value::AsInt(self.rng.next() as i64)),
                    _ => Some(number_data_point::Value::AsDouble(self.double())),
                },
            })
            .collect()
    }

    fn counts(&mut self) -> Vec<u64> {
        (0..self.count(5))
            .map(|_| self.rng.below(100) as u64)
            .collect()
    }

    fn metric(&mut self) -> Metric {
        let temporality = self.rng.below(4) as i32;
        let data = match self.rng.below(7) {
            0 => None,
            1 => Some(metric::Data::Gauge(Gauge {
                data_points: self.number_points(),
            })),
            2 => Some(metric::Data::Sum(Sum {
                data_points: self.number_points(),
                aggregation_temporality: temporality,
                is_monotonic: self.rng.chance(50),
            })),
            3 => Some(metric::Data::Histogram(Histogram {
                data_points: (0..self.count(3))
                    .map(|_| HistogramDataPoint {
                        attributes: self.attributes(),
                        start_time_unix_nano: self.rng.timestamp(),
                        time_unix_nano: self.rng.timestamp(),
                        count: self.rng.below(1000) as u64,
                        sum: self.option_double(),
                        bucket_counts: self.counts(),
                        explicit_bounds: (0..self.count(4)).map(|_| self.double()).collect(),
                        exemplars: self.exemplars(),
                        flags: self.rng.below(3) as u32,
                        min: self.option_double(),
                        max: self.option_double(),
                    })
                    .collect(),
                aggregation_temporality: temporality,
            })),
            4 => Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: (0..self.count(3))
                    .map(|_| ExponentialHistogramDataPoint {
                        attributes: self.attributes(),
                        start_time_unix_nano: self.rng.timestamp(),
                        time_unix_nano: self.rng.timestamp(),
                        count: self.rng.below(1000) as u64,
                        sum: self.option_double(),
                        scale: self.rng.below(20) as i32 - 10,
                        zero_count: self.rng.below(10) as u64,
                        positive: self.rng.chance(70).then(|| {
                            exponential_histogram_data_point::Buckets {
                                offset: self.rng.below(10) as i32 - 5,
                                bucket_counts: self.counts(),
                            }
                        }),
                        negative: self.rng.chance(50).then(|| {
                            exponential_histogram_data_point::Buckets {
                                offset: self.rng.below(10) as i32 - 5,
                                bucket_counts: self.counts(),
                            }
                        }),
                        flags: self.rng.below(3) as u32,
                        exemplars: self.exemplars(),
                        min: self.option_double(),
                        max: self.option_double(),
                        zero_threshold: self.double(),
                    })
                    .collect(),
                aggregation_temporality: temporality,
            })),
            5 => Some(metric::Data::Summary(Summary {
                data_points: (0..self.count(3))
                    .map(|_| SummaryDataPoint {
                        attributes: self.attributes(),
                        start_time_unix_nano: self.rng.timestamp(),
                        time_unix_nano: self.rng.timestamp(),
                        count: self.rng.below(1000) as u64,
                        sum: self.double(),
                        quantile_values: (0..self.count(4))
                            .map(|_| summary_data_point::ValueAtQuantile {
                                quantile: self.double(),
                                value: self.double(),
                            })
                            .collect(),
                        flags: self.rng.below(3) as u32,
                    })
                    .collect(),
            })),
            // Data with no data points
            _ => Some(metric::Data::Gauge(Gauge {
                data_points: Vec::new(),
            })),
        };
        Metric {
            name: self.text(),
            description: self.text(),
            unit: self.text(),
            metadata: self.attributes_up_to(4, 2),
            data,
        }
    }

    fn metrics(&mut self) -> Vec<ResourceMetrics> {
        (0..self.count(3))
            .map(|_| ResourceMetrics {
                resource: self.resource(),
                scope_metrics: (0..self.count(3))
                    .map(|_| ScopeMetrics {
                        scope: self.scope(),
                        metrics: (0..self.count(4)).map(|_| self.metric()).collect(),
                        schema_url: self.schema_url(),
                    })
                    .collect(),
                schema_url: self.schema_url(),
            })
            .collect()
    }
}

/// FNV-1a over everything written, independent of any crate's hasher
struct Digest {
    hash: u64,
    rows: usize,
    bytes: usize,
}

impl Digest {
    fn new() -> Self {
        Self {
            hash: 0xcbf2_9ce4_8422_2325,
            rows: 0,
            bytes: 0,
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash ^= u64::from(*byte);
            self.hash = self.hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.bytes += bytes.len();
    }

    /// The rows of one request in order, each with its keys in order, or the
    /// error that stopped it
    fn request<E: std::fmt::Display>(&mut self, rows: &[Value], result: Result<(), E>) {
        for row in rows {
            self.write(&serde_json::to_vec(row).unwrap());
            self.write(b"\n");
            self.rows += 1;
        }
        if let Err(error) = result {
            self.write(format!("error: {error}\n").as_bytes());
        }
        self.write(b"--\n");
    }
}

fn settings() -> (u64, usize, bool) {
    let seed = std::env::var("DST_SEED").ok().map(|seed| {
        let seed = seed.trim();
        seed.strip_prefix("0x")
            .map_or_else(|| seed.parse(), |hex| u64::from_str_radix(hex, 16))
            .expect("DST_SEED is a number")
    });
    let iters = std::env::var("DST_ITERS")
        .ok()
        .map(|iters| iters.parse().expect("DST_ITERS is a number"));
    let pinned = seed.is_none_or(|seed| seed == DEFAULT_SEED)
        && iters.is_none_or(|iters| iters == DEFAULT_ITERS);
    (
        seed.unwrap_or(DEFAULT_SEED),
        iters.unwrap_or(DEFAULT_ITERS),
        pinned,
    )
}

/// Runs `signal` over the corpus and checks its digest against `expected` (rows,
/// bytes, digest) when the default seed and count are used
fn check(
    signal: &str,
    salt: u64,
    expected: (usize, usize, u64),
    mut run: impl FnMut(&mut Gen, &mut Digest),
) {
    let (seed, iters, pinned) = settings();
    let mut generator = Gen {
        rng: Rng(seed ^ salt),
        keys: key_pool(),
    };
    let mut digest = Digest::new();
    for _ in 0..iters {
        run(&mut generator, &mut digest);
    }
    println!(
        "CORPUS {signal}: seed {seed:#x}, {iters} requests, {} rows, {} bytes, digest {:016x}",
        digest.rows, digest.bytes, digest.hash
    );
    if pinned {
        assert_eq!(
            (digest.rows, digest.bytes, digest.hash),
            expected,
            "rows of the {signal} corpus changed (seed {seed:#x})"
        );
    }
}

#[test]
fn seeded_corpus_log_rows_are_stable() {
    check(
        "logs",
        1,
        (13_819, 8_786_447, 0x5247_f2af_863c_6427),
        |generator, digest| {
            let request = generator.logs();
            let mut rows = Vec::new();
            let result = super::logs::flatten_logs_into(
                &request,
                &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                &mut rows,
            );
            digest.request(&rows, result);
        },
    );
}

#[test]
fn seeded_corpus_metric_rows_are_stable() {
    check(
        "metrics",
        2,
        (42_718, 40_623_570, 0xb299_fcc3_bdad_0047),
        |generator, digest| {
            let request = generator.metrics();
            // Both exemplar modes over the same request
            for flatten_exemplars in [false, true] {
                let mut rows = Vec::new();
                let result =
                    super::metrics::flatten_metrics_into(&request, flatten_exemplars, &mut rows);
                digest.request(&rows, result);
            }
        },
    );
}

#[test]
fn seeded_corpus_trace_rows_are_stable() {
    check(
        "traces",
        3,
        (65_400, 83_307_143, 0x2f81_3bb5_32c5_11a5),
        |generator, digest| {
            let request = generator.traces();
            let mut rows = Vec::new();
            let result = super::traces::flatten_traces_into(&request, &mut rows);
            digest.request(&rows, result);
        },
    );
}
