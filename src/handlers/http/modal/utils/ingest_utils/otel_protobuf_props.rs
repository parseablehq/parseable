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

//! Property tests and seeded deterministic ("DST-style") tests of OTLP/HTTP
//! protobuf ingestion, checked against the OTLP JSON path.
//!
//! The protobuf and JSON paths share the flatteners (they work on the same
//! typed messages), so the encodings can only diverge where decoding does:
//! prost on one side, the `with-serde` mapping of `opentelemetry-proto` plus
//! `deserialize_otel_json` on the other. The differential properties encode one
//! generated request both ways and require identical rows.
//!
//! The JSON mapping can not carry every protobuf message. The differential
//! properties run over the requests it can carry (`json_representable`); every
//! excluded construct has its own `json_mapping_*` test below that pins what the
//! JSON path does with it instead.
//!
//! Nothing here touches the `PARSEABLE` global, so every test may be run with a
//! name filter. The pipeline is followed down to the flattened rows; see
//! `dst_otlp_mixed_encoding_ingest` for where it stops and why.
//!
//! Environment:
//! - `OTLP_PB_PROPTEST_CASES`: cases per property (defaults keep the run quick)
//! - `OTLP_PB_PROPTEST_SEED`: u64 seed of the property runs (default 0, so a
//!   default run is deterministic)
//! - `DST_SEED`, `DST_ITERS`: first seed and number of seeds of the DST scenario

use super::*;
use crate::otel::logs::flatten_otel_logs;
use crate::otel::metrics::{
    flatten_otel_metrics_protobuf_with_options, flatten_otel_metrics_with_options,
};
use crate::otel::otel_utils::flatten_attributes;
use crate::otel::sink::{ChunkLimits, Chunker};
use crate::otel::traces::flatten_otel_traces;
use crate::utils::json::flatten::{FlattenCost, JsonFlattenError};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
    SummaryDataPoint, exemplar, exponential_histogram_data_point::Buckets, metric,
    number_data_point, summary_data_point::ValueAtQuantile,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status,
    span::{Event, Link},
};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{
    Config, RngAlgorithm, TestCaseError, TestCaseResult, TestRng, TestRunner,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

const TENANT: &str = "tenant";

/// The server defaults the protobuf path reads from the configuration
const PROTOBUF_OPTIONS: ProtobufOptions = ProtobufOptions {
    max_payload: 10 * 1024 * 1024,
    flatten_exemplars: false,
};

/// The staging limits of the server defaults, for a body at the payload limit
fn test_limits() -> StageLimits {
    StageLimits::for_body(
        PROTOBUF_OPTIONS.max_payload,
        PROTOBUF_OPTIONS.max_payload,
        FlattenLimits::DEFAULT,
    )
}

// ---------------------------------------------------------------------------
// Seeds and runners
// ---------------------------------------------------------------------------

/// SplitMix64, the generator of the DST conventions
#[derive(Debug, Clone)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }

    fn percent(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }

    fn seed32(&mut self) -> [u8; 32] {
        let mut seed = [0u8; 32];
        for chunk in seed.chunks_mut(8) {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        seed
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().map(|value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a u64, got {value:?}"))
    })
}

/// A deterministic runner: the same `OTLP_PB_PROPTEST_SEED` gives the same cases
fn runner(default_cases: u32) -> TestRunner {
    let cases = env_u64("OTLP_PB_PROPTEST_CASES").map_or(default_cases, |cases| cases as u32);
    let seed = env_u64("OTLP_PB_PROPTEST_SEED").unwrap_or(0);
    TestRunner::new_with_rng(
        Config {
            cases,
            failure_persistence: None,
            max_shrink_iters: 4096,
            max_shrink_time: 60_000,
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &SplitMix64(seed).seed32()),
    )
}

fn check<S>(name: &str, default_cases: u32, strategy: S, test: impl Fn(S::Value) -> TestCaseResult)
where
    S: Strategy,
    S::Value: Debug,
{
    if let Err(error) = runner(default_cases).run(&strategy, test) {
        panic!(
            "{name} (OTLP_PB_PROPTEST_SEED={}): {error}",
            env_u64("OTLP_PB_PROPTEST_SEED").unwrap_or(0)
        );
    }
}

/// One value of `strategy`, drawn deterministically from `rng`
fn sample<T: Debug>(strategy: &BoxedStrategy<T>, rng: &mut SplitMix64) -> T {
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &rng.seed32()),
    );
    strategy
        .new_tree(&mut runner)
        .expect("generators never reject")
        .current()
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------
//
// `wild` widens the domain to protobuf messages the JSON mapping can not carry:
// non-finite doubles in plain `double` fields, an `AnyValue` with no value, and
// `string_value_strindex`. Without it the generators still produce non-finite
// doubles where the JSON mapping carries them (`Option<f64>` histogram
// sum/min/max, which become null, and summary quantiles, which are "NaN"
// strings).

fn text() -> BoxedStrategy<String> {
    prop_oneof![
        4 => "[a-z]{0,6}",
        1 => Just(String::new()),
        1 => Just("ünïcødé 日本語 🎉".to_string()),
        1 => Just("\u{0}\t\"\\/\u{7f}\u{2028}\u{feff}".to_string()),
        1 => Just(r#"{"x":1}"#.to_string()),
        1 => any::<String>().prop_map(|s| s.chars().take(10).collect()),
    ]
    .boxed()
}

/// A small pool, so keys repeat, plus names of fields the flatteners set themselves
fn key() -> BoxedStrategy<String> {
    prop_oneof![
        4 => prop::sample::select(vec![
            "", "k", "dup", "a.b", "service.name", "service.instance.id", "service.namespace",
            "body", "trace_id", "time_unix_nano", "p_log_category", "metric_name",
            "span_name", "exemplars",
        ])
        .prop_map(str::to_string),
        1 => text(),
    ]
    .boxed()
}

fn int64() -> BoxedStrategy<i64> {
    prop_oneof![
        Just(i64::MIN),
        Just(i64::MAX),
        Just(0),
        Just(-1),
        any::<i64>()
    ]
    .boxed()
}

fn finite_f64() -> BoxedStrategy<f64> {
    use prop::num::f64::{NEGATIVE, NORMAL, POSITIVE, SUBNORMAL, ZERO};
    prop_oneof![
        4 => POSITIVE | NEGATIVE | NORMAL | SUBNORMAL | ZERO,
        1 => prop::sample::select(vec![
            0.0, -0.0, f64::MAX, f64::MIN, f64::MIN_POSITIVE, 5e-324, 0.1 + 0.2, 1e300, -1.5e-300,
        ]),
    ]
    .boxed()
}

fn any_f64() -> BoxedStrategy<f64> {
    prop_oneof![
        4 => finite_f64(),
        1 => prop::sample::select(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY]),
    ]
    .boxed()
}

/// Whether serde_json reads back exactly the double it writes. It does not
/// always: the crate is built without serde_json's `float_roundtrip` feature,
/// see `json_path_reads_some_doubles_one_ulp_off`.
fn json_round_trips(double: f64) -> bool {
    !double.is_finite()
        || serde_json::from_str::<f64>(&serde_json::to_string(&double).unwrap())
            .is_ok_and(|read| read.to_bits() == double.to_bits())
}

/// A double for a plain `double` field: finite and read back exactly by
/// serde_json, unless `wild`
fn plain_f64(wild: bool) -> BoxedStrategy<f64> {
    if wild {
        any_f64()
    } else {
        finite_f64()
            .prop_filter("serde_json reads it back exactly", |d| json_round_trips(*d))
            .boxed()
    }
}

/// A double the JSON mapping may write as null or "NaN", but otherwise exact
fn carried_f64() -> BoxedStrategy<f64> {
    any_f64()
        .prop_filter("serde_json reads it back exactly", |d| json_round_trips(*d))
        .boxed()
}

/// Unsigned nanoseconds, never 0: an unset timestamp is filled from the clock
fn ts() -> BoxedStrategy<u64> {
    prop_oneof![
        Just(1u64),
        Just(u64::MAX),
        Just(1u64 << 63),
        1u64..=u64::MAX,
        1_600_000_000_000_000_000u64..1_800_000_000_000_000_000,
    ]
    .boxed()
}

fn id() -> BoxedStrategy<Vec<u8>> {
    prop_oneof![
        Just(vec![]),
        Just(vec![0; 16]),
        Just(vec![0xff; 16]),
        Just(vec![0; 8]),
        Just(vec![0xff; 8]),
        vec(any::<u8>(), 0..20),
    ]
    .boxed()
}

fn count32() -> BoxedStrategy<u32> {
    prop_oneof![Just(0u32), Just(u32::MAX), any::<u32>()].boxed()
}

fn scalar_value(wild: bool) -> BoxedStrategy<Option<any_value::Value>> {
    use any_value::Value as V;
    let representable = prop_oneof![
        text().prop_map(V::StringValue),
        any::<bool>().prop_map(V::BoolValue),
        int64().prop_map(V::IntValue),
        plain_f64(wild).prop_map(V::DoubleValue),
        vec(any::<u8>(), 0..8).prop_map(V::BytesValue),
    ]
    .prop_map(Some);
    if wild {
        prop_oneof![
            6 => representable,
            1 => Just(None),
            1 => any::<i32>().prop_map(|index| Some(V::StringValueStrindex(index))),
        ]
        .boxed()
    } else {
        representable.boxed()
    }
}

fn key_value_of(value: BoxedStrategy<AnyValue>) -> BoxedStrategy<KeyValue> {
    (
        key(),
        prop::option::weighted(0.9, value),
        prop_oneof![4 => Just(0), 1 => any::<i32>()],
    )
        .prop_map(|(key, value, key_strindex)| KeyValue {
            key,
            value,
            key_strindex,
        })
        .boxed()
}

/// Every `AnyValue` variant, with arrays and key-value lists nested up to 3 deep
fn any_value(wild: bool) -> BoxedStrategy<AnyValue> {
    use any_value::Value as V;
    scalar_value(wild)
        .prop_map(|value| AnyValue { value })
        .prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                vec(inner.clone(), 0..4).prop_map(|values| AnyValue {
                    value: Some(V::ArrayValue(ArrayValue { values })),
                }),
                vec(key_value_of(inner), 0..4).prop_map(|values| AnyValue {
                    value: Some(V::KvlistValue(KeyValueList { values })),
                }),
            ]
        })
        .boxed()
}

fn attributes(wild: bool) -> BoxedStrategy<Vec<KeyValue>> {
    vec(key_value_of(any_value(wild)), 0..4).boxed()
}

fn resource(wild: bool) -> BoxedStrategy<Option<Resource>> {
    prop::option::weighted(
        0.8,
        (attributes(wild), count32()).prop_map(|(attributes, dropped_attributes_count)| Resource {
            attributes,
            dropped_attributes_count,
            ..Default::default()
        }),
    )
    .boxed()
}

fn scope(wild: bool) -> BoxedStrategy<Option<InstrumentationScope>> {
    prop::option::weighted(
        0.8,
        (text(), text(), attributes(wild), count32()).prop_map(
            |(name, version, attributes, dropped_attributes_count)| InstrumentationScope {
                name,
                version,
                attributes,
                dropped_attributes_count,
            },
        ),
    )
    .boxed()
}

/// A tree of JSON values, for log bodies that are JSON object text
fn json_tree() -> BoxedStrategy<Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i32>().prop_map(Value::from),
        "[a-z]{0,4}".prop_map(Value::String),
    ]
    .prop_recursive(3, 16, 3, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..4).prop_map(Value::Array),
            vec(("[a-c]{1,2}", inner), 0..3)
                .prop_map(|fields| Value::Object(fields.into_iter().collect())),
        ]
    })
    .boxed()
}

fn json_object_text() -> BoxedStrategy<String> {
    vec(("[a-c]{1,2}", json_tree()), 0..4)
        .prop_map(|fields| Value::Object(fields.into_iter().collect()).to_string())
        .boxed()
}

fn log_body(wild: bool) -> BoxedStrategy<Option<AnyValue>> {
    prop::option::weighted(
        0.85,
        prop_oneof![
            3 => any_value(wild),
            2 => json_object_text().prop_map(|text| AnyValue {
                value: Some(any_value::Value::StringValue(text)),
            }),
            1 => vec(("[a-c]{1,2}", json_object_text()), 0..3).prop_map(|fields| AnyValue {
                value: Some(any_value::Value::KvlistValue(KeyValueList {
                    values: fields
                        .into_iter()
                        .map(|(key, text)| KeyValue {
                            key,
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(text)),
                            }),
                            key_strindex: 0,
                        })
                        .collect(),
                })),
            }),
        ],
    )
    .boxed()
}

fn log_record(wild: bool) -> BoxedStrategy<LogRecord> {
    (
        (ts(), ts()),
        prop_oneof![
            0..=24i32,
            Just(i32::MIN),
            Just(i32::MAX),
            Just(-1),
            Just(25)
        ],
        prop_oneof![
            Just(String::new()),
            prop::sample::select(vec![
                "INFO",
                "error",
                "WARN",
                "UNSPECIFIED",
                " unspecified ",
                "SEVERITY_NUMBER_FATAL4",
            ])
            .prop_map(str::to_string),
            text(),
        ],
        log_body(wild),
        attributes(wild),
        (count32(), count32()),
        (id(), id()),
        text(),
    )
        .prop_map(
            |(
                (time_unix_nano, observed_time_unix_nano),
                severity_number,
                severity_text,
                body,
                attributes,
                (dropped_attributes_count, flags),
                (trace_id, span_id),
                event_name,
            )| LogRecord {
                time_unix_nano,
                observed_time_unix_nano,
                severity_number,
                severity_text,
                body,
                attributes,
                dropped_attributes_count,
                flags,
                trace_id,
                span_id,
                event_name,
            },
        )
        .boxed()
}

fn logs_data(wild: bool) -> BoxedStrategy<LogsData> {
    let scope_logs = (scope(wild), vec(log_record(wild), 0..4), text()).prop_map(
        |(scope, log_records, schema_url)| ScopeLogs {
            scope,
            log_records,
            schema_url,
        },
    );
    vec(
        (resource(wild), vec(scope_logs, 0..3), text()).prop_map(
            |(resource, scope_logs, schema_url)| ResourceLogs {
                resource,
                scope_logs,
                schema_url,
            },
        ),
        0..3,
    )
    .prop_map(|resource_logs| LogsData { resource_logs })
    .boxed()
}

fn span_flags() -> BoxedStrategy<u32> {
    prop_oneof![
        prop::sample::select(vec![0u32, 1, 255, 256, 512, 768]),
        any::<u32>()
    ]
    .boxed()
}

fn span(wild: bool) -> BoxedStrategy<Span> {
    let event = (ts(), text(), attributes(wild), count32()).prop_map(
        |(time_unix_nano, name, attributes, dropped_attributes_count)| Event {
            time_unix_nano,
            name,
            attributes,
            dropped_attributes_count,
        },
    );
    let link = (
        id(),
        id(),
        text(),
        attributes(wild),
        count32(),
        span_flags(),
    )
        .prop_map(
            |(trace_id, span_id, trace_state, attributes, dropped_attributes_count, flags)| Link {
                trace_id,
                span_id,
                trace_state,
                attributes,
                dropped_attributes_count,
                flags,
            },
        );
    let status = prop::option::weighted(
        0.7,
        (text(), prop_oneof![0..=2i32, any::<i32>()])
            .prop_map(|(message, code)| Status { message, code }),
    );
    (
        (id(), id(), text(), id()),
        (span_flags(), text(), prop_oneof![0..=5i32, any::<i32>()]),
        (ts(), ts()),
        (attributes(wild), count32()),
        (vec(event, 0..3), count32()),
        (vec(link, 0..3), count32()),
        status,
    )
        .prop_map(
            |(
                (trace_id, span_id, trace_state, parent_span_id),
                (flags, name, kind),
                (start_time_unix_nano, end_time_unix_nano),
                (attributes, dropped_attributes_count),
                (events, dropped_events_count),
                (links, dropped_links_count),
                status,
            )| Span {
                trace_id,
                span_id,
                trace_state,
                parent_span_id,
                flags,
                name,
                kind,
                start_time_unix_nano,
                end_time_unix_nano,
                attributes,
                dropped_attributes_count,
                events,
                dropped_events_count,
                links,
                dropped_links_count,
                status,
            },
        )
        .boxed()
}

fn traces_data(wild: bool) -> BoxedStrategy<TracesData> {
    let scope_spans =
        (scope(wild), vec(span(wild), 0..4), text()).prop_map(|(scope, spans, schema_url)| {
            ScopeSpans {
                scope,
                spans,
                schema_url,
            }
        });
    vec(
        (resource(wild), vec(scope_spans, 0..3), text()).prop_map(
            |(resource, scope_spans, schema_url)| ResourceSpans {
                resource,
                scope_spans,
                schema_url,
            },
        ),
        0..3,
    )
    .prop_map(|resource_spans| TracesData { resource_spans })
    .boxed()
}

fn data_point_flags() -> BoxedStrategy<u32> {
    prop_oneof![Just(0u32), Just(1), any::<u32>()].boxed()
}

fn temporality() -> BoxedStrategy<i32> {
    prop_oneof![0..=2i32, any::<i32>()].boxed()
}

fn exemplars(wild: bool) -> BoxedStrategy<Vec<Exemplar>> {
    vec(
        (
            attributes(wild),
            ts(),
            id(),
            id(),
            prop::option::weighted(
                0.9,
                prop_oneof![
                    plain_f64(wild).prop_map(exemplar::Value::AsDouble),
                    int64().prop_map(exemplar::Value::AsInt),
                ],
            ),
        )
            .prop_map(
                |(filtered_attributes, time_unix_nano, span_id, trace_id, value)| Exemplar {
                    filtered_attributes,
                    time_unix_nano,
                    span_id,
                    trace_id,
                    value,
                },
            ),
        0..3,
    )
    .boxed()
}

fn number_data_points(wild: bool) -> BoxedStrategy<Vec<NumberDataPoint>> {
    vec(
        (
            attributes(wild),
            (ts(), ts()),
            exemplars(wild),
            data_point_flags(),
            prop::option::weighted(
                0.9,
                prop_oneof![
                    plain_f64(wild).prop_map(number_data_point::Value::AsDouble),
                    int64().prop_map(number_data_point::Value::AsInt),
                ],
            ),
        )
            .prop_map(
                |(attributes, (start_time_unix_nano, time_unix_nano), exemplars, flags, value)| {
                    NumberDataPoint {
                        attributes,
                        start_time_unix_nano,
                        time_unix_nano,
                        exemplars,
                        flags,
                        value,
                    }
                },
            ),
        0..3,
    )
    .boxed()
}

fn optional_f64() -> BoxedStrategy<Option<f64>> {
    prop::option::weighted(0.8, carried_f64()).boxed()
}

fn histogram_data_points(wild: bool) -> BoxedStrategy<Vec<HistogramDataPoint>> {
    vec(
        (
            attributes(wild),
            (ts(), ts()),
            (any::<u64>(), optional_f64()),
            (vec(any::<u64>(), 0..4), vec(plain_f64(wild), 0..4)),
            exemplars(wild),
            data_point_flags(),
            (optional_f64(), optional_f64()),
        )
            .prop_map(
                |(
                    attributes,
                    (start_time_unix_nano, time_unix_nano),
                    (count, sum),
                    (bucket_counts, explicit_bounds),
                    exemplars,
                    flags,
                    (min, max),
                )| HistogramDataPoint {
                    attributes,
                    start_time_unix_nano,
                    time_unix_nano,
                    count,
                    sum,
                    bucket_counts,
                    explicit_bounds,
                    exemplars,
                    flags,
                    min,
                    max,
                },
            ),
        0..3,
    )
    .boxed()
}

fn buckets() -> BoxedStrategy<Option<Buckets>> {
    prop::option::weighted(
        0.7,
        (any::<i32>(), vec(any::<u64>(), 0..4)).prop_map(|(offset, bucket_counts)| Buckets {
            offset,
            bucket_counts,
        }),
    )
    .boxed()
}

fn exp_histogram_data_points(wild: bool) -> BoxedStrategy<Vec<ExponentialHistogramDataPoint>> {
    vec(
        (
            attributes(wild),
            (ts(), ts()),
            (any::<u64>(), optional_f64(), any::<i32>(), any::<u64>()),
            (buckets(), buckets()),
            (data_point_flags(), exemplars(wild)),
            (optional_f64(), optional_f64(), plain_f64(wild)),
        )
            .prop_map(
                |(
                    attributes,
                    (start_time_unix_nano, time_unix_nano),
                    (count, sum, scale, zero_count),
                    (positive, negative),
                    (flags, exemplars),
                    (min, max, zero_threshold),
                )| ExponentialHistogramDataPoint {
                    attributes,
                    start_time_unix_nano,
                    time_unix_nano,
                    count,
                    sum,
                    scale,
                    zero_count,
                    positive,
                    negative,
                    flags,
                    exemplars,
                    min,
                    max,
                    zero_threshold,
                },
            ),
        0..3,
    )
    .boxed()
}

fn summary_data_points(wild: bool) -> BoxedStrategy<Vec<SummaryDataPoint>> {
    vec(
        (
            attributes(wild),
            (ts(), ts()),
            (any::<u64>(), plain_f64(wild)),
            vec(
                (carried_f64(), carried_f64())
                    .prop_map(|(quantile, value)| ValueAtQuantile { quantile, value }),
                0..3,
            ),
            data_point_flags(),
        )
            .prop_map(
                |(
                    attributes,
                    (start_time_unix_nano, time_unix_nano),
                    (count, sum),
                    quantile_values,
                    flags,
                )| SummaryDataPoint {
                    attributes,
                    start_time_unix_nano,
                    time_unix_nano,
                    count,
                    sum,
                    quantile_values,
                    flags,
                },
            ),
        0..3,
    )
    .boxed()
}

fn metric_data(wild: bool) -> BoxedStrategy<Option<metric::Data>> {
    prop::option::weighted(
        0.9,
        prop_oneof![
            number_data_points(wild)
                .prop_map(|data_points| metric::Data::Gauge(Gauge { data_points })),
            (number_data_points(wild), temporality(), any::<bool>()).prop_map(
                |(data_points, aggregation_temporality, is_monotonic)| metric::Data::Sum(Sum {
                    data_points,
                    aggregation_temporality,
                    is_monotonic,
                })
            ),
            (histogram_data_points(wild), temporality()).prop_map(
                |(data_points, aggregation_temporality)| metric::Data::Histogram(Histogram {
                    data_points,
                    aggregation_temporality,
                })
            ),
            (exp_histogram_data_points(wild), temporality()).prop_map(
                |(data_points, aggregation_temporality)| {
                    metric::Data::ExponentialHistogram(ExponentialHistogram {
                        data_points,
                        aggregation_temporality,
                    })
                }
            ),
            summary_data_points(wild)
                .prop_map(|data_points| metric::Data::Summary(Summary { data_points })),
        ],
    )
    .boxed()
}

fn metric(wild: bool) -> BoxedStrategy<Metric> {
    (text(), text(), text(), attributes(wild), metric_data(wild))
        .prop_map(|(name, description, unit, metadata, data)| Metric {
            name,
            description,
            unit,
            metadata,
            data,
        })
        .boxed()
}

fn metrics_data(wild: bool) -> BoxedStrategy<MetricsData> {
    let scope_metrics =
        (scope(wild), vec(metric(wild), 0..4), text()).prop_map(|(scope, metrics, schema_url)| {
            ScopeMetrics {
                scope,
                metrics,
                schema_url,
            }
        });
    vec(
        (resource(wild), vec(scope_metrics, 0..3), text()).prop_map(
            |(resource, scope_metrics, schema_url)| ResourceMetrics {
                resource,
                scope_metrics,
                schema_url,
            },
        ),
        0..3,
    )
    .prop_map(|resource_metrics| MetricsData { resource_metrics })
    .boxed()
}

fn tight_limits() -> BoxedStrategy<FlattenLimits> {
    (1usize..16, 1usize..24, 16usize..1024)
        .prop_map(
            |(max_rows_per_event, max_rows_per_request, max_bytes_per_request)| FlattenLimits {
                max_rows_per_event,
                max_rows_per_request,
                max_bytes_per_request,
            },
        )
        .boxed()
}

// ---------------------------------------------------------------------------
// One request, both encodings
// ---------------------------------------------------------------------------

/// A request of one signal, held as the JSON-side message type
#[derive(Debug, Clone)]
enum Req {
    Logs(LogsData),
    Metrics(MetricsData),
    Traces(TracesData),
}

impl Req {
    fn source(&self) -> LogSource {
        match self {
            Self::Logs(_) => LogSource::OtelLogs,
            Self::Metrics(_) => LogSource::OtelMetrics,
            Self::Traces(_) => LogSource::OtelTraces,
        }
    }

    fn telemetry_type(&self) -> TelemetryType {
        match self {
            Self::Logs(_) => TelemetryType::Logs,
            Self::Metrics(_) => TelemetryType::Metrics,
            Self::Traces(_) => TelemetryType::Traces,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Logs(_) => "logs",
            Self::Metrics(_) => "metrics",
            Self::Traces(_) => "traces",
        }
    }

    /// The protobuf body an OTLP/HTTP client sends
    fn encode_pb(&self) -> Vec<u8> {
        match self {
            Self::Logs(data) => ExportLogsServiceRequest {
                resource_logs: data.resource_logs.clone(),
            }
            .encode_to_vec(),
            Self::Metrics(data) => ExportMetricsServiceRequest {
                resource_metrics: data.resource_metrics.clone(),
            }
            .encode_to_vec(),
            Self::Traces(data) => ExportTraceServiceRequest {
                resource_spans: data.resource_spans.clone(),
            }
            .encode_to_vec(),
        }
    }

    /// The OTLP JSON body, as the `with-serde` mapping of opentelemetry-proto writes it
    fn encode_json(&self) -> Vec<u8> {
        match self {
            Self::Logs(data) => serde_json::to_vec(data),
            Self::Metrics(data) => serde_json::to_vec(data),
            Self::Traces(data) => serde_json::to_vec(data),
        }
        .expect("OTLP messages serialise")
    }

    fn from_decoded(request: OtelProtobufRequest) -> Self {
        match request {
            OtelProtobufRequest::Logs(m) => Self::Logs(LogsData {
                resource_logs: m.resource_logs,
            }),
            OtelProtobufRequest::Metrics(m) => Self::Metrics(MetricsData {
                resource_metrics: m.resource_metrics,
            }),
            OtelProtobufRequest::Traces(m) => Self::Traces(TracesData {
                resource_spans: m.resource_spans,
            }),
        }
    }

    /// Rows the flatteners build for this request, counted from its structure
    fn expected_rows(&self) -> usize {
        match self {
            Self::Logs(data) => data
                .resource_logs
                .iter()
                .flat_map(|r| &r.scope_logs)
                .map(|s| s.log_records.len())
                .sum(),
            Self::Traces(data) => data
                .resource_spans
                .iter()
                .flat_map(|r| &r.scope_spans)
                .flat_map(|s| &s.spans)
                .map(|span| (span.events.len() + span.links.len()).max(1))
                .sum(),
            Self::Metrics(data) => data.resource_metrics.iter().map(expected_metric_rows).sum(),
        }
    }
}

fn metric_points(metric: &Metric) -> usize {
    match &metric.data {
        Some(metric::Data::Gauge(m)) => m.data_points.len(),
        Some(metric::Data::Sum(m)) => m.data_points.len(),
        Some(metric::Data::Histogram(m)) => m.data_points.len(),
        Some(metric::Data::ExponentialHistogram(m)) => m.data_points.len(),
        Some(metric::Data::Summary(m)) => m.data_points.len(),
        None => 0,
    }
}

/// One row per data point, one for a metric without data points, none for a
/// metric without data, and one `target_info` row for a resource with a
/// Prometheus target identity once a row of it has a string `time_unix_nano`.
/// The metric's metadata is merged over each of its rows, so a metadata
/// attribute named `time_unix_nano` decides that (see
/// `metric_metadata_can_not_supply_the_target_info_timestamp`). Only valid with
/// nested exemplars (`flatten_exemplars == false`): flat exemplars copy their
/// attributes over the data point's fields.
fn expected_metric_rows(resource: &ResourceMetrics) -> usize {
    let mut rows = 0;
    let mut timestamped = false;
    for metric in resource.scope_metrics.iter().flat_map(|s| &s.metrics) {
        if metric.data.is_none() {
            continue;
        }
        let points = metric_points(metric);
        rows += points.max(1);
        // Metadata is copied over each row before its timestamp is read
        let metadata = flatten_attributes(&metric.metadata);
        timestamped |= match metadata.get("time_unix_nano") {
            Some(Value::String(_)) => true,
            None => points > 0,
            Some(_) => false,
        };
    }
    let identity = resource.resource.as_ref().is_some_and(|r| {
        let fields = flatten_attributes(&r.attributes);
        ["service.name", "service.instance.id"].iter().any(|key| {
            matches!(
                fields.get(*key),
                Some(Value::String(_) | Value::Bool(_) | Value::Number(_))
            )
        })
    });
    rows + usize::from(identity && timestamped)
}

/// Why a stage refused a body
#[derive(Debug, Clone, PartialEq)]
enum StageError {
    /// The body is not a message of the signal (protobuf decode, JSON parse or OTLP mapping)
    Decode(String),
    /// Flattening went over the limits; the `PostError` text the client gets
    Flatten(String),
}

#[derive(Debug)]
struct Outcome {
    result: Result<Vec<Value>, StageError>,
    /// Flatten budget used by the request
    used: FlattenCost,
}

fn flatten_error(error: JsonFlattenError) -> StageError {
    StageError::Flatten(PostError::from(error).to_string())
}

/// `flatten_and_push_otel_protobuf` up to the rows it hands to `push_logs`:
/// decode, then flatten with one budget for the request
fn pb_stage(
    source: &LogSource,
    body: &[u8],
    limits: FlattenLimits,
    flatten_exemplars: bool,
) -> (Option<Req>, Outcome) {
    let mut budget = FlattenBudget::new(limits);
    let request = match OtelProtobufRequest::decode(body, source) {
        Ok(request) => request,
        Err(error) => {
            return (
                None,
                Outcome {
                    result: Err(StageError::Decode(error.to_string())),
                    used: budget.used(),
                },
            );
        }
    };
    let result =
        match &request {
            // `OtelProtobufRequest::flatten` reads the exemplar flag from the server configuration
            OtelProtobufRequest::Metrics(message) => Ok(
                flatten_otel_metrics_protobuf_with_options(message, TENANT, flatten_exemplars),
            ),
            _ => request.flatten(TENANT, &mut budget).map_err(flatten_error),
        };
    (
        Some(Req::from_decoded(request)),
        Outcome {
            result,
            used: budget.used(),
        },
    )
}

/// The OTEL arms of `handle_otel_ingestion` and `flatten_and_push_logs` for a
/// JSON body, up to the rows they hand to `push_logs`
fn json_stage(
    source: &LogSource,
    body: &[u8],
    limits: FlattenLimits,
    flatten_exemplars: bool,
    normalise: bool,
) -> (Option<Req>, Outcome) {
    let mut budget = FlattenBudget::new(limits);
    let decode_error = |error: serde_json::Error| Outcome {
        result: Err(StageError::Decode(error.to_string())),
        used: FlattenCost::default(),
    };
    let json: Value = match serde_json::from_slice(body) {
        Ok(json) => json,
        Err(error) => return (None, decode_error(error)),
    };
    let (request, result) = match source {
        LogSource::OtelLogs => match deserialize_otel_json::<LogsData>(json, normalise) {
            Ok(logs) => {
                let rows = flatten_otel_logs(&logs, TENANT, &mut budget).map_err(flatten_error);
                (Req::Logs(logs), rows)
            }
            Err(error) => return (None, decode_error(error)),
        },
        LogSource::OtelMetrics => match deserialize_otel_json::<MetricsData>(json, normalise) {
            Ok(metrics) => {
                let rows =
                    flatten_otel_metrics_with_options(metrics.clone(), TENANT, flatten_exemplars);
                (Req::Metrics(metrics), Ok(rows))
            }
            Err(error) => return (None, decode_error(error)),
        },
        LogSource::OtelTraces => match deserialize_otel_json::<TracesData>(json, normalise) {
            Ok(traces) => {
                let rows = flatten_otel_traces(&traces, TENANT);
                (Req::Traces(traces), Ok(rows))
            }
            Err(error) => return (None, decode_error(error)),
        },
        other => panic!("not an OTEL source: {other}"),
    };
    (
        Some(request),
        Outcome {
            result,
            used: budget.used(),
        },
    )
}

// ---------------------------------------------------------------------------
// What the JSON mapping can carry
// ---------------------------------------------------------------------------

fn value_representable(value: &AnyValue) -> bool {
    use any_value::Value as V;
    match &value.value {
        None | Some(V::StringValueStrindex(_)) => false,
        Some(V::DoubleValue(double)) => double.is_finite() && json_round_trips(*double),
        Some(V::ArrayValue(array)) => array.values.iter().all(value_representable),
        Some(V::KvlistValue(list)) => attributes_representable(&list.values),
        Some(V::StringValue(_) | V::BoolValue(_) | V::IntValue(_) | V::BytesValue(_)) => true,
    }
}

fn attributes_representable(attributes: &[KeyValue]) -> bool {
    attributes
        .iter()
        .all(|kv| kv.value.as_ref().is_none_or(value_representable))
}

fn envelope_representable(
    resource: &Option<Resource>,
    scope: Option<&Option<InstrumentationScope>>,
) -> bool {
    resource
        .as_ref()
        .is_none_or(|r| attributes_representable(&r.attributes))
        && scope
            .and_then(Option::as_ref)
            .is_none_or(|s| attributes_representable(&s.attributes))
}

fn exemplars_representable(exemplars: &[Exemplar]) -> bool {
    exemplars.iter().all(|e| {
        attributes_representable(&e.filtered_attributes)
            && !matches!(e.value, Some(exemplar::Value::AsDouble(d)) if !d.is_finite() || !json_round_trips(d))
    })
}

fn metric_representable(metric: &Metric) -> bool {
    attributes_representable(&metric.metadata)
        && match &metric.data {
            None => true,
            Some(metric::Data::Gauge(Gauge { data_points }))
            | Some(metric::Data::Sum(Sum { data_points, .. })) => data_points.iter().all(|p| {
                attributes_representable(&p.attributes)
                    && exemplars_representable(&p.exemplars)
                    && !matches!(p.value, Some(number_data_point::Value::AsDouble(d)) if !d.is_finite() || !json_round_trips(d))
            }),
            Some(metric::Data::Histogram(h)) => h.data_points.iter().all(|p| {
                attributes_representable(&p.attributes)
                    && exemplars_representable(&p.exemplars)
                    && p.explicit_bounds.iter().all(|b| b.is_finite() && json_round_trips(*b))
                    && [p.sum, p.min, p.max].into_iter().flatten().all(json_round_trips)
            }),
            Some(metric::Data::ExponentialHistogram(h)) => h.data_points.iter().all(|p| {
                attributes_representable(&p.attributes)
                    && exemplars_representable(&p.exemplars)
                    && p.zero_threshold.is_finite()
                    && [Some(p.zero_threshold), p.sum, p.min, p.max]
                        .into_iter()
                        .flatten()
                        .all(json_round_trips)
            }),
            Some(metric::Data::Summary(s)) => s.data_points.iter().all(|p| {
                attributes_representable(&p.attributes)
                    && p.sum.is_finite()
                    && json_round_trips(p.sum)
                    && p.quantile_values
                        .iter()
                        .all(|q| json_round_trips(q.quantile) && json_round_trips(q.value))
            }),
        }
}

/// Whether the OTLP JSON mapping of opentelemetry-proto, read back by this
/// crate's serde_json, carries this request unchanged (non-finite optional
/// doubles become unset, which the flatteners treat alike). See the
/// `json_mapping_*` and `json_path_*` tests for what happens otherwise.
fn json_representable(request: &Req) -> bool {
    match request {
        Req::Logs(data) => data.resource_logs.iter().all(|r| {
            envelope_representable(&r.resource, None)
                && r.scope_logs.iter().all(|s| {
                    envelope_representable(&None, Some(&s.scope))
                        && s.log_records.iter().all(|l| {
                            attributes_representable(&l.attributes)
                                && l.body.as_ref().is_none_or(value_representable)
                        })
                })
        }),
        Req::Traces(data) => data.resource_spans.iter().all(|r| {
            envelope_representable(&r.resource, None)
                && r.scope_spans.iter().all(|s| {
                    envelope_representable(&None, Some(&s.scope))
                        && s.spans.iter().all(|span| {
                            attributes_representable(&span.attributes)
                                && span
                                    .events
                                    .iter()
                                    .all(|e| attributes_representable(&e.attributes))
                                && span
                                    .links
                                    .iter()
                                    .all(|l| attributes_representable(&l.attributes))
                        })
                })
        }),
        Req::Metrics(data) => data.resource_metrics.iter().all(|r| {
            envelope_representable(&r.resource, None)
                && r.scope_metrics.iter().all(|s| {
                    envelope_representable(&None, Some(&s.scope))
                        && s.metrics.iter().all(metric_representable)
                })
        }),
    }
}

/// Sets every unset (zero) timestamp to 1 ns, so that rows do not depend on the
/// clock. Returns whether any was unset.
fn fill_unset_timestamps(request: &mut Req) -> bool {
    let mut filled = false;
    let mut fill = |ts: &mut u64| {
        if *ts == 0 {
            *ts = 1;
            filled = true;
        }
    };
    match request {
        Req::Logs(data) => {
            for record in data
                .resource_logs
                .iter_mut()
                .flat_map(|r| &mut r.scope_logs)
                .flat_map(|s| &mut s.log_records)
            {
                fill(&mut record.time_unix_nano);
                fill(&mut record.observed_time_unix_nano);
            }
        }
        Req::Traces(data) => {
            for span in data
                .resource_spans
                .iter_mut()
                .flat_map(|r| &mut r.scope_spans)
                .flat_map(|s| &mut s.spans)
            {
                fill(&mut span.start_time_unix_nano);
                fill(&mut span.end_time_unix_nano);
                for event in &mut span.events {
                    fill(&mut event.time_unix_nano);
                }
            }
        }
        Req::Metrics(data) => {
            fn fill_exemplars(exemplars: &mut [Exemplar], fill: &mut dyn FnMut(&mut u64)) {
                for exemplar in exemplars {
                    fill(&mut exemplar.time_unix_nano);
                }
            }
            for metric in data
                .resource_metrics
                .iter_mut()
                .flat_map(|r| &mut r.scope_metrics)
                .flat_map(|s| &mut s.metrics)
            {
                match &mut metric.data {
                    Some(metric::Data::Gauge(Gauge { data_points }))
                    | Some(metric::Data::Sum(Sum { data_points, .. })) => {
                        for p in data_points {
                            fill(&mut p.start_time_unix_nano);
                            fill(&mut p.time_unix_nano);
                            fill_exemplars(&mut p.exemplars, &mut fill);
                        }
                    }
                    Some(metric::Data::Histogram(h)) => {
                        for p in &mut h.data_points {
                            fill(&mut p.start_time_unix_nano);
                            fill(&mut p.time_unix_nano);
                            fill_exemplars(&mut p.exemplars, &mut fill);
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(h)) => {
                        for p in &mut h.data_points {
                            fill(&mut p.start_time_unix_nano);
                            fill(&mut p.time_unix_nano);
                            fill_exemplars(&mut p.exemplars, &mut fill);
                        }
                    }
                    Some(metric::Data::Summary(s)) => {
                        for p in &mut s.data_points {
                            fill(&mut p.start_time_unix_nano);
                            fill(&mut p.time_unix_nano);
                        }
                    }
                    None => {}
                }
            }
        }
    }
    filled
}

// ---------------------------------------------------------------------------
// 1. Differential properties
// ---------------------------------------------------------------------------

/// Both encodings of `request` give the same outcome and use the same budget
fn assert_encodings_agree(
    request: &Req,
    limits: FlattenLimits,
    flatten_exemplars: bool,
    normalise: bool,
) -> Result<Outcome, TestCaseError> {
    let source = request.source();
    let (_, pb) = pb_stage(&source, &request.encode_pb(), limits, flatten_exemplars);
    let (_, js) = json_stage(
        &source,
        &request.encode_json(),
        limits,
        flatten_exemplars,
        normalise,
    );
    prop_assert_eq!(
        &pb.result,
        &js.result,
        "protobuf (left) and JSON (right) differ"
    );
    prop_assert_eq!(pb.used, js.used, "flatten budget used differs");
    Ok(pb)
}

fn differential(request: Req, flatten_exemplars: bool, normalise: bool) -> TestCaseResult {
    prop_assume!(json_representable(&request));
    let outcome = assert_encodings_agree(
        &request,
        FlattenLimits::DEFAULT,
        flatten_exemplars,
        normalise,
    )?;
    let rows = outcome
        .result
        .map_err(|e| TestCaseError::fail(format!("valid request refused: {e:?}")))?;
    if !flatten_exemplars {
        prop_assert_eq!(rows.len(), request.expected_rows());
    }
    Ok(())
}

#[test]
fn otel_pb_differential_logs() {
    check(
        "otel_pb_differential_logs",
        96,
        (logs_data(false), any::<bool>()),
        |(logs, normalise)| differential(Req::Logs(logs), false, normalise),
    );
}

#[test]
fn otel_pb_differential_traces() {
    check(
        "otel_pb_differential_traces",
        96,
        (traces_data(false), any::<bool>()),
        |(traces, normalise)| differential(Req::Traces(traces), false, normalise),
    );
}

#[test]
fn otel_pb_differential_metrics() {
    check(
        "otel_pb_differential_metrics",
        96,
        (metrics_data(false), any::<bool>(), any::<bool>()),
        |(metrics, flatten_exemplars, normalise)| {
            differential(Req::Metrics(metrics), flatten_exemplars, normalise)
        },
    );
}

/// The wider domain: the protobuf path takes every message, and the JSON path
/// agrees with it on every one `json_representable` admits. This checks that
/// predicate, which the differential properties above rely on.
#[test]
fn otel_pb_wild_requests_flatten_and_agree_where_json_can_carry_them() {
    let (carried, not_carried) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let wild = prop_oneof![
        logs_data(true).prop_map(Req::Logs),
        traces_data(true).prop_map(Req::Traces),
        metrics_data(true).prop_map(Req::Metrics),
    ];
    check("otel_pb_wild_requests", 96, wild, |request| {
        let (_, pb) = pb_stage(
            &request.source(),
            &request.encode_pb(),
            FlattenLimits::DEFAULT,
            false,
        );
        let rows = pb
            .result
            .map_err(|e| TestCaseError::fail(format!("protobuf refused: {e:?}")))?;
        prop_assert_eq!(rows.len(), request.expected_rows());
        if json_representable(&request) {
            carried.fetch_add(1, Ordering::Relaxed);
            assert_encodings_agree(&request, FlattenLimits::DEFAULT, false, false)?;
        } else {
            not_carried.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    });
    let (carried, not_carried) = (carried.into_inner(), not_carried.into_inner());
    println!("otel_pb_wild_requests: json_representable={carried} not={not_carried}");
    assert!(carried > 0 && not_carried > 0, "{carried} / {not_carried}");
}

// ---------------------------------------------------------------------------
// Chunked staging
// ---------------------------------------------------------------------------

impl Req {
    fn resources(&self) -> OtelResources<'_> {
        match self {
            Self::Logs(data) => OtelResources::Logs(&data.resource_logs),
            Self::Metrics(data) => OtelResources::Metrics(&data.resource_metrics),
            Self::Traces(data) => OtelResources::Traces(&data.resource_spans),
        }
    }

    /// Leaf records: log records, spans or metrics
    fn record_count(&self) -> u64 {
        let n: usize = match self {
            Self::Logs(data) => data
                .resource_logs
                .iter()
                .flat_map(|r| &r.scope_logs)
                .map(|s| s.log_records.len())
                .sum(),
            Self::Metrics(data) => data
                .resource_metrics
                .iter()
                .flat_map(|r| &r.scope_metrics)
                .map(|s| s.metrics.len())
                .sum(),
            Self::Traces(data) => data
                .resource_spans
                .iter()
                .flat_map(|r| &r.scope_spans)
                .map(|s| s.spans.len())
                .sum(),
        };
        n as u64
    }

    /// Metric data points
    fn point_count(&self) -> u64 {
        match self {
            Self::Metrics(data) => data
                .resource_metrics
                .iter()
                .flat_map(|r| &r.scope_metrics)
                .flat_map(|s| &s.metrics)
                .map(|m| metric_points(m) as u64)
                .sum(),
            Self::Logs(_) | Self::Traces(_) => 0,
        }
    }
}

/// The request decoded from `body` in `encoding`, as ingestion decodes it
fn decode_as(request: &Req, protobuf: bool) -> Req {
    let source = request.source();
    if protobuf {
        Req::from_decoded(OtelProtobufRequest::decode(&request.encode_pb(), &source).unwrap())
    } else {
        let json: Value = serde_json::from_slice(&request.encode_json()).unwrap();
        match request {
            Req::Logs(_) => Req::Logs(deserialize_otel_json(json, false).unwrap()),
            Req::Metrics(_) => Req::Metrics(deserialize_otel_json(json, false).unwrap()),
            Req::Traces(_) => Req::Traces(deserialize_otel_json(json, false).unwrap()),
        }
    }
}

/// Chunks of at most `rows` rows (and a byte limit small enough to cut chunks of a
/// row or two) give the same rows in the same order as one chunk, for both
/// encodings, and count every record and data point as staged: as many as the
/// request has, which is what a partial success counts its rejected records from
/// (`OtelResources::total`, and for protobuf the outline and the wire count of
/// data points).
fn chunking_keeps_rows(
    mut request: Req,
    rows: usize,
    bytes: usize,
    flatten_exemplars: bool,
) -> TestCaseResult {
    fill_unset_timestamps(&mut request);
    prop_assume!(json_representable(&request));
    let limits = ChunkLimits { rows, bytes };
    for protobuf in [true, false] {
        let decoded = decode_as(&request, protobuf);
        let resources = decoded.resources();
        let mut whole = Vec::new();
        resources
            .flatten_into(
                &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                flatten_exemplars,
                &mut whole,
            )
            .unwrap();
        let mut chunks: Vec<Vec<Value>> = Vec::new();
        let mut chunker = Chunker::new(limits, |rows, _| {
            chunks.push(rows);
            Ok::<_, JsonFlattenError>(())
        });
        resources
            .flatten_into(
                &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                flatten_exemplars,
                &mut chunker,
            )
            .unwrap();
        chunker.finish().unwrap();
        let staged = chunker.staged();
        drop(chunker);

        prop_assert!(
            chunks
                .iter()
                .all(|chunk| !chunk.is_empty() && chunk.len() <= rows)
        );
        prop_assert_eq!(staged.chunks as usize, chunks.len());
        let chunked: Vec<Value> = chunks.into_iter().flatten().collect();
        prop_assert_eq!(
            serde_json::to_vec(&chunked).unwrap(),
            serde_json::to_vec(&whole).unwrap(),
            "chunked rows differ (protobuf: {})",
            protobuf
        );
        prop_assert_eq!(staged.rows as usize, whole.len());
        prop_assert_eq!(staged.records, decoded.record_count());
        prop_assert_eq!(staged.points, decoded.point_count());
        let units = match decoded {
            Req::Metrics(_) => staged.points,
            Req::Logs(_) | Req::Traces(_) => staged.records,
        };
        prop_assert_eq!(resources.total(), units);
        if protobuf {
            let body = web::Bytes::from(request.encode_pb());
            let signal = streamed::signal(&decoded);
            let outline = crate::otel::protobuf_stream::outline(&body, signal).unwrap();
            prop_assert_eq!(outline.records, staged.records);
            if signal == crate::otel::protobuf_stream::Signal::Metrics {
                prop_assert_eq!(
                    crate::otel::protobuf_stream::count_points(&body),
                    staged.points
                );
            }
        }
    }
    Ok(())
}

fn chunk_limits() -> impl Strategy<Value = (usize, usize)> {
    (
        1usize..5,
        prop_oneof![Just(1usize), Just(4096), Just(usize::MAX)],
    )
}

#[test]
fn otel_chunked_rows_match_one_chunk_logs() {
    check(
        "otel_chunked_rows_match_one_chunk_logs",
        64,
        (logs_data(false), chunk_limits()),
        |(logs, (rows, bytes))| chunking_keeps_rows(Req::Logs(logs), rows, bytes, false),
    );
}

#[test]
fn otel_chunked_rows_match_one_chunk_traces() {
    check(
        "otel_chunked_rows_match_one_chunk_traces",
        64,
        (traces_data(false), chunk_limits()),
        |(traces, (rows, bytes))| chunking_keeps_rows(Req::Traces(traces), rows, bytes, false),
    );
}

#[test]
fn otel_chunked_rows_match_one_chunk_metrics() {
    check(
        "otel_chunked_rows_match_one_chunk_metrics",
        64,
        (metrics_data(false), chunk_limits(), any::<bool>()),
        |(metrics, (rows, bytes), flatten_exemplars)| {
            chunking_keeps_rows(Req::Metrics(metrics), rows, bytes, flatten_exemplars)
        },
    );
}

// ---------------------------------------------------------------------------
// 2. Robustness of the decoder
// ---------------------------------------------------------------------------

const OTEL_SOURCES: [(LogSource, TelemetryType); 3] = [
    (LogSource::OtelLogs, TelemetryType::Logs),
    (LogSource::OtelMetrics, TelemetryType::Metrics),
    (LogSource::OtelTraces, TelemetryType::Traces),
];

/// Decoding `body` never panics, is deterministic, and a body that does not
/// decode is refused by `flatten_and_push_otel_protobuf` with the decode error
/// before anything is flattened or staged. A body that decodes flattens.
fn decode_is_total(
    body: &[u8],
    source: &LogSource,
    telemetry_type: TelemetryType,
) -> TestCaseResult {
    let first = OtelProtobufRequest::decode(body, source);
    let second = OtelProtobufRequest::decode(body, source);
    match (&first, &second) {
        (Ok(a), Ok(b)) => prop_assert_eq!(
            Req::from_decoded(clone_request(a)).encode_pb(),
            Req::from_decoded(clone_request(b)).encode_pb()
        ),
        (Err(a), Err(b)) => prop_assert_eq!(a.to_string(), b.to_string()),
        _ => prop_assert!(false, "decode is not deterministic: {first:?} / {second:?}"),
    }
    match first {
        Ok(request) => {
            // May hold unset timestamps, so only the shape of the result is checked
            let (_, outcome) = pb_stage(source, body, FlattenLimits::DEFAULT, false);
            let request = Req::from_decoded(request);
            if let Ok(rows) = &outcome.result {
                prop_assert_eq!(rows.len(), request.expected_rows());
            } else {
                // Only the log flatten budget refuses a decoded request
                prop_assert!(
                    matches!(
                        (&request, &outcome.result),
                        (Req::Logs(_), Err(StageError::Flatten(_)))
                    ),
                    "{:?}",
                    outcome.result
                );
            }
        }
        Err(error) => {
            let target = OtelTarget {
                stream_name: "otel_pb_props",
                log_source: source,
                p_custom_fields: &HashMap::new(),
                telemetry_type,
                tenant_id: &None,
            };
            let expected =
                format!("Invalid Request: Ingestion failed for stream otel_pb_props: {error}");
            let body = web::Bytes::copy_from_slice(body);
            let signal = match source {
                LogSource::OtelLogs => crate::otel::protobuf_stream::Signal::Logs,
                LogSource::OtelMetrics => crate::otel::protobuf_stream::Signal::Metrics,
                _ => crate::otel::protobuf_stream::Signal::Traces,
            };
            if crate::otel::protobuf_stream::outline(&body, signal).is_err() {
                // Broken above its records: refused before the dataset field check,
                // the first use of `PARSEABLE`
                let refused =
                    flatten_and_push_otel_protobuf(body.clone(), &target, || PROTOBUF_OPTIONS)
                        .expect_err("a body that does not decode is refused");
                prop_assert!(matches!(refused, PostError::Invalid(_)), "{refused:?}");
                prop_assert_eq!(refused.to_string(), expected.clone());
            }
            // A broken record is met while staging, before the first chunk is full
            let (refused, staged) =
                stage_otel_protobuf(body, &target, PROTOBUF_OPTIONS, test_limits(), |_| Ok(()));
            let refused = refused.expect_err("a body that does not decode is refused");
            prop_assert!(matches!(refused, PostError::Invalid(_)), "{refused:?}");
            prop_assert_eq!(refused.to_string(), expected);
            prop_assert_eq!(staged.chunks, 0);
        }
    }
    Ok(())
}

fn clone_request(request: &OtelProtobufRequest) -> OtelProtobufRequest {
    match request {
        OtelProtobufRequest::Logs(m) => OtelProtobufRequest::Logs(m.clone()),
        OtelProtobufRequest::Metrics(m) => OtelProtobufRequest::Metrics(m.clone()),
        OtelProtobufRequest::Traces(m) => OtelProtobufRequest::Traces(m.clone()),
    }
}

#[test]
fn otel_pb_arbitrary_bytes_never_panic() {
    let decoded = AtomicUsize::new(0);
    check(
        "otel_pb_arbitrary_bytes_never_panic",
        512,
        (vec(any::<u8>(), 0..256), 0..3usize),
        |(body, signal)| {
            let (source, telemetry_type) = &OTEL_SOURCES[signal];
            if OtelProtobufRequest::decode(&body, source).is_ok() {
                decoded.fetch_add(1, Ordering::Relaxed);
            }
            decode_is_total(&body, source, *telemetry_type)
        },
    );
    println!("otel_pb_arbitrary_bytes: decoded={}", decoded.into_inner());
}

#[derive(Debug, Clone)]
enum Mutation {
    Flip(usize, u8),
    Set(usize, u8),
    Insert(usize, u8),
    Remove(usize),
}

fn mutate(body: &mut Vec<u8>, mutation: &Mutation) {
    if body.is_empty() {
        return;
    }
    let len = body.len();
    match *mutation {
        Mutation::Flip(at, mask) => body[at % len] ^= mask.max(1),
        Mutation::Set(at, byte) => body[at % len] = byte,
        Mutation::Insert(at, byte) => body.insert(at % (len + 1), byte),
        Mutation::Remove(at) => {
            body.remove(at % len);
        }
    }
}

fn mutation() -> BoxedStrategy<Mutation> {
    prop_oneof![
        (any::<usize>(), any::<u8>()).prop_map(|(at, mask)| Mutation::Flip(at, mask)),
        (any::<usize>(), any::<u8>()).prop_map(|(at, byte)| Mutation::Set(at, byte)),
        (any::<usize>(), any::<u8>()).prop_map(|(at, byte)| Mutation::Insert(at, byte)),
        any::<usize>().prop_map(Mutation::Remove),
    ]
    .boxed()
}

fn any_request(wild: bool) -> BoxedStrategy<Req> {
    prop_oneof![
        logs_data(wild).prop_map(Req::Logs),
        traces_data(wild).prop_map(Req::Traces),
        metrics_data(wild).prop_map(Req::Metrics),
    ]
    .boxed()
}

#[test]
fn otel_pb_truncated_encodings_never_panic() {
    let (decoded, refused) = (AtomicUsize::new(0), AtomicUsize::new(0));
    check(
        "otel_pb_truncated_encodings_never_panic",
        128,
        (any_request(true), any::<prop::sample::Index>()),
        |(request, cut)| {
            let body = request.encode_pb();
            // An empty request encodes to no bytes, which is a valid (empty) message
            let body = &body[..cut.index(body.len().max(1)).min(body.len())];
            match OtelProtobufRequest::decode(body, &request.source()) {
                Ok(_) => decoded.fetch_add(1, Ordering::Relaxed),
                Err(_) => refused.fetch_add(1, Ordering::Relaxed),
            };
            decode_is_total(body, &request.source(), request.telemetry_type())
        },
    );
    println!(
        "otel_pb_truncated: decoded={} refused={}",
        decoded.into_inner(),
        refused.into_inner()
    );
}

#[test]
fn otel_pb_mutated_encodings_never_panic() {
    let (decoded, refused) = (AtomicUsize::new(0), AtomicUsize::new(0));
    check(
        "otel_pb_mutated_encodings_never_panic",
        128,
        (any_request(true), vec(mutation(), 1..5)),
        |(request, mutations)| {
            let mut body = request.encode_pb();
            for m in &mutations {
                mutate(&mut body, m);
            }
            match OtelProtobufRequest::decode(&body, &request.source()) {
                Ok(_) => decoded.fetch_add(1, Ordering::Relaxed),
                Err(_) => refused.fetch_add(1, Ordering::Relaxed),
            };
            decode_is_total(&body, &request.source(), request.telemetry_type())
        },
    );
    println!(
        "otel_pb_mutated: decoded={} refused={}",
        decoded.into_inner(),
        refused.into_inner()
    );
}

/// A body of one signal sent to another signal's endpoint decodes or is
/// refused like any other bytes
#[test]
fn otel_pb_bodies_sent_to_the_wrong_signal_never_panic() {
    check(
        "otel_pb_bodies_sent_to_the_wrong_signal",
        128,
        (any_request(true), 0..3usize),
        |(request, signal)| {
            let (source, telemetry_type) = &OTEL_SOURCES[signal];
            decode_is_total(&request.encode_pb(), source, *telemetry_type)
        },
    );
}

// ---------------------------------------------------------------------------
// 3. Flatten limits of logs
// ---------------------------------------------------------------------------

/// Log requests whose bodies expand into rows, against small limits: both
/// encodings are refused for the same requests with the same error, and charge
/// the budget alike
#[test]
fn otel_pb_log_flatten_limits_match_json() {
    let outcomes: [AtomicUsize; 4] = Default::default();
    check(
        "otel_pb_log_flatten_limits_match_json",
        192,
        (logs_data(false), tight_limits(), any::<bool>()),
        |(logs, limits, normalise)| {
            let request = Req::Logs(logs);
            prop_assume!(json_representable(&request));
            let outcome = assert_encodings_agree(&request, limits, false, normalise)?;
            let kind = match &outcome.result {
                Ok(_) => 0,
                Err(StageError::Flatten(e)) if e.contains("P_MAX_FLATTEN_ROWS_PER_EVENT") => 1,
                Err(StageError::Flatten(e)) if e.contains("P_MAX_FLATTEN_ROWS_PER_REQUEST") => 2,
                Err(StageError::Flatten(_)) => 3,
                Err(StageError::Decode(e)) => {
                    return Err(TestCaseError::fail(format!("decode failed: {e}")));
                }
            };
            outcomes[kind].fetch_add(1, Ordering::Relaxed);
            Ok(())
        },
    );
    let [ok, per_event, per_request, bytes] = outcomes.map(AtomicUsize::into_inner);
    println!(
        "otel_pb_log_flatten_limits: ok={ok} rows_per_event={per_event} rows_per_request={per_request} other_limit={bytes}"
    );
    // Not vacuous: the generated requests both pass and hit the limits
    assert!(ok > 0 && per_event + per_request + bytes > 0);
}

// ---------------------------------------------------------------------------
// What the OTLP JSON mapping can not carry
// ---------------------------------------------------------------------------
//
// The JSON path decodes with the `with-serde` mapping of opentelemetry-proto
// 0.32. These tests pin what it does with messages the protobuf path takes
// whole, so a dependency upgrade that changes it is noticed.

fn kv(key: &str, value: Option<any_value::Value>) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value }),
        key_strindex: 0,
    }
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    kv(key, Some(any_value::Value::StringValue(value.to_string())))
}

fn one_log(attributes: Vec<KeyValue>, body: Option<AnyValue>) -> Req {
    Req::Logs(LogsData {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    observed_time_unix_nano: 1_700_000_000_000_000_000,
                    severity_number: 9,
                    attributes,
                    body,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    })
}

fn both(request: &Req) -> (Outcome, Outcome) {
    let source = request.source();
    let (_, pb) = pb_stage(&source, &request.encode_pb(), FlattenLimits::DEFAULT, false);
    let (_, js) = json_stage(
        &source,
        &request.encode_json(),
        FlattenLimits::DEFAULT,
        false,
        true,
    );
    (pb, js)
}

fn json_text_stage(source: LogSource, json: Value) -> Outcome {
    json_stage(
        &source,
        json.to_string().as_bytes(),
        FlattenLimits::DEFAULT,
        false,
        true,
    )
    .1
}

/// A non-finite `doubleValue` is accepted over protobuf (the flatteners drop
/// the attribute) but refuses the whole request over JSON: serde_json writes
/// it as `null`, and the OTLP/JSON spelling `"NaN"` is refused too, since the
/// mapping reads `doubleValue` as a plain JSON number.
#[test]
fn json_mapping_refuses_non_finite_any_value_doubles() {
    for double in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let request = one_log(
            vec![
                string_kv("kept", "x"),
                kv("d", Some(any_value::Value::DoubleValue(double))),
            ],
            None,
        );
        let (pb, js) = both(&request);
        let rows = pb.result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["kept"], json!("x"));
        assert!(rows[0].get("d").is_none(), "{double}: {:?}", rows[0]);
        assert!(
            matches!(&js.result, Err(StageError::Decode(e)) if e.contains("null")),
            "{double}: {:?}",
            js.result
        );
    }
    for spelling in ["NaN", "Infinity", "-Infinity"] {
        let js = json_text_stage(
            LogSource::OtelLogs,
            json!({"resourceLogs": [{"scopeLogs": [{"logRecords": [{
                "timeUnixNano": "1700000000000000000",
                "attributes": [{"key": "d", "value": {"doubleValue": spelling}}]
            }]}]}]}),
        );
        assert!(
            matches!(&js.result, Err(StageError::Decode(e)) if e.contains("expected f64")),
            "{spelling}: {:?}",
            js.result
        );
    }
}

/// An `AnyValue` with no value set (`{}` in JSON, valid in OTLP) is accepted
/// over protobuf and refuses the whole request over JSON, wherever it is
#[test]
fn json_mapping_refuses_empty_any_values() {
    let empty = || AnyValue { value: None };
    let cases = [
        ("attribute", one_log(vec![kv("e", None)], None)),
        ("body", one_log(vec![], Some(empty()))),
        (
            "array element",
            one_log(
                vec![kv(
                    "a",
                    Some(any_value::Value::ArrayValue(ArrayValue {
                        values: vec![empty()],
                    })),
                )],
                None,
            ),
        ),
    ];
    for (place, request) in cases {
        let (pb, js) = both(&request);
        assert_eq!(pb.result.unwrap().len(), 1, "{place}");
        assert!(
            matches!(&js.result, Err(StageError::Decode(e)) if e.contains("no known keys")),
            "{place}: {:?}",
            js.result
        );
    }
}

/// `string_value_strindex` (profiles only) is ignored by the flatteners over
/// protobuf; the JSON mapping does not read it back, so the request is refused
#[test]
fn json_mapping_refuses_string_value_strindex() {
    let request = one_log(
        vec![
            string_kv("kept", "x"),
            kv("s", Some(any_value::Value::StringValueStrindex(7))),
        ],
        None,
    );
    let (pb, js) = both(&request);
    let rows = pb.result.unwrap();
    assert!(rows[0].get("s").is_none());
    assert!(
        matches!(&js.result, Err(StageError::Decode(e)) if e.contains("no known keys")),
        "{:?}",
        js.result
    );
}

fn one_metric(data: metric::Data) -> Req {
    Req::Metrics(MetricsData {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    Metric {
                        name: "probe".to_string(),
                        data: Some(data),
                        ..Default::default()
                    },
                    Metric {
                        name: "witness".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![number_point(number_data_point::Value::AsInt(1))],
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    })
}

fn number_point(value: number_data_point::Value) -> NumberDataPoint {
    NumberDataPoint {
        start_time_unix_nano: 1_700_000_000_000_000_000,
        time_unix_nano: 1_700_000_000_000_000_000,
        value: Some(value),
        ..Default::default()
    }
}

fn metric_names(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["metric_name"].as_str().unwrap())
        .collect()
}

/// A non-finite gauge or sum value: protobuf stores it as `null`, the JSON
/// mapping loses the value. `NumberDataPoint.value` is a `#[serde(flatten)]`
/// `Option`, and serde turns an error inside a flattened `Option` into `None`,
/// so the data point is kept without `data_point_value`.
#[test]
fn json_mapping_drops_non_finite_number_values() {
    for double in [f64::NAN, f64::INFINITY] {
        let request = one_metric(metric::Data::Gauge(Gauge {
            data_points: vec![number_point(number_data_point::Value::AsDouble(double))],
        }));
        let (pb, js) = both(&request);
        let (pb, js) = (pb.result.unwrap(), js.result.unwrap());
        assert_eq!(metric_names(&pb), ["probe", "witness"]);
        assert_eq!(metric_names(&js), ["probe", "witness"]);
        assert_eq!(pb[0]["data_point_value"], Value::Null);
        assert!(
            js[0].get("data_point_value").is_none(),
            "{double}: {}",
            js[0]
        );
        let mut without_value = pb[0].clone();
        without_value
            .as_object_mut()
            .unwrap()
            .remove("data_point_value");
        assert_eq!(without_value, js[0]);
    }
}

/// Anything the JSON mapping can not read inside a metric's data — an empty
/// `AnyValue` or non-finite double in a data point attribute, a non-finite
/// exemplar, histogram bound, summary sum or exponential histogram zero
/// threshold — silently removes the whole metric over JSON, while the request
/// still succeeds. `Metric.data` is a flattened `Option` too. Over protobuf the
/// metric is stored. In production an all-dropped request is a 200 with no rows.
#[test]
fn json_mapping_silently_drops_metrics_it_can_not_read() {
    let nan_attribute = vec![kv("d", Some(any_value::Value::DoubleValue(f64::NAN)))];
    let empty_attribute = vec![kv("e", None)];
    let cases: Vec<(&str, metric::Data)> = vec![
        (
            "empty AnyValue attribute",
            metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: empty_attribute,
                    ..number_point(number_data_point::Value::AsInt(1))
                }],
            }),
        ),
        (
            "NaN attribute",
            metric::Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    attributes: nan_attribute,
                    ..number_point(number_data_point::Value::AsInt(1))
                }],
                aggregation_temporality: 2,
                is_monotonic: true,
            }),
        ),
        (
            "NaN exemplar",
            metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    exemplars: vec![Exemplar {
                        time_unix_nano: 1,
                        value: Some(exemplar::Value::AsDouble(f64::NAN)),
                        ..Default::default()
                    }],
                    ..number_point(number_data_point::Value::AsInt(1))
                }],
            }),
        ),
        (
            "infinite histogram bound",
            metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    start_time_unix_nano: 1,
                    time_unix_nano: 1,
                    count: 1,
                    bucket_counts: vec![0, 1],
                    explicit_bounds: vec![f64::INFINITY],
                    ..Default::default()
                }],
                aggregation_temporality: 1,
            }),
        ),
        (
            "NaN summary sum",
            metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    start_time_unix_nano: 1,
                    time_unix_nano: 1,
                    sum: f64::NAN,
                    ..Default::default()
                }],
            }),
        ),
        (
            "NaN exponential histogram zero threshold",
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    start_time_unix_nano: 1,
                    time_unix_nano: 1,
                    zero_threshold: f64::NAN,
                    ..Default::default()
                }],
                aggregation_temporality: 1,
            }),
        ),
    ];
    for (case, data) in cases {
        let request = one_metric(data);
        let (pb, js) = both(&request);
        assert_eq!(
            metric_names(&pb.result.unwrap()),
            ["probe", "witness"],
            "{case}"
        );
        assert_eq!(metric_names(&js.result.unwrap()), ["witness"], "{case}");
    }

    // The same with the OTLP/JSON spelling of NaN a protojson client sends
    let js = json_text_stage(
        LogSource::OtelMetrics,
        json!({"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "probe",
            "summary": {"dataPoints": [{
                "timeUnixNano": "1700000000000000000",
                "sum": "NaN",
                "quantileValues": [{"quantile": 0.5, "value": "NaN"}]
            }]}
        }]}]}]}),
    );
    assert_eq!(js.result, Ok(vec![]));
}

/// Over protobuf a NaN summary sum is stored as `null`; the OTLP/JSON spelling
/// `"NaN"` is read only for quantile values (the one field the mapping
/// special-cases), which are stored as `null` either way
#[test]
fn json_mapping_reads_non_finite_quantiles_and_optional_sums() {
    let request = one_metric(metric::Data::Summary(Summary {
        data_points: vec![SummaryDataPoint {
            start_time_unix_nano: 1,
            time_unix_nano: 1,
            sum: 1.5,
            quantile_values: vec![ValueAtQuantile {
                quantile: f64::NAN,
                value: f64::NEG_INFINITY,
            }],
            ..Default::default()
        }],
    }));
    let (pb, js) = both(&request);
    let rows = pb.result.unwrap();
    assert_eq!(
        rows[0]["data_point_quantile_values"],
        json!([{"quantile": null, "value": null}])
    );
    assert_eq!(Ok(rows), js.result);

    // Optional histogram sum, min and max become null, which reads back as unset
    let request = one_metric(metric::Data::Histogram(Histogram {
        data_points: vec![HistogramDataPoint {
            start_time_unix_nano: 1,
            time_unix_nano: 1,
            sum: Some(f64::NAN),
            min: Some(f64::NEG_INFINITY),
            max: Some(f64::INFINITY),
            ..Default::default()
        }],
        aggregation_temporality: 2,
    }));
    let (pb, js) = both(&request);
    let rows = pb.result.unwrap();
    assert!(rows[0].get("data_point_sum").is_none());
    assert_eq!(Ok(rows), js.result);
}

/// The JSON path reads some doubles one ULP off: serde_json writes the
/// shortest decimal that reads back exactly, but this crate builds serde_json
/// without `float_roundtrip`, whose faster parser rounds some 17-digit
/// decimals wrong. Protobuf carries the bits. Found by
/// `otel_pb_differential_logs` and `otel_pb_differential_traces`; minimal input
/// a single attribute of this value.
#[test]
fn json_path_reads_some_doubles_one_ulp_off() {
    let double = -4.4597825407728543e-212_f64;
    let request = one_log(
        vec![kv("d", Some(any_value::Value::DoubleValue(double)))],
        None,
    );
    let (pb, js) = both(&request);
    let (pb, js) = (pb.result.unwrap(), js.result.unwrap());
    assert_eq!(pb[0]["d"].as_f64().unwrap().to_bits(), double.to_bits());
    let read = js[0]["d"].as_f64().unwrap();
    assert_eq!(read.to_bits().abs_diff(double.to_bits()), 1, "{read:e}");
    assert_eq!(
        serde_json::to_string(&double).unwrap(),
        "-4.4597825407728543e-212"
    );
}

/// How wrong the JSON path's doubles get: a few ULPs at most, and only for
/// some doubles. The count and the largest error seen are printed.
#[test]
fn json_path_double_error_is_a_few_ulps() {
    let (off, worst) = (AtomicUsize::new(0), AtomicUsize::new(0));
    check(
        "json_path_double_error_is_a_few_ulps",
        4096,
        finite_f64(),
        |double| {
            let read: f64 = serde_json::from_str(&serde_json::to_string(&double).unwrap()).unwrap();
            prop_assert_eq!(read.is_sign_negative(), double.is_sign_negative());
            let ulps = read.to_bits().abs_diff(double.to_bits());
            if ulps > 0 {
                off.fetch_add(1, Ordering::Relaxed);
                worst.fetch_max(ulps as usize, Ordering::Relaxed);
                prop_assert!(ulps <= 4, "{double:e} read back as {read:e}, {ulps} ULPs");
            }
            Ok(())
        },
    );
    println!(
        "json_path_double_error: inexact={} worst_ulps={}",
        off.into_inner(),
        worst.into_inner()
    );
}

/// One log record whose attribute `deep` is an array nested `depth` times,
/// written by hand: prost encodes recursively, and so does serde
fn deeply_nested_log_body(depth: usize) -> Vec<u8> {
    fn wrap(field: u32, inner: Vec<u8>) -> Vec<u8> {
        let mut out = Vec::with_capacity(inner.len() + 11);
        prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(inner.len() as u64, &mut out);
        out.extend(inner);
        out
    }
    let mut value = Vec::new(); // AnyValue with no value
    for _ in 0..depth {
        value = wrap(5, wrap(1, value)); // AnyValue.array_value -> ArrayValue.values
    }
    let mut key_value = wrap(1, b"deep".to_vec()); // KeyValue.key
    key_value.extend(wrap(2, value)); // KeyValue.value
    let mut record = Vec::new();
    prost::encoding::encode_key(1, prost::encoding::WireType::SixtyFourBit, &mut record);
    record.extend(1u64.to_le_bytes()); // LogRecord.time_unix_nano
    prost::encoding::encode_key(11, prost::encoding::WireType::SixtyFourBit, &mut record);
    record.extend(1u64.to_le_bytes()); // LogRecord.observed_time_unix_nano
    record.extend(wrap(6, key_value)); // LogRecord.attributes
    // ExportLogsServiceRequest.resource_logs -> ResourceLogs.scope_logs -> ScopeLogs.log_records
    wrap(1, wrap(2, wrap(2, record)))
}

/// Deep nesting can not exhaust the stack: prost stops at its recursion limit
/// of 100 messages and the body is refused. Each encoding refuses at its own
/// depth (serde_json allows 128 nested JSON containers, three per array level),
/// so a request nested between the two limits is taken over protobuf only.
#[test]
fn nesting_depth_limits_of_both_encodings() {
    let accepted = |depth: usize| {
        OtelProtobufRequest::decode(&deeply_nested_log_body(depth), &LogSource::OtelLogs).is_ok()
    };
    assert!(accepted(1));
    for depth in [1_000, 100_000] {
        let body = deeply_nested_log_body(depth);
        let error = OtelProtobufRequest::decode(&body, &LogSource::OtelLogs).unwrap_err();
        assert!(error.to_string().contains("recursion limit"), "{error}");
    }
    let pb_max = (1..200)
        .take_while(|&depth| accepted(depth))
        .last()
        .unwrap();

    let json_accepted = |depth: usize| {
        let mut value = json!({"arrayValue": {}});
        for _ in 1..depth {
            value = json!({"arrayValue": {"values": [value]}});
        }
        let body = json!({"resourceLogs": [{"scopeLogs": [{"logRecords": [{
            "timeUnixNano": "1",
            "observedTimeUnixNano": "1",
            "attributes": [{"key": "deep", "value": value}]
        }]}]}]});
        json_text_stage(LogSource::OtelLogs, body).result.is_ok()
    };
    let json_max = (1..200)
        .take_while(|&depth| json_accepted(depth))
        .last()
        .unwrap();

    // The rows of the deepest protobuf request
    let (_, outcome) = pb_stage(
        &LogSource::OtelLogs,
        &deeply_nested_log_body(pb_max),
        FlattenLimits::DEFAULT,
        false,
    );
    assert_eq!(outcome.result.unwrap().len(), 1);
    println!("nesting_depth_limits: protobuf={pb_max} json={json_max}");
    assert_eq!((pb_max, json_max), (47, 39));
}

/// BUG (pre-existing in the metrics flattener, both encodings, same upstream):
/// `flatten_metrics_record` merges a metric's `metadata` attributes over the
/// fields of each of its rows, after `time_unix_nano` and `metric_name` are
/// set, and `process_resource_metrics` takes the `target_info` timestamp from
/// any row's `time_unix_nano` string. So a metric without data points whose
/// metadata has a `time_unix_nano` attribute emits a `target_info` sample
/// whose timestamp is that attribute: here the string "garbage". (The same
/// merge lets metadata replace a data point's timestamp or metric name.)
/// Found by `otel_pb_differential_metrics` with 3000 cases (row count 2, model
/// 1); this is the minimal input reduced by hand. Expected: no `target_info`
/// row without a data point, as for a resource whose metrics have none.
#[test]
#[ignore = "bug: metric metadata supplies the target_info timestamp, see doc comment"]
fn metric_metadata_can_not_supply_the_target_info_timestamp() {
    let request = Req::Metrics(MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "checkout")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "empty".to_string(),
                    metadata: vec![string_kv("time_unix_nano", "garbage")],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
    let (pb, js) = both(&request);
    let rows = pb.result.unwrap();
    assert_eq!(Ok(rows.clone()), js.result, "the encodings agree");
    assert_eq!(metric_names(&rows), ["empty"], "rows: {rows:?}");
}

// ---------------------------------------------------------------------------
// 4. Seeded deterministic scenario
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Encoding {
    Protobuf,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Damage {
    None,
    Truncated,
    Mutated,
    Garbage,
}

struct Scenario {
    /// Outcome counts by kind, for the coverage line
    counts: BTreeMap<String, u64>,
    /// Digest of everything that does not depend on the clock
    digest: u64,
    trace: Vec<String>,
}

/// One seed: a sequence of generated requests of all three signals, sent in
/// either encoding, some damaged, some against small flatten limits, through
/// the decode and flatten stages into a sink of accepted rows.
///
/// Invariants per request:
/// - an undamaged body decodes, and with the default limits flattens;
/// - a body that does not decode yields no rows, and for protobuf
///   `flatten_and_push_otel_protobuf` refuses it with the decode error;
/// - a decoded request yields exactly the rows its structure implies (records;
///   spans times events and links; data points plus `target_info`), or for
///   logs a flatten-limit error and no rows;
/// - every decoded request the JSON mapping can carry gives the same outcome
///   in the other encoding (unset timestamps of damaged bodies filled first).
///
/// Across the run: the sink holds exactly the rows of accepted requests, and
/// the same seed gives the same run.
fn run_scenario(seed: u64) -> Result<Scenario, (String, Vec<String>)> {
    let mut rng = SplitMix64(seed);
    let mut counts = BTreeMap::<String, u64>::new();
    let mut trace = Vec::new();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut sink: Vec<Value> = Vec::new();
    let mut accepted_rows = 0usize;
    let requests_of = |wild: bool| -> [BoxedStrategy<Req>; 3] {
        [
            logs_data(wild).prop_map(Req::Logs).boxed(),
            traces_data(wild).prop_map(Req::Traces).boxed(),
            metrics_data(wild).prop_map(Req::Metrics).boxed(),
        ]
    };
    let (strategies, wild) = (requests_of(false), requests_of(true));
    let tight = tight_limits();

    let requests = 6 + rng.below(10);
    for index in 0..requests {
        let signal = rng.below(3) as usize;
        let request = if rng.percent(25) {
            sample(&wild[signal], &mut rng)
        } else {
            sample(&strategies[signal], &mut rng)
        };
        let encoding = if rng.percent(50) {
            Encoding::Protobuf
        } else {
            Encoding::Json
        };
        // JSON can not encode what it can not carry; such a request goes as protobuf
        let encoding = if json_representable(&request) {
            encoding
        } else {
            Encoding::Protobuf
        };
        let damage = match rng.below(100) {
            0..60 => Damage::None,
            60..75 => Damage::Truncated,
            75..90 => Damage::Mutated,
            _ => Damage::Garbage,
        };
        let limits = if rng.percent(20) {
            sample(&tight, &mut rng)
        } else {
            FlattenLimits::DEFAULT
        };
        let mut body = match encoding {
            Encoding::Protobuf => request.encode_pb(),
            Encoding::Json => request.encode_json(),
        };
        match damage {
            Damage::None => {}
            Damage::Truncated => {
                let len = rng.below(body.len() as u64) as usize;
                body.truncate(len);
            }
            Damage::Mutated => {
                for _ in 0..1 + rng.below(4) {
                    let m = match rng.below(4) {
                        0 => Mutation::Flip(rng.next_u64() as usize, rng.next_u64() as u8),
                        1 => Mutation::Set(rng.next_u64() as usize, rng.next_u64() as u8),
                        2 => Mutation::Insert(rng.next_u64() as usize, rng.next_u64() as u8),
                        _ => Mutation::Remove(rng.next_u64() as usize),
                    };
                    mutate(&mut body, &m);
                }
            }
            Damage::Garbage => {
                body = (0..rng.below(64)).map(|_| rng.next_u64() as u8).collect();
            }
        }
        let label = format!(
            "#{index} {} {encoding:?} {damage:?} limits={limits:?} body={}B",
            request.name(),
            body.len()
        );
        trace.push(label.clone());
        let fail =
            |what: String, trace: &mut Vec<String>| -> Result<Scenario, (String, Vec<String>)> {
                trace.push(format!("  FAILED: {what}"));
                Err((format!("{label}: {what}"), std::mem::take(trace)))
            };
        let source = request.source();

        let normalise = rng.percent(50);
        let (decoded, outcome) = match encoding {
            Encoding::Protobuf => pb_stage(&source, &body, limits, false),
            Encoding::Json => json_stage(&source, &body, limits, false, normalise),
        };
        let kind = match (&decoded, &outcome.result) {
            (None, Err(StageError::Decode(error))) => {
                if damage == Damage::None {
                    return fail(format!("undamaged body refused: {error}"), &mut trace);
                }
                if encoding == Encoding::Protobuf {
                    let (refused, _) = stage_otel_protobuf(
                        web::Bytes::copy_from_slice(&body),
                        &OtelTarget {
                            stream_name: "dst",
                            log_source: &source,
                            p_custom_fields: &HashMap::new(),
                            telemetry_type: request.telemetry_type(),
                            tenant_id: &None,
                        },
                        PROTOBUF_OPTIONS,
                        test_limits(),
                        |_| Ok(()),
                    );
                    if !matches!(&refused, Err(PostError::Invalid(e)) if e.to_string().ends_with(error.as_str()))
                    {
                        return fail(
                            format!("entry point did not refuse: {refused:?}"),
                            &mut trace,
                        );
                    }
                }
                error.hash(&mut hasher);
                "decode_refused"
            }
            (Some(decoded), Ok(rows)) => {
                let expected = decoded.expected_rows();
                if rows.len() != expected {
                    return fail(
                        format!("{} rows, structure implies {expected}", rows.len()),
                        &mut trace,
                    );
                }
                let mut replay = decoded.clone();
                let clock = fill_unset_timestamps(&mut replay);
                if !clock {
                    serde_json::to_string(rows).unwrap().hash(&mut hasher);
                }
                rows.len().hash(&mut hasher);
                accepted_rows += rows.len();
                sink.extend(rows.iter().cloned());
                if damage == Damage::None {
                    "accepted"
                } else {
                    "damaged_but_accepted"
                }
            }
            (Some(decoded), Err(StageError::Flatten(error))) => {
                if !matches!(decoded, Req::Logs(_)) || limits == FlattenLimits::DEFAULT {
                    return fail(format!("unexpected flatten refusal: {error}"), &mut trace);
                }
                error.hash(&mut hasher);
                "flatten_limit_refused"
            }
            other => return fail(format!("impossible stage result {other:?}"), &mut trace),
        };
        trace.push(format!("  -> {kind}"));
        (index, encoding, damage, kind).hash(&mut hasher);
        *counts
            .entry(format!("{}_{kind}_{encoding:?}", request.name()))
            .or_default() += 1;

        // The other encoding of what was decoded agrees
        if let Some(mut decoded) = decoded {
            fill_unset_timestamps(&mut decoded);
            if json_representable(&decoded) {
                if let Err(error) = assert_encodings_agree(&decoded, limits, false, normalise) {
                    return fail(format!("encodings disagree: {error}"), &mut trace);
                }
                *counts.entry("cross_encoding_checked".into()).or_default() += 1;
            } else {
                *counts
                    .entry("cross_encoding_not_representable".into())
                    .or_default() += 1;
            }
        }
    }
    if sink.len() != accepted_rows {
        return Err((
            format!("sink holds {} rows, accepted {accepted_rows}", sink.len()),
            trace,
        ));
    }
    *counts.entry("rows".into()).or_default() += sink.len() as u64;
    Ok(Scenario {
        counts,
        digest: hasher.finish(),
        trace,
    })
}

/// The seeded scenario, after the DST conventions: `DST_SEED` (default 0) is
/// the first seed, `DST_ITERS` (default 32) the number of seeds, a failure
/// prints its seed and trace, and each seed is run twice to check the run is
/// deterministic.
///
/// How far down it goes: decode and flatten, i.e. everything
/// `flatten_and_push_otel_protobuf` and the OTEL arms of `flatten_and_push_logs`
/// do before `push_logs`. It stops there because every later step reads the
/// `PARSEABLE` global:
/// - `verify_dataset_fields_count` and `push_logs` look the stream up in
///   `PARSEABLE` (a stream handle passed in, or a lookup trait, would be the seam);
/// - `FlattenLimits::from_config` reads the server options (the tests pass
///   limits instead, which is what that function returns);
/// - `convert_array_to_object` calls `has_more_than_max_allowed_levels`, which
///   reads `PARSEABLE.options.event_flatten_level`, for schema version V1, even
///   for OTEL sources, because `should_apply_generic_flattening` tests the depth
///   before the log source (testing the source first, or passing the level in,
///   would open `convert_array_to_object` and `json::Event::into_recordbatch`,
///   which takes `p_timestamp` as an argument, to these tests);
/// - `Event::process` stages into the stream from `PARSEABLE`.
///
/// Rows also depend on the clock wherever a timestamp is unset
/// (`convert_epoch_nano_to_timestamp`); generated requests set them all, and
/// damaged bodies that decode with unset ones are compared after filling them.
#[test]
fn dst_otlp_mixed_encoding_ingest() {
    let first = env_u64("DST_SEED").unwrap_or(0);
    let iterations = env_u64("DST_ITERS").unwrap_or(32);
    assert!(iterations > 0, "DST_ITERS must be positive");
    let mut totals = BTreeMap::<String, u64>::new();
    for seed in first..first + iterations {
        let run = match run_scenario(seed) {
            Ok(run) => run,
            Err((error, trace)) => panic!(
                "DST otlp_mixed_encoding seed={seed}: {error}\nreplay: DST_SEED={seed} DST_ITERS=1\n{}",
                trace.join("\n")
            ),
        };
        let again = run_scenario(seed).unwrap_or_else(|(error, _)| {
            panic!("DST otlp_mixed_encoding seed={seed}: second run failed: {error}")
        });
        assert_eq!(
            run.digest,
            again.digest,
            "DST otlp_mixed_encoding seed={seed}: the same seed gave a different run\n{}",
            run.trace.join("\n")
        );
        assert_eq!(run.counts, again.counts, "seed={seed}");
        for (key, value) in run.counts {
            *totals.entry(key).or_default() += value;
        }
    }
    println!(
        "DST_COVERAGE scenario=otlp_mixed_encoding first_seed={first} iterations={iterations} counts={totals:?}"
    );
    if iterations >= 32 {
        // Not vacuous: every kind of outcome happened
        let seen = |needle: &str| totals.keys().any(|key| key.contains(needle));
        for needle in [
            "_accepted_Protobuf",
            "_accepted_Json",
            "decode_refused_Protobuf",
            "decode_refused_Json",
            "damaged_but_accepted",
            "flatten_limit_refused",
            "cross_encoding_checked",
            "cross_encoding_not_representable",
        ] {
            assert!(seen(needle), "no {needle} outcome in {totals:?}");
        }
    }
}

/// The scenario itself is seed-sensitive: different seeds give different runs
#[test]
fn dst_otlp_mixed_encoding_seeds_differ() {
    let digests: std::collections::HashSet<u64> = (0..4)
        .map(|seed| run_scenario(seed).map_err(|(e, _)| e).unwrap().digest)
        .collect();
    assert_eq!(digests.len(), 4);
}

// ---------------------------------------------------------------------------
// Streamed decoding (`protobuf_stream`) against whole-request decoding
// ---------------------------------------------------------------------------

mod streamed {
    use super::*;
    use crate::otel::decoded_size::{
        Leaf, bytes_per_wire_byte, leaf_decoded_size, leaf_struct_size,
        metric_points as wire_points,
    };
    use crate::otel::protobuf_stream::{
        Signal, StreamError, count_points, flatten_request, outline, refusal,
    };
    use prost::Message as ProstMessage;
    use prost::encoding::{WireType, encode_key, encode_varint};

    /// Damage to one leaf record of a body: byte `at` set to `byte`, or the record
    /// cut at `at`, inside the record (its framing is rewritten to fit)
    #[derive(Debug, Clone, Copy)]
    struct RecordDamage {
        index: u64,
        at: u64,
        byte: u8,
        cut: bool,
    }

    /// Wire edits applied while re-encoding a request field by field, and the
    /// record damage to apply, with the count of records seen so far
    struct Edits<'a>(&'a mut SplitMix64, Option<RecordDamage>, u64);

    impl Edits<'_> {
        /// The encoding of the next leaf record, damaged if it is the one to damage
        fn record(&mut self, mut encoded: Vec<u8>) -> Vec<u8> {
            let index = self.2;
            self.2 += 1;
            if let Some(damage) = self.1
                && damage.index == index
                && !encoded.is_empty()
            {
                let at = (damage.at % encoded.len() as u64) as usize;
                if damage.cut {
                    encoded.truncate(at);
                } else {
                    encoded[at] = damage.byte;
                }
            }
            encoded
        }

        /// An unknown field (numbers 4-40 or a large one) of a random wire type
        fn unknown(&mut self, out: &mut Vec<u8>) {
            let tag = if self.0.percent(80) {
                4 + self.0.below(37) as u32
            } else {
                1 << 20
            };
            match self.0.below(5) {
                0 => {
                    encode_key(tag, WireType::Varint, out);
                    encode_varint(self.0.next_u64() >> self.0.below(64), out);
                }
                1 => {
                    encode_key(tag, WireType::SixtyFourBit, out);
                    out.extend_from_slice(&self.0.next_u64().to_le_bytes());
                }
                2 => {
                    encode_key(tag, WireType::ThirtyTwoBit, out);
                    out.extend_from_slice(&(self.0.next_u64() as u32).to_le_bytes());
                }
                3 => {
                    let len = self.0.below(12) as usize;
                    encode_key(tag, WireType::LengthDelimited, out);
                    encode_varint(len as u64, out);
                    out.extend((0..len).map(|_| self.0.next_u64() as u8));
                }
                _ => {
                    encode_key(tag, WireType::StartGroup, out);
                    if self.0.percent(50) {
                        encode_key(tag + 1, WireType::Varint, out);
                        encode_varint(7, out);
                    }
                    encode_key(tag, WireType::EndGroup, out);
                }
            }
        }

        /// `fields` in a shuffled order, with unknown fields between them
        fn level(&mut self, mut fields: Vec<Vec<u8>>) -> Vec<u8> {
            for i in (1..fields.len()).rev() {
                if self.0.percent(50) {
                    let j = self.0.below(i as u64 + 1) as usize;
                    fields.swap(i, j);
                }
            }
            let mut out = Vec::new();
            for field in fields {
                if self.0.percent(25) {
                    self.unknown(&mut out);
                }
                out.extend(field);
            }
            if self.0.percent(25) {
                self.unknown(&mut out);
            }
            out
        }

        fn len_field(tag: u32, body: &[u8]) -> Vec<u8> {
            let mut out = Vec::new();
            encode_key(tag, WireType::LengthDelimited, &mut out);
            encode_varint(body.len() as u64, &mut out);
            out.extend_from_slice(body);
            out
        }

        /// A singular message field, sometimes split over two occurrences, which
        /// prost merges: repeated fields concatenate, the last scalar wins
        fn singular<M: ProstMessage + Clone>(
            &mut self,
            tag: u32,
            msg: &M,
            split: impl Fn(&M) -> (M, M),
        ) -> Vec<Vec<u8>> {
            if self.0.percent(30) {
                let (first, second) = split(msg);
                vec![
                    Self::len_field(tag, &first.encode_to_vec()),
                    Self::len_field(tag, &second.encode_to_vec()),
                ]
            } else {
                vec![Self::len_field(tag, &msg.encode_to_vec())]
            }
        }

        /// `schema_url`, sometimes after an earlier occurrence it replaces
        fn schema_url(&mut self, url: &str) -> Vec<Vec<u8>> {
            let mut fields = vec![];
            if self.0.percent(20) {
                fields.push(Self::len_field(3, b"https://replaced.example"));
            }
            if !url.is_empty() || self.0.percent(20) {
                fields.push(Self::len_field(3, url.as_bytes()));
            }
            fields
        }

        /// A parent level: `resource`/`scope` (field 1), children (field 2), schema_url (3)
        fn parent<P: ProstMessage + Clone>(
            &mut self,
            parent: Option<&P>,
            split: impl Fn(&P) -> (P, P),
            children: Vec<Vec<u8>>,
            url: &str,
        ) -> Vec<u8> {
            let mut fields = vec![];
            if let Some(parent) = parent {
                fields.extend(self.singular(1, parent, split));
            }
            fields.extend(children.iter().map(|child| Self::len_field(2, child)));
            fields.extend(self.schema_url(url));
            self.level(fields)
        }
    }

    fn split_resource(resource: &Resource) -> (Resource, Resource) {
        let at = resource.attributes.len() / 2;
        (
            Resource {
                attributes: resource.attributes[..at].to_vec(),
                dropped_attributes_count: 3,
                ..Default::default()
            },
            Resource {
                attributes: resource.attributes[at..].to_vec(),
                ..resource.clone()
            },
        )
    }

    fn split_scope(scope: &InstrumentationScope) -> (InstrumentationScope, InstrumentationScope) {
        let at = scope.attributes.len() / 2;
        (
            InstrumentationScope {
                name: "replaced".into(),
                attributes: scope.attributes[..at].to_vec(),
                ..Default::default()
            },
            InstrumentationScope {
                attributes: scope.attributes[at..].to_vec(),
                ..scope.clone()
            },
        )
    }

    /// The protobuf body of `request`, re-encoded field by field with edits drawn
    /// from `rng`, with `damage` applied to one of its leaf records
    fn edited_body(request: &Req, rng: &mut SplitMix64, damage: Option<RecordDamage>) -> Vec<u8> {
        let mut edits = Edits(rng, damage, 0);
        let resources: Vec<Vec<u8>> = match request {
            Req::Logs(data) => data
                .resource_logs
                .iter()
                .map(|r| {
                    let scopes = r
                        .scope_logs
                        .iter()
                        .map(|s| {
                            let records = s
                                .log_records
                                .iter()
                                .map(|record| edits.record(record.encode_to_vec()))
                                .collect();
                            edits.parent(s.scope.as_ref(), split_scope, records, &s.schema_url)
                        })
                        .collect();
                    edits.parent(r.resource.as_ref(), split_resource, scopes, &r.schema_url)
                })
                .collect(),
            Req::Traces(data) => data
                .resource_spans
                .iter()
                .map(|r| {
                    let scopes = r
                        .scope_spans
                        .iter()
                        .map(|s| {
                            let spans = s
                                .spans
                                .iter()
                                .map(|span| edits.record(span.encode_to_vec()))
                                .collect();
                            edits.parent(s.scope.as_ref(), split_scope, spans, &s.schema_url)
                        })
                        .collect();
                    edits.parent(r.resource.as_ref(), split_resource, scopes, &r.schema_url)
                })
                .collect(),
            Req::Metrics(data) => data
                .resource_metrics
                .iter()
                .map(|r| {
                    let scopes = r
                        .scope_metrics
                        .iter()
                        .map(|s| {
                            let metrics = s
                                .metrics
                                .iter()
                                .map(|metric| edits.record(metric.encode_to_vec()))
                                .collect();
                            edits.parent(s.scope.as_ref(), split_scope, metrics, &s.schema_url)
                        })
                        .collect();
                    edits.parent(r.resource.as_ref(), split_resource, scopes, &r.schema_url)
                })
                .collect(),
        };
        let fields = resources.iter().map(|r| Edits::len_field(1, r)).collect();
        edits.level(fields)
    }

    pub(super) fn signal(request: &Req) -> Signal {
        match request {
            Req::Logs(_) => Signal::Logs,
            Req::Metrics(_) => Signal::Metrics,
            Req::Traces(_) => Signal::Traces,
        }
    }

    fn leaf(request: &Req) -> Leaf {
        match request {
            Req::Logs(_) => Leaf::LogRecord,
            Req::Metrics(_) => Leaf::Metric,
            Req::Traces(_) => Leaf::Span,
        }
    }

    /// The rows the flatteners build for `request`, or the flatten error text
    fn rows_of(request: &Req, flatten_exemplars: bool) -> Result<Vec<Value>, String> {
        let mut rows = Vec::new();
        request
            .resources()
            .flatten_into(
                &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                flatten_exemplars,
                &mut rows,
            )
            .map_err(|e| format!("flatten: {e}"))?;
        Ok(rows)
    }

    /// The rows of `request`, and whether some of them take the wall clock for an
    /// unset timestamp (then only their number can be compared)
    fn clocked_rows(request: &Req, flatten_exemplars: bool) -> Result<(Vec<Value>, bool), String> {
        let clock = fill_unset_timestamps(&mut request.clone());
        Ok((rows_of(request, flatten_exemplars)?, clock))
    }

    /// Rows of `body` decoded whole, or prost's error text
    fn whole(
        request: &Req,
        body: &[u8],
        flatten_exemplars: bool,
    ) -> Result<(Vec<Value>, bool), String> {
        let decoded =
            OtelProtobufRequest::decode(body, &request.source()).map_err(|e| e.to_string())?;
        clocked_rows(&Req::from_decoded(decoded), flatten_exemplars)
    }

    /// Whether streamed rows are the expected ones: the same, or as many where the
    /// wall clock filled a timestamp
    fn same_rows(streamed: &[Value], expected: &[Value], clock: bool) -> bool {
        if clock {
            streamed.len() == expected.len()
        } else {
            serde_json::to_vec(streamed).unwrap() == serde_json::to_vec(expected).unwrap()
        }
    }

    /// What reading `body` one record at a time staged, in chunks of one row
    #[derive(Debug)]
    struct Streamed {
        rows: Vec<Value>,
        staged: Staged,
        /// The error text, if it stopped
        error: Option<String>,
        /// What a partial success counts its rejected records from: the records of
        /// the outline, or the data points read from the wire
        total: u64,
    }

    fn text(e: StreamError<JsonFlattenError>) -> String {
        match e {
            StreamError::Decode(e) => OtelProtobufError::Decode(e).to_string(),
            StreamError::RecordTooLarge { .. } => "record too large".to_string(),
            StreamError::Consumer(e) => format!("flatten: {e}"),
        }
    }

    fn streamed(request: &Req, body: &[u8], flatten_exemplars: bool) -> Streamed {
        let body = web::Bytes::copy_from_slice(body);
        let signal = signal(request);
        let outlined = match outline(&body, signal) {
            Ok(outlined) => outlined,
            Err(error) => {
                return Streamed {
                    rows: Vec::new(),
                    staged: Staged::default(),
                    error: Some(text(refusal(&body, signal, error, || usize::MAX).widen())),
                    total: 0,
                };
            }
        };
        let mut rows: Vec<Value> = Vec::new();
        let mut chunker = Chunker::new(
            ChunkLimits {
                rows: 1,
                bytes: usize::MAX,
            },
            |chunk, _| {
                rows.extend(chunk);
                Ok::<_, JsonFlattenError>(())
            },
        );
        let result = flatten_request(
            &body,
            signal,
            usize::MAX,
            &mut FlattenBudget::new(FlattenLimits::DEFAULT),
            flatten_exemplars,
            &mut chunker,
        )
        .and_then(|()| chunker.finish().map_err(StreamError::Consumer));
        let staged = chunker.staged();
        drop(chunker);
        Streamed {
            rows,
            staged,
            error: result.err().map(text),
            total: match signal {
                Signal::Metrics => count_points(&body),
                Signal::Logs | Signal::Traces => outlined.records,
            },
        }
    }

    /// The levels of a request above its leaf records, with the records kept as
    /// bytes: prost decodes a body as this exactly when its framing above the
    /// records holds, and merges `resource`, `scope` and `schema_url` as it does
    /// for the real messages
    #[derive(Clone, PartialEq, prost::Message)]
    struct ShallowRequest {
        #[prost(message, repeated, tag = "1")]
        resources: Vec<ShallowResource>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct ShallowResource {
        #[prost(message, optional, tag = "1")]
        resource: Option<Resource>,
        #[prost(message, repeated, tag = "2")]
        scopes: Vec<ShallowScope>,
        #[prost(string, tag = "3")]
        schema_url: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct ShallowScope {
        #[prost(message, optional, tag = "1")]
        scope: Option<InstrumentationScope>,
        #[prost(bytes = "vec", repeated, tag = "2")]
        records: Vec<Vec<u8>>,
        #[prost(string, tag = "3")]
        schema_url: String,
    }

    type Scopes<L> = Vec<(Option<InstrumentationScope>, String, Vec<L>)>;

    /// A request rebuilt from its shallow decoding, with the records that prost
    /// decodes on their own up to the first that it does not
    struct Prefix<L> {
        resources: Vec<(Option<Resource>, String, Scopes<L>)>,
        /// Position of the first record that does not decode, in body order
        failed: Option<u64>,
        records: u64,
        /// Every record, decoded where it decodes
        all: Vec<Result<L, Vec<u8>>>,
    }

    fn prefix<L: ProstMessage + Default + Clone>(shallow: &ShallowRequest) -> Prefix<L> {
        let mut prefix = Prefix {
            resources: vec![],
            failed: None,
            records: 0,
            all: vec![],
        };
        for resource in &shallow.resources {
            if prefix.failed.is_none() {
                prefix.resources.push((
                    resource.resource.clone(),
                    resource.schema_url.clone(),
                    vec![],
                ));
            }
            for scope in &resource.scopes {
                if prefix.failed.is_none() {
                    let scopes = &mut prefix.resources.last_mut().unwrap().2;
                    scopes.push((scope.scope.clone(), scope.schema_url.clone(), vec![]));
                }
                for encoded in &scope.records {
                    let decoded = L::decode(encoded.as_slice()).map_err(|_| encoded.clone());
                    if prefix.failed.is_none() {
                        match &decoded {
                            Ok(record) => {
                                let scopes = &mut prefix.resources.last_mut().unwrap().2;
                                scopes.last_mut().unwrap().2.push(record.clone());
                            }
                            Err(_) => prefix.failed = Some(prefix.records),
                        }
                    }
                    prefix.all.push(decoded);
                    prefix.records += 1;
                }
            }
        }
        prefix
    }

    /// What streaming must stage for a body
    enum Expected {
        /// The framing above the records is broken: nothing is staged
        Outer,
        Records {
            /// The rows of the records before the first that does not decode (all
            /// of them if each does), or the flatten error text
            rows: Result<(Vec<Value>, bool), String>,
            failed: Option<u64>,
            records: u64,
            /// Data points of the records before the first that does not decode
            staged_points: u64,
            /// Data points of every record, those that do not decode read from the wire
            total_points: u64,
        },
    }

    fn is_target_info(row: &Value) -> bool {
        row.get("metric_name") == Some(&json!("target_info"))
            && row.get("metric_description") == Some(&json!("Target metadata"))
    }

    fn expected(request: &Req, body: &[u8], flatten_exemplars: bool) -> Expected {
        let Ok(shallow) = ShallowRequest::decode(body) else {
            return Expected::Outer;
        };
        let (rows, failed, records, staged_points, total_points) = match request {
            Req::Logs(_) => {
                let p = prefix::<LogRecord>(&shallow);
                let request = Req::Logs(LogsData {
                    resource_logs: p
                        .resources
                        .into_iter()
                        .map(|(resource, schema_url, scopes)| ResourceLogs {
                            resource,
                            scope_logs: scopes
                                .into_iter()
                                .map(|(scope, schema_url, log_records)| ScopeLogs {
                                    scope,
                                    log_records,
                                    schema_url,
                                })
                                .collect(),
                            schema_url,
                        })
                        .collect(),
                });
                (
                    clocked_rows(&request, flatten_exemplars),
                    p.failed,
                    p.records,
                    0,
                    0,
                )
            }
            Req::Traces(_) => {
                let p = prefix::<Span>(&shallow);
                let request = Req::Traces(TracesData {
                    resource_spans: p
                        .resources
                        .into_iter()
                        .map(|(resource, schema_url, scopes)| ResourceSpans {
                            resource,
                            scope_spans: scopes
                                .into_iter()
                                .map(|(scope, schema_url, spans)| ScopeSpans {
                                    scope,
                                    spans,
                                    schema_url,
                                })
                                .collect(),
                            schema_url,
                        })
                        .collect(),
                });
                (
                    clocked_rows(&request, flatten_exemplars),
                    p.failed,
                    p.records,
                    0,
                    0,
                )
            }
            Req::Metrics(_) => {
                let p = prefix::<Metric>(&shallow);
                let points = |record: &Result<Metric, Vec<u8>>| match record {
                    Ok(metric) => metric_points(metric) as u64,
                    Err(encoded) => wire_points(encoded),
                };
                let failed_at = p.failed.unwrap_or(p.records) as usize;
                let staged_points = p.all[..failed_at].iter().map(points).sum();
                let total_points = p.all.iter().map(points).sum();
                let resources: Vec<ResourceMetrics> = p
                    .resources
                    .into_iter()
                    .map(|(resource, schema_url, scopes)| ResourceMetrics {
                        resource,
                        scope_metrics: scopes
                            .into_iter()
                            .map(|(scope, schema_url, metrics)| ScopeMetrics {
                                scope,
                                metrics,
                                schema_url,
                            })
                            .collect(),
                        schema_url,
                    })
                    .collect();
                let clock = fill_unset_timestamps(&mut Req::Metrics(MetricsData {
                    resource_metrics: resources.clone(),
                }));
                let rows = if p.failed.is_some() {
                    // Streaming stops inside the last resource, before its `target_info` row
                    let (last, complete) = resources.split_last().unwrap();
                    let complete = rows_of(
                        &Req::Metrics(MetricsData {
                            resource_metrics: complete.to_vec(),
                        }),
                        flatten_exemplars,
                    );
                    let last = rows_of(
                        &Req::Metrics(MetricsData {
                            resource_metrics: vec![last.clone()],
                        }),
                        flatten_exemplars,
                    );
                    complete.and_then(|mut rows| {
                        let mut last = last?;
                        if last.last().is_some_and(is_target_info) {
                            last.pop();
                        }
                        rows.extend(last);
                        Ok(rows)
                    })
                } else {
                    rows_of(
                        &Req::Metrics(MetricsData {
                            resource_metrics: resources,
                        }),
                        flatten_exemplars,
                    )
                };
                (
                    rows.map(|rows| (rows, clock)),
                    p.failed,
                    p.records,
                    staged_points,
                    total_points,
                )
            }
        };
        Expected::Records {
            rows,
            failed,
            records,
            staged_points,
            total_points,
        }
    }

    /// Which of the semantics a body met
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Case {
        /// Decodes whole: the same rows, every record staged
        Decoded,
        /// Broken above its records: refused with prost's error, nothing staged
        Outer,
        /// The first record does not decode: prost's error, nothing staged
        FirstRecord,
        /// A later record does not decode: prost's error, the rows of the records
        /// before it staged, the rest counted as rejected
        Partial,
        /// Flattening refused a record first
        Flatten,
    }

    /// The differential property of streamed decoding, for one body: either
    /// whole-request decoding and streaming both succeed with identical rows; or
    /// streaming stages exactly the rows of the records before the first record
    /// prost can not decode and counts the rest rejected; or the framing above the
    /// records is broken and nothing is staged. Every failure has prost's text.
    /// Also checks the bound on the decoded size of every record of the body.
    fn check_body(
        request: &Req,
        body: &[u8],
        flatten_exemplars: bool,
    ) -> Result<Case, TestCaseError> {
        let whole = whole(request, body, flatten_exemplars);
        let streamed = streamed(request, body, flatten_exemplars);
        let leaf = leaf(request);
        if let Ok(shallow) = ShallowRequest::decode(body) {
            for encoded in shallow
                .resources
                .iter()
                .flat_map(|r| &r.scopes)
                .flat_map(|s| &s.records)
            {
                prop_assert!(
                    leaf_decoded_size(encoded, leaf)
                        <= leaf_struct_size(leaf) + bytes_per_wire_byte(leaf) * encoded.len(),
                    "a {leaf:?} of {} bytes is over the bound",
                    encoded.len()
                );
            }
        }
        let units = |staged: &Staged| match request {
            Req::Metrics(_) => staged.points,
            Req::Logs(_) | Req::Traces(_) => staged.records,
        };
        match (&whole, expected(request, body, flatten_exemplars)) {
            (Ok((rows, clock)), _) => {
                prop_assert_eq!(&streamed.error, &None);
                prop_assert!(
                    same_rows(&streamed.rows, rows, *clock),
                    "whole and streamed rows differ ({} and {})",
                    rows.len(),
                    streamed.rows.len()
                );
                prop_assert_eq!(streamed.total, units(&streamed.staged));
                Ok(Case::Decoded)
            }
            (Err(text), Expected::Outer) => {
                prop_assert_eq!(streamed.error.as_ref(), Some(text));
                prop_assert!(streamed.rows.is_empty());
                prop_assert_eq!(streamed.staged, Staged::default());
                Ok(Case::Outer)
            }
            (
                Err(text),
                Expected::Records {
                    rows,
                    failed,
                    records,
                    staged_points,
                    total_points,
                },
            ) => {
                let (rows, clock) = match rows {
                    Ok(rows) => rows,
                    Err(flatten) => {
                        prop_assert_eq!(streamed.error, Some(flatten));
                        return Ok(Case::Flatten);
                    }
                };
                prop_assert_eq!(streamed.error.as_ref(), Some(text));
                let Some(failed) = failed else {
                    return Err(TestCaseError::fail(format!(
                        "whole decoding failed ({text}) but every record decodes on its own"
                    )));
                };
                prop_assert!(
                    same_rows(&streamed.rows, &rows, clock),
                    "streamed did not stage the rows of the records before the broken one ({} of {})",
                    streamed.rows.len(),
                    rows.len()
                );
                prop_assert_eq!(streamed.staged.records, failed);
                let rejected = streamed.total - units(&streamed.staged);
                match request {
                    Req::Metrics(_) => {
                        prop_assert_eq!(streamed.staged.points, staged_points);
                        prop_assert_eq!(rejected, total_points - staged_points);
                    }
                    Req::Logs(_) | Req::Traces(_) => prop_assert_eq!(rejected, records - failed),
                }
                Ok(if failed == 0 {
                    Case::FirstRecord
                } else {
                    Case::Partial
                })
            }
        }
    }

    fn requests() -> BoxedStrategy<Req> {
        prop_oneof![
            logs_data(true).prop_map(Req::Logs),
            traces_data(true).prop_map(Req::Traces),
            metrics_data(true).prop_map(Req::Metrics),
        ]
        .boxed()
    }

    /// Counts the cases a property met, by kind
    #[derive(Default)]
    struct Cases([AtomicUsize; 5]);

    impl Cases {
        fn add(&self, case: Case) {
            self.0[case as usize].fetch_add(1, Ordering::Relaxed);
        }

        fn get(&self, case: Case) -> usize {
            self.0[case as usize].load(Ordering::Relaxed)
        }

        fn print(&self, name: &str) {
            println!(
                "{name}: decoded {} outer {} first_record {} partial {} flatten {}",
                self.get(Case::Decoded),
                self.get(Case::Outer),
                self.get(Case::FirstRecord),
                self.get(Case::Partial),
                self.get(Case::Flatten)
            );
        }
    }

    #[test]
    fn otel_pb_streamed_rows_match_whole_decoding() {
        check(
            "otel_pb_streamed_rows_match_whole_decoding",
            192,
            (requests(), any::<u64>(), any::<bool>()),
            |(mut request, seed, flatten_exemplars)| {
                fill_unset_timestamps(&mut request);
                let body = edited_body(&request, &mut SplitMix64(seed), None);
                let case = check_body(&request, &body, flatten_exemplars)?;
                prop_assert_eq!(case, Case::Decoded);
                Ok(())
            },
        );
    }

    #[test]
    fn otel_pb_streamed_cut_bodies_match_whole_decoding() {
        let cases = Cases::default();
        check(
            "otel_pb_streamed_cut_bodies_match_whole_decoding",
            192,
            (requests(), any::<u64>(), any::<u64>()),
            |(mut request, seed, cut)| {
                fill_unset_timestamps(&mut request);
                let mut body = edited_body(&request, &mut SplitMix64(seed), None);
                body.truncate((cut % (body.len() as u64 + 1)) as usize);
                cases.add(check_body(&request, &body, false)?);
                Ok(())
            },
        );
        cases.print("otel_pb_streamed_cut_bodies");
    }

    /// Single-byte damage anywhere in the body
    #[test]
    fn otel_pb_streamed_damaged_bodies_match_whole_decoding() {
        let cases = Cases::default();
        check(
            "otel_pb_streamed_damaged_bodies_match_whole_decoding",
            192,
            (requests(), any::<u64>(), any::<u64>(), any::<u8>()),
            |(mut request, seed, at, byte)| {
                fill_unset_timestamps(&mut request);
                let mut body = edited_body(&request, &mut SplitMix64(seed), None);
                if !body.is_empty() {
                    let at = (at % body.len() as u64) as usize;
                    body[at] = byte;
                }
                cases.add(check_body(&request, &body, false)?);
                Ok(())
            },
        );
        cases.print("otel_pb_streamed_damaged_bodies");
    }

    /// Damage inside one record, whose framing is rewritten to fit: the framing
    /// above the records holds, so the rows of every record before a record that
    /// no longer decodes are staged
    #[test]
    fn otel_pb_streamed_damaged_records_keep_the_rows_before_them() {
        let cases = Cases::default();
        check(
            "otel_pb_streamed_damaged_records_keep_the_rows_before_them",
            192,
            (
                requests(),
                any::<u64>(),
                any::<(u64, u64, u8, bool)>(),
                any::<bool>(),
            ),
            |(mut request, seed, (index, at, byte, cut), flatten_exemplars)| {
                fill_unset_timestamps(&mut request);
                let records = request.record_count();
                prop_assume!(records > 0);
                let damage = RecordDamage {
                    index: index % records,
                    at,
                    byte,
                    cut,
                };
                let body = edited_body(&request, &mut SplitMix64(seed), Some(damage));
                let case = check_body(&request, &body, flatten_exemplars)?;
                prop_assert!(case != Case::Outer, "record damage broke the levels above");
                cases.add(case);
                Ok(())
            },
        );
        cases.print("otel_pb_streamed_damaged_records");
        assert!(cases.get(Case::Partial) > 0, "no partial case was met");
    }

    /// Arbitrary bytes as a record of every kind stay under the bound on decoded
    /// bytes per wire byte
    #[test]
    fn otel_pb_decoded_size_bound_holds_for_arbitrary_records() {
        check(
            "otel_pb_decoded_size_bound_holds_for_arbitrary_records",
            512,
            (vec(any::<u8>(), 0..512), 0..3usize),
            |(bytes, leaf)| {
                let leaf = [Leaf::LogRecord, Leaf::Span, Leaf::Metric][leaf];
                prop_assert!(
                    leaf_decoded_size(&bytes, leaf)
                        <= leaf_struct_size(leaf) + bytes_per_wire_byte(leaf) * bytes.len()
                );
                Ok(())
            },
        );
    }

    /// The cap on the decoded size of one record refuses the record before it is
    /// decoded, and only a record over it
    #[test]
    fn oversized_records_are_refused_before_decoding() {
        let span = Span {
            events: vec![Event::default(); 1 << 16],
            ..Default::default()
        };
        let request = Req::Traces(TracesData {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });
        let body = web::Bytes::from(request.encode_pb());
        let estimate = leaf_decoded_size(
            &request_span_bytes(&request),
            crate::otel::decoded_size::Leaf::Span,
        );
        let read = |max_record: usize| {
            let mut rows = 0usize;
            let mut chunker = Chunker::new(CHUNK_LIMITS, |chunk: Vec<Value>, _| {
                rows += chunk.len();
                Ok::<_, JsonFlattenError>(())
            });
            let result = flatten_request(
                &body,
                Signal::Traces,
                max_record,
                &mut FlattenBudget::new(FlattenLimits::DEFAULT),
                false,
                &mut chunker,
            )
            .and_then(|()| chunker.finish().map_err(StreamError::Consumer));
            drop(chunker);
            (result, rows)
        };
        let (refused, allocated) = crate::test_alloc::measure(|| read(estimate - 1));
        assert!(matches!(refused.0, Err(StreamError::RecordTooLarge { .. })));
        assert_eq!(refused.1, 0);
        assert!(allocated.peak < 64 * 1024, "peak {} B", allocated.peak);
        let (read, rows) = read(estimate);
        assert!(read.is_ok());
        assert_eq!(rows, 1 << 16);
    }

    fn request_span_bytes(request: &Req) -> Vec<u8> {
        match request {
            Req::Traces(data) => data.resource_spans[0].scope_spans[0].spans[0].encode_to_vec(),
            _ => unreachable!(),
        }
    }

    // -----------------------------------------------------------------------
    // Partial success after chunks were staged, without the server
    // -----------------------------------------------------------------------

    const T0: u64 = 1_700_000_000_000_000_000;

    fn string_value(value: String) -> Option<AnyValue> {
        Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        })
    }

    /// One resource of `attributes` attributes over `records` log records
    fn logs_request(records: usize, attributes: usize) -> Req {
        Req::Logs(LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: (0..attributes)
                        .map(|i| KeyValue {
                            key: format!("resource.attribute.{i:02}"),
                            value: string_value(format!("value-of-attribute-{i:02}")),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: (0..records)
                        .map(|i| LogRecord {
                            time_unix_nano: T0 + i as u64,
                            observed_time_unix_nano: T0 + i as u64,
                            severity_number: 9,
                            severity_text: format!("S{i:07}"),
                            body: string_value(format!("record {i}")),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        })
    }

    /// `metrics` sum metrics of `points` points each, under a resource without a
    /// target identity (so no `target_info` row)
    fn metrics_request(metrics: usize, points: usize) -> Req {
        Req::Metrics(MetricsData {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: (0..metrics)
                        .map(|m| Metric {
                            name: format!("metric{m:05}"),
                            data: Some(metric::Data::Sum(Sum {
                                data_points: (0..points)
                                    .map(|p| NumberDataPoint {
                                        time_unix_nano: T0 + p as u64,
                                        start_time_unix_nano: T0,
                                        value: Some(number_data_point::Value::AsInt(p as i64)),
                                        ..Default::default()
                                    })
                                    .collect(),
                                aggregation_temporality: 2,
                                is_monotonic: true,
                            })),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        })
    }

    /// Replaces the first byte of the only occurrence of `marker` in `body` with
    /// 0xff, so the string it is in is not UTF-8
    fn damage_marker(body: &mut [u8], marker: &[u8]) {
        let at: Vec<usize> = body
            .windows(marker.len())
            .enumerate()
            .filter(|(_, window)| *window == marker)
            .map(|(at, _)| at)
            .collect();
        assert_eq!(at.len(), 1);
        body[at[0]] = 0xff;
    }

    /// Where staging hands its chunks in these tests
    #[derive(Default)]
    struct Staged2 {
        rows: Vec<Value>,
        bytes: usize,
        chunks: usize,
    }

    fn collect(staged: &mut Staged2) -> impl FnMut(Vec<Value>) -> Result<(), PostError> + '_ {
        |rows| {
            staged.bytes += rows
                .iter()
                .map(|row| crate::otel::sink::row_bytes(row.as_object().unwrap()))
                .sum::<usize>();
            staged.chunks += 1;
            staged.rows.extend(rows);
            Ok(())
        }
    }

    fn target(source: &LogSource) -> OtelTarget<'_> {
        static NO_FIELDS: std::sync::LazyLock<HashMap<String, String>> =
            std::sync::LazyLock::new(HashMap::new);
        OtelTarget {
            stream_name: "test_stream",
            log_source: source,
            p_custom_fields: &NO_FIELDS,
            telemetry_type: match source {
                LogSource::OtelLogs => TelemetryType::Logs,
                LogSource::OtelMetrics => TelemetryType::Metrics,
                _ => TelemetryType::Traces,
            },
            tenant_id: &None,
        }
    }

    fn limits(rows: usize, max_expanded: u64) -> StageLimits {
        StageLimits {
            chunk: ChunkLimits {
                rows,
                bytes: usize::MAX,
            },
            max_expanded,
            flatten: FlattenLimits::DEFAULT,
        }
    }

    /// Stages `request` in both encodings with `stage`, the JSON one decoded as
    /// ingestion decodes it
    fn stage_both(
        request: &Req,
        limits: StageLimits,
        mut stage: impl FnMut(bool, Vec<Value>) -> Result<(), PostError>,
    ) -> [(Result<OtelIngested, PostError>, Staged); 2] {
        let source = request.source();
        let target = target(&source);
        let protobuf = stage_otel_protobuf(
            web::Bytes::from(request.encode_pb()),
            &target,
            PROTOBUF_OPTIONS,
            limits,
            |rows| stage(true, rows),
        );
        let decoded = decode_as(request, false);
        let json = stage_otel_resources(decoded.resources(), false, limits, |rows| {
            stage(false, rows)
        });
        [protobuf, json]
    }

    /// The expansion cap is a running total: a request passing it after chunks
    /// were staged is a partial success, the same in both encodings; one passing
    /// it in its first chunk is refused, as before anything is staged
    #[test]
    fn otel_over_the_expansion_cap_stops_alike_in_both_encodings() {
        let request = logs_request(2000, 40);
        let mut all = Staged2::default();
        let (whole, _) = stage_otel_protobuf(
            web::Bytes::from(request.encode_pb()),
            &target(&request.source()),
            PROTOBUF_OPTIONS,
            limits(100, u64::MAX),
            collect(&mut all),
        );
        assert_eq!(whole.unwrap(), OtelIngested::Full);
        assert_eq!(all.rows.len(), 2000);

        // About two fifths of the rows fit under the cap
        let cap = all.bytes as u64 * 2 / 5;
        let mut staged = [Staged2::default(), Staged2::default()];
        let [protobuf, json] = stage_both(&request, limits(100, cap), |protobuf, rows| {
            collect(&mut staged[usize::from(protobuf)])(rows)
        });
        for (outcome, counted) in [&protobuf, &json] {
            assert!(counted.chunks > 0 && counted.records < 2000, "{counted:?}");
            let message = format!(
                "Invalid Request: The rows of the request take more than the {cap} bytes allowed (4096 times the length of its body, at least 16777216 and at most 1024 times the payload limit). Split the batch into smaller requests, or send fewer resource, scope and span attributes per record"
            );
            assert_eq!(
                outcome.as_ref().unwrap(),
                &OtelIngested::Partial {
                    rejected: 2000 - counted.records as i64,
                    message
                }
            );
            assert!(counted.rows as f64 * 1.0 <= 2000.0 * 0.4 + 100.0);
        }
        assert_eq!(protobuf.1, json.1);
        assert_eq!(
            serde_json::to_vec(&staged[0].rows).unwrap(),
            serde_json::to_vec(&staged[1].rows).unwrap()
        );
        println!(
            "over the cap: {} of 2000 records staged in {} chunks, {} rejected",
            protobuf.1.records,
            protobuf.1.chunks,
            2000 - protobuf.1.records
        );

        // Passed in the first chunk: refused as before staging
        let [protobuf, json] = stage_both(&request, limits(100, 1000), |_, _| Ok(()));
        for (outcome, counted) in [protobuf, json] {
            assert_eq!(counted.chunks, 0);
            let error = outcome.unwrap_err();
            assert!(matches!(error, PostError::Invalid(_)), "{error:?}");
            assert!(
                error
                    .to_string()
                    .contains("more than the 1000 bytes allowed")
            );
        }
    }

    /// A record that does not decode, met after chunks were staged, is a partial
    /// success with prost's error; as the first record, a refusal with it
    #[test]
    fn otel_pb_record_damaged_late_is_a_partial_success() {
        for (damaged, staged_records) in [(999, 900), (0, 0)] {
            let mut request = logs_request(1000, 5);
            if let Req::Logs(data) = &mut request {
                data.resource_logs[0].scope_logs[0].log_records[damaged].severity_text =
                    "DAMAGEME".into();
            }
            let mut body = request.encode_pb();
            damage_marker(&mut body, b"DAMAGEME");
            let prost_error = OtelProtobufRequest::decode(&body, &request.source())
                .unwrap_err()
                .to_string();
            let mut staged_rows = Staged2::default();
            let (outcome, staged) = stage_otel_protobuf(
                web::Bytes::from(body),
                &target(&request.source()),
                PROTOBUF_OPTIONS,
                limits(100, u64::MAX),
                collect(&mut staged_rows),
            );
            let message =
                format!("Invalid Request: Ingestion failed for stream test_stream: {prost_error}");
            assert_eq!(staged.records, staged_records);
            if staged_records > 0 {
                assert_eq!(staged.chunks, 9);
                assert_eq!(
                    outcome.unwrap(),
                    OtelIngested::Partial {
                        rejected: 1000 - staged_records as i64,
                        message
                    }
                );
            } else {
                assert_eq!(outcome.unwrap_err().to_string(), message);
            }
            println!("damaged record {damaged}: {prost_error}");
        }
    }

    /// A record over the decoded size cap, met after chunks were staged, is a
    /// partial success; it is measured because it is long enough to reach the cap
    #[test]
    fn otel_pb_record_over_the_cap_late_is_a_partial_success() {
        let mut request = logs_request(1000, 5);
        if let Req::Logs(data) = &mut request {
            data.resource_logs[0].scope_logs[0].log_records[999].body =
                string_value("x".repeat(100_000));
        }
        let options = ProtobufOptions {
            max_payload: 4096,
            flatten_exemplars: false,
        };
        let max_record = crate::otel::protobuf_stream::max_record_bytes(options.max_payload);
        assert!(crate::otel::decoded_size::needs_measuring(
            100_010,
            Leaf::LogRecord,
            max_record
        ));
        assert!(!crate::otel::decoded_size::needs_measuring(
            100,
            Leaf::LogRecord,
            max_record
        ));
        let (outcome, staged) = stage_otel_protobuf(
            web::Bytes::from(request.encode_pb()),
            &target(&request.source()),
            options,
            limits(100, u64::MAX),
            |_| Ok(()),
        );
        assert_eq!(staged.records, 900);
        match outcome.unwrap() {
            OtelIngested::Partial { rejected, message } => {
                assert_eq!(rejected, 100);
                assert!(
                    message
                        .contains("one LogRecord of the request would take more than 65536 bytes"),
                    "{message}"
                );
            }
            other => panic!("{other:?}"),
        }
    }

    /// A metrics request stopped after some chunks rejects exactly the data points
    /// not staged: when staging refuses a chunk (both encodings), when a later
    /// metric does not decode (its points read from the wire), and a server error
    /// keeps its status
    #[test]
    fn otel_metrics_stopped_after_some_chunks_reject_the_points_not_staged() {
        let request = metrics_request(10, 37);
        let refusal = || PostError::Invalid(anyhow::anyhow!("schema conflict"));
        let mut calls = [0, 0];
        let [protobuf, json] = stage_both(&request, limits(50, u64::MAX), |protobuf, _| {
            let calls = &mut calls[usize::from(protobuf)];
            *calls += 1;
            if *calls == 4 { Err(refusal()) } else { Ok(()) }
        });
        for (outcome, staged) in [protobuf, json] {
            assert_eq!(staged.points, 150);
            assert_eq!(staged.records, 4);
            assert_eq!(
                outcome.unwrap(),
                OtelIngested::Partial {
                    rejected: 370 - 150,
                    message: "Invalid Request: schema conflict".into()
                }
            );
        }

        // Metric 8 does not decode: metrics 0-7 (296 points) were flattened, 250 of
        // them in full chunks; its 37 points are read from the wire
        let mut damaged = request.clone();
        if let Req::Metrics(data) = &mut damaged {
            data.resource_metrics[0].scope_metrics[0].metrics[8].name = "DAMAGEME".into();
        }
        let mut body = damaged.encode_pb();
        damage_marker(&mut body, b"DAMAGEME");
        let body = web::Bytes::from(body);
        assert_eq!(count_points(&body), 370);
        let (outcome, staged) = stage_otel_protobuf(
            body,
            &target(&request.source()),
            PROTOBUF_OPTIONS,
            limits(50, u64::MAX),
            |_| Ok(()),
        );
        assert_eq!((staged.chunks, staged.points), (5, 250));
        match outcome.unwrap() {
            OtelIngested::Partial { rejected, message } => {
                assert_eq!(rejected, 370 - 250);
                assert!(message.contains("invalid string value"), "{message}");
            }
            other => panic!("{other:?}"),
        }

        // A server error after chunks were staged keeps its status
        let mut calls = 0;
        let [protobuf, _] = stage_both(&request, limits(50, u64::MAX), |protobuf, _| {
            calls += usize::from(protobuf);
            if protobuf && calls == 2 {
                Err(PostError::JsonFlattenError(JsonFlattenError::CannotFlatten))
            } else {
                Ok(())
            }
        });
        let error = protobuf.0.unwrap_err();
        assert!(
            actix_web::ResponseError::status_code(&error).is_server_error(),
            "{error:?}"
        );
        assert_eq!(protobuf.1.chunks, 1);
    }
}
