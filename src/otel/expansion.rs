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

//! What an OTLP request expands to, computed from the decoded message without
//! building a row.
//!
//! Every row carries its own copy of the resource, scope, span and metric fields,
//! so a small request can expand to a very large number of row bytes: 15 KB of
//! resource attributes copied into each of 5 million two-byte log records is tens
//! of GiB. Chunking bounds the memory this takes at once, not what is written to
//! staging, so the request is refused before anything is staged when its rows
//! would hold more than [`max_expanded_bytes`]. The sizes are in the units of
//! [`super::sink::row_bytes`], and follow the flatteners field by field; they are
//! estimates (JSON log bodies are counted as their text, not as the fields parsed
//! from them, which the flatten budget charges).

use std::collections::HashSet;

use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value as OtelValue,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, Metric, ResourceMetrics, exponential_histogram_data_point::Buckets, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, Span,
    span::{Event, Link},
};

use super::sink::{ENTRY_BYTES, VALUE_BYTES, heap_bytes};

/// Rows, estimated row bytes and leaf records of one request
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestSize {
    /// Rows the request expands to
    pub rows: u64,
    /// Estimated memory of all of its rows, see `row_bytes`
    pub bytes: u64,
    /// Log records, spans or metrics
    pub records: u64,
    /// Metric data points
    pub points: u64,
}

impl RequestSize {
    /// Adds `rows` rows of `row` bytes each
    fn add_rows(&mut self, rows: u64, row: usize) {
        self.rows += rows;
        self.bytes = self.bytes.saturating_add(rows.saturating_mul(row as u64));
    }

    /// Adds bytes that only one row holds
    fn add_bytes(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes as u64);
    }
}

/// Estimated bytes of rows one request may expand to, for a payload limit of
/// `max_payload` bytes: `EXPANSION_FACTOR` row bytes per payload byte
pub fn max_expanded_bytes(max_payload: usize) -> u64 {
    (max_payload as u64).saturating_mul(EXPANSION_FACTOR)
}

/// Row bytes per payload byte a request may expand to. The most measured for a
/// request shaped like collector traffic is 459: short log records (a time, a
/// severity and a two-byte body) or bare gauge points under 30 Kubernetes resource
/// attributes; k8s logs, spans and metrics with ordinary records expand 40-110x
/// (`realistic_tests`). 1024 leaves 2.2x headroom over the worst of them, and
/// refuses the requests that copy kilobytes of shared fields into millions of
/// records of a few bytes (47,000 row bytes per body byte for 500 attributes over
/// empty records, `copied_fields_over_tiny_records_are_over_the_expansion_cap`), at 10 GiB of
/// rows for the default 10 MiB payload limit.
pub const EXPANSION_FACTOR: u64 = 1024;

/// Timestamps are formatted as `%Y-%m-%dT%H:%M:%S%.9fZ`
const TIMESTAMP_LEN: usize = 30;
/// Longest decimal `i64`
const INT_LEN: usize = 20;

#[inline]
fn field(key_len: usize, value_heap: usize) -> usize {
    ENTRY_BYTES + heap_bytes(key_len) + value_heap
}

#[inline]
fn str_field(key: &str, value_len: usize) -> usize {
    field(key.len(), heap_bytes(value_len))
}

#[inline]
fn num_field(key: &str) -> usize {
    field(key.len(), 0)
}

#[inline]
fn timestamp_field(key: &str) -> usize {
    str_field(key, TIMESTAMP_LEN)
}

#[inline]
fn hex_field(key: &str, bytes: usize) -> usize {
    str_field(key, 2 * bytes)
}

/// Fields `insert_attributes` adds for `attributes`, with `prefix` before each key
fn attributes_bytes(attributes: &[KeyValue], prefix: usize) -> usize {
    attributes
        .iter()
        .filter_map(|attribute| {
            let value = attribute.value.as_ref()?.value.as_ref()?;
            Some(value_fields(attribute.key.len() + prefix, value, prefix))
        })
        .sum()
}

/// Fields one OTLP value adds under a key of `key_len` bytes
fn value_fields(key_len: usize, value: &OtelValue, prefix: usize) -> usize {
    match value {
        OtelValue::StringValue(s) => field(key_len, heap_bytes(s.len())),
        OtelValue::BoolValue(_) | OtelValue::DoubleValue(_) => field(key_len, 0),
        OtelValue::IntValue(_) => field(key_len, heap_bytes(INT_LEN)),
        OtelValue::BytesValue(b) => field(key_len, heap_bytes(b.len())),
        // Serialised to a JSON string
        OtelValue::ArrayValue(array) => field(key_len, heap_bytes(array_json_len(&array.values))),
        // Every entry becomes a field of its own, under its own key
        OtelValue::KvlistValue(list) => list
            .values
            .iter()
            .filter_map(|entry| {
                let value = entry.value.as_ref()?.value.as_ref()?;
                Some(value_fields(entry.key.len() + prefix, value, prefix))
            })
            .sum(),
        OtelValue::StringValueStrindex(_) => 0,
    }
}

/// Approximate length of the JSON text of an array value
fn array_json_len(values: &[AnyValue]) -> usize {
    2 + values
        .iter()
        .map(|value| {
            1 + match &value.value {
                Some(OtelValue::StringValue(s)) => s.len() + 2,
                Some(OtelValue::BytesValue(b)) => b.len() + 2,
                Some(OtelValue::ArrayValue(array)) => array_json_len(&array.values),
                Some(OtelValue::KvlistValue(list)) => list
                    .values
                    .iter()
                    .map(|entry| {
                        entry.key.len()
                            + 4
                            + entry
                                .value
                                .as_ref()
                                .map_or(4, |value| array_json_len(std::slice::from_ref(value)))
                    })
                    .sum::<usize>(),
                _ => INT_LEN,
            }
        })
        .sum::<usize>()
}

fn scope_bytes(scope: Option<&InstrumentationScope>) -> usize {
    scope.map_or(0, |scope| {
        str_field("scope_name", scope.name.len())
            + str_field("scope_version", scope.version.len())
            + attributes_bytes(&scope.attributes, 0)
            + num_field("scope_dropped_attributes_count")
    })
}

fn resource_bytes(resource: Option<&Resource>) -> usize {
    resource.map_or(0, |resource| {
        attributes_bytes(&resource.attributes, 0) + num_field("resource_dropped_attributes_count")
    })
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

fn log_record_bytes(record: &LogRecord) -> usize {
    timestamp_field("time_unix_nano")
        + timestamp_field("observed_time_unix_nano")
        + num_field("severity_number")
        + str_field("severity_text", record.severity_text.len().max(11))
        + record
            .body
            .as_ref()
            .and_then(|body| body.value.as_ref())
            .map_or(0, |body| value_fields("body".len(), body, 0))
        + attributes_bytes(&record.attributes, 0)
        + str_field("p_log_category", 11)
        + num_field("log_record_dropped_attributes_count")
        + num_field("flags")
        + hex_field("span_id", record.span_id.len())
        + hex_field("trace_id", record.trace_id.len())
}

/// Sizes a logs request one part at a time, in the order of the request
#[derive(Debug, Default)]
pub struct LogsSizer {
    size: RequestSize,
    resource: usize,
    shared: usize,
}

impl LogsSizer {
    pub fn resource(&mut self, resource: Option<&Resource>, schema_url: &str) {
        self.resource = resource_bytes(resource) + str_field("schema_url", schema_url.len());
    }

    pub fn scope(&mut self, scope: Option<&InstrumentationScope>, schema_url: &str) {
        self.shared = VALUE_BYTES
            + self.resource
            + scope_bytes(scope)
            + str_field("scope_log_schema_url", schema_url.len());
    }

    pub fn record(&mut self, record: &LogRecord) {
        self.size.records += 1;
        self.size.add_rows(1, self.shared);
        self.size.add_bytes(log_record_bytes(record));
    }

    pub fn finish(self) -> RequestSize {
        self.size
    }
}

/// Rows and row bytes of a logs request
pub fn logs_size(resource_logs: &[ResourceLogs]) -> RequestSize {
    let mut sizer = LogsSizer::default();
    for resource_log in resource_logs {
        sizer.resource(resource_log.resource.as_ref(), &resource_log.schema_url);
        for scope_log in &resource_log.scope_logs {
            sizer.scope(scope_log.scope.as_ref(), &scope_log.schema_url);
            for record in &scope_log.log_records {
                sizer.record(record);
            }
        }
    }
    sizer.finish()
}

// ---------------------------------------------------------------------------
// Traces
// ---------------------------------------------------------------------------

fn span_bytes(span: &Span) -> usize {
    hex_field("span_trace_id", span.trace_id.len())
        + hex_field("span_span_id", span.span_id.len())
        + str_field("span_trace_state", span.trace_state.len())
        + hex_field("span_parent_span_id", span.parent_span_id.len())
        + num_field("span_flags")
        + str_field("span_flags_description", 26)
        + str_field("span_name", span.name.len())
        + num_field("span_kind")
        + str_field("span_kind_description", 11)
        + timestamp_field("span_start_time_unix_nano")
        + num_field("span_start_time_unix_nano_epoch")
        + timestamp_field("span_end_time_unix_nano")
        + num_field("span_end_time_unix_nano_epoch")
        + num_field("span_duration_ns")
        + attributes_bytes(&span.attributes, 0)
        + num_field("span_dropped_attributes_count")
        + num_field("span_dropped_events_count")
        + num_field("span_dropped_links_count")
        + span.status.as_ref().map_or(0, |status| {
            str_field("span_status_message", status.message.len())
                + num_field("span_status_code")
                + str_field("span_status_description", 5)
        })
}

fn event_bytes(event: &Event) -> usize {
    timestamp_field("event_time_unix_nano")
        + num_field("event_time_unix_nano_epoch")
        + str_field("event_name", event.name.len())
        + num_field("event_duration_ns")
        + attributes_bytes(&event.attributes, "event_".len())
        + num_field("event_dropped_attributes_count")
}

fn link_bytes(link: &Link) -> usize {
    hex_field("link_span_id", link.span_id.len())
        + hex_field("link_trace_id", link.trace_id.len())
        + attributes_bytes(&link.attributes, "link_".len())
        + num_field("link_dropped_attributes_count")
}

/// Sizes a traces request one part at a time, in the order of the request
#[derive(Debug, Default)]
pub struct TracesSizer {
    size: RequestSize,
    resource: usize,
    shared: usize,
}

impl TracesSizer {
    pub fn resource(&mut self, resource: Option<&Resource>, schema_url: &str) {
        self.resource =
            resource_bytes(resource) + str_field("resource_schema_url", schema_url.len());
    }

    pub fn scope(&mut self, scope: Option<&InstrumentationScope>, schema_url: &str) {
        self.shared = VALUE_BYTES
            + self.resource
            + scope_bytes(scope)
            + str_field("scope_schema_url", schema_url.len());
    }

    pub fn span(&mut self, span: &Span) {
        self.size.records += 1;
        let rows = (span.events.len() + span.links.len()).max(1) as u64;
        self.size.add_rows(rows, self.shared + span_bytes(span));
        for event in &span.events {
            self.size.add_bytes(event_bytes(event));
        }
        for link in &span.links {
            self.size.add_bytes(link_bytes(link));
        }
    }

    pub fn finish(self) -> RequestSize {
        self.size
    }
}

/// Rows and row bytes of a traces request
pub fn traces_size(resource_spans: &[ResourceSpans]) -> RequestSize {
    let mut sizer = TracesSizer::default();
    for resource_span in resource_spans {
        sizer.resource(resource_span.resource.as_ref(), &resource_span.schema_url);
        for scope_span in &resource_span.scope_spans {
            sizer.scope(scope_span.scope.as_ref(), &scope_span.schema_url);
            for span in &scope_span.spans {
                sizer.span(span);
            }
        }
    }
    sizer.finish()
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

fn exemplar_fields(exemplar: &Exemplar) -> usize {
    attributes_bytes(&exemplar.filtered_attributes, 0)
        + timestamp_field("exemplar_time_unix_nano")
        + hex_field("exemplar_span_id", exemplar.span_id.len())
        + hex_field("exemplar_trace_id", exemplar.trace_id.len())
        + exemplar
            .value
            .as_ref()
            .map_or(0, |_| num_field("exemplar_value"))
}

/// Bytes of the exemplars of one data point, including the column table that
/// structural flattening builds from them when they are kept nested
fn exemplars_bytes(exemplars: &[Exemplar], flatten_exemplars: bool) -> usize {
    if exemplars.is_empty() {
        return 0;
    }
    let fields: usize = exemplars.iter().map(exemplar_fields).sum();
    if flatten_exemplars {
        return fields;
    }
    let mut keys: HashSet<&str> = HashSet::from([
        "exemplar_time_unix_nano",
        "exemplar_span_id",
        "exemplar_trace_id",
        "exemplar_value",
    ]);
    for exemplar in exemplars {
        keys.extend(exemplar.filtered_attributes.iter().map(|a| a.key.as_str()));
    }
    let n = exemplars.len();
    field("exemplars".len(), heap_bytes(n * VALUE_BYTES))
        + fields
        + keys.len() * (ENTRY_BYTES + n * VALUE_BYTES)
}

fn array_field(key: &str, len: usize) -> usize {
    field(key.len(), heap_bytes(len * VALUE_BYTES))
}

fn buckets_bytes(prefix: &str, buckets: Option<&Buckets>) -> usize {
    buckets.map_or(0, |buckets| {
        field(prefix.len() + "_offset".len(), 0)
            + field(
                prefix.len() + "_bucket_count".len(),
                heap_bytes(buckets.bucket_counts.len() * VALUE_BYTES),
            )
    })
}

fn temporality_bytes() -> usize {
    num_field("aggregation_temporality") + str_field("aggregation_temporality_description", 11)
}

/// Own fields every data point row has, whatever its type
fn point_common(attributes: &[KeyValue]) -> usize {
    attributes_bytes(attributes, 0)
        + timestamp_field("start_time_unix_nano")
        + timestamp_field("time_unix_nano")
}

/// Rows and bytes of the data points of one metric, and the count of points
fn metric_points(metric: &Metric, flatten_exemplars: bool, row: usize, size: &mut RequestSize) {
    let flags = num_field("data_point_flags") + str_field("data_point_flags_description", 22);
    let mut points = |own: usize| {
        size.add_rows(1, row);
        size.add_bytes(own);
        size.points += 1;
    };
    let count = match &metric.data {
        None => return,
        Some(metric::Data::Gauge(gauge)) => {
            for dp in &gauge.data_points {
                points(
                    point_common(&dp.attributes)
                        + exemplars_bytes(&dp.exemplars, flatten_exemplars)
                        + flags
                        + num_field("data_point_value"),
                );
            }
            gauge.data_points.len()
        }
        Some(metric::Data::Sum(sum)) => {
            for dp in &sum.data_points {
                points(
                    point_common(&dp.attributes)
                        + exemplars_bytes(&dp.exemplars, flatten_exemplars)
                        + flags
                        + num_field("data_point_value")
                        + temporality_bytes()
                        + num_field("is_monotonic"),
                );
            }
            sum.data_points.len()
        }
        Some(metric::Data::Histogram(histogram)) => {
            for dp in &histogram.data_points {
                points(
                    point_common(&dp.attributes)
                        + num_field("data_point_count")
                        + num_field("data_point_sum")
                        + array_field("data_point_bucket_counts", dp.bucket_counts.len())
                        + array_field("data_point_explicit_bounds", dp.explicit_bounds.len())
                        + exemplars_bytes(&dp.exemplars, flatten_exemplars)
                        + flags
                        + num_field("data_point_min")
                        + num_field("data_point_max")
                        + temporality_bytes(),
                );
            }
            histogram.data_points.len()
        }
        Some(metric::Data::ExponentialHistogram(histogram)) => {
            for dp in &histogram.data_points {
                points(
                    point_common(&dp.attributes)
                        + num_field("data_point_count")
                        + num_field("data_point_sum")
                        + num_field("data_point_scale")
                        + num_field("data_point_zero_count")
                        + buckets_bytes("positive", dp.positive.as_ref())
                        + buckets_bytes("negative", dp.negative.as_ref())
                        + exemplars_bytes(&dp.exemplars, flatten_exemplars)
                        + temporality_bytes(),
                );
            }
            histogram.data_points.len()
        }
        Some(metric::Data::Summary(summary)) => {
            for dp in &summary.data_points {
                let n = dp.quantile_values.len();
                points(
                    point_common(&dp.attributes)
                        + num_field("data_point_count")
                        + num_field("data_point_sum")
                        + array_field("data_point_quantile_values", n)
                        // Two fields per quantile, then a column table of two keys
                        + n * 2 * field("quantile".len(), 0)
                        + if n > 0 {
                            2 * (ENTRY_BYTES + n * VALUE_BYTES)
                        } else {
                            0
                        },
                );
            }
            summary.data_points.len()
        }
    };
    if count == 0 {
        // The one row of a metric that has data but no data points
        size.add_rows(1, row);
        size.add_bytes(temporality_bytes() + num_field("is_monotonic"));
    }
}

/// Sizes a metrics request one part at a time, in the order of the request
#[derive(Debug, Default)]
pub struct MetricsSizer {
    size: RequestSize,
    flatten_exemplars: bool,
    identity: bool,
    resource: usize,
    envelope: usize,
    any_rows: bool,
}

impl MetricsSizer {
    pub fn new(flatten_exemplars: bool) -> Self {
        Self {
            flatten_exemplars,
            ..Self::default()
        }
    }

    pub fn resource(&mut self, resource: Option<&Resource>, schema_url: &str) {
        // Built once per resource, as the flattener does, since the identity is
        // read from the flattened resource fields
        self.identity =
            super::metrics::ResourceMetricFields::new(resource, schema_url).has_target_identity();
        self.resource = resource_bytes(resource)
            + str_field("resource_schema_url", schema_url.len())
            + if self.identity {
                str_field("job", 32) + str_field("instance", 32)
            } else {
                0
            };
        self.any_rows = false;
    }

    pub fn scope(&mut self, scope: Option<&InstrumentationScope>, schema_url: &str) {
        self.envelope =
            self.resource + scope_bytes(scope) + str_field("scope_schema_url", schema_url.len());
    }

    pub fn metric(&mut self, metric: &Metric) {
        self.size.records += 1;
        let row = VALUE_BYTES
            + self.envelope
            + str_field("metric_name", metric.name.len())
            + str_field("metric_description", metric.description.len())
            + str_field("metric_unit", metric.unit.len())
            + str_field("metric_type", 21)
            + attributes_bytes(&metric.metadata, 0)
            + num_field(super::metrics::SERIES_HASH_COLUMN);
        self.any_rows |= metric.data.is_some();
        metric_points(metric, self.flatten_exemplars, row, &mut self.size);
    }

    pub fn resource_end(&mut self) {
        if self.identity && self.any_rows {
            // target_info
            self.size.add_rows(
                1,
                VALUE_BYTES
                    + self.resource
                    + 6 * str_field("metric_description", 16)
                    + num_field(super::metrics::SERIES_HASH_COLUMN),
            );
        }
    }

    pub fn finish(self) -> RequestSize {
        self.size
    }
}

/// Rows and row bytes of a metrics request
pub fn metrics_size(resource_metrics: &[ResourceMetrics], flatten_exemplars: bool) -> RequestSize {
    let mut sizer = MetricsSizer::new(flatten_exemplars);
    for resource_metric in resource_metrics {
        sizer.resource(
            resource_metric.resource.as_ref(),
            &resource_metric.schema_url,
        );
        for scope_metric in &resource_metric.scope_metrics {
            sizer.scope(scope_metric.scope.as_ref(), &scope_metric.schema_url);
            for metric in &scope_metric.metrics {
                sizer.metric(metric);
            }
        }
        sizer.resource_end();
    }
    sizer.finish()
}
