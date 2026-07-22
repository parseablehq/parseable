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
use once_cell::sync::Lazy;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberDataPointValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogram, Gauge, Histogram, Metric, MetricsData, NumberDataPoint, Sum,
    Summary, exemplar::Value as ExemplarValue, exponential_histogram_data_point::Buckets, metric,
};
use serde_json::{Map, Value};

use rustc_hash::FxHasher;
use std::borrow::Cow;
use std::collections::HashSet;
use std::hash::Hasher;
use tracing::info_span;

use crate::metrics::increment_metrics_collected_by_date;

use super::otel_utils::{
    convert_epoch_nano_to_timestamp, insert_attributes, insert_number_if_some,
};

pub const OTEL_METRICS_KNOWN_FIELD_LIST: [&str; 36] = [
    "metric_name",
    "metric_description",
    "metric_unit",
    "start_time_unix_nano",
    "time_unix_nano",
    "exemplar_time_unix_nano",
    "exemplar_span_id",
    "exemplar_trace_id",
    "exemplar_value",
    "data_point_value",
    "data_point_count",
    "data_point_sum",
    "data_point_bucket_counts",
    "data_point_explicit_bounds",
    // Nested array of summary quantile objects, each carrying `quantile`
    // and `value`. Stored under this single key and kept out of the
    // series hash so per-sample quantile values never fragment series
    // identity. (The bare `quantile`/`value` keys only ever appear nested
    // inside this array, never as top-level data-point fields.)
    "data_point_quantile_values",
    // Histogram per-sample min/max statistics.
    "min",
    "max",
    "data_point_scale",
    "data_point_zero_count",
    "data_point_flags",
    "data_point_flags_description",
    "positive_offset",
    "positive_bucket_count",
    "negative_offset",
    "negative_bucket_count",
    "is_monotonic",
    "aggregation_temporality",
    "aggregation_temporality_description",
    "metric_type",
    "scope_name",
    "scope_version",
    "scope_schema_url",
    "scope_dropped_attributes_count",
    "resource_dropped_attributes_count",
    "resource_schema_url",
    // Precomputed per-sample identity of the physical series. Stable
    // u64 hash of `metric_name` + sorted attribute key/value pairs,
    // stored as a decimal-encoded string so arrow-json infers Utf8 and
    // we get byte-exact roundtrip. Int64/Float64 inference dropped bits
    // for hashes near the high range; string sidesteps that entirely.
    // Lets the query layer group samples into physical series via a
    // single column read instead of decoding every label column and
    // hashing per row.
    "__series_hash",
];

static OTEL_METRICS_KNOWN_FIELDS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| OTEL_METRICS_KNOWN_FIELD_LIST.iter().copied().collect());

/// Compute a stable u64 identifier for the physical series a sample
/// belongs to. Hashes `metric_name` plus every attribute key/value pair
/// that survived OTel flattening — everything in the flattened data
/// point that isn't a known sample-level field is treated as a label.
///
/// Hash output must be stable across process restarts and matching at
/// query time. Uses rustc-hash's FxHasher (fast, deterministic,
/// non-cryptographic) and feeds keys in sorted order so the hash
/// doesn't depend on JSON Map iteration order.
fn compute_series_hash(dp: &Map<String, Value>) -> u64 {
    let mut label_pairs: Vec<(&str, Cow<'_, str>)> = Vec::with_capacity(dp.len());
    for (key, value) in dp {
        if OTEL_METRICS_KNOWN_FIELDS.contains(key.as_str()) {
            continue;
        }
        let value = match value {
            Value::String(s) => Cow::Borrowed(s.as_str()),
            other => Cow::Owned(other.to_string()),
        };
        label_pairs.push((key.as_str(), value));
    }
    label_pairs.sort_by(|a, b| a.0.cmp(b.0));

    let mut hasher = FxHasher::default();
    // Include metric_name in the identity. Without it, two different
    // metrics with identical label sets would collide into one series.
    if let Some(Value::String(name)) = dp.get("metric_name") {
        hasher.write(name.as_bytes());
        hasher.write(b"\0");
    }
    for (k, v) in &label_pairs {
        hasher.write(k.as_bytes());
        hasher.write(b"=");
        hasher.write(v.as_bytes());
        hasher.write(b"\0");
    }
    hasher.finish()
}

fn insert_exemplars(map: &mut Map<String, Value>, exemplars: &[Exemplar]) {
    for exemplar in exemplars {
        insert_attributes(map, &exemplar.filtered_attributes);
        map.insert(
            "exemplar_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                exemplar.time_unix_nano as i64,
            )),
        );
        map.insert(
            "exemplar_span_id".to_string(),
            Value::String(hex::encode(&exemplar.span_id)),
        );
        map.insert(
            "exemplar_trace_id".to_string(),
            Value::String(hex::encode(&exemplar.trace_id)),
        );
        if let Some(value) = &exemplar.value {
            match value {
                ExemplarValue::AsDouble(double_val) => {
                    map.insert(
                        "exemplar_value".to_string(),
                        serde_json::Number::from_f64(*double_val)
                            .map(Value::Number)
                            .unwrap_or(Value::Null),
                    );
                }
                ExemplarValue::AsInt(int_val) => {
                    map.insert(
                        "exemplar_value".to_string(),
                        Value::Number(serde_json::Number::from(*int_val)),
                    );
                }
            }
        }
    }
}

/// otel metrics event has json array for number data points
/// this function flatten the number data points json array
/// and returns a `Vec` of `Map` of the flattened json
/// this function is reused in all json objects that have number data points
fn flatten_number_data_points(data_points: &[NumberDataPoint]) -> Vec<Map<String, Value>> {
    data_points
        .iter()
        .map(|data_point| {
            let mut data_point_json = Map::with_capacity(data_point.attributes.len() + 8);
            insert_attributes(&mut data_point_json, &data_point.attributes);
            data_point_json.insert(
                "start_time_unix_nano".to_string(),
                Value::String(convert_epoch_nano_to_timestamp(
                    data_point.start_time_unix_nano as i64,
                )),
            );
            data_point_json.insert(
                "time_unix_nano".to_string(),
                Value::String(convert_epoch_nano_to_timestamp(
                    data_point.time_unix_nano as i64,
                )),
            );

            insert_exemplars(&mut data_point_json, &data_point.exemplars);

            insert_data_point_flags(&mut data_point_json, data_point.flags);
            if let Some(value) = &data_point.value {
                match value {
                    NumberDataPointValue::AsDouble(double_val) => {
                        data_point_json.insert(
                            "data_point_value".to_string(),
                            serde_json::Number::from_f64(*double_val)
                                .map(Value::Number)
                                .unwrap_or(Value::Null),
                        );
                    }
                    NumberDataPointValue::AsInt(int_val) => {
                        data_point_json.insert(
                            "data_point_value".to_string(),
                            Value::Number(serde_json::Number::from(*int_val)),
                        );
                    }
                }
            }
            data_point_json
        })
        .collect()
}

/// otel metrics event has json object for gauge
/// each gauge object has json array for data points
/// this function flatten the gauge json object
/// and returns a `Vec` of `Map` for each data point
fn flatten_gauge(gauge: &Gauge) -> Vec<Map<String, Value>> {
    flatten_number_data_points(&gauge.data_points)
}

/// otel metrics event has json object for sum
/// each sum object has json array for data points
/// this function flatten the sum json object
/// and returns a `Vec` of `Map` for each data point
fn flatten_sum(sum: &Sum) -> Vec<Map<String, Value>> {
    let mut data_points = flatten_number_data_points(&sum.data_points);
    for dp in &mut data_points {
        insert_aggregation_temporality(dp, sum.aggregation_temporality);
        dp.insert("is_monotonic".to_string(), Value::Bool(sum.is_monotonic));
    }
    data_points
}

/// otel metrics event has json object for histogram
/// each histogram object has json array for data points
/// this function flatten the histogram json object
/// and returns a `Vec` of `Map` for each data point
fn flatten_histogram(histogram: &Histogram) -> Vec<Map<String, Value>> {
    let mut data_points_json = Vec::with_capacity(histogram.data_points.len());
    for data_point in &histogram.data_points {
        let mut data_point_json = Map::with_capacity(data_point.attributes.len() + 10);
        insert_attributes(&mut data_point_json, &data_point.attributes);
        data_point_json.insert(
            "start_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.start_time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "data_point_count".to_string(),
            Value::Number(data_point.count.into()),
        );
        insert_number_if_some(&mut data_point_json, "data_point_sum", &data_point.sum);
        let data_point_bucket_counts = Value::Array(
            data_point
                .bucket_counts
                .iter()
                .map(|&count| Value::Number(count.into()))
                .collect(),
        );
        data_point_json.insert(
            "data_point_bucket_counts".to_string(),
            data_point_bucket_counts,
        );
        let data_point_explicit_bounds = Value::Array(
            data_point
                .explicit_bounds
                .iter()
                .map(|bound| {
                    serde_json::Number::from_f64(*bound)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                })
                .collect(),
        );
        data_point_json.insert(
            "data_point_explicit_bounds".to_string(),
            data_point_explicit_bounds,
        );
        insert_exemplars(&mut data_point_json, &data_point.exemplars);

        insert_data_point_flags(&mut data_point_json, data_point.flags);
        insert_number_if_some(&mut data_point_json, "min", &data_point.min);
        insert_number_if_some(&mut data_point_json, "max", &data_point.max);
        insert_aggregation_temporality(&mut data_point_json, histogram.aggregation_temporality);
        data_points_json.push(data_point_json);
    }
    data_points_json
}

/// otel metrics event has json object for buckets
/// this function flatten the buckets json object
/// and returns a `Map` of the flattened json
fn flatten_buckets(bucket: &Buckets) -> Map<String, Value> {
    let mut bucket_json = Map::with_capacity(2);
    bucket_json.insert("offset".to_string(), Value::Number(bucket.offset.into()));
    bucket_json.insert(
        "bucket_count".to_string(),
        Value::Array(
            bucket
                .bucket_counts
                .iter()
                .map(|&count| Value::Number(count.into()))
                .collect(),
        ),
    );
    bucket_json
}

/// otel metrics event has json object for exponential histogram
/// each exponential histogram object has json array for data points
/// this function flatten the exponential histogram json object
/// and returns a `Vec` of `Map` for each data point
fn flatten_exp_histogram(exp_histogram: &ExponentialHistogram) -> Vec<Map<String, Value>> {
    let mut data_points_json = Vec::with_capacity(exp_histogram.data_points.len());
    for data_point in &exp_histogram.data_points {
        let mut data_point_json = Map::with_capacity(data_point.attributes.len() + 12);
        insert_attributes(&mut data_point_json, &data_point.attributes);
        data_point_json.insert(
            "start_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.start_time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "data_point_count".to_string(),
            Value::Number(data_point.count.into()),
        );
        insert_number_if_some(&mut data_point_json, "data_point_sum", &data_point.sum);
        data_point_json.insert(
            "data_point_scale".to_string(),
            Value::Number(data_point.scale.into()),
        );
        data_point_json.insert(
            "data_point_zero_count".to_string(),
            Value::Number(data_point.zero_count.into()),
        );
        if let Some(positive) = &data_point.positive {
            let positive_json = flatten_buckets(positive);
            for (key, value) in positive_json {
                data_point_json.insert(format!("positive_{key}"), value);
            }
        }
        if let Some(negative) = &data_point.negative {
            let negative_json = flatten_buckets(negative);
            for (key, value) in negative_json {
                data_point_json.insert(format!("negative_{key}"), value);
            }
        }
        insert_exemplars(&mut data_point_json, &data_point.exemplars);

        insert_aggregation_temporality(&mut data_point_json, exp_histogram.aggregation_temporality);
        data_points_json.push(data_point_json);
    }
    data_points_json
}

/// otel metrics event has json object for summary
/// each summary object has json array for data points
/// this function flatten the summary json object
/// and returns a `Vec` of `Map` for each data point
fn flatten_summary(summary: &Summary) -> Vec<Map<String, Value>> {
    let mut data_points_json = Vec::with_capacity(summary.data_points.len());
    for data_point in &summary.data_points {
        let mut data_point_json = Map::with_capacity(data_point.attributes.len() + 6);
        insert_attributes(&mut data_point_json, &data_point.attributes);
        data_point_json.insert(
            "start_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.start_time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "data_point_count".to_string(),
            Value::Number(data_point.count.into()),
        );
        data_point_json.insert(
            "data_point_sum".to_string(),
            serde_json::Number::from_f64(data_point.sum)
                .map(Value::Number)
                .unwrap_or(Value::Null),
        );
        data_point_json.insert(
            "data_point_quantile_values".to_string(),
            Value::Array(
                data_point
                    .quantile_values
                    .iter()
                    .map(|quantile_value| {
                        let mut quantile_map = Map::with_capacity(2);
                        quantile_map.insert(
                            "quantile".to_string(),
                            serde_json::Number::from_f64(quantile_value.quantile)
                                .map(Value::Number)
                                .unwrap_or(Value::Null),
                        );
                        quantile_map.insert(
                            "value".to_string(),
                            serde_json::Number::from_f64(quantile_value.value)
                                .map(Value::Number)
                                .unwrap_or(Value::Null),
                        );
                        Value::Object(quantile_map)
                    })
                    .collect(),
            ),
        );

        data_points_json.push(data_point_json);
    }
    data_points_json
}

/// this function flattens the `Metric` object
/// each metric object has json object for gauge, sum, histogram, exponential histogram, summary
/// this function flatten the metric json object
/// and returns a `Vec` of `Map` of the flattened json
/// this function is called recursively for each metric record object in the otel metrics event
pub fn flatten_metrics_record(metrics_record: &Metric) -> Vec<Map<String, Value>> {
    let (mut data_points, metric_type) = match &metrics_record.data {
        Some(metric::Data::Gauge(gauge)) => (flatten_gauge(gauge), "gauge"),
        Some(metric::Data::Sum(sum)) => (flatten_sum(sum), "sum"),
        Some(metric::Data::Histogram(histogram)) => (flatten_histogram(histogram), "histogram"),
        Some(metric::Data::ExponentialHistogram(exp_histogram)) => (
            flatten_exp_histogram(exp_histogram),
            "exponential_histogram",
        ),
        Some(metric::Data::Summary(summary)) => (flatten_summary(summary), "summary"),
        None => return Vec::new(),
    };

    // Build metric-level fields once
    let metric_name = Value::String(metrics_record.name.clone());
    let metric_desc = Value::String(metrics_record.description.clone());
    let metric_unit = Value::String(metrics_record.unit.clone());
    let metric_type_val = Value::String(metric_type.to_string());
    let mut metadata = Map::with_capacity(metrics_record.metadata.len());
    insert_attributes(&mut metadata, &metrics_record.metadata);

    if data_points.is_empty() {
        let mut single = Map::with_capacity(metadata.len() + 8);
        single.insert("metric_name".to_string(), metric_name);
        single.insert("metric_description".to_string(), metric_desc);
        single.insert("metric_unit".to_string(), metric_unit);
        single.insert("metric_type".to_string(), metric_type_val);
        match &metrics_record.data {
            Some(metric::Data::Sum(sum)) => {
                insert_aggregation_temporality(&mut single, sum.aggregation_temporality);
                single.insert("is_monotonic".to_string(), Value::Bool(sum.is_monotonic));
            }
            Some(metric::Data::Histogram(histogram)) => {
                insert_aggregation_temporality(&mut single, histogram.aggregation_temporality);
            }
            Some(metric::Data::ExponentialHistogram(exp_histogram)) => {
                insert_aggregation_temporality(&mut single, exp_histogram.aggregation_temporality);
            }
            _ => {}
        }
        single.extend(metadata);
        return vec![single];
    }

    // Insert metric-level fields directly into each data point
    for dp in &mut data_points {
        dp.insert("metric_name".to_string(), metric_name.clone());
        dp.insert("metric_description".to_string(), metric_desc.clone());
        dp.insert("metric_unit".to_string(), metric_unit.clone());
        dp.insert("metric_type".to_string(), metric_type_val.clone());
        for (k, v) in &metadata {
            dp.insert(k.clone(), v.clone());
        }
    }

    data_points
}

fn metric_data_point_count(metric: &Metric) -> usize {
    match &metric.data {
        Some(metric::Data::Gauge(gauge)) => gauge.data_points.len(),
        Some(metric::Data::Sum(sum)) => sum.data_points.len(),
        Some(metric::Data::Histogram(histogram)) => histogram.data_points.len(),
        Some(metric::Data::ExponentialHistogram(exp_histogram)) => exp_histogram.data_points.len(),
        Some(metric::Data::Summary(summary)) => summary.data_points.len(),
        None => 0,
    }
}

/// Common function to process resource metrics and merge resource-level fields
#[allow(clippy::too_many_arguments)]
fn process_resource_metrics<T, S, M>(
    resource_metrics: &[T],
    get_resource: fn(&T) -> Option<&opentelemetry_proto::tonic::resource::v1::Resource>,
    get_scope_metrics: fn(&T) -> &[S],
    get_schema_url: fn(&T) -> &str,
    get_scope: fn(&S) -> Option<&opentelemetry_proto::tonic::common::v1::InstrumentationScope>,
    get_scope_schema_url: fn(&S) -> &str,
    get_metrics: fn(&S) -> &[M],
    get_metric: fn(&M) -> &Metric,
    tenant_id: &str,
) -> Vec<Value> {
    let _span = info_span!(
        "process_resource_metrics",
        resource_count = resource_metrics.len(),
    )
    .entered();
    let mut vec_otel_json = Vec::new();

    for resource_metric in resource_metrics {
        // Build resource-level fields once per resource
        let mut resource_fields = Map::with_capacity(
            get_resource(resource_metric)
                .map(|resource| resource.attributes.len())
                .unwrap_or_default()
                + 2,
        );
        if let Some(resource) = get_resource(resource_metric) {
            insert_attributes(&mut resource_fields, &resource.attributes);
            resource_fields.insert(
                "resource_dropped_attributes_count".to_string(),
                Value::Number(resource.dropped_attributes_count.into()),
            );
        }
        resource_fields.insert(
            "resource_schema_url".to_string(),
            Value::String(get_schema_url(resource_metric).to_string()),
        );

        for scope_metric in get_scope_metrics(resource_metric) {
            // Build envelope = resource + scope fields (once per scope)
            let mut envelope = resource_fields.clone();

            if let Some(scope) = get_scope(scope_metric) {
                envelope.insert("scope_name".to_string(), Value::String(scope.name.clone()));
                envelope.insert(
                    "scope_version".to_string(),
                    Value::String(scope.version.clone()),
                );
                insert_attributes(&mut envelope, &scope.attributes);
                envelope.insert(
                    "scope_dropped_attributes_count".to_string(),
                    Value::Number(scope.dropped_attributes_count.into()),
                );
            }
            envelope.insert(
                "scope_schema_url".to_string(),
                Value::String(get_scope_schema_url(scope_metric).to_string()),
            );

            let metrics = get_metrics(scope_metric);
            vec_otel_json.reserve(
                metrics
                    .iter()
                    .map(|metric| metric_data_point_count(get_metric(metric)).max(1))
                    .sum::<usize>(),
            );
            let date = chrono::Utc::now().date_naive().to_string();
            increment_metrics_collected_by_date(metrics.len() as u64, &date, tenant_id);

            // Flatten each metric's data points and merge envelope in one pass
            for metric in metrics {
                let data_points = flatten_metrics_record(get_metric(metric));
                for mut dp in data_points {
                    for (k, v) in &envelope {
                        dp.insert(k.clone(), v.clone());
                    }
                    // Compute the physical-series hash AFTER envelope merge
                    // so resource/scope attributes participate in series
                    // identity (they're labels from the query layer's
                    // perspective). Computed once per data point — O(label
                    // count) per sample, ~200 ns at typical attribute counts.
                    let series_hash = compute_series_hash(&dp);
                    // Stored as decimal-encoded string. Arrow-json
                    // infers Utf8, preserving all 64 bits — Int64/Float64
                    // inference truncated values near the high range.
                    dp.insert(
                        "__series_hash".to_string(),
                        Value::String(series_hash.to_string()),
                    );
                    vec_otel_json.push(Value::Object(dp));
                }
            }
        }
    }

    vec_otel_json
}

/// this function performs the custom flattening of the otel metrics
/// and returns a `Vec` of `Value::Object` of the flattened json
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn flatten_otel_metrics(message: MetricsData, tenant_id: &str) -> Vec<Value> {
    process_resource_metrics(
        &message.resource_metrics,
        |record| record.resource.as_ref(),
        |record| &record.scope_metrics,
        |record| &record.schema_url,
        |scope_metric| scope_metric.scope.as_ref(),
        |scope_metric| &scope_metric.schema_url,
        |scope_metric| &scope_metric.metrics,
        |metric| metric,
        tenant_id,
    )
}

/// Flattens OpenTelemetry metrics from protobuf format
pub fn flatten_otel_metrics_protobuf(
    message: &ExportMetricsServiceRequest,
    tenant_id: &str,
) -> Vec<Value> {
    let span = info_span!(
        "flatten_otel_metrics_protobuf",
        resource_metrics_count = message.resource_metrics.len(),
        output_count = tracing::field::Empty,
    );
    let _guard = span.enter();

    let result = process_resource_metrics(
        &message.resource_metrics,
        |record| record.resource.as_ref(),
        |record| &record.scope_metrics,
        |record| &record.schema_url,
        |scope_metric| scope_metric.scope.as_ref(),
        |scope_metric| &scope_metric.schema_url,
        |scope_metric| &scope_metric.metrics,
        |metric| metric,
        tenant_id,
    );

    span.record("output_count", result.len());
    result
}

fn insert_aggregation_temporality(map: &mut Map<String, Value>, aggregation_temporality: i32) {
    map.insert(
        "aggregation_temporality".to_string(),
        Value::Number(aggregation_temporality.into()),
    );
    let description = match aggregation_temporality {
        0 => "UNSPECIFIED",
        1 => "DELTA",
        2 => "CUMULATIVE",
        _ => "",
    };
    map.insert(
        "aggregation_temporality_description".to_string(),
        Value::String(description.to_string()),
    );
}

fn insert_data_point_flags(map: &mut Map<String, Value>, flags: u32) {
    map.insert("data_point_flags".to_string(), Value::Number(flags.into()));
    let description = match flags {
        0 => "DO_NOT_USE",
        1 => "NO_RECORDED_VALUE_MASK",
        _ => "",
    };
    map.insert(
        "data_point_flags_description".to_string(),
        Value::String(description.to_string()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dp() -> Map<String, Value> {
        let mut dp = Map::new();
        dp.insert(
            "metric_name".to_string(),
            Value::String("counter.app.metric_0006".into()),
        );
        dp.insert(
            "time_unix_nano".to_string(),
            Value::String("2026-05-19T09:00:00Z".into()),
        );
        dp.insert("data_point_value".to_string(), Value::Number(1000.into()));
        dp.insert("is_monotonic".to_string(), Value::Bool(true));
        dp.insert("service.name".to_string(), Value::String("api".into()));
        dp.insert("http.method".to_string(), Value::String("GET".into()));
        dp.insert("request.id".to_string(), Value::String("req-1".into()));
        dp
    }

    #[test]
    fn series_hash_stable_across_runs() {
        // Same input → same hash. Locks in the wire contract between
        // ingest and query layers; any algorithm change here breaks
        // grouping for already-ingested data.
        let dp = make_dp();
        let h1 = compute_series_hash(&dp);
        let h2 = compute_series_hash(&dp);
        assert_eq!(h1, h2);
    }

    #[test]
    fn series_hash_independent_of_label_insertion_order() {
        // serde_json::Map preserves insertion order; query side may see
        // labels in different order. Hash must be insertion-order-agnostic.
        let mut a = Map::new();
        a.insert("metric_name".to_string(), Value::String("m".into()));
        a.insert("service.name".to_string(), Value::String("api".into()));
        a.insert("http.method".to_string(), Value::String("GET".into()));

        let mut b = Map::new();
        b.insert("http.method".to_string(), Value::String("GET".into()));
        b.insert("metric_name".to_string(), Value::String("m".into()));
        b.insert("service.name".to_string(), Value::String("api".into()));

        assert_eq!(compute_series_hash(&a), compute_series_hash(&b));
    }

    #[test]
    fn series_hash_changes_with_label_value() {
        let dp = make_dp();
        let mut dp2 = dp.clone();
        dp2.insert("service.name".to_string(), Value::String("billing".into()));
        assert_ne!(compute_series_hash(&dp), compute_series_hash(&dp2));
    }

    #[test]
    fn series_hash_changes_with_metric_name() {
        // Two different metrics with identical labels must hash to
        // different values, otherwise samples for `requests_total` and
        // `latency_seconds` would collide into one logical series.
        let dp = make_dp();
        let mut dp2 = dp.clone();
        dp2.insert(
            "metric_name".to_string(),
            Value::String("other.metric".into()),
        );
        assert_ne!(compute_series_hash(&dp), compute_series_hash(&dp2));
    }

    #[test]
    fn series_hash_ignores_sample_level_fields() {
        // time_unix_nano and data_point_value belong to the SAMPLE, not
        // the series. Hash must be identical across samples of the same
        // physical series taken at different times with different values.
        let dp = make_dp();
        let mut dp_later = dp.clone();
        dp_later.insert(
            "time_unix_nano".to_string(),
            Value::String("2026-05-19T10:00:00Z".into()),
        );
        dp_later.insert("data_point_value".to_string(), Value::Number(2000.into()));
        assert_eq!(compute_series_hash(&dp), compute_series_hash(&dp_later));
    }

    #[test]
    fn series_hash_distinguishes_label_kv_swap() {
        // Pathological pair: {a=bc, d=e} vs {a=b, cd=e}. A naive
        // concatenation hash would emit identical bytes. Delimiters in
        // the hasher input prevent this — verify here so a future
        // optimisation can't silently regress collision resistance.
        let mut a = Map::new();
        a.insert("metric_name".to_string(), Value::String("m".into()));
        a.insert("a".to_string(), Value::String("bc".into()));
        a.insert("d".to_string(), Value::String("e".into()));

        let mut b = Map::new();
        b.insert("metric_name".to_string(), Value::String("m".into()));
        b.insert("a".to_string(), Value::String("b".into()));
        b.insert("cd".to_string(), Value::String("e".into()));

        assert_ne!(compute_series_hash(&a), compute_series_hash(&b));
    }

    fn number(value: f64) -> Value {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }

    #[test]
    fn series_hash_ignores_histogram_min_max() {
        // `min`/`max` are per-sample histogram statistics, not series
        // labels. Two samples of the same series with different min/max
        // must hash identically, otherwise histogram series fragment on
        // every scrape.
        let mut a = make_dp();
        a.insert("min".to_string(), number(1.0));
        a.insert("max".to_string(), number(9.0));
        let mut b = make_dp();
        b.insert("min".to_string(), number(2.0));
        b.insert("max".to_string(), number(8.0));
        assert_eq!(compute_series_hash(&a), compute_series_hash(&b));
    }

    #[test]
    fn series_hash_ignores_summary_quantile_values() {
        // `data_point_quantile_values` is the nested per-sample quantile
        // array. Differing quantile values must not change series identity.
        let mut a = make_dp();
        a.insert(
            "data_point_quantile_values".to_string(),
            Value::Array(vec![number(0.5), number(0.9)]),
        );
        let mut b = make_dp();
        b.insert(
            "data_point_quantile_values".to_string(),
            Value::Array(vec![number(0.7), number(0.99)]),
        );
        assert_eq!(compute_series_hash(&a), compute_series_hash(&b));
    }

    #[test]
    fn series_hash_distinguishes_attribute_named_value_or_quantile() {
        // `value` and `quantile` are only ever emitted nested inside the
        // quantile array, never as top-level fields. They must NOT be
        // treated as known fields: a user attribute literally named
        // `value` or `quantile` is a real label and must affect series
        // identity (previously it was wrongly excluded, colliding series).
        let mut a = make_dp();
        a.insert("value".to_string(), Value::String("x".into()));
        let mut b = make_dp();
        b.insert("value".to_string(), Value::String("y".into()));
        assert_ne!(compute_series_hash(&a), compute_series_hash(&b));

        let mut c = make_dp();
        c.insert("quantile".to_string(), Value::String("p50".into()));
        let mut d = make_dp();
        d.insert("quantile".to_string(), Value::String("p99".into()));
        assert_ne!(compute_series_hash(&c), compute_series_hash(&d));
    }

    #[test]
    fn known_field_list_matches_emitted_keys() {
        // Guard the list against drifting from the keys the flatteners
        // actually emit as top-level data-point fields.
        assert_eq!(OTEL_METRICS_KNOWN_FIELD_LIST.len(), 36);
        for field in ["min", "max", "data_point_quantile_values"] {
            assert!(
                OTEL_METRICS_KNOWN_FIELDS.contains(field),
                "expected `{field}` to be a known field"
            );
        }
        // Removed dead entries that were never emitted as top-level keys.
        for field in [
            "quantile",
            "value",
            "data_point_quantile_values_quantile",
            "data_point_quantile_values_value",
        ] {
            assert!(
                !OTEL_METRICS_KNOWN_FIELDS.contains(field),
                "`{field}` must not be a known field"
            );
        }
    }
}
