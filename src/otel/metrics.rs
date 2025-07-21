/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberDataPointValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogram, Gauge, Histogram, Metric, MetricsData, NumberDataPoint, Sum,
    Summary, exemplar::Value as ExemplarValue, exponential_histogram_data_point::Buckets, metric,
};
use serde_json::{Map, Value};

use super::otel_utils::{
    convert_epoch_nano_to_timestamp, insert_attributes, insert_number_if_some,
};

pub const OTEL_METRICS_KNOWN_FIELD_LIST: [&str; 34] = [
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
    "data_point_scale",
    "data_point_zero_count",
    "data_point_flags",
    "data_point_flags_description",
    "positive_offset",
    "positive_bucket_count",
    "negative_offset",
    "negative_bucket_count",
    "quantile",
    "value",
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
];

/// otel metrics event has json array for exemplar
/// this function flatten the exemplar json array
/// and returns a `Map` of the exemplar json
/// this function is reused in all json objects that have exemplar
fn flatten_exemplar(exemplars: &[Exemplar]) -> Vec<Map<String, Value>> {
    exemplars
        .iter()
        .map(|exemplar| {
            let mut exemplar_json = Map::new();
            insert_attributes(&mut exemplar_json, &exemplar.filtered_attributes);
            exemplar_json.insert(
                "exemplar_time_unix_nano".to_string(),
                Value::String(convert_epoch_nano_to_timestamp(
                    exemplar.time_unix_nano as i64,
                )),
            );
            exemplar_json.insert(
                "exemplar_span_id".to_string(),
                Value::String(hex::encode(&exemplar.span_id)),
            );
            exemplar_json.insert(
                "exemplar_trace_id".to_string(),
                Value::String(hex::encode(&exemplar.trace_id)),
            );
            if let Some(value) = &exemplar.value {
                match value {
                    ExemplarValue::AsDouble(double_val) => {
                        exemplar_json.insert(
                            "exemplar_value".to_string(),
                            serde_json::Number::from_f64(*double_val)
                                .map(Value::Number)
                                .unwrap_or(Value::Null),
                        );
                    }
                    ExemplarValue::AsInt(int_val) => {
                        exemplar_json.insert(
                            "exemplar_value".to_string(),
                            Value::Number(serde_json::Number::from(*int_val)),
                        );
                    }
                }
            }
            exemplar_json
        })
        .collect()
}

/// otel metrics event has json array for number data points
/// this function flatten the number data points json array
/// and returns a `Vec` of `Map` of the flattened json
/// this function is reused in all json objects that have number data points
fn flatten_number_data_points(data_points: &[NumberDataPoint]) -> Vec<Map<String, Value>> {
    data_points
        .iter()
        .map(|data_point| {
            let mut data_point_json = Map::new();
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

            let exemplar_json = flatten_exemplar(&data_point.exemplars);
            if !exemplar_json.is_empty() {
                for exemplar in exemplar_json {
                    for (key, value) in exemplar {
                        data_point_json.insert(key, value);
                    }
                }
            }

            data_point_json.extend(flatten_data_point_flags(data_point.flags));
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
    let mut vec_gauge_json = Vec::new();
    let data_points_json = flatten_number_data_points(&gauge.data_points);

    for data_point_json in data_points_json {
        let mut gauge_json = Map::new();
        for (key, value) in &data_point_json {
            gauge_json.insert(key.clone(), value.clone());
        }
        vec_gauge_json.push(gauge_json);
    }
    vec_gauge_json
}

/// otel metrics event has json object for sum
/// each sum object has json array for data points
/// this function flatten the sum json object
/// and returns a `Vec` of `Map` for each data point
fn flatten_sum(sum: &Sum) -> Vec<Map<String, Value>> {
    let mut vec_sum_json = Vec::new();
    let data_points_json = flatten_number_data_points(&sum.data_points);
    for data_point_json in data_points_json {
        let mut sum_json = Map::new();
        for (key, value) in &data_point_json {
            sum_json.insert(key.clone(), value.clone());
        }
        vec_sum_json.push(sum_json);
    }
    let mut sum_json = Map::new();
    sum_json.extend(flatten_aggregation_temporality(sum.aggregation_temporality));
    sum_json.insert("is_monotonic".to_string(), Value::Bool(sum.is_monotonic));
    for data_point_json in &mut vec_sum_json {
        for (key, value) in &sum_json {
            data_point_json.insert(key.clone(), value.clone());
        }
    }
    vec_sum_json
}

/// otel metrics event has json object for histogram
/// each histogram object has json array for data points
/// this function flatten the histogram json object
/// and returns a `Vec` of `Map` for each data point
fn flatten_histogram(histogram: &Histogram) -> Vec<Map<String, Value>> {
    let mut data_points_json = Vec::new();
    for data_point in &histogram.data_points {
        let mut data_point_json = Map::new();
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
        let exemplar_json = flatten_exemplar(&data_point.exemplars);
        if !exemplar_json.is_empty() {
            for exemplar in exemplar_json {
                for (key, value) in exemplar {
                    data_point_json.insert(key, value);
                }
            }
        }

        data_point_json.extend(flatten_data_point_flags(data_point.flags));
        insert_number_if_some(&mut data_point_json, "min", &data_point.min);
        insert_number_if_some(&mut data_point_json, "max", &data_point.max);
        data_points_json.push(data_point_json);
    }
    let mut histogram_json = Map::new();
    histogram_json.extend(flatten_aggregation_temporality(
        histogram.aggregation_temporality,
    ));
    for data_point_json in &mut data_points_json {
        for (key, value) in &histogram_json {
            data_point_json.insert(key.clone(), value.clone());
        }
    }
    data_points_json
}

/// otel metrics event has json object for buckets
/// this function flatten the buckets json object
/// and returns a `Map` of the flattened json
fn flatten_buckets(bucket: &Buckets) -> Map<String, Value> {
    let mut bucket_json = Map::new();
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
    let mut data_points_json = Vec::new();
    for data_point in &exp_histogram.data_points {
        let mut data_point_json = Map::new();
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
        let exemplar_json = flatten_exemplar(&data_point.exemplars);
        if !exemplar_json.is_empty() {
            for exemplar in exemplar_json {
                for (key, value) in exemplar {
                    data_point_json.insert(key, value);
                }
            }
        }

        data_points_json.push(data_point_json);
    }
    let mut exp_histogram_json = Map::new();
    exp_histogram_json.extend(flatten_aggregation_temporality(
        exp_histogram.aggregation_temporality,
    ));
    for data_point_json in &mut data_points_json {
        for (key, value) in &exp_histogram_json {
            data_point_json.insert(key.clone(), value.clone());
        }
    }
    data_points_json
}

/// otel metrics event has json object for summary
/// each summary object has json array for data points
/// this function flatten the summary json object
/// and returns a `Vec` of `Map` for each data point
fn flatten_summary(summary: &Summary) -> Vec<Map<String, Value>> {
    let mut data_points_json = Vec::new();
    for data_point in &summary.data_points {
        let mut data_point_json = Map::new();
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
                        Value::Object(
                            vec![
                                (
                                    "quantile",
                                    serde_json::Number::from_f64(quantile_value.quantile)
                                        .map(Value::Number)
                                        .unwrap_or(Value::Null),
                                ),
                                (
                                    "value",
                                    serde_json::Number::from_f64(quantile_value.value)
                                        .map(Value::Number)
                                        .unwrap_or(Value::Null),
                                ),
                            ]
                            .into_iter()
                            .map(|(k, v)| (k.to_string(), v))
                            .collect(),
                        )
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
    let mut data_points_json = Vec::new();
    let mut metric_json = Map::new();
    let mut metric_type = String::default();
    match &metrics_record.data {
        Some(metric::Data::Gauge(gauge)) => {
            metric_type = "gauge".to_string();
            data_points_json.extend(flatten_gauge(gauge));
        }
        Some(metric::Data::Sum(sum)) => {
            metric_type = "sum".to_string();
            data_points_json.extend(flatten_sum(sum));
        }
        Some(metric::Data::Histogram(histogram)) => {
            metric_type = "histogram".to_string();
            data_points_json.extend(flatten_histogram(histogram));
        }
        Some(metric::Data::ExponentialHistogram(exp_histogram)) => {
            metric_type = "exponential_histogram".to_string();
            data_points_json.extend(flatten_exp_histogram(exp_histogram));
        }
        Some(metric::Data::Summary(summary)) => {
            metric_type = "summary".to_string();
            data_points_json.extend(flatten_summary(summary));
        }
        None => {}
    }
    metric_json.insert(
        "metric_name".to_string(),
        Value::String(metrics_record.name.clone()),
    );
    metric_json.insert(
        "metric_description".to_string(),
        Value::String(metrics_record.description.clone()),
    );
    metric_json.insert(
        "metric_unit".to_string(),
        Value::String(metrics_record.unit.clone()),
    );
    metric_json.insert("metric_type".to_string(), Value::String(metric_type));
    insert_attributes(&mut metric_json, &metrics_record.metadata);
    for data_point_json in &mut data_points_json {
        for (key, value) in &metric_json {
            data_point_json.insert(key.clone(), value.clone());
        }
    }
    if data_points_json.is_empty() {
        data_points_json.push(metric_json);
    }
    data_points_json
}

/// this function performs the custom flattening of the otel metrics
/// and returns a `Vec` of `Value::Object` of the flattened json
pub fn flatten_otel_metrics(message: MetricsData) -> Vec<Value> {
    let mut vec_otel_json = Vec::new();
    for record in &message.resource_metrics {
        let mut resource_metrics_json = Map::new();
        if let Some(resource) = &record.resource {
            insert_attributes(&mut resource_metrics_json, &resource.attributes);
            resource_metrics_json.insert(
                "resource_dropped_attributes_count".to_string(),
                Value::Number(resource.dropped_attributes_count.into()),
            );
        }

        let mut vec_scope_metrics_json = Vec::new();
        for scope_metric in &record.scope_metrics {
            let mut scope_metrics_json = Map::new();
            for metrics_record in &scope_metric.metrics {
                vec_scope_metrics_json.extend(flatten_metrics_record(metrics_record));
            }

            if let Some(scope) = &scope_metric.scope {
                scope_metrics_json
                    .insert("scope_name".to_string(), Value::String(scope.name.clone()));
                scope_metrics_json.insert(
                    "scope_version".to_string(),
                    Value::String(scope.version.clone()),
                );
                insert_attributes(&mut scope_metrics_json, &scope.attributes);
                scope_metrics_json.insert(
                    "scope_dropped_attributes_count".to_string(),
                    Value::Number(scope.dropped_attributes_count.into()),
                );
            }

            scope_metrics_json.insert(
                "scope_schema_url".to_string(),
                Value::String(scope_metric.schema_url.clone()),
            );

            for scope_metric_json in &mut vec_scope_metrics_json {
                for (key, value) in &scope_metrics_json {
                    scope_metric_json.insert(key.clone(), value.clone());
                }
            }
        }

        resource_metrics_json.insert(
            "resource_schema_url".to_string(),
            Value::String(record.schema_url.clone()),
        );

        for resource_metric_json in &mut vec_scope_metrics_json {
            for (key, value) in &resource_metrics_json {
                resource_metric_json.insert(key.clone(), value.clone());
            }

            vec_otel_json.push(Value::Object(resource_metric_json.clone()));
        }
    }

    vec_otel_json
}

/// otel metrics event has json object for aggregation temporality
/// there is a mapping of aggregation temporality to its description provided in proto
/// this function fetches the description from the aggregation temporality
/// and adds it to the flattened json
fn flatten_aggregation_temporality(aggregation_temporality: i32) -> Map<String, Value> {
    let mut aggregation_temporality_json = Map::new();
    aggregation_temporality_json.insert(
        "aggregation_temporality".to_string(),
        Value::Number(aggregation_temporality.into()),
    );
    let description = match aggregation_temporality {
        0 => "AGGREGATION_TEMPORALITY_UNSPECIFIED",
        1 => "AGGREGATION_TEMPORALITY_DELTA",
        2 => "AGGREGATION_TEMPORALITY_CUMULATIVE",
        _ => "",
    };
    aggregation_temporality_json.insert(
        "aggregation_temporality_description".to_string(),
        Value::String(description.to_string()),
    );

    aggregation_temporality_json
}

fn flatten_data_point_flags(flags: u32) -> Map<String, Value> {
    let mut data_point_flags_json = Map::new();
    data_point_flags_json.insert("data_point_flags".to_string(), Value::Number(flags.into()));
    let description = match flags {
        0 => "DATA_POINT_FLAGS_DO_NOT_USE",
        1 => "DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK",
        _ => "",
    };
    data_point_flags_json.insert(
        "data_point_flags_description".to_string(),
        Value::String(description.to_string()),
    );
    data_point_flags_json
}
