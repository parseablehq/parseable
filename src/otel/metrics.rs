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

use std::collections::BTreeMap;

use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberDataPointValue;
use opentelemetry_proto::tonic::metrics::v1::{
    exemplar::Value as ExemplarValue, exponential_histogram_data_point::Buckets, metric, Exemplar,
    ExponentialHistogram, Gauge, Histogram, Metric, MetricsData, NumberDataPoint, Sum, Summary,
};
use serde_json::Value;

use super::otel_utils::{
    convert_epoch_nano_to_timestamp, insert_attributes, insert_number_if_some,
};

/// otel metrics event has json array for exemplar
/// this function flatten the exemplar json array
/// and returns a `BTreeMap` of the exemplar json
/// this function is reused in all json objects that have exemplar
fn flatten_exemplar(exemplars: &[Exemplar]) -> BTreeMap<String, Value> {
    let mut exemplar_json = BTreeMap::new();
    for exemplar in exemplars {
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
                        "exemplar_value_as_double".to_string(),
                        Value::Number(serde_json::Number::from_f64(*double_val).unwrap()),
                    );
                }
                ExemplarValue::AsInt(int_val) => {
                    exemplar_json.insert(
                        "exemplar_value_as_int".to_string(),
                        Value::Number(serde_json::Number::from(*int_val)),
                    );
                }
            }
        }
    }
    exemplar_json
}

/// otel metrics event has json array for number data points
/// this function flatten the number data points json array
/// and returns a `Vec` of `BTreeMap` of the flattened json
/// this function is reused in all json objects that have number data points
fn flatten_number_data_points(data_points: &[NumberDataPoint]) -> Vec<BTreeMap<String, Value>> {
    data_points
        .iter()
        .map(|data_point| {
            let mut data_point_json = BTreeMap::new();
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
            for (key, value) in exemplar_json {
                data_point_json.insert(key, value);
            }
            data_point_json.extend(flatten_data_point_flags(data_point.flags));
            if let Some(value) = &data_point.value {
                match value {
                    NumberDataPointValue::AsDouble(double_val) => {
                        data_point_json.insert(
                            "data_point_value_as_double".to_string(),
                            Value::Number(serde_json::Number::from_f64(*double_val).unwrap()),
                        );
                    }
                    NumberDataPointValue::AsInt(int_val) => {
                        data_point_json.insert(
                            "data_point_value_as_int".to_string(),
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
/// and returns a `Vec` of `BTreeMap` for each data point
fn flatten_gauge(gauge: &Gauge) -> Vec<BTreeMap<String, Value>> {
    let mut vec_gauge_json = Vec::new();
    let data_points_json = flatten_number_data_points(&gauge.data_points);
    for data_point_json in data_points_json {
        let mut gauge_json = BTreeMap::new();
        for (key, value) in &data_point_json {
            gauge_json.insert(format!("gauge_{}", key), value.clone());
        }
        vec_gauge_json.push(gauge_json);
    }
    vec_gauge_json
}

/// otel metrics event has json object for sum
/// each sum object has json array for data points
/// this function flatten the sum json object
/// and returns a `Vec` of `BTreeMap` for each data point
fn flatten_sum(sum: &Sum) -> Vec<BTreeMap<String, Value>> {
    let mut vec_sum_json = Vec::new();
    let data_points_json = flatten_number_data_points(&sum.data_points);
    for data_point_json in data_points_json {
        let mut sum_json = BTreeMap::new();
        for (key, value) in &data_point_json {
            sum_json.insert(format!("sum_{}", key), value.clone());
        }
        vec_sum_json.push(sum_json);
    }
    let mut sum_json = BTreeMap::new();
    sum_json.extend(flatten_aggregation_temporality(sum.aggregation_temporality));
    sum_json.insert(
        "sum_is_monotonic".to_string(),
        Value::Bool(sum.is_monotonic),
    );
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
/// and returns a `Vec` of `BTreeMap` for each data point
fn flatten_histogram(histogram: &Histogram) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json = Vec::new();
    for data_point in &histogram.data_points {
        let mut data_point_json = BTreeMap::new();
        insert_attributes(&mut data_point_json, &data_point.attributes);
        data_point_json.insert(
            "histogram_start_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.start_time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "histogram_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "histogram_data_point_count".to_string(),
            Value::Number(data_point.count.into()),
        );
        insert_number_if_some(
            &mut data_point_json,
            "histogram_data_point_sum",
            &data_point.sum,
        );
        for (index, bucket_count) in data_point.bucket_counts.iter().enumerate() {
            data_point_json.insert(
                format!("histogram_data_point_bucket_count_{}", index + 1),
                Value::String(bucket_count.to_string()),
            );
        }
        for (index, explicit_bound) in data_point.explicit_bounds.iter().enumerate() {
            data_point_json.insert(
                format!("histogram_data_point_explicit_bound_{}", index + 1),
                Value::String(explicit_bound.to_string()),
            );
        }
        let exemplar_json = flatten_exemplar(&data_point.exemplars);
        for (key, value) in exemplar_json {
            data_point_json.insert(format!("histogram_{}", key), value);
        }
        data_point_json.extend(flatten_data_point_flags(data_point.flags));
        insert_number_if_some(&mut data_point_json, "histogram_min", &data_point.min);
        insert_number_if_some(&mut data_point_json, "histogram_max", &data_point.max);
        data_points_json.push(data_point_json);
    }
    let mut histogram_json = BTreeMap::new();
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
/// and returns a `BTreeMap` of the flattened json
fn flatten_buckets(bucket: &Buckets) -> BTreeMap<String, Value> {
    let mut bucket_json = BTreeMap::new();
    bucket_json.insert("offset".to_string(), Value::Number(bucket.offset.into()));

    for (index, bucket_count) in bucket.bucket_counts.iter().enumerate() {
        bucket_json.insert(
            format!("bucket_count_{}", index + 1),
            Value::String(bucket_count.to_string()),
        );
    }
    bucket_json
}

/// otel metrics event has json object for exponential histogram
/// each exponential histogram object has json array for data points
/// this function flatten the exponential histogram json object
/// and returns a `Vec` of `BTreeMap` for each data point
fn flatten_exp_histogram(exp_histogram: &ExponentialHistogram) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json = Vec::new();
    for data_point in &exp_histogram.data_points {
        let mut data_point_json = BTreeMap::new();
        insert_attributes(&mut data_point_json, &data_point.attributes);
        data_point_json.insert(
            "exponential_histogram_start_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.start_time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "exponential_histogram_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "exponential_histogram_data_point_count".to_string(),
            Value::Number(data_point.count.into()),
        );
        insert_number_if_some(
            &mut data_point_json,
            "exponential_histogram_data_point_sum",
            &data_point.sum,
        );
        data_point_json.insert(
            "exponential_histogram_data_point_scale".to_string(),
            Value::Number(data_point.scale.into()),
        );
        data_point_json.insert(
            "exponential_histogram_data_point_zero_count".to_string(),
            Value::Number(data_point.zero_count.into()),
        );
        if let Some(positive) = &data_point.positive {
            let positive_json = flatten_buckets(positive);
            for (key, value) in positive_json {
                data_point_json.insert(format!("exponential_histogram_positive_{}", key), value);
            }
        }
        if let Some(negative) = &data_point.negative {
            let negative_json = flatten_buckets(negative);
            for (key, value) in negative_json {
                data_point_json.insert(format!("exponential_histogram_negative_{}", key), value);
            }
        }
        let exemplar_json = flatten_exemplar(&data_point.exemplars);
        for (key, value) in exemplar_json {
            data_point_json.insert(format!("exponential_histogram_{}", key), value);
        }
        data_points_json.push(data_point_json);
    }
    let mut exp_histogram_json = BTreeMap::new();
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
/// and returns a `Vec` of `BTreeMap` for each data point
fn flatten_summary(summary: &Summary) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json = Vec::new();
    for data_point in &summary.data_points {
        let mut data_point_json = BTreeMap::new();
        insert_attributes(&mut data_point_json, &data_point.attributes);
        data_point_json.insert(
            "summary_start_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.start_time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "summary_time_unix_nano".to_string(),
            Value::String(convert_epoch_nano_to_timestamp(
                data_point.time_unix_nano as i64,
            )),
        );
        data_point_json.insert(
            "summary_data_point_count".to_string(),
            Value::Number(data_point.count.into()),
        );
        data_point_json.insert(
            "summary_data_point_sum".to_string(),
            Value::Number(serde_json::Number::from_f64(data_point.sum).unwrap()),
        );
        for (index, quantile_value) in data_point.quantile_values.iter().enumerate() {
            data_point_json.insert(
                format!("summary_quantile_value_quantile_{}", index + 1),
                Value::Number(serde_json::Number::from_f64(quantile_value.quantile).unwrap()),
            );
            data_point_json.insert(
                format!("summary_quantile_value_value_{}", index + 1),
                Value::Number(serde_json::Number::from_f64(quantile_value.value).unwrap()),
            );
        }
        data_points_json.push(data_point_json);
    }
    data_points_json
}

/// this function flattens the `Metric` object
/// each metric object has json object for gauge, sum, histogram, exponential histogram, summary
/// this function flatten the metric json object
/// and returns a `Vec` of `BTreeMap` of the flattened json
/// this function is called recursively for each metric record object in the otel metrics event
pub fn flatten_metrics_record(metrics_record: &Metric) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json = Vec::new();
    let mut metric_json = BTreeMap::new();

    match &metrics_record.data {
        Some(metric::Data::Gauge(gauge)) => {
            data_points_json.extend(flatten_gauge(gauge));
        }
        Some(metric::Data::Sum(sum)) => {
            data_points_json.extend(flatten_sum(sum));
        }
        Some(metric::Data::Histogram(histogram)) => {
            data_points_json.extend(flatten_histogram(histogram));
        }
        Some(metric::Data::ExponentialHistogram(exp_histogram)) => {
            data_points_json.extend(flatten_exp_histogram(exp_histogram));
        }
        Some(metric::Data::Summary(summary)) => {
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
    insert_attributes(&mut metric_json, &metrics_record.metadata);
    for data_point_json in &mut data_points_json {
        for (key, value) in &metric_json {
            data_point_json.insert(key.clone(), value.clone());
        }
    }
    data_points_json
}

/// this function performs the custom flattening of the otel metrics
/// and returns a `Vec` of `BTreeMap` of the flattened json
pub fn flatten_otel_metrics(message: MetricsData) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json = Vec::new();
    for record in &message.resource_metrics {
        let mut resource_metrics_json = BTreeMap::new();
        if let Some(resource) = &record.resource {
            insert_attributes(&mut resource_metrics_json, &resource.attributes);
            resource_metrics_json.insert(
                "resource_dropped_attributes_count".to_string(),
                Value::Number(resource.dropped_attributes_count.into()),
            );
        }
        let mut vec_scope_metrics_json = Vec::new();
        for scope_metric in &record.scope_metrics {
            let mut scope_metrics_json = BTreeMap::new();
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
                "scope_metrics_schema_url".to_string(),
                Value::String(scope_metric.schema_url.clone()),
            );

            for scope_metric_json in &mut vec_scope_metrics_json {
                for (key, value) in &scope_metrics_json {
                    scope_metric_json.insert(key.clone(), value.clone());
                }
            }
        }
        resource_metrics_json.insert(
            "resource_metrics_schema_url".to_string(),
            Value::String(record.schema_url.clone()),
        );
        for resource_metric_json in &mut vec_scope_metrics_json {
            for (key, value) in &resource_metrics_json {
                resource_metric_json.insert(key.clone(), value.clone());
            }
        }
        vec_otel_json.extend(vec_scope_metrics_json);
    }
    vec_otel_json
}

/// otel metrics event has json object for aggregation temporality
/// there is a mapping of aggregation temporality to its description provided in proto
/// this function fetches the description from the aggregation temporality
/// and adds it to the flattened json
fn flatten_aggregation_temporality(aggregation_temporality: i32) -> BTreeMap<String, Value> {
    let mut aggregation_temporality_json = BTreeMap::new();
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

fn flatten_data_point_flags(flags: u32) -> BTreeMap<String, Value> {
    let mut data_point_flags_json = BTreeMap::new();
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
