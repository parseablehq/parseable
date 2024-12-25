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

use bytes::Bytes;
use serde_json::Value;

use super::{
    insert_attributes, insert_bool_if_some, insert_if_some, insert_number_if_some,
    proto::metrics::v1::{
        exponential_histogram_data_point::Buckets, Exemplar, ExponentialHistogram, Gauge,
        Histogram, Metric, MetricsData, NumberDataPoint, Sum, Summary,
    },
};

/// otel metrics event has json array for exemplar
/// this function flatten the exemplar json array
/// and returns a `BTreeMap` of the exemplar json
/// this function is reused in all json objects that have exemplar
fn flatten_exemplar(exemplars: &[Exemplar]) -> BTreeMap<String, Value> {
    let mut exemplar_json = BTreeMap::new();
    for exemplar in exemplars {
        insert_attributes(&mut exemplar_json, &exemplar.filtered_attributes);
        insert_if_some(
            &mut exemplar_json,
            "exemplar_time_unix_nano",
            &exemplar.time_unix_nano,
        );
        insert_if_some(&mut exemplar_json, "exemplar_span_id", &exemplar.span_id);
        insert_if_some(&mut exemplar_json, "exemplar_trace_id", &exemplar.trace_id);
        insert_number_if_some(
            &mut exemplar_json,
            "exemplar_as_double",
            &exemplar.as_double,
        );
        insert_if_some(&mut exemplar_json, "exemplar_as_int", &exemplar.as_int);
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
            insert_if_some(
                &mut data_point_json,
                "start_time_unix_nano",
                &data_point.start_time_unix_nano,
            );
            insert_if_some(
                &mut data_point_json,
                "time_unix_nano",
                &data_point.time_unix_nano,
            );
            insert_number_if_some(
                &mut data_point_json,
                "data_point_as_double",
                &data_point.as_double,
            );
            insert_if_some(
                &mut data_point_json,
                "data_point_as_int",
                &data_point.as_int,
            );
            if let Some(exemplars) = &data_point.exemplars {
                let exemplar_json = flatten_exemplar(exemplars);
                for (key, value) in exemplar_json {
                    data_point_json.insert(key, value);
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
    if let Some(data_points) = &gauge.data_points {
        let data_points_json = flatten_number_data_points(data_points);
        for data_point_json in data_points_json {
            let mut gauge_json = BTreeMap::new();
            for (key, value) in &data_point_json {
                gauge_json.insert(format!("gauge_{}", key), value.clone());
            }
            vec_gauge_json.push(gauge_json);
        }
    }
    vec_gauge_json
}

/// otel metrics event has json object for sum
/// each sum object has json array for data points
/// this function flatten the sum json object
/// and returns a `Vec` of `BTreeMap` for each data point
fn flatten_sum(sum: &Sum) -> Vec<BTreeMap<String, Value>> {
    let mut vec_sum_json = Vec::new();
    if let Some(data_points) = &sum.data_points {
        let data_points_json = flatten_number_data_points(data_points);
        for data_point_json in data_points_json {
            let mut sum_json = BTreeMap::new();
            for (key, value) in &data_point_json {
                sum_json.insert(format!("sum_{}", key), value.clone());
            }
            vec_sum_json.push(sum_json);
        }
        let mut sum_json = BTreeMap::new();
        sum_json.extend(flatten_aggregation_temporality(
            &sum.aggregation_temporality,
        ));
        insert_bool_if_some(&mut sum_json, "sum_is_monotonic", &sum.is_monotonic);
        for data_point_json in &mut vec_sum_json {
            for (key, value) in &sum_json {
                data_point_json.insert(key.clone(), value.clone());
            }
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
    if let Some(histogram_data_points) = &histogram.data_points {
        for data_point in histogram_data_points {
            let mut data_point_json = BTreeMap::new();
            insert_attributes(&mut data_point_json, &data_point.attributes);
            insert_if_some(
                &mut data_point_json,
                "histogram_start_time_unix_nano",
                &data_point.start_time_unix_nano,
            );
            insert_if_some(
                &mut data_point_json,
                "histogram_time_unix_nano",
                &data_point.time_unix_nano,
            );
            insert_if_some(
                &mut data_point_json,
                "histogram_data_point_count",
                &data_point.count,
            );
            insert_number_if_some(
                &mut data_point_json,
                "histogram_data_point_sum",
                &data_point.sum,
            );
            if let Some(bucket_counts) = &data_point.bucket_counts {
                for (index, bucket_count) in bucket_counts.iter().enumerate() {
                    data_point_json.insert(
                        format!("histogram_data_point_bucket_count_{}", index + 1),
                        Value::String(bucket_count.to_string()),
                    );
                }
            }
            if let Some(explicit_bounds) = &data_point.explicit_bounds {
                for (index, explicit_bound) in explicit_bounds.iter().enumerate() {
                    data_point_json.insert(
                        format!("histogram_data_point_explicit_bound_{}", index + 1),
                        Value::String(explicit_bound.to_string()),
                    );
                }
            }
            if let Some(exemplars) = &data_point.exemplars {
                let exemplar_json = flatten_exemplar(exemplars);
                for (key, value) in exemplar_json {
                    data_point_json.insert(format!("histogram_{}", key), value);
                }
            }
            data_points_json.push(data_point_json);
        }
    }
    let mut histogram_json = BTreeMap::new();
    histogram_json.extend(flatten_aggregation_temporality(
        &histogram.aggregation_temporality,
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
    insert_number_if_some(&mut bucket_json, "offset", &bucket.offset.map(|v| v as f64));
    if let Some(bucket_counts) = &bucket.bucket_counts {
        for (index, bucket_count) in bucket_counts.iter().enumerate() {
            bucket_json.insert(
                format!("bucket_count_{}", index + 1),
                Value::String(bucket_count.to_string()),
            );
        }
    }
    bucket_json
}

/// otel metrics event has json object for exponential histogram
/// each exponential histogram object has json array for data points
/// this function flatten the exponential histogram json object
/// and returns a `Vec` of `BTreeMap` for each data point
fn flatten_exp_histogram(exp_histogram: &ExponentialHistogram) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json = Vec::new();
    if let Some(exp_histogram_data_points) = &exp_histogram.data_points {
        for data_point in exp_histogram_data_points {
            let mut data_point_json = BTreeMap::new();
            insert_attributes(&mut data_point_json, &data_point.attributes);
            insert_if_some(
                &mut data_point_json,
                "exponential_histogram_start_time_unix_nano",
                &data_point.start_time_unix_nano,
            );
            insert_if_some(
                &mut data_point_json,
                "exponential_histogram_time_unix_nano",
                &data_point.time_unix_nano,
            );
            insert_if_some(
                &mut data_point_json,
                "exponential_histogram_data_point_count",
                &data_point.count,
            );
            insert_number_if_some(
                &mut data_point_json,
                "exponential_histogram_data_point_sum",
                &data_point.sum,
            );
            insert_number_if_some(
                &mut data_point_json,
                "exponential_histogram_data_point_scale",
                &data_point.scale.map(|v| v as f64),
            );
            insert_number_if_some(
                &mut data_point_json,
                "exponential_histogram_data_point_zero_count",
                &data_point.zero_count.map(|v| v as f64),
            );
            if let Some(positive) = &data_point.positive {
                let positive_json = flatten_buckets(positive);
                for (key, value) in positive_json {
                    data_point_json
                        .insert(format!("exponential_histogram_positive_{}", key), value);
                }
            }
            if let Some(negative) = &data_point.negative {
                let negative_json = flatten_buckets(negative);
                for (key, value) in negative_json {
                    data_point_json
                        .insert(format!("exponential_histogram_negative_{}", key), value);
                }
            }
            if let Some(exemplars) = &data_point.exemplars {
                let exemplar_json = flatten_exemplar(exemplars);
                for (key, value) in exemplar_json {
                    data_point_json.insert(format!("exponential_histogram_{}", key), value);
                }
            }
            data_points_json.push(data_point_json);
        }
    }
    let mut exp_histogram_json = BTreeMap::new();
    exp_histogram_json.extend(flatten_aggregation_temporality(
        &exp_histogram.aggregation_temporality,
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
    if let Some(summary_data_points) = &summary.data_points {
        for data_point in summary_data_points {
            let mut data_point_json = BTreeMap::new();
            insert_attributes(&mut data_point_json, &data_point.attributes);
            insert_if_some(
                &mut data_point_json,
                "summary_start_time_unix_nano",
                &data_point.start_time_unix_nano,
            );
            insert_if_some(
                &mut data_point_json,
                "summary_time_unix_nano",
                &data_point.time_unix_nano,
            );
            insert_if_some(
                &mut data_point_json,
                "summary_data_point_count",
                &data_point.count,
            );
            insert_number_if_some(
                &mut data_point_json,
                "summary_data_point_sum",
                &data_point.sum,
            );
            if let Some(quantile_values) = &data_point.quantile_values {
                for (index, quantile_value) in quantile_values.iter().enumerate() {
                    insert_number_if_some(
                        &mut data_point_json,
                        &format!("summary_quantile_value_quantile_{}", index + 1),
                        &quantile_value.quantile,
                    );
                    insert_if_some(
                        &mut data_point_json,
                        &format!("summary_quantile_value_value_{}", index + 1),
                        &quantile_value.value,
                    );
                }
            }
            data_points_json.push(data_point_json);
        }
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
    if let Some(gauge) = &metrics_record.gauge {
        data_points_json.extend(flatten_gauge(gauge));
    }
    if let Some(sum) = &metrics_record.sum {
        data_points_json.extend(flatten_sum(sum));
    }
    if let Some(histogram) = &metrics_record.histogram {
        data_points_json.extend(flatten_histogram(histogram));
    }
    if let Some(exp_histogram) = &metrics_record.exponential_histogram {
        data_points_json.extend(flatten_exp_histogram(exp_histogram));
    }
    if let Some(summary) = &metrics_record.summary {
        data_points_json.extend(flatten_summary(summary));
    }
    insert_if_some(&mut metric_json, "metric_name", &metrics_record.name);
    insert_if_some(
        &mut metric_json,
        "metric_description",
        &metrics_record.description,
    );
    insert_if_some(&mut metric_json, "metric_unit", &metrics_record.unit);
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
pub fn flatten_otel_metrics(body: &Bytes) -> Vec<BTreeMap<String, Value>> {
    let body_str = std::str::from_utf8(body).unwrap();
    let message: MetricsData = serde_json::from_str(body_str).unwrap();
    let mut vec_otel_json = Vec::new();
    if let Some(records) = &message.resource_metrics {
        for record in records {
            let mut resource_metrics_json = BTreeMap::new();
            if let Some(resource) = &record.resource {
                insert_attributes(&mut resource_metrics_json, &resource.attributes);
                insert_number_if_some(
                    &mut resource_metrics_json,
                    "resource_dropped_attributes_count",
                    &resource.dropped_attributes_count.map(|f| f as f64),
                );
            }
            let mut vec_scope_metrics_json = Vec::new();
            if let Some(scope_metrics) = &record.scope_metrics {
                for scope_metric in scope_metrics {
                    let mut scope_metrics_json = BTreeMap::new();
                    for metrics_record in &scope_metric.metrics {
                        vec_scope_metrics_json.extend(flatten_metrics_record(metrics_record));
                    }
                    if let Some(scope) = &scope_metric.scope {
                        insert_if_some(&mut scope_metrics_json, "scope_name", &scope.name);
                        insert_if_some(&mut scope_metrics_json, "scope_version", &scope.version);
                        insert_attributes(&mut scope_metrics_json, &scope.attributes);
                        insert_number_if_some(
                            &mut scope_metrics_json,
                            "scope_dropped_attributes_count",
                            &scope.dropped_attributes_count.map(|f| f as f64),
                        );
                        for scope_metric_json in &mut vec_scope_metrics_json {
                            for (key, value) in &scope_metrics_json {
                                scope_metric_json.insert(key.clone(), value.clone());
                            }
                        }
                    }
                    if let Some(schema_url) = &scope_metric.schema_url {
                        for scope_metrics_json in &mut vec_scope_metrics_json {
                            scope_metrics_json.insert(
                                "scope_metrics_schema_url".to_string(),
                                Value::String(schema_url.clone()),
                            );
                        }
                    }
                }
            }
            insert_if_some(
                &mut resource_metrics_json,
                "resource_metrics_schema_url",
                &record.schema_url,
            );
            for resource_metric_json in &mut vec_scope_metrics_json {
                for (key, value) in &resource_metrics_json {
                    resource_metric_json.insert(key.clone(), value.clone());
                }
            }
            vec_otel_json.extend(vec_scope_metrics_json);
        }
    }
    vec_otel_json
}

/// otel metrics event has json object for aggregation temporality
/// there is a mapping of aggregation temporality to its description provided in proto
/// this function fetches the description from the aggregation temporality
/// and adds it to the flattened json
fn flatten_aggregation_temporality(
    aggregation_temporality: &Option<i32>,
) -> BTreeMap<String, Value> {
    let mut aggregation_temporality_json = BTreeMap::new();
    insert_number_if_some(
        &mut aggregation_temporality_json,
        "aggregation_temporality",
        &aggregation_temporality.map(|f| f as f64),
    );

    if let Some(aggregation_temporality) = aggregation_temporality {
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
    }

    aggregation_temporality_json
}
