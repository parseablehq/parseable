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

use super::otel::{
    flatten_attributes,
    proto::metrics::v1::{
        exponential_histogram_data_point::Buckets, Exemplar, ExponentialHistogram, Gauge,
        Histogram, Metric, MetricsData, Sum, Summary,
    },
};

pub fn flatten_otel_metrics(body: &Bytes) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let body_str = std::str::from_utf8(body).unwrap();
    let message: MetricsData = serde_json::from_str(body_str).unwrap();

    if let Some(records) = message.resource_metrics.as_ref() {
        let mut vec_resource_metrics_json: Vec<BTreeMap<String, Value>> = Vec::new();
        for record in records.iter() {
            let mut resource_metrics_json: BTreeMap<String, Value> = BTreeMap::new();

            if let Some(resource) = record.resource.as_ref() {
                //flatten attributes
                if let Some(attributes) = resource.attributes.as_ref() {
                    let attributes_json = flatten_attributes(attributes);
                    for key in attributes_json.keys() {
                        resource_metrics_json
                            .insert(key.to_owned(), attributes_json[key].to_owned());
                    }
                }

                if resource.dropped_attributes_count.is_some() {
                    resource_metrics_json.insert(
                        "resource_dropped_attributes_count".to_string(),
                        Value::Number(serde_json::Number::from(
                            resource.dropped_attributes_count.unwrap(),
                        )),
                    );
                }
            }

            if let Some(scope_metrics) = record.scope_metrics.as_ref() {
                let mut vec_scope_metrics_json: Vec<BTreeMap<String, Value>> = Vec::new();
                for scope_metric in scope_metrics.iter() {
                    for metrics_record in scope_metric.metrics.iter() {
                        let metrics_record_json = flatten_metrics_record(metrics_record);

                        for metrics_record_json in metrics_record_json.iter() {
                            vec_scope_metrics_json.push(metrics_record_json.clone());
                        }
                    }

                    if scope_metric.scope.is_some() {
                        let instrumentation_scope = scope_metric.scope.as_ref().unwrap();
                        if instrumentation_scope.name.is_some() {
                            for scope_metrics_json in vec_scope_metrics_json.iter_mut() {
                                scope_metrics_json.insert(
                                    "instrumentation_scope_name".to_string(),
                                    Value::String(
                                        instrumentation_scope.name.as_ref().unwrap().to_string(),
                                    ),
                                );
                            }
                        }
                        if instrumentation_scope.version.is_some() {
                            for scope_metrics_json in vec_scope_metrics_json.iter_mut() {
                                scope_metrics_json.insert(
                                    "instrumentation_scope_version".to_string(),
                                    Value::String(
                                        instrumentation_scope.version.as_ref().unwrap().to_string(),
                                    ),
                                );
                            }
                        }

                        if let Some(attributes) = instrumentation_scope.attributes.as_ref() {
                            let attributes_json = flatten_attributes(attributes);
                            for key in attributes_json.keys() {
                                for scope_metrics_json in vec_scope_metrics_json.iter_mut() {
                                    scope_metrics_json
                                        .insert(key.to_owned(), attributes_json[key].to_owned());
                                }
                            }
                        }

                        if instrumentation_scope.dropped_attributes_count.is_some() {
                            for scope_metrics_json in vec_scope_metrics_json.iter_mut() {
                                scope_metrics_json.insert(
                                    "instrumentation_scope_dropped_attributes_count".to_string(),
                                    Value::Number(serde_json::Number::from(
                                        instrumentation_scope.dropped_attributes_count.unwrap(),
                                    )),
                                );
                            }
                        }
                    }
                    if scope_metric.schema_url.is_some() {
                        for scope_metrics_json in vec_scope_metrics_json.iter_mut() {
                            scope_metrics_json.insert(
                                "scope_metrics_schema_url".to_string(),
                                Value::String(
                                    scope_metric.schema_url.as_ref().unwrap().to_string(),
                                ),
                            );
                        }
                    }
                }
                for scope_metrics_json in vec_scope_metrics_json.iter() {
                    vec_resource_metrics_json.push(scope_metrics_json.clone());
                }
            }

            if record.schema_url.is_some() {
                resource_metrics_json.insert(
                    "resource_metrics_schema_url".to_string(),
                    Value::String(record.schema_url.as_ref().unwrap().to_string()),
                );
            }

            for resource_metric_json in vec_resource_metrics_json.iter_mut() {
                for key in resource_metrics_json.keys() {
                    resource_metric_json
                        .insert(key.to_owned(), resource_metrics_json[key].to_owned());
                }
            }
        }
        for resource_metrics_json in vec_resource_metrics_json.iter() {
            vec_otel_json.push(resource_metrics_json.clone());
        }
    }
    vec_otel_json
}

pub fn flatten_exemplar(exemplars: &[Exemplar]) -> BTreeMap<String, Value> {
    let mut exemplar_json: BTreeMap<String, Value> = BTreeMap::new();
    for exemplar in exemplars.iter() {
        if let Some(attributes) = exemplar.filtered_attributes.as_ref() {
            let attributes_json = flatten_attributes(attributes);
            for key in attributes_json.keys() {
                exemplar_json.insert(
                    format!("exemplar_{}", key.to_owned()),
                    attributes_json[key].to_owned(),
                );
            }
        }

        if exemplar.time_unix_nano.is_some() {
            exemplar_json.insert(
                "exemplar_time_unix_nano".to_string(),
                Value::String(exemplar.time_unix_nano.as_ref().unwrap().to_string()),
            );
        }

        if exemplar.span_id.is_some() {
            exemplar_json.insert(
                "exemplar_span_id".to_string(),
                Value::String(exemplar.span_id.as_ref().unwrap().to_string()),
            );
        }

        if exemplar.trace_id.is_some() {
            exemplar_json.insert(
                "exemplar_trace_id".to_string(),
                Value::String(exemplar.trace_id.as_ref().unwrap().to_string()),
            );
        }

        if exemplar.as_double.is_some() {
            exemplar_json.insert(
                "exemplar_as_double".to_string(),
                Value::Number(serde_json::Number::from_f64(exemplar.as_double.unwrap()).unwrap()),
            );
        }

        if exemplar.as_int.is_some() {
            exemplar_json.insert(
                "exemplar_as_int".to_string(),
                Value::String(exemplar.as_int.as_ref().unwrap().to_string()),
            );
        }
    }
    exemplar_json
}

fn flatten_gauge(gauge: &Gauge) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json: Vec<BTreeMap<String, Value>> = Vec::new();
    if let Some(data_points) = gauge.data_points.as_ref() {
        for data_point in data_points.iter() {
            let mut data_point_json: BTreeMap<String, Value> = BTreeMap::new();

            if let Some(attributes) = data_point.attributes.as_ref() {
                let attributes_json = flatten_attributes(attributes);
                for key in attributes_json.keys() {
                    data_point_json.insert(
                        format!("gauge_{}", key.to_owned()),
                        attributes_json[key].to_owned(),
                    );
                }
            }

            if data_point.start_time_unix_nano.is_some() {
                data_point_json.insert(
                    "gauge_start_time_unix_nano".to_string(),
                    Value::String(
                        data_point
                            .start_time_unix_nano
                            .as_ref()
                            .unwrap()
                            .to_string(),
                    ),
                );
            }

            if data_point.time_unix_nano.is_some() {
                data_point_json.insert(
                    "gauge_time_unix_nano".to_string(),
                    Value::String(data_point.time_unix_nano.as_ref().unwrap().to_string()),
                );
            }

            if let Some(exemplars) = data_point.exemplars.as_ref() {
                let exemplar_json = flatten_exemplar(exemplars);
                for key in exemplar_json.keys() {
                    data_point_json.insert(
                        format!("gauge_{}", key.to_owned()),
                        exemplar_json[key].to_owned(),
                    );
                }
            }

            if data_point.flags.is_some() {
                data_point_json.insert(
                    "gauge_data_point_flags".to_string(),
                    Value::Number(serde_json::Number::from(data_point.flags.unwrap())),
                );
            }

            if data_point.as_double.is_some() {
                data_point_json.insert(
                    "gauge_data_point_as_double".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(data_point.as_double.unwrap()).unwrap(),
                    ),
                );
            }

            if data_point.as_int.is_some() {
                data_point_json.insert(
                    "gauge_data_point_as_int".to_string(),
                    Value::String(data_point.as_int.as_ref().unwrap().to_string()),
                );
            }

            data_points_json.push(data_point_json.clone());
        }
    }
    data_points_json
}

fn flatten_sum(sum: &Sum) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json: Vec<BTreeMap<String, Value>> = Vec::new();
    if let Some(data_points) = sum.data_points.as_ref() {
        for data_point in data_points.iter() {
            let mut data_point_json: BTreeMap<String, Value> = BTreeMap::new();

            if let Some(attributes) = data_point.attributes.as_ref() {
                let attributes_json = flatten_attributes(attributes);
                for key in attributes_json.keys() {
                    data_point_json.insert(
                        format!("sum_{}", key.to_owned()),
                        attributes_json[key].to_owned(),
                    );
                }
            }

            if data_point.start_time_unix_nano.is_some() {
                data_point_json.insert(
                    "sum_start_time_unix_nano".to_string(),
                    Value::String(
                        data_point
                            .start_time_unix_nano
                            .as_ref()
                            .unwrap()
                            .to_string(),
                    ),
                );
            }

            if data_point.time_unix_nano.is_some() {
                data_point_json.insert(
                    "sum_time_unix_nano".to_string(),
                    Value::String(data_point.time_unix_nano.as_ref().unwrap().to_string()),
                );
            }

            if let Some(exemplars) = data_point.exemplars.as_ref() {
                let exemplar_json = flatten_exemplar(exemplars);
                for key in exemplar_json.keys() {
                    data_point_json.insert(
                        format!("sum_{}", key.to_owned()),
                        exemplar_json[key].to_owned(),
                    );
                }
            }

            if data_point.flags.is_some() {
                data_point_json.insert(
                    "sum_data_point_flags".to_string(),
                    Value::Number(serde_json::Number::from(data_point.flags.unwrap())),
                );
            }

            if data_point.as_double.is_some() {
                data_point_json.insert(
                    "sum_data_point_as_double".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(data_point.as_double.unwrap()).unwrap(),
                    ),
                );
            }

            if data_point.as_int.is_some() {
                data_point_json.insert(
                    "sum_data_point_as_int".to_string(),
                    Value::String(data_point.as_int.as_ref().unwrap().to_string()),
                );
            }

            data_points_json.push(data_point_json.clone());
        }
    }
    data_points_json
}

fn flatten_histogram(histogram: &Histogram) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json: Vec<BTreeMap<String, Value>> = Vec::new();
    if let Some(histogram_data_points) = histogram.data_points.as_ref() {
        for data_point in histogram_data_points.iter() {
            let mut data_point_json: BTreeMap<String, Value> = BTreeMap::new();

            if let Some(attributes) = data_point.attributes.as_ref() {
                let attributes_json = flatten_attributes(attributes);
                for key in attributes_json.keys() {
                    data_point_json.insert(
                        format!("histogram_{}", key.to_owned()),
                        attributes_json[key].to_owned(),
                    );
                }
            }

            if data_point.start_time_unix_nano.is_some() {
                data_point_json.insert(
                    "histogram_start_time_unix_nano".to_string(),
                    Value::String(
                        data_point
                            .start_time_unix_nano
                            .as_ref()
                            .unwrap()
                            .to_string(),
                    ),
                );
            }

            if data_point.time_unix_nano.is_some() {
                data_point_json.insert(
                    "histogram_time_unix_nano".to_string(),
                    Value::String(data_point.time_unix_nano.as_ref().unwrap().to_string()),
                );
            }

            if data_point.count.is_some() {
                data_point_json.insert(
                    "histogram_data_point_count".to_string(),
                    Value::String(data_point.count.as_ref().unwrap().to_string()),
                );
            }

            if data_point.sum.is_some() {
                data_point_json.insert(
                    "histogram_data_point_sum".to_string(),
                    Value::Number(serde_json::Number::from_f64(data_point.sum.unwrap()).unwrap()),
                );
            }

            if let Some(bucket_counts) = data_point.bucket_counts.as_ref() {
                let mut index = 1;
                for bucket_count in bucket_counts.iter() {
                    data_point_json.insert(
                        format!("histogram_data_point_bucket_count_{}", index),
                        Value::String(bucket_count.to_string()),
                    );
                    index += 1;
                }
            }

            if let Some(explicit_bounds) = data_point.explicit_bounds.as_ref() {
                let mut index = 1;
                for explicit_bound in explicit_bounds.iter() {
                    data_point_json.insert(
                        format!("histogram_data_point_explicit_bound_{}", index),
                        Value::String(explicit_bound.to_string()),
                    );
                    index += 1;
                }
            }
            if let Some(exemplars) = data_point.exemplars.as_ref() {
                let exemplar_json = flatten_exemplar(exemplars);
                for key in exemplar_json.keys() {
                    data_point_json.insert(
                        format!("histogram_{}", key.to_owned()),
                        exemplar_json[key].to_owned(),
                    );
                }
            }

            if data_point.flags.is_some() {
                data_point_json.insert(
                    "histogram_data_point_flags".to_string(),
                    Value::Number(serde_json::Number::from(data_point.flags.unwrap())),
                );
            }

            if data_point.min.is_some() {
                data_point_json.insert(
                    "histogram_data_point_min".to_string(),
                    Value::Number(serde_json::Number::from_f64(data_point.min.unwrap()).unwrap()),
                );
            }

            if data_point.max.is_some() {
                data_point_json.insert(
                    "histogram_data_point_max".to_string(),
                    Value::Number(serde_json::Number::from_f64(data_point.max.unwrap()).unwrap()),
                );
            }

            data_points_json.push(data_point_json.clone());
        }
    }
    if histogram.aggregation_temporality.is_some() {
        for data_point_json in data_points_json.iter_mut() {
            data_point_json.insert(
                "histogram_aggregation_temporality".to_string(),
                Value::String(
                    histogram
                        .aggregation_temporality
                        .as_ref()
                        .unwrap()
                        .to_string(),
                ),
            );
        }
    }

    data_points_json
}

fn flatten_buckets(bucket: &Buckets) -> BTreeMap<String, Value> {
    let mut bucket_json = BTreeMap::new();
    if bucket.offset.is_some() {
        bucket_json.insert(
            "offset".to_string(),
            Value::Number(serde_json::Number::from_f64(bucket.offset.unwrap() as f64).unwrap()),
        );
    }

    if let Some(bucket_counts) = bucket.bucket_counts.as_ref() {
        let mut index = 1;
        for bucket_count in bucket_counts.iter() {
            bucket_json.insert(
                format!("bucket_count_{}", index),
                Value::String(bucket_count.to_string()),
            );
            index += 1;
        }
    }

    bucket_json
}

fn flatten_exp_histogram(exp_histogram: &ExponentialHistogram) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json: Vec<BTreeMap<String, Value>> = Vec::new();
    if let Some(exp_histogram_data_points) = exp_histogram.data_points.as_ref() {
        for data_point in exp_histogram_data_points.iter() {
            let mut data_point_json: BTreeMap<String, Value> = BTreeMap::new();

            if let Some(attributes) = data_point.attributes.as_ref() {
                let attributes_json = flatten_attributes(attributes);
                for key in attributes_json.keys() {
                    data_point_json.insert(
                        format!("exponential_histogram_{}", key.to_owned()),
                        attributes_json[key].to_owned(),
                    );
                }
            }

            if data_point.start_time_unix_nano.is_some() {
                data_point_json.insert(
                    "exponential_histogram_start_time_unix_nano".to_string(),
                    Value::String(
                        data_point
                            .start_time_unix_nano
                            .as_ref()
                            .unwrap()
                            .to_string(),
                    ),
                );
            }

            if data_point.time_unix_nano.is_some() {
                data_point_json.insert(
                    "exponential_histogram_time_unix_nano".to_string(),
                    Value::String(data_point.time_unix_nano.as_ref().unwrap().to_string()),
                );
            }

            if data_point.count.is_some() {
                data_point_json.insert(
                    "exponential_histogram_data_point_count".to_string(),
                    Value::Number(serde_json::Number::from(data_point.count.unwrap())),
                );
            }

            if data_point.sum.is_some() {
                data_point_json.insert(
                    "exponential_histogram_data_point_sum".to_string(),
                    Value::Number(serde_json::Number::from_f64(data_point.sum.unwrap()).unwrap()),
                );
            }

            if data_point.scale.is_some() {
                data_point_json.insert(
                    "exponential_histogram_data_point_scale".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(data_point.scale.unwrap() as f64).unwrap(),
                    ),
                );
            }

            if data_point.zero_count.is_some() {
                data_point_json.insert(
                    "exponential_histogram_data_point_zero_count".to_string(),
                    Value::Number(serde_json::Number::from(data_point.zero_count.unwrap())),
                );
            }

            if let Some(positive) = data_point.positive.as_ref() {
                let positive_json = flatten_buckets(positive);
                for key in positive_json.keys() {
                    data_point_json.insert(
                        format!("exponential_histogram_positive_{}", key),
                        positive_json[key].to_owned(),
                    );
                }
            }

            if let Some(negative) = data_point.negative.as_ref() {
                let negative_json = flatten_buckets(negative);
                for key in negative_json.keys() {
                    data_point_json.insert(
                        format!("exponential_histogram_negative_{}", key),
                        negative_json[key].to_owned(),
                    );
                }
            }

            if data_point.flags.is_some() {
                data_point_json.insert(
                    "exponential_histogram_data_point_flags".to_string(),
                    Value::Number(serde_json::Number::from(data_point.flags.unwrap())),
                );
            }

            if let Some(exemplars) = data_point.exemplars.as_ref() {
                let exemplar_json = flatten_exemplar(exemplars);
                for key in exemplar_json.keys() {
                    data_point_json.insert(
                        format!("exponential_histogram_{}", key.to_owned()),
                        exemplar_json[key].to_owned(),
                    );
                }
            }
            if data_point.min.is_some() {
                data_point_json.insert(
                    "exponential_histogram_data_point_min".to_string(),
                    Value::Number(serde_json::Number::from_f64(data_point.min.unwrap()).unwrap()),
                );
            }

            if data_point.max.is_some() {
                data_point_json.insert(
                    "exponential_histogram_data_point_max".to_string(),
                    Value::Number(serde_json::Number::from_f64(data_point.max.unwrap()).unwrap()),
                );
            }

            if data_point.zero_threshold.is_some() {
                data_point_json.insert(
                    "exponential_histogram_data_point_zero_threshold".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(data_point.zero_threshold.unwrap()).unwrap(),
                    ),
                );
            }

            data_points_json.push(data_point_json.clone());
        }
    }

    data_points_json
}

fn flatten_summary(summary: &Summary) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json: Vec<BTreeMap<String, Value>> = Vec::new();
    if let Some(summary_data_points) = summary.data_points.as_ref() {
        for data_point in summary_data_points.iter() {
            let mut data_point_json: BTreeMap<String, Value> = BTreeMap::new();

            if let Some(attributes) = data_point.attributes.as_ref() {
                let attributes_json = flatten_attributes(attributes);
                for key in attributes_json.keys() {
                    data_point_json.insert(
                        format!("summary_{}", key.to_owned()),
                        attributes_json[key].to_owned(),
                    );
                }
            }

            if data_point.start_time_unix_nano.is_some() {
                data_point_json.insert(
                    "summary_start_time_unix_nano".to_string(),
                    Value::String(
                        data_point
                            .start_time_unix_nano
                            .as_ref()
                            .unwrap()
                            .to_string(),
                    ),
                );
            }

            if data_point.time_unix_nano.is_some() {
                data_point_json.insert(
                    "summary_time_unix_nano".to_string(),
                    Value::String(data_point.time_unix_nano.as_ref().unwrap().to_string()),
                );
            }

            if data_point.count.is_some() {
                data_point_json.insert(
                    "summary_data_point_count".to_string(),
                    Value::Number(serde_json::Number::from(data_point.count.unwrap())),
                );
            }

            if data_point.sum.is_some() {
                data_point_json.insert(
                    "summary_data_point_sum".to_string(),
                    Value::Number(serde_json::Number::from_f64(data_point.sum.unwrap()).unwrap()),
                );
            }

            if let Some(quantile_values) = data_point.quantile_values.as_ref() {
                for quantile_value in quantile_values.iter() {
                    if quantile_value.quantile.is_some() {
                        data_point_json.insert(
                            "summary_quantile_value_quantile".to_string(),
                            Value::Number(
                                serde_json::Number::from_f64(quantile_value.quantile.unwrap())
                                    .unwrap(),
                            ),
                        );
                    }

                    if quantile_value.value.is_some() {
                        data_point_json.insert(
                            "summary_quantile_value_value".to_string(),
                            Value::Number(
                                serde_json::Number::from_f64(quantile_value.value.unwrap())
                                    .unwrap(),
                            ),
                        );
                    }
                }
            }

            if data_point.flags.is_some() {
                data_point_json.insert(
                    "summary_data_point_flags".to_string(),
                    Value::Number(serde_json::Number::from(data_point.flags.unwrap())),
                );
            }

            data_points_json.push(data_point_json.clone());
        }
    }
    data_points_json
}

pub fn flatten_metrics_record(metrics_record: &Metric) -> Vec<BTreeMap<String, Value>> {
    let mut data_points_json: Vec<BTreeMap<String, Value>> = Vec::new();

    if let Some(gauge) = metrics_record.gauge.as_ref() {
        let gauge_json = flatten_gauge(gauge);
        for gauge_json in gauge_json.iter() {
            data_points_json.push(gauge_json.clone());
        }
    }

    if let Some(sum) = metrics_record.sum.as_ref() {
        let sum_json = flatten_sum(sum);
        for sum_json in sum_json.iter() {
            data_points_json.push(sum_json.clone());
        }
    }

    if let Some(histogram) = metrics_record.histogram.as_ref() {
        let histogram_json = flatten_histogram(histogram);
        for histogram_json in histogram_json.iter() {
            data_points_json.push(histogram_json.clone());
        }
    }

    if let Some(exp_histogram) = metrics_record.exponential_histogram.as_ref() {
        let exp_histogram_json = flatten_exp_histogram(exp_histogram);
        for exp_histogram_json in exp_histogram_json.iter() {
            data_points_json.push(exp_histogram_json.clone());
        }
    }

    if let Some(summary) = metrics_record.summary.as_ref() {
        let summary_json = flatten_summary(summary);
        for summary_json in summary_json.iter() {
            data_points_json.push(summary_json.clone());
        }
    }

    if metrics_record.name.is_some() {
        for data_point_json in data_points_json.iter_mut() {
            data_point_json.insert(
                "metric_name".to_string(),
                Value::String(metrics_record.name.as_ref().unwrap().to_string()),
            );
        }
    }

    if metrics_record.description.is_some() {
        for data_point_json in data_points_json.iter_mut() {
            data_point_json.insert(
                "metric_description".to_string(),
                Value::String(metrics_record.description.as_ref().unwrap().to_string()),
            );
        }
    }

    if metrics_record.unit.is_some() {
        for data_point_json in data_points_json.iter_mut() {
            data_point_json.insert(
                "metric_unit".to_string(),
                Value::String(metrics_record.unit.as_ref().unwrap().to_string()),
            );
        }
    }

    if let Some(metadata) = metrics_record.metadata.as_ref() {
        let metadata_json = flatten_attributes(metadata);
        for key in metadata_json.keys() {
            for data_point_json in data_points_json.iter_mut() {
                data_point_json.insert(key.to_owned(), metadata_json[key].to_owned());
            }
        }
    }
    data_points_json
}
