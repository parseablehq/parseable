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

//! Regression tests for what two reviews of chunked OTLP staging found, at the
//! level of one request. Each prints `REGRESSION <name>: ...`. None touches
//! `PARSEABLE`, so they may be run with the filter `otel_regression_tests`; the
//! ones that need the server are in `otel_server_state_tests`.

use super::*;
use crate::metadata::SchemaVersion;
use crate::otel::sink::RowSink;
use crate::utils::json::flatten::JsonFlattenError;
use arrow_schema::Field;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use serde_json::{Map, json};
use std::collections::BTreeMap;
use std::sync::Arc;

const MIB: usize = 1024 * 1024;

/// `P_MAX_EVENT_PAYLOAD_SIZE` default
const PAYLOAD: usize = 10 * MIB;

const OPTIONS: ProtobufOptions = ProtobufOptions {
    max_payload: PAYLOAD,
    flatten_exemplars: false,
};

fn kv(key: &str, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

fn target<'a>(
    log_source: &'a LogSource,
    p_custom_fields: &'a HashMap<String, String>,
) -> OtelTarget<'a> {
    OtelTarget {
        stream_name: "otel_regression",
        log_source,
        p_custom_fields,
        telemetry_type: TelemetryType::Logs,
        tenant_id: &None,
    }
}

/// The staging limits ingestion uses for a body of `body_len` bytes
fn limits_for(body_len: usize) -> StageLimits {
    StageLimits::for_body(body_len, PAYLOAD, FlattenLimits::DEFAULT)
}

/// Review finding 8: the expansion cap was 1024 x `P_MAX_EVENT_PAYLOAD_SIZE`
/// (10 GiB) whatever the size of the body, so a body of a few KiB could stage
/// gigabytes of rows. It is now 4096 x the length of the body, with a floor, and
/// that 10 GiB (1024 x the payload limit) is kept as a ceiling, which decides for
/// bodies over a quarter of the limit: without it a body of the full limit could
/// stage 40 GiB of rows.
#[test]
fn expansion_cap_is_4096_times_the_body_with_a_floor_and_a_ceiling() {
    use crate::otel::sink::{EXPANSION_CEILING_FACTOR, EXPANSION_FACTOR, MIN_EXPANDED_BYTES};
    assert_eq!(EXPANSION_FACTOR, 4096);
    assert_eq!(EXPANSION_CEILING_FACTOR, 1024);
    assert_eq!(max_expanded_bytes(0, PAYLOAD), MIN_EXPANDED_BYTES);
    assert_eq!(max_expanded_bytes(1000, PAYLOAD), MIN_EXPANDED_BYTES);
    assert_eq!(max_expanded_bytes(1 << 20, PAYLOAD), 4u64 << 30);
    assert_eq!(max_expanded_bytes(PAYLOAD / 4, PAYLOAD), 10u64 << 30);
    assert_eq!(max_expanded_bytes(PAYLOAD / 4 + 1, PAYLOAD), 10u64 << 30);
    assert_eq!(max_expanded_bytes(PAYLOAD, PAYLOAD), 10u64 << 30);
    // The ceiling follows the payload limit, and is never under the floor
    assert_eq!(max_expanded_bytes(4 * PAYLOAD, 4 * PAYLOAD), 40u64 << 30);
    assert_eq!(max_expanded_bytes(usize::MAX, usize::MAX), u64::MAX);
    assert_eq!(max_expanded_bytes(1 << 20, 1024), MIN_EXPANDED_BYTES);
    assert_eq!(max_expanded_bytes(1 << 20, 0), MIN_EXPANDED_BYTES);
}

/// Review finding 8, as a request: a body of 47 KiB whose rows expand past 4096x
/// its length is cut off by the cap (it passed whole under the 10 GiB cap)
#[test]
fn a_small_body_expanding_past_4096_times_its_length_is_refused() {
    // 100 resource attributes over records of two bytes each: their rows hold
    // about 460 MB, past 4096 x the body's length but far under 10 GiB
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: (0..100)
                    .map(|i| kv(&format!("k8s.pod.label.{i:03}"), format!("{i:032}")))
                    .collect(),
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord::default(); 20_000],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let body = web::Bytes::from(request.encode_to_vec());
    let limits = limits_for(body.len());
    let fields = HashMap::new();
    let mut staged_bytes = 0u64;
    let (outcome, staged) = stage_otel_protobuf(
        body.clone(),
        &target(&LogSource::OtelLogs, &fields),
        OPTIONS,
        limits,
        |rows| {
            staged_bytes += rows
                .iter()
                .map(|row| crate::otel::sink::row_bytes(row.as_object().unwrap()) as u64)
                .sum::<u64>();
            Ok(())
        },
    );
    println!(
        "REGRESSION expansion cap: body {} B, cap {} B, staged {} of 20000 rows ({} B), outcome {:?}",
        body.len(),
        limits.max_expanded,
        staged.rows,
        staged_bytes,
        outcome.as_ref().map_err(|e| e.to_string())
    );
    assert!(
        matches!(&outcome, Ok(OtelIngested::Partial { rejected, .. }) if *rejected > 0),
        "{outcome:?}"
    );
    assert!(staged_bytes <= limits.max_expanded);
}

/// Columns staged for `rows`, as one batch or in chunks of `chunk` rows, each
/// chunk typed against the schema the chunks before it committed, as a stream's
/// schema is
fn staged_columns(rows: &[Value], chunk: usize) -> Vec<(String, String, Vec<String>)> {
    use crate::event::format::{EventFormat as _, json as json_format};
    let mut storage: HashMap<String, Arc<Field>> = HashMap::new();
    let mut cells: BTreeMap<String, (String, Vec<String>)> = BTreeMap::new();
    for part in rows.chunks(chunk) {
        let (rb, _) = json_format::Event {
            json: Value::Array(part.to_vec()),
            p_timestamp: chrono::Utc::now(),
        }
        .into_recordbatch(
            &storage,
            false,
            None,
            SchemaVersion::V1,
            &HashMap::new(),
            true,
        )
        .unwrap();
        for (i, field) in rb.schema().fields().iter().enumerate() {
            if field.name() == "p_timestamp" {
                continue;
            }
            storage.insert(field.name().clone(), field.clone());
            let column = rb.column(i);
            let entry = cells
                .entry(field.name().clone())
                .or_insert_with(|| (format!("{:?}", field.data_type()), Vec::new()));
            for row in 0..rb.num_rows() {
                entry.1.push(
                    arrow::util::display::array_value_to_string(column, row).unwrap_or_default(),
                );
            }
        }
    }
    cells
        .into_iter()
        .map(|(name, (ty, values))| (name, ty, values))
        .collect()
}

/// Review finding 3, kept as documented behaviour: each chunk of a request is
/// typed against the schema the chunk before it committed, so a value whose type
/// changes between the records of a request that is cut into chunks can land in
/// a differently named column than one batch would put it in, exactly as two
/// separate requests would. Requests that fit in one chunk are not affected.
#[test]
fn a_type_change_between_chunks_lands_where_a_second_request_would_put_it() {
    let columns = |columns: &[(String, String, Vec<String>)]| {
        columns
            .iter()
            .map(|(name, ty, values)| format!("{name}:{ty}:{}", values.join("|")))
            .collect::<Vec<_>>()
    };
    let cases = [
        (
            vec![json!({"x": true, "n": 1}), json!({"x": "on", "n": 2})],
            ["n:Float64:1.0|2.0", "x:Utf8:|on", "x_bool:Boolean:true|"],
            ["n:Float64:1.0|2.0", "x:Boolean:true", "x_utf8:Utf8:on"],
        ),
        (
            vec![
                json!({"http_status_code": 200, "n": 1}),
                json!({"http_status_code": "OK", "n": 2}),
            ],
            [
                "http_status_code:Utf8:|OK",
                "http_status_code_int64:Float64:200.0|",
                "n:Float64:1.0|2.0",
            ],
            [
                "http_status_code:Float64:200.0",
                "http_status_code_utf8:Utf8:OK",
                "n:Float64:1.0|2.0",
            ],
        ),
    ];
    for (rows, one_batch, two_chunks) in cases {
        let whole = columns(&staged_columns(&rows, rows.len()));
        let chunked = columns(&staged_columns(&rows, 1));
        println!("REGRESSION type change: one batch {whole:?}, two chunks {chunked:?}");
        assert_eq!(whole, one_batch);
        assert_eq!(chunked, two_chunks);
    }
}

/// Security review finding 2: the sizing pass charged the Prometheus `job` and
/// `instance` labels at 32 bytes, but they hold copies of `service.name` and
/// `service.instance.id`. There is no sizing pass any more: the running total is
/// the `row_bytes` of every staged row, labels included at their real size, so a
/// request whose labels are long is refused where its rows pass the cap.
#[test]
fn expansion_total_counts_the_prometheus_identity_labels_at_their_size() {
    const LABEL: usize = 64 * 1024;
    let label = LABEL;
    let points = 13_000usize;
    let resource_metrics = vec![ResourceMetrics {
        resource: Some(Resource {
            attributes: vec![kv("service.name", "n".repeat(label))],
            ..Default::default()
        }),
        scope_metrics: vec![ScopeMetrics {
            metrics: vec![Metric {
                name: "m".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: (0..points)
                        .map(|i| NumberDataPoint {
                            time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
                            start_time_unix_nano: 1_700_000_000_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(1.0)),
                            ..Default::default()
                        })
                        .collect(),
                })),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }];

    /// Keeps the `row_bytes` of the first row
    #[derive(Default)]
    struct First(Option<usize>);
    impl RowSink for First {
        type Error = JsonFlattenError;
        fn row(&mut self, row: Map<String, Value>) -> Result<(), Self::Error> {
            assert_eq!(row["job"], json!("n".repeat(LABEL)));
            self.0.get_or_insert(crate::otel::sink::row_bytes(&row));
            Err(JsonFlattenError::CannotFlatten)
        }
    }
    let mut first = First::default();
    let _ = flatten_metrics_into(&resource_metrics, false, &mut first);
    let per_row = first.0.unwrap() as u64;

    let max_expanded = 1u64 << 30;
    let limits = StageLimits {
        chunk: CHUNK_LIMITS,
        max_expanded,
        flatten: FlattenLimits::DEFAULT,
    };
    let mut staged_bytes = 0u64;
    let (outcome, staged) = stage_otel_resources(
        OtelResources::Metrics(&resource_metrics),
        false,
        limits,
        |rows| {
            staged_bytes += rows
                .iter()
                .map(|row| crate::otel::sink::row_bytes(row.as_object().unwrap()) as u64)
                .sum::<u64>();
            Ok(())
        },
    );
    println!(
        "REGRESSION identity labels: {per_row} row bytes per row ({label} B label twice), staged {} of {points} rows, {staged_bytes} B, cap {max_expanded} B, outcome {:?}",
        staged.rows,
        outcome.as_ref().map_err(|e| e.to_string())
    );
    // `service.name` and its copy in `job`, each at its real size
    assert!(per_row > 2 * label as u64);
    assert!(matches!(outcome, Ok(OtelIngested::Partial { .. })));
    assert!(staged_bytes <= max_expanded);
    // Stopped by the cap, not before it: one more chunk would have passed it
    assert!(staged_bytes + CHUNK_LIMITS.bytes as u64 + per_row > max_expanded);
}
