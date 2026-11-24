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
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

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
    StageLimits::for_body(body_len, FlattenLimits::DEFAULT)
}

/// Review finding 8: the expansion cap was 1024 x `P_MAX_EVENT_PAYLOAD_SIZE`
/// (10 GiB) whatever the size of the body, so a body of a few KiB could stage
/// gigabytes of rows. It is now 4096 x the length of the body, with a floor.
#[test]
fn expansion_cap_is_4096_times_the_body_with_a_floor() {
    use crate::otel::sink::{EXPANSION_FACTOR, MIN_EXPANDED_BYTES};
    assert_eq!(EXPANSION_FACTOR, 4096);
    assert_eq!(max_expanded_bytes(0), MIN_EXPANDED_BYTES);
    assert_eq!(max_expanded_bytes(1000), MIN_EXPANDED_BYTES);
    assert_eq!(max_expanded_bytes(1 << 20), 4u64 << 30);
    assert_eq!(max_expanded_bytes(PAYLOAD), 40u64 << 30);
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
