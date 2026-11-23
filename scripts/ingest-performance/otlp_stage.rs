//! Body bytes to flattened OTLP rows, using the server's default decoding options.
use std::{env, fs, hint::black_box, io};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    logs::v1::LogsData,
    metrics::v1::MetricsData,
    trace::v1::TracesData,
};
use parseable::{
    otel::{
        logs::{flatten_otel_logs, flatten_otel_protobuf},
        metrics::{flatten_otel_metrics_protobuf_with_options, flatten_otel_metrics_with_options},
        otel_utils::deserialize_otel_json,
        traces::{flatten_otel_traces, flatten_otel_traces_protobuf},
    },
    parseable::DEFAULT_TENANT,
    utils::json::flatten::{FlattenBudget, FlattenLimits},
};
use prost::Message as _;
use serde::de::DeserializeOwned;
use serde_json::Value;

#[cfg(all(not(target_env = "msvc"), not(feature = "hotpath")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn decode_json<T: DeserializeOwned>(body: &[u8]) -> T {
    deserialize_otel_json(serde_json::from_slice(body).unwrap(), false).unwrap()
}

fn stage(signal: &str, encoding: &str, body: &[u8]) -> Vec<Value> {
    let mut budget = FlattenBudget::new(FlattenLimits::DEFAULT);
    match (signal, encoding) {
        ("logs", "json") => {
            flatten_otel_logs(&decode_json::<LogsData>(body), DEFAULT_TENANT, &mut budget).unwrap()
        }
        ("logs", "protobuf") => flatten_otel_protobuf(
            &ExportLogsServiceRequest::decode(body).unwrap(),
            DEFAULT_TENANT,
            &mut budget,
        )
        .unwrap(),
        ("metrics", "json") => flatten_otel_metrics_with_options(
            decode_json::<MetricsData>(body),
            DEFAULT_TENANT,
            false,
        ),
        ("metrics", "protobuf") => flatten_otel_metrics_protobuf_with_options(
            &ExportMetricsServiceRequest::decode(body).unwrap(),
            DEFAULT_TENANT,
            false,
        ),
        ("traces", "json") => flatten_otel_traces(&decode_json::<TracesData>(body), DEFAULT_TENANT),
        ("traces", "protobuf") => flatten_otel_traces_protobuf(
            &ExportTraceServiceRequest::decode(body).unwrap(),
            DEFAULT_TENANT,
        ),
        _ => panic!("expected logs, metrics or traces and json or protobuf"),
    }
}

fn main() {
    assert!(!cfg!(debug_assertions), "build otlp_stage with --release");
    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [signal, mode, json, protobuf, rows] if mode == "check" => {
            let from_json = stage(signal, "json", &fs::read(json).unwrap());
            let from_protobuf = stage(signal, "protobuf", &fs::read(protobuf).unwrap());
            assert_eq!(from_json.len(), rows.parse::<usize>().unwrap());
            assert!(from_json == from_protobuf, "{signal} flattened rows differ");
        }
        [signal, encoding, body, iterations] => {
            let body = fs::read(body).unwrap();
            let iterations = iterations.parse::<usize>().unwrap();
            assert!(iterations > 0);
            let mut command = String::new();
            let input = io::stdin();
            println!("ready");
            input.read_line(&mut command).unwrap();
            assert_eq!(command.trim_end(), "start");
            for _ in 0..iterations {
                black_box(stage(signal, encoding, black_box(&body)));
            }
            println!("done");
            // Stay alive until the caller has disabled and read the counters.
            command.clear();
            assert_eq!(input.read_line(&mut command).unwrap(), 0);
        }
        _ => panic!("otlp_stage SIGNAL (check JSON PROTOBUF ROWS | ENCODING BODY ITERATIONS)"),
    }
}
