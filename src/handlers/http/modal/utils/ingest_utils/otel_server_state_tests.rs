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

//! Regression tests of OTLP ingestion that read the server state (`PARSEABLE`).
//! Each first sets up the global server state as the metastore contract tests do
//! (`CONTRACT_PARSEABLE_FACTORY`: default options, local storage in a temporary
//! directory under `target`), so the configuration is never parsed from the test
//! binary's command line and the tests may be run with a name filter. Each prints
//! `REGRESSION <name>: ...`.

use super::*;
use crate::metadata::{LogStreamMetadata, SchemaVersion};
use actix_web::test::TestRequest;
use arrow_schema::{DataType, Field};
use std::sync::Arc;

/// Sets up `PARSEABLE` for tests, unless a test already did
fn initialize_server() {
    fn factory() -> crate::parseable::Parseable {
        use crate::storage::object_storage::ObjectStorageProvider;
        let root = tempfile::tempdir_in(concat!(env!("CARGO_MANIFEST_DIR"), "/target"))
            .unwrap()
            .keep();
        let provider = Arc::new(crate::storage::FSConfig { root });
        let storage = provider.construct_client();
        let metastore = Arc::new(
            crate::metastore::metastores::object_store_metastore::ObjectStoreMetastore {
                storage: storage.clone(),
            },
        );
        crate::parseable::Parseable::new(
            crate::cli::Options::default(),
            #[cfg(feature = "kafka")]
            <crate::connectors::kafka::config::KafkaConfig as clap::Parser>::parse_from(["otel"]),
            provider,
            metastore,
            storage,
        )
    }
    let _ = crate::parseable::CONTRACT_PARSEABLE_FACTORY.set(factory);
    once_cell::sync::Lazy::force(&PARSEABLE);
}

const SIGNALS: [(TelemetryType, LogSource); 3] = [
    (TelemetryType::Logs, LogSource::OtelLogs),
    (TelemetryType::Metrics, LogSource::OtelMetrics),
    (TelemetryType::Traces, LogSource::OtelTraces),
];

/// Review finding 7d: an empty request must be answered 200 without the stream,
/// in both encodings and for every signal. Collector setup scripts detect
/// protobuf support by posting an empty protobuf body to `/v1/metrics`.
#[tokio::test]
async fn empty_requests_are_answered_200_without_a_stream() {
    initialize_server();
    for (telemetry_type, log_source) in SIGNALS {
        for (content_type, body) in [
            ("application/x-protobuf", &b""[..]),
            ("application/json", &b"{}"[..]),
        ] {
            let req = TestRequest::default()
                .insert_header((CONTENT_TYPE, content_type))
                .to_http_request();
            let ingested = process_otel_content(
                &req,
                web::Bytes::from_static(body),
                "otel_regression_no_such_stream",
                &log_source,
                telemetry_type,
            )
            .await;
            println!(
                "REGRESSION empty request, {log_source}, {content_type}: {:?}",
                ingested.as_ref().map_err(|e| e.to_string())
            );
            let ingested = ingested.expect("an empty request is accepted");
            assert_eq!(ingested, OtelIngested::Full);
            let response = otel_response(&req, telemetry_type, &ingested);
            assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        }
    }
}

/// Review finding 7a: for OTLP JSON the dataset field count is checked before
/// the body is decoded into a request, as at `b8c7d979`: a body that is not a
/// request, sent to a dataset over its field limit, is refused for the limit.
#[tokio::test]
async fn otlp_json_checks_the_field_count_before_decoding() {
    initialize_server();
    let stream = "otel_regression_fields_limit";
    let limit = PARSEABLE.options.dataset_fields_allowed_limit;
    let schema = (0..=limit)
        .map(|i| {
            let name = format!("f{i}");
            (
                name.clone(),
                Arc::new(Field::new(name, DataType::Utf8, true)),
            )
        })
        .collect();
    PARSEABLE.streams.get_or_create(
        PARSEABLE.options.clone(),
        stream.to_string(),
        LogStreamMetadata {
            schema,
            schema_version: SchemaVersion::V1,
            ..Default::default()
        },
        None,
        &None,
    );
    for (telemetry_type, log_source, body) in [
        (
            TelemetryType::Logs,
            LogSource::OtelLogs,
            r#"{"resourceLogs":5}"#,
        ),
        (
            TelemetryType::Metrics,
            LogSource::OtelMetrics,
            r#"{"resourceMetrics":5}"#,
        ),
        (
            TelemetryType::Traces,
            LogSource::OtelTraces,
            r#"{"resourceSpans":5}"#,
        ),
    ] {
        let req = TestRequest::default()
            .insert_header((CONTENT_TYPE, "application/json"))
            .to_http_request();
        let refused = process_otel_content(
            &req,
            web::Bytes::from_static(body.as_bytes()),
            stream,
            &log_source,
            telemetry_type,
        )
        .await
        .expect_err("refused");
        println!("REGRESSION field count before decoding, {log_source}: {refused}");
        assert!(
            matches!(refused, PostError::FieldsCountLimitExceeded(..)),
            "{log_source}: {refused:?}"
        );
    }
    PARSEABLE.streams.delete(stream, &None);
}
