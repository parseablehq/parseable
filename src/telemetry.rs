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

use opentelemetry_otlp::Protocol;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{BatchSpanProcessor, SdkTracerProvider},
};

const EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const EXPORTER_OTLP_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";

/// Initialise an OTLP tracer provider.
///
/// **Required env var:**
/// - `OTEL_EXPORTER_OTLP_ENDPOINT` — collector address.
///   For HTTP exporters the SDK appends the signal path automatically:
///   e.g. `http://localhost:4318` → `http://localhost:4318/v1/traces`.
///   Set a signal-specific var `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to
///   supply a full URL without any path suffix being added.
///
/// **Optional env vars (all read by the SDK automatically):**
/// - `OTEL_EXPORTER_OTLP_PROTOCOL` — transport + serialisation (default: `http/json`):
///   - `grpc`           → gRPC / tonic  (Jaeger, Tempo, …)
///   - `http/json`      → HTTP + JSON   (Parseable OSS ingest at `/v1/traces`)
///   - `http/protobuf`  → HTTP + protobuf
/// - `OTEL_EXPORTER_OTLP_HEADERS` — comma-separated `key=value` pairs forwarded
///   as gRPC metadata or HTTP headers, e.g.
///   `authorization=Basic <token>,x-p-stream=my-stream,x-p-log-source=otel-traces`
///
/// Returns `None` when `OTEL_EXPORTER_OTLP_ENDPOINT` is not set (OTEL disabled).
/// The caller must call `provider.shutdown()` before process exit.
pub fn init_otel_tracer() -> Option<SdkTracerProvider> {
    // Only used to decide whether OTEL is enabled; the SDK reads it again
    // from env to build the exporter (which also appends /v1/traces for HTTP).
    std::env::var(EXPORTER_OTLP_ENDPOINT).ok()?;

    let protocol =
        std::env::var(EXPORTER_OTLP_PROTOCOL).unwrap_or_else(|_| "http/json".to_string());

    // Build the exporter using the SDK's env-var-aware builders.
    // We intentionally do NOT call .with_endpoint() / .with_headers() /
    // .with_metadata() here — the SDK reads OTEL_EXPORTER_OTLP_ENDPOINT and
    // OTEL_EXPORTER_OTLP_HEADERS from the environment automatically, which
    // preserves correct path-appending behaviour for HTTP exporters.
    let exporter = match protocol.as_str() {
        // ── gRPC ─────────────────────────────────────────────────────────────
        "grpc" => SpanExporter::builder().with_tonic().build(),
        // ── HTTP/Protobuf ────────────────────────────────────────────────────
        "http/protobuf" => SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build(),
        // ── HTTP/JSON (default) ──────────────────────────────────────────────
        // Default when OTEL_EXPORTER_OTLP_PROTOCOL is unset.
        // Required for Parseable OSS — it only accepts application/json.
        "http/json" => SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .build(),
        other => {
            tracing::warn!(
                "Unknown OTEL_EXPORTER_OTLP_PROTOCOL value '{}'; disabling OTEL tracing. \
                 Supported values: grpc, http/protobuf, http/json",
                other
            );
            return None;
        }
    };

    let exporter = exporter
        .map_err(|e| tracing::warn!("Failed to build OTEL span exporter: {}", e))
        .ok()?;

    let resource = Resource::builder_empty()
        .with_service_name("parseable")
        .build();

    let processor = BatchSpanProcessor::builder(exporter).build();

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource)
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());

    // Register the W3C TraceContext propagator globally.
    // This is REQUIRED for:
    //   - Incoming HTTP header extraction (traceparent/tracestate)
    //   - Cross-thread channel propagation via inject/extract
    // Without this, propagator.extract() returns an empty context.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    Some(provider)
}
