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

use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};

/// Initialise an OTLP tracer provider.
///
/// **Required env var:**
/// - `OTEL_EXPORTER_OTLP_ENDPOINT` — collector address.
///   For HTTP exporters the SDK appends the signal path automatically:
///   e.g. `http://localhost:4318` → `http://localhost:4318/v1/traces`.
///
/// **Optional env var:**
/// - `OTEL_EXPORTER_OTLP_PROTOCOL` — transport + serialisation:
///   - `grpc`      → gRPC / tonic  (Jaeger, Tempo, …)
///   - (default)   → HTTP / JSON   (Parseable OSS ingest at `/v1/traces`)
///
/// Returns `None` when `OTEL_EXPORTER_OTLP_ENDPOINT` is not set (OTEL disabled).
/// The caller must call `provider.shutdown()` before process exit.
pub fn init_otel_tracer() -> Option<SdkTracerProvider> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;

    // Register W3C TraceContext propagator globally — required for traceparent
    // header extraction in middleware AND cross-runtime context propagation.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_default();

    let exporter = match protocol.as_str() {
        "grpc" => SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()
            .ok()?,
        // HTTP/JSON is the default — required for Parseable OSS which only
        // accepts application/json at /v1/traces.
        _ => SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .with_endpoint(&endpoint)
            .build()
            .ok()?,
    };

    let resource = Resource::builder_empty()
        .with_service_name("parseable")
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());
    Some(provider)
}
