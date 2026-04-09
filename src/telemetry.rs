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

use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{BatchSpanProcessor, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use tracing_opentelemetry::OpenTelemetryLayer;

/// Initializes the OpenTelemetry tracer provider if `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
///
/// Returns `Some(SdkTracerProvider)` when tracing is configured, `None` otherwise.
/// The caller must call `provider.shutdown()` before process exit to flush buffered spans.
pub fn init_otel_tracer() -> Option<SdkTracerProvider> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
    if endpoint.is_empty() {
        return None;
    }

    // Register the W3C TraceContext propagator globally.
    // This is REQUIRED unconditionally for cross-runtime context propagation
    // (e.g., QUERY_RUNTIME uses a separate OS thread pool).
    global::set_text_map_propagator(TraceContextPropagator::new());

    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_default();

    let exporter = match protocol.as_str() {
        "grpc" => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()
            .expect("Failed to build gRPC OTLP span exporter"),
        _ => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(&endpoint)
            .build()
            .expect("Failed to build HTTP/JSON OTLP span exporter"),
    };

    let processor = BatchSpanProcessor::builder(exporter).build();

    let resource = Resource::builder()
        .with_service_name("parseable")
        .build();

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource)
        .build();

    Some(provider)
}

/// Builds a `tracing_opentelemetry::OpenTelemetryLayer` from the given provider.
///
/// Compose this layer into the `tracing_subscriber::Registry` alongside other layers.
pub fn build_otel_layer<S>(
    provider: &SdkTracerProvider,
) -> OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    let tracer = provider.tracer("parseable");
    tracing_opentelemetry::layer().with_tracer(tracer)
}
