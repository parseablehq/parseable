/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use once_cell::sync::Lazy;
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    metrics::v1::{
        AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        metric, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span, status},
};
use rand::{Rng, RngCore, seq::SliceRandom};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use serde::Serialize;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::INTRA_CLUSTER_CLIENT;

const GENERATION_INTERVAL: Duration = Duration::from_secs(2);
const SERVICES: [&str; 5] = [
    "api-gateway",
    "user-service",
    "order-service",
    "payment-service",
    "inventory-service",
];
const OPERATIONS: [&str; 5] = [
    "GET /api/checkout",
    "auth.validate",
    "order.create",
    "payment.charge",
    "inventory.reserve",
];
const HTTP_METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const HTTP_PATHS: [&str; 5] = [
    "/api/users",
    "/api/orders",
    "/api/products",
    "/api/checkout",
    "/api/inventory",
];
const LOG_MESSAGES: [&str; 6] = [
    "Request processed successfully",
    "Database query executed",
    "User authenticated",
    "Cache hit for key",
    "Event published to queue",
    "Retrying failed request",
];

pub static OTEL_GENERATOR: Lazy<OtelGenerator> = Lazy::new(OtelGenerator::default);

#[derive(Debug, Serialize, Clone)]
pub struct OtelGeneratorResult {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct OtelGeneratorStatus {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_secs: Option<f64>,
}

#[derive(Debug, thiserror::Error)]
pub enum OtelGeneratorError {
    #[error("Invalid OTel endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("Invalid Authorization header: {0}")]
    InvalidAuthorization(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionState {
    Running,
    Stopping,
}

impl SessionState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopping => "stopping",
        }
    }
}

#[derive(Debug)]
struct GeneratorSession {
    id: u64,
    endpoint: String,
    duration_secs: u64,
    started_at: Instant,
    state: SessionState,
    cancellation: CancellationToken,
}

#[derive(Debug)]
pub struct OtelGenerator {
    sessions: Arc<Mutex<HashMap<Option<String>, GeneratorSession>>>,
    next_session_id: AtomicU64,
    export_enabled: bool,
}

impl Default for OtelGenerator {
    fn default() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: AtomicU64::new(1),
            export_enabled: true,
        }
    }
}

impl OtelGenerator {
    pub fn start(
        &self,
        endpoint: &str,
        auth: &str,
        duration_secs: Option<u64>,
        tenant_id: Option<&str>,
    ) -> Result<OtelGeneratorResult, OtelGeneratorError> {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        reqwest::Url::parse(&format!("{endpoint}/v1/traces"))
            .map_err(|error| OtelGeneratorError::InvalidEndpoint(error.to_string()))?;
        HeaderValue::from_str(auth)
            .map_err(|error| OtelGeneratorError::InvalidAuthorization(error.to_string()))?;

        let duration_secs = duration_secs.unwrap_or_default();
        let tenant_key = tenant_id.map(str::to_owned);
        let tenant_description = tenant_id
            .map(|tenant| format!(" for tenant '{tenant}'"))
            .unwrap_or_default();
        let cancellation = CancellationToken::new();
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);

        {
            let mut sessions = lock_sessions(&self.sessions);
            if let Some(session) = sessions.get(&tenant_key) {
                return Ok(OtelGeneratorResult {
                    status: "error".to_string(),
                    message: format!(
                        "Generator is already {}{tenant_description}",
                        session.state.as_str()
                    ),
                });
            }
            sessions.insert(
                tenant_key.clone(),
                GeneratorSession {
                    id: session_id,
                    endpoint: endpoint.clone(),
                    duration_secs,
                    started_at: Instant::now(),
                    state: SessionState::Running,
                    cancellation: cancellation.clone(),
                },
            );
        }

        let sessions = Arc::clone(&self.sessions);
        let tenant_for_task = tenant_key.clone();
        let tenant_header = tenant_key.clone();
        let auth = auth.to_string();
        let export_enabled = self.export_enabled;
        tokio::spawn(async move {
            if export_enabled {
                run_generator(
                    &endpoint,
                    &auth,
                    tenant_header.as_deref(),
                    duration_secs,
                    cancellation,
                )
                .await;
            } else {
                cancellation.cancelled().await;
            }

            let mut sessions = lock_sessions(&sessions);
            if sessions
                .get(&tenant_for_task)
                .is_some_and(|session| session.id == session_id)
            {
                sessions.remove(&tenant_for_task);
            }
        });

        let duration = if duration_secs == 0 {
            "infinite".to_string()
        } else {
            format!("{duration_secs}s")
        };
        Ok(OtelGeneratorResult {
            status: "started".to_string(),
            message: format!("Generator started{tenant_description} (duration: {duration})"),
        })
    }

    pub fn stop(&self, tenant_id: Option<&str>) -> OtelGeneratorResult {
        let tenant_key = tenant_id.map(str::to_owned);
        let tenant_description = tenant_id
            .map(|tenant| format!(" for tenant '{tenant}'"))
            .unwrap_or_default();
        let mut sessions = lock_sessions(&self.sessions);
        let Some(session) = sessions.get_mut(&tenant_key) else {
            return OtelGeneratorResult {
                status: "not_running".to_string(),
                message: format!("Generator is not running{tenant_description}"),
            };
        };

        if session.state == SessionState::Stopping {
            return OtelGeneratorResult {
                status: "stopping".to_string(),
                message: format!("Generator is already stopping{tenant_description}"),
            };
        }

        session.state = SessionState::Stopping;
        session.cancellation.cancel();
        OtelGeneratorResult {
            status: "stopping".to_string(),
            message: format!("Generator stop requested{tenant_description}"),
        }
    }

    pub fn status(&self, tenant_id: Option<&str>) -> OtelGeneratorStatus {
        let tenant_key = tenant_id.map(str::to_owned);
        let sessions = lock_sessions(&self.sessions);
        let Some(session) = sessions.get(&tenant_key) else {
            return OtelGeneratorStatus {
                state: "stopped".to_string(),
                endpoint: None,
                duration: None,
                elapsed_secs: None,
            };
        };

        OtelGeneratorStatus {
            state: session.state.as_str().to_string(),
            endpoint: Some(session.endpoint.clone()),
            duration: Some(session.duration_secs),
            elapsed_secs: Some(session.started_at.elapsed().as_secs_f64()),
        }
    }

    #[cfg(test)]
    fn without_exports() -> Self {
        Self {
            export_enabled: false,
            ..Self::default()
        }
    }
}

fn lock_sessions(
    sessions: &Mutex<HashMap<Option<String>, GeneratorSession>>,
) -> MutexGuard<'_, HashMap<Option<String>, GeneratorSession>> {
    sessions
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

async fn run_generator(
    endpoint: &str,
    auth: &str,
    tenant_id: Option<&str>,
    duration_secs: u64,
    cancellation: CancellationToken,
) {
    let started_at = Instant::now();
    let mut interval = tokio::time::interval(GENERATION_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sequence = 0_u64;
    info!(%endpoint, ?tenant_id, duration_secs, "native OTel demo generator started");

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            _ = interval.tick() => {
                if duration_secs > 0 && started_at.elapsed() >= Duration::from_secs(duration_secs) {
                    break;
                }
                sequence = sequence.wrapping_add(1);
                let batch = build_batch(sequence);
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = send_batch(endpoint, auth, tenant_id, batch) => {}
                }
            }
        }
    }

    info!(%endpoint, ?tenant_id, "native OTel demo generator stopped");
}

struct TelemetryBatch {
    traces: ExportTraceServiceRequest,
    metrics: ExportMetricsServiceRequest,
    logs: ExportLogsServiceRequest,
}

async fn send_batch(endpoint: &str, auth: &str, tenant_id: Option<&str>, batch: TelemetryBatch) {
    let (traces, metrics, logs) = tokio::join!(
        send_signal(
            endpoint,
            "traces",
            "otel-traces",
            "otel-demo-traces",
            auth,
            tenant_id,
            &batch.traces,
        ),
        send_signal(
            endpoint,
            "metrics",
            "otel-metrics",
            "otel-demo-metrics",
            auth,
            tenant_id,
            &batch.metrics,
        ),
        send_signal(
            endpoint,
            "logs",
            "otel-logs",
            "otel-demo-logs",
            auth,
            tenant_id,
            &batch.logs,
        ),
    );

    for (signal, result) in [("traces", traces), ("metrics", metrics), ("logs", logs)] {
        if let Err(error) = result {
            warn!(%error, signal, "native OTel demo export failed");
        }
    }
}

async fn send_signal<T: Serialize + ?Sized>(
    endpoint: &str,
    signal: &str,
    log_source: &str,
    stream: &str,
    auth: &str,
    tenant_id: Option<&str>,
    payload: &T,
) -> Result<(), String> {
    let body = serde_json::to_vec(payload).map_err(|error| error.to_string())?;
    let mut request = INTRA_CLUSTER_CLIENT
        .post(format!("{endpoint}/v1/{signal}"))
        .header(AUTHORIZATION, auth)
        .header(CONTENT_TYPE, "application/json")
        .header("X-P-Log-Source", log_source)
        .header("X-P-Stream", stream)
        .body(body);
    if let Some(tenant_id) = tenant_id {
        request = request.header("X-P-Tenant", tenant_id);
    }

    let response = request.send().await.map_err(|error| error.to_string())?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(format!("{status}: {body}"))
}

fn build_batch(sequence: u64) -> TelemetryBatch {
    let mut rng = rand::thread_rng();
    let now = unix_nanos();
    let mut trace_id = vec![0; 16];
    rng.fill_bytes(&mut trace_id);
    let span_ids: Vec<Vec<u8>> = SERVICES
        .iter()
        .map(|_| {
            let mut span_id = vec![0; 8];
            rng.fill_bytes(&mut span_id);
            span_id
        })
        .collect();
    let method = *HTTP_METHODS
        .choose(&mut rng)
        .expect("methods are non-empty");
    let path = *HTTP_PATHS.choose(&mut rng).expect("paths are non-empty");
    let error_index = if rng.gen_ratio(1, 5) {
        Some(rng.gen_range(1..SERVICES.len()))
    } else {
        None
    };

    let mut resource_spans = Vec::with_capacity(SERVICES.len());
    let mut resource_logs = Vec::with_capacity(SERVICES.len());
    let mut resource_metrics = Vec::with_capacity(SERVICES.len());

    for (index, service) in SERVICES.iter().enumerate() {
        let is_error = error_index == Some(index);
        let start = now + (index as u64 * 1_000_000);
        // Child spans stay inside their parent while retaining varied latency.
        let duration_ms = ((SERVICES.len() - index) as u64 * 20) + rng.gen_range(5_u64..15);
        let end = start + duration_ms * 1_000_000;
        let parent_span_id = if index == 0 {
            Vec::new()
        } else {
            span_ids[index - 1].clone()
        };
        let status_code = if is_error { 500 } else { 200 };
        let resource = resource(service, sequence);
        let span_attributes = vec![
            kv_string("service.name", service),
            kv_string("service.version", "1.3.0"),
            kv_string("http.method", method),
            kv_string("http.url", path),
            kv_int("http.status_code", status_code),
            kv_string("http.scheme", "http"),
            kv_string("http.target", path),
            kv_string("http.host", &format!("{service}.internal:8080")),
            kv_string("deployment.environment", "production"),
            kv_string("k8s.namespace.name", "production"),
            kv_string("k8s.cluster.name", "demo-cluster"),
            kv_string("k8s.pod.name", &format!("{service}-demo-{sequence}")),
            kv_string("db.system", if index > 1 { "postgresql" } else { "none" }),
            kv_double("app.demo.sequence", sequence as f64),
        ];
        let events = if is_error {
            vec![span::Event {
                time_unix_nano: end,
                name: "error.occurred".to_string(),
                attributes: vec![
                    kv_string("exception.type", "DemoServiceError"),
                    kv_string("exception.message", "Synthetic demo request failed"),
                ],
                ..Default::default()
            }]
        } else {
            vec![span::Event {
                time_unix_nano: start,
                name: "request.received".to_string(),
                attributes: vec![kv_string("request.id", &format!("req-{sequence}-{index}"))],
                ..Default::default()
            }]
        };
        let span = Span {
            trace_id: trace_id.clone(),
            span_id: span_ids[index].clone(),
            parent_span_id,
            flags: 1,
            name: if index == 0 {
                format!("{method} {path}")
            } else {
                OPERATIONS[index].to_string()
            },
            kind: if index == 0 {
                span::SpanKind::Server as i32
            } else {
                span::SpanKind::Client as i32
            },
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            attributes: span_attributes,
            events,
            status: Some(Status {
                message: if is_error {
                    "Synthetic HTTP 500 error".to_string()
                } else {
                    String::default()
                },
                code: if is_error {
                    status::StatusCode::Error as i32
                } else {
                    status::StatusCode::Ok as i32
                },
            }),
            ..Default::default()
        };
        resource_spans.push(ResourceSpans {
            resource: Some(resource.clone()),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope("parseable.otel-demo.traces")),
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        });

        let (severity_number, severity_text, message) = if is_error {
            (
                SeverityNumber::Error as i32,
                "ERROR",
                "Synthetic request failed",
            )
        } else {
            (
                SeverityNumber::Info as i32,
                "INFO",
                *LOG_MESSAGES
                    .choose(&mut rng)
                    .expect("messages are non-empty"),
            )
        };
        resource_logs.push(ResourceLogs {
            resource: Some(resource.clone()),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope("parseable.otel-demo.logs")),
                log_records: vec![LogRecord {
                    time_unix_nano: end,
                    observed_time_unix_nano: end,
                    severity_number,
                    severity_text: severity_text.to_string(),
                    body: Some(any_string(message)),
                    attributes: vec![
                        kv_string("service.name", service),
                        kv_string("http.method", method),
                        kv_string("http.target", path),
                        kv_int("http.status_code", status_code),
                    ],
                    flags: 1,
                    trace_id: trace_id.clone(),
                    span_id: span_ids[index].clone(),
                    event_name: if is_error {
                        "request.failed".to_string()
                    } else {
                        "request.completed".to_string()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });

        let base_metric_attributes = vec![
            kv_string("service", service),
            kv_string("k8s.namespace.name", "production"),
            kv_string("k8s.cluster.name", "demo-cluster"),
        ];
        let mut request_metric_attributes = base_metric_attributes.clone();
        request_metric_attributes.extend([
            kv_string("method", method),
            kv_string("endpoint", path),
            kv_string("status", &status_code.to_string()),
        ]);
        let mut duration_metric_attributes = base_metric_attributes.clone();
        duration_metric_attributes
            .extend([kv_string("method", method), kv_string("endpoint", path)]);
        let mut database_metric_attributes = base_metric_attributes.clone();
        database_metric_attributes.push(kv_string(
            "db.operation",
            if index % 2 == 0 { "SELECT" } else { "UPDATE" },
        ));
        let mut auth_metric_attributes = base_metric_attributes.clone();
        auth_metric_attributes.push(kv_string(
            "success",
            if is_error { "false" } else { "true" },
        ));
        let mut error_metric_attributes = base_metric_attributes.clone();
        error_metric_attributes.extend([
            kv_string("status", &status_code.to_string()),
            kv_string("endpoint", path),
        ]);
        let connection_attributes = vec![
            kv_string("service", service),
            kv_string("k8s.namespace.name", "production"),
        ];
        let gauge_attributes = vec![kv_string("service", "otel-demo")];
        let service_factor = index as u64 + 1;
        let counter_value = sequence.max(1).saturating_mul(service_factor);
        resource_metrics.push(ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(scope("parseable.otel-demo.metrics")),
                metrics: vec![
                    counter_metric(
                        "http_requests_total",
                        "Total HTTP requests",
                        counter_value,
                        sequence,
                        now,
                        request_metric_attributes,
                    ),
                    counter_metric(
                        "cache_hits_total",
                        "Total cache hits",
                        counter_value.saturating_mul(3),
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    counter_metric(
                        "cache_misses_total",
                        "Total cache misses",
                        counter_value,
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    counter_metric(
                        "errors_total",
                        "Total errors",
                        counter_value.div_ceil(5),
                        sequence,
                        now,
                        error_metric_attributes,
                    ),
                    counter_metric(
                        "bytes_sent_total",
                        "Total bytes sent",
                        counter_value.saturating_mul(5_000),
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    counter_metric(
                        "bytes_received_total",
                        "Total bytes received",
                        counter_value.saturating_mul(2_500),
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    counter_metric(
                        "db_queries_total",
                        "Total database queries",
                        counter_value.saturating_mul(3),
                        sequence,
                        now,
                        database_metric_attributes,
                    ),
                    counter_metric(
                        "auth_attempts_total",
                        "Total authentication attempts",
                        counter_value,
                        sequence,
                        now,
                        auth_metric_attributes,
                    ),
                    up_down_counter_metric(
                        "active_connections",
                        "Current active connections",
                        10 + index as i64,
                        sequence,
                        now,
                        connection_attributes.clone(),
                    ),
                    up_down_counter_metric(
                        "queue_size",
                        "Current queue size",
                        rng.gen_range(0_i64..20),
                        sequence,
                        now,
                        connection_attributes,
                    ),
                    up_down_counter_metric(
                        "request_duration_ms",
                        "Request duration in ms",
                        duration_ms as i64,
                        sequence,
                        now,
                        duration_metric_attributes,
                    ),
                    up_down_counter_metric(
                        "active_requests",
                        "Current in-flight requests",
                        rng.gen_range(0_i64..15),
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    up_down_counter_metric(
                        "open_file_handles",
                        "Current open file handles",
                        rng.gen_range(20_i64..200),
                        sequence,
                        now,
                        base_metric_attributes,
                    ),
                    gauge_metric(
                        "cpu_usage_percent",
                        "CPU usage",
                        rng.gen_range(10.0..90.0),
                        now,
                        gauge_attributes.clone(),
                    ),
                    gauge_metric(
                        "memory_usage_percent",
                        "Memory usage",
                        rng.gen_range(20.0..85.0),
                        now,
                        gauge_attributes.clone(),
                    ),
                    gauge_metric(
                        "thread_count",
                        "Current thread count",
                        rng.gen_range(5.0..50.0),
                        now,
                        gauge_attributes.clone(),
                    ),
                    gauge_metric(
                        "connection_pool_available",
                        "Available connection pool entries",
                        rng.gen_range(1.0..20.0),
                        now,
                        gauge_attributes,
                    ),
                ],
                ..Default::default()
            }],
            ..Default::default()
        });
    }

    TelemetryBatch {
        traces: ExportTraceServiceRequest { resource_spans },
        metrics: ExportMetricsServiceRequest { resource_metrics },
        logs: ExportLogsServiceRequest { resource_logs },
    }
}

fn resource(service: &str, sequence: u64) -> Resource {
    Resource {
        attributes: vec![
            kv_string("service.name", service),
            kv_string("service.version", "1.3.0"),
            kv_string("service.instance.id", &format!("{service}-{sequence}")),
            kv_string("deployment.environment", "production"),
            kv_string("telemetry.sdk.language", "rust"),
            kv_string("telemetry.sdk.name", "parseable-otel-demo"),
        ],
        ..Default::default()
    }
}

fn scope(name: &str) -> InstrumentationScope {
    InstrumentationScope {
        name: name.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        ..Default::default()
    }
}

fn counter_metric(
    name: &str,
    description: &str,
    value: u64,
    sequence: u64,
    now: u64,
    attributes: Vec<KeyValue>,
) -> Metric {
    let elapsed_nanos = (GENERATION_INTERVAL.as_nanos() as u64).saturating_mul(sequence);
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: "1".to_string(),
        data: Some(metric::Data::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes,
                start_time_unix_nano: now.saturating_sub(elapsed_nanos),
                time_unix_nano: now,
                value: Some(number_data_point::Value::AsInt(
                    value.min(i64::MAX as u64) as i64
                )),
                ..Default::default()
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        })),
        ..Default::default()
    }
}

fn up_down_counter_metric(
    name: &str,
    description: &str,
    value: i64,
    sequence: u64,
    now: u64,
    attributes: Vec<KeyValue>,
) -> Metric {
    let elapsed_nanos = (GENERATION_INTERVAL.as_nanos() as u64).saturating_mul(sequence);
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: "1".to_string(),
        data: Some(metric::Data::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes,
                start_time_unix_nano: now.saturating_sub(elapsed_nanos),
                time_unix_nano: now,
                value: Some(number_data_point::Value::AsInt(value)),
                ..Default::default()
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: false,
        })),
        ..Default::default()
    }
}

fn gauge_metric(
    name: &str,
    description: &str,
    value: f64,
    now: u64,
    attributes: Vec<KeyValue>,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: "1".to_string(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes,
                time_unix_nano: now,
                value: Some(number_data_point::Value::AsDouble(value)),
                ..Default::default()
            }],
        })),
        ..Default::default()
    }
}

fn kv_string(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(any_string(value)),
        ..Default::default()
    }
}

fn kv_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
        ..Default::default()
    }
}

fn kv_double(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::DoubleValue(value)),
        }),
        ..Default::default()
    }
}

fn any_string(value: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value.to_string())),
    }
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        logs::v1::LogsData, metrics::v1::MetricsData, trace::v1::TracesData,
    };

    #[test]
    fn generated_batch_contains_all_signals_and_services() {
        let batch = build_batch(7);
        assert_eq!(batch.traces.resource_spans.len(), SERVICES.len());
        assert_eq!(batch.metrics.resource_metrics.len(), SERVICES.len());
        assert_eq!(batch.logs.resource_logs.len(), SERVICES.len());

        let trace_id = &batch.traces.resource_spans[0].scope_spans[0].spans[0].trace_id;
        assert_eq!(trace_id.len(), 16);
        for (index, resource_spans) in batch.traces.resource_spans.iter().enumerate() {
            let span = &resource_spans.scope_spans[0].spans[0];
            assert_eq!(&span.trace_id, trace_id);
            assert_eq!(span.span_id.len(), 8);
            if index > 0 {
                let parent = &batch.traces.resource_spans[index - 1].scope_spans[0].spans[0];
                assert_eq!(span.parent_span_id, parent.span_id);
                assert!(span.end_time_unix_nano <= parent.end_time_unix_nano);
            }
        }
    }

    #[test]
    fn generated_batch_matches_python_metric_set() {
        let batch = build_batch(7);
        let mut names: Vec<&str> = batch.metrics.resource_metrics[0].scope_metrics[0]
            .metrics
            .iter()
            .map(|metric| metric.name.as_str())
            .collect();
        names.sort_unstable();
        let mut expected = vec![
            "active_connections",
            "active_requests",
            "auth_attempts_total",
            "bytes_received_total",
            "bytes_sent_total",
            "cache_hits_total",
            "cache_misses_total",
            "connection_pool_available",
            "cpu_usage_percent",
            "db_queries_total",
            "errors_total",
            "http_requests_total",
            "memory_usage_percent",
            "open_file_handles",
            "queue_size",
            "request_duration_ms",
            "thread_count",
        ];
        expected.sort_unstable();
        assert_eq!(names, expected);
    }

    #[test]
    fn generated_batch_serializes_as_otlp_json() {
        let batch = build_batch(1);
        let traces = serde_json::to_value(batch.traces).unwrap();
        let metrics = serde_json::to_value(batch.metrics).unwrap();
        let logs = serde_json::to_value(batch.logs).unwrap();
        assert!(traces.get("resourceSpans").is_some());
        assert!(metrics.get("resourceMetrics").is_some());
        assert!(logs.get("resourceLogs").is_some());
        assert_eq!(
            serde_json::from_value::<TracesData>(traces)
                .unwrap()
                .resource_spans
                .len(),
            SERVICES.len()
        );
        assert_eq!(
            serde_json::from_value::<MetricsData>(metrics)
                .unwrap()
                .resource_metrics
                .len(),
            SERVICES.len()
        );
        assert_eq!(
            serde_json::from_value::<LogsData>(logs)
                .unwrap()
                .resource_logs
                .len(),
            SERVICES.len()
        );
    }

    #[tokio::test]
    async fn lifecycle_is_per_tenant() {
        let generator = OtelGenerator::without_exports();
        let first = generator
            .start("http://127.0.0.1:1", "Basic test", Some(60), Some("acme"))
            .unwrap();
        assert_eq!(first.status, "started");
        assert_eq!(generator.status(Some("acme")).state, "running");
        assert_eq!(generator.status(Some("other")).state, "stopped");

        let duplicate = generator
            .start("http://127.0.0.1:1", "Basic test", Some(60), Some("acme"))
            .unwrap();
        assert_eq!(duplicate.status, "error");
        assert_eq!(generator.stop(Some("acme")).status, "stopping");

        tokio::time::timeout(Duration::from_secs(1), async {
            while generator.status(Some("acme")).state != "stopped" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
