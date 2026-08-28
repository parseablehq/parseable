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
const HTTP_METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const HTTP_PATHS: [&str; 7] = [
    "/api/users",
    "/api/orders",
    "/api/products",
    "/api/checkout",
    "/api/inventory",
    "/health",
    "/metrics",
];
const LOG_MESSAGES: [&str; 10] = [
    "Request processed successfully",
    "Database query executed",
    "User authenticated",
    "Cache hit for key",
    "Event published to queue",
    "Retrying failed request",
    "Rate limit checked",
    "Connection pool acquired",
    "Circuit breaker open",
    "Validation passed",
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
            "otel-demo-traces",
            auth,
            tenant_id,
            &batch.traces,
        ),
        send_signal(
            endpoint,
            "metrics",
            "otel-demo-metrics",
            auth,
            tenant_id,
            &batch.metrics,
        ),
        send_signal(
            endpoint,
            "logs",
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
    let method = *HTTP_METHODS
        .choose(&mut rng)
        .expect("methods are non-empty");
    let path = *HTTP_PATHS.choose(&mut rng).expect("paths are non-empty");
    let mut resource_spans = Vec::with_capacity(SERVICES.len());
    let mut resource_logs = Vec::with_capacity(SERVICES.len());
    let mut resource_metrics = Vec::with_capacity(SERVICES.len());

    for (index, service) in SERVICES.iter().enumerate() {
        let is_error = rng.gen_ratio(1, 4);
        let start = now + (index as u64 * 1_000_000);
        let duration_ms = rng.gen_range(250_u64..500);
        let end = start + duration_ms * 1_000_000;
        let status_code = if is_error {
            *[400_i64, 404, 500]
                .choose(&mut rng)
                .expect("error status codes are non-empty")
        } else {
            *[200_i64, 200, 200, 201]
                .choose(&mut rng)
                .expect("success status codes are non-empty")
        };
        let resource = resource(service);
        let (trace_id, root_span_id, spans) = build_service_trace(
            &mut rng,
            service,
            sequence,
            method,
            path,
            status_code,
            start,
            end,
            is_error,
        );
        resource_spans.push(ResourceSpans {
            resource: Some(resource.clone()),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope("parseable.otel-demo.traces")),
                spans,
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
            let (severity_number, severity_text) = match rng.gen_range(0_u8..5) {
                3 => (SeverityNumber::Warn as i32, "WARN"),
                4 => (SeverityNumber::Error as i32, "ERROR"),
                _ => (SeverityNumber::Info as i32, "INFO"),
            };
            let message = *LOG_MESSAGES
                .choose(&mut rng)
                .expect("messages are non-empty");
            (severity_number, severity_text, message)
        };
        let log_message = format!("{message} - {method} {path} {status_code}");
        resource_logs.push(ResourceLogs {
            resource: Some(resource.clone()),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope("parseable.otel-demo.logs")),
                log_records: vec![LogRecord {
                    time_unix_nano: end,
                    observed_time_unix_nano: end,
                    severity_number,
                    severity_text: severity_text.to_string(),
                    body: Some(any_string(&log_message)),
                    attributes: vec![
                        kv_string("service", service),
                        kv_string("k8s.namespace.name", "production"),
                        kv_string("k8s.pod.name", &format!("{service}-demo-{sequence}")),
                        kv_string("k8s.cluster.name", "demo-cluster"),
                        kv_string("cloud.provider", "aws"),
                        kv_string("cloud.region", "us-east-1"),
                        kv_string("http.method", method),
                        kv_string("http.url", path),
                        kv_int("http.response.status_code", status_code),
                        kv_string("trace.id", &hex::encode(&trace_id)),
                        kv_string("span.id", &hex::encode(&root_span_id)),
                        kv_string("net.peer.ip", &format!("10.0.{}.{}", index + 1, 10 + index)),
                        kv_string("container.id", &format!("container-{sequence}-{index}")),
                    ],
                    flags: 1,
                    trace_id: trace_id.clone(),
                    span_id: root_span_id,
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
        let metric_method = HTTP_METHODS[index % HTTP_METHODS.len()];
        let metric_path = HTTP_PATHS[index % HTTP_PATHS.len()];
        let mut request_metric_attributes = base_metric_attributes.clone();
        request_metric_attributes.extend([
            kv_string("method", metric_method),
            kv_string("endpoint", metric_path),
            kv_string("status", "200"),
        ]);
        let mut duration_metric_attributes = base_metric_attributes.clone();
        duration_metric_attributes.extend([
            kv_string("method", metric_method),
            kv_string("endpoint", metric_path),
        ]);
        let mut database_metric_attributes = base_metric_attributes.clone();
        database_metric_attributes.push(kv_string(
            "db.operation",
            if index % 2 == 0 { "SELECT" } else { "UPDATE" },
        ));
        let mut auth_metric_attributes = base_metric_attributes.clone();
        auth_metric_attributes.push(kv_string("success", "true"));
        let mut error_metric_attributes = base_metric_attributes.clone();
        error_metric_attributes.extend([
            kv_string("status", "500"),
            kv_string("endpoint", metric_path),
        ]);
        let connection_attributes = vec![
            kv_string("service", service),
            kv_string("k8s.namespace.name", "production"),
        ];
        let gauge_attributes = vec![kv_string("service", "otel-demo")];
        let service_factor = index as u64 + 1;
        resource_metrics.push(ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(scope("parseable.otel-demo.metrics")),
                metrics: vec![
                    counter_metric(
                        "http_requests_total",
                        "Total HTTP requests",
                        random_cumulative_value(&mut rng, sequence, service_factor),
                        sequence,
                        now,
                        request_metric_attributes,
                    ),
                    counter_metric(
                        "cache_hits_total",
                        "Total cache hits",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(3),
                        ),
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    counter_metric(
                        "cache_misses_total",
                        "Total cache misses",
                        random_cumulative_value(&mut rng, sequence, service_factor),
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    counter_metric(
                        "errors_total",
                        "Total errors",
                        random_cumulative_value(&mut rng, sequence, 2),
                        sequence,
                        now,
                        error_metric_attributes,
                    ),
                    counter_metric(
                        "bytes_sent_total",
                        "Total bytes sent",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(5_000),
                        ),
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    counter_metric(
                        "bytes_received_total",
                        "Total bytes received",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(2_500),
                        ),
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    counter_metric(
                        "db_queries_total",
                        "Total database queries",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(3),
                        ),
                        sequence,
                        now,
                        database_metric_attributes,
                    ),
                    counter_metric(
                        "auth_attempts_total",
                        "Total authentication attempts",
                        random_cumulative_value(&mut rng, sequence, service_factor),
                        sequence,
                        now,
                        auth_metric_attributes,
                    ),
                    up_down_counter_metric(
                        "active_connections",
                        "Current active connections",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(3),
                        ) as i64,
                        sequence,
                        now,
                        connection_attributes.clone(),
                    ),
                    up_down_counter_metric(
                        "queue_size",
                        "Current queue size",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(5),
                        ) as i64,
                        sequence,
                        now,
                        connection_attributes,
                    ),
                    up_down_counter_metric(
                        "request_duration_ms",
                        "Request duration in ms",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(100),
                        ) as i64,
                        sequence,
                        now,
                        duration_metric_attributes,
                    ),
                    up_down_counter_metric(
                        "active_requests",
                        "Current in-flight requests",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(2),
                        ) as i64,
                        sequence,
                        now,
                        base_metric_attributes.clone(),
                    ),
                    up_down_counter_metric(
                        "open_file_handles",
                        "Current open file handles",
                        random_cumulative_value(
                            &mut rng,
                            sequence,
                            service_factor.saturating_mul(10),
                        ) as i64,
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

#[allow(clippy::too_many_arguments)]
fn build_service_trace(
    rng: &mut impl Rng,
    service: &str,
    sequence: u64,
    method: &str,
    path: &str,
    status_code: i64,
    start: u64,
    end: u64,
    is_error: bool,
) -> (Vec<u8>, Vec<u8>, Vec<Span>) {
    const SPAN_COUNT: usize = 13;
    let trace_id = random_id(rng, 16);
    let span_ids: Vec<Vec<u8>> = (0..SPAN_COUNT).map(|_| random_id(rng, 8)).collect();
    let at = |percent: u64| start + end.saturating_sub(start) * percent / 100;

    let mut root_events = vec![demo_event(
        start,
        "request.received",
        vec![
            kv_string("client.ip", "10.0.1.42"),
            kv_string("request.id", &format!("req-{sequence}-{service}")),
            kv_int("trace.flags", 1),
        ],
    )];
    if is_error {
        root_events.push(demo_event(
            at(95),
            "error.occurred",
            vec![
                kv_string("exception.type", "InternalServerError"),
                kv_string("exception.code", &status_code.to_string()),
                kv_string("exception.message", "Synthetic demo request failed"),
                kv_string(
                    "exception.stacktrace",
                    &format!("at {service}.handler({service}.rs:123)"),
                ),
            ],
        ));
    }
    root_events.push(demo_event(
        end,
        "request.completed",
        vec![
            kv_int("duration_ms", end.saturating_sub(start) as i64 / 1_000_000),
            kv_int("response.size_bytes", 8_192),
        ],
    ));

    let root_attributes = vec![
        kv_string("service.name", service),
        kv_string("service.version", "1.3.0"),
        kv_string(
            "service.instance.id",
            &format!("{service}-{}", sequence % 5 + 1),
        ),
        kv_string("http.method", method),
        kv_string("http.url", path),
        kv_int("http.response.status_code", status_code),
        kv_string("http.scheme", "https"),
        kv_string("http.target", path),
        kv_string("http.host", &format!("{service}.internal:8080")),
        kv_string("http.flavor", "HTTP/2"),
        kv_string("http.user_agent", "parseable-otel-demo/1.0"),
        kv_int("http.request_content_length", 1_024),
        kv_int("http.response_content_length", 8_192),
        kv_string("k8s.namespace.name", "production"),
        kv_string("k8s.cluster.name", "prod-us-east-1"),
        kv_string("k8s.pod.name", &format!("{service}-demo-{sequence}")),
        kv_string("k8s.deployment.name", &format!("{service}-deploy")),
        kv_string("k8s.replicaset.name", &format!("{service}-rs-123")),
        kv_string("k8s.node.name", "node-pool-1-abc123"),
        kv_string("k8s.container.name", service),
        kv_string("cloud.provider", "aws"),
        kv_string("cloud.region", "us-east-1"),
        kv_string("cloud.availability_zone", "us-east-1a"),
        kv_string("cloud.account.id", "123456789012"),
        kv_string("cloud.platform", "aws_ec2"),
        kv_string("net.protocol.name", "HTTP/2"),
        kv_string("net.transport", "tcp"),
        kv_string("net.peer.ip", "10.0.1.42"),
        kv_int("net.peer.port", 44_321),
        kv_string("net.host.ip", "10.0.1.10"),
        kv_int("net.host.port", 8_080),
        kv_string("container.id", &format!("container-{sequence}-{service}")),
        kv_string("container.name", service),
        kv_string(
            "container.image.name",
            &format!("registry.internal/{service}"),
        ),
        kv_string("container.image.tag", "v1.3.0"),
        kv_string("container.runtime", "containerd"),
        kv_int("process.pid", 1_000 + sequence as i64 % 50_000),
        kv_string("process.executable.name", "parseable-demo"),
        kv_string("process.runtime.name", "Rust"),
        kv_string("process.runtime.version", env!("CARGO_PKG_VERSION")),
        kv_double("app.demo.sequence", sequence as f64),
    ];

    let spans = vec![
        demo_span(
            &trace_id,
            &span_ids[0],
            &[],
            &format!("{method} {path}"),
            span::SpanKind::Server,
            start,
            end,
            root_attributes,
            root_events,
            is_error,
        ),
        demo_span(
            &trace_id,
            &span_ids[1],
            &span_ids[0],
            "auth.validate",
            span::SpanKind::Internal,
            at(2),
            at(12),
            vec![
                kv_string("auth.method", "jwt"),
                kv_string("auth.token.type", "bearer"),
                kv_string("enduser.id", &format!("user_{}", sequence % 9_000 + 1_000)),
                kv_string("enduser.role", "user"),
                kv_string("enduser.scope", "read:write"),
                kv_bool("auth.success", !is_error),
                kv_double("auth.latency_ms", 12.5),
            ],
            vec![
                demo_event(
                    at(3),
                    "auth.started",
                    vec![kv_string("auth.provider", "internal")],
                ),
                if is_error {
                    demo_event(
                        at(11),
                        "auth.failed",
                        vec![
                            kv_string("reason", "invalid_token"),
                            kv_string("auth.error_code", "AUTH001"),
                        ],
                    )
                } else {
                    demo_event(
                        at(11),
                        "auth.completed",
                        vec![
                            kv_string("user.id", &format!("user_{}", sequence % 9_000 + 1_000)),
                            kv_string("session.id", &format!("sess-{sequence}")),
                            kv_int("token.expires_in", 3_600),
                        ],
                    )
                },
            ],
            is_error,
        ),
        demo_span(
            &trace_id,
            &span_ids[2],
            &span_ids[0],
            "business_logic",
            span::SpanKind::Internal,
            at(15),
            at(70),
            vec![kv_string("operation.type", "compute")],
            vec![
                demo_event(at(15), "processing.started", vec![]),
                demo_event(at(70), "processing.completed", vec![]),
            ],
            false,
        ),
        demo_span(
            &trace_id,
            &span_ids[3],
            &span_ids[2],
            "db.query",
            span::SpanKind::Client,
            at(20),
            at(42),
            vec![
                kv_string("db.system", "postgresql"),
                kv_string("db.name", "app_production"),
                kv_string("db.user", "app_user"),
                kv_string(
                    "db.connection_string",
                    "postgresql://db-primary.internal:5432",
                ),
                kv_string("db.operation", "SELECT"),
                kv_string("db.sql.table", "orders"),
                kv_string("db.statement", "SELECT * FROM orders WHERE id = $1"),
                kv_string("db.server.address", "db-primary.internal"),
                kv_int("db.server.port", 5_432),
                kv_string("db.pool.name", "postgresql_pool"),
                kv_int("db.pool.max_size", 50),
                kv_int("db.pool.min_size", 5),
                kv_string("db.query.id", &format!("query-{sequence}")),
                kv_string("db.query.plan", "index_scan"),
                kv_double("db.query.cost", 42.5),
                kv_string("db.transaction.id", &format!("txn-{sequence}")),
                kv_int("db.rows_returned", 12),
                kv_double("db.duration_ms", 35.5),
            ],
            vec![
                demo_event(
                    at(21),
                    "query.prepared",
                    vec![
                        kv_string("query.hash", &format!("hash-{sequence}")),
                        kv_bool("query.parameterized", true),
                    ],
                ),
                demo_event(
                    at(40),
                    "query.executed",
                    vec![
                        kv_int("rows_affected", 12),
                        kv_double("execution_time_ms", 35.5),
                        kv_double("lock_time_ms", 1.2),
                    ],
                ),
            ],
            false,
        ),
        demo_span(
            &trace_id,
            &span_ids[4],
            &span_ids[3],
            "db.connection_pool",
            span::SpanKind::Internal,
            at(22),
            at(30),
            vec![
                kv_string("db.pool.name", "postgresql_pool"),
                kv_int("db.pool.max_size", 50),
                kv_int("db.pool.min_size", 5),
                kv_int("db.pool.active_connections", 8),
                kv_int("db.pool.idle_connections", 3),
                kv_int("db.pool.waiting_requests", 1),
                kv_string("db.connection.id", &format!("conn-{sequence}")),
            ],
            vec![
                demo_event(
                    at(23),
                    "connection.acquired",
                    vec![
                        kv_double("wait_time_ms", 2.5),
                        kv_bool("connection.reused", true),
                    ],
                ),
                demo_event(at(29), "connection.released", vec![]),
            ],
            false,
        ),
        demo_span(
            &trace_id,
            &span_ids[5],
            &span_ids[2],
            "cache.operation",
            span::SpanKind::Client,
            at(43),
            at(52),
            vec![
                kv_string("cache.operation", "get"),
                kv_string("cache.backend", "redis"),
                kv_string("cache.key", &format!("{service}:orders:{sequence}")),
                kv_bool("cache.hit", !sequence.is_multiple_of(3)),
                kv_string("db.system", "redis"),
                kv_string("db.server.address", "redis-master.internal"),
                kv_int("db.server.port", 6_379),
                kv_int("db.redis.database_index", 0),
                kv_int("cache.ttl_seconds", 300),
            ],
            vec![demo_event(
                at(44),
                "cache.get",
                vec![
                    kv_string("key", &format!("{service}:orders:{sequence}")),
                    kv_string("key.prefix", service),
                    kv_int("value.size_bytes", 512),
                ],
            )],
            false,
        ),
        demo_span(
            &trace_id,
            &span_ids[6],
            &span_ids[5],
            "redis.command",
            span::SpanKind::Client,
            at(44),
            at(49),
            vec![
                kv_string("db.system", "redis"),
                kv_string("db.operation", "GET"),
                kv_string("db.statement", "GET demo:key"),
                kv_string("db.redis.flags", ""),
                kv_string("net.peer.name", "redis-master.internal"),
                kv_int("net.peer.port", 6_379),
            ],
            vec![
                demo_event(
                    at(45),
                    "command.sent",
                    vec![kv_int("command.args_count", 1)],
                ),
                demo_event(
                    at(48),
                    "response.received",
                    vec![kv_string("response.type", "string")],
                ),
            ],
            false,
        ),
        demo_span(
            &trace_id,
            &span_ids[7],
            &span_ids[2],
            "messaging.publish",
            span::SpanKind::Producer,
            at(53),
            at(65),
            vec![
                kv_string("messaging.system", "kafka"),
                kv_string("messaging.destination.name", "orders"),
                kv_string("messaging.destination.kind", "topic"),
                kv_string("messaging.operation", "publish"),
                kv_string("messaging.message.id", &format!("msg-{sequence}")),
                kv_int("messaging.message.payload_size_bytes", 2_048),
                kv_int("messaging.kafka.partition", sequence as i64 % 12),
                kv_int("messaging.kafka.message.offset", sequence as i64 * 100),
            ],
            vec![
                demo_event(
                    at(54),
                    "message.created",
                    vec![
                        kv_string("message.type", "event"),
                        kv_string("message.priority", "normal"),
                    ],
                ),
                demo_event(
                    at(64),
                    "message.published",
                    vec![kv_double("delivery.time_ms", 8.5)],
                ),
            ],
            false,
        ),
        demo_span(
            &trace_id,
            &span_ids[8],
            &span_ids[7],
            "messaging.broker",
            span::SpanKind::Client,
            at(55),
            at(61),
            vec![
                kv_string("net.peer.name", "kafka-broker.internal"),
                kv_int("net.peer.port", 9_092),
                kv_string("messaging.client_id", &format!("client-{service}")),
            ],
            vec![
                demo_event(at(56), "broker.connected", vec![]),
                demo_event(at(60), "message.sent", vec![kv_bool("ack.received", true)]),
            ],
            false,
        ),
        demo_span(
            &trace_id,
            &span_ids[9],
            &span_ids[0],
            "external.call",
            span::SpanKind::Client,
            at(72),
            at(98),
            vec![
                kv_string("peer.service", "stripe"),
                kv_string("http.method", "POST"),
                kv_string("rpc.system", "http"),
                kv_string("rpc.service", "stripe"),
                kv_string("rpc.method", "createPayment"),
            ],
            vec![
                demo_event(
                    at(73),
                    "request.sent",
                    vec![
                        kv_string("target", "stripe"),
                        kv_string("request.id", &format!("ext-req-{sequence}")),
                        kv_bool("request.retryable", true),
                    ],
                ),
                demo_event(
                    at(97),
                    "response.received",
                    vec![
                        kv_int("status", if is_error { 503 } else { 200 }),
                        kv_double("response.time_ms", 125.0),
                    ],
                ),
            ],
            is_error,
        ),
        demo_span(
            &trace_id,
            &span_ids[10],
            &span_ids[9],
            "http.client",
            span::SpanKind::Client,
            at(75),
            at(96),
            vec![
                kv_string("http.url", "https://api.stripe.com/v1/charge"),
                kv_string("http.method", "POST"),
                kv_string("http.scheme", "https"),
                kv_string("http.host", "api.stripe.com"),
                kv_string("http.target", "/v1/charge"),
                kv_string("http.flavor", "HTTP/2"),
                kv_int("http.request_content_length", 1_024),
                kv_int(
                    "http.response.status_code",
                    if is_error { 503 } else { 200 },
                ),
                kv_int("http.response_content_length", 2_048),
                kv_string("net.peer.name", "api.stripe.com"),
                kv_int("net.peer.port", 443),
            ],
            if is_error {
                vec![demo_event(
                    at(95),
                    "external.error",
                    vec![
                        kv_int("status", 503),
                        kv_string("service", "stripe"),
                        kv_string("error.type", "ServiceError"),
                        kv_bool("retry.recommended", true),
                    ],
                )]
            } else {
                vec![demo_event(at(95), "response.received", vec![])]
            },
            is_error,
        ),
        demo_span(
            &trace_id,
            &span_ids[11],
            &span_ids[10],
            "dns.resolve",
            span::SpanKind::Client,
            at(77),
            at(82),
            vec![
                kv_string("dns.hostname", "api.stripe.com"),
                kv_string("dns.question.type", "A"),
                kv_string("dns.question.class", "IN"),
                kv_string("dns.answers", "54.187.174.169"),
                kv_string("dns.response_code", "NOERROR"),
            ],
            vec![
                demo_event(
                    at(78),
                    "dns.lookup.started",
                    vec![kv_string("dns.resolver", "8.8.8.8")],
                ),
                demo_event(
                    at(81),
                    "dns.lookup.completed",
                    vec![
                        kv_string("ip", "54.187.174.169"),
                        kv_int("ttl_seconds", 300),
                    ],
                ),
            ],
            false,
        ),
        demo_span(
            &trace_id,
            &span_ids[12],
            &span_ids[10],
            "tls.handshake",
            span::SpanKind::Client,
            at(84),
            at(90),
            vec![
                kv_string("tls.protocol.version", "TLSv1.3"),
                kv_string("tls.cipher", "TLS_AES_256_GCM_SHA384"),
                kv_bool("tls.resumed", false),
            ],
            vec![demo_event(
                at(89),
                "handshake.completed",
                vec![
                    kv_string("certificate.issuer", "Let's Encrypt Authority X3"),
                    kv_string("certificate.valid_until", "2027-12-31"),
                ],
            )],
            false,
        ),
    ];

    (trace_id, span_ids[0].clone(), spans)
}

#[allow(clippy::too_many_arguments)]
fn demo_span(
    trace_id: &[u8],
    span_id: &[u8],
    parent_span_id: &[u8],
    name: &str,
    kind: span::SpanKind,
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    attributes: Vec<KeyValue>,
    events: Vec<span::Event>,
    is_error: bool,
) -> Span {
    Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: parent_span_id.to_vec(),
        flags: 1,
        name: name.to_string(),
        kind: kind as i32,
        start_time_unix_nano,
        end_time_unix_nano,
        attributes,
        events,
        status: Some(Status {
            message: if is_error {
                "Synthetic demo error".to_string()
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
    }
}

fn demo_event(time_unix_nano: u64, name: &str, attributes: Vec<KeyValue>) -> span::Event {
    span::Event {
        time_unix_nano,
        name: name.to_string(),
        attributes,
        ..Default::default()
    }
}

fn random_id(rng: &mut impl RngCore, len: usize) -> Vec<u8> {
    let mut id = vec![0; len];
    rng.fill_bytes(&mut id);
    id
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![
            kv_string("service.name", service),
            kv_string("service.version", "1.3.0"),
            kv_string("service.instance.id", &format!("{service}-demo")),
            kv_string("deployment.environment", "production"),
            kv_string("telemetry.sdk.language", "rust"),
            kv_string("telemetry.sdk.name", "parseable-otel-demo"),
        ],
        ..Default::default()
    }
}

fn random_cumulative_value(rng: &mut impl Rng, sequence: u64, average_step: u64) -> u64 {
    let step = average_step.max(2);
    sequence
        .max(1)
        .saturating_mul(step)
        .saturating_add(rng.gen_range(0..step))
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

fn kv_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::BoolValue(value)),
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

        for resource_spans in &batch.traces.resource_spans {
            let spans = &resource_spans.scope_spans[0].spans;
            assert_eq!(spans.len(), 13);
            let root = &spans[0];
            assert_eq!(root.trace_id.len(), 16);
            assert_eq!(root.span_id.len(), 8);
            assert!(root.parent_span_id.is_empty());
            for span in &spans[1..] {
                assert_eq!(span.trace_id, root.trace_id);
                assert_eq!(span.span_id.len(), 8);
                assert!(
                    spans
                        .iter()
                        .any(|parent| parent.span_id == span.parent_span_id)
                );
                assert!(span.start_time_unix_nano >= root.start_time_unix_nano);
                assert!(span.end_time_unix_nano <= root.end_time_unix_nano);
            }
        }
    }

    #[test]
    fn every_generated_trace_record_has_service_name() {
        let batch = build_batch(7);
        let records = crate::otel::traces::flatten_otel_traces_protobuf(&batch.traces, "default");
        assert!(!records.is_empty());
        assert!(records.iter().all(|record| {
            record
                .get("service.name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|service| !service.is_empty())
        }));
    }

    #[test]
    fn generated_logs_include_python_context_attributes() {
        let batch = build_batch(7);
        let log = &batch.logs.resource_logs[0].scope_logs[0].log_records[0];
        let keys: Vec<&str> = log
            .attributes
            .iter()
            .map(|attribute| attribute.key.as_str())
            .collect();
        for key in [
            "service",
            "k8s.namespace.name",
            "k8s.pod.name",
            "k8s.cluster.name",
            "cloud.provider",
            "cloud.region",
            "http.method",
            "http.url",
            "http.response.status_code",
            "trace.id",
            "span.id",
            "net.peer.ip",
            "container.id",
        ] {
            assert!(keys.contains(&key), "missing log attribute {key}");
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
    fn generated_metric_series_are_stable_and_all_values_change() {
        let mut values_by_series =
            std::collections::HashMap::<(String, String), std::collections::HashSet<String>>::new();

        for sequence in 1..=8 {
            let batch = build_batch(sequence);
            for resource_metrics in batch.metrics.resource_metrics {
                let resource_attributes = resource_metrics.resource.unwrap().attributes;
                for metric in &resource_metrics.scope_metrics[0].metrics {
                    let data_points = match &metric.data {
                        Some(metric::Data::Sum(sum)) => &sum.data_points,
                        Some(metric::Data::Gauge(gauge)) => &gauge.data_points,
                        _ => panic!("demo metric must be a sum or gauge"),
                    };
                    for data_point in data_points {
                        let mut labels: Vec<(String, String)> = resource_attributes
                            .iter()
                            .chain(&data_point.attributes)
                            .map(|attribute| {
                                (attribute.key.clone(), format!("{:?}", attribute.value))
                            })
                            .collect();
                        labels.sort_unstable();
                        let series = format!("{labels:?}");
                        let value = format!("{:?}", data_point.value);
                        values_by_series
                            .entry((metric.name.clone(), series))
                            .or_default()
                            .insert(value);
                    }
                }
            }
        }

        assert_eq!(values_by_series.len(), SERVICES.len() * 17);
        for ((metric_name, _), values) in values_by_series {
            assert!(
                values.len() > 1,
                "metric series {metric_name} did not change value"
            );
        }
    }

    #[test]
    fn cumulative_metric_rates_are_randomized_and_monotonic() {
        use rand::{SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(42);
        let values: Vec<u64> = (1..=64)
            .map(|sequence| random_cumulative_value(&mut rng, sequence, 10))
            .collect();
        assert!(values.windows(2).all(|window| window[0] < window[1]));

        let rates: std::collections::HashSet<u64> = values
            .windows(2)
            .map(|window| window[1] - window[0])
            .collect();
        assert!(rates.len() > 1);
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
