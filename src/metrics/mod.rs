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

pub mod prom_utils;
use crate::{
    handlers::{TelemetryType, http::metrics_path},
    stats::FullStats,
};
use actix_web::Responder;
use actix_web_prometheus::{PrometheusMetrics, PrometheusMetricsBuilder};
use error::MetricsError;
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};

pub const METRICS_NAMESPACE: &str = env!("CARGO_PKG_NAME");

pub static METRICS_REGISTRY: Lazy<Registry> = Lazy::new(|| {
    let registry = Registry::new();
    custom_metrics(&registry);
    registry
});

pub static EVENTS_INGESTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_ingested", "Events ingested for a stream").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_ingested_size",
            "Events ingested size bytes for a stream",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("storage_size", "Storage size bytes for a stream").namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_DELETED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_deleted", "Events deleted for a stream").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_DELETED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_deleted_size",
            "Events deleted size bytes for a stream",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static DELETED_EVENTS_STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "deleted_events_storage_size",
            "Deleted events storage size bytes for a stream",
        )
        .namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static LIFETIME_EVENTS_INGESTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "lifetime_events_ingested",
            "Lifetime events ingested for a stream",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static LIFETIME_EVENTS_INGESTED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "lifetime_events_ingested_size",
            "Lifetime events ingested size bytes for a stream",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static LIFETIME_EVENTS_STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "lifetime_events_storage_size",
            "Lifetime events storage size bytes for a stream",
        )
        .namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "events_ingested_date",
            "Events ingested for a stream on a particular date",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format", "date"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_SIZE_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "events_ingested_size_date",
            "Events ingested size in bytes for a stream on a particular date",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format", "date"],
    )
    .expect("metric can be created")
});

pub static EVENTS_STORAGE_SIZE_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "events_storage_size_date",
            "Events storage size in bytes for a stream on a particular date",
        )
        .namespace(METRICS_NAMESPACE),
        &["type", "stream", "format", "date"],
    )
    .expect("metric can be created")
});

pub static STAGING_FILES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("staging_files", "Active Staging files").namespace(METRICS_NAMESPACE),
        &["stream"],
    )
    .expect("metric can be created")
});

pub static QUERY_EXECUTE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new("query_execute_time", "Query execute time").namespace(METRICS_NAMESPACE),
        &["stream"],
    )
    .expect("metric can be created")
});

pub static QUERY_CACHE_HIT: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("QUERY_CACHE_HIT", "Full Cache hit").namespace(METRICS_NAMESPACE),
        &["stream"],
    )
    .expect("metric can be created")
});

pub static ALERTS_STATES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("alerts_states", "Alerts States").namespace(METRICS_NAMESPACE),
        &["stream", "name", "state"],
    )
    .expect("metric can be created")
});

// Billing Metrics - Counter type metrics for billing/usage tracking
pub static TOTAL_EVENTS_INGESTED_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_events_ingested_by_date",
            "Total events ingested by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_EVENTS_INGESTED_SIZE_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_events_ingested_size_by_date",
            "Total events ingested size in bytes by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_PARQUETS_STORED_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_parquets_stored_by_date",
            "Total parquet files stored by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_PARQUETS_STORED_SIZE_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_parquets_stored_size_by_date",
            "Total parquet files stored size in bytes by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_QUERY_CALLS_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("total_query_calls_by_date", "Total query calls by date")
            .namespace(METRICS_NAMESPACE),
        &["date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_FILES_SCANNED_IN_QUERY_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_files_scanned_in_query_by_date",
            "Total files scanned in queries by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_BYTES_SCANNED_IN_QUERY_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_bytes_scanned_in_query_by_date",
            "Total bytes scanned in queries by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_OBJECT_STORE_CALLS_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_object_store_calls_by_date",
            "Total object store calls by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["method", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_FILES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE: Lazy<IntCounterVec> =
    Lazy::new(|| {
        IntCounterVec::new(
            Opts::new(
                "total_files_scanned_in_object_store_calls_by_date",
                "Total files scanned in object store calls by date",
            )
            .namespace(METRICS_NAMESPACE),
            &["method", "date"],
        )
        .expect("metric can be created")
    });

pub static TOTAL_BYTES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE: Lazy<IntCounterVec> =
    Lazy::new(|| {
        IntCounterVec::new(
            Opts::new(
                "total_bytes_scanned_in_object_store_calls_by_date",
                "Total bytes scanned in object store calls by date",
            )
            .namespace(METRICS_NAMESPACE),
            &["method", "date"],
        )
        .expect("metric can be created")
    });

pub static TOTAL_INPUT_LLM_TOKENS_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_input_llm_tokens_by_date",
            "Total input LLM tokens used by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider", "model", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_OUTPUT_LLM_TOKENS_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_output_llm_tokens_by_date",
            "Total output LLM tokens used by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider", "model", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_CACHED_LLM_TOKENS_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_cached_llm_tokens_by_date",
            "Total cached LLM tokens used by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider", "model", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_REASONING_LLM_TOKENS_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_reasoning_llm_tokens_by_date",
            "Total reasoning LLM tokens used by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider", "model", "date"],
    )
    .expect("metric can be created")
});

pub static STORAGE_REQUEST_RESPONSE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new("storage_request_response_time", "Storage Request Latency")
            .namespace(METRICS_NAMESPACE),
        &["provider", "method", "status"],
    )
    .expect("metric can be created")
});

pub static TOTAL_METRICS_COLLECTED_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_metrics_collected_by_date",
            "Total metrics collected by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["team", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_METRICS_COLLECTED_SIZE_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_metrics_collected_size_by_date",
            "Total metrics collected size in bytes by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["team", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_LOGS_COLLECTED_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_logs_collected_by_date",
            "Total logs collected by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["team", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_LOGS_COLLECTED_SIZE_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_logs_collected_size_by_date",
            "Total logs collected size in bytes by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["team", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_TRACES_COLLECTED_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_traces_collected_by_date",
            "Total traces collected by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["team", "date"],
    )
    .expect("metric can be created")
});

pub static TOTAL_TRACES_COLLECTED_SIZE_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_traces_collected_size_by_date",
            "Total traces collected size in bytes by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["team", "date"],
    )
    .expect("metric can be created")
});

fn custom_metrics(registry: &Registry) {
    registry
        .register(Box::new(EVENTS_INGESTED.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STORAGE_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_DELETED.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_DELETED_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(DELETED_EVENTS_STORAGE_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(LIFETIME_EVENTS_INGESTED.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(LIFETIME_EVENTS_INGESTED_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(LIFETIME_EVENTS_STORAGE_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_SIZE_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_STORAGE_SIZE_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STAGING_FILES.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(QUERY_EXECUTE_TIME.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(QUERY_CACHE_HIT.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(ALERTS_STATES.clone()))
        .expect("metric can be registered");
    // Register billing metrics
    registry
        .register(Box::new(TOTAL_EVENTS_INGESTED_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_EVENTS_INGESTED_SIZE_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_PARQUETS_STORED_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_PARQUETS_STORED_SIZE_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_QUERY_CALLS_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_FILES_SCANNED_IN_QUERY_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_BYTES_SCANNED_IN_QUERY_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_OBJECT_STORE_CALLS_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(
            TOTAL_FILES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE.clone(),
        ))
        .expect("metric can be registered");
    registry
        .register(Box::new(
            TOTAL_BYTES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE.clone(),
        ))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_INPUT_LLM_TOKENS_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_OUTPUT_LLM_TOKENS_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_CACHED_LLM_TOKENS_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_REASONING_LLM_TOKENS_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STORAGE_REQUEST_RESPONSE_TIME.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_METRICS_COLLECTED_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_METRICS_COLLECTED_SIZE_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_LOGS_COLLECTED_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_LOGS_COLLECTED_SIZE_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_TRACES_COLLECTED_BY_DATE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(TOTAL_TRACES_COLLECTED_SIZE_BY_DATE.clone()))
        .expect("metric can be registered");
}

pub fn build_metrics_handler() -> PrometheusMetrics {
    // Force initialization of the global registry
    let _ = &*METRICS_REGISTRY;

    let prometheus = PrometheusMetricsBuilder::new(METRICS_NAMESPACE)
        .registry(METRICS_REGISTRY.clone())
        .endpoint(metrics_path().as_str())
        .build()
        .expect("Prometheus initialization");

    prom_process_metrics(&prometheus);
    prometheus
}

#[cfg(target_os = "linux")]
fn prom_process_metrics(metrics: &PrometheusMetrics) {
    use prometheus::process_collector::ProcessCollector;
    metrics
        .registry
        .register(Box::new(ProcessCollector::for_self()))
        .expect("metric can be registered");
}

#[cfg(not(target_os = "linux"))]
fn prom_process_metrics(_metrics: &PrometheusMetrics) {}

pub async fn fetch_stats_from_storage(stream_name: &str, stats: FullStats) {
    EVENTS_INGESTED
        .with_label_values(&[stream_name, "json"])
        .set(stats.current_stats.events as i64);
    EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, "json"])
        .set(stats.current_stats.ingestion as i64);
    STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .set(stats.current_stats.storage as i64);
    EVENTS_DELETED
        .with_label_values(&[stream_name, "json"])
        .set(stats.deleted_stats.events as i64);
    EVENTS_DELETED_SIZE
        .with_label_values(&[stream_name, "json"])
        .set(stats.deleted_stats.ingestion as i64);
    DELETED_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .set(stats.deleted_stats.storage as i64);

    LIFETIME_EVENTS_INGESTED
        .with_label_values(&[stream_name, "json"])
        .set(stats.lifetime_stats.events as i64);
    LIFETIME_EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, "json"])
        .set(stats.lifetime_stats.ingestion as i64);
    LIFETIME_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .set(stats.lifetime_stats.storage as i64);
}

// Helper functions for tracking billing metrics
pub fn increment_events_ingested_by_date(count: u64, date: &str) {
    TOTAL_EVENTS_INGESTED_BY_DATE
        .with_label_values(&[date])
        .inc_by(count);
}

pub fn increment_events_ingested_size_by_date(
    size: u64,
    date: &str,
    telemetry_type: TelemetryType,
) {
    TOTAL_EVENTS_INGESTED_SIZE_BY_DATE
        .with_label_values(&[date])
        .inc_by(size);
    match telemetry_type {
        TelemetryType::Logs | TelemetryType::Events => {
            TOTAL_LOGS_COLLECTED_SIZE_BY_DATE
                .with_label_values(&["all", date])
                .inc_by(size);
        }
        TelemetryType::Metrics => {
            TOTAL_METRICS_COLLECTED_SIZE_BY_DATE
                .with_label_values(&["all", date])
                .inc_by(size);
        }
        TelemetryType::Traces => {
            TOTAL_TRACES_COLLECTED_SIZE_BY_DATE
                .with_label_values(&["all", date])
                .inc_by(size);
        }
    }
}

pub fn increment_parquets_stored_by_date(date: &str) {
    TOTAL_PARQUETS_STORED_BY_DATE
        .with_label_values(&[date])
        .inc();
}

pub fn increment_parquets_stored_size_by_date(size: u64, date: &str) {
    TOTAL_PARQUETS_STORED_SIZE_BY_DATE
        .with_label_values(&[date])
        .inc_by(size);
}

pub fn increment_query_calls_by_date(date: &str) {
    TOTAL_QUERY_CALLS_BY_DATE.with_label_values(&[date]).inc();
}

pub fn increment_files_scanned_in_query_by_date(count: u64, date: &str) {
    TOTAL_FILES_SCANNED_IN_QUERY_BY_DATE
        .with_label_values(&[date])
        .inc_by(count);
}

pub fn increment_bytes_scanned_in_query_by_date(bytes: u64, date: &str) {
    TOTAL_BYTES_SCANNED_IN_QUERY_BY_DATE
        .with_label_values(&[date])
        .inc_by(bytes);
}

pub fn increment_object_store_calls_by_date(method: &str, date: &str) {
    TOTAL_OBJECT_STORE_CALLS_BY_DATE
        .with_label_values(&[method, date])
        .inc();
}

pub fn increment_files_scanned_in_object_store_calls_by_date(method: &str, count: u64, date: &str) {
    TOTAL_FILES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE
        .with_label_values(&[method, date])
        .inc_by(count);
}

pub fn increment_bytes_scanned_in_object_store_calls_by_date(method: &str, bytes: u64, date: &str) {
    TOTAL_BYTES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE
        .with_label_values(&[method, date])
        .inc_by(bytes);
}

pub fn increment_input_llm_tokens_by_date(provider: &str, model: &str, tokens: u64, date: &str) {
    TOTAL_INPUT_LLM_TOKENS_BY_DATE
        .with_label_values(&[provider, model, date])
        .inc_by(tokens);
}

pub fn increment_output_llm_tokens_by_date(provider: &str, model: &str, tokens: u64, date: &str) {
    TOTAL_OUTPUT_LLM_TOKENS_BY_DATE
        .with_label_values(&[provider, model, date])
        .inc_by(tokens);
}

pub fn increment_cached_llm_tokens_by_date(provider: &str, model: &str, tokens: u64, date: &str) {
    TOTAL_CACHED_LLM_TOKENS_BY_DATE
        .with_label_values(&[provider, model, date])
        .inc_by(tokens);
}

pub fn increment_reasoning_llm_tokens_by_date(
    provider: &str,
    model: &str,
    tokens: u64,
    date: &str,
) {
    TOTAL_REASONING_LLM_TOKENS_BY_DATE
        .with_label_values(&[provider, model, date])
        .inc_by(tokens);
}

pub fn increment_metrics_collected_by_date(count: u64, date: &str) {
    TOTAL_METRICS_COLLECTED_BY_DATE
        .with_label_values(&["all", date])
        .inc_by(count);
}

pub fn increment_logs_collected_by_date(count: u64, date: &str) {
    TOTAL_LOGS_COLLECTED_BY_DATE
        .with_label_values(&["all", date])
        .inc_by(count);
}

pub fn increment_traces_collected_by_date(count: u64, date: &str) {
    TOTAL_TRACES_COLLECTED_BY_DATE
        .with_label_values(&["all", date])
        .inc_by(count);
}

use actix_web::HttpResponse;
use prometheus::Encoder;

pub async fn get() -> Result<impl Responder, MetricsError> {
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    let metric_families = METRICS_REGISTRY.gather();
    encoder.encode(&metric_families, &mut buffer).map_err(|e| {
        MetricsError::Custom(e.to_string(), http::StatusCode::INTERNAL_SERVER_ERROR)
    })?;

    Ok(HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body(buffer))
}

pub mod error {

    use actix_web::http::header::ContentType;
    use http::StatusCode;

    #[derive(Debug, thiserror::Error)]
    pub enum MetricsError {
        #[error("{0}")]
        Custom(String, StatusCode),
    }

    impl actix_web::ResponseError for MetricsError {
        fn status_code(&self) -> http::StatusCode {
            match self {
                Self::Custom(_, status) => *status,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }
}
