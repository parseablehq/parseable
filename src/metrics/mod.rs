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
use std::sync::OnceLock;

use crate::{
    handlers::{TelemetryType, http::metrics_path},
    stats::FullStats,
};
use actix_web::Responder;
use actix_web_prometheus::{PrometheusMetrics, PrometheusMetricsBuilder};
use error::MetricsError;
use once_cell::sync::Lazy;
use prometheus::{
    Gauge, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry,
    core::{Atomic, AtomicF64},
};

pub const METRICS_NAMESPACE: &str = env!("CARGO_PKG_NAME");

pub static METRICS_REGISTRY: Lazy<Registry> = Lazy::new(|| {
    let registry = Registry::new();
    custom_metrics(&registry);
    registry
});

pub static EVENTS_INGESTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_ingested", "Events ingested for a stream").namespace(METRICS_NAMESPACE),
        &["stream", "format", "tenant_id"],
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
        &["stream", "format", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("storage_size", "Storage size bytes for a stream").namespace(METRICS_NAMESPACE),
        &["type", "stream", "format", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static EVENTS_DELETED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_deleted", "Events deleted for a stream").namespace(METRICS_NAMESPACE),
        &["stream", "format", "tenant_id"],
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
        &["stream", "format", "tenant_id"],
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
        &["type", "stream", "format", "tenant_id"],
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
        &["stream", "format", "tenant_id"],
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
        &["stream", "format", "tenant_id"],
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
        &["type", "stream", "format", "tenant_id"],
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
        &["stream", "format", "date", "tenant_id"],
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
        &["stream", "format", "date", "tenant_id"],
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
        &["type", "stream", "format", "date", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static STAGING_FILES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("staging_files", "Active Staging files").namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static PROCESS_CPU_USAGE_PERCENT_AVG: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "process_cpu_usage_percent_avg",
            "Lifetime average CPU usage percent for this Parseable process",
        )
        .namespace(METRICS_NAMESPACE),
    )
    .expect("metric can be created")
});

pub static PROCESS_MEMORY_BYTES_AVG: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "process_memory_bytes_avg",
            "Lifetime average resident memory used by this Parseable process in bytes",
        )
        .namespace(METRICS_NAMESPACE),
    )
    .expect("metric can be created")
});
pub static PROCESS_METRICS_INIT: OnceLock<(f64, u64)> = OnceLock::new();
struct ProcessMetricsAccumulator {
    cpu_usage_avg: AtomicF64,
    memory_bytes_avg: AtomicF64,
}

impl Default for ProcessMetricsAccumulator {
    fn default() -> Self {
        // PROCESS_METRICS_INIT must be initialized by now
        let (cpu, mem) = *PROCESS_METRICS_INIT.get().unwrap();
        Self {
            cpu_usage_avg: AtomicF64::new(cpu),
            memory_bytes_avg: AtomicF64::new(mem as f64),
        }
    }
}

impl ProcessMetricsAccumulator {
    fn record(&self, cpu_usage_percent: f64, memory_bytes: u64) -> (f64, f64) {
        // Exponentially Weighted Moving Average is better than
        // a lifetime average
        // A spike which occurred 5 days ago should not affect the average utilization
        // for the last minute
        // α = 1 - exp(-Δt / τ) = 1 - exp(-5/60) ≈ 0.0800
        // S_new = S_old + α * (x_new - S_old)
        let s_cpu_old = self.cpu_usage_avg.get();
        let s_cpu_new = s_cpu_old + 0.08 * (cpu_usage_percent - s_cpu_old);

        let s_mem_old = self.memory_bytes_avg.get();
        let s_mem_new = s_mem_old + 0.08 * (memory_bytes as f64 - s_mem_old);

        // update accumulator
        self.cpu_usage_avg.set(s_cpu_new);
        self.memory_bytes_avg.set(s_mem_new);

        (s_cpu_new, s_mem_new)
    }
}

static PROCESS_METRICS_ACCUMULATOR: Lazy<ProcessMetricsAccumulator> =
    Lazy::new(ProcessMetricsAccumulator::default);

pub fn record_process_metrics_sample(cpu_usage_percent: f64, memory_bytes: u64) {
    if PROCESS_METRICS_INIT.get().is_none() {
        // first measurement
        let _ = PROCESS_METRICS_INIT.set((cpu_usage_percent, memory_bytes));
    }
    let (average_cpu_usage, average_memory_bytes) =
        PROCESS_METRICS_ACCUMULATOR.record(cpu_usage_percent, memory_bytes);
    PROCESS_CPU_USAGE_PERCENT_AVG.set(average_cpu_usage);
    PROCESS_MEMORY_BYTES_AVG.set(average_memory_bytes);
}

#[cfg(test)]
mod process_metrics_tests {
    use crate::metrics::PROCESS_METRICS_INIT;

    use super::ProcessMetricsAccumulator;

    #[test]
    fn averages_process_metric_samples() {
        // init PROCESS_METRICS_INIT
        PROCESS_METRICS_INIT.get_or_init(|| (10.0, 100));
        let accumulator = ProcessMetricsAccumulator::default();

        assert_eq!(accumulator.record(10.0, 100), (10.0, 100.0));
        assert_eq!(accumulator.record(20.0, 300), (10.8, 116.0));
    }
}

pub static QUERY_EXECUTE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new("query_execute_time", "Query execute time").namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static QUERY_CACHE_HIT: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("QUERY_CACHE_HIT", "Full Cache hit").namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static ALERTS_STATES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("alerts_states", "Alerts States").namespace(METRICS_NAMESPACE),
        &["stream", "name", "state", "tenant_id"],
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
        &["date", "tenant_id"],
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
        &["date", "tenant_id"],
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
        &["date", "tenant_id"],
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
        &["date", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static TOTAL_QUERY_CALLS_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("total_query_calls_by_date", "Total query calls by date")
            .namespace(METRICS_NAMESPACE),
        &["date", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static TOTAL_FILES_SCANNED_IN_HOTTIER_BY_DATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "total_files_scanned_in_hottier_by_date",
            "Total files scanned in hottier by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "date", "tenant_id"],
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
        &["date", "tenant_id"],
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
        &["date", "tenant_id"],
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
        &["method", "date", "tenant_id"],
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
            &["method", "date", "tenant_id"],
        )
        .expect("metric can be created")
    });

pub static PARTIAL_FILE_SCANS_IN_OBJECT_STORE_CALLS_BY_DATE: Lazy<IntCounterVec> =
    Lazy::new(|| {
        IntCounterVec::new(
            Opts::new(
                "partial_file_scans_in_object_store_calls_by_date",
                "Partial file scans in object store calls by date",
            )
            .namespace(METRICS_NAMESPACE),
            &["method", "date", "tenant_id"],
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
            &["method", "date", "tenant_id"],
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
        &["provider", "model", "date", "tenant_id"],
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
        &["provider", "model", "date", "tenant_id"],
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
        &["provider", "model", "date", "tenant_id"],
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
        &["provider", "model", "date", "tenant_id"],
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

pub static STORAGE_REQUESTS_INFLIGHT: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "storage_requests_inflight",
            "Number of in-flight object store requests",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider", "method"],
    )
    .expect("metric can be created")
});

pub static STORAGE_READ_BYTES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "storage_read_bytes_total",
            "Logical bytes requested from an object store",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider"],
    )
    .expect("metric can be created")
});

pub static STORAGE_READ_RANGES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "storage_read_ranges_total",
            "Logical byte ranges requested from an object store",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_SOURCE_WATERMARK_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "hot_tier_source_watermark_seconds",
            "Latest source timestamp observed by hot tier",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_ACTIVE_MISSING_FILES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "hot_tier_active_missing_files",
            "Missing files in the active hot-tier window",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_ACTIVE_MISSING_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "hot_tier_active_missing_bytes",
            "Missing bytes in the active hot-tier window",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_OLDEST_MISSING_LAG_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "hot_tier_oldest_missing_lag_seconds",
            "Age of the oldest missing hot-tier file",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_INFLIGHT_FILES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "hot_tier_inflight_files",
            "Hot-tier files currently being processed",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_USED_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "hot_tier_used_bytes",
            "Bytes committed in hot-tier runtime state",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_RESERVED_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "hot_tier_reserved_bytes",
            "Bytes reserved by in-flight hot-tier downloads",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_DOWNLOAD_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "hot_tier_download_bytes",
            "Bytes successfully downloaded into hot tier",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_DOWNLOAD_OUTCOMES: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("hot_tier_download_outcomes", "Hot-tier download outcomes")
            .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id", "outcome"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_INVENTORY_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "hot_tier_inventory_duration_seconds",
            "Hot-tier inventory duration",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
    )
    .expect("metric can be created")
});

pub static HOT_TIER_TICK_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new("hot_tier_tick_duration_seconds", "Hot-tier pass duration")
            .namespace(METRICS_NAMESPACE),
        &["stream", "tenant_id"],
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
        &["team", "date", "tenant_id"],
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
        &["team", "date", "tenant_id"],
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
        &["team", "date", "tenant_id"],
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
        &["team", "date", "tenant_id"],
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
        &["team", "date", "tenant_id"],
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
        &["team", "date", "tenant_id"],
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
        .register(Box::new(PROCESS_CPU_USAGE_PERCENT_AVG.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(PROCESS_MEMORY_BYTES_AVG.clone()))
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
            PARTIAL_FILE_SCANS_IN_OBJECT_STORE_CALLS_BY_DATE.clone(),
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
        .register(Box::new(STORAGE_REQUESTS_INFLIGHT.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STORAGE_READ_BYTES_TOTAL.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STORAGE_READ_RANGES_TOTAL.clone()))
        .expect("metric can be registered");
    for metric in [
        &*HOT_TIER_SOURCE_WATERMARK_SECONDS,
        &*HOT_TIER_ACTIVE_MISSING_FILES,
        &*HOT_TIER_ACTIVE_MISSING_BYTES,
        &*HOT_TIER_OLDEST_MISSING_LAG_SECONDS,
        &*HOT_TIER_INFLIGHT_FILES,
        &*HOT_TIER_USED_BYTES,
        &*HOT_TIER_RESERVED_BYTES,
    ] {
        registry
            .register(Box::new(metric.clone()))
            .expect("metric can be registered");
    }
    registry
        .register(Box::new(HOT_TIER_DOWNLOAD_BYTES.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(HOT_TIER_DOWNLOAD_OUTCOMES.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(HOT_TIER_INVENTORY_DURATION.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(HOT_TIER_TICK_DURATION.clone()))
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

pub async fn fetch_stats_from_storage(stream_name: &str, stats: FullStats, tenant_id: &str) {
    EVENTS_INGESTED
        .with_label_values(&[stream_name, "json", tenant_id])
        .set(stats.current_stats.events as i64);
    EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, "json", tenant_id])
        .set(stats.current_stats.ingestion as i64);
    STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet", tenant_id])
        .set(stats.current_stats.storage as i64);
    EVENTS_DELETED
        .with_label_values(&[stream_name, "json", tenant_id])
        .set(stats.deleted_stats.events as i64);
    EVENTS_DELETED_SIZE
        .with_label_values(&[stream_name, "json", tenant_id])
        .set(stats.deleted_stats.ingestion as i64);
    DELETED_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet", tenant_id])
        .set(stats.deleted_stats.storage as i64);

    LIFETIME_EVENTS_INGESTED
        .with_label_values(&[stream_name, "json", tenant_id])
        .set(stats.lifetime_stats.events as i64);
    LIFETIME_EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, "json", tenant_id])
        .set(stats.lifetime_stats.ingestion as i64);
    LIFETIME_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet", tenant_id])
        .set(stats.lifetime_stats.storage as i64);
}

// Helper functions for tracking billing metrics
pub fn increment_events_ingested_by_date(count: u64, date: &str, tenant_id: &str) {
    TOTAL_EVENTS_INGESTED_BY_DATE
        .with_label_values(&[date, tenant_id])
        .inc_by(count);
}

pub fn increment_events_ingested_size_by_date(
    size: u64,
    date: &str,
    telemetry_type: TelemetryType,
    tenant_id: &str,
) {
    TOTAL_EVENTS_INGESTED_SIZE_BY_DATE
        .with_label_values(&[date, tenant_id])
        .inc_by(size);
    match telemetry_type {
        TelemetryType::Logs | TelemetryType::Events => {
            TOTAL_LOGS_COLLECTED_SIZE_BY_DATE
                .with_label_values(&["all", date, tenant_id])
                .inc_by(size);
        }
        TelemetryType::Metrics => {
            TOTAL_METRICS_COLLECTED_SIZE_BY_DATE
                .with_label_values(&["all", date, tenant_id])
                .inc_by(size);
        }
        TelemetryType::Traces => {
            TOTAL_TRACES_COLLECTED_SIZE_BY_DATE
                .with_label_values(&["all", date, tenant_id])
                .inc_by(size);
        }
    }
}

pub fn increment_parquets_stored_by_date(date: &str, tenant_id: &str) {
    TOTAL_PARQUETS_STORED_BY_DATE
        .with_label_values(&[date, tenant_id])
        .inc();
}

pub fn increment_parquets_stored_size_by_date(size: u64, date: &str, tenant_id: &str) {
    TOTAL_PARQUETS_STORED_SIZE_BY_DATE
        .with_label_values(&[date, tenant_id])
        .inc_by(size);
}

pub fn increment_query_calls_by_date(date: &str, tenant_id: &str) {
    TOTAL_QUERY_CALLS_BY_DATE
        .with_label_values(&[date, tenant_id])
        .inc();
}

pub fn increment_files_scanned_in_query_by_date(count: u64, date: &str, tenant_id: &str) {
    TOTAL_FILES_SCANNED_IN_QUERY_BY_DATE
        .with_label_values(&[date, tenant_id])
        .inc_by(count);
}

pub fn increment_files_scanned_in_hottier_by_date(
    count: u64,
    date: &str,
    tenant_id: &str,
    stream_name: &str,
) {
    TOTAL_FILES_SCANNED_IN_HOTTIER_BY_DATE
        .with_label_values(&[stream_name, date, tenant_id])
        .inc_by(count);
}

pub fn increment_bytes_scanned_in_query_by_date(bytes: u64, date: &str, tenant_id: &str) {
    TOTAL_BYTES_SCANNED_IN_QUERY_BY_DATE
        .with_label_values(&[date, tenant_id])
        .inc_by(bytes);
}

pub fn increment_object_store_calls_by_date(method: &str, date: &str, tenant_id: &str) {
    TOTAL_OBJECT_STORE_CALLS_BY_DATE
        .with_label_values(&[method, date, tenant_id])
        .inc();
}

pub fn increment_partial_file_scans_in_object_store_calls_by_date(
    method: &str,
    count: u64,
    date: &str,
    tenant_id: &str,
) {
    PARTIAL_FILE_SCANS_IN_OBJECT_STORE_CALLS_BY_DATE
        .with_label_values(&[method, date, tenant_id])
        .inc_by(count);
}

pub fn increment_files_scanned_in_object_store_calls_by_date(
    method: &str,
    count: u64,
    date: &str,
    tenant_id: &str,
) {
    TOTAL_FILES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE
        .with_label_values(&[method, date, tenant_id])
        .inc_by(count);
}

pub fn increment_bytes_scanned_in_object_store_calls_by_date(
    method: &str,
    bytes: u64,
    date: &str,
    tenant_id: &str,
) {
    TOTAL_BYTES_SCANNED_IN_OBJECT_STORE_CALLS_BY_DATE
        .with_label_values(&[method, date, tenant_id])
        .inc_by(bytes);
}

pub fn increment_input_llm_tokens_by_date(
    provider: &str,
    model: &str,
    tokens: u64,
    date: &str,
    tenant_id: &str,
) {
    TOTAL_INPUT_LLM_TOKENS_BY_DATE
        .with_label_values(&[provider, model, date, tenant_id])
        .inc_by(tokens);
}

pub fn increment_output_llm_tokens_by_date(
    provider: &str,
    model: &str,
    tokens: u64,
    date: &str,
    tenant_id: &str,
) {
    TOTAL_OUTPUT_LLM_TOKENS_BY_DATE
        .with_label_values(&[provider, model, date, tenant_id])
        .inc_by(tokens);
}

pub fn increment_cached_llm_tokens_by_date(
    provider: &str,
    model: &str,
    tokens: u64,
    date: &str,
    tenant_id: &str,
) {
    TOTAL_CACHED_LLM_TOKENS_BY_DATE
        .with_label_values(&[provider, model, date, tenant_id])
        .inc_by(tokens);
}

pub fn increment_reasoning_llm_tokens_by_date(
    provider: &str,
    model: &str,
    tokens: u64,
    date: &str,
    tenant_id: &str,
) {
    TOTAL_REASONING_LLM_TOKENS_BY_DATE
        .with_label_values(&[provider, model, date, tenant_id])
        .inc_by(tokens);
}

pub fn increment_metrics_collected_by_date(count: u64, date: &str, tenant_id: &str) {
    TOTAL_METRICS_COLLECTED_BY_DATE
        .with_label_values(&["all", date, tenant_id])
        .inc_by(count);
}

pub fn increment_logs_collected_by_date(count: u64, date: &str, tenant_id: &str) {
    TOTAL_LOGS_COLLECTED_BY_DATE
        .with_label_values(&["all", date, tenant_id])
        .inc_by(count);
}

pub fn increment_traces_collected_by_date(count: u64, date: &str, tenant_id: &str) {
    TOTAL_TRACES_COLLECTED_BY_DATE
        .with_label_values(&["all", date, tenant_id])
        .inc_by(count);
}

use actix_web::HttpResponse;
use prometheus::Encoder;

pub async fn get() -> Result<impl Responder, MetricsError> {
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    let metric_families = METRICS_REGISTRY.gather();
    encoder.encode(&metric_families, &mut buffer).map_err(|e| {
        MetricsError::Custom(
            e.to_string(),
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;

    Ok(HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body(buffer))
}

pub mod error {

    use actix_web::http::StatusCode;
    use actix_web::http::header::ContentType;

    #[derive(Debug, thiserror::Error)]
    pub enum MetricsError {
        #[error("{0}")]
        Custom(String, StatusCode),
    }

    impl actix_web::ResponseError for MetricsError {
        fn status_code(&self) -> StatusCode {
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
