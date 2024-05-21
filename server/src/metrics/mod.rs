/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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
pub mod storage;
use std::sync::Mutex;

use crate::{handlers::http::metrics_path, metadata::STREAM_INFO, metrics, option::CONFIG};
use actix_web_prometheus::{PrometheusMetrics, PrometheusMetricsBuilder};
use clokwerk::AsyncScheduler;
use clokwerk::Job;
use clokwerk::TimeUnits;
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};
use std::thread;
use std::time::Duration;

pub const METRICS_NAMESPACE: &str = env!("CARGO_PKG_NAME");
type SchedulerHandle = thread::JoinHandle<()>;

static METRIC_SCHEDULER_HANDLER: Lazy<Mutex<Option<SchedulerHandle>>> =
    Lazy::new(|| Mutex::new(None));

pub static EVENTS_INGESTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_ingested", "Events ingested").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_TODAY: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_ingested_today", "Events ingested today").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_ingested_size", "Events ingested size bytes")
            .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_SIZE_TODAY: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_ingested_size_today",
            "Events ingested size today in bytes",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("storage_size", "Storage size bytes").namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static STORAGE_SIZE_TODAY: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("storage_size_today", "Storage size today in bytes").namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_DELETED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_deleted", "Events deleted").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static EVENTS_DELETED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_deleted_size", "Events deleted size bytes").namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static DELETED_EVENTS_STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "deleted_events_storage_size",
            "Deleted events storage size bytes",
        )
        .namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
    )
    .expect("metric can be created")
});

pub static LIFETIME_EVENTS_INGESTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("lifetime_events_ingested", "Lifetime events ingested")
            .namespace(METRICS_NAMESPACE),
        &["stream", "format"],
    )
    .expect("metric can be created")
});

pub static LIFETIME_EVENTS_INGESTED_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "lifetime_events_ingested_size",
            "Lifetime events ingested size bytes",
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
            "Lifetime events storage size bytes",
        )
        .namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"],
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

fn custom_metrics(registry: &Registry) {
    registry
        .register(Box::new(EVENTS_INGESTED.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_TODAY.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(EVENTS_INGESTED_SIZE_TODAY.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STORAGE_SIZE.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(STORAGE_SIZE_TODAY.clone()))
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
}

pub fn build_metrics_handler() -> PrometheusMetrics {
    let registry = prometheus::Registry::new();
    custom_metrics(&registry);

    let prometheus = PrometheusMetricsBuilder::new(METRICS_NAMESPACE)
        .registry(registry)
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

pub async fn fetch_stats_from_storage() {
    for stream_name in STREAM_INFO.list_streams() {
        let stats = CONFIG
            .storage()
            .get_object_store()
            .get_stats(&stream_name)
            .await
            .expect("stats are loaded properly");

        EVENTS_INGESTED
            .with_label_values(&[&stream_name, "json"])
            .set(stats.current_stats.events as i64);
        EVENTS_INGESTED_TODAY
            .with_label_values(&[&stream_name, "json"])
            .set(stats.current_date_stats.events as i64);
        EVENTS_INGESTED_SIZE
            .with_label_values(&[&stream_name, "json"])
            .set(stats.current_stats.ingestion as i64);
        EVENTS_INGESTED_SIZE_TODAY
            .with_label_values(&[&stream_name, "json"])
            .set(stats.current_date_stats.ingestion as i64);
        STORAGE_SIZE
            .with_label_values(&["data", &stream_name, "parquet"])
            .set(stats.current_stats.storage as i64);
        STORAGE_SIZE_TODAY
            .with_label_values(&["data", &stream_name, "parquet"])
            .set(stats.current_date_stats.storage as i64);
        EVENTS_DELETED
            .with_label_values(&[&stream_name, "json"])
            .set(stats.deleted_stats.events as i64);
        EVENTS_DELETED_SIZE
            .with_label_values(&[&stream_name, "json"])
            .set(stats.deleted_stats.ingestion as i64);
        DELETED_EVENTS_STORAGE_SIZE
            .with_label_values(&["data", &stream_name, "parquet"])
            .set(stats.deleted_stats.storage as i64);

        LIFETIME_EVENTS_INGESTED
            .with_label_values(&[&stream_name, "json"])
            .set(stats.lifetime_stats.events as i64);
        LIFETIME_EVENTS_INGESTED_SIZE
            .with_label_values(&[&stream_name, "json"])
            .set(stats.lifetime_stats.ingestion as i64);
        LIFETIME_EVENTS_STORAGE_SIZE
            .with_label_values(&["data", &stream_name, "parquet"])
            .set(stats.lifetime_stats.storage as i64);
    }
}

fn async_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .thread_name("reset-metrics-task-thread")
        .enable_all()
        .build()
        .unwrap()
}

pub fn reset_daily_metric_from_global() {
    init_reset_daily_metric_scheduler();
}

pub fn init_reset_daily_metric_scheduler() {
    log::info!("Setting up schedular");
    let mut scheduler = AsyncScheduler::new();
    let func = move || async {
        //get retention every day at 12 am
        for stream in STREAM_INFO.list_streams() {
            metrics::EVENTS_INGESTED_TODAY
                .with_label_values(&[&stream, "json"])
                .set(0);
            metrics::EVENTS_INGESTED_SIZE_TODAY
                .with_label_values(&[&stream, "json"])
                .set(0);
            metrics::STORAGE_SIZE_TODAY
                .with_label_values(&["data", &stream, "parquet"])
                .set(0);
        }
    };

    scheduler.every(1.day()).at("00:00").run(func);

    let scheduler_handler = thread::spawn(|| {
        let rt = async_runtime();
        rt.block_on(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                scheduler.run_pending().await;
            }
        });
    });

    *METRIC_SCHEDULER_HANDLER.lock().unwrap() = Some(scheduler_handler);
    log::info!("Scheduler is initialized")
}
