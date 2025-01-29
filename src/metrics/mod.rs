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
use actix_web::HttpResponse;
use clokwerk::{AsyncScheduler, Interval};
use http::StatusCode;
use serde::Serialize;
use std::{path::Path, time::Duration};
use sysinfo::{Disks, System};
use tracing::{error, info};

use crate::{handlers::http::metrics_path, option::CONFIG, stats::FullStats};
use actix_web::Responder;
use actix_web_prometheus::{PrometheusMetrics, PrometheusMetricsBuilder};
use error::MetricsError;
use once_cell::sync::Lazy;
use prometheus::{
    GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry,
};

pub const METRICS_NAMESPACE: &str = env!("CARGO_PKG_NAME");
const SYSTEM_METRICS_INTERVAL_SECONDS: Interval = clokwerk::Interval::Minutes(1);

pub static EVENTS_INGESTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("events_ingested", "Events ingested").namespace(METRICS_NAMESPACE),
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

pub static STORAGE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("storage_size", "Storage size bytes").namespace(METRICS_NAMESPACE),
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

pub static EVENTS_INGESTED_DATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_ingested_date",
            "Events ingested on a particular date",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format", "date"],
    )
    .expect("metric can be created")
});

pub static EVENTS_INGESTED_SIZE_DATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_ingested_size_date",
            "Events ingested size in bytes on a particular date",
        )
        .namespace(METRICS_NAMESPACE),
        &["stream", "format", "date"],
    )
    .expect("metric can be created")
});

pub static EVENTS_STORAGE_SIZE_DATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "events_storage_size_date",
            "Events storage size in bytes on a particular date",
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

pub static TOTAL_DISK: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("total_disk", "Total Disk Size").namespace(METRICS_NAMESPACE),
        &["volume"],
    )
    .expect("metric can be created")
});
pub static USED_DISK: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("used_disk", "Used Disk Size").namespace(METRICS_NAMESPACE),
        &["volume"],
    )
    .expect("metric can be created")
});
pub static AVAILABLE_DISK: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("available_disk", "Available Disk Size").namespace(METRICS_NAMESPACE),
        &["volume"],
    )
    .expect("metric can be created")
});
pub static MEMORY: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("memory_usage", "Memory Usage").namespace(METRICS_NAMESPACE),
        &["memory_usage"],
    )
    .expect("metric can be created")
});
pub static CPU: Lazy<GaugeVec> = Lazy::new(|| {
    GaugeVec::new(
        Opts::new("cpu_usage", "CPU Usage").namespace(METRICS_NAMESPACE),
        &["cpu_usage"],
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
    registry
        .register(Box::new(TOTAL_DISK.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(USED_DISK.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(AVAILABLE_DISK.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(MEMORY.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(CPU.clone()))
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

pub async fn get() -> Result<impl Responder, MetricsError> {
    Ok(HttpResponse::Ok().body(format!("{:?}", build_metrics_handler())))
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct DiskMetrics {
    total: u64,
    used: u64,
    available: u64,
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct SystemMetrics {
    memory: MemoryMetrics,
    cpu: Vec<CpuMetrics>,
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct MemoryMetrics {
    total: u64,
    used: u64,
    total_swap: u64,
    used_swap: u64,
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct CpuMetrics {
    name: String,
    usage: f64,
}

// Scheduler for collecting all system metrics
pub async fn init_system_metrics_scheduler() -> Result<(), MetricsError> {
    info!("Setting up scheduler for capturing system metrics");
    let mut scheduler = AsyncScheduler::new();

    scheduler
        .every(SYSTEM_METRICS_INTERVAL_SECONDS)
        .run(move || async {
            if let Err(err) = collect_all_metrics().await {
                error!("Error in capturing system metrics: {:#}", err);
            }
        });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    Ok(())
}

// Function to collect memory, CPU and disk usage metrics
pub async fn collect_all_metrics() -> Result<(), MetricsError> {
    // Collect system metrics (CPU and memory)
    collect_system_metrics().await?;

    // Collect disk metrics for all volumes
    collect_disk_metrics().await?;

    Ok(())
}

// Function to collect disk usage metrics
async fn collect_disk_metrics() -> Result<(), MetricsError> {
    // collect staging volume metrics
    collect_volume_disk_usage("staging", CONFIG.staging_dir())?;
    // Collect data volume metrics for local storage
    if CONFIG.get_storage_mode_string() == "Local drive" {
        collect_volume_disk_usage("data", Path::new(&CONFIG.storage().get_endpoint()))?;
    }

    // Collect hot tier volume metrics if configured
    if let Some(hot_tier_dir) = CONFIG.hot_tier_dir() {
        collect_volume_disk_usage("hot_tier", hot_tier_dir)?;
    }

    Ok(())
}

// Function to collect disk usage metrics for a specific volume
fn collect_volume_disk_usage(label: &str, path: &Path) -> Result<(), MetricsError> {
    let metrics = get_volume_disk_usage(path)?;

    TOTAL_DISK
        .with_label_values(&[label])
        .set(metrics.total as i64);
    USED_DISK
        .with_label_values(&[label])
        .set(metrics.used as i64);
    AVAILABLE_DISK
        .with_label_values(&[label])
        .set(metrics.available as i64);

    Ok(())
}

// Function to get disk usage for a specific volume
fn get_volume_disk_usage(path: &Path) -> Result<DiskMetrics, MetricsError> {
    let mut disks = Disks::new_with_refreshed_list();
    disks.sort_by(|a, b| {
        b.mount_point()
            .to_str()
            .unwrap_or("")
            .len()
            .cmp(&a.mount_point().to_str().unwrap_or("").len())
    });

    for disk in disks.iter() {
        let mount_point = disk.mount_point().to_str().unwrap();

        if path.starts_with(mount_point) {
            return Ok(DiskMetrics {
                total: disk.total_space(),
                used: disk.total_space() - disk.available_space(),
                available: disk.available_space(),
            });
        }
    }

    Err(MetricsError::Custom(
        format!("No matching disk found for path: {:?}", path),
        StatusCode::INTERNAL_SERVER_ERROR,
    ))
}

// Function to collect CPU and memory usage metrics
async fn collect_system_metrics() -> Result<(), MetricsError> {
    let metrics = get_system_metrics()?;

    // Set memory metrics
    MEMORY
        .with_label_values(&["total_memory"])
        .set(metrics.memory.total as i64);
    MEMORY
        .with_label_values(&["used_memory"])
        .set(metrics.memory.used as i64);
    MEMORY
        .with_label_values(&["total_swap"])
        .set(metrics.memory.total_swap as i64);
    MEMORY
        .with_label_values(&["used_swap"])
        .set(metrics.memory.used_swap as i64);

    // Set CPU metrics
    for cpu in metrics.cpu {
        CPU.with_label_values(&[&cpu.name]).set(cpu.usage);
    }

    Ok(())
}

// Get system metrics
fn get_system_metrics() -> Result<SystemMetrics, MetricsError> {
    let mut sys = System::new_all();
    sys.refresh_all();

    // Collect memory metrics
    let memory = MemoryMetrics {
        total: sys.total_memory(),
        used: sys.used_memory(),
        total_swap: sys.total_swap(),
        used_swap: sys.used_swap(),
    };

    // Collect CPU metrics
    let mut cpu_metrics = Vec::new();

    // Add global CPU usage
    cpu_metrics.push(CpuMetrics {
        name: "global".to_string(),
        usage: sys.global_cpu_usage() as f64,
    });

    // Add individual CPU usage
    for cpu in sys.cpus() {
        cpu_metrics.push(CpuMetrics {
            name: cpu.name().to_string(),
            usage: cpu.cpu_usage() as f64,
        });
    }

    Ok(SystemMetrics {
        memory,
        cpu: cpu_metrics,
    })
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
                Self::Custom(_, _) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }
}
