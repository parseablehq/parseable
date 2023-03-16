/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

pub mod storage;

use actix_web_prometheus::{PrometheusMetrics, PrometheusMetricsBuilder};
use lazy_static::lazy_static;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};

use crate::{handlers::transport::http::metrics_path, metadata::STREAM_INFO};

pub const METRICS_NAMESPACE: &str = env!("CARGO_PKG_NAME");

lazy_static! {
    pub static ref EVENTS_INGESTED: IntCounterVec = IntCounterVec::new(
        Opts::new("events_ingested", "Events ingested").namespace(METRICS_NAMESPACE),
        &["stream", "format"]
    )
    .expect("metric can be created");
    pub static ref EVENTS_INGESTED_SIZE: IntGaugeVec = IntGaugeVec::new(
        Opts::new("events_ingested_size", "Events ingested size bytes")
            .namespace(METRICS_NAMESPACE),
        &["stream", "format"]
    )
    .expect("metric can be created");
    pub static ref STORAGE_SIZE: IntGaugeVec = IntGaugeVec::new(
        Opts::new("storage_size", "Storage size bytes").namespace(METRICS_NAMESPACE),
        &["type", "stream", "format"]
    )
    .expect("metric can be created");
    pub static ref STAGING_FILES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("staging_files", "Active Staging files").namespace(METRICS_NAMESPACE),
        &["stream"]
    )
    .expect("metric can be created");
    pub static ref QUERY_EXECUTE_TIME: HistogramVec = HistogramVec::new(
        HistogramOpts::new("query_execute_time", "Query execute time").namespace(METRICS_NAMESPACE),
        &["stream"]
    )
    .expect("metric can be created");
    pub static ref ALERTS_STATES: IntCounterVec = IntCounterVec::new(
        Opts::new("alerts_states", "Alerts States").namespace(METRICS_NAMESPACE),
        &["stream", "name", "state"]
    )
    .expect("metric can be created");
}

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
        .register(Box::new(STAGING_FILES.clone()))
        .expect("metric can be registered");
    registry
        .register(Box::new(QUERY_EXECUTE_TIME.clone()))
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

pub fn load_from_global_stats() {
    for stream_name in STREAM_INFO.list_streams() {
        let stats = STREAM_INFO.get_stats(&stream_name).expect("stream exists");
        EVENTS_INGESTED
            .with_label_values(&[&stream_name, "json"])
            .inc_by(stats.events);
        EVENTS_INGESTED_SIZE
            .with_label_values(&[&stream_name, "json"])
            .set(stats.ingestion as i64);
        STORAGE_SIZE
            .with_label_values(&["data", &stream_name, "parquet"])
            .set(stats.storage as i64)
    }
}
