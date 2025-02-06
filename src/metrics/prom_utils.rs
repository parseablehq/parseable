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

use std::collections::HashMap;
use std::path::Path;

use crate::about::current;
use crate::handlers::http::base_path_without_preceding_slash;
use crate::handlers::http::ingest::PostError;
use crate::handlers::http::modal::IngestorMetadata;
use crate::option::CONFIG;
use crate::utils::get_url;
use crate::HTTP_CLIENT;
use actix_web::http::header;
use chrono::NaiveDateTime;
use chrono::Utc;
use prometheus_parse::Sample as PromSample;
use prometheus_parse::Value as PromValue;
use serde::Serialize;
use serde_json::Error as JsonError;
use serde_json::Value as JsonValue;
use tracing::error;
use tracing::warn;
use url::Url;

use super::get_system_metrics;
use super::get_volume_disk_usage;
use super::DiskMetrics;
use super::MemoryMetrics;

#[derive(Debug, Serialize, Clone)]
pub struct Metrics {
    address: String,
    parseable_events_ingested: f64, // all streams
    parseable_events_ingested_size: f64,
    parseable_lifetime_events_ingested: f64, // all streams
    parseable_lifetime_events_ingested_size: f64,
    parseable_deleted_events_ingested: f64, // all streams
    parseable_deleted_events_ingested_size: f64,
    parseable_staging_files: f64,
    process_resident_memory_bytes: f64,
    parseable_storage_size: StorageMetrics,
    parseable_lifetime_storage_size: StorageMetrics,
    parseable_deleted_storage_size: StorageMetrics,
    event_type: String,
    event_time: NaiveDateTime,
    commit: String,
    staging: String,
    parseable_data_disk_usage: DiskMetrics,
    parseable_staging_disk_usage: DiskMetrics,
    parseable_hot_tier_disk_usage: DiskMetrics,
    parseable_memory_usage: MemoryMetrics,
    parseable_cpu_usage: HashMap<String, f64>,
}

#[derive(Debug, Serialize, Default, Clone)]
struct StorageMetrics {
    staging: f64,
    data: f64,
}

impl Metrics {
    fn new(address: String) -> Self {
        Metrics {
            address,
            parseable_events_ingested: 0.0,
            parseable_events_ingested_size: 0.0,
            parseable_staging_files: 0.0,
            process_resident_memory_bytes: 0.0,
            parseable_storage_size: StorageMetrics::default(),
            parseable_lifetime_events_ingested: 0.0,
            parseable_lifetime_events_ingested_size: 0.0,
            parseable_deleted_events_ingested: 0.0,
            parseable_deleted_events_ingested_size: 0.0,
            parseable_deleted_storage_size: StorageMetrics::default(),
            parseable_lifetime_storage_size: StorageMetrics::default(),
            event_type: "cluster-metrics".to_string(),
            event_time: Utc::now().naive_utc(),
            commit: "".to_string(),
            staging: "".to_string(),
            parseable_data_disk_usage: DiskMetrics {
                total: 0,
                used: 0,
                available: 0,
            },
            parseable_staging_disk_usage: DiskMetrics {
                total: 0,
                used: 0,
                available: 0,
            },
            parseable_hot_tier_disk_usage: DiskMetrics {
                total: 0,
                used: 0,
                available: 0,
            },
            parseable_memory_usage: MemoryMetrics {
                total: 0,
                used: 0,
                total_swap: 0,
                used_swap: 0,
            },
            parseable_cpu_usage: HashMap::new(),
        }
    }
}

#[derive(Debug)]
enum MetricType {
    SimpleGauge(String),
    StorageSize(String),
    DiskUsage(String),
    MemoryUsage(String),
    CpuUsage,
}

impl MetricType {
    fn from_metric(metric: &str, labels: &HashMap<String, String>) -> Option<Self> {
        match metric {
            "parseable_events_ingested" => {
                Some(Self::SimpleGauge("parseable_events_ingested".into()))
            }
            "parseable_events_ingested_size" => {
                Some(Self::SimpleGauge("parseable_events_ingested_size".into()))
            }
            "parseable_lifetime_events_ingested" => Some(Self::SimpleGauge(
                "parseable_lifetime_events_ingested".into(),
            )),
            "parseable_lifetime_events_ingested_size" => Some(Self::SimpleGauge(
                "parseable_lifetime_events_ingested_size".into(),
            )),
            "parseable_events_deleted" => {
                Some(Self::SimpleGauge("parseable_events_deleted".into()))
            }
            "parseable_events_deleted_size" => {
                Some(Self::SimpleGauge("parseable_events_deleted_size".into()))
            }
            "parseable_staging_files" => Some(Self::SimpleGauge("parseable_staging_files".into())),
            "process_resident_memory_bytes" => {
                Some(Self::SimpleGauge("process_resident_memory_bytes".into()))
            }
            "parseable_storage_size" => labels.get("type").map(|t| Self::StorageSize(t.clone())),
            "parseable_lifetime_events_storage_size" => {
                labels.get("type").map(|t| Self::StorageSize(t.clone()))
            }
            "parseable_deleted_events_storage_size" => {
                labels.get("type").map(|t| Self::StorageSize(t.clone()))
            }
            "parseable_total_disk" | "parseable_used_disk" | "parseable_available_disk" => {
                labels.get("volume").map(|v| Self::DiskUsage(v.clone()))
            }
            "parseable_memory_usage" => labels
                .get("memory_usage")
                .map(|m| Self::MemoryUsage(m.clone())),
            "parseable_cpu_usage" => Some(Self::CpuUsage),
            _ => None,
        }
    }
}
impl Metrics {
    pub async fn ingestor_prometheus_samples(
        samples: Vec<PromSample>,
        ingestor_metadata: &IngestorMetadata,
    ) -> Result<Self, PostError> {
        let mut metrics = Metrics::new(ingestor_metadata.domain_name.to_string());

        Self::build_metrics_from_samples(samples, &mut metrics)?;

        // Get additional metadata
        let (commit_id, staging) = Self::from_about_api_response(ingestor_metadata.clone())
            .await
            .map_err(|err| {
                error!("Fatal: failed to get ingestor info: {:?}", err);
                PostError::Invalid(err.into())
            })?;

        metrics.commit = commit_id;
        metrics.staging = staging;

        Ok(metrics)
    }

    pub async fn querier_prometheus_metrics() -> Self {
        let mut metrics = Metrics::new(get_url().to_string());

        let system_metrics = get_system_metrics().expect("Failed to get system metrics");

        metrics.parseable_memory_usage.total = system_metrics.memory.total;
        metrics.parseable_memory_usage.used = system_metrics.memory.used;
        metrics.parseable_memory_usage.total_swap = system_metrics.memory.total_swap;
        metrics.parseable_memory_usage.used_swap = system_metrics.memory.used_swap;
        for cpu_usage in system_metrics.cpu {
            metrics
                .parseable_cpu_usage
                .insert(cpu_usage.name.clone(), cpu_usage.usage);
        }

        let staging_disk_usage = get_volume_disk_usage(CONFIG.staging_dir())
            .expect("Failed to get staging volume disk usage");

        metrics.parseable_staging_disk_usage.total = staging_disk_usage.total;
        metrics.parseable_staging_disk_usage.used = staging_disk_usage.used;
        metrics.parseable_staging_disk_usage.available = staging_disk_usage.available;

        if CONFIG.get_storage_mode_string() == "Local drive" {
            let data_disk_usage =
                get_volume_disk_usage(Path::new(&CONFIG.storage().get_endpoint()))
                    .expect("Failed to get data volume disk usage");

            metrics.parseable_data_disk_usage.total = data_disk_usage.total;
            metrics.parseable_data_disk_usage.used = data_disk_usage.used;
            metrics.parseable_data_disk_usage.available = data_disk_usage.available;
        }

        if CONFIG.options.hot_tier_storage_path.is_some() {
            let hot_tier_disk_usage =
                get_volume_disk_usage(CONFIG.hot_tier_dir().as_ref().unwrap())
                    .expect("Failed to get hot tier volume disk usage");

            metrics.parseable_hot_tier_disk_usage.total = hot_tier_disk_usage.total;
            metrics.parseable_hot_tier_disk_usage.used = hot_tier_disk_usage.used;
            metrics.parseable_hot_tier_disk_usage.available = hot_tier_disk_usage.available;
        }

        metrics.commit = current().commit_hash;
        metrics.staging = CONFIG.staging_dir().display().to_string();

        metrics
    }

    fn build_metrics_from_samples(
        samples: Vec<PromSample>,
        metrics: &mut Metrics,
    ) -> Result<(), PostError> {
        for sample in samples {
            let metric_type = MetricType::from_metric(&sample.metric, &sample.labels);

            match (sample.value.clone(), metric_type) {
                (PromValue::Gauge(val), Some(metric_type)) => {
                    Self::process_gauge_metric(
                        metrics,
                        metric_type,
                        val,
                        &sample.metric,
                        sample.clone(),
                    );
                }
                _ => continue,
            }
        }
        Ok(())
    }

    fn process_gauge_metric(
        metrics: &mut Metrics,
        metric_type: MetricType,
        val: f64,
        metric_name: &str,
        sample: PromSample,
    ) {
        match metric_type {
            MetricType::SimpleGauge(metric_name) => {
                Self::process_simple_gauge(metrics, &metric_name, val)
            }
            MetricType::StorageSize(storage_type) => {
                Self::process_storage_size(metrics, &storage_type, val)
            }
            MetricType::DiskUsage(volume_type) => {
                Self::process_disk_usage(metrics, &volume_type, val, metric_name)
            }
            MetricType::MemoryUsage(memory_type) => {
                Self::process_memory_usage(metrics, &memory_type, val)
            }
            MetricType::CpuUsage => Self::process_cpu_usage(metrics, val, sample),
        }
    }

    fn process_simple_gauge(metrics: &mut Metrics, metric_name: &str, val: f64) {
        match metric_name {
            "parseable_events_ingested" => metrics.parseable_events_ingested += val,
            "parseable_events_ingested_size" => metrics.parseable_events_ingested_size += val,
            "parseable_lifetime_events_ingested" => {
                metrics.parseable_lifetime_events_ingested += val
            }
            "parseable_lifetime_events_ingested_size" => {
                metrics.parseable_lifetime_events_ingested_size += val
            }
            "parseable_events_deleted" => metrics.parseable_deleted_events_ingested += val,
            "parseable_events_deleted_size" => {
                metrics.parseable_deleted_events_ingested_size += val
            }
            "parseable_staging_files" => metrics.parseable_staging_files += val,
            "process_resident_memory_bytes" => metrics.process_resident_memory_bytes += val,
            _ => {}
        }
    }

    fn process_storage_size(metrics: &mut Metrics, storage_type: &str, val: f64) {
        match storage_type {
            "staging" => metrics.parseable_storage_size.staging += val,
            "data" => metrics.parseable_storage_size.data += val,
            _ => {}
        }
    }

    fn process_disk_usage(metrics: &mut Metrics, volume_type: &str, val: f64, metric_name: &str) {
        let disk_usage = match volume_type {
            "data" => &mut metrics.parseable_data_disk_usage,
            "staging" => &mut metrics.parseable_staging_disk_usage,
            "hot_tier" => &mut metrics.parseable_hot_tier_disk_usage,
            _ => return,
        };

        match metric_name {
            "parseable_total_disk" => disk_usage.total = val as u64,
            "parseable_used_disk" => disk_usage.used = val as u64,
            "parseable_available_disk" => disk_usage.available = val as u64,
            _ => {}
        }
    }

    fn process_memory_usage(metrics: &mut Metrics, memory_type: &str, val: f64) {
        match memory_type {
            "total_memory" => metrics.parseable_memory_usage.total = val as u64,
            "used_memory" => metrics.parseable_memory_usage.used = val as u64,
            "total_swap" => metrics.parseable_memory_usage.total_swap = val as u64,
            "used_swap" => metrics.parseable_memory_usage.used_swap = val as u64,
            _ => {}
        }
    }

    fn process_cpu_usage(metrics: &mut Metrics, val: f64, sample: PromSample) {
        if let Some(cpu_name) = sample.labels.get("cpu_usage") {
            metrics
                .parseable_cpu_usage
                .insert(cpu_name.to_string(), val);
        }
    }

    pub fn get_daily_stats_from_samples(
        samples: Vec<PromSample>,
        stream_name: &str,
        date: &str,
    ) -> (u64, u64, u64) {
        let mut events_ingested: u64 = 0;
        let mut ingestion_size: u64 = 0;
        let mut storage_size: u64 = 0;
        for sample in samples {
            if let PromValue::Gauge(val) = sample.value {
                match sample.metric.as_str() {
                    "parseable_events_ingested_date" => {
                        if sample.labels.get("stream").expect("stream name is present")
                            == stream_name
                            && sample.labels.get("date").expect("date is present") == date
                        {
                            events_ingested = val as u64;
                        }
                    }
                    "parseable_events_ingested_size_date" => {
                        if sample.labels.get("stream").expect("stream name is present")
                            == stream_name
                            && sample.labels.get("date").expect("date is present") == date
                        {
                            ingestion_size = val as u64;
                        }
                    }
                    "parseable_events_storage_size_date" => {
                        if sample.labels.get("stream").expect("stream name is present")
                            == stream_name
                            && sample.labels.get("date").expect("date is present") == date
                        {
                            storage_size = val as u64;
                        }
                    }
                    _ => {}
                }
            }
        }
        (events_ingested, ingestion_size, storage_size)
    }

    pub async fn from_about_api_response(
        ingestor_metadata: IngestorMetadata,
    ) -> Result<(String, String), PostError> {
        let uri = Url::parse(&format!(
            "{}{}/about",
            &ingestor_metadata.domain_name,
            base_path_without_preceding_slash()
        ))
        .map_err(|err| {
            PostError::Invalid(anyhow::anyhow!("Invalid URL in Ingestor Metadata: {}", err))
        })?;
        let res = HTTP_CLIENT
            .get(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, ingestor_metadata.token)
            .send()
            .await;
        if let Ok(res) = res {
            let about_api_json = res.text().await.map_err(PostError::NetworkError)?;
            let about_api_json: serde_json::Value =
                serde_json::from_str(&about_api_json).map_err(PostError::SerdeError)?;
            let commit_id = about_api_json
                .get("commit")
                .and_then(|x| x.as_str())
                .unwrap_or_default();
            let staging = about_api_json
                .get("staging")
                .and_then(|x| x.as_str())
                .unwrap_or_default();
            Ok((commit_id.to_string(), staging.to_string()))
        } else {
            warn!(
                "Failed to fetch about API response from ingestor: {}\n",
                &ingestor_metadata.domain_name,
            );
            Err(PostError::Invalid(anyhow::anyhow!(
                "Failed to fetch about API response from ingestor: {}\n",
                &ingestor_metadata.domain_name
            )))
        }
    }

    #[allow(unused)]
    pub fn to_json(&self) -> Result<JsonValue, JsonError> {
        serde_json::to_value(self)
    }
}
