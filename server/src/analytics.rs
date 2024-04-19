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
 *
 */

use crate::about::{current, platform};
use crate::handlers::http::cluster::utils::check_liveness;
use crate::handlers::http::{base_path_without_preceding_slash, cluster};
use crate::option::{Mode, CONFIG};
use crate::storage;
use crate::{metadata, stats};

use actix_web::{web, HttpRequest, Responder};
use chrono::{DateTime, Utc};
use clokwerk::{AsyncScheduler, Interval};
use http::header;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use sysinfo::{CpuExt, System, SystemExt};
use ulid::Ulid;

const ANALYTICS_SERVER_URL: &str = "https://analytics.parseable.io:80";
const ANALYTICS_SEND_INTERVAL_SECONDS: Interval = clokwerk::Interval::Hours(1);

pub static SYS_INFO: Lazy<Mutex<System>> = Lazy::new(|| Mutex::new(System::new_all()));

pub fn refresh_sys_info() {
    let mut sys_info = SYS_INFO.lock().unwrap();
    sys_info.refresh_all();
}

#[derive(Serialize, Deserialize)]
pub struct Report {
    deployment_id: Ulid,
    report_created_at: DateTime<Utc>,
    #[serde(rename = "uptime_secs")]
    uptime: f64,
    #[serde(rename = "os_name")]
    operating_system_name: String,
    #[serde(rename = "os_version")]
    operating_system_version: String,
    cpu_count: usize,
    memory_total_bytes: u64,
    platform: String,
    storage_mode: String,
    server_mode: String,
    version: String,
    commit_hash: String,
    active_ingestors: u64,
    inactive_ingestors: u64,
    stream_count: usize,
    total_events_count: u64,
    total_json_bytes: u64,
    total_parquet_bytes: u64,
    metrics: HashMap<String, Value>,
}

impl Report {
    pub async fn new() -> anyhow::Result<Self> {
        let mut upt: f64 = 0.0;
        if let Ok(uptime) = uptime_lib::get() {
            upt = uptime.as_secs_f64();
        }

        refresh_sys_info();
        let mut os_version = "Unknown".to_string();
        let mut os_name = "Unknown".to_string();
        let mut cpu_count = 0;
        let mut mem_total = 0;
        if let Ok(info) = SYS_INFO.lock() {
            os_version = info.os_version().unwrap_or_default();
            os_name = info.name().unwrap_or_default();
            cpu_count = info.cpus().len();
            mem_total = info.total_memory();
        }
        let ingestor_metrics = fetch_ingestors_metrics().await?;

        Ok(Self {
            deployment_id: storage::StorageMetadata::global().deployment_id,
            uptime: upt,
            report_created_at: Utc::now(),
            operating_system_name: os_name,
            operating_system_version: os_version,
            cpu_count,
            memory_total_bytes: mem_total,
            platform: platform().to_string(),
            storage_mode: CONFIG.get_storage_mode_string().to_string(),
            server_mode: CONFIG.parseable.mode.to_string(),
            version: current().released_version.to_string(),
            commit_hash: current().commit_hash,
            active_ingestors: ingestor_metrics.0,
            inactive_ingestors: ingestor_metrics.1,
            stream_count: ingestor_metrics.2,
            total_events_count: ingestor_metrics.3,
            total_json_bytes: ingestor_metrics.4,
            total_parquet_bytes: ingestor_metrics.5,
            metrics: build_metrics().await,
        })
    }

    pub async fn send(&self) {
        let client = reqwest::Client::new();
        let _ = client.post(ANALYTICS_SERVER_URL).json(&self).send().await;
    }
}

/// build the node metrics for the node ingestor endpoint
pub async fn get_analytics(_: HttpRequest) -> impl Responder {
    let json = NodeMetrics::build();
    web::Json(json)
}

fn total_streams() -> usize {
    metadata::STREAM_INFO.list_streams().len()
}

fn total_event_stats() -> (u64, u64, u64) {
    let mut total_events: u64 = 0;
    let mut total_parquet_bytes: u64 = 0;
    let mut total_json_bytes: u64 = 0;

    for stream in metadata::STREAM_INFO.list_streams() {
        let Some(stats) = stats::get_current_stats(&stream, "json") else {
            continue;
        };
        total_events += stats.events;
        total_parquet_bytes += stats.storage;
        total_json_bytes += stats.ingestion;
    }
    (total_events, total_json_bytes, total_parquet_bytes)
}

async fn fetch_ingestors_metrics() -> anyhow::Result<(u64, u64, usize, u64, u64, u64)> {
    let event_stats = total_event_stats();
    let mut node_metrics =
        NodeMetrics::new(total_streams(), event_stats.0, event_stats.1, event_stats.2);

    let mut vec = vec![];
    let mut active_ingestors = 0u64;
    let mut offline_ingestors = 0u64;
    if CONFIG.parseable.mode == Mode::Query {
        // send analytics for ingest servers

        // ingestor infos should be valid here, if not some thing is wrong
        let ingestor_infos = cluster::get_ingestor_info().await.unwrap();

        for im in ingestor_infos {
            if !check_liveness(&im.domain_name).await {
                offline_ingestors += 1;
                continue;
            }

            let uri = url::Url::parse(&format!(
                "{}{}/analytics",
                im.domain_name,
                base_path_without_preceding_slash()
            ))
            .expect("Should be a valid URL");

            let resp = reqwest::Client::new()
                .get(uri)
                .header(header::AUTHORIZATION, im.token.clone())
                .header(header::CONTENT_TYPE, "application/json")
                .send()
                .await
                .expect("should respond");

            let data = serde_json::from_slice::<NodeMetrics>(&resp.bytes().await?)?;
            vec.push(data);
            active_ingestors += 1;
        }

        node_metrics.accumulate(&mut vec);
    }

    Ok((
        active_ingestors,
        offline_ingestors,
        node_metrics.stream_count,
        node_metrics.total_events_count,
        node_metrics.total_json_bytes,
        node_metrics.total_parquet_bytes,
    ))
}

async fn build_metrics() -> HashMap<String, Value> {
    // sysinfo refreshed in previous function
    // so no need to refresh again
    let sys = SYS_INFO.lock().unwrap();

    let mut metrics = HashMap::new();
    metrics.insert("memory_in_use_bytes".to_string(), sys.used_memory().into());
    metrics.insert("memory_free_bytes".to_string(), sys.free_memory().into());

    for cpu in sys.cpus() {
        metrics.insert(
            format!("cpu_{}_usage_percent", cpu.name()),
            cpu.cpu_usage().into(),
        );
    }

    metrics
}

pub fn init_analytics_scheduler() -> anyhow::Result<()> {
    log::info!("Setting up schedular for anonymous user analytics");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(ANALYTICS_SEND_INTERVAL_SECONDS)
        .run(move || async {
            Report::new()
                .await
                .unwrap_or_else(|err| {
                    // panicing because seperate thread
                    // TODO: a better way to handle this
                    log::error!("Error while sending analytics: {}", err.to_string());
                    panic!("{}", err.to_string());
                })
                .send()
                .await;
        });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    Ok(())
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct NodeMetrics {
    stream_count: usize,
    total_events_count: u64,
    total_json_bytes: u64,
    total_parquet_bytes: u64,
}

impl NodeMetrics {
    fn build() -> Self {
        let event_stats = total_event_stats();
        Self {
            stream_count: total_streams(),
            total_events_count: event_stats.0,
            total_json_bytes: event_stats.1,
            total_parquet_bytes: event_stats.2,
        }
    }

    fn new(
        stream_count: usize,
        total_events_count: u64,
        total_json_bytes: u64,
        total_parquet_bytes: u64,
    ) -> Self {
        Self {
            stream_count,
            total_events_count,
            total_json_bytes,
            total_parquet_bytes,
        }
    }

    fn accumulate(&mut self, other: &mut [NodeMetrics]) {
        other.iter().for_each(|nm| {
            self.total_events_count += nm.total_events_count;
            self.total_json_bytes += nm.total_json_bytes;
            self.total_parquet_bytes += nm.total_parquet_bytes;
        });
    }
}
