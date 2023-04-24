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
 *
 */

use crate::about::{current, platform};
use crate::option::CONFIG;
use crate::storage;
use crate::{metadata, stats};

use chrono::{DateTime, Utc};
use clokwerk::{AsyncScheduler, Interval};
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
    mode: String,
    version: String,
    commit_hash: String,
    metrics: HashMap<String, Value>,
}

impl Report {
    pub fn new() -> Self {
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

        Self {
            deployment_id: storage::StorageMetadata::global().deployment_id,
            uptime: upt,
            report_created_at: Utc::now(),
            operating_system_name: os_name,
            operating_system_version: os_version,
            cpu_count,
            memory_total_bytes: mem_total,
            platform: platform().to_string(),
            mode: CONFIG.mode_string().to_string(),
            version: current().released_version.to_string(),
            commit_hash: current().commit_hash,
            metrics: build_metrics(),
        }
    }

    pub async fn send(&self) {
        let client = reqwest::Client::new();
        let _ = client.post(ANALYTICS_SERVER_URL).json(&self).send().await;
    }
}

fn total_streams() -> usize {
    metadata::STREAM_INFO.list_streams().len()
}

fn total_event_stats() -> (u64, u64, u64) {
    let mut total_events: u64 = 0;
    let mut total_parquet_bytes: u64 = 0;
    let mut total_json_bytes: u64 = 0;

    for stream in metadata::STREAM_INFO.list_streams() {
        let Some(stats) = stats::get_current_stats(&stream, "json") else {continue;};
        total_events += stats.events;
        total_parquet_bytes += stats.storage;
        total_json_bytes += stats.ingestion;
    }
    (total_events, total_json_bytes, total_parquet_bytes)
}

fn build_metrics() -> HashMap<String, Value> {
    // sysinfo refreshed in previous function
    // so no need to refresh again
    let sys = SYS_INFO.lock().unwrap();

    let mut metrics = HashMap::new();
    metrics.insert("stream_count".to_string(), total_streams().into());

    // total_event_stats returns event count, json bytes, parquet bytes in that order
    metrics.insert(
        "total_events_count".to_string(),
        total_event_stats().0.into(),
    );
    metrics.insert("total_json_bytes".to_string(), total_event_stats().1.into());
    metrics.insert(
        "total_parquet_bytes".to_string(),
        total_event_stats().2.into(),
    );

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

pub async fn init_analytics_scheduler() {
    log::info!("Setting up schedular for anonymous user analytics");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(ANALYTICS_SEND_INTERVAL_SECONDS)
        .run(move || async {
            Report::new().send().await;
        });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}
