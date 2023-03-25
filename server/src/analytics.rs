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
use crate::metadata;
use crate::option::CONFIG;
use crate::storage;

use chrono::{DateTime, Utc};
use clokwerk::{AsyncScheduler, Interval};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use ulid::Ulid;


const ANALYTICS_SERVER_URL: &str = "https://webhook.site/8d522b54-2d38-4796-86d3-82612948fe3d";
const ANALYTICS_SEND_INTERVAL_SECONDS: Interval = clokwerk::Interval::Seconds(20);

#[derive(Serialize, Deserialize)]
pub struct Report {
    deployment_id: Ulid,
    report_created_at: DateTime<Utc>,
    #[serde(rename = "uptime_secs")]
    uptime: f64,
    #[serde(rename = "os_type")]
    operating_system_type: String,
    #[serde(rename = "os_version")]
    operating_system_version: String,
    #[serde(rename = "os_bitness")]
    operating_system_bitness: String,
    platform: String,
    mode: String,
    version: String,
    commit_hash: String,
    metrics: HashMap<String, Value>,
}

impl Report {
    pub fn new(metrics: HashMap<String, Value>) -> Self {
        let mut upt: f64 = 0.0;
        if let Ok(uptime) = uptime_lib::get() {
            upt = uptime.as_secs_f64();
        }
        Self {
            deployment_id: storage::StorageMetadata::global().deployment_id,
            uptime: upt,
            report_created_at: Utc::now(),
            operating_system_type: os_info::get().os_type().to_string(),
            operating_system_version: os_info::get().version().to_string(),
            operating_system_bitness: os_info::get().bitness().to_string(),
            platform: platform().to_string(),
            mode: CONFIG.mode_string().to_string(),
            version: current().released_version.to_string(),
            commit_hash: current().commit_hash,
            metrics,
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

fn total_events() -> u64 {
    let mut total_events: u64 = 0;
    for stream in metadata::STREAM_INFO.list_streams() {
        let stats = metadata::STREAM_INFO.get_stats(&stream).unwrap();
        total_events += stats.events;
    }
    total_events
}

fn build_metrics() -> HashMap<String, Value> {
    let mut metrics = HashMap::new();
    metrics.insert("total_stream_count".to_string(), total_streams().into());
    metrics.insert("total_events_ingested".to_string(), total_events().into());

    metrics
}

pub async fn init_analytics_scheduler() {
    log::info!("Setting up schedular for anonymous user analytics");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(ANALYTICS_SEND_INTERVAL_SECONDS)
        .run(move || async {
            Report::new(build_metrics()).send().await;
        });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}
