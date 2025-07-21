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
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use clokwerk::{AsyncScheduler, Interval};
use http::header;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use sysinfo::System;
use tracing::{error, info};
use ulid::Ulid;

use crate::{
    HTTP_CLIENT, INTRA_CLUSTER_CLIENT,
    about::{current, platform},
    handlers::{
        STREAM_NAME_HEADER_KEY,
        http::{
            base_path_without_preceding_slash,
            cluster::{self, utils::check_liveness},
            modal::{NodeMetadata, NodeType},
        },
    },
    option::Mode,
    parseable::PARSEABLE,
    stats::{self, Stats},
    storage,
};

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
    server_mode: Mode,
    version: String,
    commit_hash: String,
    active_ingestors: u64,
    inactive_ingestors: u64,
    active_indexers: u64,
    inactive_indexers: u64,
    active_queriers: u64,
    inactive_queriers: u64,
    stream_count: usize,
    total_events_count: u64,
    total_json_bytes: u64,
    total_parquet_bytes: u64,
    current_events_count: u64,
    current_json_bytes: u64,
    current_parquet_bytes: u64,
    deleted_events_count: u64,
    deleted_json_bytes: u64,
    deleted_parquet_bytes: u64,
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
            os_version = System::os_version().unwrap_or_default();
            os_name = System::name().unwrap_or_default();
            cpu_count = info.cpus().len();
            mem_total = info.total_memory();
        }
        let ingestor_metrics = fetch_ingestors_metrics().await?;
        let mut active_indexers = 0;
        let mut inactive_indexers = 0;
        let mut active_queriers = 0;
        let mut inactive_queriers = 0;

        // check liveness of indexers
        // get the count of active and inactive indexers
        let indexer_infos: Vec<NodeMetadata> = cluster::get_node_info(NodeType::Indexer).await?;
        for indexer in indexer_infos {
            if check_liveness(&indexer.domain_name).await {
                active_indexers += 1;
            } else {
                inactive_indexers += 1;
            }
        }

        // check liveness of queriers
        // get the count of active and inactive queriers
        let query_infos: Vec<NodeMetadata> = cluster::get_node_info(NodeType::Querier).await?;
        for query in query_infos {
            if check_liveness(&query.domain_name).await {
                active_queriers += 1;
            } else {
                inactive_queriers += 1;
            }
        }
        Ok(Self {
            deployment_id: storage::StorageMetadata::global().deployment_id,
            uptime: upt,
            report_created_at: Utc::now(),
            operating_system_name: os_name,
            operating_system_version: os_version,
            cpu_count,
            memory_total_bytes: mem_total,
            platform: platform().to_string(),
            storage_mode: PARSEABLE.get_storage_mode_string().to_string(),
            server_mode: PARSEABLE.options.mode,
            version: current().released_version.to_string(),
            commit_hash: current().commit_hash,
            active_ingestors: ingestor_metrics.0,
            inactive_ingestors: ingestor_metrics.1,
            active_indexers,
            inactive_indexers,
            active_queriers,
            inactive_queriers,
            stream_count: ingestor_metrics.2,
            total_events_count: ingestor_metrics.3,
            total_json_bytes: ingestor_metrics.4,
            total_parquet_bytes: ingestor_metrics.5,
            current_events_count: ingestor_metrics.6,
            current_json_bytes: ingestor_metrics.7,
            current_parquet_bytes: ingestor_metrics.8,
            deleted_events_count: ingestor_metrics.9,
            deleted_json_bytes: ingestor_metrics.10,
            deleted_parquet_bytes: ingestor_metrics.11,
            metrics: build_metrics().await,
        })
    }

    pub async fn send(&self) {
        let _ = HTTP_CLIENT
            .post(ANALYTICS_SERVER_URL)
            .header(STREAM_NAME_HEADER_KEY, "serverusageevent")
            .json(&self)
            .send()
            .await;
    }
}

/// build the node metrics for the node ingestor endpoint
pub async fn get_analytics(_: HttpRequest) -> impl Responder {
    let json = NodeMetrics::build();
    web::Json(json)
}

fn total_streams() -> usize {
    PARSEABLE.streams.len()
}

fn total_event_stats() -> (Stats, Stats, Stats) {
    let mut total_events: u64 = 0;
    let mut total_parquet_bytes: u64 = 0;
    let mut total_json_bytes: u64 = 0;

    let mut current_events: u64 = 0;
    let mut current_parquet_bytes: u64 = 0;
    let mut current_json_bytes: u64 = 0;

    let mut deleted_events: u64 = 0;
    let mut deleted_parquet_bytes: u64 = 0;
    let mut deleted_json_bytes: u64 = 0;

    for stream in PARSEABLE.streams.list() {
        let Some(stats) = stats::get_current_stats(&stream, "json") else {
            continue;
        };
        total_events += stats.lifetime_stats.events;
        total_parquet_bytes += stats.lifetime_stats.storage;
        total_json_bytes += stats.lifetime_stats.ingestion;

        current_events += stats.current_stats.events;
        current_parquet_bytes += stats.current_stats.storage;
        current_json_bytes += stats.current_stats.ingestion;

        deleted_events += stats.deleted_stats.events;
        deleted_parquet_bytes += stats.deleted_stats.storage;
        deleted_json_bytes += stats.deleted_stats.ingestion;
    }

    (
        Stats {
            events: total_events,
            ingestion: total_json_bytes,
            storage: total_parquet_bytes,
        },
        Stats {
            events: current_events,
            ingestion: current_json_bytes,
            storage: current_parquet_bytes,
        },
        Stats {
            events: deleted_events,
            ingestion: deleted_json_bytes,
            storage: deleted_parquet_bytes,
        },
    )
}

async fn fetch_ingestors_metrics()
-> anyhow::Result<(u64, u64, usize, u64, u64, u64, u64, u64, u64, u64, u64, u64)> {
    let event_stats = total_event_stats();
    let mut node_metrics = NodeMetrics::new(
        total_streams(),
        event_stats.0.events,
        event_stats.0.ingestion,
        event_stats.0.storage,
        event_stats.1.events,
        event_stats.1.ingestion,
        event_stats.1.storage,
        event_stats.2.events,
        event_stats.2.ingestion,
        event_stats.2.storage,
    );

    let mut vec = vec![];
    let mut active_ingestors = 0u64;
    let mut offline_ingestors = 0u64;

    // for OSS, Query mode fetches the analytics report
    // for Enterprise, Prism mode fetches the analytics report
    if PARSEABLE.options.mode == Mode::Query || PARSEABLE.options.mode == Mode::Prism {
        // send analytics for ingest servers

        // ingestor infos should be valid here, if not some thing is wrong
        let ingestor_infos: Vec<NodeMetadata> = cluster::get_node_info(NodeType::Ingestor).await?;

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

            let resp = INTRA_CLUSTER_CLIENT
                .get(uri)
                .header(header::AUTHORIZATION, im.token.clone())
                .header(header::CONTENT_TYPE, "application/json")
                .send()
                .await
                .expect("should respond");
            // check if the response is valid
            if let Ok(data) = serde_json::from_slice::<NodeMetrics>(&resp.bytes().await?) {
                active_ingestors += 1;
                vec.push(data);
            } else {
                offline_ingestors += 1;
                continue;
            }
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
        node_metrics.current_events_count,
        node_metrics.current_json_bytes,
        node_metrics.current_parquet_bytes,
        node_metrics.deleted_events_count,
        node_metrics.deleted_json_bytes,
        node_metrics.deleted_parquet_bytes,
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
    info!("Setting up schedular for anonymous user analytics");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(ANALYTICS_SEND_INTERVAL_SECONDS)
        .run(move || async {
            Report::new()
                .await
                .unwrap_or_else(|err| {
                    // panicing because seperate thread
                    // TODO: a better way to handle this
                    error!("Error while sending analytics: {}", err.to_string());
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
    current_events_count: u64,
    current_json_bytes: u64,
    current_parquet_bytes: u64,
    deleted_events_count: u64,
    deleted_json_bytes: u64,
    deleted_parquet_bytes: u64,
}

impl NodeMetrics {
    fn build() -> Self {
        let event_stats = total_event_stats();
        Self {
            stream_count: total_streams(),
            total_events_count: event_stats.0.events,
            total_json_bytes: event_stats.0.ingestion,
            total_parquet_bytes: event_stats.0.storage,

            current_events_count: event_stats.1.events,
            current_json_bytes: event_stats.1.ingestion,
            current_parquet_bytes: event_stats.1.storage,

            deleted_events_count: event_stats.2.events,
            deleted_json_bytes: event_stats.2.ingestion,
            deleted_parquet_bytes: event_stats.2.storage,
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_count: usize,
        total_events_count: u64,
        total_json_bytes: u64,
        total_parquet_bytes: u64,
        current_events_count: u64,
        current_json_bytes: u64,
        current_parquet_bytes: u64,
        deleted_events_count: u64,
        deleted_json_bytes: u64,
        deleted_parquet_bytes: u64,
    ) -> Self {
        Self {
            stream_count,
            total_events_count,
            total_json_bytes,
            total_parquet_bytes,
            current_events_count,
            current_json_bytes,
            current_parquet_bytes,
            deleted_events_count,
            deleted_json_bytes,
            deleted_parquet_bytes,
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
