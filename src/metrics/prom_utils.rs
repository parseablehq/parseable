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

use crate::INTRA_CLUSTER_CLIENT;
use crate::handlers::http::base_path_without_preceding_slash;
use crate::handlers::http::ingest::PostError;
use crate::handlers::http::modal::Metadata;
use crate::option::Mode;
use crate::parseable::PARSEABLE;
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

#[derive(Debug, Serialize, Clone)]
pub struct Metrics {
    address: String,
    node_type: String,
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
}

#[derive(Debug, Serialize, Default, Clone)]
struct StorageMetrics {
    staging: f64,
    data: f64,
}

impl Default for Metrics {
    fn default() -> Self {
        // for now it is only for ingestor
        let url = PARSEABLE.options.get_url(Mode::Ingest);
        let address = format!(
            "http://{}:{}",
            url.domain()
                .unwrap_or(url.host_str().expect("should have a host")),
            url.port().unwrap_or_default()
        );
        Metrics {
            address,
            node_type: "ingestor".to_string(),
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
        }
    }
}

impl Metrics {
    fn new(address: String, node_type: String) -> Self {
        Metrics {
            address,
            node_type,
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
        }
    }
}

impl Metrics {
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
    pub async fn from_prometheus_samples<T: Metadata>(
        samples: Vec<PromSample>,
        metadata: &T,
    ) -> Result<Self, PostError> {
        let mut prom_dress = Metrics::new(
            metadata.domain_name().to_string(),
            metadata.node_type().to_string(),
        );
        for sample in samples {
            if let PromValue::Gauge(val) = sample.value {
                match sample.metric.as_str() {
                    "parseable_events_ingested" => prom_dress.parseable_events_ingested += val,
                    "parseable_events_ingested_size" => {
                        prom_dress.parseable_events_ingested_size += val
                    }
                    "parseable_lifetime_events_ingested" => {
                        prom_dress.parseable_lifetime_events_ingested += val
                    }
                    "parseable_lifetime_events_ingested_size" => {
                        prom_dress.parseable_lifetime_events_ingested_size += val
                    }
                    "parseable_events_deleted" => {
                        prom_dress.parseable_deleted_events_ingested += val
                    }
                    "parseable_events_deleted_size" => {
                        prom_dress.parseable_deleted_events_ingested_size += val
                    }
                    "parseable_staging_files" => prom_dress.parseable_staging_files += val,
                    "process_resident_memory_bytes" => {
                        prom_dress.process_resident_memory_bytes += val
                    }
                    "parseable_storage_size" => {
                        if sample.labels.get("type").expect("type is present") == "staging" {
                            prom_dress.parseable_storage_size.staging += val;
                        }
                        if sample.labels.get("type").expect("type is present") == "data" {
                            prom_dress.parseable_storage_size.data += val;
                        }
                    }
                    "parseable_lifetime_events_storage_size" => {
                        if sample.labels.get("type").expect("type is present") == "data" {
                            prom_dress.parseable_lifetime_storage_size.data += val;
                        }
                    }
                    "parseable_deleted_events_storage_size" => {
                        if sample.labels.get("type").expect("type is present") == "data" {
                            prom_dress.parseable_deleted_storage_size.data += val;
                        }
                    }
                    _ => {}
                }
            }
        }
        let (commit_id, staging) =
            Self::from_about_api_response(metadata)
                .await
                .map_err(|err| {
                    error!("Fatal: failed to get server info: {:?}", err);
                    PostError::Invalid(err.into())
                })?;

        prom_dress.commit = commit_id;
        prom_dress.staging = staging;

        Ok(prom_dress)
    }

    pub async fn from_about_api_response<T: Metadata>(
        metadata: &T,
    ) -> Result<(String, String), PostError> {
        let uri = Url::parse(&format!(
            "{}{}/about",
            &metadata.domain_name(),
            base_path_without_preceding_slash()
        ))
        .map_err(|err| {
            PostError::Invalid(anyhow::anyhow!("Invalid URL in Ingestor Metadata: {}", err))
        })?;
        let res = INTRA_CLUSTER_CLIENT
            .get(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, metadata.token())
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
                "Failed to fetch about API response from server: {}\n",
                &metadata.domain_name(),
            );
            Err(PostError::Invalid(anyhow::anyhow!(
                "Failed to fetch about API response from server: {}\n",
                &metadata.domain_name()
            )))
        }
    }

    #[allow(unused)]
    pub fn to_json(&self) -> Result<JsonValue, JsonError> {
        serde_json::to_value(self)
    }
}
