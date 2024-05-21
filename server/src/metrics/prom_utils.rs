use crate::handlers::http::base_path_without_preceding_slash;
use crate::handlers::http::ingest::PostError;
use crate::handlers::http::modal::IngestorMetadata;
use crate::utils::get_url;
use actix_web::http::header;
use chrono::NaiveDateTime;
use chrono::Utc;
use prometheus_parse::Sample as PromSample;
use prometheus_parse::Value as PromValue;
use serde::Serialize;
use serde_json::Error as JsonError;
use serde_json::Value as JsonValue;
use url::Url;

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
    cache: String,
}

#[derive(Debug, Serialize, Default, Clone)]
struct StorageMetrics {
    staging: f64,
    data: f64,
}

impl Default for Metrics {
    fn default() -> Self {
        let url = get_url();
        let address = format!(
            "http://{}:{}",
            url.domain()
                .unwrap_or(url.host_str().expect("should have a host")),
            url.port().unwrap_or_default()
        );
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
            cache: "".to_string(),
        }
    }
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
            cache: "".to_string(),
        }
    }
}

impl Metrics {
    pub async fn from_prometheus_samples(
        samples: Vec<PromSample>,
        ingestor_metadata: &IngestorMetadata,
    ) -> Result<Self, PostError> {
        let mut prom_dress = Metrics::new(ingestor_metadata.domain_name.to_string());
        for sample in samples {
            if &sample.metric == "parseable_events_ingested" {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.parseable_events_ingested += val;
                }
            } else if &sample.metric == "parseable_events_ingested_size" {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.parseable_events_ingested_size += val;
                }
            } else if &sample.metric == "parseable_lifetime_events_ingested" {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.parseable_lifetime_events_ingested += val;
                }
            } else if &sample.metric == "parseable_lifetime_events_ingested_size" {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.parseable_lifetime_events_ingested_size += val;
                }
            } else if &sample.metric == "parseable_deleted_events_ingested" {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.parseable_deleted_events_ingested += val;
                }
            } else if &sample.metric == "parseable_deleted_events_ingested_size" {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.parseable_deleted_events_ingested_size += val;
                }
            } else if sample.metric == "parseable_staging_files" {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.parseable_staging_files += val;
                }
            } else if sample.metric == "process_resident_memory_bytes" {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.process_resident_memory_bytes += val;
                }
            } else if sample.metric == "parseable_storage_size" {
                if sample.labels.get("type").expect("type is present") == "data" {
                    if let PromValue::Gauge(val) = sample.value {
                        prom_dress.parseable_storage_size.data += val;
                    }
                } else if sample.labels.get("type").expect("type is present") == "staging" {
                    if let PromValue::Gauge(val) = sample.value {
                        prom_dress.parseable_storage_size.staging += val;
                    }
                }
            } else if sample.metric == "parseable_lifetime_events_storage_size" {
                if sample.labels.get("type").expect("type is present") == "data" {
                    if let PromValue::Gauge(val) = sample.value {
                        prom_dress.parseable_lifetime_storage_size.data += val;
                    }
                }
            } else if sample.metric == "parseable_deleted_events_storage_size"
                && sample.labels.get("type").expect("type is present") == "data"
            {
                if let PromValue::Gauge(val) = sample.value {
                    prom_dress.parseable_deleted_storage_size.data += val;
                }
            }
        }
        let about_api_json = Self::from_about_api_response(ingestor_metadata.clone())
            .await
            .map_err(|err| {
                log::error!("Fatal: failed to get ingestor info: {:?}", err);
                PostError::Invalid(err.into())
            })?;
        let commit_id = about_api_json
            .get("commit")
            .and_then(|x| x.as_str())
            .unwrap_or_default();
        let staging = about_api_json
            .get("staging")
            .and_then(|x| x.as_str())
            .unwrap_or_default();
        let cache = about_api_json
            .get("cache")
            .and_then(|x| x.as_str())
            .unwrap_or_default();
        prom_dress.commit = commit_id.to_string();
        prom_dress.staging = staging.to_string();
        prom_dress.cache = cache.to_string();

        Ok(prom_dress)
    }

    pub async fn from_about_api_response(
        ingestor_metadata: IngestorMetadata,
    ) -> Result<serde_json::Value, PostError> {
        let uri = Url::parse(&format!(
            "{}{}/about",
            &ingestor_metadata.domain_name,
            base_path_without_preceding_slash()
        ))
        .map_err(|err| {
            PostError::Invalid(anyhow::anyhow!("Invalid URL in Ingestor Metadata: {}", err))
        })?;
        let res = reqwest::Client::new()
            .get(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, ingestor_metadata.token)
            .send()
            .await;
        if let Ok(res) = res {
            let about_api_json = res.text().await.map_err(PostError::NetworkError)?;
            let about_api_json: serde_json::Value =
                serde_json::from_str(&about_api_json).map_err(PostError::SerdeError)?;
            Ok(about_api_json)
        } else {
            log::warn!(
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
