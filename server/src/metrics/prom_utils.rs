use crate::utils::get_url;
use prometheus_parse::Sample as PromSample;
use prometheus_parse::Value as PromValue;
use serde::Serialize;
use serde_json::Error as JsonError;
use serde_json::Value as JsonValue;

#[derive(Debug, Serialize, Clone)]
pub struct Metrics {
    address: String,
    parseable_events_ingested: f64, // all streams
    parseable_staging_files: f64,
    process_resident_memory_bytes: f64,
    parseable_storage_size: StorageMetrics,
}

#[derive(Debug, Serialize, Default, Clone)]
struct StorageMetrics {
    staging: f64,
    data: f64,
}

impl Default for Metrics {
    fn default() -> Self {
        let socket = get_url();
        let address = format!(
            "http://{}:{}",
            socket.domain().unwrap(),
            socket.port().unwrap_or_default()
        );
        Metrics {
            address,
            parseable_events_ingested: 0.0,
            parseable_staging_files: 0.0,
            process_resident_memory_bytes: 0.0,
            parseable_storage_size: StorageMetrics::default(),
        }
    }
}

impl Metrics {
    fn new(address: String) -> Self {
        Metrics {
            address,
            parseable_events_ingested: 0.0,
            parseable_staging_files: 0.0,
            process_resident_memory_bytes: 0.0,
            parseable_storage_size: StorageMetrics::default(),
        }
    }
}

impl Metrics {
    pub fn from_prometheus_samples(samples: Vec<PromSample>, address: String) -> Self {
        let mut prom_dress = Metrics::new(address);

        for sample in samples {
            if &sample.metric == "parseable_events_ingested" {
                if let PromValue::Counter(val) = sample.value {
                    prom_dress.parseable_events_ingested += val;
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
                if sample.labels.get("type").unwrap() == "data" {
                    if let PromValue::Gauge(val) = sample.value {
                        prom_dress.parseable_storage_size.data += val;
                    }
                } else if sample.labels.get("type").unwrap() == "staging" {
                    if let PromValue::Gauge(val) = sample.value {
                        prom_dress.parseable_storage_size.staging += val;
                    }
                }
            }
        }

        prom_dress
    }

    #[allow(unused)]
    pub fn to_json(&self) -> Result<JsonValue, JsonError> {
        serde_json::to_value(self)
    }
}
