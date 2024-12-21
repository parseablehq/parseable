use serde::{Deserialize, Serialize};
pub mod config;
pub mod processor;
pub mod shutdown;
pub mod types;

#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Processing error: {0}")]
    Processing(String),
    #[error("Initialization error: {0}")]
    Init(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum BadData {
    Fail {},
    Drop {},
    Dlt {},
}

impl Default for BadData {
    fn default() -> Self {
        BadData::Drop {}
    }
}
