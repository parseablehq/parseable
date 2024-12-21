use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ConnectorType {
    KafkaSource,
}

impl Display for ConnectorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorType::KafkaSource => write!(f, "kafka_source"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionType {
    Source,
    Sink,
}

impl Display for ConnectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionType::Source => write!(f, "SOURCE"),
            ConnectionType::Sink => write!(f, "SINK"),
        }
    }
}

impl TryFrom<String> for ConnectionType {
    type Error = String;

    fn try_from(value: String) -> anyhow::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "source" => Ok(ConnectionType::Source),
            "sink" => Ok(ConnectionType::Sink),
            _ => Err(format!("Invalid connection type: {}", value)),
        }
    }
}
