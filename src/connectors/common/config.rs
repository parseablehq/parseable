use crate::connectors::kafka::config::KafkaConfig;
use clap::{ArgMatches, FromArgMatches, Parser, Subcommand, ValueEnum};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Parser)]
#[command(name = "connectors", about = "Configure Parseable connectors")]
#[group(id = "connector-config")]
pub struct ConnectorConfig {
    #[command(subcommand)]
    pub connectors: Connectors,

    #[arg(
        value_enum,
        long = "bad-data-policy",
        required = false,
        default_value_t = BadData::Fail,
        env = "P_CONNECTOR_BAD_DATA_POLICY",
        help = "Policy for handling bad data"
    )]
    pub bad_data: BadData,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Connectors {
    #[command(
        name = "kafka-sink",
        about = "Configure Kafka Sink",
        next_help_heading = "KAFKA OPTIONS"
    )]
    KafkaSink(#[command(flatten)] KafkaConfig),
    // KinesisSink,
    // PulsarSink, etc.
}

impl fmt::Display for Connectors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Connectors::KafkaSink(_) => write!(f, "KafkaSink"),
        }
    }
}

#[derive(ValueEnum, Default, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BadData {
    #[default]
    Fail,
    Drop,
    Dlt, //TODO: Implement Dead Letter Topic support when needed
}

impl FromStr for BadData {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "drop" => Ok(BadData::Drop),
            "fail" => Ok(BadData::Fail),
            "dlt" => Ok(BadData::Dlt),
            _ => Err(format!("Invalid bad data policy: {}", s)),
        }
    }
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        ConnectorConfig {
            bad_data: BadData::Drop,
            connectors: Connectors::KafkaSink(KafkaConfig::default()),
        }
    }
}

impl ConnectorConfig {
    pub fn from(matches: &ArgMatches) -> Option<ConnectorConfig> {
        matches
            .subcommand_matches("connectors")
            .and_then(|connector_matches| {
                match ConnectorConfig::from_arg_matches(connector_matches) {
                    Ok(config) => Some(config),
                    Err(err) => err.exit(),
                }
            })
    }
}
