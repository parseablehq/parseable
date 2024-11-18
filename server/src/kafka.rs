use chrono::Utc;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError as NativeKafkaError, RDKafkaError};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use std::env::VarError;
use std::fmt::Display;
use std::num::ParseIntError;
use std::{collections::HashMap, env, fmt::Debug, str::FromStr, time::Duration};
use tokio::task::{self, JoinHandle};

use crate::{
    event::{
        self,
        error::EventError,
        format::{self, EventFormat},
    },
    handlers::http::ingest::{create_stream_if_not_exists, PostError},
    metadata::{error::stream_info::MetadataError, STREAM_INFO},
    storage::StreamType,
};

enum SslProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}
impl Display for SslProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SslProtocol::Plaintext => "plaintext",
            SslProtocol::Ssl => "ssl",
            SslProtocol::SaslPlaintext => "sasl_plaintext",
            SslProtocol::SaslSsl => "sasl_ssl",
        })
    }
}
impl FromStr for SslProtocol {
    type Err = KafkaError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "plaintext" => Ok(SslProtocol::Plaintext),
            "ssl" => Ok(SslProtocol::Ssl),
            "sasl_plaintext" => Ok(SslProtocol::SaslPlaintext),
            "sasl_ssl" => Ok(SslProtocol::SaslSsl),
            _ => Err(KafkaError::InvalidSslProtocolError(s.to_string())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Error loading environment variable {0}")]
    NoVarError(&'static str),

    #[error("Kafka error {0}")]
    NativeError(#[from] NativeKafkaError),
    #[error("RDKafka error {0}")]
    RDKError(#[from] RDKafkaError),

    #[error("Error parsing int {1} for environment variable {0}")]
    ParseIntError(&'static str, ParseIntError),
    #[error("Error parsing duration int {1} for environment variable {0}")]
    ParseDurationError(&'static str, ParseIntError),

    #[error("Stream not found: #{0}")]
    StreamNotFound(String),
    #[error("Post error: #{0}")]
    PostError(#[from] PostError),
    #[error("Metadata error: #{0}")]
    MetadataError(#[from] MetadataError),
    #[error("Event error: #{0}")]
    EventError(#[from] EventError),
    #[error("JSON error: #{0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Invalid group offset storage: #{0}")]
    InvalidGroupOffsetStorage(String),

    #[error("Invalid SSL protocol: #{0}")]
    InvalidSslProtocolError(String),
    #[error("Invalid unicode for environment variable {0}")]
    EnvNotUnicode(&'static str),
}

fn load_env_or_err(key: &'static str) -> Result<String, KafkaError> {
    env::var(key).map_err(|_| KafkaError::NoVarError(key))
}
fn parse_auto_env<T>(key: &'static str) -> Result<Option<T>, <T as FromStr>::Err>
where
    T: FromStr,
{
    Ok(if let Ok(val) = env::var(key) {
        Some(val.parse::<T>()?)
    } else {
        None
    })
}
fn handle_duration_env_prefix(key: &'static str) -> Result<Option<Duration>, ParseIntError> {
    if let Ok(raw_secs) = env::var(format!("{key}_S")) {
        Ok(Some(Duration::from_secs(u64::from_str(&raw_secs)?)))
    } else if let Ok(raw_secs) = env::var(format!("{key}_M")) {
        Ok(Some(Duration::from_secs(u64::from_str(&raw_secs)? * 60)))
    } else {
        Ok(None)
    }
}
fn parse_i32_env(key: &'static str) -> Result<Option<i32>, KafkaError> {
    parse_auto_env::<i32>(key).map_err(|raw| KafkaError::ParseIntError(key, raw))
}

fn parse_duration_env_prefixed(key_prefix: &'static str) -> Result<Option<Duration>, KafkaError> {
    handle_duration_env_prefix(key_prefix)
        .map_err(|raw| KafkaError::ParseDurationError(key_prefix, raw))
}

fn get_flag_env_val(key: &'static str) -> Result<Option<bool>, KafkaError> {
    let raw = env::var(key);
    match raw {
        Ok(val) => Ok(Some(val != "0" && val != "false")),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(KafkaError::EnvNotUnicode(key)),
    }
}

fn setup_consumer() -> Result<StreamConsumer, KafkaError> {
    let hosts = load_env_or_err("KAFKA_HOSTS")?;
    let topic = load_env_or_err("KAFKA_TOPIC")?;

    let mut conf = ClientConfig::new();
    conf.set("bootstrap.servers", &hosts);

    if let Ok(val) = env::var("KAFKA_CLIENT_ID") {
        conf.set("client.id", &val);
    }

    if let Some(val) = get_flag_env_val("a")? {
        conf.set("api.version.request", val.to_string());
    }
    if let Ok(val) = env::var("KAFKA_GROUP") {
        conf.set("group.id", &val);
    }

    if let Ok(val) = env::var("KAFKA_SECURITY_PROTOCOL") {
        let mapped: SslProtocol = val.parse()?;
        conf.set("security.protocol", &mapped.to_string());
    }
    let consumer: StreamConsumer = conf.create()?;

    if let Ok(vals_raw) = env::var("KAFKA_PARTITIONS") {
        let vals = vals_raw
            .split(',')
            .map(i32::from_str)
            .collect::<Result<Vec<i32>, ParseIntError>>()
            .map_err(|raw| KafkaError::ParseIntError("KAFKA_PARTITIONS", raw))?;

        let mut parts = TopicPartitionList::new();
        for val in vals {
            parts.add_partition(&topic, val);
        }
        consumer.seek_partitions(parts, Timeout::Never)?;
    }
    Ok(consumer)
}

fn ingest_message<'a>(stream_name: &str, msg: BorrowedMessage<'a>) -> Result<(), KafkaError> {
    log::debug!("{}: Message: {:?}", stream_name, msg);
    if let Some(payload) = msg.payload() {
        let hash_map = STREAM_INFO.read().unwrap();
        let schema = hash_map
            .get(stream_name)
            .ok_or(KafkaError::StreamNotFound(stream_name.to_owned()))?
            .schema
            .clone();

        let event = format::json::Event {
            data: serde_json::from_slice(payload)?,
            tags: String::default(),
            metadata: String::default(),
        };
        log::debug!("Generated event: {:?}", event.data);
        let (rb, is_first) = event.into_recordbatch(schema, None, None).unwrap();

        event::Event {
            rb,
            stream_name: stream_name.to_string(),
            origin_format: "json",
            origin_size: payload.len() as u64,
            is_first_event: is_first,
            parsed_timestamp: Utc::now().naive_utc(),
            time_partition: None,
            custom_partition_values: HashMap::new(),
            stream_type: StreamType::UserDefined,
        }
        .process_unchecked()?;
    } else {
        log::debug!("{} No payload for stream", stream_name);
    }
    Ok(())
}

pub async fn setup_integration() -> Result<JoinHandle<()>, KafkaError> {
    let my_res = if let Ok(stream_name) = env::var("KAFKA_TOPIC") {
        log::info!("Setup kafka integration for {stream_name}");
        create_stream_if_not_exists(&stream_name, &StreamType::UserDefined.to_string()).await?;

        let res = task::spawn(async move {
            let consumer = setup_consumer().unwrap();
            let mut stream = consumer.stream();
            loop {
                while let Some(curr) = stream.next().await {
                    let msg = curr.unwrap();
                    ingest_message(&stream_name, msg).unwrap();
                }
            }
        });
        log::info!("Done Setup kafka integration");
        res
    } else {
        task::spawn_blocking(|| {})
    };
    Ok(my_res)
}
