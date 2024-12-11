use arrow_schema::Field;
use chrono::Utc;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError as NativeKafkaError, RDKafkaError};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use std::fmt::Display;
use std::num::ParseIntError;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug, str::FromStr};
use tracing::{debug, error, info};

use crate::option::CONFIG;
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

#[allow(dead_code)]
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
    #[error("")]
    DoNotPrintError,
}

// // Commented out functions
// // Might come in handy later
// fn parse_auto_env<T>(key: &'static str) -> Result<Option<T>, <T as FromStr>::Err>
// where
//     T: FromStr,
// {
//     Ok(if let Ok(val) = env::var(key) {
//         Some(val.parse::<T>()?)
//     } else {
//         None
//     })
// }

// fn handle_duration_env_prefix(key: &'static str) -> Result<Option<Duration>, ParseIntError> {
//     if let Ok(raw_secs) = env::var(format!("{key}_S")) {
//         Ok(Some(Duration::from_secs(u64::from_str(&raw_secs)?)))
//     } else if let Ok(raw_secs) = env::var(format!("{key}_M")) {
//         Ok(Some(Duration::from_secs(u64::from_str(&raw_secs)? * 60)))
//     } else {
//         Ok(None)
//     }
// }

// fn parse_i32_env(key: &'static str) -> Result<Option<i32>, KafkaError> {
//     parse_auto_env::<i32>(key).map_err(|raw| KafkaError::ParseIntError(key, raw))
// }

// fn parse_duration_env_prefixed(key_prefix: &'static str) -> Result<Option<Duration>, KafkaError> {
//     handle_duration_env_prefix(key_prefix)
//         .map_err(|raw| KafkaError::ParseDurationError(key_prefix, raw))
// }

fn setup_consumer() -> Result<(StreamConsumer, String), KafkaError> {
    if let Some(topic) = &CONFIG.parseable.kafka_topic {
        let host = if CONFIG.parseable.kafka_host.is_some() {
            CONFIG.parseable.kafka_host.as_ref()
        } else {
            return Err(KafkaError::NoVarError("Please set P_KAKFA_HOST env var (To use Kafka integration env vars P_KAFKA_TOPIC, P_KAFKA_HOST, and P_KAFKA_GROUP are mandatory)"));
        };

        let group = if CONFIG.parseable.kafka_group.is_some() {
            CONFIG.parseable.kafka_group.as_ref()
        } else {
            return Err(KafkaError::NoVarError("Please set P_KAKFA_GROUP env var (To use Kafka integration env vars P_KAFKA_TOPIC, P_KAFKA_HOST, and P_KAFKA_GROUP are mandatory)"));
        };

        let mut conf = ClientConfig::new();
        conf.set("bootstrap.servers", host.unwrap());
        conf.set("group.id", group.unwrap());

        if let Some(val) = CONFIG.parseable.kafka_client_id.as_ref() {
            conf.set("client.id", val);
        }

        // if let Some(val) = get_flag_env_val("a")? {
        //     conf.set("api.version.request", val.to_string());
        // }

        if let Some(val) = CONFIG.parseable.kafka_security_protocol.as_ref() {
            let mapped: SslProtocol = val.parse()?;
            conf.set("security.protocol", mapped.to_string());
        }

        let consumer: StreamConsumer = conf.create()?;
        consumer.subscribe(&[topic.as_str()])?;

        if let Some(vals_raw) = CONFIG.parseable.kafka_partitions.as_ref() {
            let vals = vals_raw
                .split(',')
                .map(i32::from_str)
                .collect::<Result<Vec<i32>, ParseIntError>>()
                .map_err(|raw| KafkaError::ParseIntError("P_KAFKA_PARTITIONS", raw))?;

            let mut parts = TopicPartitionList::new();
            for val in vals {
                parts.add_partition(topic, val);
            }
            consumer.seek_partitions(parts, Timeout::Never)?;
        }
        Ok((consumer, topic.clone()))
    } else {
        // if the user hasn't even set KAFKA_TOPIC
        // then they probably don't want to use the integration
        // send back the DoNotPrint error
        Err(KafkaError::DoNotPrintError)
    }
}

fn resolve_schema(stream_name: &str) -> Result<HashMap<String, Arc<Field>>, KafkaError> {
    let hash_map = STREAM_INFO.read().unwrap();
    let raw = hash_map
        .get(stream_name)
        .ok_or_else(|| KafkaError::StreamNotFound(stream_name.to_owned()))?;
    Ok(raw.schema.clone())
}

async fn ingest_message<'a>(stream_name: &str, msg: BorrowedMessage<'a>) -> Result<(), KafkaError> {
    if let Some(payload) = msg.payload() {
        // stream should get created only if there is an incoming event, not before that
        create_stream_if_not_exists(stream_name, &StreamType::UserDefined.to_string()).await?;

        let schema = resolve_schema(stream_name)?;
        let event = format::json::Event {
            data: serde_json::from_slice(payload)?,
            tags: String::default(),
            metadata: String::default(),
        };

        let time_partition = STREAM_INFO.get_time_partition(stream_name)?;
        let static_schema_flag = STREAM_INFO.get_static_schema_flag(stream_name)?;

        let (rb, is_first) = event
            .into_recordbatch(schema, static_schema_flag, time_partition)
            .map_err(|err| KafkaError::PostError(PostError::CustomError(err.to_string())))?;

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
        .process()
        .await?;
    } else {
        debug!("{} No payload for stream", stream_name);
    }
    Ok(())
}

pub async fn setup_integration() {
    tokio::task::spawn(async move {
        let (consumer, stream_name) = match setup_consumer() {
            Ok(c) => c,
            Err(err) => {
                match err {
                    KafkaError::DoNotPrintError => {
                        debug!("P_KAFKA_TOPIC not set, skipping kafka integration");
                    }
                    _ => {
                        error!("{err}");
                    }
                }
                return;
            }
        };

        info!("Setup kafka integration for {stream_name}");
        let mut stream = consumer.stream();

        while let Ok(curr) = stream.next().await.unwrap() {
            match ingest_message(&stream_name, curr).await {
                Ok(_) => {}
                Err(err) => error!("Unable to ingest incoming kafka message- {err}"),
            };
        }
    });
}
