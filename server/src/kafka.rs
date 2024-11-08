use chrono::Utc;
use kafka::consumer::{Consumer, Message};
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
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Error loading environment variable {0}")]
    NoVarError(&'static str),

    #[error("Kafka error {0}")]
    NativeError(#[from] kafka::Error),

    #[error("Error parsing int {1} for environment variable {0}")]
    ParseIntError(&'static str, ParseIntError),
    #[error("Error parsing duration int {1} for environment variable {0}")]
    ParseDurationError(&'static str, std::num::ParseIntError),

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
fn handle_duration_env_prefix(
    key: &'static str,
) -> Result<Option<Duration>, ParseIntError> {
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
fn setup_consumer() -> Result<Consumer, KafkaError> {
    let hosts = load_env_or_err("KAFKA_HOSTS")?;
    let topic = load_env_or_err("KAFKA_TOPIC")?;
    let mapped_hosts = hosts.split(",").map(|s| s.to_string()).collect();

    let mut cb = Consumer::from_hosts(mapped_hosts).with_topic(topic.clone());

    if let Ok(val) = env::var("KAFKA_CLIENT_ID") {
        cb = cb.with_client_id(val)
    }
    if let Some(val) = parse_i32_env("KAFKA_FETCH_MAX_BYTES_PER_PARTITION")? {
        cb = cb.with_fetch_max_bytes_per_partition(val)
    }

    if let Some(val) = parse_i32_env("KAFKA_FETCH_MIN_BYTES")? {
        cb = cb.with_fetch_min_bytes(val)
    }

    if let Some(val) = parse_i32_env("KAFKA_RETRY_MAX_BYTES_LIMIT")? {
        cb = cb.with_retry_max_bytes_limit(val)
    }

    if let Some(val) = parse_duration_env_prefixed("KAFKA_CONNECTION_IDLE_TIMEOUT")? {
        cb = cb.with_connection_idle_timeout(val)
    }

    if let Some(val) = parse_duration_env_prefixed("KAFKA_FETCH_MAX_WAIT_TIME")? {
        cb = cb.with_fetch_max_wait_time(val)
    }

    if let Ok(val) = env::var("KAFKA_GROUP") {
        cb = cb.with_group(val)
    }
    if let Ok(vals_raw) = env::var("KAFKA_PARTITIONS") {
        let vals = vals_raw
            .split(',')
            .map(i32::from_str)
            .collect::<Result<Vec<i32>, ParseIntError>>();
        cb = cb.with_topic_partitions(
            topic,
            &vals.map_err(|raw| KafkaError::ParseIntError("KAFKA_PARTITIONS", raw))?,
        );
    }

    let res = cb.create()?;
    Ok(res)
}

fn ingest_message<'a>(stream_name: &str, msg: &Message<'a>) -> Result<(), KafkaError> {
    log::debug!("Message: {:?}", msg);
    let hash_map = STREAM_INFO.read().unwrap();
    let schema = hash_map
        .get(stream_name)
        .ok_or(KafkaError::StreamNotFound(stream_name.to_owned()))?
        .schema
        .clone();

    let txt = String::from_utf8(msg.value.into_iter().copied().collect());
    log::debug!("Message text: {}", txt.unwrap());
    let event = format::json::Event {
        data: serde_json::from_slice(&msg.value)?,
        tags: String::default(),
        metadata: String::default(),
    };
    log::debug!("Generated event: {:?}", event.data);
    let (rb, is_first) = event.into_recordbatch(schema, None, None).unwrap();

    event::Event {
        rb,
        stream_name: stream_name.to_string(),
        origin_format: "json",
        origin_size: msg.value.len() as u64,
        is_first_event: is_first,
        parsed_timestamp: Utc::now().naive_utc(),
        time_partition: None,
        custom_partition_values: HashMap::new(),
        stream_type: StreamType::UserDefined,
    }
    .process_unchecked()?;
    Ok(())
}

pub async fn setup_integration() -> Result<JoinHandle<()>, KafkaError> {
    let my_res = if let Ok(stream_name) = env::var("KAFKA_TOPIC") {
        log::info!("Setup kafka integration for {stream_name}");
        create_stream_if_not_exists(&stream_name, &StreamType::UserDefined.to_string()).await?;

        let res = task::spawn_blocking(move || {
            let mut c = setup_consumer().unwrap();
            loop {
                for ev in c.poll().unwrap().iter() {
                    for msg in ev.messages() {
                        ingest_message(&stream_name, msg).unwrap();
                    }
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
