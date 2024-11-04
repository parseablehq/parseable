use kafka::{client::FetchPartition, consumer::Consumer};
use std::{env, fmt::Debug, str::FromStr, time::Duration};
use tokio::task::JoinHandle;
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Error loading environment variable {0}")]
    NoVarError(&'static str),

    #[error("Kafka error {0}")]
    NativeError(#[from] kafka::Error),

    #[error("Error parsing int {1} for environment variable {0}")]
    ParseIntError(&'static str, std::num::ParseIntError),
    #[error("Error parsing duration int {1} for environment variable {0}")]
    ParseDurationError(&'static str, std::num::ParseIntError),
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
) -> Result<Option<Duration>, std::num::ParseIntError> {
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

    let mut cb = Consumer::from_hosts(mapped_hosts).with_topic(topic);

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
    let res = cb.create()?;
    Ok(res)
}

pub fn setup_integration() -> Result<JoinHandle<()>, KafkaError> {
    log::info!("Setup kafka integration");
    let mut c = setup_consumer()?;
    let parts: &[FetchPartition] = &[];
    println!("Messages: {:?}", c.client_mut().fetch_messages(parts));
    let res = tokio::task::spawn_blocking(move || loop {
        for ev in c.poll().unwrap().iter() {
            for msg in ev.messages() {
                log::info!("Message: {:?}", msg)
            }
        }
    });
    log::info!("Done Setup kafka integration");
    Ok(res)
}
