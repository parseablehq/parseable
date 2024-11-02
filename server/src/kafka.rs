use kafka::{client::FetchPartition, consumer::Consumer};
use std::{env, fmt::Debug, str::FromStr};
use tokio::task::JoinHandle;
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Error loading environment variable {0}")]
    NoVarError(&'static str),

    #[error("Kafka error {0}")]
    NativeError(#[from] kafka::Error),
}

fn load_env_or_err(key: &'static str) -> Result<String, KafkaError> {
    env::var(key).map_err(|_| KafkaError::NoVarError(key))
}
fn parse_auto_env<T>(key: &'static str) -> Option<T>
where
    T: FromStr,
    <T as FromStr>::Err: Debug,
{
    if let Ok(val) = env::var(key) {
        Some(val.parse::<T>().unwrap())
    } else {
        None
    }
}
fn setup_consumer() -> Result<Consumer, KafkaError> {
    let hosts = load_env_or_err("KAFKA_HOSTS")?;
    let topic = load_env_or_err("KAFKA_TOPIC")?;
    let mapped_hosts = hosts.split(",").map(|s| s.to_string()).collect();

    let mut cb = Consumer::from_hosts(mapped_hosts).with_topic(topic);

    if let Ok(val) = env::var("KAFKA_CLIENT_ID") {
        cb = cb.with_client_id(val)
    }
    if let Some(val) = parse_auto_env::<i32>("KAFKA_FETCH_MAX_BYTES_PER_PARTITION") {
        cb = cb.with_fetch_max_bytes_per_partition(val)
    }

    if let Some(val) = parse_auto_env::<i32>("KAFKA_FETCH_MIN_BYTES") {
        cb = cb.with_fetch_min_bytes(val)
    }

    if let Some(val) = parse_auto_env::<i32>("KAFKA_RETRY_MAX_BYTES_LIMIT") {
        cb = cb.with_retry_max_bytes_limit(val)
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
