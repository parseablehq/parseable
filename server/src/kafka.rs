use kafka::{client::FetchPartition, consumer::Consumer};
use std::env;
use tokio::task::JoinHandle;
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Error loading environment variable {0}")]
    NoVarError(&'static str),

    #[error("Kafka error {0}")]
    NativeError(#[from] kafka::Error),
}

fn load_env_or_err(name: &'static str) -> Result<String, KafkaError> {
    env::var(name).map_err(|_| KafkaError::NoVarError(name))
}

fn setup_consumer() -> Result<Consumer, KafkaError> {
    let hosts = load_env_or_err("KAFKA_HOSTS")?;
    let topic = load_env_or_err("KAFKA_TOPIC")?;
    let mapped_hosts = hosts.split(",").map(|s| s.to_string()).collect();

    let mut cb = Consumer::from_hosts(mapped_hosts).with_topic(topic);

    if let Ok(val) = env::var("KAFKA_CLIENT_ID") {
        cb = cb.with_client_id(val);
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
