use std::env::{self, VarError};
use kafka::consumer::Consumer;
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Error loading environment variable {0}")]
    VarError(#[from] VarError),
}
pub fn setup_integration() -> Result<(), KafkaError> {
    let hosts = env::var("KAFKA_HOSTS")?;
    let c = Consumer::from_hosts(hosts.split(",").map(|s| s.to_string()).collect());
    Ok(())
}