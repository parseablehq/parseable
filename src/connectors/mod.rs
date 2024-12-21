use crate::connectors::common::config::ConnectorConfig;
use crate::connectors::common::processor::Processor;
use crate::connectors::common::shutdown::Shutdown;
use crate::connectors::kafka::consumer::KafkaStreams;
use crate::connectors::kafka::metrics::KafkaConsumerMetricsCollector;
use crate::connectors::kafka::processor::ParseableSinkProcessor;
use crate::connectors::kafka::rebalance_listener::RebalanceListener;
use crate::connectors::kafka::sink::KafkaSinkConnector;
use crate::connectors::kafka::state::StreamState;
use crate::connectors::kafka::{ConsumerRecord, KafkaContext};
use crate::metrics;
use crate::option::{Mode, CONFIG};
use prometheus::Registry;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub mod common;
pub mod kafka;

pub async fn init() -> anyhow::Result<()> {
    if matches!(CONFIG.parseable.mode, Mode::Ingest | Mode::All) {
        match CONFIG.parseable.connector_config.clone() {
            Some(config) => {
                let shutdown_handle = Shutdown::new();
                let prometheus = metrics::build_metrics_handler();
                let registry = prometheus.registry.clone();
                let processor = ParseableSinkProcessor;

                tokio::spawn({
                    let shutdown_handle = shutdown_handle.clone();
                    async move {
                        shutdown_handle.signal_listener().await;
                        info!("Connector received shutdown signal!");
                    }
                });

                run_kafka2parseable(config, registry, processor, shutdown_handle).await?
            }
            None => {
                warn!("Kafka connector configuration is missing. Skipping Kafka pipeline.");
            }
        }
    }

    Ok(())
}

async fn run_kafka2parseable<P>(
    config: ConnectorConfig,
    registry: Registry,
    processor: P,
    shutdown_handle: Shutdown,
) -> anyhow::Result<()>
where
    P: Processor<Vec<ConsumerRecord>, ()> + Send + Sync + 'static,
{
    let kafka_config = Arc::new(config.kafka_config.clone().unwrap_or_default());
    let (kafka_context, rebalance_rx) = KafkaContext::new(kafka_config);

    //TODO: fetch topics metadata from kafka then give dynamic value to StreamState
    let stream_state = Arc::new(RwLock::new(StreamState::new(60)));
    let rebalance_listener = RebalanceListener::new(
        rebalance_rx,
        Arc::clone(&stream_state),
        shutdown_handle.clone(),
    );

    let kafka_streams = KafkaStreams::init(kafka_context, stream_state, shutdown_handle.clone())?;

    let stats = kafka_streams.statistics();
    registry.register(Box::new(KafkaConsumerMetricsCollector::new(stats)))?;

    let kafka_parseable_sink_connector = KafkaSinkConnector::new(
        kafka_streams,
        processor,
        config.buffer_size,
        config.buffer_timeout,
    );

    rebalance_listener.start();
    kafka_parseable_sink_connector.run().await?;

    Ok(())
}