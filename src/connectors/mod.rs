/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::sync::Arc;

use actix_web_prometheus::PrometheusMetrics;
use common::{processor::Processor, shutdown::Shutdown};
use kafka::{
    ConsumerRecord, KafkaContext, config::KafkaConfig, consumer::KafkaStreams,
    metrics::KafkaMetricsCollector, processor::ParseableSinkProcessor,
    rebalance_listener::RebalanceListener, sink::KafkaSinkConnector, state::StreamState,
};
use prometheus::Registry;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{option::Mode, parseable::PARSEABLE};

pub mod common;
pub mod kafka;

pub async fn init(prometheus: &PrometheusMetrics) -> anyhow::Result<()> {
    if matches!(PARSEABLE.options.mode, Mode::Ingest | Mode::All) {
        match PARSEABLE.kafka_config.validate() {
            Err(e) => {
                warn!("Kafka connector configuration invalid. {}", e);
            }
            Ok(_) => {
                let config = PARSEABLE.kafka_config.clone();
                let shutdown_handle = Shutdown::default();
                let registry = prometheus.registry.clone();
                let processor = ParseableSinkProcessor;

                tokio::spawn({
                    let shutdown_handle = shutdown_handle.clone();
                    async move {
                        shutdown_handle.signal_listener().await;
                        info!("Connector received shutdown signal!");
                    }
                });

                run_kafka2parseable(config, registry, processor, shutdown_handle).await?;
            }
        }
    }

    Ok(())
}

async fn run_kafka2parseable<P>(
    config: KafkaConfig,
    registry: Registry,
    processor: P,
    shutdown_handle: Shutdown,
) -> anyhow::Result<()>
where
    P: Processor<Vec<ConsumerRecord>, ()> + Send + Sync + 'static,
{
    info!("Initializing KafkaSink connector...");

    let kafka_config = Arc::new(config.clone());
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
    registry.register(Box::new(KafkaMetricsCollector::new(stats)?))?;

    let kafka_parseable_sink_connector = KafkaSinkConnector::new(kafka_streams, processor);

    rebalance_listener.start();
    kafka_parseable_sink_connector.run().await?;

    Ok(())
}
