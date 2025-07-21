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

use crate::connectors::common::ConnectorError;
use crate::connectors::common::shutdown::Shutdown;
use crate::connectors::kafka::partition_stream::{PartitionStreamReceiver, PartitionStreamSender};
use crate::connectors::kafka::state::StreamState;
use crate::connectors::kafka::{
    ConsumerRecord, KafkaContext, StreamConsumer, TopicPartition, partition_stream,
};
use futures_util::FutureExt;
use rdkafka::Statistics;
use rdkafka::consumer::Consumer;
use rdkafka::message::BorrowedMessage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

pub struct KafkaStreams {
    consumer: Arc<StreamConsumer>,
    stream_state: Arc<RwLock<StreamState>>,
    statistics: Arc<std::sync::RwLock<Statistics>>,
    shutdown_handle: Shutdown,
}

impl KafkaStreams {
    pub fn init(
        context: KafkaContext,
        stream_state: Arc<RwLock<StreamState>>,
        shutdown_handle: Shutdown,
    ) -> anyhow::Result<KafkaStreams> {
        info!("Initializing KafkaStreams...");
        let statistics = Arc::clone(&context.statistics);
        let consumer = KafkaStreams::create_consumer(context);
        info!("KafkaStreams initialized successfully.");

        Ok(Self {
            consumer,
            stream_state,
            statistics,
            shutdown_handle,
        })
    }

    pub fn consumer(&self) -> Arc<StreamConsumer> {
        Arc::clone(&self.consumer)
    }

    pub fn statistics(&self) -> Arc<std::sync::RwLock<Statistics>> {
        Arc::clone(&self.statistics)
    }

    pub fn state(&self) -> Arc<RwLock<StreamState>> {
        Arc::clone(&self.stream_state)
    }

    /// Manages Kafka partition streams manually due to limitations in `rust-rdkafka`'s `split_partition_queue`.
    ///
    /// This method continuously listens incoming Kafka messages, dynamically creating
    /// or updating streams for each partition. It is implemented using a separate standard thread to avoid
    /// potential deadlocks and long-running task issues encountered with `tokio::spawn`.
    ///
    /// Steps:
    /// 1. Consumes Kafka messages in a loop, processes each message to identify the associated partition.
    /// 2. Dynamically creates a new stream for untracked partitions, allowing for isolated processing.
    /// 3. Updates existing streams when new messages arrive for already tracked partitions.
    /// 4. Listens for shutdown signals and gracefully terminates all partition streams, unsubscribing the consumer.
    ///
    /// Limitations and References:
    ///   - Issues with `split_partition_queue` in rust-rdkafka:
    ///   - https://github.com/fede1024/rust-rdkafka/issues/535
    ///   - https://github.com/confluentinc/librdkafka/issues/4059
    ///   - https://github.com/confluentinc/librdkafka/issues/4059
    ///   - https://github.com/fede1024/rust-rdkafka/issues/654
    ///   - https://github.com/fede1024/rust-rdkafka/issues/651
    ///   - https://github.com/fede1024/rust-rdkafka/issues/604
    ///   - https://github.com/fede1024/rust-rdkafka/issues/564
    ///
    /// - Potential deadlocks and long-running task issues with `tokio::spawn`:
    /// - Details on blocking vs. async design choices:
    ///   - https://ryhl.io/blog/async-what-is-blocking/
    ///
    /// Returns:
    /// A `ReceiverStream` that produces `PartitionStreamReceiver` for each active partition.
    pub fn partitioned(&self) -> ReceiverStream<PartitionStreamReceiver> {
        let (stream_tx, stream_rx) = mpsc::channel(100);
        let consumer = self.consumer();
        let stream_state = self.state();
        let tokio_handle = tokio::runtime::Handle::current();
        let shutdown_handle = self.shutdown_handle.clone();

        std::thread::Builder::new().name("kafka-streams-thread".to_string()).spawn(move || {
            tokio_handle.block_on(async move {
                loop {
                    let result: Result<(), ConnectorError> = tokio::select! {
                        result = consumer.recv() => {
                            match result {
                                Ok(msg) => KafkaStreams::handle_message(msg, &stream_state, &stream_tx).await.map_err(Into::into),
                                Err(e) => Err(e.into())
                            }
                        }
                        _ = shutdown_handle.recv() => {
                            KafkaStreams::shutdown(&consumer, &stream_state).await;
                            break;
                        }
                    };

                    match result {
                        Ok(_) => continue,
                        Err(error) => match &error {
                            ConnectorError::Connection(msg) => {
                                error!("Connection error: {}", msg);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                            ConnectorError::Fatal(msg) => {
                                error!("Fatal error: {}", msg);
                                break;
                            }
                            ConnectorError::Auth(msg) => {
                                error!("Authentication error: {}", msg);
                                break;
                            }
                            error => {
                                warn!("Non-fatal error: {}", error);
                            }
                        },
                    }
                }

                info!("Kafka stream processing terminated");
            });
        }).expect("Failed to spawn Kafka partitioned stream thread");

        ReceiverStream::new(stream_rx)
    }

    /// Handle individual Kafka message and route it to the proper partition stream
    async fn handle_message(
        msg: BorrowedMessage<'_>,
        stream_state: &RwLock<StreamState>,
        stream_tx: &mpsc::Sender<PartitionStreamReceiver>,
    ) -> anyhow::Result<()> {
        let mut state = stream_state.write().await;
        let tp = TopicPartition::from_kafka_msg(&msg);
        let consumer_record = ConsumerRecord::from_borrowed_msg(msg);

        let partition_stream_tx =
            KafkaStreams::get_or_create_partition_stream(&mut state, stream_tx, tp).await;
        partition_stream_tx.send(consumer_record).await;

        Ok(())
    }

    async fn get_or_create_partition_stream(
        state: &mut StreamState,
        stream_tx: &mpsc::Sender<PartitionStreamReceiver>,
        tp: TopicPartition,
    ) -> PartitionStreamSender {
        if let Some(ps_tx) = state.get_partition_sender(&tp) {
            ps_tx.clone()
        } else {
            Self::create_new_partition_stream(state, stream_tx, tp).await
        }
    }

    async fn create_new_partition_stream(
        state: &mut StreamState,
        stream_tx: &mpsc::Sender<PartitionStreamReceiver>,
        tp: TopicPartition,
    ) -> PartitionStreamSender {
        info!("Creating new stream for {:?}", tp);

        let (ps_tx, ps_rx) = partition_stream::bounded(100_000, tp.clone());
        state.insert_partition_sender(tp.clone(), ps_tx.clone());

        if let Err(e) = stream_tx.send(ps_rx).await {
            error!(
                "Failed to send partition stream receiver for {:?}: {:?}",
                tp, e
            );
        }

        ps_tx
    }

    async fn shutdown(consumer: &Arc<StreamConsumer>, stream_state: &RwLock<StreamState>) {
        info!("Gracefully stopping kafka partition streams!");
        let mut state = stream_state.write().await;
        state.clear();
        consumer.unsubscribe();
    }

    fn create_consumer(context: KafkaContext) -> Arc<StreamConsumer> {
        let kafka_config = &context.config;
        let consumer_config = kafka_config.to_rdkafka_consumer_config();
        info!(
            "Creating Kafka consumer from configs: {:#?}",
            &consumer_config
        );

        let consumer: StreamConsumer = consumer_config
            .create_with_context(context.clone())
            .expect("Consumer creation failed");

        if consumer.recv().now_or_never().is_some() {
            panic!("Consumer should not have any messages");
        }

        let consumer = Arc::new(consumer);
        let topics = kafka_config
            .consumer()
            .expect("Consumer config is missing")
            .topics();

        KafkaStreams::subscribe(&consumer, &topics);

        consumer
    }

    fn subscribe(consumer: &Arc<StreamConsumer>, topics: &[&str]) {
        match consumer.subscribe(topics) {
            Ok(_) => {
                info!("Subscribed to topics: {:?}", topics);
            }
            Err(e) => {
                error!("Error subscribing to topics: {:?} {:?}", topics, e);
            }
        };
    }
}
