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

use crate::connectors::common::shutdown::Shutdown;
use crate::connectors::kafka::partition_stream::{PartitionStreamReceiver, PartitionStreamSender};
use crate::connectors::kafka::state::StreamState;
use crate::connectors::kafka::{
    partition_stream, ConsumerRecord, KafkaContext, StreamConsumer, TopicPartition,
};
use backon::{ExponentialBuilder, Retryable};
use futures_util::FutureExt;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::Statistics;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

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

        std::thread::spawn(move || {
            tokio_handle.block_on(async move {
                let retry_policy = ExponentialBuilder::default().with_max_times(5000);

                loop {
                    let result = KafkaStreams::process_consumer_messages(
                        &consumer,
                        &stream_state,
                        &stream_tx,
                        &shutdown_handle,
                        &retry_policy,
                    )
                    .await;

                    if let Err(e) = result {
                        error!(
                            "Partitioned processing encountered a critical error: {:?}",
                            e
                        );
                        break;
                    }
                }
            });
        });

        ReceiverStream::new(stream_rx)
    }

    async fn process_consumer_messages(
        consumer: &Arc<StreamConsumer>,
        stream_state: &RwLock<StreamState>,
        stream_tx: &mpsc::Sender<PartitionStreamReceiver>,
        shutdown_handle: &Shutdown,
        retry_policy: &ExponentialBuilder,
    ) -> anyhow::Result<()> {
        tokio::select! {
            result = KafkaStreams::receive_with_retry(consumer, retry_policy) => match result {
                Ok(msg) => KafkaStreams::handle_message(msg, stream_state, stream_tx).await,
                Err(err) => {
                    anyhow::bail!("Unrecoverable error occurred while receiving Kafka message: {:?}", err);
                },
            },
            _ = shutdown_handle.recv() => {
                KafkaStreams::handle_shutdown(consumer, stream_state).await;
                Ok(())
            },
            else => {
                error!("KafkaStreams terminated unexpectedly!");
                Ok(())
            }
        }
    }

    async fn receive_with_retry<'a>(
        consumer: &'a Arc<StreamConsumer>,
        retry_policy: &'a ExponentialBuilder,
    ) -> Result<BorrowedMessage<'a>, KafkaError> {
        let recv_fn = || consumer.recv();

        recv_fn
            .retry(*retry_policy)
            .sleep(tokio::time::sleep)
            .notify(|err, dur| {
                tracing::warn!(
                    "Retrying message reception due to error: {:?}. Waiting for {:?}...",
                    err,
                    dur
                );
            })
            .await
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

    async fn handle_shutdown(consumer: &Arc<StreamConsumer>, stream_state: &RwLock<StreamState>) {
        info!("Gracefully stopping kafka partition streams!");
        let mut state = stream_state.write().await;
        state.clear();
        consumer.unsubscribe();
    }

    fn create_consumer(context: KafkaContext) -> Arc<StreamConsumer> {
        info!("Creating Kafka consumer from configs {:#?}", context.config);

        let kafka_config = &context.config;
        let consumer_config = kafka_config.to_rdkafka_consumer_config();
        info!("Consumer configs: {:#?}", &consumer_config);

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
