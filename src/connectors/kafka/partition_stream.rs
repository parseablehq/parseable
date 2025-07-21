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

use crate::connectors::kafka::{ConsumerRecord, TopicPartition};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

#[derive(Clone)]
pub struct PartitionStreamSender {
    inner: mpsc::Sender<ConsumerRecord>,
    notify: Arc<Notify>,
}

impl PartitionStreamSender {
    fn new(inner: mpsc::Sender<ConsumerRecord>, notify: Arc<Notify>) -> Self {
        Self { inner, notify }
    }

    pub fn terminate(&self) {
        self.notify.notify_waiters();
    }

    pub async fn send(&self, consumer_record: ConsumerRecord) {
        if let Err(e) = self.inner.send(consumer_record).await {
            error!("Failed to send message to partition stream: {:?}", e);
        }
    }

    pub fn sender(&self) -> mpsc::Sender<ConsumerRecord> {
        self.inner.clone()
    }
}

pub struct PartitionStreamReceiver {
    inner: ReceiverStream<ConsumerRecord>,
    topic_partition: TopicPartition,
    notify: Arc<Notify>,
}

impl PartitionStreamReceiver {
    fn new(
        receiver: mpsc::Receiver<ConsumerRecord>,
        topic_partition: TopicPartition,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            inner: ReceiverStream::new(receiver),
            topic_partition,
            notify,
        }
    }

    /// Processes the stream with a provided callback and listens for termination.
    ///
    /// # Parameters
    /// - `invoke`: A callback function that processes the `ReceiverStream<ConsumerRecord>`.
    ///
    /// # Behavior
    /// - The callback runs until either the stream is completed or a termination signal is received.
    pub async fn run_drain<Fut, F>(self, f: F)
    where
        F: Fn(ReceiverStream<ConsumerRecord>) -> Fut,
        Fut: futures_util::Future<Output = ()>,
    {
        let notify = self.notify.clone();

        tokio::select! {
            _ = f(self.inner) => {
                info!("PartitionStreamReceiver completed processing for {:?}.", self.topic_partition);
            }
            _ = notify.notified() => {
                info!("Received termination signal for {:?}.", self.topic_partition);
            }
        }
    }

    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }
}

pub fn bounded(
    size: usize,
    topic_partition: TopicPartition,
) -> (PartitionStreamSender, PartitionStreamReceiver) {
    let (tx, rx) = mpsc::channel(size);
    let notify = Arc::new(Notify::new());

    let sender = PartitionStreamSender::new(tx, notify.clone());
    let receiver = PartitionStreamReceiver::new(rx, topic_partition, notify);

    (sender, receiver)
}
