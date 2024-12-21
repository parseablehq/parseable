use crate::connectors::kafka::partition_stream_queue::PartitionStreamSender;
use crate::connectors::kafka::{TopicPartition, TopicPartitionList};
use std::collections::HashMap;
use tracing::info;

pub struct StreamState {
    partition_senders: HashMap<TopicPartition, PartitionStreamSender>,
}

impl StreamState {
    pub fn new(capacity: usize) -> Self {
        Self {
            partition_senders: HashMap::with_capacity(capacity),
        }
    }

    pub fn insert_partition_sender(
        &mut self,
        tp: TopicPartition,
        sender: PartitionStreamSender,
    ) -> Option<PartitionStreamSender> {
        self.partition_senders.insert(tp, sender)
    }

    pub fn get_partition_sender(&self, tp: &TopicPartition) -> Option<&PartitionStreamSender> {
        self.partition_senders.get(tp)
    }

    pub async fn terminate_partition_streams(&mut self, tpl: TopicPartitionList) {
        info!("Terminating streams: {:?}", tpl);

        for tp in tpl.tpl {
            if let Some(sender) = self.partition_senders.remove(&tp) {
                info!("Terminating stream for {:?}", tp);
                drop(sender.sender());
                sender.terminate();
                info!("Waiting for stream to finish for {:?}", tp);
            } else {
                info!("Stream already completed for {:?}", tp);
            }
        }

        info!("All streams terminated!");
    }

    pub fn clear(&mut self) {
        info!("Clearing all stream states...");
        self.partition_senders.clear();
    }
}
