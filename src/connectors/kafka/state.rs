/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use crate::connectors::kafka::partition_stream::PartitionStreamSender;
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
                sender.terminate();
                drop(sender);
                info!("Stream terminated for {:?}", tp);
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
