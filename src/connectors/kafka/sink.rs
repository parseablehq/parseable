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

use crate::connectors::common::processor::Processor;
use crate::connectors::kafka::consumer::KafkaStreams;
use crate::connectors::kafka::processor::StreamWorker;
use crate::connectors::kafka::ConsumerRecord;
use anyhow::Result;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::error;

pub struct KafkaSinkConnector<P>
where
    P: Processor<Vec<ConsumerRecord>, ()>,
{
    kafka_streams: KafkaStreams,
    worker: Arc<StreamWorker<P>>,
}

impl<P> KafkaSinkConnector<P>
where
    P: Processor<Vec<ConsumerRecord>, ()> + Send + Sync + 'static,
{
    pub fn new(
        kafka_streams: KafkaStreams,
        processor: P,
        buffer_size: usize,
        buffer_timeout: Duration,
    ) -> Self {
        let worker = Arc::new(StreamWorker::new(
            Arc::new(processor),
            kafka_streams.consumer(),
            buffer_size,
            buffer_timeout,
        ));

        Self {
            kafka_streams,
            worker,
        }
    }

    pub async fn run(self) -> Result<()> {
        self.kafka_streams
            .partitioned()
            .map(|partition_queue| {
                let worker = Arc::clone(&self.worker);
                let tp = partition_queue.topic_partition().clone();
                tokio::spawn(async move {
                    partition_queue
                        .run_drain(|record_stream| async {
                            worker
                                .process_partition(tp.clone(), record_stream)
                                .await
                                .unwrap();
                        })
                        .await
                })
            })
            .for_each_concurrent(None, |task| async {
                if let Err(e) = task.await {
                    error!("Task failed: {:?}", e);
                }
            })
            .await;

        Ok(())
    }
}
