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
use crate::connectors::common::build_runtime;
use crate::connectors::common::processor::Processor;
use crate::connectors::kafka::ConsumerRecord;
use crate::connectors::kafka::consumer::KafkaStreams;
use crate::connectors::kafka::processor::StreamWorker;
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::consumer::Consumer;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::{error, info};

pub struct KafkaSinkConnector<P>
where
    P: Processor<Vec<ConsumerRecord>, ()>,
{
    streams: KafkaStreams,
    stream_processor: Arc<StreamWorker<P>>,
    runtime: Runtime,
}

impl<P> KafkaSinkConnector<P>
where
    P: Processor<Vec<ConsumerRecord>, ()> + Send + Sync + 'static,
{
    pub fn new(kafka_streams: KafkaStreams, processor: P) -> Self {
        let consumer = kafka_streams.consumer();
        let stream_processor = Arc::new(StreamWorker::new(
            Arc::new(processor),
            Arc::clone(&consumer),
        ));

        let runtime = build_runtime(
            consumer.context().config.partition_listener_concurrency,
            "kafka-sink-worker",
        )
        .expect("Failed to build runtime");
        let _ = runtime.enter();

        Self {
            streams: kafka_streams,
            stream_processor,
            runtime,
        }
    }

    pub async fn run(self) -> Result<()> {
        self.streams
            .partitioned()
            .map(|partition_stream| {
                let worker = Arc::clone(&self.stream_processor);
                let tp = partition_stream.topic_partition().clone();
                self.runtime.spawn(async move {
                    partition_stream
                        .run_drain(|partition_records| async {
                            info!("Starting task for partition: {:?}", tp);

                            worker
                                .process_partition(tp.clone(), partition_records)
                                .await
                                .unwrap();
                        })
                        .await;

                    info!("Task completed for partition: {:?}", tp);
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
