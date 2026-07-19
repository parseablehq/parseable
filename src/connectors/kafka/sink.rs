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
use crate::connectors::common::build_runtime;
use crate::connectors::common::processor::Processor;
use crate::connectors::kafka::ConsumerRecord;
use crate::connectors::kafka::consumer::KafkaStreams;
use crate::connectors::kafka::processor::StreamWorker;
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::consumer::Consumer;
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::{error, info};

/// Owns the sink worker runtime and shuts it down without blocking on drop.
///
/// Dropping a `Runtime` directly inside an async context panics ("Cannot drop
/// a runtime in a context where blocking is not allowed") — which is exactly
/// where the connector future is dropped when the server shuts down or a
/// sibling future in `try_join!` fails.
struct WorkerRuntime(Option<Runtime>);

impl WorkerRuntime {
    fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0
            .as_ref()
            .expect("worker runtime is only taken on drop")
            .spawn(future)
    }
}

impl Drop for WorkerRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

pub struct KafkaSinkConnector<P>
where
    P: Processor<Vec<ConsumerRecord>, ()>,
{
    streams: KafkaStreams,
    stream_processor: Arc<StreamWorker<P>>,
    runtime: WorkerRuntime,
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

        Self {
            streams: kafka_streams,
            stream_processor,
            runtime: WorkerRuntime(Some(runtime)),
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

#[cfg(test)]
mod tests {
    use super::WorkerRuntime;
    use crate::connectors::common::build_runtime;

    #[tokio::test]
    async fn worker_runtime_can_be_dropped_inside_async_context() {
        // A bare `Runtime` dropped here would panic ("Cannot drop a runtime in
        // a context where blocking is not allowed"). This is the failure path
        // hit when the connector future is cancelled by `try_join!`.
        let runtime = WorkerRuntime(Some(build_runtime(1, "test-worker").unwrap()));
        drop(runtime);
    }
}
