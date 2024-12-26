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
use crate::connectors::kafka::config::BufferConfig;
use crate::connectors::kafka::{ConsumerRecord, StreamConsumer, TopicPartition};
use crate::event::format;
use crate::event::format::EventFormat;
use crate::handlers::http::ingest::create_stream_if_not_exists;
use crate::metadata::STREAM_INFO;
use crate::storage::StreamType;
use async_trait::async_trait;
use chrono::Utc;
use futures_util::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, warn};

#[derive(Default, Debug, Clone)]
pub struct ParseableSinkProcessor;

impl ParseableSinkProcessor {
    async fn deserialize(
        &self,
        consumer_record: &ConsumerRecord,
    ) -> anyhow::Result<Option<crate::event::Event>> {
        let stream_name = consumer_record.topic.as_str();

        create_stream_if_not_exists(stream_name, &StreamType::UserDefined.to_string()).await?;
        let schema = STREAM_INFO.schema_raw(stream_name)?;

        match &consumer_record.payload {
            None => {
                warn!(
                    "Skipping tombstone or empty payload in partition {} key {}",
                    consumer_record.partition,
                    consumer_record.key_str()
                );
                Ok(None)
            }
            Some(payload) => {
                let data: Value = serde_json::from_slice(payload.as_ref())?;

                let event = format::json::Event {
                    data,
                    tags: String::default(),
                    metadata: String::default(),
                };

                // TODO: Implement a buffer (e.g., a wrapper around [Box<dyn ArrayBuilder>]) to optimize the creation of ParseableEvent by compacting the internal RecordBatch.
                let (record_batch, is_first) = event.into_recordbatch(&schema, None, None)?;

                let p_event = crate::event::Event {
                    rb: record_batch,
                    stream_name: stream_name.to_string(),
                    origin_format: "json",
                    origin_size: payload.len() as u64,
                    is_first_event: is_first,
                    parsed_timestamp: Utc::now().naive_utc(),
                    time_partition: None,
                    custom_partition_values: HashMap::new(),
                    stream_type: StreamType::UserDefined,
                };

                Ok(Some(p_event))
            }
        }
    }
}

#[async_trait]
impl Processor<Vec<ConsumerRecord>, ()> for ParseableSinkProcessor {
    async fn process(&self, records: Vec<ConsumerRecord>) -> anyhow::Result<()> {
        let len = records.len();
        debug!("Processing {} records", len);

        for cr in records {
            if let Some(event) = self.deserialize(&cr).await? {
                event.process().await?;
            }
        }

        debug!("Processed {} records", len);
        Ok(())
    }
}

#[derive(Clone)]
pub struct StreamWorker<P>
where
    P: Processor<Vec<ConsumerRecord>, ()>,
{
    processor: Arc<P>,
    consumer: Arc<StreamConsumer>,
    buffer_config: BufferConfig,
}

impl<P> StreamWorker<P>
where
    P: Processor<Vec<ConsumerRecord>, ()> + Send + Sync + 'static,
{
    pub fn new(processor: Arc<P>, consumer: Arc<StreamConsumer>) -> Self {
        let buffer_config = consumer
            .context()
            .config()
            .consumer()
            .expect("Consumer config is missing")
            .buffer_config();

        Self {
            processor,
            consumer,
            buffer_config,
        }
    }

    pub async fn process_partition(
        &self,
        tp: TopicPartition,
        record_stream: ReceiverStream<ConsumerRecord>,
    ) -> anyhow::Result<()> {
        let chunked_stream = tokio_stream::StreamExt::chunks_timeout(
            record_stream,
            self.buffer_config.buffer_size,
            self.buffer_config.buffer_timeout,
        );

        chunked_stream
            .for_each_concurrent(None, |records| async {
                if let Some(last_record) = records.iter().max_by_key(|r| r.offset) {
                    let tpl = last_record.offset_to_commit().unwrap();

                    if let Err(e) = self.processor.process(records).await {
                        error!("Failed to process records for {:?}: {:?}", tp, e);
                    }

                    //CommitMode::Async race condition.
                    //@see https://github.com/confluentinc/librdkafka/issues/4534
                    //@see https://github.com/confluentinc/librdkafka/issues/4059
                    if let Err(e) = self.consumer.commit(&tpl, CommitMode::Sync) {
                        error!("Failed to commit offsets for {:?}: {:?}", tp, e);
                    }
                }
            })
            .await;

        self.processor.post_stream().await?;

        Ok(())
    }
}
