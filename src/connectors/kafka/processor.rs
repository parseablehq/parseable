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
use crate::event::format::EventFormat;
use crate::event::format::{self, LogSource};
use crate::event::Event as ParseableEvent;
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
use tracing::{debug, error};

#[derive(Default, Debug, Clone)]
pub struct ParseableSinkProcessor;

impl ParseableSinkProcessor {
    async fn build_event_from_chunk(
        &self,
        records: &[ConsumerRecord],
    ) -> anyhow::Result<ParseableEvent> {
        let stream_name = records
            .first()
            .map(|r| r.topic.as_str())
            .unwrap_or_default();

        create_stream_if_not_exists(stream_name, StreamType::UserDefined, LogSource::Json).await?;

        let schema = STREAM_INFO.schema_raw(stream_name)?;
        let time_partition = STREAM_INFO.get_time_partition(stream_name)?;
        let static_schema_flag = STREAM_INFO.get_static_schema_flag(stream_name)?;
        let schema_version = STREAM_INFO.get_schema_version(stream_name)?;

        let (json_vec, total_payload_size) = Self::json_vec(records);
        let batch_json_event = format::json::Event {
            data: Value::Array(json_vec.to_vec()),
        };

        let p_event = batch_json_event.to_event(
            stream_name,
            total_payload_size,
            &schema,
            static_schema_flag,
            Utc::now().naive_utc(),
            time_partition,
            HashMap::new(),
            schema_version,
            StreamType::UserDefined,
        )?;

        Ok(p_event)
    }

    fn json_vec(records: &[ConsumerRecord]) -> (Vec<Value>, u64) {
        let mut json_vec = Vec::with_capacity(records.len());
        let mut total_payload_size = 0u64;

        for record in records.iter().filter_map(|r| r.payload.as_ref()) {
            total_payload_size += record.len() as u64;
            if let Ok(value) = serde_json::from_slice::<Value>(record) {
                json_vec.push(value);
            }
        }

        (json_vec, total_payload_size)
    }
}

#[async_trait]
impl Processor<Vec<ConsumerRecord>, ()> for ParseableSinkProcessor {
    async fn process(&self, records: Vec<ConsumerRecord>) -> anyhow::Result<()> {
        let len = records.len();
        debug!("Processing {} records", len);

        self.build_event_from_chunk(&records).await?.process()?;

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
                        error!(error = %e, "Failed to commit offsets for {:?}", tpl);
                    } else {
                        debug!("Committed offsets for {:?}", tpl);
                    }
                }
            })
            .await;

        self.processor.post_stream().await?;

        Ok(())
    }
}
