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

use crate::{
    connectors::common::processor::Processor,
    event::{
        USER_AGENT_KEY,
        format::{EventFormat, LogSourceEntry, json},
    },
    handlers::TelemetryType,
    metadata::SchemaVersion,
    parseable::PARSEABLE,
    storage::StreamType,
};
use arrow_schema::Field;
use async_trait::async_trait;
use chrono::Utc;
use futures_util::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error};

use super::{ConsumerRecord, StreamConsumer, TopicPartition, config::BufferConfig};

#[derive(Default, Debug, Clone)]
pub struct ParseableSinkProcessor;

impl ParseableSinkProcessor {
    /// Ensures the destination stream exists and fetches the metadata needed
    /// to build an event from a chunk. Stays on the (small, I/O-focused)
    /// kafka tokio runtime since it does async catalog/metadata lookups.
    async fn prepare_stream(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> anyhow::Result<StreamMeta> {
        let log_source_entry = LogSourceEntry::default();
        PARSEABLE
            .create_stream_if_not_exists(
                stream_name,
                StreamType::UserDefined,
                None,
                vec![log_source_entry],
                TelemetryType::default(),
                tenant_id,
                vec![],
                vec![],
            )
            .await?;

        let stream = PARSEABLE.get_stream(stream_name, tenant_id)?;
        Ok(StreamMeta {
            schema: stream.get_schema_raw(),
            time_partition: stream.get_time_partition(),
            custom_partition: stream.get_custom_partition(),
            static_schema_flag: stream.get_static_schema_flag(),
            schema_version: stream.get_schema_version(),
            infer_timestamp: stream.get_infer_timestamp(),
        })
    }
}

struct StreamMeta {
    schema: HashMap<String, Arc<Field>>,
    time_partition: Option<String>,
    custom_partition: Option<String>,
    static_schema_flag: bool,
    schema_version: SchemaVersion,
    infer_timestamp: bool,
}

/// JSON parse + Arrow record-batch encode for a chunk of records. CPU-bound,
/// so it must run off the kafka connector's dedicated tokio runtime (sized to
/// `partition_listener_concurrency`, default 2 — shared by every partition's
/// poll/commit/recv), same reasoning as the rayon offload used for otel and
/// prometheus remote-write ingestion.
fn build_and_process_event(
    records: Vec<ConsumerRecord>,
    stream_name: String,
    tenant_id: Option<String>,
    meta: StreamMeta,
) -> anyhow::Result<()> {
    let mut json_vec = Vec::with_capacity(records.len());
    let mut total_payload_size = 0u64;

    for record in records.iter().filter_map(|r| r.payload.as_ref()) {
        total_payload_size += record.len() as u64;
        if let Ok(value) = serde_json::from_slice::<Value>(record) {
            json_vec.push(value);
        }
    }

    let mut p_custom_fields = HashMap::new();
    p_custom_fields.insert(USER_AGENT_KEY.to_string(), "kafka".to_string());

    json::Event::new(Value::Array(json_vec), Utc::now())
        .into_event(
            stream_name,
            total_payload_size,
            &meta.schema,
            meta.static_schema_flag,
            meta.custom_partition.as_ref(),
            meta.time_partition.as_ref(),
            meta.schema_version,
            StreamType::UserDefined,
            &p_custom_fields,
            TelemetryType::Logs,
            &tenant_id,
            meta.infer_timestamp,
        )?
        .process()?;

    Ok(())
}

#[async_trait]
impl Processor<Vec<ConsumerRecord>, ()> for ParseableSinkProcessor {
    async fn process(&self, records: Vec<ConsumerRecord>) -> anyhow::Result<()> {
        let len = records.len();
        debug!("Processing {len} records");

        let stream_name = records.first().map(|r| r.topic.clone()).unwrap_or_default();
        let tenant_id = records.first().and_then(|r| r.tenant_id.clone());

        let meta = self.prepare_stream(&stream_name, &tenant_id).await?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let res = build_and_process_event(records, stream_name, tenant_id, meta);
            let _ = tx.send(res);
        });
        rx.await??;

        debug!("Processed {len} records");
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
