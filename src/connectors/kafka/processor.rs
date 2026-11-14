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
        error::EventError,
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
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

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
    records: &[ConsumerRecord],
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

/// Matches the ingest-side `Retry-After` for a stream blocked by schema resolution.
const SCHEMA_BLOCKED_RETRY: Duration = Duration::from_secs(60);

/// A chunk whose stream rejects writes until its schema is reconciled with the cluster.
/// Carries the records back, so the chunk can be held (and its offsets left uncommitted)
/// and retried instead of dropped.
#[derive(thiserror::Error)]
#[error("ingestion blocked by schema resolution: {reason}")]
pub struct SchemaBlocked {
    pub records: Vec<ConsumerRecord>,
    reason: String,
}

impl std::fmt::Debug for SchemaBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaBlocked")
            .field("records", &self.records.len())
            .field("reason", &self.reason)
            .finish()
    }
}

#[async_trait]
impl Processor<Vec<ConsumerRecord>, ()> for ParseableSinkProcessor {
    /// Fails with [`SchemaBlocked`] while the stream rejects writes.
    async fn process(&self, records: Vec<ConsumerRecord>) -> anyhow::Result<()> {
        let len = records.len();
        debug!("Processing {len} records");

        let stream_name = records.first().map(|r| r.topic.clone()).unwrap_or_default();
        let tenant_id = records.first().and_then(|r| r.tenant_id.clone());

        let meta = self.prepare_stream(&stream_name, &tenant_id).await?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let res = build_and_process_event(&records, stream_name, tenant_id, meta);
            let _ = tx.send((records, res));
        });
        let (records, res) = rx.await?;
        if let Err(err) = res {
            return Err(match err.downcast::<EventError>() {
                Ok(EventError::SchemaResolutionBlocked(_, reason)) => {
                    SchemaBlocked { records, reason }.into()
                }
                Ok(other) => other.into(),
                Err(other) => other,
            });
        }

        debug!("Processed {len} records");
        Ok(())
    }
}

/// Pauses fetching for one partition while any of its chunks is blocked by schema
/// resolution, and resumes it when the last one goes through.
///
/// All partitions share one consumer loop, which waits whenever a partition's queue is
/// full. Pausing keeps a blocked partition from filling its queue, so the loop keeps
/// serving the other partitions; the blocked chunks hold at most what was already fetched.
/// A resumed partition fetches again from the last record handed to the application, so
/// nothing is skipped or read twice.
struct PartitionPause<'a> {
    consumer: &'a StreamConsumer,
    tp: &'a TopicPartition,
    blocked_chunks: std::sync::Mutex<usize>,
}

impl<'a> PartitionPause<'a> {
    fn new(consumer: &'a StreamConsumer, tp: &'a TopicPartition) -> Self {
        Self {
            consumer,
            tp,
            blocked_chunks: std::sync::Mutex::new(0),
        }
    }

    fn tpl(&self) -> rdkafka::TopicPartitionList {
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition(&self.tp.topic, self.tp.partition);
        tpl
    }

    fn hold(&self) {
        let mut blocked = self.blocked_chunks.lock().expect("pause lock poisoned");
        if *blocked == 0 {
            match self.consumer.pause(&self.tpl()) {
                Ok(()) => warn!("Paused {:?}: its stream rejects writes", self.tp),
                Err(e) => error!(error = %e, "Failed to pause {:?}", self.tp),
            }
        }
        *blocked += 1;
    }

    fn release(&self) {
        let mut blocked = self.blocked_chunks.lock().expect("pause lock poisoned");
        *blocked -= 1;
        if *blocked == 0 {
            self.resume();
        }
    }

    fn resume(&self) {
        match self.consumer.resume(&self.tpl()) {
            Ok(()) => info!("Resumed {:?}", self.tp),
            Err(e) => error!(error = %e, "Failed to resume {:?}", self.tp),
        }
    }
}

impl Drop for PartitionPause<'_> {
    /// A revoked partition drops its worker with chunks still held. Resume it anyway, so a
    /// later assignment of the partition does not start paused with nothing to resume it.
    fn drop(&mut self) {
        let blocked = self.blocked_chunks.get_mut().map_or(0, |b| *b);
        if blocked > 0 {
            self.resume();
        }
    }
}

/// Commits one partition's chunks in order.
///
/// Chunks finish out of order: one can wait out a schema block while later ones go
/// through. Committing each chunk's own offset would then move the committed offset past
/// records not written yet, and a restart or rebalance would skip them. A chunk's offset
/// is committed only once every earlier chunk is done, so the committed offset never
/// passes an unwritten record. Records written after it may be read again after a restart.
#[derive(Default)]
struct OrderedCommits<T = rdkafka::TopicPartitionList> {
    state: std::sync::Mutex<CommitState<T>>,
}

struct CommitState<T> {
    /// Sequence number of the earliest chunk not done yet.
    next: usize,
    /// Done chunks after `next`, with the offsets each would commit.
    done: BTreeMap<usize, Option<T>>,
}

impl<T> Default for CommitState<T> {
    fn default() -> Self {
        Self {
            next: 0,
            done: BTreeMap::new(),
        }
    }
}

impl<T> OrderedCommits<T> {
    /// Marks chunk `seq` (numbered from 0 in stream order) done and calls `commit` with the
    /// offsets of the latest chunk that every earlier chunk is done before. Runs `commit`
    /// under the lock, so commits never go backwards.
    fn complete(&self, seq: usize, offsets: Option<T>, commit: impl FnOnce(T)) {
        let mut state = self.state.lock().expect("commit lock poisoned");
        state.done.insert(seq, offsets);
        let mut latest = None;
        loop {
            let next = state.next;
            let Some(offsets) = state.done.remove(&next) else {
                break;
            };
            state.next += 1;
            latest = offsets.or(latest);
        }
        if let Some(offsets) = latest {
            commit(offsets);
        }
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

    /// Processes one chunk, holding it while its stream is blocked by schema resolution:
    /// committing then would drop its records. Returns once the chunk went through or
    /// failed for another reason.
    async fn process_chunk(
        &self,
        tp: &TopicPartition,
        pause: &PartitionPause<'_>,
        mut records: Vec<ConsumerRecord>,
    ) -> anyhow::Result<()> {
        let mut held = false;
        let result = loop {
            match self.processor.process(records).await {
                Err(err) => match err.downcast::<SchemaBlocked>() {
                    Ok(blocked) => {
                        if !held {
                            pause.hold();
                            held = true;
                        }
                        warn!(
                            "{:?}: {}; retrying {} records in {}s",
                            tp,
                            blocked.reason,
                            blocked.records.len(),
                            SCHEMA_BLOCKED_RETRY.as_secs()
                        );
                        records = blocked.records;
                        tokio::time::sleep(SCHEMA_BLOCKED_RETRY).await;
                    }
                    Err(err) => break Err(err),
                },
                Ok(()) => break Ok(()),
            }
        };
        if held {
            pause.release();
        }
        result
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
        let pause = PartitionPause::new(&self.consumer, &tp);
        let commits = OrderedCommits::default();

        // Unbounded on purpose: blocked chunks must keep draining the partition's queue, or
        // a full queue would stall the consumer loop shared by every partition. The pause
        // bounds what a blocked partition holds.
        chunked_stream
            .enumerate()
            .for_each_concurrent(None, |(seq, records)| {
                let (tp, pause, commits) = (&tp, &pause, &commits);
                async move {
                    let tpl = records
                        .iter()
                        .max_by_key(|r| r.offset)
                        .map(|last| last.offset_to_commit().unwrap());
                    if !records.is_empty()
                        && let Err(e) = self.process_chunk(tp, pause, records).await
                    {
                        error!("Failed to process records for {:?}: {:?}", tp, e);
                    }

                    commits.complete(seq, tpl, |tpl| {
                        //CommitMode::Async race condition.
                        //@see https://github.com/confluentinc/librdkafka/issues/4534
                        //@see https://github.com/confluentinc/librdkafka/issues/4059
                        if let Err(e) = self.consumer.commit(&tpl, CommitMode::Sync) {
                            error!(error = %e, "Failed to commit offsets for {:?}", tpl);
                        } else {
                            debug!("Committed offsets for {:?}", tpl);
                        }
                    });
                }
            })
            .await;

        self.processor.post_stream().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(commits: &OrderedCommits<i64>, seq: usize, offset: Option<i64>) -> Option<i64> {
        let mut committed = None;
        commits.complete(seq, offset, |o| committed = Some(o));
        committed
    }

    #[test]
    fn later_chunk_waits_for_earlier_ones() {
        let commits = OrderedCommits::<i64>::default();
        // Chunk 0 is held (schema block) while 1 and 2 go through: nothing committed.
        assert_eq!(complete(&commits, 1, Some(20)), None);
        assert_eq!(complete(&commits, 2, Some(30)), None);
        // Chunk 0 done: commits through the furthest consecutive chunk.
        assert_eq!(complete(&commits, 0, Some(10)), Some(30));
        assert_eq!(complete(&commits, 3, Some(40)), Some(40));
    }

    #[test]
    fn chunk_without_offsets_still_advances() {
        let commits = OrderedCommits::<i64>::default();
        assert_eq!(complete(&commits, 1, Some(20)), None);
        assert_eq!(complete(&commits, 0, None), Some(20));
        assert_eq!(complete(&commits, 2, None), None);
        assert_eq!(complete(&commits, 3, Some(40)), Some(40));
    }
}
