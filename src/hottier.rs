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

use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock, mpsc};

use crate::{
    catalog::manifest::{File, Manifest},
    handlers::http::cluster::PMETA_STREAM_NAME,
    metrics::{
        HOT_TIER_ACTIVE_MISSING_BYTES, HOT_TIER_ACTIVE_MISSING_FILES, HOT_TIER_DOWNLOAD_BYTES,
        HOT_TIER_DOWNLOAD_OUTCOMES, HOT_TIER_INFLIGHT_FILES, HOT_TIER_INVENTORY_DURATION,
        HOT_TIER_OLDEST_MISSING_LAG_SECONDS, HOT_TIER_RESERVED_BYTES,
        HOT_TIER_SOURCE_WATERMARK_SECONDS, HOT_TIER_TICK_DURATION, HOT_TIER_USED_BYTES,
    },
    parseable::{DEFAULT_TENANT, PARSEABLE},
    storage::{ObjectStorageError, field_stats::DATASET_STATS_STREAM_NAME},
    tenants::TENANT_METADATA,
    utils::human_size::bytes_to_human_size,
    validator::error::HotTierValidationError,
};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use futures::{StreamExt, TryStreamExt};
use futures_util::TryFutureExt;
use object_store::{ObjectStoreExt, local::LocalFileSystem};
use parquet::errors::ParquetError;
use relative_path::RelativePathBuf;
use std::time::Duration;
use sysinfo::Disks;
use tokio::fs::{self, DirEntry};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{Instrument, error, info};

mod local_state;
mod planner;

use local_state::{
    RuntimeState, cleanup_stale_partials, load_or_rebuild, persist_checkpoint, verify_bucket,
};
use planner::{WorkItem, build_work, reconcile_local_file};

pub enum HotTierMessage {
    StartTask(StreamKey),
    KillTask(StreamKey),
    // KillAll,
    StartAll,
}

pub static GLOBAL_HOTTIER: OnceLock<HotTierManager> = OnceLock::new();

/// Floor a timestamp to the start of its minute (seconds + sub-second zeroed).
/// Used to produce a stable per-tick anchor so all spans within one tick share
/// the same cutoff value.
fn floor_to_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.with_second(0)
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(ts)
}

pub const STREAM_HOT_TIER_FILENAME: &str = ".hot_tier.json";
pub const MIN_STREAM_HOT_TIER_SIZE_BYTES: u64 = 10737418240; // 10 GiB
pub const INTERNAL_STREAM_HOT_TIER_SIZE_BYTES: u64 = 10485760; //10 MiB
pub const CURRENT_HOT_TIER_VERSION: &str = "v2";

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize, Default)]
pub struct StreamHotTier {
    pub version: Option<String>,
    #[serde(with = "crate::utils::human_size")]
    pub size: u64,
    #[serde(default, with = "crate::utils::human_size")]
    pub used_size: u64,
    #[serde(default, with = "crate::utils::human_size")]
    pub available_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_date_time_entry: Option<String>,
}

/// Per-stream in-memory bookkeeping. Downloads run outside the lock.
struct StreamSyncState {
    runtime: AsyncMutex<RuntimeState>,
}

struct DownloadContext {
    stream: String,
    tenant_id: Option<String>,
    tenant_label: String,
    state: Arc<StreamSyncState>,
    stream_root: PathBuf,
    quota: u64,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}

#[derive(Debug, Default)]
struct TickStats {
    inventory_files: usize,
    missing_files: usize,
    missing_bytes: u64,
    downloaded: usize,
    failed: usize,
    capacity: usize,
    downloaded_bytes: u64,
    evicted_bytes: u64,
    max_in_flight: usize,
}

enum DownloadKind {
    Downloaded(u64),
    Failed,
    Capacity,
}

struct DownloadOutcome {
    kind: DownloadKind,
    evicted_bytes: u64,
}

impl DownloadOutcome {
    fn downloaded(bytes: u64, evicted_bytes: u64) -> Self {
        Self {
            kind: DownloadKind::Downloaded(bytes),
            evicted_bytes,
        }
    }

    fn failed(evicted_bytes: u64) -> Self {
        Self {
            kind: DownloadKind::Failed,
            evicted_bytes,
        }
    }

    fn capacity(evicted_bytes: u64) -> Self {
        Self {
            kind: DownloadKind::Capacity,
            evicted_bytes,
        }
    }
}

struct InFlightGuard {
    active: Arc<AtomicUsize>,
    stream: String,
    tenant: String,
}

impl InFlightGuard {
    fn new(
        active: Arc<AtomicUsize>,
        maximum: &AtomicUsize,
        stream: String,
        tenant: String,
    ) -> Self {
        let now = active.fetch_add(1, Ordering::Relaxed) + 1;
        maximum.fetch_max(now, Ordering::Relaxed);
        HOT_TIER_INFLIGHT_FILES
            .with_label_values(&[&stream, &tenant])
            .inc();
        Self {
            active,
            stream,
            tenant,
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
        HOT_TIER_INFLIGHT_FILES
            .with_label_values(&[&self.stream, &self.tenant])
            .dec();
    }
}

pub type StreamKey = (Option<String>, String);
pub type HotTierResponse = Result<(), HotTierError>;

struct StreamTasks {
    latest: tokio::task::JoinHandle<()>,
}

pub struct HotTierManager {
    filesystem: LocalFileSystem,
    hot_tier_path: &'static Path,
    state_cache: AsyncRwLock<HashMap<StreamKey, Arc<StreamSyncState>>>,
    tasks: AsyncRwLock<HashMap<StreamKey, StreamTasks>>,
    sender: mpsc::UnboundedSender<HotTierMessage>,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn hottier_runtime(
    mut receiver: mpsc::UnboundedReceiver<HotTierMessage>,
    // sender: mpsc::UnboundedSender<HotTierResponse>,
) {
    while let Some(msg) = receiver.recv().await {
        match msg {
            HotTierMessage::StartTask((tenant_id, stream)) => {
                tokio::spawn(async move {
                    if let Some(htm) = GLOBAL_HOTTIER.get() {
                        htm.spawn_stream_task_inner(stream, tenant_id).await;
                    }
                });
            }
            HotTierMessage::KillTask((tenant_id, stream)) => {
                if let Some(htm) = GLOBAL_HOTTIER.get() {
                    htm.abort_stream_tasks(&stream, &tenant_id).await;
                    let path = if let Some(tenant_id) = tenant_id.as_ref() {
                        htm.hot_tier_path.join(tenant_id).join(&stream)
                    } else {
                        htm.hot_tier_path.join(&stream)
                    };
                    let _ = fs::remove_dir_all(path)
                        .await
                        .map_err(|e| {
                            error!(
                                stream = %stream,
                                tenant = ?tenant_id,
                                error = ?e
                            );
                            e
                        })
                        .map_err(|e| {
                            error!(
                                stream=?stream,
                                tenant_id=?tenant_id,
                                error=?e,
                                "kill task"
                            )
                        });
                    htm.invalidate_state(&stream, &tenant_id).await;
                }
            }
            HotTierMessage::StartAll => {
                let htm = GLOBAL_HOTTIER.get().unwrap();

                let startup_span = tracing::info_span!("hottier.startup.bootstrap");
                let span = startup_span.clone();
                tokio::spawn(
            async move {
                // pstats hot tier may need to be created on boot before any tasks
                // can pick it up.
                if let Err(e) = htm.create_pstats_hot_tier().await {
                    tracing::error!("Skipping pstats hot tier creation because of error: {e}");
                }
                let tenants = if let Some(tenants) = PARSEABLE.list_tenants() {
                    tenants.into_iter().map(Some).collect::<Vec<_>>()
                } else {
                    vec![None]
                };
                for tenant_id in tenants {
                    for stream in PARSEABLE.streams.list(&tenant_id) {
                        if htm.check_stream_hot_tier_exists(&stream, &tenant_id) {
                            let tenant_id = tenant_id.clone();

                            tokio::spawn(
                                async move {
                                    htm.spawn_stream_task_inner(stream, tenant_id).await;
                                }
                                .instrument(span.clone()),
                            );
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        } else {
                            // check for potential orphan directory on disk
                            let path = if let Some(tenant_id) = tenant_id.as_ref() {
                                htm.hot_tier_path.join(tenant_id).join(stream)
                            } else {
                                htm.hot_tier_path.join(stream)
                            };
                            if path.exists() {
                                // delete this entire folder as stream meta says no hottier for stream
                                if let Err(e) = fs::remove_dir_all(&path).await {
                                    tracing::error!(
                                        "Unable to remove orphaned hottier dir- `{path:?}` with error- {e}"
                                    );
                                };
                            }
                        }
                    }
                }
            }
            .instrument(startup_span.clone()),
        );
            }
        }
    }
}

impl HotTierManager {
    pub fn new(
        hot_tier_path: &'static Path,
        sender: mpsc::UnboundedSender<HotTierMessage>,
    ) -> Self {
        std::fs::create_dir_all(hot_tier_path).unwrap();
        HotTierManager {
            filesystem: LocalFileSystem::new(),
            hot_tier_path,
            state_cache: AsyncRwLock::new(HashMap::new()),
            tasks: AsyncRwLock::new(HashMap::new()),
            sender,
        }
    }

    #[tracing::instrument(name = "hottier.startup", skip(self))]
    pub async fn start_all_tasks(&'static self) {
        let _ = tokio::spawn(async move {
            self.sender.send(HotTierMessage::StartAll).unwrap();
        })
        .instrument(tracing::Span::current())
        .await;
    }

    /// Lazy-load and cache reconstructible local state for a (tenant, stream) pair.
    async fn get_or_load_state(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<Arc<StreamSyncState>, HotTierError> {
        let key: StreamKey = (tenant_id.clone(), stream.to_owned());
        {
            if let Some(state) = self.state_cache.read().await.get(&key).cloned() {
                return Ok(state);
            }
        }
        // Ensure the stream configuration exists before creating runtime state.
        self.read_hot_tier_config(stream, tenant_id).await?;
        let stream_root = self.stream_root(stream, tenant_id);
        let runtime = load_or_rebuild(&stream_root).await?;
        let loaded = Arc::new(StreamSyncState {
            runtime: AsyncMutex::new(runtime),
        });
        let (state, inserted) = {
            let mut cache = self.state_cache.write().await;
            match cache.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => (entry.get().clone(), false),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(loaded.clone());
                    (loaded, true)
                }
            }
        };
        if inserted {
            tokio::spawn(async move {
                match cleanup_stale_partials(stream_root).await {
                    Ok(removed) if removed > 0 => info!(removed, "removed stale hot-tier partials"),
                    Ok(_) => {}
                    Err(error) => error!(%error, "failed to clean stale hot-tier partials"),
                }
            });
        }
        Ok(state)
    }

    fn stream_root(&self, stream: &str, tenant_id: &Option<String>) -> PathBuf {
        if let Some(tenant_id) = tenant_id {
            self.hot_tier_path.join(tenant_id).join(stream)
        } else {
            self.hot_tier_path.join(stream)
        }
    }

    /// Drop cached state for a stream (used after delete).
    pub async fn invalidate_state(&self, stream: &str, tenant_id: &Option<String>) {
        let key: StreamKey = (tenant_id.clone(), stream.to_owned());
        {
            self.state_cache.write().await.remove(&key);
        }
    }

    /// get the total hot tier size for all streams
    #[tracing::instrument(
        name = "hottier.get_hot_tiers_size",
        skip(self),
        fields(current_stream = %current_stream, current_tenant = ?current_tenant_id),
        err
    )]
    pub async fn get_hot_tiers_size(
        &self,
        current_stream: &str,
        current_tenant_id: &Option<String>,
    ) -> Result<(u64, u64), HotTierError> {
        let mut total_hot_tier_size = 0;
        let mut total_hot_tier_used_size = 0;
        let tenants = if let Some(tenants) = PARSEABLE.list_tenants() {
            tenants.into_iter().map(Some).collect()
        } else {
            vec![None]
        };
        for tenant_id in tenants {
            for stream in PARSEABLE.streams.list(&tenant_id) {
                if self.check_stream_hot_tier_exists(&stream, &tenant_id)
                    && !(stream == current_stream && tenant_id == *current_tenant_id)
                {
                    let stream_hot_tier = self.get_hot_tier(&stream, &tenant_id).await?;
                    total_hot_tier_size += &stream_hot_tier.size;
                    total_hot_tier_used_size += stream_hot_tier.used_size;
                }
            }
        }

        Ok((total_hot_tier_size, total_hot_tier_used_size))
    }

    /// validate if hot tier size can be fit in the disk
    /// check disk usage and hot tier size of all other streams
    /// check if total hot tier size of all streams is less than max disk usage
    /// delete all the files from hot tier once validation is successful and hot tier is ready to be updated
    #[tracing::instrument(
        name = "hottier.validate_size",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id, size = stream_hot_tier_size),
        err
    )]
    pub async fn validate_hot_tier_size(
        &self,
        stream: &str,
        stream_hot_tier_size: u64,
        tenant_id: &Option<String>,
    ) -> Result<u64, HotTierError> {
        let mut existing_hot_tier_used_size = 0;
        if self.check_stream_hot_tier_exists(stream, tenant_id) {
            //delete existing hot tier if its size is less than the updated hot tier size else return error
            let existing_hot_tier = self.get_hot_tier(stream, tenant_id).await?;
            existing_hot_tier_used_size = existing_hot_tier.used_size;

            if stream_hot_tier_size < existing_hot_tier_used_size {
                return Err(HotTierError::ObjectStorageError(
                    ObjectStorageError::Custom(format!(
                        "Reducing hot tier size is not supported, failed to reduce the hot tier size from {} to {}",
                        bytes_to_human_size(existing_hot_tier_used_size),
                        bytes_to_human_size(stream_hot_tier_size)
                    )),
                ));
            }
        }

        let DiskUtil {
            total_space,
            used_space,
            ..
        } = self
            .get_disk_usage()
            .expect("Codepath should only be hit if hottier is enabled");

        let (total_hot_tier_size, total_hot_tier_used_size) =
            self.get_hot_tiers_size(stream, tenant_id).await?;
        let disk_threshold = (PARSEABLE.options.max_disk_usage * total_space as f64) / 100.0;
        let max_allowed_hot_tier_size = disk_threshold
            - total_hot_tier_size as f64
            - (used_space as f64
                - total_hot_tier_used_size as f64
                - existing_hot_tier_used_size as f64);

        if stream_hot_tier_size as f64 > max_allowed_hot_tier_size {
            error!(
                "disk_threshold: {}, used_disk_space: {}, total_hot_tier_used_size: {}, existing_hot_tier_used_size: {}, total_hot_tier_size: {}",
                bytes_to_human_size(disk_threshold as u64),
                bytes_to_human_size(used_space),
                bytes_to_human_size(total_hot_tier_used_size),
                bytes_to_human_size(existing_hot_tier_used_size),
                bytes_to_human_size(total_hot_tier_size)
            );

            return Err(HotTierError::ObjectStorageError(
                ObjectStorageError::Custom(format!(
                    "{} is the total usable disk space for hot tier, cannot set a bigger value.",
                    bytes_to_human_size(max_allowed_hot_tier_size as u64)
                )),
            ));
        }

        Ok(existing_hot_tier_used_size)
    }

    /// get the hot tier metadata file for the stream
    #[tracing::instrument(
        name = "hottier.get_hot_tier",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id),
        err
    )]
    pub async fn get_hot_tier(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<StreamHotTier, HotTierError> {
        let mut stream_hot_tier = self.read_hot_tier_config(stream, tenant_id).await?;
        let state = self.get_or_load_state(stream, tenant_id).await?;
        let runtime = state.runtime.lock().await;
        stream_hot_tier.used_size = runtime.used_bytes();
        stream_hot_tier.available_size = stream_hot_tier.size.saturating_sub(
            runtime
                .used_bytes()
                .saturating_add(runtime.reserved_bytes()),
        );
        drop(runtime);
        stream_hot_tier.oldest_date_time_entry =
            self.get_oldest_date_time_entry(stream, tenant_id).await?;

        Ok(stream_hot_tier)
    }

    async fn read_hot_tier_config(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<StreamHotTier, HotTierError> {
        if !self.check_stream_hot_tier_exists(stream, tenant_id) {
            return Err(HotTierValidationError::NotFound(stream.to_owned()).into());
        }
        let path = self.hot_tier_file_path(stream, tenant_id)?;
        let bytes = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await?;

        Ok(serde_json::from_slice(&bytes)?)
    }

    #[tracing::instrument(
        name = "hottier.delete_hot_tier",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id),
        err
    )]
    pub async fn delete_hot_tier(
        &'static self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), HotTierError> {
        if !self.check_stream_hot_tier_exists(stream, tenant_id) {
            return Err(HotTierValidationError::NotFound(stream.to_owned()).into());
        }
        let stream_name = stream.to_owned();
        let tenant = tenant_id.to_owned();
        let _ = tokio::spawn(async move {
            self.sender
                .send(HotTierMessage::KillTask((tenant, stream_name)))
                .unwrap();
        })
        .instrument(tracing::Span::current())
        .await;

        Ok(())
    }

    /// put the hot tier metadata file for the stream
    /// set the updated_date_range in the hot tier metadata file
    #[tracing::instrument(
        name = "hottier.put_hot_tier",
        skip(self, hot_tier),
        fields(stream = %stream, tenant = ?tenant_id, size = hot_tier.size),
        err
    )]
    pub async fn put_hot_tier(
        &self,
        stream: &str,
        hot_tier: &mut StreamHotTier,
        tenant_id: &Option<String>,
    ) -> Result<(), HotTierError> {
        let path = self.hot_tier_file_path(stream, tenant_id)?;
        let bytes = serde_json::to_vec(&hot_tier)?.into();
        self.filesystem.put(&path, bytes).await?;
        Ok(())
    }

    /// get the hot tier file path for the stream
    pub fn hot_tier_file_path(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<object_store::path::Path, HotTierError> {
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            self.hot_tier_path
                .join(tenant_id)
                .join(stream)
                .join(STREAM_HOT_TIER_FILENAME)
        } else {
            self.hot_tier_path
                .join(stream)
                .join(STREAM_HOT_TIER_FILENAME)
        };
        let path = object_store::path::Path::from_absolute_path(path)?;

        Ok(path)
    }

    #[tracing::instrument(name = "hottier.abort", skip(self))]
    pub async fn abort_all(&self) {
        {
            let guard = self.tasks.write().await;
            for (streamkey, task) in guard.iter() {
                task.latest.abort();
                info!("aborted hot tier tasks for- {streamkey:?}");
            }
        }
    }

    #[tracing::instrument(
        name = "hottier.spawn_stream_task",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id)
    )]
    pub async fn spawn_stream_task(&'static self, stream: String, tenant_id: Option<String>) {
        let _ = tokio::spawn(async move {
            self.sender
                .send(HotTierMessage::StartTask((tenant_id, stream)))
                .unwrap();
        })
        .instrument(tracing::Span::current())
        .await;
    }

    /// Spawn Latest loop for a single stream. Idempotent:
    /// if tasks already exist for this (tenant, stream), no-op.
    async fn spawn_stream_task_inner(&'static self, stream: String, tenant_id: Option<String>) {
        let key: StreamKey = (tenant_id.clone(), stream.clone());

        let mut tasks = self.tasks.write().await;
        if let Some(existing) = tasks.get(&key)
            && !existing.latest.is_finished()
        {
            return;
        }

        let latest_interval = Duration::from_secs(30);

        info!(stream = %stream, tenant = ?tenant_id, "spawning per-stream hot tier tasks");

        let s = stream.clone();
        let t = tenant_id.clone();
        let latest = tokio::spawn(async move {
            let mut interval = tokio::time::interval(latest_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let anchor = floor_to_minute(Utc::now());
                let tick_span = tracing::info_span!(
                    "hottier.tick",
                    stream = %s,
                    tenant = ?t,
                    anchor = %anchor
                );
                async {
                    if let Err(err) = self.process_stream(s.clone(), t.clone(), anchor).await {
                        error!("latest sync error: {err:?}");
                    }
                }
                .instrument(tick_span)
                .await;
            }
        });

        if let Some(old) = tasks.insert(key, StreamTasks { latest }) {
            old.latest.abort();
        }
    }

    /// Abort and remove per-stream tasks. Caller must ensure no further work
    /// will be enqueued for the stream after this returns.
    async fn abort_stream_tasks(&self, stream: &str, tenant_id: &Option<String>) {
        let key: StreamKey = (tenant_id.clone(), stream.to_owned());
        {
            if let Some(t) = self.tasks.write().await.remove(&key) {
                t.latest.abort();
                info!(stream = %stream, tenant = ?tenant_id, "aborted per-stream hot tier tasks");
            }
        }
    }

    /// process the hot tier files for the stream
    /// delete the files from the hot tier directory if the available date range is outside the hot tier range
    #[tracing::instrument(
        name = "hottier.process_stream",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id, anchor = %anchor),
        err
    )]
    async fn process_stream(
        &self,
        stream: String,
        tenant_id: Option<String>,
        anchor: DateTime<Utc>,
    ) -> Result<(), HotTierError> {
        let stream_start = std::time::Instant::now();
        self.process_manifest(&stream, &tenant_id, anchor)
            .await
            .map_err(|e| {
                error!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    error = ?e
                );
                e
            })?;

        info!(
            stream = %stream,
            tenant = ?tenant_id,
            elapsed_seconds = stream_start.elapsed().as_secs_f64(),
            delayed = stream_start.elapsed().as_secs() > 30,
            "stream sync done"
        );
        Ok(())
    }

    /// Inventory the active source window, reconcile its local buckets, and
    /// run bounded unordered downloads. Individual file failures are outcomes,
    /// not pass failures.
    #[tracing::instrument(
        name = "hottier.process_manifest",
        skip(self),
        fields(
            stream = %stream,
            tenant = ?tenant_id,
            anchor = %anchor,
            candidate_dates = tracing::field::Empty,
            work_count = tracing::field::Empty,
            total_bytes = tracing::field::Empty,
        ),
        err
    )]
    async fn process_manifest(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
        anchor: DateTime<Utc>,
    ) -> Result<(), HotTierError> {
        let started = std::time::Instant::now();
        let state = self.get_or_load_state(stream, tenant_id).await?;
        let latest_minutes = PARSEABLE.options.hot_tier_latest_minutes;
        let previous_watermark = state.runtime.lock().await.source_watermark;
        let candidate_dates = Self::inventory_dates(anchor, previous_watermark);
        let inventory_started = std::time::Instant::now();

        let s3_manifests = self
            .fetch_manifests(stream, tenant_id, &candidate_dates)
            .await?;
        let plan = build_work(
            &s3_manifests,
            latest_minutes,
            self.hot_tier_path,
            reconcile_local_file,
        );
        let stream_root = self.stream_root(stream, tenant_id);
        let total_bytes: u64 = plan.items.iter().map(|item| item.file.file_size).sum();
        let oldest_missing_lag_seconds = plan
            .items
            .first()
            .map(|item| (anchor - item.timestamp).num_seconds().max(0))
            .unwrap_or_default();
        let tenant_label = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        HOT_TIER_INVENTORY_DURATION
            .with_label_values(&[stream, tenant_label])
            .observe(inventory_started.elapsed().as_secs_f64());
        HOT_TIER_ACTIVE_MISSING_FILES
            .with_label_values(&[stream, tenant_label])
            .set(plan.items.len().min(i64::MAX as usize) as i64);
        HOT_TIER_ACTIVE_MISSING_BYTES
            .with_label_values(&[stream, tenant_label])
            .set(total_bytes.min(i64::MAX as u64) as i64);
        HOT_TIER_OLDEST_MISSING_LAG_SECONDS
            .with_label_values(&[stream, tenant_label])
            .set(oldest_missing_lag_seconds);
        tracing::Span::current()
            .record("work_count", plan.items.len())
            .record("total_bytes", total_bytes);

        // The checkpoint is authoritative for old buckets, but the active
        // source window is remeasured on every pass so wrong-sized/missing
        // files cannot inflate runtime usage.
        {
            let mut runtime = state.runtime.lock().await;
            runtime.source_watermark = plan.source_watermark.or(previous_watermark);
            for minute_path in &plan.active_minute_paths {
                let Ok(minute) = minute_path.strip_prefix(&stream_root) else {
                    continue;
                };
                let key = minute.to_string_lossy().into_owned();
                runtime
                    .minutes
                    .insert(key.clone(), verify_bucket(&stream_root, &key).await?);
            }
        }

        let quota = self.read_hot_tier_config(stream, tenant_id).await?.size;
        let mut stats = TickStats {
            inventory_files: plan.inventory_files,
            missing_files: plan.items.len(),
            missing_bytes: total_bytes,
            ..TickStats::default()
        };
        let download_stats = self
            .download_work(stream, tenant_id, &state, &stream_root, quota, plan.items)
            .await;
        stats.downloaded = download_stats.downloaded;
        stats.failed = download_stats.failed;
        stats.capacity = download_stats.capacity;
        stats.downloaded_bytes = download_stats.downloaded_bytes;
        stats.evicted_bytes = download_stats.evicted_bytes;
        stats.max_in_flight = download_stats.max_in_flight;

        let (used, reserved, watermark) = {
            let runtime = state.runtime.lock().await;
            persist_checkpoint(&stream_root, &runtime).await?;
            (
                runtime.used_bytes(),
                runtime.reserved_bytes(),
                runtime.source_watermark,
            )
        };
        let mut metadata = self.read_hot_tier_config(stream, tenant_id).await?;
        metadata.used_size = used;
        metadata.available_size = quota.saturating_sub(used.saturating_add(reserved));
        self.put_hot_tier(stream, &mut metadata, tenant_id).await?;

        if let Some(watermark) = watermark {
            HOT_TIER_SOURCE_WATERMARK_SECONDS
                .with_label_values(&[stream, tenant_label])
                .set(watermark.timestamp());
        }
        HOT_TIER_USED_BYTES
            .with_label_values(&[stream, tenant_label])
            .set(used.min(i64::MAX as u64) as i64);
        HOT_TIER_RESERVED_BYTES
            .with_label_values(&[stream, tenant_label])
            .set(reserved.min(i64::MAX as u64) as i64);
        HOT_TIER_DOWNLOAD_BYTES
            .with_label_values(&[stream, tenant_label])
            .inc_by(stats.downloaded_bytes);
        for (outcome, count) in [
            ("downloaded", stats.downloaded),
            ("failed", stats.failed),
            ("capacity", stats.capacity),
        ] {
            HOT_TIER_DOWNLOAD_OUTCOMES
                .with_label_values(&[stream, tenant_label, outcome])
                .inc_by(count as u64);
        }
        HOT_TIER_TICK_DURATION
            .with_label_values(&[stream, tenant_label])
            .observe(started.elapsed().as_secs_f64());
        info!(
            stream = %stream,
            tenant = ?tenant_id,
            source_watermark = ?watermark,
            inventory_files = stats.inventory_files,
            missing_files = stats.missing_files,
            missing_bytes = stats.missing_bytes,
            downloaded = stats.downloaded,
            failed = stats.failed,
            capacity = stats.capacity,
            downloaded_bytes = stats.downloaded_bytes,
            evicted_bytes = stats.evicted_bytes,
            max_in_flight = stats.max_in_flight,
            configured_concurrency = PARSEABLE.options.hot_tier_files_per_stream_concurrency,
            used_bytes = used,
            reserved_bytes = reserved,
            oldest_missing_lag_seconds,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "hot-tier tick summary"
        );

        Ok(())
    }

    fn inventory_dates(
        anchor: DateTime<Utc>,
        previous_watermark: Option<DateTime<Utc>>,
    ) -> Vec<String> {
        let mut dates = std::collections::BTreeSet::from([
            anchor
                .date_naive()
                .pred_opt()
                .unwrap_or(anchor.date_naive()),
            anchor.date_naive(),
        ]);
        if let Some(watermark) = previous_watermark {
            dates.insert(watermark.date_naive());
        }
        dates
            .into_iter()
            .map(|date| format!("date={date}"))
            .collect()
    }

    async fn fetch_manifests(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
        candidate_dates: &[String],
    ) -> Result<BTreeMap<String, Vec<Manifest>>, HotTierError> {
        PARSEABLE
            .metastore
            .get_manifest_files_for_dates(stream, tenant_id, candidate_dates)
            .await
            .map_err(|e| {
                error!(
                    stream = %stream, tenant = ?tenant_id,
                    error = ?e, "manifest fetch failed"
                );
                HotTierError::ObjectStorageError(ObjectStorageError::MetastoreError(Box::new(
                    e.to_detail(),
                )))
            })
    }

    async fn download_work(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
        state: &Arc<StreamSyncState>,
        stream_root: &Path,
        quota: u64,
        work: Vec<WorkItem>,
    ) -> TickStats {
        let concurrency = PARSEABLE.options.hot_tier_files_per_stream_concurrency as usize;
        let stream_owned = stream.to_owned();
        let tenant_owned = tenant_id.clone();
        let root_owned = stream_root.to_path_buf();
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let results = futures::stream::iter(work)
            .map(|item| {
                let context = DownloadContext {
                    stream: stream_owned.clone(),
                    tenant_id: tenant_owned.clone(),
                    tenant_label: tenant_owned.as_deref().unwrap_or(DEFAULT_TENANT).to_owned(),
                    state: state.clone(),
                    stream_root: root_owned.clone(),
                    quota,
                    active: active.clone(),
                    maximum: maximum.clone(),
                };
                async move { self.process_work_item(context, item).await }
            })
            .buffer_unordered(concurrency.max(1))
            .collect::<Vec<_>>()
            .await;
        let mut stats = TickStats {
            max_in_flight: maximum.load(Ordering::Relaxed),
            ..TickStats::default()
        };
        for outcome in results {
            stats.evicted_bytes = stats.evicted_bytes.saturating_add(outcome.evicted_bytes);
            match outcome.kind {
                DownloadKind::Downloaded(bytes) => {
                    stats.downloaded += 1;
                    stats.downloaded_bytes = stats.downloaded_bytes.saturating_add(bytes);
                }
                DownloadKind::Failed => stats.failed += 1,
                DownloadKind::Capacity => stats.capacity += 1,
            }
        }
        stats
    }

    async fn process_work_item(&self, context: DownloadContext, item: WorkItem) -> DownloadOutcome {
        let minute = match item.minute_path.strip_prefix(&context.stream_root) {
            Ok(value) => value.to_string_lossy().into_owned(),
            Err(error) => {
                error!(file = %item.file.file_path, %error, "invalid hot-tier minute path");
                return DownloadOutcome::failed(0);
            }
        };
        let reservation = self
            .reserve_item(
                &context.state,
                &context.stream_root,
                &minute,
                &item,
                context.quota,
            )
            .await;
        let evicted_bytes = match reservation {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return DownloadOutcome::capacity(0),
            Err(error) => {
                error!(stream = %context.stream, tenant = ?context.tenant_id, file = %item.file.file_path, %error, "reservation failed");
                return DownloadOutcome::failed(0);
            }
        };
        let _inflight = InFlightGuard::new(
            context.active.clone(),
            &context.maximum,
            context.stream.clone(),
            context.tenant_label.clone(),
        );

        if let Some(parent) = item.local_path.parent()
            && let Err(error) = fs::create_dir_all(parent).await
        {
            self.finish_reservation(&context.state, &item, &minute, false)
                .await;
            error!(file = %item.file.file_path, %error, "failed to create hot-tier directory");
            return DownloadOutcome::failed(evicted_bytes);
        }
        let result = PARSEABLE
            .hottier_connection_pool
            .parallel_chunked_download(
                &RelativePathBuf::from(item.file.file_path.clone()),
                &context.tenant_id,
                item.local_path.clone(),
            )
            .await;
        let valid = result.is_ok()
            && fs::metadata(&item.local_path)
                .await
                .is_ok_and(|metadata| metadata.len() == item.file.file_size);
        if !valid {
            let _ = fs::remove_file(&item.local_path).await;
        }
        self.finish_reservation(&context.state, &item, &minute, valid)
            .await;

        match result {
            Ok(()) if valid => DownloadOutcome::downloaded(item.file.file_size, evicted_bytes),
            Ok(()) => {
                error!(file = %item.file.file_path, expected_size = item.file.file_size, "hot-tier download size mismatch");
                DownloadOutcome::failed(evicted_bytes)
            }
            Err(error) => {
                error!(stream = %context.stream, tenant = ?context.tenant_id, file = %item.file.file_path, %error, "hot-tier download failed");
                DownloadOutcome::failed(evicted_bytes)
            }
        }
    }

    async fn reserve_item(
        &self,
        state: &Arc<StreamSyncState>,
        stream_root: &Path,
        minute: &str,
        item: &WorkItem,
        quota: u64,
    ) -> Result<Option<u64>, HotTierError> {
        if item.file.file_size > quota {
            return Ok(None);
        }
        let mut runtime = state.runtime.lock().await;
        runtime.mark_bucket_inflight(minute);
        let mut evicted = 0_u64;
        loop {
            if self.is_disk_available(item.file.file_size).await
                && runtime.try_reserve(&item.file.file_path, item.file.file_size, quota)
            {
                return Ok(Some(evicted));
            }
            let Some(oldest) = runtime.oldest_evictable_bucket().map(str::to_owned) else {
                runtime.unmark_bucket_inflight(minute);
                return Ok(None);
            };
            let totals = verify_bucket(stream_root, &oldest).await?;
            runtime.minutes.insert(oldest.clone(), totals);
            let path = stream_root.join(&oldest);
            match fs::remove_dir_all(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    runtime.unmark_bucket_inflight(minute);
                    return Err(error.into());
                }
            }
            evicted = evicted.saturating_add(runtime.remove_bucket(&oldest));
        }
    }

    async fn finish_reservation(
        &self,
        state: &Arc<StreamSyncState>,
        item: &WorkItem,
        minute: &str,
        commit: bool,
    ) {
        let mut runtime = state.runtime.lock().await;
        if commit {
            runtime.commit(&item.file.file_path, minute.to_owned());
        } else {
            runtime.refund(&item.file.file_path);
        }
        runtime.unmark_bucket_inflight(minute);
    }

    /// fetch the list of dates available in the hot tier directory for the stream and sort them
    #[tracing::instrument(
        name = "hottier.fetch_dates",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id),
        err
    )]
    pub async fn fetch_hot_tier_dates(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<NaiveDate>, HotTierError> {
        let mut date_list = Vec::new();
        let path = if let Some(tenant) = tenant_id.as_ref() {
            self.hot_tier_path.join(tenant).join(stream)
        } else {
            self.hot_tier_path.join(stream)
        };
        // let path = self.hot_tier_path.join(stream);
        if !path.exists() {
            return Ok(date_list);
        }

        let directories = fs::read_dir(&path).await?;
        let mut dates = ReadDirStream::new(directories);
        while let Some(date) = dates.next().await {
            let date = date?;
            if !date.path().is_dir() {
                continue;
            }
            let name = date.file_name();
            let name = name.to_string_lossy();
            if let Some(value) = name.strip_prefix("date=")
                && let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d")
            {
                date_list.push(date);
            }
        }
        date_list.sort();

        Ok(date_list)
    }

    /// get hot tier path for the stream and date
    pub fn get_stream_path_for_date(
        &self,
        stream: &str,
        date: &NaiveDate,
        tenant_id: &Option<String>,
    ) -> PathBuf {
        if let Some(tenant) = tenant_id.as_ref() {
            self.hot_tier_path
                .join(tenant)
                .join(stream)
                .join(format!("date={date}"))
        } else {
            self.hot_tier_path.join(stream).join(format!("date={date}"))
        }
    }

    /// Returns the list of manifest files present in hot tier directory for the stream
    pub async fn get_hot_tier_manifest_files(
        &self,
        manifest_files: &mut Vec<File>,
    ) -> Result<Vec<File>, HotTierError> {
        // Check which query-relevant files exist locally in the hot tier directory.
        let mut hot_tier_files = Vec::new();
        let mut remaining = Vec::with_capacity(manifest_files.len());

        for file in manifest_files.drain(..) {
            let hot_tier_path = self.hot_tier_path.join(&file.file_path);
            match fs::metadata(&hot_tier_path).await {
                Ok(meta) => {
                    if meta.len() == file.file_size {
                        hot_tier_files.push(file);
                        continue;
                    } else {
                        tracing::error!(
                            "hot tier file metadata check failed for {hot_tier_path:?} - meta.len() != file.file_size"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "hot tier file metadata check failed for {hot_tier_path:?} - {e}"
                    );
                }
            }

            remaining.push(file);
        }

        *manifest_files = remaining;

        // Sort both lists in descending order by file path.
        hot_tier_files.sort_unstable_by(|a, b| b.file_path.cmp(&a.file_path));
        manifest_files.sort_unstable_by(|a, b| b.file_path.cmp(&a.file_path));

        Ok(hot_tier_files)
    }

    ///check if the hot tier metadata file exists for the stream
    pub fn check_stream_hot_tier_exists(&self, stream: &str, tenant_id: &Option<String>) -> bool {
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            self.hot_tier_path
                .join(tenant_id)
                .join(stream)
                .join(STREAM_HOT_TIER_FILENAME)
        } else {
            self.hot_tier_path
                .join(stream)
                .join(STREAM_HOT_TIER_FILENAME)
        };
        path.exists()
    }

    /// check if the disk is available to download the parquet file
    /// check if the disk usage is above the threshold
    pub async fn is_disk_available(&self, size_to_download: u64) -> bool {
        if let Some(DiskUtil {
            total_space,
            available_space,
            used_space,
        }) = self.get_disk_usage()
        {
            if available_space < size_to_download {
                return false;
            }

            if ((used_space + size_to_download) as f64 * 100.0 / total_space as f64)
                > PARSEABLE.options.max_disk_usage
            {
                return false;
            }
        }

        true
    }

    pub async fn get_oldest_date_time_entry(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<Option<String>, HotTierError> {
        let date_list = self.fetch_hot_tier_dates(stream, tenant_id).await?;
        if date_list.is_empty() {
            return Ok(None);
        }

        for date in date_list {
            let path = self.get_stream_path_for_date(stream, &date, tenant_id);
            let hours_dir = ReadDirStream::new(fs::read_dir(&path).await?);
            let mut hours: Vec<DirEntry> = hours_dir.try_collect().await?;
            hours.retain(|entry| {
                entry.path().is_dir() && entry.file_name().to_string_lossy().starts_with("hour=")
            });
            hours.sort_by_key(|entry| entry.file_name().to_string_lossy().to_string());

            for hour in hours {
                let hour_str = hour
                    .file_name()
                    .to_string_lossy()
                    .trim_start_matches("hour=")
                    .to_string();

                let minutes_dir = ReadDirStream::new(fs::read_dir(hour.path()).await?);
                let mut minutes: Vec<DirEntry> = minutes_dir.try_collect().await?;
                minutes.retain(|entry| {
                    entry.path().is_dir()
                        && entry.file_name().to_string_lossy().starts_with("minute=")
                });
                minutes.sort_by_key(|entry| entry.file_name().to_string_lossy().to_string());

                if let Some(minute) = minutes.first() {
                    let minute_str = minute
                        .file_name()
                        .to_string_lossy()
                        .trim_start_matches("minute=")
                        .to_string();
                    let oldest_date_time = format!("{date}T{hour_str}:{minute_str}:00.000Z");
                    return Ok(Some(oldest_date_time));
                }
            }
        }

        Ok(None)
    }

    #[tracing::instrument(name = "hottier.put_internal_stream", skip(self), err)]
    pub async fn put_internal_stream_hot_tier(&self) -> Result<(), HotTierError> {
        let tenants = if let Some(tenants) = PARSEABLE.list_tenants() {
            tenants.into_iter().map(Some).collect()
        } else {
            vec![None]
        };

        for tenant_id in tenants {
            // Skip suspended tenants — their hot tier directories are cleaned up on suspension
            if let Some(tid) = tenant_id.as_ref()
                && TENANT_METADATA.is_workspace_suspended(tid)
            {
                continue;
            }

            if !self.check_stream_hot_tier_exists(PMETA_STREAM_NAME, &tenant_id) {
                let mut stream_hot_tier = StreamHotTier {
                    version: Some(CURRENT_HOT_TIER_VERSION.to_string()),
                    size: INTERNAL_STREAM_HOT_TIER_SIZE_BYTES,
                    used_size: 0,
                    available_size: INTERNAL_STREAM_HOT_TIER_SIZE_BYTES,
                    oldest_date_time_entry: None,
                };
                self.put_hot_tier(PMETA_STREAM_NAME, &mut stream_hot_tier, &tenant_id)
                    .await?;
            }
        }
        Ok(())
    }

    /// Creates hot tier for pstats internal stream if the stream exists in storage
    #[tracing::instrument(name = "hottier.create_pstats", skip(self), err)]
    async fn create_pstats_hot_tier(&self) -> Result<(), HotTierError> {
        let tenants = if let Some(tenants) = PARSEABLE.list_tenants() {
            tenants.into_iter().map(Some).collect()
        } else {
            vec![None]
        };
        for tenant_id in tenants {
            // Skip suspended tenants — their hot tier directories are cleaned up on suspension
            if let Some(tid) = tenant_id.as_ref()
                && TENANT_METADATA.is_workspace_suspended(tid)
            {
                continue;
            }

            // Check if pstats hot tier already exists
            if !self.check_stream_hot_tier_exists(DATASET_STATS_STREAM_NAME, &tenant_id) {
                // Check if pstats stream exists in storage by attempting to load it
                if PARSEABLE
                    .check_or_load_stream(DATASET_STATS_STREAM_NAME, &tenant_id)
                    .await
                {
                    let mut stream_hot_tier = StreamHotTier {
                        version: Some(CURRENT_HOT_TIER_VERSION.to_string()),
                        size: MIN_STREAM_HOT_TIER_SIZE_BYTES,
                        used_size: 0,
                        available_size: MIN_STREAM_HOT_TIER_SIZE_BYTES,
                        oldest_date_time_entry: None,
                    };
                    self.put_hot_tier(DATASET_STATS_STREAM_NAME, &mut stream_hot_tier, &tenant_id)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Get the disk usage for the hot tier storage path. If we have a three disk paritions
    /// mounted as follows:
    /// 1. /
    /// 2. /home/parseable
    /// 3. /home/example/ignore
    ///
    /// And parseable is running with `P_HOT_TIER_DIR` pointing to a directory in
    /// `/home/parseable`, we should return the usage stats of the disk mounted there.
    fn get_disk_usage(&self) -> Option<DiskUtil> {
        let mut disks = Disks::new_with_refreshed_list();
        // Order the disk partitions by decreasing length of mount path
        disks.sort_by_key(|disk| disk.mount_point().to_str().unwrap().len());
        disks.reverse();

        for disk in disks.iter() {
            // Returns disk utilisation of first matching mount point
            if self.hot_tier_path.starts_with(disk.mount_point()) {
                return Some(DiskUtil {
                    total_space: disk.total_space(),
                    available_space: disk.available_space(),
                    used_space: disk.total_space() - disk.available_space(),
                });
            }
        }

        None
    }
}

struct DiskUtil {
    total_space: u64,
    available_space: u64,
    used_space: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum HotTierError {
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    #[error("{0}")]
    IOError(#[from] io::Error),
    #[error("{0}")]
    MoveError(#[from] fs_extra::error::Error),
    #[error("{0}")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("{0}")]
    ObjectStorePathError(#[from] object_store::path::Error),
    #[error("{0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("{0}")]
    ParquetError(#[from] ParquetError),
    #[error("{0}")]
    HotTierValidationError(#[from] HotTierValidationError),
    #[error("{0}")]
    Anyhow(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::HotTierManager;

    #[test]
    fn inventory_dates_cover_watermark_and_midnight_rollover() {
        let anchor = Utc.with_ymd_and_hms(2026, 7, 16, 0, 2, 0).unwrap();
        let watermark = Utc.with_ymd_and_hms(2026, 7, 14, 23, 59, 0).unwrap();

        assert_eq!(
            HotTierManager::inventory_dates(anchor, Some(watermark)),
            vec!["date=2026-07-14", "date=2026-07-15", "date=2026-07-16",]
        );
    }
}
