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

use datafusion::common::HashSet;
use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock};

use crate::{
    catalog::manifest::{File, Manifest},
    handlers::http::cluster::PMETA_STREAM_NAME,
    parseable::PARSEABLE,
    storage::{ObjectStorageError, field_stats::DATASET_STATS_STREAM_NAME},
    tenants::TENANT_METADATA,
    utils::{extract_datetime, human_size::bytes_to_human_size},
    validator::error::HotTierValidationError,
};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use futures::{StreamExt, TryStreamExt, stream::FuturesUnordered};
use futures_util::TryFutureExt;
use object_store::{ObjectStoreExt, local::LocalFileSystem};
use once_cell::sync::OnceCell;
use parquet::errors::ParquetError;
use relative_path::RelativePathBuf;
use std::time::Duration;
use sysinfo::Disks;
use tokio::fs::{self, DirEntry};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{Instrument, error, info};

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

/// Per-stream in-memory bookkeeping. Mutex protects concurrent reservation,
/// commit, and per-date manifest writes. Downloads run outside the lock.
struct StreamSyncState {
    sht: AsyncMutex<StreamHotTier>,
}

/// Hot-tier sync runs in two phases. Latest pulls files newer than
/// `hot_tier_latest_minutes` ago and may evict historic to make room.
/// Historic pulls older files, runs less often, never triggers eviction.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SyncPhase {
    Latest,
    Historic,
}

type StreamKey = (Option<String>, String);

struct StreamTasks {
    latest: tokio::task::JoinHandle<()>,
    historic: tokio::task::JoinHandle<()>,
}

pub struct HotTierManager {
    filesystem: LocalFileSystem,
    hot_tier_path: &'static Path,
    state_cache: AsyncRwLock<HashMap<StreamKey, Arc<StreamSyncState>>>,
    tasks: AsyncRwLock<HashMap<StreamKey, StreamTasks>>,
}

impl HotTierManager {
    pub fn new(hot_tier_path: &'static Path) -> Self {
        std::fs::create_dir_all(hot_tier_path).unwrap();
        HotTierManager {
            filesystem: LocalFileSystem::new(),
            hot_tier_path,
            state_cache: AsyncRwLock::new(HashMap::new()),
            tasks: AsyncRwLock::new(HashMap::new()),
        }
    }

    /// Lazy-load and cache the `StreamHotTier` for a (tenant, stream) pair.
    /// All sync-path mutations should acquire `state.sht.lock()`.
    async fn get_or_load_state(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<Arc<StreamSyncState>, HotTierError> {
        let key: StreamKey = (tenant_id.clone(), stream.to_owned());
        if let Some(state) = self.state_cache.read().await.get(&key).cloned() {
            return Ok(state);
        }
        // key not present, reconcile
        let sht = self.reconcile_stream(stream, tenant_id).await?;
        let state = Arc::new(StreamSyncState {
            sht: AsyncMutex::new(sht),
        });

        let mut cache = self.state_cache.write().await;
        if cache.insert(key, state.clone()).is_some() {
            tracing::warn!(
                "Key- {:?} was absent during read lock but already exists after reconcile!",
                (tenant_id, stream),
            );
        };
        Ok(state)
    }

    /// Drop cached state for a stream (used after delete).
    pub async fn invalidate_state(&self, stream: &str, tenant_id: &Option<String>) {
        let key: StreamKey = (tenant_id.clone(), stream.to_owned());
        self.state_cache.write().await.remove(&key);
    }

    /// Walk the on-disk hot-tier directory for a stream and bring it into
    /// agreement with `hottier.manifest.json` files. Removes `.partial`
    /// orphans, drops manifest entries whose files are missing or wrong size,
    /// deletes parquet files that exist but are not in their date manifest,
    /// then recomputes `used_size` / `available_size` from the cleaned
    /// manifests and persists the updated `StreamHotTier`.
    #[tracing::instrument(
        name = "hottier.reconcile_stream",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id),
        err
    )]
    async fn reconcile_stream(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<StreamHotTier, HotTierError> {
        info!(stream = %stream, tenant = ?tenant_id, "reconcile starting");
        let mut sht = self.get_hot_tier(stream, tenant_id).await?;
        let dates = self.fetch_hot_tier_dates(stream, tenant_id).await?;
        let mut total_used: u64 = 0;
        let mut partials_removed = 0usize;
        let mut entries_dropped = 0usize;
        let mut orphans_removed = 0usize;

        for date in dates {
            let date_dir = self.get_stream_path_for_date(stream, &date, tenant_id);
            if !date_dir.exists() {
                continue;
            }

            let mut on_disk: HashSet<String> = HashSet::new();

            // Pass 1: collect on-disk parquet files (drop .partial orphans).
            self.drop_partials(
                &mut on_disk,
                &date_dir,
                &mut partials_removed,
                stream,
                tenant_id,
            )
            .await?;

            // Pass 2: clean manifest of stale entries.
            let mut keep_names: HashSet<String> = HashSet::new();
            self.clean_manifest(
                &mut keep_names,
                &date_dir,
                &mut total_used,
                &mut entries_dropped,
                stream,
                tenant_id,
            )
            .await?;

            // Pass 3: delete on-disk parquet files not referenced by the cleaned manifest.
            for name in on_disk.difference(&keep_names) {
                let p = date_dir.join(name);
                let _ = fs::remove_file(&p).await;
                orphans_removed += 1;
                info!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    file = %p.display(),
                    "reconcile: deleted orphan parquet not in manifest"
                );
            }
        }

        sht.used_size = total_used;
        sht.available_size = sht.size.saturating_sub(total_used);
        self.put_hot_tier(stream, &mut sht, tenant_id).await?;
        info!(
            stream = %stream,
            tenant = ?tenant_id,
            partials_removed,
            entries_dropped,
            orphans_removed,
            used = sht.used_size,
            available = sht.available_size,
            "reconcile done"
        );
        Ok(sht)
    }

    #[tracing::instrument(
        name = "hottier.drop_partials",
        skip(self, on_disk, partials_removed),
        fields(stream = %stream, tenant = ?tenant_id, date_dir = %date_dir.display())
    )]
    async fn drop_partials(
        &self,
        on_disk: &mut HashSet<String>,
        date_dir: &PathBuf,
        partials_removed: &mut usize,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), HotTierError> {
        let mut stack: Vec<PathBuf> = vec![date_dir.clone()];
        while let Some(dir) = stack.pop() {
            let mut entries = fs::read_dir(&dir).await.map_err(|e| {
                error!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    dir = ?dir,
                    error = ?e
                );
                e
            })?;
            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                error!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    error = ?e
                );
                e
            })? {
                let p = entry.path();
                let ft = entry.file_type().await.map_err(|e| {
                    error!(
                        stream = %stream,
                        tenant = ?tenant_id,
                        entry = ?entry,
                        error = ?e
                    );
                    e
                })?;
                if ft.is_dir() {
                    stack.push(p);
                    continue;
                }
                let Some(name_os) = p.file_name() else {
                    continue;
                };
                let name = name_os.to_string_lossy();
                if name.ends_with(".partial") {
                    let _ = fs::remove_file(&p).await;
                    *partials_removed += 1;
                    info!(
                        stream = %stream,
                        tenant = ?tenant_id,
                        path = %p.display(),
                        "reconcile: deleted partial orphan"
                    );
                    continue;
                }
                if name.ends_with(".manifest.json") {
                    continue;
                }
                if !ft.is_file() {
                    continue;
                }
                if let Ok(rel) = p.strip_prefix(date_dir) {
                    on_disk.insert(rel.to_string_lossy().into_owned());
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "hottier.clean_manifest",
        skip(self, keep_names, total_used, entries_dropped),
        fields(stream = %stream, tenant = ?tenant_id, date_dir = %date_dir.display())
    )]
    async fn clean_manifest(
        &self,
        keep_names: &mut HashSet<String>,
        date_dir: &PathBuf,
        total_used: &mut u64,
        entries_dropped: &mut usize,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), HotTierError> {
        let manifest_path = date_dir.join("hottier.manifest.json");
        let mut manifest: Manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path).await.map_err(|e| {
                error!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    manifest_path = ?manifest_path,
                    error = ?e
                );
                e
            })?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            Manifest::default()
        };

        let mut kept = Vec::with_capacity(manifest.files.len());
        for f in manifest.files.drain(..) {
            let local = self.hot_tier_path.join(&f.file_path);
            let ok = match fs::metadata(&local).await {
                Ok(m) => m.len() == f.file_size,
                Err(_) => false,
            };
            if ok {
                if let Ok(rel) = local.strip_prefix(date_dir) {
                    keep_names.insert(rel.to_string_lossy().into_owned());
                }
                *total_used += f.file_size;
                kept.push(f);
            } else {
                let _ = fs::remove_file(&local).await;
                *entries_dropped += 1;
                info!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    file = %f.file_path,
                    "reconcile: dropped manifest entry (file missing or wrong size)"
                );
            }
        }
        kept.sort_by_key(|f| f.file_path.clone());
        manifest.files = kept;

        if manifest_path.exists() || !manifest.files.is_empty() {
            fs::create_dir_all(&date_dir).await?;
            fs::write(
                &manifest_path,
                serde_json::to_vec(&manifest).map_err(|e| {
                    error!(
                        stream = %stream,
                        tenant = ?tenant_id,
                        mainfest_path = ?manifest_path,
                        error = ?e
                    );
                    e
                })?,
            )
            .await
            .map_err(|e| {
                error!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    manifest_path = ?manifest_path,
                    error = ?e
                );
                e
            })?;
        }
        Ok(())
    }

    /// Get a global
    pub fn global() -> Option<&'static HotTierManager> {
        static INSTANCE: OnceCell<HotTierManager> = OnceCell::new();

        PARSEABLE
            .options
            .hot_tier_storage_path
            .as_ref()
            .map(|hot_tier_path| INSTANCE.get_or_init(|| HotTierManager::new(hot_tier_path)))
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
        if !self.check_stream_hot_tier_exists(stream, tenant_id) {
            return Err(HotTierValidationError::NotFound(stream.to_owned()).into());
        }
        let path = self.hot_tier_file_path(stream, tenant_id)?;
        let bytes = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await?;

        let mut stream_hot_tier: StreamHotTier = serde_json::from_slice(&bytes)?;
        stream_hot_tier.oldest_date_time_entry =
            self.get_oldest_date_time_entry(stream, tenant_id).await?;

        Ok(stream_hot_tier)
    }

    #[tracing::instrument(
        name = "hottier.delete_hot_tier",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id),
        err
    )]
    pub async fn delete_hot_tier(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), HotTierError> {
        if !self.check_stream_hot_tier_exists(stream, tenant_id) {
            return Err(HotTierValidationError::NotFound(stream.to_owned()).into());
        }
        // Stop loops before tearing down the directory so no in-flight tick
        // re-creates files mid-delete.
        self.abort_stream_tasks(stream, tenant_id).await;
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            self.hot_tier_path.join(tenant_id).join(stream)
        } else {
            self.hot_tier_path.join(stream)
        };
        fs::remove_dir_all(path).await.map_err(|e| {
            error!(
                stream = %stream,
                tenant = ?tenant_id,
                error = ?e
            );
            e
        })?;
        self.invalidate_state(stream, tenant_id).await;

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
        let guard = self.tasks.write().await;
        for (streamkey, task) in guard.iter() {
            task.latest.abort();
            task.historic.abort();
            info!("aborted hot tier tasks for- {streamkey:?}");
        }
    }

    /// Discover hot-tier-enabled streams at boot and spawn a per-stream pair
    /// of (Latest, Historic) loops for each. New streams added later acquire
    /// their own loops via `spawn_stream_tasks` from the PUT hot-tier handler.
    #[tracing::instrument(name = "hottier.startup", skip(self), err)]
    pub fn download_from_s3<'a>(&'a self) -> Result<(), HotTierError>
    where
        'a: 'static,
    {
        let latest_min = PARSEABLE.options.hot_tier_latest_minutes;
        let historic_min = PARSEABLE.options.hot_tier_historic_sync_minutes;
        info!(
            latest_minutes = latest_min,
            historic_sync_minutes = historic_min,
            "hot tier scheduler starting"
        );

        let this: &'static HotTierManager = self;
        let startup_span = tracing::info_span!("hottier.startup.bootstrap");
        let span = startup_span.clone();
        tokio::spawn(
            async move {
                // pstats hot tier may need to be created on boot before any tasks
                // can pick it up.
                if let Err(e) = this.create_pstats_hot_tier().await {
                    tracing::error!("Skipping pstats hot tier creation because of error: {e}");
                }
                let tenants = if let Some(tenants) = PARSEABLE.list_tenants() {
                    tenants.into_iter().map(Some).collect::<Vec<_>>()
                } else {
                    vec![None]
                };
                for tenant_id in tenants {
                    for stream in PARSEABLE.streams.list(&tenant_id) {
                        if this.check_stream_hot_tier_exists(&stream, &tenant_id) {
                            let tenant_id = tenant_id.clone();

                            tokio::spawn(async move {
                                this.spawn_stream_tasks(stream, tenant_id).await;
                            }.instrument(span.clone()));
                            tokio::time::sleep(Duration::from_secs(8)).await;
                        } else {
                            // check for potential orphan directory on disk
                            let path = if let Some(tenant_id) = tenant_id.as_ref() {
                                self.hot_tier_path.join(tenant_id).join(stream)
                            } else {
                                self.hot_tier_path.join(stream)
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
        Ok(())
    }

    /// Spawn (Latest, Historic) loops for a single stream. Idempotent:
    /// if tasks already exist for this (tenant, stream), no-op.
    #[tracing::instrument(
        name = "hottier.spawn_stream_tasks",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id)
    )]
    pub async fn spawn_stream_tasks(&'static self, stream: String, tenant_id: Option<String>) {
        let key: StreamKey = (tenant_id.clone(), stream.clone());
        {
            let tasks = self.tasks.read().await;
            if let Some(existing) = tasks.get(&key)
                && !existing.latest.is_finished()
                && !existing.historic.is_finished()
            {
                return;
            }
        }

        let latest_interval = Duration::from_secs(60);
        let historic_interval =
            Duration::from_secs(PARSEABLE.options.hot_tier_historic_sync_minutes as u64 * 60);

        info!(stream = %stream, tenant = ?tenant_id, "spawning per-stream hot tier tasks");

        let s = stream.clone();
        let t = tenant_id.clone();
        let latest = tokio::spawn(async move {
            loop {
                let anchor = floor_to_minute(Utc::now());
                let tick_span = tracing::info_span!(
                    "hottier.tick",
                    stream = %s,
                    tenant = ?t,
                    phase = "latest",
                    anchor = %anchor
                );
                async {
                    info!("stream tick fired");
                    if let Err(err) = self
                        .process_stream(s.clone(), t.clone(), SyncPhase::Latest, anchor)
                        .await
                    {
                        error!("latest sync error: {err:?}");
                    }
                }
                .instrument(tick_span)
                .await;
                tokio::time::sleep(latest_interval).await;
            }
        });

        let s = stream.clone();
        let t = tenant_id.clone();
        let historic = tokio::spawn(async move {
            loop {
                let anchor = floor_to_minute(Utc::now());
                let tick_span = tracing::info_span!(
                    "hottier.tick",
                    stream = %s,
                    tenant = ?t,
                    phase = "historic",
                    anchor = %anchor
                );
                async {
                    info!("stream tick fired");
                    if let Err(err) = self
                        .process_stream(s.clone(), t.clone(), SyncPhase::Historic, anchor)
                        .await
                    {
                        error!("historic sync error: {err:?}");
                    }
                }
                .instrument(tick_span)
                .await;
                tokio::time::sleep(historic_interval).await;
            }
        });

        let mut tasks = self.tasks.write().await;
        if let Some(old) = tasks.insert(key, StreamTasks { latest, historic }) {
            old.latest.abort();
            old.historic.abort();
        }
    }

    /// Abort and remove per-stream tasks. Caller must ensure no further work
    /// will be enqueued for the stream after this returns.
    async fn abort_stream_tasks(&self, stream: &str, tenant_id: &Option<String>) {
        let key: StreamKey = (tenant_id.clone(), stream.to_owned());
        if let Some(t) = self.tasks.write().await.remove(&key) {
            t.latest.abort();
            t.historic.abort();
            info!(stream = %stream, tenant = ?tenant_id, "aborted per-stream hot tier tasks");
        }
    }

    /// process the hot tier files for the stream
    /// delete the files from the hot tier directory if the available date range is outside the hot tier range
    #[tracing::instrument(
        name = "hottier.process_stream",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id, phase = ?phase, anchor = %anchor),
        err
    )]
    async fn process_stream(
        &self,
        stream: String,
        tenant_id: Option<String>,
        phase: SyncPhase,
        anchor: DateTime<Utc>,
    ) -> Result<(), HotTierError> {
        let stream_start = std::time::Instant::now();
        self.process_manifest(&stream, &tenant_id, phase, anchor)
            .await
            .map_err(|e| {
                error!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    phase = ?phase,
                    error = ?e
                );
                e
            })?;

        info!(
            stream = %stream,
            tenant = ?tenant_id,
            phase = ?phase,
            elapsed_ms = stream_start.elapsed().as_millis() as u64,
            "stream sync done"
        );
        Ok(())
    }

    /// process the hot tier files for the stream
    /// Determine the candidate dates for the current phase, fetch only those
    /// manifests from the metastore, build a work list sorted newest-first by
    /// file timestamp, then download via the existing reserve/commit flow.
    #[tracing::instrument(
        name = "hottier.process_manifest",
        skip(self),
        fields(
            stream = %stream,
            tenant = ?tenant_id,
            phase = ?phase,
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
        phase: SyncPhase,
        anchor: DateTime<Utc>,
    ) -> Result<(), HotTierError> {
        let latest_minutes = PARSEABLE.options.hot_tier_latest_minutes;
        let latest_window = chrono::Duration::minutes(latest_minutes.try_into().unwrap());
        let historic_cutoff = anchor - latest_window;
        let historic_cutoff_naive = historic_cutoff.naive_utc();
        let today_date_key = format!("date={}", anchor.date_naive());

        // Determine which date keys to fetch from the metastore this tick.
        let candidate_dates: Vec<String> = match phase {
            SyncPhase::Latest => {
                // Dates covered by [historic_cutoff, anchor]. Usually just today,
                // or today + yesterday if window crosses midnight.
                let start = historic_cutoff.date_naive();
                let end = anchor.date_naive();
                let mut out = Vec::new();
                let mut d = start;
                while d <= end {
                    out.push(format!("date={d}"));
                    d = d.succ_opt().unwrap_or(d);
                    if out.len() > 365 {
                        break;
                    }
                }
                out
            }
            SyncPhase::Historic => {
                let local = self
                    .fetch_hot_tier_dates(stream, tenant_id)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|d| format!("date={d}"))
                    .collect::<Vec<_>>();
                let s3 = PARSEABLE
                    .storage()
                    .get_object_store()
                    .list_dates(stream, tenant_id)
                    .await
                    .unwrap_or_default();

                let mut union: std::collections::BTreeSet<String> = local.into_iter().collect();
                union.extend(s3);

                let mut out = Vec::new();
                for date_key in union {
                    // drop today and anything >= today (Latest handles those)
                    if date_key.as_str() >= today_date_key.as_str() {
                        continue;
                    }
                    if self.is_locally_complete(stream, &date_key, tenant_id).await {
                        continue;
                    }
                    out.push(date_key);
                }
                // Newest-first: discover newest missing past date first.
                out.sort();
                out.reverse();
                out
            }
        };
        tracing::Span::current().record("candidate_dates", candidate_dates.len());

        if candidate_dates.is_empty() {
            info!(
                stream = %stream,
                tenant = ?tenant_id,
                phase = ?phase,
                "no candidate dates this tick"
            );
            return Ok(());
        }

        let s3_manifests = PARSEABLE
            .metastore
            .get_manifest_files_for_dates(stream, tenant_id, &candidate_dates)
            .await
            .map_err(|e| {
                error!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    phase = ?phase,
                    error = ?e,
                    "manifest fetch failed"
                );
                HotTierError::ObjectStorageError(ObjectStorageError::MetastoreError(Box::new(
                    e.to_detail(),
                )))
            })?;

        // Build flat work list across all candidate dates: keep only files
        // matching this phase's cutoff and not already on disk.
        let mut work: Vec<(NaiveDate, chrono::NaiveDateTime, File, PathBuf)> = Vec::new();
        for (str_date, manifest_files) in s3_manifests.iter() {
            let date =
                match NaiveDate::parse_from_str(str_date.trim_start_matches("date="), "%Y-%m-%d") {
                    Ok(d) => d,
                    Err(_) => {
                        info!("Invalid date format: {}", str_date);
                        continue;
                    }
                };

            for storage_manifest in manifest_files {
                for parquet_file in &storage_manifest.files {
                    let parquet_path = self.hot_tier_path.join(&parquet_file.file_path);
                    if parquet_path.exists() {
                        continue;
                    }
                    let dt = match extract_datetime(&parquet_file.file_path) {
                        Some(d) => d,
                        None => continue,
                    };
                    let is_latest = dt >= historic_cutoff_naive;
                    let keep = match phase {
                        SyncPhase::Latest => is_latest,
                        SyncPhase::Historic => !is_latest,
                    };
                    if keep {
                        work.push((date, dt, parquet_file.clone(), parquet_path));
                    }
                }
            }
        }

        // Newest first by file timestamp.
        work.sort_by_key(|b| std::cmp::Reverse(b.1));

        let work_count = work.len();
        let total_bytes: u64 = work.iter().map(|(_, _, f, _)| f.file_size).sum();
        tracing::Span::current()
            .record("work_count", work_count)
            .record("total_bytes", total_bytes);
        if work.is_empty() {
            info!(
                stream = %stream,
                tenant = ?tenant_id,
                phase = ?phase,
                "no files to download this tick"
            );
            return Ok(());
        }
        info!(
            stream = %stream,
            tenant = ?tenant_id,
            phase = ?phase,
            work_count,
            total_bytes,
            "work list built"
        );

        let state = self.get_or_load_state(stream, tenant_id).await?;
        let concurrency = PARSEABLE.options.hot_tier_files_per_stream_concurrency;

        // Reservation failure (out of disk + nothing to evict) is sticky:
        // once one file can't be placed, no subsequent file will fit either.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let stream_owned = stream.to_owned();
        let tenant_owned = tenant_id.clone();

        let results: Vec<Result<(), HotTierError>> = futures::stream::iter(work)
            .map(|(date, _dt, file, parquet_path)| {
                let state = state.clone();
                let stream = stream_owned.clone();
                let tenant_id = tenant_owned.clone();
                let stop = stop.clone();
                async move {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        return Ok(());
                    }
                    let processed = self
                        .process_parquet_file_concurrent(
                            &stream,
                            &file,
                            parquet_path,
                            date,
                            &tenant_id,
                            &state,
                            phase,
                        )
                        .await?;
                    if !processed && !stop.swap(true, std::sync::atomic::Ordering::Relaxed) {
                        info!(
                            stream = %stream,
                            tenant = ?tenant_id,
                            phase = ?phase,
                            "sticky stop: halting further reservations this tick"
                        );
                    }
                    Ok(())
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        for r in results {
            r?;
        }

        Ok(())
    }

    /// Reserve disk budget under the per-stream lock, download outside the lock,
    /// then commit usage + per-date manifest under the lock again.
    /// Returns false when no budget is available (caller should stop scheduling
    /// further work for this stream).
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        name = "hottier.process_parquet_file",
        skip(self, parquet_file, parquet_path, state),
        fields(
            stream = %stream,
            tenant = ?tenant_id,
            phase = ?phase,
            date = %date,
            file = %parquet_file.file_path,
            file_size = parquet_file.file_size
        ),
        err
    )]
    async fn process_parquet_file_concurrent(
        &self,
        stream: &str,
        parquet_file: &File,
        parquet_path: PathBuf,
        date: NaiveDate,
        tenant_id: &Option<String>,
        state: &Arc<StreamSyncState>,
        phase: SyncPhase,
    ) -> Result<bool, HotTierError> {
        // RESERVE
        {
            let mut sht = state.sht.lock().await;
            info!(
                stream = %stream,
                tenant = ?tenant_id,
                phase = ?phase,
                file = %parquet_file.file_path,
                file_size = parquet_file.file_size,
                available = sht.available_size,
                used = sht.used_size,
                "reserving"
            );
            if !self.is_disk_available(parquet_file.file_size).await?
                || sht.available_size < parquet_file.file_size
            {
                match phase {
                    SyncPhase::Latest => {
                        info!(
                            stream = %stream,
                            tenant = ?tenant_id,
                            file = %parquet_file.file_path,
                            file_size = parquet_file.file_size,
                            available = sht.available_size,
                            "tight on space; triggering eviction"
                        );
                        if !self
                            .cleanup_hot_tier_old_data(
                                stream,
                                &mut sht,
                                &parquet_path,
                                parquet_file.file_size,
                                tenant_id,
                            )
                            .await?
                        {
                            info!(
                                stream = %stream,
                                tenant = ?tenant_id,
                                file = %parquet_file.file_path,
                                file_size = parquet_file.file_size,
                                "eviction freed nothing, skipping file"
                            );
                            return Ok(false);
                        }
                    }
                    SyncPhase::Historic => {
                        info!(
                            stream = %stream,
                            tenant = ?tenant_id,
                            file = %parquet_file.file_path,
                            file_size = parquet_file.file_size,
                            available = sht.available_size,
                            "historic phase: full, skipping file"
                        );
                        return Ok(false);
                    }
                }
            }
            if sht.available_size < parquet_file.file_size {
                info!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    file = %parquet_file.file_path,
                    file_size = parquet_file.file_size,
                    available = sht.available_size,
                    "still no space after eviction, skipping"
                );
                return Ok(false);
            }
            sht.available_size =
                if let Some(val) = sht.available_size.checked_sub(parquet_file.file_size) {
                    val
                } else {
                    tracing::error!(
                        stream = %stream,
                        tenant = ?tenant_id,
                        file = %parquet_file.file_path,
                        file_size = parquet_file.file_size,
                        available = sht.available_size,
                        "file_size > sht.available_size, setting available_size to 0 and moving on"
                    );
                    0
                };
            self.put_hot_tier(stream, &mut sht, tenant_id).await?;
            info!(
                stream = %stream,
                tenant = ?tenant_id,
                file = %parquet_file.file_path,
                deducted = parquet_file.file_size,
                new_available = sht.available_size,
                "reserved"
            );
        }

        // DOWNLOAD (no lock held)
        let parquet_file_path = RelativePathBuf::from(parquet_file.file_path.clone());
        fs::create_dir_all(parquet_path.parent().unwrap()).await?;
        info!(
            stream = %stream,
            tenant = ?tenant_id,
            file = %parquet_file.file_path,
            file_size = parquet_file.file_size,
            "download starting"
        );
        let dl_start = std::time::Instant::now();
        let download_result = PARSEABLE
            .storage
            .get_object_store()
            .parallel_chunked_download(&parquet_file_path, tenant_id, parquet_path.clone())
            .await;
        let dl_elapsed = dl_start.elapsed();

        if let Err(e) = download_result {
            info!(
                stream = %stream,
                tenant = ?tenant_id,
                file = %parquet_file.file_path,
                elapsed_ms = dl_elapsed.as_millis() as u64,
                err = %e,
                "download failed, refunding reservation"
            );
            // refund reservation
            let mut sht = state.sht.lock().await;
            sht.available_size += parquet_file.file_size;
            if let Err(put_err) = self.put_hot_tier(stream, &mut sht, tenant_id).await {
                error!("failed to persist refund after download failure: {put_err:?}");
            }
            // backend already cleaned up its `.partial` file; final path was never created.
            return Err(e.into());
        }
        let elapsed_ms = dl_elapsed.as_millis() as u64;
        let mbps = if dl_elapsed.as_secs_f64() > 0.0 {
            (parquet_file.file_size as f64 * 8.0) / dl_elapsed.as_secs_f64() / 1_000_000.0
        } else {
            0.0
        };
        info!(
            stream = %stream,
            tenant = ?tenant_id,
            file = %parquet_file.file_path,
            file_size = parquet_file.file_size,
            elapsed_ms,
            mbps = format!("{mbps:.1}"),
            "download finished, committing"
        );

        // COMMIT
        {
            let mut sht = state.sht.lock().await;
            sht.used_size += parquet_file.file_size;
            self.put_hot_tier(stream, &mut sht, tenant_id).await?;

            let path = self.get_stream_path_for_date(stream, &date, tenant_id);
            let mut hot_tier_manifest =
                HotTierManager::get_hot_tier_manifest_from_path(path).await?;
            hot_tier_manifest.files.push(parquet_file.clone());
            hot_tier_manifest
                .files
                .sort_by_key(|file| file.file_path.clone());
            // write the manifest file to the hot tier directory
            let manifest_path = self
                .get_stream_path_for_date(stream, &date, tenant_id)
                .join("hottier.manifest.json");
            fs::create_dir_all(manifest_path.parent().unwrap()).await?;
            fs::write(manifest_path, serde_json::to_vec(&hot_tier_manifest)?).await?;
            info!(
                stream = %stream,
                tenant = ?tenant_id,
                file = %parquet_file.file_path,
                used = sht.used_size,
                available = sht.available_size,
                "committed"
            );
        }

        Ok(true)
    }

    ///fetch the list of dates available in the hot tier directory for the stream and sort them
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
            let date = NaiveDate::parse_from_str(
                date.file_name()
                    .to_string_lossy()
                    .trim_start_matches("date="),
                "%Y-%m-%d",
            )
            .unwrap();
            date_list.push(date);
        }
        date_list.sort();

        Ok(date_list)
    }

    ///get hot tier manifest on path
    pub async fn get_hot_tier_manifest_from_path(path: PathBuf) -> Result<Manifest, HotTierError> {
        if !path.exists() {
            return Ok(Manifest::default());
        }

        // List the directories and prepare the hot tier manifest
        let mut date_dirs = fs::read_dir(&path).await?;
        let mut hot_tier_manifest = Manifest::default();

        // Avoid unnecessary checks and keep only valid manifest files
        while let Some(manifest) = date_dirs.next_entry().await? {
            if !manifest
                .file_name()
                .to_string_lossy()
                .ends_with(".manifest.json")
            {
                continue;
            }
            // Deserialize each manifest file and extend the hot tier manifest with its files
            let file = fs::read(manifest.path()).await?;
            let manifest: Manifest = serde_json::from_slice(&file)?;
            hot_tier_manifest.files.extend(manifest.files);
        }

        Ok(hot_tier_manifest)
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

    /// A past date is treated as fully synced when its on-disk hottier
    /// manifest exists and lists at least one parquet. Used by the Historic
    /// phase to skip the metastore fetch for dates we already have data for.
    /// `date_key` is the partition directory name (e.g. "date=2026-05-12").
    async fn is_locally_complete(
        &self,
        stream: &str,
        date_key: &str,
        tenant_id: &Option<String>,
    ) -> bool {
        let date_dir = if let Some(tenant) = tenant_id.as_ref() {
            self.hot_tier_path.join(tenant).join(stream).join(date_key)
        } else {
            self.hot_tier_path.join(stream).join(date_key)
        };
        let manifest_path = date_dir.join("hottier.manifest.json");
        if !manifest_path.exists() {
            return false;
        }
        match fs::read(&manifest_path).await {
            Ok(bytes) => match serde_json::from_slice::<Manifest>(&bytes) {
                Ok(m) => !m.files.is_empty(),
                Err(_) => false,
            },
            Err(_) => false,
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
            if let Ok(meta) = fs::metadata(&hot_tier_path).await
                && meta.len() == file.file_size
            {
                hot_tier_files.push(file);
                continue;
            }

            remaining.push(file);
        }

        *manifest_files = remaining;

        // Sort both lists in descending order by file path.
        hot_tier_files.sort_unstable_by(|a, b| b.file_path.cmp(&a.file_path));
        manifest_files.sort_unstable_by(|a, b| b.file_path.cmp(&a.file_path));

        Ok(hot_tier_files)
    }

    ///get the list of parquet files from the hot tier directory for the stream
    #[tracing::instrument(
        name = "hottier.get_parquet_files",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id),
        err
    )]
    pub async fn get_hot_tier_parquet_files(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<File>, HotTierError> {
        // Fetch list of dates for the given stream
        let date_list = self.fetch_hot_tier_dates(stream, tenant_id).await?;

        // Create an unordered iter of futures to async collect files
        let mut tasks = FuturesUnordered::new();

        // For each date, fetch the manifest and extract parquet files
        for date in date_list {
            let path = self.get_stream_path_for_date(stream, &date, tenant_id);
            tasks.push(async move {
                HotTierManager::get_hot_tier_manifest_from_path(path)
                    .await
                    .map(|manifest| manifest.files.clone())
                    .unwrap_or_default() // If fetching manifest fails, return an empty vector
            });
        }

        // Collect parquet files for all dates
        let mut hot_tier_parquet_files: Vec<File> = vec![];
        while let Some(files) = tasks.next().await {
            hot_tier_parquet_files.extend(files);
        }

        Ok(hot_tier_parquet_files)
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

    /// delete entire parquet file minute from the hot tier directory for the stream
    /// loop through all manifests in the hot tier directory for the stream
    /// loop through all parquet files in the manifest
    /// check for the oldest entry to delete if the path exists in hot tier
    /// update the used and available size in the hot tier metadata
    /// loop if available size is still less than the parquet file size
    #[tracing::instrument(
        name = "hottier.cleanup_old_data",
        skip(self, stream_hot_tier, download_file_path),
        fields(stream = %stream, tenant = ?tenant_id, target_size = parquet_file_size),
        err
    )]
    pub async fn cleanup_hot_tier_old_data(
        &self,
        stream: &str,
        stream_hot_tier: &mut StreamHotTier,
        download_file_path: &Path,
        parquet_file_size: u64,
        tenant_id: &Option<String>,
    ) -> Result<bool, HotTierError> {
        info!(
            stream = %stream,
            tenant = ?tenant_id,
            target_size = parquet_file_size,
            available = stream_hot_tier.available_size,
            "eviction starting"
        );
        let mut delete_successful = false;
        let mut freed_total: u64 = 0;
        let dates = self.fetch_hot_tier_dates(stream, tenant_id).await?;
        if dates.is_empty() {
            info!(
                stream = %stream,
                tenant = ?tenant_id,
                "eviction: no date dirs found, nothing to evict"
            );
        }
        'loop_dates: for date in dates {
            let path = self.get_stream_path_for_date(stream, &date, tenant_id);
            if !path.exists() {
                info!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    date = %date,
                    path = %path.display(),
                    "eviction: date path missing, skipping"
                );
                continue;
            }

            let date_dirs = ReadDirStream::new(fs::read_dir(&path).await?);
            let mut manifest_files: Vec<DirEntry> = date_dirs.try_collect().await?;
            manifest_files.retain(|manifest| {
                manifest
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".manifest.json")
            });
            if manifest_files.is_empty() {
                info!(
                    stream = %stream,
                    tenant = ?tenant_id,
                    date = %date,
                    path = %path.display(),
                    "eviction: no .manifest.json files in date dir"
                );
                continue;
            }
            for manifest_file in manifest_files {
                let file = fs::read(manifest_file.path()).await?;
                let mut manifest: Manifest = serde_json::from_slice(&file)?;

                if manifest.files.is_empty() {
                    info!(
                        stream = %stream,
                        tenant = ?tenant_id,
                        manifest = %manifest_file.path().display(),
                        "eviction: manifest has zero file entries"
                    );
                    continue;
                }

                // sort in an ascending manner
                // idx0: minute=00
                // idx59: minute=59
                manifest.files.sort_by_key(|file| file.file_path.clone());

                // get first file's parent (/hottier/stream/date=d/hour=h/minute=m)
                let first_file = manifest.files.first().unwrap();
                let first_file_path = self.hot_tier_path.join(&first_file.file_path);
                let minute_to_delete = first_file_path.parent().unwrap();

                if !minute_to_delete.exists() {
                    info!(
                        stream = %stream,
                        tenant = ?tenant_id,
                        manifest = %manifest_file.path().display(),
                        first_file = %first_file.file_path,
                        minute = %minute_to_delete.display(),
                        "eviction: minute dir referenced by manifest does not exist on disk"
                    );
                    continue;
                }
                {
                    if let (Some(download_date_time), Some(delete_date_time)) = (
                        extract_datetime(download_file_path.to_str().unwrap()),
                        extract_datetime(first_file_path.to_str().unwrap()),
                    ) && download_date_time <= delete_date_time
                    {
                        info!(
                            stream = %stream,
                            tenant = ?tenant_id,
                            candidate = %minute_to_delete.display(),
                            target = %download_file_path.display(),
                            "skip evict: candidate newer than target"
                        );
                        continue;
                    }

                    let minute_to_delete_owned = minute_to_delete.to_path_buf();
                    let mut minute_freed: u64 = 0;
                    manifest.files.retain(|file| {
                        let file_path = self.hot_tier_path.join(&file.file_path);
                        let file_minute = file_path.parent().unwrap();
                        if file_minute == minute_to_delete_owned {
                            minute_freed = minute_freed.saturating_add(file.file_size);
                            false
                        } else {
                            true
                        }
                    });

                    stream_hot_tier.used_size = stream_hot_tier
                        .used_size
                        .checked_sub(minute_freed)
                        .unwrap_or_else(|| {
                            tracing::error!(
                                stream = %stream,
                                tenant = ?tenant_id,
                                minute = %minute_to_delete_owned.display(),
                                minute_freed,
                                used_size = stream_hot_tier.used_size,
                                "minute_freed > used_size, clamping used_size to 0"
                            );
                            0
                        });
                    stream_hot_tier.available_size =
                        stream_hot_tier.available_size.saturating_add(minute_freed);
                    freed_total = freed_total.saturating_add(minute_freed);

                    fs::write(manifest_file.path(), serde_json::to_vec(&manifest)?).await?;
                    fs::remove_dir_all(&minute_to_delete_owned).await?;
                    delete_empty_directory_hot_tier(minute_to_delete_owned.clone()).await?;
                    self.put_hot_tier(stream, stream_hot_tier, tenant_id)
                        .await?;
                    delete_successful = true;
                    info!(
                        stream = %stream,
                        tenant = ?tenant_id,
                        evicted_minute = %minute_to_delete_owned.display(),
                        evicted_size = minute_freed,
                        freed_total,
                        new_available = stream_hot_tier.available_size,
                        "evicted"
                    );
                    if stream_hot_tier.available_size < parquet_file_size {
                        continue;
                    } else {
                        break 'loop_dates;
                    }
                }
            }
        }

        info!(
            stream = %stream,
            tenant = ?tenant_id,
            freed_total,
            success = delete_successful,
            "eviction complete"
        );
        Ok(delete_successful)
    }

    /// check if the disk is available to download the parquet file
    /// check if the disk usage is above the threshold
    pub async fn is_disk_available(&self, size_to_download: u64) -> Result<bool, HotTierError> {
        if let Some(DiskUtil {
            total_space,
            available_space,
            used_space,
        }) = self.get_disk_usage()
        {
            if available_space < size_to_download {
                return Ok(false);
            }

            if ((used_space + size_to_download) as f64 * 100.0 / total_space as f64)
                > PARSEABLE.options.max_disk_usage
            {
                return Ok(false);
            }
        }

        Ok(true)
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

async fn delete_empty_directory_hot_tier(path: PathBuf) -> io::Result<()> {
    if !path.is_dir() {
        return Ok(());
    }
    let mut read_dir = fs::read_dir(&path).await?;

    let mut tasks = vec![];
    while let Some(entry) = read_dir.next_entry().await? {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            tasks.push(delete_empty_directory_hot_tier(entry_path));
        }
    }

    futures::stream::iter(tasks)
        .buffer_unordered(10)
        .try_collect::<Vec<_>>()
        .await?;

    // Re-check the directory after deleting its subdirectories
    let mut read_dir = fs::read_dir(&path).await?;
    if read_dir.next_entry().await?.is_none() {
        fs::remove_dir(&path).await?;
    }

    Ok(())
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
