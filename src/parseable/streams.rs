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
 *
 */

use arrow::{
    compute::take,
    row::{RowConverter, SortField},
};
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{ArrowError, Field, Fields, Schema, SortOptions};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};
use derive_more::derive::{Deref, DerefMut};
use itertools::Itertools;
use once_cell::sync::{Lazy, OnceCell};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_writer::{ArrowColumnChunk, ArrowRowGroupWriterFactory, compute_leaves},
    },
    basic::Encoding,
    errors::ParquetError,
    file::{
        FOOTER_SIZE,
        metadata::SortingColumn,
        properties::{BloomFilterPosition, WriterProperties},
        reader::FileReader,
        serialized_reader::SerializedFileReader,
        writer::SerializedFileWriter,
    },
    schema::types::ColumnPath,
};
use rayon::{
    iter::{
        IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
    },
    slice::ParallelSliceMut,
};
use std::sync::PoisonError;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions, remove_file, write},
    io::{Read, Write},
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinSet;
use tracing::{debug, error, info, info_span, instrument, trace, warn};
use ulid::Ulid;

use crate::{
    LOCK_EXPECT, OBJECT_STORE_DATA_GRANULARITY,
    cli::Options,
    event::{
        DEFAULT_TIMESTAMP_KEY,
        format::{LogSource, LogSourceEntry},
    },
    handlers::{DatasetTag, http::ingest::INGESTION_THREADPOOL},
    hottier::StreamHotTier,
    metadata::{LogStreamMetadata, SchemaVersion},
    metrics,
    option::Mode,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    storage::{StreamType, object_storage::to_bytes, retention::Retention},
    sync::FLUSH_AND_CONVERT_RUNTIME,
    utils::{
        arrow::adapt_batch,
        time::{Minute, TimeRange},
    },
};

use super::{
    ARROW_FILE_EXTENSION, ARROW_PART_FILE_SUFFIX, LogStream, PARQUET_PART_FILE_SUFFIX,
    PART_FILE_EXTENSION,
    staging::{
        StagingError,
        reader::{
            ForwardFilePlan, ForwardStreamReader, MergedForwardRecordReader,
            MergedReverseRecordReader, get_reverse_reader,
        },
        writer::{DiskWriter, Writer},
    },
};

mod conversion_journal;
use conversion_journal::{ConversionJournal, has_journal, has_journal_for_part};
mod log_clustering;
use log_clustering::LogClusterKey;

static HOSTNAME: OnceCell<String> = OnceCell::new();
static METRIC_PARQUET_THREADPOOL: Lazy<rayon::ThreadPool> = Lazy::new(|| {
    rayon::ThreadPoolBuilder::new()
        .thread_name(|index| format!("metric-parquet-{index}"))
        .build()
        .expect("Metric Parquet thread pool should be constructible")
});

/// Bounded pool for Arrow-to-Parquet conversion of staged groups. Conversion
/// runs at every minute boundary; on the global Rayon pool it competed with
/// the HTTP ingest jobs spawned there and caused tail-latency spikes at `:00`.
/// Nested Rayon work inside a conversion stays on this pool.
static LOG_CONVERSION_THREADPOOL: Lazy<rayon::ThreadPool> = Lazy::new(|| {
    let threads = log_conversion_threads(
        std::env::var(LOG_CONVERSION_THREADS_VAR).ok().as_deref(),
        num_cpus::get(),
    );
    #[cfg(test)]
    let threads = threads.max(6);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("log-conversion-{index}"))
        .build()
        .expect("Log conversion thread pool should be constructible")
});
const LOG_CONVERSION_THREADS_VAR: &str = "LOG_CONVERSION_THREADS";

/// Threads for `LOG_CONVERSION_THREADPOOL`: a positive override, else half the
/// CPUs clamped to 2..=8 so conversion cannot take every core from ingest.
fn log_conversion_threads(var: Option<&str>, cpus: usize) -> usize {
    var.and_then(|var| var.trim().parse::<usize>().ok())
        .filter(|&threads| threads > 0)
        .unwrap_or_else(|| (cpus / 2).clamp(2, 8))
}

const INPROCESS_DIR_PREFIX: &str = "processing_";
const METRIC_NAME_BLOOM_FILTER_NDV: u64 = 32768;
const MAX_PERIODIC_PARQUET_GROUPS_PER_STREAM: usize = 2;
const PARQUET_COLUMNS_PER_RAYON_TASK_VAR: &str = "PARQUET_COLUMNS_PER_RAYON_TASK";
const METRIC_ARROW_READERS_PER_FILE_VAR: &str = "METRIC_ARROW_READERS_PER_FILE";
const METRIC_ARROW_READERS_IN_FLIGHT_VAR: &str = "METRIC_ARROW_READERS_IN_FLIGHT";
const METRIC_ROW_GROUP_PREP_IN_FLIGHT_VAR: &str = "METRIC_ROW_GROUP_PREP_IN_FLIGHT";
const METRIC_ROW_GROUP_ENCODE_IN_FLIGHT_VAR: &str = "METRIC_ROW_GROUP_ENCODE_IN_FLIGHT";
/// Minimum number of Parquet columns encoded by one Rayon task. Lower values
/// expose more column parallelism; higher values reduce scheduling overhead.
static PARQUET_COLUMNS_PER_RAYON_TASK: Lazy<usize> = Lazy::new(|| {
    if let Ok(var) = std::env::var(PARQUET_COLUMNS_PER_RAYON_TASK_VAR)
        && let Ok(var) = var.parse::<usize>()
        && var > 0
    {
        var
    } else {
        8
    }
});
/// Number of independent Arrow IPC decoders used for each metric source file.
/// Batches are striped across readers and emitted in their original order.
static METRIC_ARROW_READERS_PER_FILE: Lazy<usize> = Lazy::new(|| {
    if let Ok(var) = std::env::var(METRIC_ARROW_READERS_PER_FILE_VAR)
        && let Ok(var) = var.parse::<usize>()
        && var > 0
    {
        var
    } else {
        4
    }
});
/// Total Arrow IPC readers allowed to decode ahead for one metric Parquet.
/// A one-batch file consumes one slot, so small files are decoded concurrently;
/// a multi-batch file may consume several slots up to the per-file limit.
static METRIC_ARROW_READERS_IN_FLIGHT: Lazy<usize> = Lazy::new(|| {
    if let Ok(var) = std::env::var(METRIC_ARROW_READERS_IN_FLIGHT_VAR)
        && let Ok(var) = var.parse::<usize>()
        && var > 0
    {
        var
    } else {
        4
    }
});
/// Caps concurrent metric row-group preparation tasks. Preparation results are
/// forwarded in source order so Parquet ordering remains deterministic.
static METRIC_ROW_GROUP_PREP_IN_FLIGHT: Lazy<usize> = Lazy::new(|| {
    if let Ok(var) = std::env::var(METRIC_ROW_GROUP_PREP_IN_FLIGHT_VAR)
        && let Ok(var) = var.parse::<usize>()
        && var > 0
    {
        var
    } else {
        2
    }
});

/// Caps concurrently encoded row groups for one metric Parquet file. Row
/// groups are appended to the final file in source order after encoding.
static METRIC_ROW_GROUP_ENCODE_IN_FLIGHT: Lazy<usize> = Lazy::new(|| {
    if let Ok(var) = std::env::var(METRIC_ROW_GROUP_ENCODE_IN_FLIGHT_VAR)
        && let Ok(var) = var.parse::<usize>()
        && var > 0
    {
        var
    } else {
        2
    }
});

const MAX_ARROW_FILES_PER_PARQUET_VAR: &str = "MAX_ARROW_FILES_PER_PARQUET";
/// Caps how many arrow files feed a single parquet conversion group. A
/// minute with heavy schema-key churn can stage thousands of small arrow
/// files; converting them as one group means one kmerge holding an open
/// reader (and a decoded batch) per file. Chunking bounds that memory
/// while still collapsing thousands of files into a handful of parquets.
static MAX_ARROW_FILES_PER_PARQUET: Lazy<usize> = Lazy::new(|| {
    if let Ok(var) = std::env::var(MAX_ARROW_FILES_PER_PARQUET_VAR)
        && let Ok(var) = var.parse::<usize>()
        && var > 0
    {
        var
    } else {
        512
    }
});

/// Splits any conversion group holding more than MAX_ARROW_FILES_PER_PARQUET
/// arrow files into chunks, giving chunk 1.. a "-{i}" suffix on the parquet
/// file stem so each chunk converts to its own parquet file.
fn chunk_arrow_file_groups(
    grouped: HashMap<PathBuf, Vec<PathBuf>>,
) -> HashMap<PathBuf, Vec<PathBuf>> {
    let max_files = *MAX_ARROW_FILES_PER_PARQUET;
    let mut chunked = HashMap::with_capacity(grouped.len());
    for (parquet_path, arrow_files) in grouped {
        if arrow_files.len() <= max_files {
            chunked.insert(parquet_path, arrow_files);
            continue;
        }
        for (i, chunk) in arrow_files.chunks(max_files).enumerate() {
            let chunk_path = if i == 0 {
                parquet_path.clone()
            } else {
                let mut path = parquet_path.clone();
                if let (Some(stem), Some(ext)) = (
                    parquet_path.file_stem().and_then(|s| s.to_str()),
                    parquet_path.extension().and_then(|e| e.to_str()),
                ) {
                    path.set_file_name(format!("{stem}-{i}.{ext}"));
                }
                path
            };
            chunked.insert(chunk_path, chunk.to_vec());
        }
    }
    chunked
}

/// Extracts the event minute from a staging output name such as
/// `date=2026-09-09.hour=10.minute=36.host.data.<ulid>.parquet`.
fn staging_event_minute(path: &Path) -> Option<NaiveDateTime> {
    let filename = path.file_name()?.to_str()?;
    let mut date = None;
    let mut hour = None;
    let mut minute = None;
    for component in filename.split('.') {
        if let Some(value) = component.strip_prefix("date=") {
            date = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok();
        } else if let Some(value) = component.strip_prefix("hour=") {
            hour = value.parse::<u32>().ok();
        } else if let Some(value) = component.strip_prefix("minute=") {
            minute = value.parse::<u32>().ok();
        }
    }
    date?.and_hms_opt(hour?, minute?, 0)
}

/// Produces deterministic, oldest-event-minute-first conversion work. The
/// file-count cap is retained so a single very large minute remains bounded.
fn ordered_arrow_file_groups(
    grouped: HashMap<PathBuf, Vec<PathBuf>>,
) -> Vec<(PathBuf, Vec<PathBuf>)> {
    let mut groups = chunk_arrow_file_groups(grouped)
        .into_iter()
        .collect::<Vec<_>>();
    for (_, files) in &mut groups {
        files.sort();
    }
    groups.sort_by(|(left, _), (right, _)| {
        match (staging_event_minute(left), staging_event_minute(right)) {
            (Some(left_minute), Some(right_minute)) => {
                left_minute.cmp(&right_minute).then_with(|| left.cmp(right))
            }
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => left.cmp(right),
        }
    });
    groups
}

struct PreparedMetricRowGroup {
    batch: RecordBatch,
    arrow_read_decode_duration: Duration,
    concat_duration: Duration,
    sort_duration: Duration,
    record_batches: usize,
}

enum MetricRowGroupPipelineMessage {
    Prepared(PreparedMetricRowGroup),
    Failed(StagingError),
    Complete,
}

#[derive(Default)]
struct MetricParquetWriteTimings {
    setup_duration: Duration,
    encode_duration: Duration,
    append_duration: Duration,
    row_groups: usize,
    column_encode_timings: Vec<MetricColumnEncodeTiming>,
}

struct MetricColumnEncodeTiming {
    path: String,
    duration: Duration,
    bytes_written: u64,
}

struct EncodedMetricRowGroup {
    row_group_index: usize,
    rows: usize,
    chunks: Vec<(ArrowColumnChunk, MetricColumnEncodeTiming)>,
    setup_duration: Duration,
    encode_duration: Duration,
}

struct MetricRowGroupPreparationTimings {
    arrow_read_decode_duration: Duration,
    concat_duration: Duration,
    sort_duration: Duration,
    prepare_wait_duration: Duration,
    record_batches: usize,
}

#[derive(Default)]
struct MetricParquetPhaseTimings {
    arrow_read_decode_duration: Duration,
    concat_duration: Duration,
    sort_duration: Duration,
    prepare_wait_duration: Duration,
    parquet_setup_duration: Duration,
    parquet_encode_duration: Duration,
    parquet_append_duration: Duration,
    parquet_close_duration: Duration,
    record_batches: usize,
    rows: usize,
    row_groups: usize,
}

/// Returns the filename for parquet if provided arrows file path is valid as per our expectation
fn arrow_path_to_parquet(
    stream_staging_path: &Path,
    path: &Path,
    random_string: &str,
) -> Option<PathBuf> {
    let filename = path.file_stem()?.to_str()?;
    let (_, front) = filename.split_once('.')?;
    // Writers may suffix the filename with a per-file ULID after the
    // ".data" marker (ONE_PARQUET_PER_ARROW). Truncate at the marker so
    // the parquet grouping key stays per-minute: with high schema-key
    // churn, keying on the full name made every arrow file its own
    // conversion group (thousands per minute), which exploded conversion
    // parallelism/memory and the object-store file count.
    let Some(idx) = front.rfind(".data") else {
        warn!(
            "Skipping unexpected arrow file without `.data`: {}",
            filename
        );
        return None;
    };
    let front = &front[..idx + ".data".len()];
    let filename_with_random_number = format!("{front}.{random_string}.parquet");
    let mut parquet_path = stream_staging_path.to_owned();
    parquet_path.push(filename_with_random_number);
    Some(parquet_path)
}

#[derive(Debug, thiserror::Error)]
#[error("Stream not found: {0}")]
pub struct StreamNotFound(pub String);

pub type StreamRef = Arc<Stream>;

/// Gets the unix timestamp for the minute as described by the `SystemTime`
fn minute_from_system_time(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .expect("Legitimate time")
        .as_millis()
        / 60000
}

/// What ingest reads from a stream's metadata to stage one request, see
/// `Stream::ingest_snapshot`
pub(crate) struct IngestSnapshot {
    pub time_partition_limit: Option<NonZeroU32>,
    pub static_schema_flag: bool,
    pub custom_partition: Option<String>,
    pub schema_version: SchemaVersion,
    pub schema: Arc<HashMap<String, Arc<Field>>>,
    pub infer_timestamp: bool,
}

/// Whether `cached` holds exactly the entries of `current`, each key mapped to
/// the very same `Arc<Field>`. Allocation-free, and linear only in pointer and
/// short key comparisons.
///
/// A clone of a map iterates in the same order as the map it was cloned from,
/// so an unchanged schema matches pairwise in one pass. Any mutation since then
/// (insert, replace, remove, reassignment) changes the length, an entry, or the
/// order, and reports a mismatch: equal sequences of entries mean equal maps, so
/// a match is never wrong, and a mismatch only costs a fresh clone.
fn same_schema_entries(
    cached: &HashMap<String, Arc<Field>>,
    current: &HashMap<String, Arc<Field>>,
) -> bool {
    if cached.len() != current.len() {
        return false;
    }
    if cached
        .iter()
        .zip(current)
        .all(|((cached_key, cached_field), (key, field))| {
            Arc::ptr_eq(cached_field, field) && cached_key == key
        })
    {
        return true;
    }
    current
        .iter()
        .all(|(key, field)| cached.get(key).is_some_and(|cv| Arc::ptr_eq(cv, field)))
}

#[derive(Default)]
struct WrittenParquetArtifacts {
    parquet_metadata: Option<Arc<parquet::file::metadata::ParquetMetaData>>,
    field_stats: Option<(usize, Vec<crate::storage::field_stats::FieldStat>)>,
}

/// All state associated with a single logstream in Parseable.
pub struct Stream {
    pub stream_name: String,
    pub metadata: RwLock<LogStreamMetadata>,
    pub(super) schema_refresh: super::schema_refresh::SchemaRefresh,
    pub data_path: PathBuf,
    pub options: Arc<Options>,
    /// Staging writers, also accessible directly to library consumers.
    ///
    /// Direct access has historically allowed inspecting and detaching writers.
    /// `Stream::flush` coordinates detachment and sealing with part recovery;
    /// direct writer operations do not acquire that lifecycle lock. Prefer
    /// Stream-managed flushing when recovery or conversion can run concurrently.
    pub writer: Mutex<Writer>,
    /// Held exclusively while a Parquet output is renamed into place and the
    /// pending memory of its sources is released, and shared while a query
    /// captures memory and the staging Parquet set, so each staged row is seen
    /// exactly once. Lock order: `publication`, then `writer`.
    publication: RwLock<()>,
    /// Held shared by `flush` from detaching writers until they are sealed,
    /// and exclusively by part recovery. Recovery therefore never sees a part
    /// that is detached but still owned by a flush, so a part with pending
    /// memory and no attached writer is one whose seal failed. Lock order:
    /// `part_lifecycle`, then `writer`.
    part_lifecycle: RwLock<()>,
    // Last schema durably staged by conversion; ingest writes invalidate it.
    schema_writer: Mutex<Option<Schema>>,
    /// Shared copy of `metadata.schema` last handed out by `ingest_snapshot`,
    /// so ingest requests share one map instead of each deep-cloning it.
    /// Revalidated against the live schema on every use, so it can never be
    /// stale whichever code mutates `metadata`.
    schema_snapshot: RwLock<Arc<HashMap<String, Arc<Field>>>>,
    // Serializes inventory with journal install/removal, without holding claims
    // across a full scan or any durability barrier. Lock order: inventory, claims.
    journal_inventory: Mutex<()>,
    /// Processing directories and temporary Parquet outputs owned by running
    /// conversions for this Stream object. Recovery may hold `part_lifecycle`
    /// when taking this leaf lock; it acquires no other locks while held.
    /// Separate Stream objects, including delete/recreate, do not share it.
    conversion_claims: Arc<Mutex<ConversionClaims>>,
    #[cfg(test)]
    journal_fault: Mutex<Option<conversion_journal::JournalFault>>,
    pub ingestor_id: Option<String>,
    #[cfg(test)]
    conversion_hook: Mutex<Option<ConversionHook>>,
    #[cfg(test)]
    lifecycle_hook: Mutex<Option<LifecycleHook>>,
    #[cfg(test)]
    conversion_clock: Mutex<Option<SystemTime>>,
}

/// Test-only pause points in the part lifecycle, for forcing interleavings.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LifecyclePhase {
    /// `flush` has detached writers and is about to seal them.
    FlushDetached,
    /// Part recovery was entered.
    RecoveryStarted,
    /// Part recovery is waiting for a flush to finish its detached writers.
    RecoveryWaitingForFlush,
    /// Part recovery selected these orphan candidates.
    RecoveryCandidates(Vec<PathBuf>),
    /// Startup preparation recovered parts and is about to flush.
    StartupRecovered,
    /// Part recovery chose this Arrow name and is about to rename an orphan.
    RecoveryPublishing(PathBuf),
    /// A conversion took these output groups and their Arrow sources.
    ConversionClaimed(Vec<(PathBuf, Vec<PathBuf>)>),
    /// A conversion opened readers on these sources for this output.
    ConversionReadersOpened {
        output: PathBuf,
        sources: Vec<PathBuf>,
    },
    /// A temporary Parquet output was created and is still empty.
    ParquetPartCreated(PathBuf),
    /// A temporary Parquet output was written and closed, not yet validated.
    ParquetPartClosed(PathBuf),
    /// A temporary Parquet output is valid and about to be renamed.
    ParquetPartWritten(PathBuf),
}

#[cfg(test)]
pub(crate) type LifecycleHook = Arc<dyn Fn(LifecyclePhase) + Send + Sync>;

/// Staging paths owned by the running conversions of one stream.
///
/// Every conversion registers its processing directories, and so every sealed
/// Arrow source inside them, before it reads them, and its temporary Parquet
/// outputs before it creates them. Periodic and shutdown conversions create
/// and register a fresh directory under the lock before moving root Arrow
/// files into it. Startup and init conversions register, under the lock, only
/// the processing directories nobody owns. Part recovery never deletes a
/// registered output. Entries are released only when their `ConversionClaim`
/// is dropped, after that conversion has stopped mutating the paths. Metric
/// workers open source paths lazily and may outlive the claim on early error;
/// they only read and never mutate staging paths. Ownership is per Stream
/// object; deletion and recreation at the same path are not coordinated here.
#[derive(Default)]
struct ConversionClaims {
    // One Stream/process must own this staging tree. Delete/recreate with old
    // Stream references and multiple processes sharing it are not coordinated.
    journal_dirs: HashMap<PathBuf, usize>,
    known_journals: HashSet<PathBuf>,
    journal_sources: HashSet<PathBuf>,
    inventory_initialized: bool,
    inventory_error: Option<String>,
    retry_attempts: HashMap<PathBuf, usize>,
    retry_dirs: HashSet<PathBuf>,
    journals_blocked: bool,
    // Retry only directories actually skipped at startup, once their journal
    // reservations or inventory gate clear. Unrelated journals do not re-arm it.
    startup_regroup_dirs: HashSet<PathBuf>,
    replay_warned_dirs: HashSet<PathBuf>,
    next_claim: u64,
    dirs: HashMap<PathBuf, u64>,
    // Only atomic directory creation grants this exemption from the inventory
    // gate. It expires with the creating claim, including pre-journal failures.
    // Value counts journals installed by this claim in the directory, so an
    // invalid unrelated inventory cannot prevent collecting our empty dirs.
    fresh_dirs: HashMap<PathBuf, usize>,
    outputs: HashMap<PathBuf, u64>,
}

// A retained pre-journal failure must not encode forever on every sync cycle.
const MAX_CONVERSION_ATTEMPTS: usize = 3;

impl ConversionClaims {
    fn add_journal(&mut self, output: PathBuf, sources: &[PathBuf]) {
        if self.known_journals.insert(output) {
            self.journal_sources.extend(sources.iter().cloned());
            for dir in sources
                .iter()
                .filter_map(|s| s.parent())
                .collect::<HashSet<_>>()
            {
                *self.journal_dirs.entry(dir.to_path_buf()).or_default() += 1;
            }
        }
    }

    fn remove_journal(&mut self, output: &Path, sources: &[PathBuf]) {
        if self.known_journals.remove(output) {
            for source in sources {
                self.journal_sources.remove(source);
            }
            for dir in sources
                .iter()
                .filter_map(|s| s.parent())
                .collect::<HashSet<_>>()
            {
                if let Some(count) = self.journal_dirs.get_mut(dir) {
                    *count -= 1;
                    if *count == 0 {
                        self.journal_dirs.remove(dir);
                    }
                }
            }
        }
    }

    fn forget_conversion_dir(&mut self, dir: &Path) {
        self.retry_dirs.remove(dir);
        self.startup_regroup_dirs.remove(dir);
        self.retry_attempts.remove(dir);
        self.replay_warned_dirs.remove(dir);
    }
}

/// Ownership of the staging paths registered by one conversion. Dropping it
/// releases exactly those entries that are still registered to this claim.
struct ConversionClaim {
    claims: Arc<Mutex<ConversionClaims>>,
    id: u64,
    dirs: Vec<PathBuf>,
    outputs: Vec<PathBuf>,
}

impl ConversionClaim {
    fn new(claims: &Arc<Mutex<ConversionClaims>>) -> Self {
        let id = {
            let mut registry = lock_conversion_claims(claims);
            registry.next_claim += 1;
            registry.next_claim
        };
        Self {
            claims: Arc::clone(claims),
            id,
            dirs: Vec::new(),
            outputs: Vec::new(),
        }
    }
}

impl Drop for ConversionClaim {
    fn drop(&mut self) {
        let mut registry = lock_conversion_claims(&self.claims);
        for dir in &self.dirs {
            if registry.dirs.get(dir) == Some(&self.id) {
                registry.dirs.remove(dir);
                registry.fresh_dirs.remove(dir);
            }
        }
        for output in &self.outputs {
            if registry.outputs.get(output) == Some(&self.id) {
                registry.outputs.remove(output);
            }
        }
    }
}

/// The registry holds only path ownership, and every update to it is a
/// single map operation, so a panic elsewhere cannot leave it inconsistent.
fn lock_conversion_claims(
    claims: &Mutex<ConversionClaims>,
) -> std::sync::MutexGuard<'_, ConversionClaims> {
    claims.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The temporary file a Parquet output is written to before its rename.
fn parquet_part_path(parquet_path: &Path) -> PathBuf {
    let mut part_path = parquet_path.to_path_buf();
    part_path.add_extension(PART_FILE_EXTENSION);
    part_path
}

#[allow(dead_code)]
enum ConversionPhase<'a> {
    /// A detached part is finished and about to be renamed to this `.arrows` path.
    BeforeSealRename(&'a Path),
    /// The Parquet part is complete; publication has not started.
    BeforePublish(&'a Path),
    /// The Parquet file is visible; its sources' memory is not yet released.
    Published,
    /// Valid, synced part; schema and journal installation have not started.
    PartSynced,
    SchemaStaged,
    InventoryScan,
    /// Tmp journal fsynced outside the registry lock; rename has not happened.
    JournalTmpSynced,
    /// Journal and part installed; publication has not started (also resolver).
    JournalInstalled,
    /// Rename and pending release complete; no unlink yet.
    AfterPublish,
    SourceRemoved {
        source: &'a Path,
        ordinal: usize,
    },
    /// All sources absent and their directories synced; J still exists.
    SourcesRetired,
}

#[cfg(test)]
type ConversionHook = Arc<dyn Fn(ConversionPhase<'_>) + Send + Sync>;

/// A startup conversion of the processing directories that no running
/// conversion owned when it was prepared. The plan owns those directories and
/// its outputs until it has been executed or dropped, so periodic, shutdown
/// and init conversions can run meanwhile without touching them.
pub(crate) struct StartupSyncPlan {
    stream: StreamRef,
    tenant_id: Option<String>,
    staging_files: Vec<(PathBuf, Vec<PathBuf>)>,
    claim: ConversionClaim,
}

#[derive(Default)]
struct ArrowFileConversionOutcome {
    schemas: Vec<Schema>,
    first_error: Option<StagingError>,
    /// The conversion's claim, released once the outcome is consumed, after
    /// the schema is staged. Callers attach it after converting.
    claim: Option<ConversionClaim>,
}

#[derive(Clone, Copy)]
enum ArrowGroupExecution {
    Sequential,
    Parallel,
}

enum ConversionRecordReader {
    Forward(MergedForwardRecordReader),
    Reverse(MergedReverseRecordReader),
}

type MetricArrowReadResult = Option<Result<RecordBatch, ArrowError>>;

struct MetricForwardReaderLane {
    reader: Arc<Mutex<ForwardStreamReader>>,
    sender: std::sync::mpsc::SyncSender<MetricArrowReadResult>,
    receiver: std::sync::mpsc::Receiver<MetricArrowReadResult>,
    remaining: usize,
}

struct ActiveMetricForwardFile {
    lanes: Vec<MetricForwardReaderLane>,
    next_batch: usize,
    total_batches: usize,
}

impl ActiveMetricForwardFile {
    fn try_new(plan: ForwardFilePlan, readers_per_file: usize) -> Result<Self, ArrowError> {
        let readers = plan
            .open_lanes(readers_per_file)
            .map_err(|err| ArrowError::IoError(err.to_string(), err))?;
        let total_batches = readers.iter().map(|lane| lane.record_batches).sum();
        let lanes = readers
            .into_iter()
            .map(|lane| {
                let reader = Arc::new(Mutex::new(lane.reader));
                let (sender, receiver) = std::sync::mpsc::sync_channel(1);
                Self::schedule_read(Arc::clone(&reader), sender.clone());
                MetricForwardReaderLane {
                    reader,
                    sender,
                    receiver,
                    remaining: lane.record_batches,
                }
            })
            .collect();
        Ok(Self {
            lanes,
            next_batch: 0,
            total_batches,
        })
    }

    fn schedule_read(
        reader: Arc<Mutex<ForwardStreamReader>>,
        sender: std::sync::mpsc::SyncSender<MetricArrowReadResult>,
    ) {
        METRIC_PARQUET_THREADPOOL.spawn(move || {
            let result = match reader.lock() {
                Ok(mut reader) => reader.next(),
                Err(err) => Some(Err(ArrowError::IpcError(format!(
                    "Metric Arrow reader lock poisoned: {err}"
                )))),
            };
            let _ = sender.send(result);
        });
    }

    fn next_batch(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        if self.next_batch == self.total_batches {
            return None;
        }

        let lane_index = self.next_batch % self.lanes.len();
        let lane = &mut self.lanes[lane_index];
        let result = match lane.receiver.recv() {
            Ok(Some(result)) => result,
            Ok(None) => Err(ArrowError::IpcError(format!(
                "Metric Arrow reader lane {lane_index} ended before all assigned batches"
            ))),
            Err(err) => Err(ArrowError::IpcError(format!(
                "Metric Arrow reader lane {lane_index} failed: {err}"
            ))),
        };
        self.next_batch += 1;
        lane.remaining -= 1;
        if result.is_ok() && lane.remaining > 0 {
            Self::schedule_read(Arc::clone(&lane.reader), lane.sender.clone());
        }
        Some(result)
    }
}

enum MetricForwardFileState {
    Ready(ActiveMetricForwardFile),
    Failed(ArrowError),
}

struct QueuedMetricForwardFile {
    reader_lanes: usize,
    state: MetricForwardFileState,
}

struct MetricForwardRecordIterator {
    plans: std::vec::IntoIter<ForwardFilePlan>,
    active_files: VecDeque<QueuedMetricForwardFile>,
    readers_in_flight: usize,
    setup_error_queued: bool,
    schema: Arc<Schema>,
}

impl MetricForwardRecordIterator {
    fn new(reader: MergedForwardRecordReader, schema: Arc<Schema>) -> Self {
        Self {
            plans: reader.into_file_plans(),
            active_files: VecDeque::new(),
            readers_in_flight: 0,
            setup_error_queued: false,
            schema,
        }
    }

    /// Opens files in source order until the shared reader-lane budget is
    /// exhausted. One-batch files therefore decode concurrently, while a
    /// large multi-batch file can use several lanes without increasing the
    /// maximum number of decoded batches retained in memory.
    fn fill_reader_budget(&mut self) {
        if self.setup_error_queued {
            return;
        }

        let total_limit = *METRIC_ARROW_READERS_IN_FLIGHT;
        let per_file_limit = (*METRIC_ARROW_READERS_PER_FILE).min(total_limit);
        while let Some(plan) = self.plans.as_slice().first() {
            let reader_lanes = plan.reader_lane_count(per_file_limit);
            if !self.active_files.is_empty() && self.readers_in_flight + reader_lanes > total_limit
            {
                break;
            }

            let plan = self.plans.next().expect("peeked Arrow file plan");
            match ActiveMetricForwardFile::try_new(plan, per_file_limit) {
                Ok(active) => {
                    let reader_lanes = active.lanes.len();
                    self.readers_in_flight += reader_lanes;
                    self.active_files.push_back(QueuedMetricForwardFile {
                        reader_lanes,
                        state: MetricForwardFileState::Ready(active),
                    });
                }
                Err(err) => {
                    self.setup_error_queued = true;
                    self.active_files.push_back(QueuedMetricForwardFile {
                        reader_lanes: 0,
                        state: MetricForwardFileState::Failed(err),
                    });
                    break;
                }
            }
        }
    }
}

impl Iterator for MetricForwardRecordIterator {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.fill_reader_budget();
            let queued = self.active_files.front_mut()?;
            match &mut queued.state {
                MetricForwardFileState::Ready(active) => {
                    if let Some(batch) = active.next_batch() {
                        return Some(batch.map(|batch| adapt_batch(self.schema.clone(), &batch)));
                    }
                }
                MetricForwardFileState::Failed(_) => {}
            }

            let queued = self
                .active_files
                .pop_front()
                .expect("front metric Arrow file");
            match queued.state {
                MetricForwardFileState::Ready(_) => {
                    self.readers_in_flight -= queued.reader_lanes;
                }
                MetricForwardFileState::Failed(err) => {
                    self.setup_error_queued = false;
                    return Some(Err(err));
                }
            }
        }
    }
}

impl ConversionRecordReader {
    fn for_stream(file_paths: &[PathBuf], forward: bool) -> Self {
        if forward {
            Self::Forward(MergedForwardRecordReader::try_new(file_paths))
        } else {
            Self::Reverse(MergedReverseRecordReader::try_new(file_paths))
        }
    }

    fn reader_count(&self) -> usize {
        match self {
            Self::Forward(reader) => reader.file_count(),
            Self::Reverse(reader) => reader.readers.len(),
        }
    }

    fn readable_files(&self) -> &[PathBuf] {
        match self {
            Self::Forward(reader) => &reader.readable_files,
            Self::Reverse(reader) => &reader.readable_files,
        }
    }

    fn invalid_files(&self) -> &[PathBuf] {
        match self {
            Self::Forward(reader) => &reader.invalid_files,
            Self::Reverse(reader) => &reader.invalid_files,
        }
    }

    fn merged_schema(&self) -> Schema {
        match self {
            Self::Forward(reader) => reader.merged_schema(),
            Self::Reverse(reader) => reader.merged_schema(),
        }
    }

    fn merged_iter(
        self,
        schema: Arc<Schema>,
        time_partition: Option<String>,
    ) -> Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + Send> {
        match self {
            Self::Forward(reader) => Box::new(MetricForwardRecordIterator::new(reader, schema)),
            Self::Reverse(reader) => Box::new(reader.merged_iter(schema, time_partition)),
        }
    }
}

impl ArrowFileConversionOutcome {
    fn into_result(self) -> Result<Option<Schema>, StagingError> {
        let Self {
            schemas,
            first_error,
            claim,
        } = self;
        drop(claim);
        if let Some(err) = first_error {
            Err(err)
        } else if schemas.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Schema::try_merge(schemas)?))
        }
    }
}

impl StartupSyncPlan {
    pub(crate) fn execute(self) -> Result<(), StagingError> {
        let Self {
            stream,
            tenant_id,
            staging_files,
            claim,
        } = self;
        let time_partition = stream.get_time_partition();
        let custom_partition = stream.get_custom_partition();
        let mut outcome = stream.convert_arrow_file_groups_to_parquet(
            staging_files,
            time_partition.as_ref(),
            custom_partition.as_ref(),
            &tenant_id,
            ArrowGroupExecution::Sequential,
        )?;
        outcome.claim = Some(claim);
        stream.finish_arrow_file_conversion(outcome)
    }
}

impl Stream {
    pub fn new(
        options: Arc<Options>,
        stream_name: impl Into<String>,
        metadata: LogStreamMetadata,
        ingestor_id: Option<String>,
        tenant_id: &Option<String>,
    ) -> StreamRef {
        let stream_name = stream_name.into();
        let data_path = options.local_stream_data_path(&stream_name, tenant_id);

        Arc::new(Self {
            stream_name: stream_name.clone(),
            metadata: RwLock::new(metadata),
            schema_refresh: Default::default(),
            data_path,
            options,
            writer: Mutex::new(Writer::default()),
            publication: RwLock::new(()),
            part_lifecycle: RwLock::new(()),
            schema_writer: Mutex::new(None),
            schema_snapshot: RwLock::default(),
            journal_inventory: Mutex::new(()),
            conversion_claims: Arc::default(),
            #[cfg(test)]
            journal_fault: Mutex::default(),
            ingestor_id,
            #[cfg(test)]
            lifecycle_hook: Mutex::new(None),
            #[cfg(test)]
            conversion_clock: Mutex::new(None),
            #[cfg(test)]
            conversion_hook: Mutex::new(None),
        })
    }

    #[cfg(test)]
    fn lifecycle_phase(&self, phase: LifecyclePhase) {
        let hook = self.lifecycle_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(phase);
        }
    }

    // Concatenates record batches and puts them in memory store for each event.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn push(
        &self,
        schema_key: &str,
        record: &RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: StreamType,
    ) -> Result<(), StagingError> {
        let _span = info_span!(
            "stream_push",
            stream_name = %self.stream_name,
            num_rows = record.num_rows(),
        )
        .entered();

        let mut guard = {
            let _lock_span = info_span!("acquire_writer_lock").entered();
            match self.writer.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!(
                        "Writer lock poisoned while ingesting data for stream {}",
                        self.stream_name
                    );

                    return Err(StagingError::PoisonError(PoisonError::new(format!(
                        "Writer lock poisoned while ingesting data for stream {} - {}",
                        self.stream_name, poisoned
                    ))));
                }
            }
        };
        if self.options.mode != Mode::Query || stream_type == StreamType::Internal {
            let filename =
                self.filename_by_partition(schema_key, parsed_timestamp, custom_partition_values);
            let range = TimeRange::granularity_range(
                parsed_timestamp.and_local_timezone(Utc).unwrap(),
                OBJECT_STORE_DATA_GRANULARITY,
            );
            let file_path = self.data_path.join(&filename);

            guard.push_disk_ref(&filename, record, file_path, range)?;
            if let Some(mem) = guard.mem.as_mut() {
                mem.push_for_file(&filename, schema_key, record)?;
            }
        } else if let Some(mem) = guard.mem.as_mut() {
            mem.push(schema_key, record)?;
        }

        Ok(())
    }

    pub fn filename_by_partition(
        &self,
        stream_hash: &str,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
    ) -> String {
        let mut hostname = HOSTNAME
            .get_or_init(|| {
                hostname::get()
                    .unwrap_or_else(|_| std::ffi::OsString::from(&Ulid::new().to_string()))
                    .into_string()
                    .unwrap_or_else(|_| Ulid::new().to_string())
                    .matches(|c: char| c.is_alphanumeric() || c == '-' || c == '_')
                    .collect::<String>()
            })
            .clone();

        if let Some(id) = &self.ingestor_id {
            hostname.push_str(id);
        }
        format!(
            "{stream_hash}.date={}.hour={:02}.minute={}.{}{hostname}.data.{ARROW_FILE_EXTENSION}",
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
            custom_partition_values
                .iter()
                .sorted_by_key(|v| v.0)
                .map(|(key, value)| format!("{key}={value}."))
                .join("")
        )
    }

    pub fn arrow_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        Self::order_arrow_files(
            dir.flatten()
                .map(|file| file.path())
                .filter(|file| {
                    file.extension()
                        .is_some_and(|ext| ext.eq(ARROW_FILE_EXTENSION))
                })
                .collect(),
        )
    }

    fn order_arrow_files(files: Vec<PathBuf>) -> Vec<PathBuf> {
        files
            .into_iter()
            .filter_map(|f| {
                let modified = f.metadata().ok().and_then(|m| m.modified().ok());
                modified.map(|modified_time| (f, modified_time))
            })
            .sorted_by_key(|(_, modified_time)| *modified_time)
            .map(|(f, _)| f)
            .collect()
    }

    /// Lists Arrow files in every processing directory. This is a read-only
    /// view; converting them requires claiming their directories first.
    pub fn inprocess_arrow_files(&self) -> Vec<PathBuf> {
        Self::arrow_files_in_dirs(&self.inprocess_dirs())
    }

    fn inprocess_dirs(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        dir.filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.is_dir()
                && path
                    .file_name()?
                    .to_str()?
                    .starts_with(INPROCESS_DIR_PREFIX)
            {
                Some(path)
            } else {
                None
            }
        })
        .collect()
    }

    fn arrow_files_in_dirs(dirs: &[PathBuf]) -> Vec<PathBuf> {
        dirs.iter()
            .flat_map(|dir| {
                fs::read_dir(dir)
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.ok().map(|file| file.path()))
            })
            .filter(|file| {
                file.extension()
                    .is_some_and(|ext| ext.eq(ARROW_FILE_EXTENSION))
            })
            .collect()
    }

    /// Groups sealed Arrow files by their prospective Parquet output.
    ///
    /// As in the legacy behavior, non-init calls move eligible root files into
    /// a fresh processing directory before grouping them: files from minutes
    /// before `exclude` are selected, or all root Arrow files on shutdown.
    /// Existing validation removes invalid Arrow files. Init leaves root files
    /// in place and groups existing processing directories, now excluding those
    /// with live conversion claims at acquisition under the registry lock.
    ///
    /// This is a concurrency-aware snapshot, not a read-only operation or a
    /// reservation for the caller. The temporary claim is released before return;
    /// the returned groups are unowned, and concurrent conversion can subsequently
    /// move or remove their files. Prefer Stream-managed flushing/conversion,
    /// whose internal claimed conversion paths retain ownership through mutation.
    /// Snapshots do not consume runtime retries or alter deferred startup regroup.
    /// Non-init snapshots select root files only, leaving retries to conversion.
    pub fn arrow_files_grouped_exclude_time(
        &self,
        exclude: SystemTime,
        group_minute: u128,
        init_signal: bool,
        shutdown_signal: bool,
    ) -> Vec<(PathBuf, Vec<PathBuf>)> {
        self.claim_conversion_groups_inner(
            exclude,
            group_minute,
            init_signal,
            shutdown_signal,
            false,
            None,
        )
        .1
    }

    /// Claims sealed Arrow files for one conversion and groups them by output.
    ///
    /// Init claims every processing directory that no running conversion owns
    /// and leaves root Arrow files to regular sync, so it never races a live
    /// writer or a periodic cycle for the same file. Otherwise root Arrow files
    /// from before the current minute (all of them on shutdown) are moved into
    /// a newly claimed processing directory. The outputs are claimed as well,
    /// and everything stays owned until the returned claim is dropped.
    fn claim_conversion_groups(
        &self,
        exclude: SystemTime,
        group_minute: u128,
        init_signal: bool,
        shutdown_signal: bool,
    ) -> (ConversionClaim, Vec<(PathBuf, Vec<PathBuf>)>) {
        self.claim_conversion_groups_inner(
            exclude,
            group_minute,
            init_signal,
            shutdown_signal,
            true,
            None,
        )
    }

    fn claim_conversion_groups_inner(
        &self,
        exclude: SystemTime,
        group_minute: u128,
        init_signal: bool,
        shutdown_signal: bool,
        track_recovery: bool,
        root_arrows: Option<Vec<PathBuf>>,
    ) -> (ConversionClaim, Vec<(PathBuf, Vec<PathBuf>)>) {
        let mut claim = ConversionClaim::new(&self.conversion_claims);
        let groups = if init_signal {
            let dirs = self.claim_unowned_inprocess_dirs(&mut claim, track_recovery);
            self.group_arrow_files_in_dirs(&dirs, &Ulid::new().to_string())
        } else {
            let arrow_files =
                self.fetch_arrow_files_for_conversion(exclude, shutdown_signal, root_arrows);
            if arrow_files.is_empty() {
                let dirs = self.claim_retry_inprocess_dirs(&mut claim, track_recovery);
                let groups = self.group_arrow_files_in_dirs(&dirs, &Ulid::new().to_string());
                let groups = self.claim_conversion_outputs(&mut claim, groups);
                return (claim, groups);
            }

            let inprocess_dir = match self.claim_new_inprocess_folder(&mut claim, group_minute) {
                Ok(inprocess_dir) => inprocess_dir,
                Err(e) => {
                    error!("Failed to create inprocess directory: {e}");
                    return (claim, Vec::new());
                }
            };
            self.move_arrow_files(arrow_files, &inprocess_dir);
            let groups =
                self.group_single_inprocess_arrow_files(&inprocess_dir, &Ulid::new().to_string());
            if groups.is_empty()
                && let Err(err) = fs::remove_dir(&inprocess_dir)
                && err.kind() != std::io::ErrorKind::NotFound
            {
                warn!(
                    "Failed to remove empty processing directory {}: {err}",
                    inprocess_dir.display()
                );
            }
            groups
        };
        let mut groups = groups;
        if !init_signal {
            let dirs = self.claim_retry_inprocess_dirs(&mut claim, track_recovery);
            groups.extend(self.group_arrow_files_in_dirs(&dirs, &Ulid::new().to_string()));
        }
        let groups = self.claim_conversion_outputs(&mut claim, groups);
        (claim, groups)
    }

    /// Claims every existing processing directory that no running conversion
    /// owns. A listed directory is checked under the registry lock before
    /// claiming, so a concurrently created directory is already registered.
    fn claim_unowned_inprocess_dirs(
        &self,
        claim: &mut ConversionClaim,
        track_recovery: bool,
    ) -> Vec<PathBuf> {
        self.refresh_conversion_inventory_if_needed();
        let dirs = self.inprocess_dirs();
        let mut registry = lock_conversion_claims(&self.conversion_claims);
        if track_recovery {
            let unowned = dirs
                .iter()
                .filter(|dir| !registry.dirs.contains_key(*dir))
                .cloned()
                .collect::<Vec<_>>();
            registry.startup_regroup_dirs.extend(unowned);
        }
        if registry.journals_blocked {
            return Vec::new();
        }
        self.claim_available_inprocess_dirs(&mut registry, claim, dirs, track_recovery, true)
    }

    /// Remember startup candidates before recovery/flush can return early.
    fn defer_unowned_inprocess_dirs(&self) {
        let dirs = self.inprocess_dirs();
        let mut registry = lock_conversion_claims(&self.conversion_claims);
        let dirs = dirs
            .into_iter()
            .filter(|dir| !registry.dirs.contains_key(dir))
            .collect::<Vec<_>>();
        registry.startup_regroup_dirs.extend(dirs);
    }

    /// Claim only explicit runtime retries and the specific startup directories
    /// previously held behind recovery. Snapshot callers leave both sets alone.
    fn claim_retry_inprocess_dirs(
        &self,
        claim: &mut ConversionClaim,
        track_recovery: bool,
    ) -> Vec<PathBuf> {
        if !track_recovery {
            return Vec::new();
        }
        {
            let registry = lock_conversion_claims(&self.conversion_claims);
            if registry.retry_dirs.is_empty() && registry.startup_regroup_dirs.is_empty() {
                return Vec::new();
            }
        }
        self.refresh_conversion_inventory_if_needed();
        let mut registry = lock_conversion_claims(&self.conversion_claims);
        if registry.journals_blocked {
            return Vec::new();
        }
        let dirs = registry
            .retry_dirs
            .union(&registry.startup_regroup_dirs)
            .cloned()
            .collect();
        self.claim_available_inprocess_dirs(&mut registry, claim, dirs, true, false)
    }

    // Called under the registry lock; remove_dir only collects empty directories
    // with no journal reservation and no other claim, including retirement residue.
    fn claim_available_inprocess_dirs(
        &self,
        registry: &mut ConversionClaims,
        claim: &mut ConversionClaim,
        dirs: Vec<PathBuf>,
        track_recovery: bool,
        startup: bool,
    ) -> Vec<PathBuf> {
        let mut claimed = Vec::new();
        for dir in dirs {
            if registry.dirs.contains_key(&dir) || registry.journal_dirs.contains_key(&dir) {
                continue;
            }
            if track_recovery {
                let deferred = registry.startup_regroup_dirs.remove(&dir);
                registry.retry_dirs.remove(&dir);
                match fs::remove_dir(&dir) {
                    Ok(()) => {
                        registry.forget_conversion_dir(&dir);
                        continue;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        registry.forget_conversion_dir(&dir);
                        continue;
                    }
                    Err(_) => {}
                }
                if registry.retry_attempts.get(&dir).copied().unwrap_or(0)
                    >= MAX_CONVERSION_ATTEMPTS
                {
                    // Keep only a cheap empty/missing-dir cleanup obligation.
                    registry.retry_dirs.insert(dir.clone());
                    continue;
                }
                if (startup || deferred) && registry.replay_warned_dirs.insert(dir.clone()) {
                    warn!(
                        "Replaying processing directory without journals for stream {}: {}; an interrupted conversion that never published is safe to retry; staging from a pre-journal version may duplicate rows",
                        self.stream_name,
                        dir.display()
                    );
                }
            }
            registry.dirs.insert(dir.clone(), claim.id);
            claim.dirs.push(dir.clone());
            claimed.push(dir);
        }
        claimed
    }

    /// Creates a fresh processing directory owned by `claim`.
    fn claim_new_inprocess_folder(
        &self,
        claim: &mut ConversionClaim,
        minute: u128,
    ) -> std::io::Result<PathBuf> {
        self.refresh_conversion_inventory_if_needed();
        let mut registry = lock_conversion_claims(&self.conversion_claims);
        let dir = self.create_inprocess_folder(minute, &registry)?;
        registry.dirs.insert(dir.clone(), claim.id);
        registry.fresh_dirs.insert(dir.clone(), 0);
        claim.dirs.push(dir.clone());
        Ok(dir)
    }

    /// Registers the temporary output of every group before it is created.
    /// Output names embed a fresh ULID, so another claim never holds one; a
    /// group whose output is somehow owned already is left for a later retry.
    fn claim_conversion_outputs(
        &self,
        claim: &mut ConversionClaim,
        groups: Vec<(PathBuf, Vec<PathBuf>)>,
    ) -> Vec<(PathBuf, Vec<PathBuf>)> {
        let mut registry = lock_conversion_claims(&self.conversion_claims);
        groups
            .into_iter()
            .filter(|(parquet_path, _)| {
                let part_path = parquet_part_path(parquet_path);
                if registry.outputs.contains_key(&part_path) {
                    error!(
                        "Temporary Parquet output {} is owned by another conversion of stream {}; retaining its sources",
                        part_path.display(),
                        self.stream_name
                    );
                    return false;
                }
                registry.outputs.insert(part_path.clone(), claim.id);
                claim.outputs.push(part_path);
                true
            })
            .collect()
    }

    /// Groups arrow files only from the specified inprocess folder
    fn group_single_inprocess_arrow_files(
        &self,
        inprocess_dir: &Path,
        random_string: &str,
    ) -> Vec<(PathBuf, Vec<PathBuf>)> {
        let mut grouped: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let Ok(dir) = fs::read_dir(inprocess_dir) else {
            return Vec::new();
        };
        for entry in dir.flatten() {
            let path = entry.path();
            if path
                .extension()
                .is_some_and(|ext| ext.eq(ARROW_FILE_EXTENSION))
            {
                if let Some(parquet_path) =
                    arrow_path_to_parquet(&self.data_path, &path, random_string)
                {
                    grouped.entry(parquet_path).or_default().push(path);
                } else {
                    warn!("Unexpected arrow file: {}", path.display());
                }
            }
        }
        ordered_arrow_file_groups(grouped)
    }

    /// Groups the files of every existing processing directory, owned or not.
    #[cfg(test)]
    fn group_inprocess_arrow_files(&self, random_string: &str) -> Vec<(PathBuf, Vec<PathBuf>)> {
        self.group_arrow_files_in_dirs(&self.inprocess_dirs(), random_string)
    }

    /// Takes one logical snapshot across the given processing directories.
    /// Files sharing the same event-minute/output prefix are consolidated even
    /// when a crash left them in different processing directories.
    fn group_arrow_files_in_dirs(
        &self,
        dirs: &[PathBuf],
        random_string: &str,
    ) -> Vec<(PathBuf, Vec<PathBuf>)> {
        let mut grouped: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for inprocess_file in Self::arrow_files_in_dirs(dirs) {
            if let Some(parquet_path) =
                arrow_path_to_parquet(&self.data_path, &inprocess_file, random_string)
            {
                grouped
                    .entry(parquet_path)
                    .or_default()
                    .push(inprocess_file);
            } else {
                warn!("Unexpected arrow file: {}", inprocess_file.display());
            }
        }
        ordered_arrow_file_groups(grouped)
    }

    /// Returns arrow files for conversion, filtering by time and removing invalid files.
    fn fetch_arrow_files_for_conversion(
        &self,
        exclude: SystemTime,
        shutdown_signal: bool,
        root_arrows: Option<Vec<PathBuf>>,
    ) -> Vec<PathBuf> {
        let mut arrow_files =
            root_arrows.map_or_else(|| self.arrow_files(), Self::order_arrow_files);
        if !shutdown_signal {
            arrow_files.retain(|path| {
                path.metadata()
                    .ok()
                    .and_then(|meta| meta.created().or_else(|_| meta.modified()).ok())
                    .is_some_and(|creation| {
                        // Compare if creation time is actually from previous minute
                        minute_from_system_time(creation) < minute_from_system_time(exclude)
                    })
            });
        }
        arrow_files
    }

    /// Moves eligible arrow files to the inprocess folder and groups them by parquet path.
    fn move_arrow_files(&self, arrow_files: Vec<PathBuf>, inprocess_dir: &Path) {
        let mut moves = Vec::with_capacity(arrow_files.len());
        for arrow_file_path in arrow_files {
            match arrow_file_path.metadata() {
                Ok(meta) if meta.len() == 0 => {
                    error!(
                        "Invalid arrow file {:?} detected for stream {}, removing it",
                        &arrow_file_path, self.stream_name
                    );
                    self.release_pending(std::slice::from_ref(&arrow_file_path));
                    remove_file(&arrow_file_path).expect("File should be removed");
                }
                Ok(_) => {
                    let new_path = inprocess_dir.join(
                        arrow_file_path
                            .file_name()
                            .expect("Arrow file should have a name"),
                    );
                    moves.push((arrow_file_path, new_path));
                }
                Err(e) => {
                    warn!(
                        "Could not get metadata for arrow file {}: {e}",
                        arrow_file_path.display()
                    );
                }
            }
        }

        // Pending rows follow their file before conversion can observe the move.
        self.rekey_pending(moves.iter().map(|(from, to)| (from, to)));
        let mut failed = Vec::new();
        for (arrow_file_path, new_path) in moves {
            if let Err(e) = fs::rename(&arrow_file_path, &new_path) {
                error!(
                    "Failed to rename arrow file to inprocess directory: {} -> {}: {e}",
                    arrow_file_path.display(),
                    new_path.display()
                );
                failed.push((new_path, arrow_file_path));
            }
        }
        self.rekey_pending(failed.iter().map(|(from, to)| (from, to)));
    }

    fn inprocess_folder(base: &Path, minute: u128) -> PathBuf {
        base.join(format!("{INPROCESS_DIR_PREFIX}{minute}"))
    }

    /// Atomically reserves a processing directory. Overlapping periodic
    /// cycles therefore never share one directory, even when they start in
    /// the same minute. A claimed name is never reused, even if its owner has
    /// already removed the directory.
    fn create_inprocess_folder(
        &self,
        minute: u128,
        registry: &ConversionClaims,
    ) -> std::io::Result<PathBuf> {
        let preferred = Self::inprocess_folder(&self.data_path, minute);
        if !registry.journals_blocked
            && !registry.dirs.contains_key(&preferred)
            && !registry.journal_dirs.contains_key(&preferred)
        {
            match fs::DirBuilder::new().create(&preferred) {
                Ok(()) => return Ok(preferred),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err),
            }
        }

        loop {
            let unique = self
                .data_path
                .join(format!("{INPROCESS_DIR_PREFIX}{minute}_{}", Ulid::new()));
            if registry.dirs.contains_key(&unique) || registry.journal_dirs.contains_key(&unique) {
                continue;
            }
            match fs::DirBuilder::new().create(&unique) {
                Ok(()) => return Ok(unique),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err),
            }
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn parquet_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        dir.flatten()
            .map(|file| file.path())
            .filter(|file| {
                file.extension().is_some_and(|ext| ext.eq("parquet"))
                    && Self::is_valid_parquet_file(file, &self.stream_name)
            })
            .collect()
    }

    pub fn schema_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        dir.flatten()
            .map(|file| file.path())
            .filter(|file| {
                file.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".schema"))
            })
            .collect()
    }

    pub fn get_schemas_if_present(&self) -> Result<Vec<Schema>, StagingError> {
        let dir = self.data_path.read_dir()?;

        let mut schemas: Vec<Schema> = Vec::new();

        for file in dir.flatten() {
            if file
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".schema"))
            {
                let file = File::open(file.path())?;

                schemas.push(serde_json::from_reader(file)?);
            }
        }

        Ok(schemas)
    }

    /// Converts arrow files in staging into parquet files, does so only for past minutes when run with `!shutdown_signal`
    #[instrument(
        name = "prepare_parquet",
        level = "info",
        skip(self, tenant_id),
        fields(stream_name = %self.stream_name)
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn prepare_parquet(
        &self,
        init_signal: bool,
        shutdown_signal: bool,
        tenant_id: &Option<String>,
    ) -> Result<(), StagingError> {
        info!(
            "Starting arrow_conversion job for stream- {}",
            self.stream_name
        );

        // The sync loop visits every stream, even without Arrow or Parquet
        // files. This is the retry owner; the upload scheduler may skip them.
        // Share the resolver's root listing with periodic Arrow discovery.
        // Files sealed after this snapshot are safely deferred to the next cycle.
        let mut root_arrows = Vec::new();
        let recovery = self.resolve_conversion_journals_with_root(tenant_id, &mut root_arrows);
        self.convert_after_journal_resolution(
            init_signal,
            shutdown_signal,
            tenant_id,
            Some(root_arrows),
            recovery,
        )
    }

    fn convert_after_journal_resolution(
        &self,
        init_signal: bool,
        shutdown_signal: bool,
        tenant_id: &Option<String>,
        root_arrows: Option<Vec<PathBuf>>,
        recovery: Result<(), StagingError>,
    ) -> Result<(), StagingError> {
        let time_partition = self.get_time_partition();
        let custom_partition = self.get_custom_partition();

        // read arrow files on disk
        // convert them to parquet
        let outcome = self.convert_disk_files_to_parquet_outcome(
            time_partition.as_ref(),
            custom_partition.as_ref(),
            init_signal,
            shutdown_signal,
            tenant_id,
            root_arrows,
        )?;
        self.finish_arrow_file_conversion(outcome)?;
        recovery
    }

    /// Ingest staging preserves main's rename-only durability semantics.
    pub fn stage_schema_file(&self, schema: Schema) -> Result<(), StagingError> {
        self.stage_schema_file_inner(schema, false)
    }

    fn stage_conversion_schema(&self, schema: Schema) -> Result<(), StagingError> {
        self.stage_schema_file_inner(schema, true)
    }

    fn stage_schema_file_inner(
        &self,
        mut schema: Schema,
        durable: bool,
    ) -> Result<(), StagingError> {
        let mut staged = self.schema_writer.lock().map_err(|poisoned| {
            StagingError::PoisonError(PoisonError::new(format!(
                "Schema writer lock poisoned while staging schema for stream {} - {}",
                self.stream_name, poisoned
            )))
        })?;
        let file_name = self.ingestor_id.as_ref().map_or_else(
            || ".schema".to_owned(),
            |id| format!(".ingestor.{id}.schema"),
        );
        let path = self.data_path.join(file_name);
        if durable && let Some(previous) = staged.as_ref() {
            // Use Arrow's merge semantics, including nested/null types and
            // metadata. Coverage means merging would add or widen nothing.
            let merged = Schema::try_merge([previous.clone(), schema.clone()])?;
            if &merged == previous && path.try_exists()? {
                return Ok(());
            }
            // The uploader deletes this file after committing its schema
            // remotely. Restage even if covered: retaining the cached union
            // also preserves older fields when a later group adds a field.
            // This does not extend the uploader's compare-and-delete race.
            schema = merged;
        }
        fs::create_dir_all(&self.data_path)?;
        let mut staging_schemas = self.get_schemas_if_present()?;
        staging_schemas.push(schema);
        let schema = Schema::try_merge(staging_schemas)?;
        let tmp_path = path.with_extension("schema.tmp");
        info!("writing schema to path - {path:?}");
        if durable {
            self.fail_journal_io(conversion_journal::JournalFault::Schema)?;
        }
        // A failed ingest replacement must never leave a stale durable cache.
        *staged = None;
        write(&tmp_path, to_bytes(&schema))?;
        if durable {
            File::open(&tmp_path)?.sync_all()?;
        }
        fs::rename(tmp_path, path)?;
        if durable {
            // No directory sync here: the staged schema lives in the stream
            // root, and every journal install syncs that same directory before
            // any source can be unlinked, which also persists this rename.
            *staged = Some(schema);
            self.conversion_phase(ConversionPhase::SchemaStaged);
        }
        Ok(())
    }

    pub fn recordbatches_cloned(
        &self,
        schema: &Arc<Schema>,
    ) -> Result<Vec<RecordBatch>, StagingError> {
        let mut writer = self.writer.lock().map_err(|poisoned| {
            StagingError::PoisonError(PoisonError::new(format!(
                "Writer lock poisoned while cloning record batches for stream {} - {}",
                self.stream_name, poisoned
            )))
        })?;

        if let Some(mem) = writer.mem.as_mut() {
            mem.recordbatch_cloned(schema)
        } else {
            Ok(Vec::new())
        }
    }

    /// Captures staged rows for one query consistently with Parquet
    /// publication: every staged row is in exactly one of the returned memory
    /// batches or the Parquet files `capture_parquet` finds in the staging
    /// directory. `capture_parquet` delays publication: it may list, lease and
    /// validate file footers, but must leave row scans and query execution
    /// outside the lock.
    ///
    /// Without memory staging (`ENABLE_MEMORY_STAGING=false`) only published
    /// Parquet is captured: unconverted rows are intentionally not queryable.
    pub fn capture_staging<T>(
        &self,
        schema: &Arc<Schema>,
        capture_parquet: impl FnOnce(&Path) -> T,
    ) -> Result<(Vec<RecordBatch>, T), StagingError> {
        // A publication that panicked may have renamed its Parquet without
        // releasing its sources' memory; fail closed rather than double count.
        let _publication = self.publication.read().map_err(|poisoned| {
            StagingError::PoisonError(PoisonError::new(format!(
                "Publication lock poisoned while capturing staging for stream {} - {}",
                self.stream_name, poisoned
            )))
        })?;
        let records = self.recordbatches_cloned(schema)?;
        Ok((records, capture_parquet(&self.data_path)))
    }

    /// Moves pending memory to the new paths of its staging files. A poisoned
    /// writer is still updated: every capture already fails closed on it.
    fn rekey_pending<'a>(&self, moves: impl IntoIterator<Item = (&'a PathBuf, &'a PathBuf)>) {
        let mut moves = moves.into_iter().peekable();
        if moves.peek().is_none() {
            return;
        }
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(mem) = writer.mem.as_mut() {
            for (from, to) in moves {
                mem.rekey_pending(from, to);
            }
        }
    }

    /// Releases pending memory of staging files that were published or discarded.
    fn release_pending(&self, paths: &[PathBuf]) {
        if paths.is_empty() {
            return;
        }
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(mem) = writer.mem.as_mut() {
            mem.release_pending(paths);
        }
    }

    pub fn clear(&self) -> Result<(), StagingError> {
        if let Some(m) = self
            .writer
            .lock()
            .map_err(|poisoned| {
                StagingError::PoisonError(PoisonError::new(format!(
                    "Writer lock poisoned while clearing stream {} - {}",
                    self.stream_name, poisoned
                )))
            })?
            .mem
            .as_mut()
        {
            m.clear()
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn flush(&self, forced: bool) -> Result<(), StagingError> {
        self.flush_with_clock(forced, Utc::now)
    }

    fn flush_with_clock(
        &self,
        forced: bool,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<(), StagingError> {
        let _span = info_span!("flush", stream_name = %self.stream_name, forced).entered();
        // A detached writer owns its part until sealing has renamed it. Keep part
        // recovery out until then; concurrent flushes only share this lock.
        // It guards no data, so poisoning by a panicked recovery is ignored.
        let _lifecycle = self
            .part_lifecycle
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        // Swap out stale writers under the lock, seal them after releasing it.
        // Sealing does I/O (IPC finish + file rename) so sealing
        // outside the lock avoids blocking concurrent push() calls.
        let stale_writers = {
            let mut writer = self.writer.lock().map_err(|poisoned| {
                StagingError::PoisonError(PoisonError::new(format!(
                    "Writer lock poisoned while flushing data for stream {} - {}",
                    self.stream_name, poisoned
                )))
            })?;

            writer.detach_flushable_disk_at(forced, clock())
        };
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::FlushDetached);
        // Sealing I/O happens here, outside the lock
        self.seal_detached(stale_writers);
        Ok(())
    }

    /// Finishes detached writers and renames their parts to `.arrows`. Pending
    /// memory is re-keyed to the final name before the rename makes the file
    /// visible to conversion, and back if the rename fails. A part that cannot
    /// be finished or renamed is left for startup recovery; its rows stay
    /// queryable from memory until then.
    fn seal_detached(&self, detached: Vec<DiskWriter>) {
        let mut sealed = Vec::with_capacity(detached.len());
        for writer in detached {
            match writer.seal() {
                Ok(part) => sealed.push(part),
                Err((part, err)) => error!("Couldn't finish arrow file {part:?}, error = {err}"),
            }
        }
        self.rekey_pending(sealed.iter().map(|s| (&s.part, &s.arrow)));
        let mut failed = Vec::new();
        for part in sealed {
            self.conversion_phase(ConversionPhase::BeforeSealRename(&part.arrow));
            if let Err(err) = fs::rename(&part.part, &part.arrow) {
                error!("Couldn't rename file {:?}, error = {err}", part.part);
                failed.push(part);
            } else {
                info!("flushing {:?}", part.part);
            }
        }
        self.rekey_pending(failed.iter().map(|s| (&s.arrow, &s.part)));
    }

    /// Renames a finished Parquet output into the staging set and releases the
    /// pending memory of its sources as one step with respect to query capture.
    fn publish_parquet(
        &self,
        part_path: &Path,
        parquet_path: &Path,
        sources: &[PathBuf],
    ) -> Result<(), StagingError> {
        self.conversion_phase(ConversionPhase::BeforePublish(parquet_path));
        // The lock covers the rename and the memory release together. If an
        // earlier publication panicked between them, captures fail closed;
        // keep the sources and their memory for retry instead of publishing.
        let _publication = match self.publication.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                return Err(StagingError::PoisonError(PoisonError::new(format!(
                    "Publication lock poisoned while publishing {parquet_path:?} for stream {} - {}",
                    self.stream_name, poisoned
                ))));
            }
        };
        let output_exists = match fs::symlink_metadata(parquet_path) {
            Ok(_) => true,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
            Err(err) => return Err(err.into()),
        };
        if output_exists {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Refusing to overwrite {}", parquet_path.display()),
            )
            .into());
        }
        if let Err(err) = std::fs::rename(part_path, parquet_path) {
            error!("Couldn't rename part file: {part_path:?} -> {parquet_path:?}, error = {err}");
            return Err(err.into());
        }
        self.conversion_phase(ConversionPhase::Published);
        self.release_pending(sources);
        drop(_publication);
        // The part and journal were synced before this rename. If the rename
        // is lost, both remain available for the resolver to publish again.
        self.conversion_phase(ConversionPhase::AfterPublish);
        Ok(())
    }

    #[cfg(test)]
    fn conversion_phase(&self, phase: ConversionPhase<'_>) {
        // Clone out so a hook blocking one group never blocks another's phase.
        let hook = self.conversion_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(phase);
        }
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn conversion_phase(&self, _phase: ConversionPhase<'_>) {}

    pub(crate) fn parquet_writer_props(
        &self,
        merged_schema: &Schema,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
        clustered: bool,
    ) -> WriterProperties {
        // Determine time partition field
        let time_partition_field = time_partition.map_or(DEFAULT_TIMESTAMP_KEY, |tp| tp.as_str());

        // Find time partition index
        let time_partition_idx = merged_schema.index_of(time_partition_field).unwrap_or(0);

        let mut props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(self.options.row_group_size))
            .set_compression(self.options.parquet_compression.into())
            .set_column_encoding(
                ColumnPath::new(vec![time_partition_field.to_string()]),
                Encoding::DELTA_BINARY_PACKED,
            );

        // Build sorting columns. For OTel-metrics streams, put
        // `metric_name` ahead of the time partition so per-page parquet
        // min/max stats can prune by metric (PromQL's universal selector
        // predicate). The actual row order is enforced at write time
        // (`sort_batch_for_metric_pruning`) — this just advertises the
        // sort in parquet footer metadata so readers can rely on it.
        let is_otel_metrics = self.is_otel_metrics();
        let mut sorting_column_vec: Vec<SortingColumn> = Vec::new();
        if is_otel_metrics && let Ok(name_idx) = merged_schema.index_of("metric_name") {
            sorting_column_vec.push(SortingColumn {
                column_idx: name_idx as i32,
                descending: false,
                nulls_first: false,
            });

            // Bloom filter on the same column. The sort already narrows per-page
            // min/max, but that only prunes pages within a row group the reader
            // has opened; a row group whose metric range merely brackets the
            // queried name still gets read. The bloom answers membership exactly,
            // so a row group that never saw the metric is rejected outright.
            let column_path = ColumnPath::new(vec!["metric_name".to_string()]);
            let bloom_filter_position = if self.options.bloom_filter_default_position {
                BloomFilterPosition::AfterRowGroup
            } else {
                BloomFilterPosition::End
            };
            props = props
                .set_column_bloom_filter_enabled(column_path.clone(), true)
                .set_column_bloom_filter_max_ndv(column_path, METRIC_NAME_BLOOM_FILTER_NDV)
                .set_bloom_filter_position(bloom_filter_position);
        }
        // Keep metrics' existing filters and position independent of this opt-in.
        // Parquet 59 folds each filter at close, reducing short/low-NDV groups.
        if !is_otel_metrics && !self.options.parquet_bloom_filter_columns.is_empty() {
            for name in &self.options.parquet_bloom_filter_columns {
                // "a, b" is a natural way to write the list
                let name = name.trim();
                if merged_schema.index_of(name).is_ok() {
                    let path = ColumnPath::new(vec![name.to_owned()]);
                    props = props
                        .set_column_bloom_filter_enabled(path.clone(), true)
                        .set_column_bloom_filter_max_ndv(
                            path.clone(),
                            self.options.row_group_size as u64,
                        )
                        .set_column_bloom_filter_fpp(path, 0.01);
                }
            }
            // A filter is allocated at its full size for every selected column of
            // every row group, and with `End` the writer keeps all of them until
            // the file is closed. Writing them after each row group releases them
            // as it goes, so that is the default.
            props = props.set_bloom_filter_position(
                if self.options.parquet_bloom_filter_position == "end" {
                    BloomFilterPosition::End
                } else {
                    BloomFilterPosition::AfterRowGroup
                },
            );
        }
        sorting_column_vec.push(SortingColumn {
            column_idx: time_partition_idx as i32,
            descending: true,
            nulls_first: false,
        });

        // Describe custom partition column encodings and sorting
        if let Some(custom_partition) = custom_partition {
            for partition in custom_partition.split(',') {
                if let Ok(idx) = merged_schema.index_of(partition) {
                    let column_path = ColumnPath::new(vec![partition.to_string()]);
                    props = props.set_column_encoding(column_path, Encoding::DELTA_BYTE_ARRAY);

                    sorting_column_vec.push(SortingColumn {
                        column_idx: idx as i32,
                        descending: true,
                        nulls_first: true,
                    });
                }
            }
        }

        // A clustered log file is ordered by the cluster column inside each
        // row group, not by time across the file. Declare no order at all
        // rather than an unverified or file-level-misleading claim.
        if clustered {
            return props.set_sorting_columns(None).build();
        }

        // Set sorting columns
        props.set_sorting_columns(Some(sorting_column_vec)).build()
    }

    /// Chooses the opt-in severity clustering key for one log conversion
    /// output, or `None` to keep the existing writer behaviour.
    fn log_cluster_key(
        &self,
        merged_schema: &Schema,
        time_partition: Option<&String>,
    ) -> Option<LogClusterKey> {
        let configured = self.options.log_severity_cluster_column.as_deref();
        configured?;
        let applicable = {
            let metadata = self.metadata.read().expect(LOCK_EXPECT);
            log_clustering::stream_is_applicable(
                metadata.stream_type,
                metadata.telemetry_type,
                metadata
                    .log_source
                    .iter()
                    .map(|entry| entry.log_source_format.clone()),
            )
        };
        let _span =
            tracing::warn_span!("log_cluster_key", stream_name = %self.stream_name).entered();
        match log_clustering::resolve_cluster_key(
            configured,
            applicable,
            merged_schema,
            time_partition.map(String::as_str),
        ) {
            Ok(key) => Some(key),
            Err(skip) => {
                debug!(
                    stream_name = %self.stream_name,
                    ?skip,
                    "Log severity clustering not applied"
                );
                None
            }
        }
    }

    /// True if this stream's log_source carries the OTel-metrics
    /// format. Determines whether per-batch sort and metric_name-first
    /// SortingColumn metadata get applied at write time.
    fn is_otel_metrics(&self) -> bool {
        self.get_log_source()
            .iter()
            .any(|s| matches!(s.log_source_format, LogSource::OtelMetrics))
    }

    /// Permute a `RecordBatch` so rows are ordered by
    /// `(metric_name ASC, time_partition DESC)`. Required for parquet
    /// page-index pruning to be effective on PromQL's
    /// `metric_name = 'X'` selector — without this, pages within a row
    /// group hold interleaved metrics and per-page min/max stats span
    /// every metric in the stream, killing pruning.
    ///
    /// Bails out without sorting when either source column is missing
    /// (non-metric stream, schema drift) so the caller can write the
    /// batch unchanged.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn sort_batch_for_metric_pruning(
        batch: &RecordBatch,
        time_partition_field: &str,
    ) -> Result<RecordBatch, StagingError> {
        let schema = batch.schema();
        let Some(name_idx) = schema.index_of("metric_name").ok() else {
            return Ok(batch.clone());
        };
        let Some(time_idx) = schema.index_of(time_partition_field).ok() else {
            return Ok(batch.clone());
        };
        if batch.num_rows() < 2 {
            return Ok(batch.clone());
        }
        let arrays = [
            batch.column(name_idx).clone(),
            batch.column(time_idx).clone(),
        ];
        let sort_options = [
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ];
        let indices = lexsort_to_indices_rows(&arrays, &sort_options)?;
        let columns: Vec<ArrayRef> = batch
            .columns()
            .par_iter()
            .map(|column| take(column.as_ref(), &indices, None))
            .collect::<Result<_, _>>()?;
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn prepare_metric_row_group(
        schema: Arc<Schema>,
        buffer: Vec<RecordBatch>,
        time_partition_field: String,
        arrow_read_decode_duration: Duration,
        record_batches: usize,
    ) -> Result<PreparedMetricRowGroup, StagingError> {
        let concat_started = Instant::now();
        let combined = arrow::compute::concat_batches(&schema, &buffer)?;
        let concat_duration = concat_started.elapsed();

        let sort_started = Instant::now();
        let batch = Self::sort_batch_for_metric_pruning(&combined, &time_partition_field)?;
        let sort_duration = sort_started.elapsed();

        Ok(PreparedMetricRowGroup {
            batch,
            arrow_read_decode_duration,
            concat_duration,
            sort_duration,
            record_batches,
        })
    }

    fn spawn_metric_row_group_prepare(
        schema: Arc<Schema>,
        buffer: Vec<RecordBatch>,
        time_partition_field: String,
        arrow_read_decode_duration: Duration,
        record_batches: usize,
    ) -> std::sync::mpsc::Receiver<Result<PreparedMetricRowGroup, StagingError>> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        METRIC_PARQUET_THREADPOOL.spawn(move || {
            let result = Self::prepare_metric_row_group(
                schema,
                buffer,
                time_partition_field,
                arrow_read_decode_duration,
                record_batches,
            );
            let _ = tx.send(result);
        });
        rx
    }

    fn forward_prepared_metric_row_group(
        pending: &mut VecDeque<
            std::sync::mpsc::Receiver<Result<PreparedMetricRowGroup, StagingError>>,
        >,
        output: &std::sync::mpsc::SyncSender<MetricRowGroupPipelineMessage>,
    ) -> bool {
        let Some(prepared) = pending.pop_front() else {
            return true;
        };
        let prepared = match prepared.recv() {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(err)) => {
                let _ = output.send(MetricRowGroupPipelineMessage::Failed(err));
                return false;
            }
            Err(err) => {
                let err = StagingError::ObjectStorage(std::io::Error::other(format!(
                    "Metric row-group preparation worker failed: {err}"
                )));
                let _ = output.send(MetricRowGroupPipelineMessage::Failed(err));
                return false;
            }
        };
        output
            .send(MetricRowGroupPipelineMessage::Prepared(prepared))
            .is_ok()
    }

    fn spawn_metric_row_group_pipeline(
        record_reader: ConversionRecordReader,
        schema: Arc<Schema>,
        time_partition: Option<String>,
        time_partition_field: String,
        target_rows: usize,
    ) -> std::sync::mpsc::Receiver<MetricRowGroupPipelineMessage> {
        // Metric batches are decoded by independent per-file reader lanes and
        // reassembled in source order. Concat and sort are also dispatched
        // independently and forwarded in order.
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        INGESTION_THREADPOOL.spawn(move || {
            let buffer_capacity = record_reader.reader_count();
            let mut buffer = Vec::with_capacity(buffer_capacity);
            let mut buffered_rows = 0;
            let mut buffered_read_decode_duration = Duration::ZERO;
            let mut buffered_record_batches = 0;
            let mut pending_preparations =
                VecDeque::with_capacity(*METRIC_ROW_GROUP_PREP_IN_FLIGHT);
            let mut merged_iter = record_reader.merged_iter(schema.clone(), time_partition);

            loop {
                let read_started = Instant::now();
                let next_record = merged_iter.next();
                let read_decode_duration = read_started.elapsed();
                let Some(record) = next_record else {
                    break;
                };
                let record = match record {
                    Ok(record) => record,
                    Err(err) => {
                        let _ = tx.send(MetricRowGroupPipelineMessage::Failed(err.into()));
                        return;
                    }
                };
                let record_rows = record.num_rows();

                buffered_record_batches += 1;
                let mut record_offset = 0;
                let mut remaining_read_decode_duration = read_decode_duration;
                while record_offset < record_rows {
                    let rows = (target_rows - buffered_rows).min(record_rows - record_offset);
                    buffer.push(record.slice(record_offset, rows));
                    buffered_rows += rows;
                    record_offset += rows;

                    let slice_read_decode_duration = if record_offset == record_rows {
                        remaining_read_decode_duration
                    } else {
                        let duration =
                            read_decode_duration.mul_f64(rows as f64 / record_rows as f64);
                        remaining_read_decode_duration = remaining_read_decode_duration
                            .checked_sub(duration)
                            .unwrap_or_default();
                        duration
                    };
                    buffered_read_decode_duration += slice_read_decode_duration;

                    if buffered_rows == target_rows {
                        let row_group_buffer =
                            std::mem::replace(&mut buffer, Vec::with_capacity(buffer_capacity));
                        let row_group_read_decode_duration =
                            std::mem::take(&mut buffered_read_decode_duration);
                        let row_group_record_batches = std::mem::take(&mut buffered_record_batches);
                        let prepared = Self::spawn_metric_row_group_prepare(
                            schema.clone(),
                            row_group_buffer,
                            time_partition_field.clone(),
                            row_group_read_decode_duration,
                            row_group_record_batches,
                        );
                        pending_preparations.push_back(prepared);
                        if pending_preparations.len() >= *METRIC_ROW_GROUP_PREP_IN_FLIGHT
                            && !Self::forward_prepared_metric_row_group(
                                &mut pending_preparations,
                                &tx,
                            )
                        {
                            return;
                        }
                        buffered_rows = 0;
                    }
                }
            }
            // Release Arrow readers before signaling completion so successful
            // conversion can safely remove source files on every platform.
            drop(merged_iter);

            if !buffer.is_empty() {
                let prepared = Self::spawn_metric_row_group_prepare(
                    schema,
                    buffer,
                    time_partition_field,
                    buffered_read_decode_duration,
                    buffered_record_batches,
                );
                pending_preparations.push_back(prepared);
            }
            while !pending_preparations.is_empty() {
                if !Self::forward_prepared_metric_row_group(&mut pending_preparations, &tx) {
                    return;
                }
            }
            let _ = tx.send(MetricRowGroupPipelineMessage::Complete);
        });
        rx
    }

    fn encode_metric_row_group(
        row_group_factory: &ArrowRowGroupWriterFactory,
        column_paths: &[String],
        batch: RecordBatch,
        row_group_index: usize,
    ) -> Result<EncodedMetricRowGroup, StagingError> {
        let rows = batch.num_rows();
        let setup_started = Instant::now();
        let column_writers = row_group_factory.create_column_writers(row_group_index)?;
        let mut leaves = Vec::with_capacity(column_writers.len());
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            leaves.extend(compute_leaves(field.as_ref(), column)?);
        }

        if column_writers.len() != leaves.len() {
            return Err(ParquetError::General(format!(
                "Parquet column writer count {} does not match Arrow leaf count {}",
                column_writers.len(),
                leaves.len()
            ))
            .into());
        }
        if column_writers.len() != column_paths.len() {
            return Err(ParquetError::General(format!(
                "Parquet column writer count {} does not match column path count {}",
                column_writers.len(),
                column_paths.len()
            ))
            .into());
        }
        let setup_duration = setup_started.elapsed();

        let writer_leaves = column_writers
            .into_iter()
            .zip(leaves)
            .zip(column_paths.iter().cloned())
            .collect::<Vec<_>>();
        let encode_started = Instant::now();
        let chunks: Result<Vec<_>, ParquetError> = writer_leaves
            .into_par_iter()
            .with_min_len(*PARQUET_COLUMNS_PER_RAYON_TASK)
            .map(|((mut column_writer, leaf), path)| {
                let column_started = Instant::now();
                column_writer.write(&leaf)?;
                let chunk = column_writer.close()?;
                let timing = MetricColumnEncodeTiming {
                    path,
                    duration: column_started.elapsed(),
                    bytes_written: chunk.close().bytes_written,
                };
                Ok((chunk, timing))
            })
            .collect();

        Ok(EncodedMetricRowGroup {
            row_group_index,
            rows,
            chunks: chunks?,
            setup_duration,
            encode_duration: encode_started.elapsed(),
        })
    }

    fn spawn_metric_row_group_encode(
        row_group_factory: Arc<ArrowRowGroupWriterFactory>,
        column_paths: Arc<Vec<String>>,
        batch: RecordBatch,
        row_group_index: usize,
    ) -> std::sync::mpsc::Receiver<Result<EncodedMetricRowGroup, StagingError>> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        METRIC_PARQUET_THREADPOOL.spawn(move || {
            let result = Self::encode_metric_row_group(
                &row_group_factory,
                &column_paths,
                batch,
                row_group_index,
            );
            let _ = tx.send(result);
        });
        rx
    }

    fn append_encoded_metric_row_group<W: Write + Send>(
        writer: &mut SerializedFileWriter<W>,
        encoded: EncodedMetricRowGroup,
    ) -> Result<MetricParquetWriteTimings, StagingError> {
        let expected_row_group_index = writer.flushed_row_groups().len();
        if encoded.row_group_index != expected_row_group_index {
            return Err(ParquetError::General(format!(
                "Encoded Parquet row group {} cannot be appended at position {}",
                encoded.row_group_index, expected_row_group_index
            ))
            .into());
        }

        let append_started = Instant::now();
        let mut row_group_writer = writer.next_row_group()?;
        let mut column_encode_timings = Vec::with_capacity(encoded.chunks.len());
        for (chunk, column_timing) in encoded.chunks {
            chunk.append_to_row_group(&mut row_group_writer)?;
            column_encode_timings.push(column_timing);
        }
        row_group_writer.close()?;

        Ok(MetricParquetWriteTimings {
            setup_duration: encoded.setup_duration,
            encode_duration: encoded.encode_duration,
            append_duration: append_started.elapsed(),
            row_groups: 1,
            column_encode_timings,
        })
    }

    fn write_encoded_metric_row_group<W: Write + Send>(
        stream_name: &str,
        part_path: &Path,
        writer: &mut SerializedFileWriter<W>,
        encoded: EncodedMetricRowGroup,
        preparation: MetricRowGroupPreparationTimings,
        timings: &mut MetricParquetPhaseTimings,
    ) -> Result<(), StagingError> {
        let first_row_group = encoded.row_group_index;
        let prepared_rows = encoded.rows;
        timings.record_batches += preparation.record_batches;
        timings.rows += prepared_rows;
        timings.arrow_read_decode_duration += preparation.arrow_read_decode_duration;
        timings.prepare_wait_duration += preparation.prepare_wait_duration;
        timings.concat_duration += preparation.concat_duration;
        timings.sort_duration += preparation.sort_duration;

        let write_timings = Self::append_encoded_metric_row_group(writer, encoded)?;
        timings.parquet_setup_duration += write_timings.setup_duration;
        timings.parquet_encode_duration += write_timings.encode_duration;
        timings.parquet_append_duration += write_timings.append_duration;
        timings.row_groups += write_timings.row_groups;
        let slowest_columns = write_timings
            .column_encode_timings
            .iter()
            .sorted_by_key(|timing| std::cmp::Reverse(timing.duration))
            .take(10)
            .map(|timing| {
                format!(
                    "{}:{:.3}ms:{}B",
                    timing.path,
                    timing.duration.as_secs_f64() * 1000.0,
                    timing.bytes_written
                )
            })
            .join(",");
        trace!(
            stream_name,
            parquet_path = %part_path.display(),
            first_row_group,
            row_groups = write_timings.row_groups,
            rows = prepared_rows,
            arrow_read_decode_ms = preparation.arrow_read_decode_duration.as_secs_f64() * 1000.0,
            concat_ms = preparation.concat_duration.as_secs_f64() * 1000.0,
            sort_ms = preparation.sort_duration.as_secs_f64() * 1000.0,
            prepare_wait_ms = preparation.prepare_wait_duration.as_secs_f64() * 1000.0,
            parquet_setup_ms = write_timings.setup_duration.as_secs_f64() * 1000.0,
            parquet_encode_ms = write_timings.encode_duration.as_secs_f64() * 1000.0,
            parquet_append_ms = write_timings.append_duration.as_secs_f64() * 1000.0,
            encoded_columns = write_timings.column_encode_timings.len(),
            slowest_columns,
            "Metric Arrow-to-Parquet row-group phase timings"
        );
        Ok(())
    }

    fn reset_staging_metrics(&self, tenant_id: &Option<String>) {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        metrics::STAGING_FILES
            .with_label_values(&[&self.stream_name, tenant_str])
            .set(0);
        metrics::STORAGE_SIZE
            .with_label_values(&["staging", &self.stream_name, "arrows", tenant_str])
            .set(0);
        metrics::STORAGE_SIZE
            .with_label_values(&["staging", &self.stream_name, "parquet", tenant_str])
            .set(0);
    }

    fn update_staging_metrics(
        &self,
        staging_files: &[(PathBuf, Vec<PathBuf>)],
        tenant_id: &Option<String>,
    ) {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let total_arrow_files = staging_files
            .iter()
            .map(|(_, files)| files.len())
            .sum::<usize>();
        metrics::STAGING_FILES
            .with_label_values(&[&self.stream_name, tenant_str])
            .set(total_arrow_files as i64);

        let total_arrow_files_size = staging_files
            .iter()
            .map(|(_, files)| {
                files
                    .iter()
                    .filter_map(|file| file.metadata().ok().map(|meta| meta.len()))
                    .sum::<u64>()
            })
            .sum::<u64>();
        metrics::STORAGE_SIZE
            .with_label_values(&["staging", &self.stream_name, "arrows", tenant_str])
            .set(total_arrow_files_size as i64);
    }

    /// This function reads arrow files, groups their schemas
    ///
    /// converts them into parquet files and returns a merged schema
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn convert_disk_files_to_parquet(
        &self,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
        init_signal: bool,
        shutdown_signal: bool,
        tenant_id: &Option<String>,
    ) -> Result<Option<Schema>, StagingError> {
        self.convert_disk_files_to_parquet_outcome(
            time_partition,
            custom_partition,
            init_signal,
            shutdown_signal,
            tenant_id,
            None,
        )?
        .into_result()
    }

    fn convert_disk_files_to_parquet_outcome(
        &self,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
        init_signal: bool,
        shutdown_signal: bool,
        tenant_id: &Option<String>,
        root_arrows: Option<Vec<PathBuf>>,
    ) -> Result<ArrowFileConversionOutcome, StagingError> {
        let span = info_span!(
            "convert_disk_files_to_parquet",
            stream_name = %self.stream_name,
            file_group_count = tracing::field::Empty,
        );
        let _guard = span.enter();

        let now = SystemTime::now();
        #[cfg(test)]
        let now = self.conversion_clock.lock().unwrap().unwrap_or(now);
        let group_minute = minute_from_system_time(now) - 1;
        let (claim, staging_files) = self.claim_conversion_groups_inner(
            now,
            group_minute,
            init_signal,
            shutdown_signal,
            true,
            root_arrows,
        );
        span.record("file_group_count", staging_files.len());
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::ConversionClaimed(staging_files.clone()));
        let mut outcome = self.convert_arrow_file_groups_to_parquet(
            staging_files,
            time_partition,
            custom_partition,
            tenant_id,
            ArrowGroupExecution::Parallel,
        )?;
        outcome.claim = Some(claim);
        Ok(outcome)
    }

    /// Converts groups whose sources and outputs the caller has claimed. Every
    /// group, parallel or not, has finished before this returns.
    fn convert_arrow_file_groups_to_parquet(
        &self,
        staging_files: Vec<(PathBuf, Vec<PathBuf>)>,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
        tenant_id: &Option<String>,
        execution: ArrowGroupExecution,
    ) -> Result<ArrowFileConversionOutcome, StagingError> {
        if staging_files.is_empty() {
            self.reset_staging_metrics(tenant_id);
            return Ok(ArrowFileConversionOutcome::default());
        }

        self.update_staging_metrics(&staging_files, tenant_id);
        let mut schemas = Vec::new();
        let mut first_error = None;

        let convert = |(parquet_path, arrow_files): (PathBuf, Vec<PathBuf>)| {
            let result = self.convert_arrow_group(
                parquet_path.clone(),
                arrow_files.clone(),
                time_partition,
                custom_partition,
                tenant_id,
            );
            (parquet_path, arrow_files, result)
        };
        let results: Vec<_> = match execution {
            // Startup groups are ordered oldest-first and remain sequential so
            // recovery memory stays bounded. They run on the calling thread.
            ArrowGroupExecution::Sequential => staging_files.into_iter().map(convert).collect(),
            // A periodic processing directory can contain many overdue minute
            // groups. Convert only two at a time so a single stream cannot
            // flood the conversion pool with column-encoding work. Periodic
            // conversion runs on the dedicated conversion pool, never on the
            // global Rayon pool that HTTP ingest uses; nested parallel work
            // stays there too.
            ArrowGroupExecution::Parallel => LOG_CONVERSION_THREADPOOL.install(|| {
                let mut results = Vec::with_capacity(staging_files.len());
                for group_batch in staging_files.chunks(MAX_PERIODIC_PARQUET_GROUPS_PER_STREAM) {
                    results.extend(
                        group_batch
                            .to_vec()
                            .into_par_iter()
                            .map(convert)
                            .collect::<Vec<_>>(),
                    );
                }
                results
            }),
        };

        let mut failed_dirs = HashSet::new();
        for (parquet_path, arrow_files, result) in results {
            match result {
                Ok(Some(schema)) => schemas.push(schema),
                Ok(None) => {}
                Err(err) => {
                    // Schema conflicts can be reported after publication. Retry
                    // only retained sources, once per directory per cycle.
                    failed_dirs.extend(
                        arrow_files
                            .iter()
                            .filter(|file| file.exists())
                            .filter_map(|file| file.parent().map(Path::to_path_buf)),
                    );
                    error!(
                        "Failed to convert Arrow group for stream {} to {}: {err}",
                        self.stream_name,
                        parquet_path.display()
                    );
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        let mut registry = lock_conversion_claims(&self.conversion_claims);
        for dir in failed_dirs {
            let attempts = registry.retry_attempts.entry(dir.clone()).or_default();
            *attempts += 1;
            if *attempts == MAX_CONVERSION_ATTEMPTS {
                warn!(
                    "Giving up runtime conversion retries for {} after {MAX_CONVERSION_ATTEMPTS} failed cycles; retained files wait for restart",
                    dir.display()
                );
            }
            registry.retry_dirs.insert(dir);
        }
        drop(registry);
        Ok(ArrowFileConversionOutcome {
            schemas,
            first_error,
            claim: None,
        })
    }

    /// Releases live claims after every group has staged its schema and either
    /// completed or retained durable ownership, then surfaces the first error.
    fn finish_arrow_file_conversion(
        &self,
        outcome: ArrowFileConversionOutcome,
    ) -> Result<(), StagingError> {
        let ArrowFileConversionOutcome {
            first_error, claim, ..
        } = outcome;
        drop(claim);
        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    /// Converts one Parquet output group and removes its Arrow sources only
    /// after the final Parquet rename succeeds.
    fn convert_arrow_group(
        &self,
        parquet_path: PathBuf,
        arrow_files: Vec<PathBuf>,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
        tenant_id: &Option<String>,
    ) -> Result<Option<Schema>, StagingError> {
        if has_journal(&parquet_path) {
            return Err(std::io::Error::other(format!(
                "Journal owns {}; resolve instead of encoding",
                parquet_path.display()
            ))
            .into());
        }
        // Refuse gated older sources before opening readers or encoding a part.
        self.check_conversion_sources(
            &mut lock_conversion_claims(&self.conversion_claims),
            &parquet_path,
            &arrow_files,
        )?;
        // Metrics sort every output row group explicitly, so reading their IPC
        // streams forward avoids reverse seeks and a full-column row reversal.
        let record_reader =
            ConversionRecordReader::for_stream(&arrow_files, self.is_otel_metrics());
        self.remove_invalid_arrow_files(record_reader.invalid_files(), tenant_id);
        let readable_arrow_files = record_reader.readable_files().to_vec();
        if record_reader.reader_count() == 0 {
            return Ok(None);
        }
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::ConversionReadersOpened {
            output: parquet_path.clone(),
            sources: readable_arrow_files.clone(),
        });

        let merged_schema = record_reader.merged_schema();
        let cluster_key = if self.is_otel_metrics() {
            None
        } else {
            self.log_cluster_key(&merged_schema, time_partition)
        };
        let props = self.parquet_writer_props(
            &merged_schema,
            time_partition,
            custom_partition,
            cluster_key.is_some(),
        );
        let schema = Arc::new(merged_schema.clone());
        let part_path = parquet_part_path(&parquet_path);

        let write_result = self.write_parquet_part_file(
            &part_path,
            record_reader,
            &schema,
            &props,
            time_partition,
            cluster_key,
        );
        let artifacts = match write_result {
            Ok(Some(artifacts)) => {
                #[cfg(test)]
                self.lifecycle_phase(LifecyclePhase::ParquetPartWritten(part_path.clone()));
                artifacts
            }
            Ok(None) => {
                Self::remove_partial_parquet_file(&part_path);
                return Ok(None);
            }
            Err(err) => {
                Self::remove_partial_parquet_file(&part_path);
                if matches!(&err, StagingError::Arrow(_)) {
                    error!(
                        "Arrow decode failed while building {}: {err}",
                        parquet_path.display()
                    );
                    let invalid_files = Self::invalid_arrow_files(&readable_arrow_files);
                    if invalid_files.is_empty() {
                        warn!(
                            "Arrow decode failure could not be attributed to a source file; retaining group for retry"
                        );
                    } else {
                        self.remove_invalid_arrow_files(&invalid_files, tenant_id);
                    }
                    return Ok(None);
                }
                return Err(err);
            }
        };

        let mut schema_error = None;
        let journal_result = (|| {
            self.fail_journal_io(conversion_journal::JournalFault::PartSync)?;
            File::open(&part_path)?.sync_all()?;
            self.conversion_phase(ConversionPhase::PartSynced);
            if !self.get_static_schema_flag() {
                match self.stage_conversion_schema(merged_schema.clone()) {
                    Ok(()) => {}
                    Err(err @ StagingError::Arrow(_)) => {
                        // As on main, schema conflicts are reported for the cycle
                        // but do not prevent this group's publication.
                        error!(
                            "Schema conflict while converting {}: {err}",
                            parquet_path.display()
                        );
                        schema_error = Some(err);
                    }
                    Err(err) => return Err(err),
                }
            }
            let journal = ConversionJournal::new(self, &parquet_path, &readable_arrow_files)?;
            self.install_conversion_journal(&journal)?;
            Ok::<_, StagingError>(journal)
        })();
        let journal = match journal_result {
            Ok(journal) => journal,
            Err(err) => {
                // Before J, intact sources can rebuild T. An install that
                // failed after its rename already owns T: the guard retains it.
                Self::remove_partial_parquet_file(&part_path);
                return Err(err);
            }
        };
        self.conversion_phase(ConversionPhase::JournalInstalled);
        self.publish_parquet(&part_path, &parquet_path, &readable_arrow_files)?;
        if let Some(metadata) = artifacts.parquet_metadata {
            crate::catalog::manifest::cache_parquet_metadata(&parquet_path, metadata);
        }
        if let Some((max_field_statistics, stats)) = artifacts.field_stats {
            crate::storage::field_stats::cache_staged_field_stats(
                &parquet_path,
                max_field_statistics,
                stats,
            );
        }
        self.retire_conversion_sources(&journal, tenant_id)?;
        if let Some(err) = schema_error {
            return Err(err);
        }
        Ok(Some(merged_schema))
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    /// Writes one conversion group to a temporary Parquet file.
    fn write_parquet_part_file(
        &self,
        part_path: &Path,
        record_reader: ConversionRecordReader,
        schema: &Arc<Schema>,
        props: &WriterProperties,
        time_partition: Option<&String>,
        cluster_key: Option<LogClusterKey>,
    ) -> Result<Option<WrittenParquetArtifacts>, StagingError> {
        let _span = info_span!(
            "write_parquet_part_file",
            stream_name = %self.stream_name,
        )
        .entered();
        let mut part_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(part_path)
            .map_err(|_| StagingError::Create)?;
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::ParquetPartCreated(part_path.to_path_buf()));
        let collect_stats = self.options.collect_dataset_stats
            && self.stream_name != crate::storage::field_stats::DATASET_STATS_STREAM_NAME;
        let max_field_statistics = self.options.max_field_statistics;
        let artifacts = if self.is_otel_metrics() {
            self.write_metric_parquet_part_file(
                part_path,
                &mut part_file,
                record_reader,
                schema,
                props,
                time_partition,
                collect_stats.then_some(max_field_statistics),
            )?
        } else if let Some(key) = cluster_key {
            let stats = log_clustering::write_clustered_log_parquet(
                &mut part_file,
                record_reader.merged_iter(schema.clone(), time_partition.cloned()),
                schema,
                props,
                self.options.row_group_size,
                key,
            )?;
            debug!(
                stream_name = %self.stream_name,
                parquet_path = %part_path.display(),
                rows = stats.rows,
                row_groups = stats.row_groups,
                cluster_ms = stats.cluster_duration.as_secs_f64() * 1000.0,
                "Clustered log Arrow-to-Parquet conversion"
            );
            WrittenParquetArtifacts::default()
        } else {
            let mut stats_acc = collect_stats.then(|| {
                crate::storage::field_stats::FieldStatsAccumulator::new(
                    &self.stream_name,
                    schema.as_ref(),
                    max_field_statistics,
                )
            });
            let mut writer =
                ArrowWriter::try_new(&mut part_file, schema.clone(), Some(props.clone()))?;
            for record in record_reader.merged_iter(schema.clone(), time_partition.cloned()) {
                let batch = record?;
                if let Some(acc) = stats_acc.as_mut() {
                    acc.update(&batch);
                }
                writer.write(&batch)?;
            }
            let parquet_metadata = Arc::new(writer.close()?);
            WrittenParquetArtifacts {
                parquet_metadata: Some(parquet_metadata),
                field_stats: stats_acc.map(|acc| (max_field_statistics, acc.finish())),
            }
        };

        drop(part_file);
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::ParquetPartClosed(part_path.to_path_buf()));

        if !Self::is_valid_parquet_file(part_path, &self.stream_name) {
            error!(
                "Invalid parquet file {part_path:?} detected for stream {stream_name}, removing it",
                stream_name = &self.stream_name
            );
            Self::remove_partial_parquet_file(part_path);
            return Ok(None);
        }
        trace!("Parquet file successfully constructed");
        Ok(Some(artifacts))
    }

    /// Writes a metric conversion group with pipelined row-group preparation
    /// and encoding while preserving row-group output order.
    #[allow(clippy::too_many_arguments)]
    fn write_metric_parquet_part_file(
        &self,
        part_path: &Path,
        part_file: &mut File,
        record_reader: ConversionRecordReader,
        schema: &Arc<Schema>,
        props: &WriterProperties,
        time_partition: Option<&String>,
        collect_stats_max: Option<usize>,
    ) -> Result<WrittenParquetArtifacts, StagingError> {
        let conversion_started = Instant::now();
        let source_arrow_files = record_reader.readable_files().len();
        let source_arrow_bytes = record_reader
            .readable_files()
            .iter()
            .filter_map(|path| path.metadata().ok().map(|metadata| metadata.len()))
            .sum::<u64>();
        let mut timings = MetricParquetPhaseTimings::default();
        let arrow_writer =
            ArrowWriter::try_new(&mut *part_file, schema.clone(), Some(props.clone()))?;
        let (mut writer, row_group_factory) = arrow_writer.into_serialized_writer()?;
        let row_group_factory = Arc::new(row_group_factory);
        let column_paths = Arc::new(
            writer
                .schema_descr()
                .columns()
                .iter()
                .map(|column| column.path().string())
                .collect::<Vec<_>>(),
        );
        let target = self.options.row_group_size;
        if target == 0 {
            return Err(ParquetError::General(
                "Parquet row-group target must be greater than zero".to_string(),
            )
            .into());
        }

        let time_partition_field = time_partition.map_or_else(
            || DEFAULT_TIMESTAMP_KEY.to_string(),
            |value| value.as_str().to_string(),
        );
        // Producer reads, decodes, concatenates, and sorts the next exact row
        // group while this thread encodes the current row group.
        let prepared_row_groups = Self::spawn_metric_row_group_pipeline(
            record_reader,
            schema.clone(),
            time_partition.cloned(),
            time_partition_field,
            target,
        );
        let mut stats_acc = collect_stats_max.map(|max_stats| {
            (
                max_stats,
                crate::storage::field_stats::FieldStatsAccumulator::new(
                    &self.stream_name,
                    schema.as_ref(),
                    max_stats,
                ),
            )
        });
        let mut preparation_complete = false;
        let mut next_row_group_index = writer.flushed_row_groups().len();
        let mut pending_encodings = VecDeque::with_capacity(*METRIC_ROW_GROUP_ENCODE_IN_FLIGHT);
        while !preparation_complete || !pending_encodings.is_empty() {
            if !preparation_complete && pending_encodings.len() < *METRIC_ROW_GROUP_ENCODE_IN_FLIGHT
            {
                let wait_started = Instant::now();
                let message = prepared_row_groups.recv().map_err(|err| {
                    StagingError::ObjectStorage(std::io::Error::other(format!(
                        "Metric row-group pipeline worker failed: {err}"
                    )))
                })?;
                let prepare_wait_duration = wait_started.elapsed();
                match message {
                    MetricRowGroupPipelineMessage::Prepared(prepared) => {
                        if prepared.batch.num_rows() > target {
                            return Err(ParquetError::General(format!(
                                "Prepared metric row group has {} rows, exceeding target {target}",
                                prepared.batch.num_rows()
                            ))
                            .into());
                        }
                        if let Some((_, acc)) = stats_acc.as_mut() {
                            acc.update(&prepared.batch);
                        }
                        let preparation = MetricRowGroupPreparationTimings {
                            arrow_read_decode_duration: prepared.arrow_read_decode_duration,
                            concat_duration: prepared.concat_duration,
                            sort_duration: prepared.sort_duration,
                            prepare_wait_duration,
                            record_batches: prepared.record_batches,
                        };
                        let encode = Self::spawn_metric_row_group_encode(
                            Arc::clone(&row_group_factory),
                            Arc::clone(&column_paths),
                            prepared.batch,
                            next_row_group_index,
                        );
                        pending_encodings.push_back((encode, preparation));
                        next_row_group_index += 1;
                        continue;
                    }
                    MetricRowGroupPipelineMessage::Failed(err) => return Err(err),
                    MetricRowGroupPipelineMessage::Complete => preparation_complete = true,
                }
            }

            if let Some((encoded, preparation)) = pending_encodings.pop_front() {
                let encoded = encoded.recv().map_err(|err| {
                    StagingError::ObjectStorage(std::io::Error::other(format!(
                        "Metric row-group encoding worker failed: {err}"
                    )))
                })??;
                Self::write_encoded_metric_row_group(
                    &self.stream_name,
                    part_path,
                    &mut writer,
                    encoded,
                    preparation,
                    &mut timings,
                )?;
            }
        }

        let close_started = Instant::now();
        let parquet_metadata = Arc::new(writer.close()?);
        timings.parquet_close_duration = close_started.elapsed();
        let total_duration = conversion_started.elapsed();
        let output_bytes = part_file
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        info!(
            stream_name = %self.stream_name,
            parquet_path = %part_path.display(),
            source_arrow_files,
            source_arrow_bytes,
            output_bytes,
            record_batches = timings.record_batches,
            rows = timings.rows,
            row_groups = timings.row_groups,
            arrow_readers_per_file = *METRIC_ARROW_READERS_PER_FILE,
            arrow_readers_in_flight = *METRIC_ARROW_READERS_IN_FLIGHT,
            row_group_prepare_in_flight = *METRIC_ROW_GROUP_PREP_IN_FLIGHT,
            row_group_encode_in_flight = *METRIC_ROW_GROUP_ENCODE_IN_FLIGHT,
            columns_per_rayon_task = *PARQUET_COLUMNS_PER_RAYON_TASK,
            total_ms = total_duration.as_secs_f64() * 1000.0,
            arrow_read_decode_ms = timings.arrow_read_decode_duration.as_secs_f64() * 1000.0,
            concat_ms = timings.concat_duration.as_secs_f64() * 1000.0,
            sort_ms = timings.sort_duration.as_secs_f64() * 1000.0,
            prepare_wait_ms = timings.prepare_wait_duration.as_secs_f64() * 1000.0,
            parquet_setup_ms = timings.parquet_setup_duration.as_secs_f64() * 1000.0,
            parquet_encode_ms = timings.parquet_encode_duration.as_secs_f64() * 1000.0,
            parquet_append_ms = timings.parquet_append_duration.as_secs_f64() * 1000.0,
            parquet_close_ms = timings.parquet_close_duration.as_secs_f64() * 1000.0,
            "Metric Arrow-to-Parquet phase timings"
        );
        Ok(WrittenParquetArtifacts {
            parquet_metadata: Some(parquet_metadata),
            field_stats: stats_acc.map(|(max_stats, acc)| (max_stats, acc.finish())),
        })
    }

    /// Removes a temporary Parquet output left by a failed conversion.
    fn remove_partial_parquet_file(part_path: &Path) {
        // Structural guard: after J exists the only legal removal of T is
        // publication rename. This also covers validation/error cleanup.
        if has_journal_for_part(part_path) {
            return;
        }
        match remove_file(part_path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => warn!(
                "Failed to remove partial parquet file {}: {err}",
                part_path.display()
            ),
        }
    }

    /// Returns files whose Arrow bodies cannot be decoded completely.
    fn invalid_arrow_files(arrow_files: &[PathBuf]) -> Vec<PathBuf> {
        arrow_files
            .iter()
            .filter_map(|path| {
                let invalid = match File::open(path).and_then(get_reverse_reader) {
                    Ok(mut reader) => reader.any(|batch| batch.is_err()),
                    Err(_) => true,
                };
                invalid.then(|| path.clone())
            })
            .collect()
    }

    /// function to validate parquet files
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn is_valid_parquet_file(path: &Path, stream_name: &str) -> bool {
        Self::validate_parquet_file(path, stream_name).unwrap_or_else(|error| {
            error!("Cannot validate parquet file {path:?} for stream {stream_name}: {error}");
            false
        })
    }

    /// The query calls this on its immutable lease, propagating access/metadata
    /// failures while retaining the uploader's corrupt/empty-file skip policy.
    pub(crate) fn validate_parquet_file(path: &Path, stream_name: &str) -> std::io::Result<bool> {
        let file = std::fs::File::open(path)?;
        let size = file.metadata()?.len();
        if size < FOOTER_SIZE as u64 {
            error!(
                "Invalid parquet file {path:?} detected for stream {stream_name}, size: {size} bytes"
            );
            return Ok(false);
        }
        match SerializedFileReader::new(file) {
            Ok(reader) => {
                let valid = reader.metadata().file_metadata().num_rows() != 0;
                if !valid {
                    error!("Invalid parquet file {path:?} for stream {stream_name}: no rows");
                }
                Ok(valid)
            }
            Err(parquet::errors::ParquetError::External(error)) => {
                // Actual filesystem read errors are capture failures too.
                match error.downcast::<std::io::Error>() {
                    Ok(error) => Err(*error),
                    Err(error) => {
                        error!(
                            "Failed to read parquet file {path:?} for stream {stream_name}: {error}"
                        );
                        Ok(false)
                    }
                }
            }
            Err(error) => {
                error!("Failed to read parquet file {path:?} for stream {stream_name}: {error}");
                Ok(false)
            }
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn cleanup_arrow_files_and_dir(&self, arrow_files: &[PathBuf], tenant_id: &Option<String>) {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let processing_dirs = arrow_files
            .iter()
            .filter_map(|file| file.parent())
            .filter(|parent| {
                parent
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(INPROCESS_DIR_PREFIX))
            })
            .map(Path::to_path_buf)
            .collect::<HashSet<_>>();

        for file in arrow_files {
            match file.metadata() {
                Ok(meta) => {
                    let file_size = meta.len();
                    match remove_file(file) {
                        Ok(_) => {
                            metrics::STORAGE_SIZE
                                .with_label_values(&[
                                    "staging",
                                    &self.stream_name,
                                    ARROW_FILE_EXTENSION,
                                    tenant_str,
                                ])
                                .sub(file_size as i64);
                        }
                        Err(e) => {
                            warn!("Failed to delete file {}: {e}", file.display());
                        }
                    }
                }
                Err(err) => {
                    warn!("File ({}) not found; Error = {err}", file.display());
                }
            }
        }

        // One logical startup group can span several processing directories.
        // Remove every contributing directory once its final file is gone.
        let mut registry = lock_conversion_claims(&self.conversion_claims);
        for parent_dir in processing_dirs {
            // Another output in this directory may still have a journal even
            // after its last source was removed. Keep its parent for retries.
            if registry.journal_dirs.contains_key(&parent_dir) || registry.journals_blocked {
                continue;
            }
            // remove_dir atomically proves emptiness without a directory scan.
            match fs::remove_dir(&parent_dir) {
                Ok(()) => registry.forget_conversion_dir(&parent_dir),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    registry.forget_conversion_dir(&parent_dir);
                }
                Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                Err(err) => warn!(
                    "Failed to remove inprocess directory {}: {err}",
                    parent_dir.display()
                ),
            }
        }
    }

    /// Logs and deletes invalid Arrow files so they cannot poison later retries.
    fn remove_invalid_arrow_files(&self, arrow_files: &[PathBuf], tenant_id: &Option<String>) {
        for file in arrow_files {
            match file.metadata() {
                Ok(meta) => warn!(
                    "Removing invalid/corrupted Arrow file {} for stream {}, size_bytes={}",
                    file.display(),
                    self.stream_name,
                    meta.len()
                ),
                Err(err) => warn!(
                    "Removing invalid/corrupted Arrow file {} for stream {}, size unavailable: {err}",
                    file.display(),
                    self.stream_name
                ),
            }
        }
        // Discarded rows are not persisted; stop serving them from memory.
        self.release_pending(arrow_files);
        self.cleanup_arrow_files_and_dir(arrow_files, tenant_id);
    }

    pub fn updated_schema(&self, current_schema: Schema) -> Schema {
        let staging_files = self.arrow_files();
        let record_reader = MergedReverseRecordReader::try_new(&staging_files);
        if record_reader.readers.is_empty() {
            return current_schema;
        }

        let schema = record_reader.merged_schema();

        Schema::try_merge(vec![schema, current_schema]).unwrap()
    }

    /// Stores the provided stream metadata in memory mapping
    pub async fn set_metadata(&self, mut updated_metadata: LogStreamMetadata) {
        let mut metadata = self.metadata.write().expect(LOCK_EXPECT);
        // mark_deleting() is documented as monotonic -- a reload racing a
        // delete must not silently clear it back to false.
        updated_metadata.deleting |= metadata.deleting;
        *metadata = updated_metadata;
    }

    pub fn get_first_event(&self) -> Option<String> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .first_event_at
            .clone()
    }

    pub fn get_time_partition(&self) -> Option<String> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .time_partition
            .clone()
    }

    pub fn get_time_partition_limit(&self) -> Option<NonZeroU32> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .time_partition_limit
    }

    pub fn get_custom_partition(&self) -> Option<String> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .custom_partition
            .clone()
    }

    pub fn get_static_schema_flag(&self) -> bool {
        self.metadata.read().expect(LOCK_EXPECT).static_schema_flag
    }

    pub fn get_retention(&self) -> Option<Retention> {
        self.metadata.read().expect(LOCK_EXPECT).retention.clone()
    }

    pub fn get_schema_version(&self) -> SchemaVersion {
        self.metadata.read().expect(LOCK_EXPECT).schema_version
    }

    pub fn get_schema(&self) -> Arc<Schema> {
        let metadata = self.metadata.read().expect(LOCK_EXPECT);

        // sort fields on read from hashmap as order of fields can differ.
        // This provides a stable output order if schema is same between calls to this function
        let fields: Fields = metadata
            .schema
            .values()
            .sorted_by_key(|field| field.name())
            .cloned()
            .collect();

        Arc::new(Schema::new(fields))
    }

    /// Field count of the stored schema, without sorting it into a `Schema`
    pub fn get_schema_len(&self) -> usize {
        self.metadata.read().expect(LOCK_EXPECT).schema.len()
    }

    pub fn get_schema_raw(&self) -> HashMap<String, Arc<Field>> {
        self.metadata.read().expect(LOCK_EXPECT).schema.clone()
    }

    /// The metadata ingest needs to stage a request, read under one lock. The
    /// schema is shared rather than cloned while it is unchanged.
    pub(crate) fn ingest_snapshot(&self) -> IngestSnapshot {
        let metadata = self.metadata.read().expect(LOCK_EXPECT);
        let schema = self.shared_schema(&metadata.schema);
        IngestSnapshot {
            time_partition_limit: metadata.time_partition_limit,
            static_schema_flag: metadata.static_schema_flag,
            custom_partition: metadata.custom_partition.clone(),
            schema_version: metadata.schema_version,
            schema,
            infer_timestamp: metadata.infer_timestamp,
        }
    }

    /// A shared map equal to `current` (the live schema, read-locked by the
    /// caller): the cached one while it still holds exactly the same entries,
    /// otherwise a fresh clone that becomes the cached one.
    fn shared_schema(
        &self,
        current: &HashMap<String, Arc<Field>>,
    ) -> Arc<HashMap<String, Arc<Field>>> {
        {
            let cached = self
                .schema_snapshot
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if same_schema_entries(&cached, current) {
                return Arc::clone(&*cached);
            }
        }
        let mut cached = self
            .schema_snapshot
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if same_schema_entries(&cached, current) {
            return Arc::clone(&*cached);
        }
        let fresh = Arc::new(current.clone());
        *cached = Arc::clone(&fresh);
        fresh
    }

    pub fn set_retention(&self, retention: Retention) {
        self.metadata.write().expect(LOCK_EXPECT).retention = Some(retention);
    }

    pub fn set_first_event_at(&self, first_event_at: &str) {
        self.metadata.write().expect(LOCK_EXPECT).first_event_at = Some(first_event_at.to_owned());
    }

    /// Removes the `first_event_at` timestamp for the specified stream from the LogStreamMetadata.
    ///
    /// This function is called during the retention task, when the parquet files along with the manifest files are deleted from the storage.
    /// The manifest path is removed from the snapshot in the stream.json
    /// and the first_event_at value in the stream.json is removed.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The name of the stream for which the `first_event_at` timestamp is to be removed.
    ///
    /// # Returns
    ///
    /// * `Result<(), StreamNotFound>` - Returns `Ok(())` if the `first_event_at` timestamp is successfully removed,
    ///   or a `StreamNotFound` if the stream metadata is not found.
    ///
    /// # Examples
    /// ```ignore
    /// ```rust
    /// let result = metadata.remove_first_event_at("my_stream");
    /// match result {
    ///     Ok(()) => println!("first-event-at removed successfully"),
    ///     Err(e) => eprintln!("Error removing first-event-at from PARSEABLE.streams: {}", e),
    /// }
    /// ```
    pub fn reset_first_event_at(&self) {
        self.metadata
            .write()
            .expect(LOCK_EXPECT)
            .first_event_at
            .take();
    }

    pub fn set_time_partition_limit(&self, time_partition_limit: NonZeroU32) {
        self.metadata
            .write()
            .expect(LOCK_EXPECT)
            .time_partition_limit = Some(time_partition_limit);
    }

    pub fn set_custom_partition(&self, custom_partition: Option<&String>) {
        self.metadata.write().expect(LOCK_EXPECT).custom_partition = custom_partition.cloned();
    }

    pub fn get_infer_timestamp(&self) -> bool {
        self.metadata.read().expect(LOCK_EXPECT).infer_timestamp
    }

    pub fn set_hot_tier(&self, hot_tier: Option<StreamHotTier>) {
        let mut metadata = self.metadata.write().expect(LOCK_EXPECT);
        metadata.hot_tier.clone_from(&hot_tier);
        metadata.hot_tier_enabled = hot_tier.is_some();
    }

    pub fn get_hot_tier(&self) -> Option<StreamHotTier> {
        self.metadata.read().expect(LOCK_EXPECT).hot_tier.clone()
    }

    pub fn is_hot_tier_enabled(&self) -> bool {
        self.metadata.read().expect(LOCK_EXPECT).hot_tier_enabled
    }

    /// Marks this stream as being deleted. Once set, this flag is never
    /// cleared for this in-memory entry — a deletion in progress runs to
    /// completion (or is resumed on restart), it is never cancelled.
    pub fn mark_deleting(&self) {
        self.metadata.write().expect(LOCK_EXPECT).deleting = true;
    }

    pub fn is_deleting(&self) -> bool {
        self.metadata.read().expect(LOCK_EXPECT).deleting
    }

    pub fn get_stream_type(&self) -> StreamType {
        self.metadata.read().expect(LOCK_EXPECT).stream_type
    }

    pub fn set_log_source(&self, log_source: Vec<LogSourceEntry>) {
        self.metadata.write().expect(LOCK_EXPECT).log_source = log_source;
    }

    pub fn get_log_source(&self) -> Vec<LogSourceEntry> {
        self.metadata.read().expect(LOCK_EXPECT).log_source.clone()
    }

    pub fn get_dataset_tags(&self) -> Vec<DatasetTag> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .dataset_tags
            .clone()
    }

    pub fn get_dataset_labels(&self) -> Vec<String> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .dataset_labels
            .clone()
    }

    pub fn set_dataset_tags(&self, tags: Vec<DatasetTag>) {
        self.metadata.write().expect(LOCK_EXPECT).dataset_tags = tags;
    }

    pub fn set_dataset_labels(&self, labels: Vec<String>) {
        self.metadata.write().expect(LOCK_EXPECT).dataset_labels = labels;
    }

    pub fn add_log_source(&self, log_source: LogSourceEntry) {
        let metadata = self.metadata.read().expect(LOCK_EXPECT);
        for existing in &metadata.log_source {
            if existing.log_source_format == log_source.log_source_format {
                drop(metadata);
                self.add_fields_to_log_source(
                    &log_source.log_source_format,
                    log_source.fields.clone(),
                );
                return;
            }
        }
        drop(metadata);

        let mut metadata = self.metadata.write().expect(LOCK_EXPECT);
        for existing in &metadata.log_source {
            if existing.log_source_format == log_source.log_source_format {
                self.add_fields_to_log_source(
                    &log_source.log_source_format,
                    log_source.fields.clone(),
                );
                return;
            }
        }
        metadata.log_source.push(log_source);
    }

    pub fn add_fields_to_log_source(&self, log_source: &LogSource, fields: HashSet<String>) {
        let mut metadata = self.metadata.write().expect(LOCK_EXPECT);
        for log_source_entry in metadata.log_source.iter_mut() {
            if log_source_entry.log_source_format == *log_source {
                log_source_entry.fields.extend(fields);
                return;
            }
        }
    }

    pub fn get_fields_from_log_source(&self, log_source: &LogSource) -> Option<HashSet<String>> {
        let metadata = self.metadata.read().expect(LOCK_EXPECT);
        for log_source_entry in metadata.log_source.iter() {
            if log_source_entry.log_source_format == *log_source {
                return Some(log_source_entry.fields.clone());
            }
        }
        None
    }

    /// Returns whether a temporary file contains Parquet output.
    ///
    /// The magic-byte fallback recognizes legacy bare `.part` files created
    /// before Arrow and Parquet temporary suffixes were separated.
    fn is_parquet_part_file(path: &Path) -> bool {
        let file_name = path.file_name().and_then(|name| name.to_str());
        if file_name.is_some_and(|name| name.ends_with(PARQUET_PART_FILE_SUFFIX)) {
            return true;
        }
        if file_name.is_some_and(|name| name.ends_with(ARROW_PART_FILE_SUFFIX)) {
            return false;
        }

        let mut magic = [0_u8; 4];
        File::open(path)
            .and_then(|mut file| file.read_exact(&mut magic))
            .is_ok()
            && magic == *b"PAR1"
    }

    /// Recovers orphaned Arrow parts and removes rebuildable Parquet parts.
    ///
    /// Parts of attached writers are live and skipped. Pending memory follows
    /// a recovered part to its `.arrows` name before the rename exposes it to
    /// conversion, and is released when a part is discarded.
    fn recover_orphan_part_files(&self) {
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::RecoveryStarted);
        // Ingestion and shutdown flushes can run concurrently with startup
        // recovery, so a part may belong to a live writer. Holding the
        // lifecycle lock exclusively until recovery returns waits for every
        // flush to finish sealing its detached writers, and stops flush from
        // detaching or renaming any writer meanwhile. Only attached writers
        // remain, and they are skipped below.
        let _lifecycle = self.lock_part_lifecycle_for_recovery();

        let Ok(dir) = self.data_path.read_dir() else {
            return;
        };
        let listed: Vec<PathBuf> = dir
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|ext| ext == PART_FILE_EXTENSION)
            })
            .collect();

        // A part that exists while no writer owns it is an orphan, and stays
        // one until recovery acts on it: DiskWriter::try_new never opens an
        // existing part. Check under the writer lock, because push_disk may
        // drop a writer whose first write failed, vacating its listed path.
        let candidates: Vec<PathBuf> = {
            let writer = match self.writer.lock() {
                Ok(writer) => writer,
                Err(poisoned) => {
                    error!(
                        "Writer lock poisoned; skipping .part recovery for stream {} - {poisoned}",
                        self.stream_name
                    );
                    return;
                }
            };
            let active = writer.active_part_paths();
            listed
                .into_iter()
                .filter(|path| !active.contains(path) && path.exists())
                .collect()
        };
        // A conversion registers its temporary Parquet output before creating
        // it and releases it only after renaming or removing it, and output
        // names are not reused by this Stream object. Delete/recreate at the
        // same path is outside this registry's lifecycle. An unclaimed output is
        // therefore an orphan that no conversion will touch again.
        let candidates: Vec<PathBuf> = {
            let registry = lock_conversion_claims(&self.conversion_claims);
            candidates
                .into_iter()
                .filter(|path| !registry.outputs.contains_key(path))
                .collect()
        };
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::RecoveryCandidates(candidates.clone()));

        for path in candidates {
            // Check before zero-length handling too, independently of whether
            // the caller ran journal recovery first. A live writer is claimed
            // before creation, so a candidate cannot acquire a new journal.
            if has_journal_for_part(&path) {
                continue;
            }
            if path
                .extension()
                .is_some_and(|ext| ext == PART_FILE_EXTENSION)
            {
                info!(
                    "Found orphaned .part file: {:?} for stream {}",
                    path, self.stream_name
                );

                // Check if file is non-empty and potentially valid
                match path.metadata() {
                    Ok(meta) if meta.len() == 0 => {
                        warn!(
                            "Removing empty orphaned .part file: {:?} for stream {}",
                            path, self.stream_name
                        );
                        self.release_pending(std::slice::from_ref(&path));
                        if let Err(e) = remove_file(&path) {
                            error!("Failed to remove empty .part file {:?}: {e}", path);
                        }
                        continue;
                    }
                    Ok(meta) => {
                        if Self::is_parquet_part_file(&path) {
                            warn!(
                                "Removing orphaned temporary Parquet file {:?} for stream {}, size_bytes={}. Deleting it is safe because its source .arrows files remain in processing_* and will rebuild the Parquet on restart",
                                path,
                                self.stream_name,
                                meta.len()
                            );
                            if let Err(delete_err) = remove_file(&path) {
                                error!(
                                    "Failed to remove orphaned temporary Parquet file {:?}: {delete_err}",
                                    path
                                );
                            }
                            continue;
                        }

                        // Validate Arrow IPC structure without decoding every
                        // record batch. get_reverse_reader scans all message
                        // boundaries, excludes a crash-truncated tail, and
                        // requires a complete schema and record batch. Full
                        // body decoding happens once during Parquet conversion.
                        match File::open(&path) {
                            Ok(file) => {
                                let validation = get_reverse_reader(file);

                                if let Err(e) = validation {
                                    warn!(
                                        "Removing invalid/corrupted .part file: {:?} for stream {}, size_bytes={}: {e}",
                                        path,
                                        self.stream_name,
                                        meta.len()
                                    );
                                    // Discarded rows are not persisted.
                                    self.release_pending(std::slice::from_ref(&path));
                                    if let Err(delete_err) = remove_file(&path) {
                                        error!(
                                            "Failed to remove invalid/corrupted .part file {:?}: {delete_err}",
                                            path
                                        );
                                    }
                                } else {
                                    // File has at least one structurally complete batch.
                                    self.publish_recovered_part(&path);
                                }
                            }
                            Err(e) => {
                                error!("Failed to open .part file {:?} for validation: {e}", path);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Could not get metadata for .part file {:?}: {e}", path);
                    }
                }
            }
        }
    }

    /// Renames a validated orphan part to a free `.arrows` name. The name is
    /// chosen and taken under the writer lock: a writer whose first write
    /// failed is dropped (and renamed) inside push's critical section, and
    /// every other writer is dropped by a flush, which recovery excludes. So
    /// no other publisher can take the same name between the check and the
    /// rename, and nothing already published is replaced.
    fn publish_recovered_part(&self, path: &Path) {
        let mut arrow_path = if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(ARROW_PART_FILE_SUFFIX))
        {
            path.with_extension("")
        } else {
            // Legacy Arrow files used a bare `.part` suffix.
            path.with_extension(ARROW_FILE_EXTENSION)
        };

        let mut writer = match self.writer.lock() {
            Ok(writer) => writer,
            Err(poisoned) => {
                error!(
                    "Writer lock poisoned; leaving .part file {:?} for a later recovery - {poisoned}",
                    path
                );
                return;
            }
        };
        // If arrow file with same name exists, generate a unique name
        if arrow_path.exists() {
            let file_name = arrow_path.file_name().unwrap().to_string_lossy();
            let Some(date_pos) = file_name.find(".date") else {
                error!(
                    "Cannot choose a free name for orphaned .part file {:?}; {:?} exists, leaving it for a later recovery",
                    path, arrow_path
                );
                return;
            };
            let new_name = format!("{}{}", Ulid::new(), &file_name[date_pos..]);
            arrow_path.set_file_name(new_name);
        }
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::RecoveryPublishing(arrow_path.clone()));

        info!(
            "Recovering orphaned .part file: {:?} -> {:?} for stream {}",
            path, arrow_path, self.stream_name
        );
        // Keep pending rows keyed to the visible file, without re-locking writer.
        if let Some(mem) = writer.mem.as_mut() {
            mem.rekey_pending(path, &arrow_path);
        }
        if let Err(e) = std::fs::rename(path, &arrow_path) {
            if let Some(mem) = writer.mem.as_mut() {
                mem.rekey_pending(&arrow_path, path);
            }
            error!(
                "Failed to rename .part file {:?} to {:?}: {e}",
                path, arrow_path
            );
        }
    }

    /// Takes the part lifecycle lock exclusively. It guards no data, and only
    /// a panicked recovery can poison it, which leaves no writer detached, so
    /// poisoning is ignored.
    fn lock_part_lifecycle_for_recovery(&self) -> std::sync::RwLockWriteGuard<'_, ()> {
        #[cfg(test)]
        if matches!(
            self.part_lifecycle.try_write(),
            Err(std::sync::TryLockError::WouldBlock)
        ) {
            self.lifecycle_phase(LifecyclePhase::RecoveryWaitingForFlush);
        }
        self.part_lifecycle
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Recovers and flushes startup writers, then claims the processing
    /// directories no running conversion owns. Root Arrow files remain owned by
    /// periodic sync, which can start as soon as every stream is snapshotted.
    fn prepare_startup_sync(
        self: &Arc<Self>,
        tenant_id: Option<String>,
    ) -> Result<StartupSyncPlan, StagingError> {
        self.defer_unowned_inprocess_dirs();
        let _ = self.resolve_conversion_journals(&tenant_id);
        self.recover_orphan_part_files();
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::StartupRecovered);

        let start_flush = Instant::now();
        self.flush(true)?;
        if self.get_stream_type().eq(&StreamType::UserDefined) {
            info!(
                "Startup flush for stream ({}) took: {}s",
                self.stream_name,
                start_flush.elapsed().as_secs_f64()
            );
        }

        let now = SystemTime::now();
        let (claim, staging_files) =
            self.claim_conversion_groups(now, minute_from_system_time(now) - 1, true, false);
        #[cfg(test)]
        self.lifecycle_phase(LifecyclePhase::ConversionClaimed(staging_files.clone()));
        info!(
            "Captured {} startup parquet groups for stream {}",
            staging_files.len(),
            self.stream_name
        );
        Ok(StartupSyncPlan {
            stream: Arc::clone(self),
            tenant_id,
            staging_files,
            claim,
        })
    }

    /// First flushes arrows onto disk and then converts the arrow into parquet
    #[instrument(
        name = "flush_and_convert",
        level = "info",
        skip(self, tenant_id),
        fields(stream_name = %self.stream_name)
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn flush_and_convert(
        &self,
        init_signal: bool,
        shutdown_signal: bool,
        tenant_id: &Option<String>,
    ) -> Result<(), StagingError> {
        // Init must resolve journals before orphan cleanup. Reuse that result
        // for conversion below instead of inventorying the root a second time.
        let recovery = if init_signal {
            self.defer_unowned_inprocess_dirs();
            let recovery = self.resolve_conversion_journals(tenant_id);
            self.recover_orphan_part_files();
            Some(recovery)
        } else {
            None
        };

        let start_flush = Instant::now();
        // Force flush for init or shutdown signals to convert all .part files to .arrows
        // For regular cycles, use false to only flush non-current writers
        let forced = init_signal || shutdown_signal;
        self.flush(forced)?;
        if self.get_stream_type().eq(&StreamType::UserDefined) {
            info!(
                "Flushing stream ({}) took: {}s",
                self.stream_name,
                start_flush.elapsed().as_secs_f64()
            );
        }

        let start_convert = Instant::now();

        if let Some(recovery) = recovery {
            self.convert_after_journal_resolution(
                init_signal,
                shutdown_signal,
                tenant_id,
                None,
                recovery,
            )?;
        } else {
            self.prepare_parquet(init_signal, shutdown_signal, tenant_id)?;
        }
        if self.get_stream_type().eq(&StreamType::UserDefined) {
            info!(
                "Converting arrows to parquet on stream ({}) took: {}s",
                self.stream_name,
                start_convert.elapsed().as_secs_f64()
            );
        }

        Ok(())
    }
}

// #[derive(Deref, DerefMut, Default)]
// pub struct Streams(RwLock<HashMap<String, StreamRef>>);

#[derive(Deref, DerefMut, Default)]
pub struct Streams(RwLock<HashMap<String, HashMap<String, StreamRef>>>);

// PARSEABLE.streams should be updated
// 1. During server start up
// 2. When a new stream is created (make a new entry in the map)
// 3. When a stream is deleted (remove the entry from the map)
// 4. When first event is sent to stream (update the schema)
// 5. When set alert API is called (update the alert)
impl Streams {
    /// Checks after getting an exclusive lock whether the stream already exists, else creates it.
    /// NOTE: This is done to ensure we don't have contention among threads.
    pub fn get_or_create(
        &self,
        options: Arc<Options>,
        stream_name: String,
        metadata: LogStreamMetadata,
        ingestor_id: Option<String>,
        tenant_id: &Option<String>,
    ) -> StreamRef {
        let mut guard = self.write().expect(LOCK_EXPECT);

        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);

        if let Some(tenant_streams) = guard.get(tenant)
            && let Some(stream) = tenant_streams.get(&stream_name)
        {
            return stream.clone();
        }

        let stream = Stream::new(options, &stream_name, metadata, ingestor_id, tenant_id);
        guard
            .entry(tenant.to_owned())
            .or_default()
            .insert(stream_name, stream.clone());
        stream
    }

    /// Performs the short, exclusive startup preflight and returns immutable
    /// plans that can be converted while regular sync runs independently.
    pub(crate) fn prepare_startup_sync(&self) -> Vec<StartupSyncPlan> {
        let tenants = PARSEABLE
            .list_tenants()
            .unwrap_or_else(|| vec![DEFAULT_TENANT.to_owned()]);
        let mut plans = Vec::new();

        for tenant_id in tenants {
            let streams = self
                .read()
                .expect(LOCK_EXPECT)
                .get(&tenant_id)
                .map(|tenant_streams| tenant_streams.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            for stream in streams {
                match stream.prepare_startup_sync(Some(tenant_id.clone())) {
                    Ok(plan) => plans.push(plan),
                    Err(err) => error!(
                        "Failed to prepare startup sync for stream {}: {err:?}",
                        stream.stream_name
                    ),
                }
            }
        }

        plans
    }

    /// TODO: validate possibility of stream continuing to exist despite being deleted
    pub fn delete(&self, stream_name: &str, tenant_id: &Option<String>) {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let mut guard = self.write().expect(LOCK_EXPECT);
        if let Some(tenant_streams) = guard.get_mut(tenant_id) {
            tenant_streams.remove(stream_name);
        }
        // self.write().expect(LOCK_EXPECT).remove(stream_name);
    }

    pub fn contains(&self, stream_name: &str, tenant_id: &Option<String>) -> bool {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(tenant) = self.read().expect(LOCK_EXPECT).get(tenant_id) {
            tenant.contains_key(stream_name)
        } else {
            false
        }
    }

    /// Returns the number of logstreams that parseable is aware of
    pub fn len(&self) -> usize {
        self.read()
            .expect(LOCK_EXPECT)
            .iter()
            .map(|map| map.1.len())
            .sum()
    }

    /// Returns true if parseable is not aware of any streams
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Listing of logstream names for a given tenant that parseable is aware of
    pub fn list(&self, tenant_id: &Option<String>) -> Vec<LogStream> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);

        let guard = self.read().expect(LOCK_EXPECT);
        if let Some(tenant_streams) = guard.get(tenant_id) {
            tenant_streams.keys().map(String::clone).collect()
        } else {
            vec![]
        }
    }

    pub fn list_internal_streams(&self, tenant_id: &Option<String>) -> Vec<String> {
        let map = self.read().expect(LOCK_EXPECT);
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(tenant_streams) = map.get(tenant_id) {
            tenant_streams
                .iter()
                .filter(|(_, stream)| {
                    stream.metadata.read().expect(LOCK_EXPECT).stream_type == StreamType::Internal
                })
                .map(|(k, _)| k.clone())
                .collect()
        } else {
            vec![]
        }
    }

    /// Asynchronously flushes arrows and compacts into parquet data on all streams in staging,
    /// so that it is ready to be pushed onto objectstore.
    pub fn flush_and_convert(
        &self,
        joinset: &mut JoinSet<Result<(), StagingError>>,
        init_signal: bool,
        shutdown_signal: bool,
    ) {
        let tenants = if let Some(tenants) = PARSEABLE.list_tenants() {
            tenants
        } else {
            vec![DEFAULT_TENANT.to_owned()]
        };

        let handle = FLUSH_AND_CONVERT_RUNTIME.handle();
        for tenant_id in tenants {
            let guard = self.read().expect(LOCK_EXPECT);
            let streams: Vec<Arc<Stream>> = if let Some(tenant_streams) = guard.get(&tenant_id) {
                tenant_streams.values().map(Arc::clone).collect()
            } else {
                vec![]
            };
            for stream in streams {
                let tenant = tenant_id.clone();
                let span = info_span!("stream_sync", stream_name = %stream.stream_name);
                joinset.spawn_blocking_on(
                    move || {
                        let _guard = span.enter();
                        stream.flush_and_convert(init_signal, shutdown_signal, &Some(tenant))
                    },
                    handle,
                );
            }
        }
    }
}

fn lexsort_to_indices_rows(
    arrays: &[ArrayRef],
    sort_options: &[SortOptions],
) -> Result<UInt32Array, ArrowError> {
    if arrays.len() != sort_options.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Expected one sort option per array, got {} arrays and {} options",
            arrays.len(),
            sort_options.len()
        )));
    }

    let fields = arrays
        .iter()
        .zip(sort_options)
        .map(|(array, options)| SortField::new_with_options(array.data_type().clone(), *options))
        .collect();
    let converter = RowConverter::new(fields)?;
    let rows = converter.convert_columns(arrays)?;
    if rows.num_rows() > u32::MAX as usize {
        return Err(ArrowError::ComputeError(format!(
            "Cannot represent {} sort indices as UInt32",
            rows.num_rows()
        )));
    }

    let mut indices = (0..rows.num_rows()).collect::<Vec<_>>();
    indices.par_sort_unstable_by(|a, b| rows.row(*a).cmp(&rows.row(*b)));
    Ok(UInt32Array::from_iter_values(
        indices.into_iter().map(|index| index as u32),
    ))
}

#[cfg(test)]
mod crash_recovery_tests;

#[cfg(test)]
mod log_clustering_tests;

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Barrier, thread::spawn, time::Duration};

    use arrow_array::{Int32Array, StringArray, TimestampMillisecondArray};
    use arrow_ipc::writer::StreamWriter as ArrowStreamWriter;
    use arrow_schema::{DataType, Field, TimeUnit};
    use chrono::{NaiveDate, TimeDelta, Utc};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use temp_dir::TempDir;
    use tokio::time::sleep;

    use super::*;

    #[test]
    fn log_conversion_pool_is_bounded_and_named() {
        assert_eq!(log_conversion_threads(None, 1), 2);
        assert_eq!(log_conversion_threads(None, 4), 2);
        assert_eq!(log_conversion_threads(None, 10), 5);
        assert_eq!(log_conversion_threads(None, 64), 8);
        assert_eq!(log_conversion_threads(Some("3"), 64), 3);
        assert_eq!(log_conversion_threads(Some(" 12 "), 4), 12);
        // Invalid overrides fall back to the default
        assert_eq!(log_conversion_threads(Some("0"), 64), 8);
        assert_eq!(log_conversion_threads(Some("many"), 4), 2);

        // Parallel work inside the pool, nested included, runs on its own
        // threads and never on the global Rayon pool.
        let names: Vec<String> = LOG_CONVERSION_THREADPOOL.install(|| {
            (0..64)
                .into_par_iter()
                .flat_map_iter(|_| {
                    (0..4)
                        .into_par_iter()
                        .map(|_| std::thread::current().name().unwrap_or_default().to_owned())
                        .collect::<Vec<_>>()
                })
                .collect()
        });
        assert!(
            names.iter().all(|name| name.starts_with("log-conversion-")),
            "{names:?}"
        );
        assert_eq!(
            LOG_CONVERSION_THREADPOOL.current_num_threads(),
            log_conversion_threads(
                std::env::var(LOG_CONVERSION_THREADS_VAR).ok().as_deref(),
                num_cpus::get()
            )
        );
    }

    #[test]
    fn ingest_snapshot_shares_schema_until_it_changes() {
        let dir = TempDir::new().unwrap();
        let stream = Stream::new(
            Arc::new(Options {
                local_staging_path: dir.path().to_path_buf(),
                ..Default::default()
            }),
            "ingest-snapshot",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let a = Arc::new(Field::new("a", DataType::Utf8, true));
        let b = Arc::new(Field::new("b", DataType::Int64, true));
        {
            let mut metadata = stream.metadata.write().unwrap();
            metadata.schema.insert("a".into(), a.clone());
            metadata.schema.insert("b".into(), b.clone());
            metadata.custom_partition = Some("a".into());
            metadata.static_schema_flag = true;
            metadata.infer_timestamp = false;
            metadata.schema_version = SchemaVersion::V1;
            metadata.time_partition_limit = NonZeroU32::new(3);
        }
        let first = stream.ingest_snapshot();
        assert_eq!(*first.schema, stream.get_schema_raw());
        assert_eq!(first.custom_partition, stream.get_custom_partition());
        assert_eq!(first.static_schema_flag, stream.get_static_schema_flag());
        assert_eq!(first.infer_timestamp, stream.get_infer_timestamp());
        assert_eq!(first.schema_version, stream.get_schema_version());
        assert_eq!(
            first.time_partition_limit,
            stream.get_time_partition_limit()
        );
        // Unchanged: the same map is shared
        let second = stream.ingest_snapshot();
        assert!(Arc::ptr_eq(&first.schema, &second.schema));

        // An added field
        let c = Arc::new(Field::new("c", DataType::Boolean, true));
        stream
            .metadata
            .write()
            .unwrap()
            .schema
            .insert("c".into(), c.clone());
        let third = stream.ingest_snapshot();
        assert!(!Arc::ptr_eq(&second.schema, &third.schema));
        assert_eq!(*third.schema, stream.get_schema_raw());
        assert_eq!(
            *second.schema,
            HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())])
        );

        // A field replaced in place under the same name, same length
        let b_float = Arc::new(Field::new("b", DataType::Float64, true));
        stream
            .metadata
            .write()
            .unwrap()
            .schema
            .insert("b".into(), b_float.clone());
        let fourth = stream.ingest_snapshot();
        assert_eq!(fourth.schema.len(), third.schema.len());
        assert!(!Arc::ptr_eq(&third.schema, &fourth.schema));
        assert!(Arc::ptr_eq(&fourth.schema["b"], &b_float));

        // The schema replaced wholesale with equal entries: still correct
        let replacement = stream.get_schema_raw();
        stream.metadata.write().unwrap().schema = replacement;
        let fifth = stream.ingest_snapshot();
        assert_eq!(*fifth.schema, stream.get_schema_raw());
        let sixth = stream.ingest_snapshot();
        assert!(Arc::ptr_eq(&fifth.schema, &sixth.schema));

        // Cleared
        stream.metadata.write().unwrap().schema.clear();
        assert!(stream.ingest_snapshot().schema.is_empty());
    }

    #[test]
    fn bloom_filter_conversion_round_trip() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        // Exercise the regular, custom partition (delta encoding), and clustered
        // conversion paths, including a final short row group.
        for (enabled, clustered, partitioned) in [
            (false, false, false),
            (true, false, false),
            (true, false, true),
            (true, true, false),
        ] {
            let dir = TempDir::new().unwrap();
            let staging = Stream::new(
                Arc::new(Options {
                    local_staging_path: dir.path().to_path_buf(),
                    row_group_size: 4,
                    parquet_bloom_filter_columns: if enabled {
                        vec!["value".into(), "missing".into()]
                    } else {
                        vec![]
                    },
                    log_severity_cluster_column: clustered.then(|| "value".into()),
                    ..Default::default()
                }),
                "bloom-roundtrip",
                LogStreamMetadata::default(),
                None,
                &None,
            );
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    DEFAULT_TIMESTAMP_KEY,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(TimestampMillisecondArray::from_iter_values(0..11)),
                    Arc::new(Int32Array::from_iter_values(0..11)),
                    Arc::new(StringArray::from_iter_values(
                        (0..11).map(|i| if i % 2 == 0 { "a" } else { "z" }),
                    )),
                ],
            )
            .unwrap();
            staging
                .push(
                    "abc",
                    &batch,
                    (Utc::now() - TimeDelta::minutes(2)).naive_utc(),
                    &HashMap::new(),
                    StreamType::UserDefined,
                )
                .unwrap();
            staging.flush(true).unwrap();
            let partition = partitioned.then(|| "value".to_string());
            staging
                .convert_disk_files_to_parquet(None, partition.as_ref(), false, true, &None)
                .unwrap();
            let paths = staging.parquet_files();
            assert_eq!(paths.len(), 1);
            let read_options = parquet::file::serialized_reader::ReadOptionsBuilder::new()
                .with_reader_properties(
                    parquet::file::properties::ReaderProperties::builder()
                        .set_read_bloom_filter(true)
                        .build(),
                )
                .build();
            let reader = SerializedFileReader::new_with_options(
                File::open(&paths[0]).unwrap(),
                read_options,
            )
            .unwrap();
            assert_eq!(reader.num_row_groups(), 3);
            for i in 0..3 {
                let rg = reader.get_row_group(i).unwrap();
                assert_eq!(rg.metadata().num_rows(), if i == 2 { 3 } else { 4 });
                for col in 0..3 {
                    assert_eq!(
                        rg.metadata().column(col).bloom_filter_offset().is_some(),
                        enabled && col == 2
                    );
                }
                assert_eq!(rg.metadata().sorting_columns().is_none(), clustered);
                if enabled {
                    let filter = rg.get_column_bloom_filter(2).unwrap();
                    assert!(filter.check(&"a"));
                    assert!(filter.check(&"z"));
                    assert!(!filter.check(&"m")); // absent, inside every group's min/max
                }
            }
            let batches = ParquetRecordBatchReaderBuilder::try_new(File::open(&paths[0]).unwrap())
                .unwrap()
                .build()
                .unwrap();
            assert_eq!(batches.map(|b| b.unwrap().num_rows()).sum::<usize>(), 11);
        }
    }

    #[test]
    fn bloom_filter_folds_short_and_low_cardinality_groups() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let dir = TempDir::new().unwrap();
        let staging = Stream::new(
            Arc::new(Options {
                row_group_size: 4096,
                // Names are trimmed, as in "id, level"
                parquet_bloom_filter_columns: vec!["id".into(), " level ".into()],
                ..Default::default()
            }),
            "bloom-folding",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("level", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(
                    (0..4099).map(|i| format!("{i:032x}")),
                )),
                Arc::new(StringArray::from(vec!["INFO"; 4099])),
            ],
        )
        .unwrap();
        let props = staging.parquet_writer_props(&schema, None, None, true);
        // Unless `end` is asked for, filters are written, and released, per row group
        assert_eq!(
            props.bloom_filter_position(),
            BloomFilterPosition::AfterRowGroup
        );
        for name in ["id", "level"] {
            assert!(
                props
                    .bloom_filter_properties(&ColumnPath::new(vec![name.into()]))
                    .is_some(),
                "{name}"
            );
        }
        let path = dir.path().join("folded.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            Some(staging.parquet_writer_props(&schema, None, None, true)),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        let large = reader
            .metadata()
            .row_group(0)
            .column(0)
            .bloom_filter_length()
            .unwrap();
        let short = reader
            .metadata()
            .row_group(1)
            .column(0)
            .bloom_filter_length()
            .unwrap();
        let low_ndv = reader
            .metadata()
            .row_group(0)
            .column(1)
            .bloom_filter_length()
            .unwrap();
        assert!(large > short);
        assert_eq!(short, low_ndv);
        assert!(short < 100); // minimum 32-byte bitset plus compact Thrift header
    }

    #[test]
    fn bloom_filter_metrics_properties_unchanged() {
        let metadata = LogStreamMetadata {
            log_source: vec![LogSourceEntry::new(LogSource::OtelMetrics, HashSet::new())],
            ..Default::default()
        };
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("trace_id", DataType::Utf8, false),
        ]);
        let original = Stream::new(
            Arc::new(Options {
                row_group_size: 4,
                bloom_filter_default_position: true,
                ..Default::default()
            }),
            "metrics-before",
            metadata.clone(),
            None,
            &None,
        );
        let configured = Stream::new(
            Arc::new(Options {
                row_group_size: 4,
                bloom_filter_default_position: true,
                parquet_bloom_filter_columns: vec!["trace_id".into(), "metric_name".into()],
                parquet_bloom_filter_position: "end".into(),
                ..Default::default()
            }),
            "metrics-after",
            metadata,
            None,
            &None,
        );
        let before = original.parquet_writer_props(&schema, None, None, false);
        let after = configured.parquet_writer_props(&schema, None, None, false);
        assert_eq!(
            after.bloom_filter_position(),
            BloomFilterPosition::AfterRowGroup
        );
        assert_eq!(
            before.max_row_group_row_count(),
            after.max_row_group_row_count()
        );
        assert_eq!(before.sorting_columns(), after.sorting_columns());
        assert_eq!(
            before.bloom_filter_position(),
            after.bloom_filter_position()
        );
        for field in schema.fields() {
            let path = ColumnPath::new(vec![field.name().clone()]);
            assert_eq!(
                before.bloom_filter_properties(&path),
                after.bloom_filter_properties(&path)
            );
            assert_eq!(before.encoding(&path), after.encoding(&path));
            assert_eq!(before.compression(&path), after.compression(&path));
            assert_eq!(
                before.dictionary_enabled(&path),
                after.dictionary_enabled(&path)
            );
        }
        assert!(
            after
                .bloom_filter_properties(&ColumnPath::from("trace_id"))
                .is_none()
        );
        assert_eq!(
            after
                .bloom_filter_properties(&ColumnPath::from("metric_name"))
                .unwrap()
                .ndv(),
            METRIC_NAME_BLOOM_FILTER_NDV
        );
    }

    #[test]
    fn current_minute_query_visibility_query_mode_flushes_ownerless_memory() {
        let dir = TempDir::new().unwrap();
        let staging = Stream::new(
            Arc::new(Options {
                local_staging_path: dir.path().to_path_buf(),
                mode: Mode::Query,
                ..Default::default()
            }),
            "query-memory",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        // Keep memory coverage independent of the process-wide staging setting.
        staging.writer.lock().unwrap().mem = Some(Default::default());
        let now = DateTime::parse_from_rfc3339("2020-01-02T12:34:30.500Z")
            .unwrap()
            .with_timezone(&Utc);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        staging
            .push(
                "id-schema",
                &batch,
                now.naive_utc(),
                &HashMap::new(),
                StreamType::UserDefined,
            )
            .unwrap();
        {
            let writer = staging.writer.lock().unwrap();
            assert!(writer.disk.is_empty());
            let mem = writer.mem.as_ref().unwrap();
            assert!(mem.read_buffer.is_empty());
            assert_eq!(mem.mutable_buffer.inner.len(), 1);
            assert!(Arc::ptr_eq(
                mem.mutable_buffer.inner[0].column(0),
                batch.column(0)
            ));
        }
        assert_eq!(
            staging.recordbatches_cloned(&schema).unwrap()[0].num_rows(),
            1
        );
        staging.flush_with_clock(false, || now).unwrap();
        assert!(staging.recordbatches_cloned(&schema).unwrap().is_empty());
    }

    #[test]
    fn current_minute_query_visibility_query_internal_exact_minute_end() {
        let dir = TempDir::new().unwrap();
        let staging = Stream::new(
            Arc::new(Options {
                local_staging_path: dir.path().to_path_buf(),
                mode: Mode::Query,
                ..Default::default()
            }),
            "query-internal",
            LogStreamMetadata {
                stream_type: StreamType::Internal,
                ..Default::default()
            },
            None,
            &None,
        );
        staging.writer.lock().unwrap().mem = Some(Default::default());
        let now = DateTime::parse_from_rfc3339("2020-01-02T12:34:30.500Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = TimeRange::granularity_range(now, OBJECT_STORE_DATA_GRANULARITY).end;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![7]))])
            .unwrap();
        for _ in 0..2 {
            staging
                .push(
                    "id-schema",
                    &batch,
                    now.naive_utc(),
                    &HashMap::new(),
                    StreamType::Internal,
                )
                .unwrap();
        }
        staging.flush_with_clock(false, || now).unwrap();
        {
            let writer = staging.writer.lock().unwrap();
            assert_eq!(writer.disk.len(), 1);
            let mem = writer.mem.as_ref().unwrap();
            assert!(mem.read_buffer.is_empty());
            assert!(mem.mutable_buffer.inner.is_empty());
        }
        assert_eq!(
            staging.recordbatches_cloned(&schema).unwrap()[0].num_rows(),
            2
        );
        let next = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![8]))])
            .unwrap();
        staging
            .push(
                "id-schema",
                &next,
                end.naive_utc(),
                &HashMap::new(),
                StreamType::Internal,
            )
            .unwrap();
        assert_eq!(staging.writer.lock().unwrap().disk.len(), 2);
        staging.flush_with_clock(false, || end).unwrap();
        let expected = staging.filename_by_partition("id-schema", end.naive_utc(), &HashMap::new());
        {
            let writer = staging.writer.lock().unwrap();
            assert_eq!(writer.disk.len(), 1);
            assert!(writer.disk.contains_key(&expected));
        }
        // The detached minute stays queryable until its Parquet is published.
        assert_eq!(captured_ids(&staging, &schema), (vec![7, 7, 8], vec![]));
        staging.flush_with_clock(true, || end).unwrap();
        assert!(staging.writer.lock().unwrap().disk.is_empty());
        assert_eq!(captured_ids(&staging, &schema), (vec![7, 7, 8], vec![]));
    }

    #[test]
    fn current_minute_query_visibility_survives_nonforced_flush() {
        // A fixed reference clock tests the production push/flush/query path without
        // relying on a wall-clock minute boundary or retrying the test.
        let now = DateTime::parse_from_rfc3339("2020-01-02T12:34:30.500Z")
            .unwrap()
            .with_timezone(&Utc);
        let dir = TempDir::new().unwrap();
        let staging = Stream::new(
            Arc::new(Options {
                local_staging_path: dir.path().to_path_buf(),
                ..Default::default()
            }),
            "visibility",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        staging.writer.lock().unwrap().mem = Some(Default::default());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        for (id, timestamp, partition) in [
            (1, now - TimeDelta::minutes(2), "past"),
            (2, now, "a"),
            (3, now, "b"),
            (4, now + TimeDelta::minutes(2), "future"),
        ] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![id]))])
                    .unwrap();
            staging
                .push(
                    "id-schema",
                    &batch,
                    timestamp.naive_utc(),
                    &HashMap::from([("tenant".to_owned(), partition.to_owned())]),
                    StreamType::UserDefined,
                )
                .unwrap();
        }
        let ids = || {
            let mut ids = staging
                .recordbatches_cloned(&schema)
                .unwrap()
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids
        };
        assert_eq!(ids(), vec![1, 2, 3, 4]);
        staging.flush_with_clock(false, || now).unwrap();
        assert_eq!(staging.writer.lock().unwrap().disk.len(), 2);
        assert_eq!(
            ids(),
            vec![1, 2, 3, 4],
            "attached and detached-but-unpublished writers must remain queryable"
        );
        staging.flush_with_clock(false, || now).unwrap();
        assert_eq!(
            ids(),
            vec![1, 2, 3, 4],
            "a second flush must neither lose nor duplicate rows"
        );
        staging.clear().unwrap();
        assert!(ids().is_empty());
        assert_eq!(staging.writer.lock().unwrap().disk.len(), 2);
        let next = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![5]))])
            .unwrap();
        staging
            .push(
                "id-schema",
                &next,
                now.naive_utc(),
                &HashMap::from([("tenant".to_owned(), "a".to_owned())]),
                StreamType::UserDefined,
            )
            .unwrap();
        assert_eq!(ids(), vec![5]);
        staging.flush_with_clock(true, || now).unwrap();
        assert_eq!(ids(), vec![5]);
        assert!(staging.writer.lock().unwrap().disk.is_empty());
        // A late append for a previously detached file starts a fresh segment.
        staging
            .push(
                "id-schema",
                &next,
                (now - TimeDelta::minutes(2)).naive_utc(),
                &HashMap::from([("tenant".to_owned(), "past".to_owned())]),
                StreamType::UserDefined,
            )
            .unwrap();
        assert_eq!(ids(), vec![5, 5]);
        staging.flush_with_clock(false, || now).unwrap();
        assert_eq!(ids(), vec![5, 5]);
        assert!(staging.writer.lock().unwrap().disk.is_empty());
    }

    /// Sorted ids from one coherent capture: (memory, staging Parquet).
    fn captured_ids(staging: &Stream, schema: &Arc<Schema>) -> (Vec<i32>, Vec<i32>) {
        fn ids(batches: impl IntoIterator<Item = RecordBatch>) -> Vec<i32> {
            let mut ids = batches
                .into_iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name("id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids
        }
        // Like a query, only list under the capture; read after releasing it.
        let (memory, parquet_paths) = staging
            .capture_staging(schema, |dir| {
                fs::read_dir(dir)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
                    .collect::<Vec<_>>()
            })
            .unwrap();
        let parquet = parquet_paths.into_iter().flat_map(|path| {
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .map(Result::unwrap)
        });
        (ids(memory), ids(parquet))
    }

    /// Bounds every cross-thread wait so a failing converter cannot hang a test.
    const CONVERSION_WAIT: Duration = Duration::from_secs(30);

    fn conversion_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2020-01-02T12:34:30.500Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn conversion_stream(dir: &TempDir, name: &str) -> (StreamRef, Arc<Schema>) {
        let staging = Stream::new(
            Arc::new(Options {
                local_staging_path: dir.path().to_path_buf(),
                row_group_size: 1048576,
                ..Default::default()
            }),
            name,
            LogStreamMetadata::default(),
            None,
            &None,
        );
        staging.writer.lock().unwrap().mem = Some(Default::default());
        // Merging several Arrow sources orders them by the leading timestamp.
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
        ]));
        (staging, schema)
    }

    fn push_ids(
        staging: &Stream,
        schema: &Arc<Schema>,
        schema_key: &str,
        ids: Vec<i32>,
        timestamp: DateTime<Utc>,
    ) {
        let times = vec![timestamp.timestamp_millis(); ids.len()];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(times)),
                Arc::new(Int32Array::from(ids)),
            ],
        )
        .unwrap();
        staging
            .push(
                schema_key,
                &batch,
                timestamp.naive_utc(),
                &HashMap::new(),
                StreamType::UserDefined,
            )
            .unwrap();
    }

    fn part_files(staging: &Stream) -> Vec<PathBuf> {
        let mut parts = fs::read_dir(&staging.data_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|ext| ext == PART_FILE_EXTENSION)
            })
            .collect::<Vec<_>>();
        parts.sort();
        parts
    }

    fn conversion_test_stream(dir: &TempDir, name: &str) -> (StreamRef, Arc<Schema>) {
        let (staging, schema) = conversion_stream(dir, name);
        let now = conversion_now();
        // Two writer keys (schema keys) for one minute share an output group;
        // the third is a separate output, so publishing one group must not
        // release the other.
        for (ids, minute, schema_key) in [
            (vec![1, 2], 0, "schema-a"),
            (vec![3], 0, "schema-b"),
            (vec![4, 5, 6], 1, "schema-a"),
        ] {
            push_ids(
                &staging,
                &schema,
                schema_key,
                ids,
                now + TimeDelta::minutes(minute),
            );
        }
        assert_eq!(
            captured_ids(&staging, &schema),
            (vec![1, 2, 3, 4, 5, 6], vec![])
        );
        // Detach every writer: rows are sealed in root Arrow files only.
        staging.flush_with_clock(true, || now).unwrap();
        assert!(staging.writer.lock().unwrap().disk.is_empty());
        assert_eq!(staging.arrow_files().len(), 3);
        (staging, schema)
    }

    fn set_conversion_hook(
        staging: &Stream,
        hook: impl Fn(ConversionPhase<'_>) + Send + Sync + 'static,
    ) {
        *staging.conversion_hook.lock().unwrap() = Some(Arc::new(hook));
    }

    #[test]
    fn conversion_visibility_rows_counted_once_across_publication() {
        let dir = TempDir::new().unwrap();
        let (staging, schema) = conversion_test_stream(&dir, "publication");
        let all = vec![1, 2, 3, 4, 5, 6];
        assert_eq!(captured_ids(&staging, &schema), (all.clone(), vec![]));

        let (paused_tx, paused_rx) = std::sync::mpsc::channel::<()>();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        let (published_tx, published_rx) = std::sync::mpsc::channel::<bool>();
        let resume_rx = Mutex::new(resume_rx);
        let weak = Arc::downgrade(&staging);
        set_conversion_hook(&staging, move |phase| match phase {
            ConversionPhase::BeforePublish(_) => {
                paused_tx.send(()).unwrap();
                resume_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(CONVERSION_WAIT)
                    .expect("test did not resume publication");
            }
            ConversionPhase::Published => {
                // The Parquet is visible and its sources' memory is not yet
                // released: query capture must be excluded right now.
                let stream = weak.upgrade().unwrap();
                published_tx
                    .send(stream.publication.try_read().is_err())
                    .unwrap();
            }
            _ => {}
        });

        let converter = {
            let staging = Arc::clone(&staging);
            spawn(move || staging.prepare_parquet(false, true, &None))
        };
        let paused = || {
            paused_rx
                .recv_timeout(CONVERSION_WAIT)
                .expect("conversion did not reach publication")
        };
        let published = || {
            published_rx
                .recv_timeout(CONVERSION_WAIT)
                .expect("conversion did not publish")
        };
        // Each output group pauses before publication with its sources moved
        // to the processing directory and its Parquet part fully written.
        paused();
        assert!(staging.arrow_files().is_empty());
        assert_eq!(staging.inprocess_arrow_files().len(), 3);
        assert_eq!(captured_ids(&staging, &schema), (all.clone(), vec![]));
        resume_tx.send(()).unwrap();
        assert!(published(), "publication must exclude capture");
        // This capture waits for the publication above to release its sources.
        // One group is published, the other is not: every row exactly once.
        let (memory, parquet) = captured_ids(&staging, &schema);
        assert!(
            (memory == [1, 2, 3] && parquet == [4, 5, 6])
                || (memory == [4, 5, 6] && parquet == [1, 2, 3]),
            "memory={memory:?} parquet={parquet:?}"
        );
        paused();
        resume_tx.send(()).unwrap();
        assert!(published(), "publication must exclude capture");
        converter.join().unwrap().unwrap();

        assert_eq!(captured_ids(&staging, &schema), (vec![], all));
        assert!(staging.inprocess_arrow_files().is_empty());
    }

    #[test]
    fn conversion_capture_leases_wait_for_publication_and_survive_source_deletion() {
        use crate::query::stream_schema_provider::staging_parquet::capture_staging_parquet_for_test;
        use std::sync::mpsc::{TryRecvError, channel};

        let dir = TempDir::new().unwrap();
        let (staging, schema) = conversion_test_stream(&dir, "publication-lease");
        let (before_tx, before_rx) = channel();
        let (before_resume_tx, before_resume_rx) = channel();
        let before_resume_rx = Mutex::new(before_resume_rx);
        let (published_tx, published_rx) = channel();
        let (published_resume_tx, published_resume_rx) = channel();
        let published_resume_rx = Mutex::new(published_resume_rx);
        let weak = Arc::downgrade(&staging);
        set_conversion_hook(&staging, move |phase| match phase {
            ConversionPhase::BeforePublish(_) => {
                before_tx.send(()).unwrap();
                before_resume_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(CONVERSION_WAIT)
                    .expect("test did not resume conversion before publication");
            }
            ConversionPhase::Published => {
                assert!(weak.upgrade().unwrap().publication.try_read().is_err());
                published_tx.send(()).unwrap();
                published_resume_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(CONVERSION_WAIT)
                    .expect("test did not resume published conversion");
            }
            _ => {}
        });
        let (done_tx, done_rx) = channel();
        let converter = {
            let staging = Arc::clone(&staging);
            spawn(move || {
                let result = staging.prepare_parquet(false, true, &None);
                done_tx.send(()).unwrap();
                result
            })
        };
        before_rx.recv_timeout(CONVERSION_WAIT).unwrap();
        before_resume_tx.send(()).unwrap();
        published_rx.recv_timeout(CONVERSION_WAIT).unwrap();
        // Renamed Parquet and pending memory coexist under the write lock.

        let (started_tx, started_rx) = channel();
        let (capture_tx, capture_rx) = channel();
        let capture = {
            let staging = Arc::clone(&staging);
            let schema = Arc::clone(&schema);
            spawn(move || {
                // This lock assertion makes exclusion deterministic even if the
                // worker is descheduled before entering capture_staging.
                assert!(staging.publication.try_read().is_err());
                started_tx.send(()).unwrap();
                capture_tx
                    .send(staging.capture_staging(&schema, capture_staging_parquet_for_test))
                    .unwrap();
            })
        };
        started_rx.recv_timeout(CONVERSION_WAIT).unwrap();
        assert!(matches!(capture_rx.try_recv(), Err(TryRecvError::Empty)));
        published_resume_tx.send(()).unwrap();
        // The other group has its own before-publication gate, so its rows
        // remain pending even when both groups convert concurrently.
        before_rx.recv_timeout(CONVERSION_WAIT).unwrap();
        let (memory, files) = capture_rx.recv_timeout(CONVERSION_WAIT).unwrap().unwrap();
        let files = files.unwrap();
        capture.join().unwrap();
        assert_eq!(files.len(), 1);
        assert!(!memory.is_empty());

        let originals = fs::read_dir(&staging.data_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
            .collect::<Vec<_>>();
        assert_eq!(originals.len(), 1);
        for original in originals {
            fs::remove_file(original).unwrap();
        }
        let leased = files
            .iter()
            .map(|file| PathBuf::from("/").join(file.object_meta.location.as_ref()))
            .collect::<Vec<_>>();
        let lease_dirs = leased
            .iter()
            .map(|path| path.parent().unwrap().to_path_buf())
            .collect::<Vec<_>>();
        assert!(lease_dirs.iter().all(|path| path.is_dir()));
        let mut ids = memory
            .into_iter()
            .chain(leased.iter().flat_map(|path| {
                ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                    .unwrap()
                    .build()
                    .unwrap()
                    .map(Result::unwrap)
            }))
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
        drop(files);
        assert!(lease_dirs.iter().all(|path| !path.exists()));

        before_resume_tx.send(()).unwrap();
        published_rx.recv_timeout(CONVERSION_WAIT).unwrap();
        published_resume_tx.send(()).unwrap();
        // Both completion and hook waits are bounded before joining.
        done_rx.recv_timeout(CONVERSION_WAIT).unwrap();
        converter.join().unwrap().unwrap();
    }

    #[test]
    fn conversion_visibility_failed_publication_retains_rows_for_retry() {
        let dir = TempDir::new().unwrap();
        let (staging, schema) = conversion_test_stream(&dir, "failed-publication");
        let all = vec![1, 2, 3, 4, 5, 6];
        let blockers = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&blockers);
        set_conversion_hook(&staging, move |phase| {
            if let ConversionPhase::BeforePublish(parquet) = phase {
                // A non-empty directory at the target makes the real rename fail.
                fs::create_dir(parquet).unwrap();
                fs::write(parquet.join("blocker"), b"x").unwrap();
                recorded.lock().unwrap().push(parquet.to_path_buf());
            }
        });
        assert!(staging.prepare_parquet(false, true, &None).is_err());
        let blockers = std::mem::take(&mut *blockers.lock().unwrap());
        assert_eq!(blockers.len(), 2);
        for blocker in blockers {
            fs::remove_dir_all(blocker).unwrap();
        }
        // Sources stay in processing directories and their rows stay in memory.
        assert_eq!(staging.inprocess_arrow_files().len(), 3);
        assert_eq!(captured_ids(&staging, &schema), (all.clone(), vec![]));

        // Startup retry converts the retained processing directories.
        *staging.conversion_hook.lock().unwrap() = None;
        staging.prepare_parquet(true, false, &None).unwrap();
        assert_eq!(captured_ids(&staging, &schema), (vec![], all));
        assert!(staging.inprocess_arrow_files().is_empty());
    }

    #[test]
    fn conversion_visibility_late_append_after_failed_seal_rename() {
        let dir = TempDir::new().unwrap();
        let (staging, schema) = conversion_stream(&dir, "failed-seal");
        let now = conversion_now();
        push_ids(&staging, &schema, "schema-a", vec![1, 2], now);
        let blockers = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&blockers);
        set_conversion_hook(&staging, move |phase| {
            if let ConversionPhase::BeforeSealRename(arrow) = phase {
                // A non-empty directory at the target makes the real rename fail.
                fs::create_dir(arrow).unwrap();
                fs::write(arrow.join("blocker"), b"x").unwrap();
                recorded.lock().unwrap().push(arrow.to_path_buf());
            }
        });
        staging.flush_with_clock(true, || now).unwrap();
        *staging.conversion_hook.lock().unwrap() = None;
        let blockers = std::mem::take(&mut *blockers.lock().unwrap());
        assert_eq!(blockers.len(), 1);
        fs::remove_dir_all(&blockers[0]).unwrap();
        // The failed part is left for recovery; its rows stay queryable.
        let failed = part_files(&staging);
        assert_eq!(failed.len(), 1);
        let failed_len = fs::metadata(&failed[0]).unwrap().len();
        assert!(staging.arrow_files().is_empty());
        assert_eq!(captured_ids(&staging, &schema), (vec![1, 2], vec![]));

        // A late event for the same writer key and minute must not reuse,
        // truncate or share the failed part.
        push_ids(&staging, &schema, "schema-a", vec![3], now);
        let parts = part_files(&staging);
        assert_eq!(parts.len(), 2);
        assert!(parts.contains(&failed[0]));
        assert_eq!(fs::metadata(&failed[0]).unwrap().len(), failed_len);
        staging.flush_with_clock(true, || now).unwrap();
        assert_eq!(part_files(&staging), failed);
        assert_eq!(staging.arrow_files().len(), 1);

        // Publishing the late file releases only its own rows.
        staging.prepare_parquet(false, true, &None).unwrap();
        assert_eq!(captured_ids(&staging, &schema), (vec![1, 2], vec![3]));
        assert_eq!(part_files(&staging), failed);

        // Startup recovery on the same Stream exposes the failed part as Arrow;
        // its rows must follow it and be published exactly once.
        startup_then_shutdown_conversion(&staging);
        assert!(part_files(&staging).is_empty());
        assert_eq!(captured_ids(&staging, &schema), (vec![], vec![1, 2, 3]));
    }

    /// Runs startup preparation and its plan, then a shutdown conversion that
    /// converts every root Arrow file, as a restart followed by sync would.
    fn startup_then_shutdown_conversion(staging: &StreamRef) {
        staging
            .prepare_startup_sync(None)
            .unwrap()
            .execute()
            .unwrap();
        staging.prepare_parquet(false, true, &None).unwrap();
    }

    #[test]
    fn conversion_visibility_startup_recovery_skips_attached_writer() {
        let dir = TempDir::new().unwrap();
        let (staging, schema) = conversion_stream(&dir, "startup-attached");
        // Accepted before startup recovery reaches this stream: the writer is
        // still attached and its part is live, not an orphan.
        push_ids(&staging, &schema, "schema-a", vec![1, 2], conversion_now());
        assert_eq!(part_files(&staging).len(), 1);
        assert_eq!(captured_ids(&staging, &schema), (vec![1, 2], vec![]));

        startup_then_shutdown_conversion(&staging);
        assert!(part_files(&staging).is_empty());
        assert!(staging.arrow_files().is_empty());
        assert_eq!(captured_ids(&staging, &schema), (vec![], vec![1, 2]));
    }

    #[test]
    fn conversion_visibility_recovery_discard_releases_pending_rows() {
        let dir = TempDir::new().unwrap();
        let (staging, schema) = conversion_stream(&dir, "recovery-discard");
        let now = conversion_now();
        push_ids(&staging, &schema, "schema-a", vec![1, 2], now);
        let blockers = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&blockers);
        set_conversion_hook(&staging, move |phase| {
            if let ConversionPhase::BeforeSealRename(arrow) = phase {
                fs::create_dir(arrow).unwrap();
                fs::write(arrow.join("blocker"), b"x").unwrap();
                recorded.lock().unwrap().push(arrow.to_path_buf());
            }
        });
        staging.flush_with_clock(true, || now).unwrap();
        *staging.conversion_hook.lock().unwrap() = None;
        for blocker in std::mem::take(&mut *blockers.lock().unwrap()) {
            fs::remove_dir_all(blocker).unwrap();
        }
        let failed = part_files(&staging);
        assert_eq!(failed.len(), 1);
        assert_eq!(captured_ids(&staging, &schema), (vec![1, 2], vec![]));

        // Recovery discards a part with no complete batch; its rows are not
        // persisted, so memory must stop serving them too.
        fs::write(&failed[0], b"bad").unwrap();
        startup_then_shutdown_conversion(&staging);
        assert!(part_files(&staging).is_empty());
        assert_eq!(captured_ids(&staging, &schema), (vec![], vec![]));
    }

    #[test]
    fn conversion_visibility_overlapping_detach_keeps_part_ownership() {
        let dir = TempDir::new().unwrap();
        let (staging, schema) = conversion_stream(&dir, "overlapping-detach");
        let now = conversion_now();
        push_ids(&staging, &schema, "schema-a", vec![1, 2], now);
        // A flush detached this writer but has not sealed it yet.
        let mut detached = staging
            .writer
            .lock()
            .unwrap()
            .detach_flushable_disk_at(true, now);
        assert_eq!(detached.len(), 1);
        let first = part_files(&staging);
        assert_eq!(first.len(), 1);

        // A late event opens a second writer for the same key and minute, and
        // an overlapping flush seals it first.
        push_ids(&staging, &schema, "schema-a", vec![3], now);
        assert_eq!(part_files(&staging).len(), 2);
        assert_eq!(captured_ids(&staging, &schema), (vec![1, 2, 3], vec![]));
        staging.flush_with_clock(true, || now).unwrap();
        assert_eq!(part_files(&staging), first);
        staging.prepare_parquet(false, true, &None).unwrap();
        assert_eq!(captured_ids(&staging, &schema), (vec![1, 2], vec![3]));

        // The first flush finishes late and publishes only its own rows.
        staging.seal_detached(std::mem::take(&mut detached));
        assert!(part_files(&staging).is_empty());
        staging.prepare_parquet(false, true, &None).unwrap();
        assert_eq!(captured_ids(&staging, &schema), (vec![], vec![1, 2, 3]));
    }

    #[test]
    fn parallel_metric_writer_creates_valid_ordered_row_groups() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("parallel-metrics.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c"])),
                Arc::new(TimestampMillisecondArray::from(vec![5, 4, 3, 2, 1])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut file = File::create(&path).unwrap();
        let arrow_writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        let (mut writer, row_group_factory) = arrow_writer.into_serialized_writer().unwrap();
        let row_group_factory = Arc::new(row_group_factory);
        let column_paths = Arc::new(
            writer
                .schema_descr()
                .columns()
                .iter()
                .map(|column| column.path().string())
                .collect::<Vec<_>>(),
        );
        let mut encodings = [(0, 2), (2, 2), (4, 1)]
            .into_iter()
            .enumerate()
            .map(|(row_group_index, (offset, length))| {
                Stream::spawn_metric_row_group_encode(
                    Arc::clone(&row_group_factory),
                    Arc::clone(&column_paths),
                    batch.slice(offset, length),
                    row_group_index,
                )
            })
            .collect::<VecDeque<_>>();
        let mut encoded_columns = 0;
        while let Some(encoding) = encodings.pop_front() {
            let encoded = encoding.recv().unwrap().unwrap();
            let timings = Stream::append_encoded_metric_row_group(&mut writer, encoded).unwrap();
            encoded_columns += timings.column_encode_timings.len();
        }
        assert_eq!(encoded_columns, 9);
        writer.close().unwrap();
        drop(file);

        let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        assert_eq!(reader.num_row_groups(), 3);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 5);
        assert_eq!(reader.get_row_group(0).unwrap().metadata().num_rows(), 2);
        assert_eq!(reader.get_row_group(1).unwrap().metadata().num_rows(), 2);
        assert_eq!(reader.get_row_group(2).unwrap().metadata().num_rows(), 1);
    }

    #[test]
    fn metric_conversion_carries_row_group_remainders_forward() {
        let temp_dir = TempDir::new().unwrap();
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 5,
            ..Default::default()
        });
        let mut metadata = LogStreamMetadata::default();
        metadata.log_source = vec![LogSourceEntry::new(LogSource::OtelMetrics, HashSet::new())];
        let staging = Stream::new(options, "metric_stream", metadata, None, &None);
        let schema = Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int32, false),
        ]));
        let event_time = Utc::now()
            .checked_sub_signed(TimeDelta::minutes(2))
            .unwrap()
            .naive_utc();

        for batch_number in 0..3 {
            let row_start = batch_number * 6;
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["metric"; 6])),
                    Arc::new(TimestampMillisecondArray::from_iter_values(
                        row_start..row_start + 6,
                    )),
                    Arc::new(Int32Array::from_iter_values(
                        row_start as i32..row_start as i32 + 6,
                    )),
                ],
            )
            .unwrap();
            staging
                .push(
                    "metric-schema",
                    &batch,
                    event_time,
                    &HashMap::new(),
                    StreamType::UserDefined,
                )
                .unwrap();
        }
        staging.flush(true).unwrap();

        let arrow_files = staging.arrow_files();
        assert_eq!(arrow_files.len(), 1);
        let ordered_batches = MetricForwardRecordIterator::new(
            MergedForwardRecordReader::try_new(&arrow_files),
            schema.clone(),
        )
        .map(|batch| {
            batch
                .unwrap()
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        })
        .collect::<Vec<_>>();
        assert_eq!(ordered_batches, vec![0, 6, 12]);

        let record_reader =
            ConversionRecordReader::Forward(MergedForwardRecordReader::try_new(&arrow_files));
        let merged_schema = Arc::new(record_reader.merged_schema());
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(5))
            .build();
        let parquet_path = temp_dir.path().join("metric.parquet.part");
        assert!(
            staging
                .write_parquet_part_file(
                    &parquet_path,
                    record_reader,
                    &merged_schema,
                    &props,
                    None,
                    None,
                )
                .unwrap()
                .is_some()
        );

        let reader = SerializedFileReader::new(File::open(parquet_path).unwrap()).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 18);
        assert_eq!(reader.num_row_groups(), 4);
        let row_group_rows = (0..reader.num_row_groups())
            .map(|index| reader.get_row_group(index).unwrap().metadata().num_rows())
            .collect::<Vec<_>>();
        assert_eq!(row_group_rows, vec![5, 5, 5, 3]);
    }

    #[test]
    fn metric_forward_reader_decodes_small_files_concurrently_in_order() {
        let temp_dir = TempDir::new().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let arrow_files = (0..6)
            .map(|value| {
                let path = temp_dir.path().join(format!("{value}.data.arrows"));
                let mut writer =
                    ArrowStreamWriter::try_new(File::create(&path).unwrap(), &schema).unwrap();
                writer
                    .write(
                        &RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int32Array::from(vec![value]))],
                        )
                        .unwrap(),
                    )
                    .unwrap();
                writer.finish().unwrap();
                path
            })
            .collect::<Vec<_>>();

        let mut reader = MetricForwardRecordIterator::new(
            MergedForwardRecordReader::try_new(&arrow_files),
            schema,
        );
        reader.fill_reader_budget();
        assert_eq!(
            reader.active_files.len(),
            (*METRIC_ARROW_READERS_IN_FLIGHT).min(arrow_files.len())
        );
        let values = reader
            .map(|batch| {
                batch
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0)
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn metric_sort_matches_parquet_sorting_metadata() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, true),
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("row_id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    Some("a"),
                    Some("a"),
                    None,
                    Some("b"),
                ])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(5),
                    None,
                ])),
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let sorted = Stream::sort_batch_for_metric_pruning(&batch, DEFAULT_TIMESTAMP_KEY).unwrap();
        let row_ids = sorted
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(row_ids.values(), &[2, 1, 0, 4, 3]);
    }

    #[test]
    fn test_staging_new_with_valid_stream() {
        let stream_name = "test_stream";

        let options = Arc::new(Options::default());
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name, &None)
        );
    }

    #[test]
    fn test_mark_deleting_sets_is_deleting() {
        let options = Arc::new(Options::default());
        let stream = Stream::new(
            options,
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );

        assert!(!stream.is_deleting());
        stream.mark_deleting();
        assert!(stream.is_deleting());
    }

    #[test]
    fn test_staging_with_special_characters() {
        let stream_name = "test_stream_!@#$%^&*()";

        let options = Arc::new(Options::default());
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name, &None)
        );
    }

    #[test]
    fn test_staging_data_path_initialization() {
        let stream_name = "example_stream";

        let options = Arc::new(Options::default());
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name, &None)
        );
    }

    #[test]
    fn test_staging_with_alphanumeric_stream_name() {
        let stream_name = "test123stream";

        let options = Arc::new(Options::default());
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name, &None)
        );
    }

    #[test]
    fn test_arrow_files_empty_directory() {
        let temp_dir = TempDir::new().unwrap();

        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let staging = Stream::new(
            Arc::new(options),
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );

        let files = staging.arrow_files();

        assert!(files.is_empty());
    }

    #[test]
    fn startup_groups_across_directories_and_orders_by_event_minute() {
        let temp_dir = TempDir::new().unwrap();
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let staging = Stream::new(
            Arc::new(options),
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let old_dir = staging.data_path.join("processing_3");
        let new_dir = staging.data_path.join("processing_20");
        let malformed_dir = staging.data_path.join("processing_unknown");
        fs::create_dir_all(&old_dir).unwrap();
        fs::create_dir_all(&new_dir).unwrap();
        fs::create_dir_all(&malformed_dir).unwrap();

        fs::write(
            old_dir.join("schema-a.date=2026-09-09.hour=10.minute=36.host.data.arrows"),
            b"old",
        )
        .unwrap();
        fs::write(
            new_dir.join("schema-b.date=2026-09-09.hour=10.minute=36.host.data.arrows"),
            b"new",
        )
        .unwrap();
        fs::write(
            old_dir.join("schema-c.date=2026-09-09.hour=10.minute=10.host.data.arrows"),
            b"ten",
        )
        .unwrap();
        fs::write(
            new_dir.join("schema-d.date=2026-09-09.hour=10.minute=9.host.data.arrows"),
            b"nine",
        )
        .unwrap();

        let groups = staging.group_inprocess_arrow_files("snapshot");
        let minutes = groups
            .iter()
            .map(|(path, _)| staging_event_minute(path).unwrap().minute())
            .collect::<Vec<_>>();
        assert_eq!(minutes, vec![9, 10, 36]);
        assert_eq!(groups[2].1.len(), 2);
        assert!(groups[2].1.iter().any(|path| path.starts_with(&old_dir)));
        assert!(groups[2].1.iter().any(|path| path.starts_with(&new_dir)));
    }

    #[test]
    fn startup_snapshot_ignores_root_arrow_files() {
        let temp_dir = TempDir::new().unwrap();
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let staging = Stream::new(
            Arc::new(options),
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let processing_dir = staging.data_path.join("processing_1");
        fs::create_dir_all(&processing_dir).unwrap();
        let filename = "schema.date=2026-09-09.hour=10.minute=36.host.data.arrows";
        fs::write(processing_dir.join(filename), b"processing").unwrap();
        let root_arrow = staging.data_path.join(filename);
        fs::write(&root_arrow, b"root").unwrap();

        let groups = staging.arrow_files_grouped_exclude_time(
            SystemTime::now(),
            minute_from_system_time(SystemTime::now()) - 1,
            true,
            false,
        );

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1.len(), 1);
        assert!(groups[0].1[0].starts_with(&processing_dir));
        assert!(root_arrow.exists());
    }

    #[test]
    fn periodic_sync_does_not_reuse_a_reserved_processing_directory() {
        let temp_dir = TempDir::new().unwrap();
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let staging = Stream::new(
            Arc::new(options),
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let group_minute = 42;
        let reserved_dir = Stream::inprocess_folder(&staging.data_path, group_minute);
        fs::create_dir_all(&reserved_dir).unwrap();
        let filename = "schema.date=2026-09-09.hour=10.minute=36.host.data.arrows";
        let reserved_arrow = reserved_dir.join(filename);
        fs::write(&reserved_arrow, b"reserved").unwrap();
        let root_arrow = staging.data_path.join(filename);
        fs::write(&root_arrow, b"root").unwrap();

        let groups =
            staging.arrow_files_grouped_exclude_time(SystemTime::now(), group_minute, false, true);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1.len(), 1);
        assert_ne!(groups[0].1[0].parent(), Some(reserved_dir.as_path()));
        assert!(reserved_arrow.exists());
        assert!(!root_arrow.exists());
    }

    #[test]
    fn cleanup_removes_all_empty_contributing_processing_directories() {
        let temp_dir = TempDir::new().unwrap();
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let staging = Stream::new(
            Arc::new(options),
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let first_dir = staging.data_path.join("processing_1");
        let second_dir = staging.data_path.join("processing_2");
        fs::create_dir_all(&first_dir).unwrap();
        fs::create_dir_all(&second_dir).unwrap();
        let first_file = first_dir.join("first.arrows");
        let second_file = second_dir.join("second.arrows");
        fs::write(&first_file, b"first").unwrap();
        fs::write(&second_file, b"second").unwrap();

        staging.cleanup_arrow_files_and_dir(&[first_file, second_file], &None);

        assert!(!first_dir.exists());
        assert!(!second_dir.exists());
    }

    #[test]
    fn generate_correct_path_with_current_time_and_no_custom_partitioning() {
        let stream_name = "test_stream";
        let stream_hash = "abc123";
        let parsed_timestamp = NaiveDate::from_ymd_opt(2023, 10, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let custom_partition_values = HashMap::new();

        let options = Options::default();
        let staging = Stream::new(
            Arc::new(options),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let hostname = hostname::get()
            .unwrap_or_else(|_| std::ffi::OsString::from(&Ulid::new().to_string()))
            .into_string()
            .unwrap_or_else(|_| Ulid::new().to_string())
            .matches(|c: char| c.is_alphanumeric() || c == '-' || c == '_')
            .collect::<String>();

        let expected = format!(
            "{stream_hash}.date={}.hour={:02}.minute={}.{}.data.{ARROW_FILE_EXTENSION}",
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
            hostname
        );

        let generated =
            staging.filename_by_partition(stream_hash, parsed_timestamp, &custom_partition_values);

        assert_eq!(generated, expected);
    }

    #[test]
    fn generate_correct_path_with_current_time_and_custom_partitioning() {
        let stream_name = "test_stream";
        let stream_hash = "abc123";
        let parsed_timestamp = NaiveDate::from_ymd_opt(2023, 10, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let mut custom_partition_values = HashMap::new();
        custom_partition_values.insert("key1".to_string(), "value1".to_string());
        custom_partition_values.insert("key2".to_string(), "value2".to_string());

        let options = Options::default();
        let staging = Stream::new(
            Arc::new(options),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let hostname = hostname::get()
            .unwrap_or_else(|_| std::ffi::OsString::from(&Ulid::new().to_string()))
            .into_string()
            .unwrap_or_else(|_| Ulid::new().to_string())
            .matches(|c: char| c.is_alphanumeric() || c == '-' || c == '_')
            .collect::<String>();

        let expected = format!(
            "{stream_hash}.date={}.hour={:02}.minute={}.key1=value1.key2=value2.{}.data.{ARROW_FILE_EXTENSION}",
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
            hostname
        );

        let generated =
            staging.filename_by_partition(stream_hash, parsed_timestamp, &custom_partition_values);

        assert_eq!(generated, expected);
    }

    #[test]
    fn test_convert_to_parquet_with_empty_staging() -> Result<(), StagingError> {
        let temp_dir = TempDir::new()?;
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let stream = "test_stream".to_string();
        let result = Stream::new(
            Arc::new(options),
            &stream,
            LogStreamMetadata::default(),
            None,
            &None,
        )
        .convert_disk_files_to_parquet(None, None, false, false, &None)?;
        assert!(result.is_none());
        // Verify metrics were set to 0
        let staging_files = metrics::STAGING_FILES
            .with_label_values(&[&stream, DEFAULT_TENANT])
            .get();
        assert_eq!(staging_files, 0);
        let storage_size_arrows = metrics::STORAGE_SIZE
            .with_label_values(&["staging", &stream, "arrows", "tenant_id"])
            .get();
        assert_eq!(storage_size_arrows, 0);
        let storage_size_parquet = metrics::STORAGE_SIZE
            .with_label_values(&["staging", &stream, "parquet", "tenant_id"])
            .get();
        assert_eq!(storage_size_parquet, 0);
        Ok(())
    }

    fn write_log(staging: &StreamRef, schema: &Schema, mins: i64) {
        let time: NaiveDateTime = Utc::now()
            .checked_sub_signed(TimeDelta::minutes(mins))
            .unwrap()
            .naive_utc();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        staging
            .push(
                "abc",
                &batch,
                time,
                &HashMap::new(),
                StreamType::UserDefined,
            )
            .unwrap();
        staging.flush(true).unwrap();
    }

    fn write_compressible_log(staging: &StreamRef, schema: &Schema, mins: i64) {
        let time: NaiveDateTime = Utc::now()
            .checked_sub_signed(TimeDelta::minutes(mins))
            .unwrap()
            .naive_utc();
        let rows = 4096;
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(TimestampMillisecondArray::from_iter_values(0..rows as i64)),
                Arc::new(Int32Array::from_iter_values(0..rows as i32)),
                Arc::new(StringArray::from(vec!["compressible-value"; rows])),
            ],
        )
        .unwrap();
        staging
            .push(
                "corrupt",
                &batch,
                time,
                &HashMap::new(),
                StreamType::UserDefined,
            )
            .unwrap();
        staging.flush(true).unwrap();
    }

    #[test]
    fn different_minutes_multiple_arrow_files_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        for i in 0..3 {
            write_log(&staging, &schema, i);
        }
        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 3);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(
            options,
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let result = staging
            .convert_disk_files_to_parquet(None, None, false, true, &None)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow files deleted
        assert_eq!(staging.parquet_files().len(), 3);
        assert_eq!(staging.arrow_files().len(), 0);
    }

    #[test]
    fn same_minute_multiple_arrow_files_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        for _ in 0..3 {
            write_log(&staging, &schema, 0);
        }
        println!("arrow files: {:?}", staging.arrow_files());
        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 3);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(
            options,
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let result = staging
            .convert_disk_files_to_parquet(None, None, false, true, &None)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow files deleted
        assert_eq!(staging.parquet_files().len(), 1);
        assert_eq!(staging.arrow_files().len(), 0);
    }

    #[test]
    fn corrupt_arrow_body_is_removed_without_blocking_other_groups() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options,
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        write_compressible_log(&staging, &schema, 2);
        let corrupt_path = staging.arrow_files().pop().unwrap();
        let mut bytes = fs::read(&corrupt_path).unwrap();
        let lz4_magic = [0x04, 0x22, 0x4d, 0x18];
        let magic_offset = bytes
            .windows(lz4_magic.len())
            .position(|window| window == lz4_magic)
            .expect("compressed Arrow body");
        bytes[magic_offset] ^= 0xff;
        fs::write(&corrupt_path, bytes).unwrap();

        // A second minute forms an independent conversion group that must
        // still finish even though the first group contains a corrupt body.
        write_log(&staging, &schema, 1);
        let result = staging
            .convert_disk_files_to_parquet(None, None, false, true, &None)
            .unwrap();

        assert!(result.is_some());
        assert_eq!(staging.parquet_files().len(), 1);
        assert!(staging.inprocess_arrow_files().is_empty());
        assert!(fs::read_dir(&staging.data_path).unwrap().all(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_none_or(|extension| extension != PART_FILE_EXTENSION)
        }));
    }

    #[test]
    fn conversion_continues_after_group_error() {
        let temp_dir = TempDir::new().unwrap();
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options,
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        write_log(&staging, &schema, 2);
        write_log(&staging, &schema, 1);

        let arrow_files = staging.arrow_files();
        assert_eq!(arrow_files.len(), 2);
        let first_dir = staging.data_path.join("processing_first");
        let second_dir = staging.data_path.join("processing_second");
        fs::create_dir_all(&first_dir).unwrap();
        fs::create_dir_all(&second_dir).unwrap();
        let first_arrow = first_dir.join(arrow_files[0].file_name().unwrap());
        let second_arrow = second_dir.join(arrow_files[1].file_name().unwrap());
        fs::rename(&arrow_files[0], &first_arrow).unwrap();
        fs::rename(&arrow_files[1], &second_arrow).unwrap();

        // The missing parent makes only the first Parquet creation fail.
        let failed_parquet = staging
            .data_path
            .join("missing")
            .join("date=2026-09-09.hour=13.minute=26.host.data.failed.parquet");
        let successful_parquet = staging
            .data_path
            .join("date=2026-09-09.hour=13.minute=27.host.data.success.parquet");
        // This direct-helper test bypasses prepare_parquet's inventory.
        staging.resolve_conversion_journals(&None).unwrap();
        let outcome = staging
            .convert_arrow_file_groups_to_parquet(
                vec![
                    (failed_parquet, vec![first_arrow.clone()]),
                    (successful_parquet.clone(), vec![second_arrow.clone()]),
                ],
                None,
                None,
                &None,
                ArrowGroupExecution::Sequential,
            )
            .unwrap();

        assert!(!outcome.schemas.is_empty());
        assert!(outcome.first_error.is_some());
        assert!(staging.finish_arrow_file_conversion(outcome).is_err());
        assert!(first_arrow.exists());
        assert!(!second_arrow.exists());
        assert!(successful_parquet.exists());
        assert_eq!(staging.schema_files().len(), 1);
    }

    #[test]
    fn orphan_arrow_part_with_complete_batch_becomes_valid_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options,
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        write_log(&staging, &schema, 1);

        // Simulate a crash after a complete batch was flushed but before Drop
        // wrote the eight-byte EOS marker and renamed the file.
        let arrow_path = staging.arrow_files().pop().unwrap();
        let len = arrow_path.metadata().unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&arrow_path)
            .unwrap()
            .set_len(len - 8)
            .unwrap();
        let mut part_path = arrow_path.clone();
        part_path.add_extension(PART_FILE_EXTENSION);
        std::fs::rename(&arrow_path, &part_path).unwrap();

        staging.recover_orphan_part_files();
        assert_eq!(staging.arrow_files().len(), 1);

        let schema = staging
            .convert_disk_files_to_parquet(None, None, false, true, &None)
            .unwrap();
        assert!(schema.is_some());
        assert_eq!(staging.parquet_files().len(), 1);
        assert!(staging.arrow_files().is_empty());
    }

    fn lifecycle_stream(temp_dir: &TempDir) -> StreamRef {
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        Stream::new(
            options,
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        )
    }

    /// A fixed past event minute: never current, so no wall-clock dependence.
    fn lifecycle_minute() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 10, 18)
            .unwrap()
            .and_hms_opt(0, 6, 30)
            .unwrap()
    }

    fn lifecycle_push_ids(staging: &StreamRef, ids: std::ops::Range<i32>) {
        push_ids_at(staging, ids, lifecycle_minute());
    }

    /// Converts all published Arrow files and returns every id stored in Parquet.
    fn convert_and_read_ids(staging: &StreamRef) -> Vec<i32> {
        staging
            .convert_disk_files_to_parquet(None, None, false, true, &None)
            .unwrap();
        let mut ids = Vec::new();
        for path in staging.parquet_files() {
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let column = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                ids.extend(column.values().iter().copied());
            }
        }
        ids.sort_unstable();
        ids
    }

    /// Reproduces the R6 loss: `flush` detaches a stale writer under the lock
    /// but finishes and renames it after releasing the lock. A late event for
    /// the same minute opened a new writer at the same `.part` path in that
    /// window, truncating the detached writer's acknowledged data and leaving
    /// a sparse file that conversion then deleted as corrupt.
    #[test]
    fn late_writer_for_detached_minute_does_not_truncate_finishing_part() {
        for forced in [false, true] {
            let temp_dir = TempDir::new().unwrap();
            let staging = lifecycle_stream(&temp_dir);
            lifecycle_push_ids(&staging, 0..1000);
            let detached_part = part_files(&staging).pop().unwrap();
            let detached_bytes = fs::read(&detached_part).unwrap();

            // Exactly the critical section of `Stream::flush`.
            let detached = staging.writer.lock().unwrap().take_flushable_disk(forced);
            assert_eq!(detached.len(), 1);

            // A late event for the detached minute arrives before its Drop.
            lifecycle_push_ids(&staging, 1000..1500);
            assert_eq!(fs::read(&detached_part).unwrap(), detached_bytes);

            // `Stream::flush` drops outside the lock: finish and publish.
            drop(detached);
            let live_parts = part_files(&staging);
            let published = staging.arrow_files();
            assert_eq!(published.len(), 1);
            let published_bytes = fs::read(&published[0]).unwrap();
            assert!(published_bytes.starts_with(&detached_bytes));

            // The replacement keeps writing; the published file is sealed.
            lifecycle_push_ids(&staging, 1500..2000);
            assert_eq!(fs::read(&published[0]).unwrap(), published_bytes);

            // Converting it neither reads nor removes the live writer's part.
            assert_eq!(
                convert_and_read_ids(&staging),
                (0..1000).collect::<Vec<_>>()
            );
            assert_eq!(live_parts.len(), 1);
            assert_ne!(live_parts[0], detached_part);
            assert_eq!(part_files(&staging), live_parts);

            staging.flush(true).unwrap();
            assert!(part_files(&staging).is_empty());
            assert_eq!(
                convert_and_read_ids(&staging),
                (0..2000).collect::<Vec<_>>()
            );
            assert!(staging.arrow_files().is_empty());
        }
    }

    /// Ingestion can start before startup recovery runs. Recovery must not
    /// publish (or delete) a part that a live writer is still appending to.
    #[test]
    fn startup_recovery_skips_part_owned_by_live_writer() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        lifecycle_push_ids(&staging, 0..100);
        let live_parts = part_files(&staging);
        assert_eq!(live_parts.len(), 1);

        staging.recover_orphan_part_files();
        let converted_early = convert_and_read_ids(&staging);
        lifecycle_push_ids(&staging, 100..200);
        staging.flush(true).unwrap();

        assert_eq!(convert_and_read_ids(&staging), (0..200).collect::<Vec<_>>());
        assert!(converted_early.is_empty());
        assert!(part_files(&staging).is_empty());
    }

    /// A crash leaves a `.part` without EOS or rename. If ingestion for the
    /// same minute resumes before startup recovery, the new writer must not
    /// truncate it, and recovery must still publish it.
    #[test]
    fn new_writer_preserves_crash_orphan_part_with_same_name() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        lifecycle_push_ids(&staging, 0..100);
        let orphan = part_files(&staging).pop().unwrap();
        // Simulate the crash: Drop (EOS + rename) never runs.
        let crashed = staging.writer.lock().unwrap().take_flushable_disk(true);
        std::mem::forget(crashed);

        lifecycle_push_ids(&staging, 100..200);
        let live_parts: Vec<_> = part_files(&staging)
            .into_iter()
            .filter(|path| *path != orphan)
            .collect();

        staging.recover_orphan_part_files();
        staging.flush(true).unwrap();

        assert_eq!(convert_and_read_ids(&staging), (0..200).collect::<Vec<_>>());
        assert_eq!(live_parts.len(), 1);
        assert!(!orphan.exists());
        assert!(part_files(&staging).is_empty());
    }

    fn push_ids_at(staging: &StreamRef, ids: std::ops::Range<i32>, minute: NaiveDateTime) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    ids.clone().map(i64::from),
                )),
                Arc::new(Int32Array::from_iter_values(ids.clone())),
                Arc::new(StringArray::from_iter_values(
                    ids.map(|id| format!("payload-{id}")),
                )),
            ],
        )
        .unwrap();
        staging
            .push(
                "abc",
                &batch,
                minute,
                &HashMap::new(),
                StreamType::UserDefined,
            )
            .unwrap();
    }

    /// The minute before `lifecycle_minute`, for crash orphans of another file.
    fn orphan_minute() -> NaiveDateTime {
        lifecycle_minute() - chrono::TimeDelta::minutes(1)
    }

    fn sorted_ids(ranges: &[std::ops::Range<i32>]) -> Vec<i32> {
        let mut ids: Vec<i32> = ranges.iter().cloned().flatten().collect();
        ids.sort_unstable();
        ids
    }

    fn id_mismatch(stage: &str, got: &[i32], expected: &[i32]) -> Option<String> {
        if got == expected {
            return None;
        }
        let got_set: HashSet<i32> = got.iter().copied().collect();
        let missing = expected.iter().filter(|id| !got_set.contains(id)).count();
        Some(format!(
            "{stage}: got {} ids ({} distinct), expected {}; {missing} expected ids missing",
            got.len(),
            got_set.len(),
            expected.len()
        ))
    }

    fn phase_kind(phase: &LifecyclePhase) -> &'static str {
        match phase {
            LifecyclePhase::FlushDetached => "FlushDetached",
            LifecyclePhase::RecoveryStarted => "RecoveryStarted",
            LifecyclePhase::RecoveryWaitingForFlush => "RecoveryWaitingForFlush",
            LifecyclePhase::RecoveryCandidates(_) => "RecoveryCandidates",
            LifecyclePhase::StartupRecovered => "StartupRecovered",
            LifecyclePhase::RecoveryPublishing(_) => "RecoveryPublishing",
            LifecyclePhase::ConversionClaimed(_) => "ConversionClaimed",
            LifecyclePhase::ConversionReadersOpened { .. } => "ConversionReadersOpened",
            LifecyclePhase::ParquetPartCreated(_) => "ParquetPartCreated",
            LifecyclePhase::ParquetPartClosed(_) => "ParquetPartClosed",
            LifecyclePhase::ParquetPartWritten(_) => "ParquetPartWritten",
        }
    }

    type PhaseKey = (String, &'static str);

    /// Reports every lifecycle phase with its thread name. A phase with a
    /// registered gate blocks its thread until the gate is released (or the
    /// harness is dropped). Timeouts only guard against a hung test.
    struct LifecycleHarness {
        events: std::sync::mpsc::Receiver<(String, LifecyclePhase)>,
        log: Vec<(String, LifecyclePhase)>,
        consumed: HashSet<usize>,
        releases: HashMap<PhaseKey, std::sync::mpsc::Sender<()>>,
    }

    impl LifecycleHarness {
        fn install(staging: &StreamRef, gated: &[(&str, &'static str)]) -> Self {
            let (events_tx, events) = std::sync::mpsc::channel();
            let mut gates = HashMap::new();
            let mut releases = HashMap::new();
            for (thread, kind) in gated {
                let (release, gate) = std::sync::mpsc::channel::<()>();
                gates.insert((thread.to_string(), *kind), gate);
                releases.insert((thread.to_string(), *kind), release);
            }
            let gates = Mutex::new(gates);
            let hook: LifecycleHook = Arc::new(move |phase: LifecyclePhase| {
                let thread = std::thread::current().name().unwrap_or("").to_owned();
                let key = (thread.clone(), phase_kind(&phase));
                let _ = events_tx.send((thread, phase));
                let gate = gates.lock().unwrap().remove(&key);
                if let Some(gate) = gate {
                    let _ = gate.recv();
                }
            });
            *staging.lifecycle_hook.lock().unwrap() = Some(hook);
            Self {
                events,
                log: Vec::new(),
                consumed: HashSet::new(),
                releases,
            }
        }

        /// Returns the next unconsumed phase of one of `kinds` on `thread`.
        fn wait_for(&mut self, thread: &str, kinds: &[&str]) -> LifecyclePhase {
            let matches = |(name, phase): &(String, LifecyclePhase)| {
                name == thread && kinds.contains(&phase_kind(phase))
            };
            let mut index = 0;
            loop {
                while index < self.log.len() {
                    if !self.consumed.contains(&index) && matches(&self.log[index]) {
                        self.consumed.insert(index);
                        return self.log[index].1.clone();
                    }
                    index += 1;
                }
                match self.events.recv_timeout(Duration::from_secs(60)) {
                    Ok(event) => self.log.push(event),
                    Err(err) => panic!("no {kinds:?} on {thread} ({err}); seen {:?}", self.log),
                }
            }
        }

        fn release(&mut self, thread: &str, kind: &'static str) {
            let release = self
                .releases
                .remove(&(thread.to_owned(), kind))
                .expect("gate registered");
            let _ = release.send(());
        }

        /// Every candidate set recovery has selected so far.
        fn candidates_seen(&mut self) -> Vec<Vec<PathBuf>> {
            while let Ok(event) = self.events.try_recv() {
                self.log.push(event);
            }
            self.log
                .iter()
                .filter_map(|(_, phase)| match phase {
                    LifecyclePhase::RecoveryCandidates(paths) => Some(paths.clone()),
                    _ => None,
                })
                .collect()
        }
    }

    fn spawn_named<T: Send + 'static>(
        name: &str,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> std::thread::JoinHandle<T> {
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(f)
            .unwrap()
    }

    fn published_files(staging: &StreamRef) -> Vec<(PathBuf, Vec<u8>)> {
        staging
            .arrow_files()
            .into_iter()
            .map(|path| {
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect()
    }

    fn changed_published(files: &[(PathBuf, Vec<u8>)]) -> Vec<PathBuf> {
        files
            .iter()
            .filter(|(path, bytes)| fs::read(path).ok().as_ref() != Some(bytes))
            .map(|(path, _)| path.clone())
            .collect()
    }

    /// Shutdown sync can run during startup recovery while an admitted request
    /// is still ingesting. The shutdown flush detaches writer A, recovery
    /// starts, A finishes and frees its deterministic part path, and the
    /// request then opens writer B at that same path. Recovery must claim
    /// neither A nor B, published files must never change, and every
    /// acknowledged id must be converted exactly once.
    #[test]
    fn startup_recovery_never_claims_part_reused_after_shutdown_flush() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        // A crash orphan of another minute, which recovery must publish.
        push_ids_at(&staging, 5000..5100, orphan_minute());
        let orphan = part_files(&staging).pop().unwrap();
        std::mem::forget(staging.writer.lock().unwrap().take_flushable_disk(true));

        lifecycle_push_ids(&staging, 0..1000);
        let a_part = part_files(&staging)
            .into_iter()
            .find(|path| *path != orphan)
            .unwrap();
        let a_bytes = fs::read(&a_part).unwrap();
        let mut violations = Vec::new();

        let mut harness = LifecycleHarness::install(
            &staging,
            &[
                ("shutdown", "FlushDetached"),
                ("startup", "RecoveryCandidates"),
                ("startup", "StartupRecovered"),
            ],
        );
        let shutdown = spawn_named("shutdown", {
            let staging = staging.clone();
            move || staging.flush(true).unwrap()
        });
        harness.wait_for("shutdown", &["FlushDetached"]);

        let startup = spawn_named("startup", {
            let staging = staging.clone();
            move || drop(staging.prepare_startup_sync(None).unwrap())
        });
        // Recovery either selects candidates now or waits for the flush.
        let first = harness.wait_for(
            "startup",
            &["RecoveryCandidates", "RecoveryWaitingForFlush"],
        );

        // A finishes and renames its part, which frees the path.
        harness.release("shutdown", "FlushDetached");
        shutdown.join().unwrap();
        // An admitted request for the same key opens B at that path.
        lifecycle_push_ids(&staging, 1000..1500);
        let active = staging.writer.lock().unwrap().active_part_paths();
        assert_eq!(active, HashSet::from([a_part.clone()]), "B reuses A's path");
        let b_part = a_part.clone();

        harness.release("startup", "RecoveryCandidates");
        let candidates = match first {
            LifecyclePhase::RecoveryCandidates(candidates) => candidates,
            _ => match harness.wait_for("startup", &["RecoveryCandidates"]) {
                LifecyclePhase::RecoveryCandidates(candidates) => candidates,
                phase => unreachable!("{phase:?}"),
            },
        };
        let candidates: HashSet<PathBuf> = candidates.into_iter().collect();
        if candidates.contains(&a_part) {
            violations.push(format!(
                "recovery selected {a_part:?}, owned by detached writer A or live writer B"
            ));
        }
        if !candidates.contains(&orphan) {
            violations.push(format!("recovery skipped crash orphan {orphan:?}"));
        }

        // Startup has recovered and is paused before its own flush.
        harness.wait_for("startup", &["StartupRecovered"]);
        if !b_part.exists() {
            violations.push(format!("recovery claimed live writer B's part {b_part:?}"));
        }
        let published = published_files(&staging);
        let a_published = a_part.with_extension("");
        match published.iter().find(|(path, _)| *path == a_published) {
            Some((_, bytes)) if bytes.starts_with(&a_bytes) => {}
            _ => violations.push(format!("A was not published intact as {a_published:?}")),
        }
        lifecycle_push_ids(&staging, 1500..1750);
        for path in changed_published(&published) {
            violations.push(format!("published {path:?} changed after publication"));
        }
        // Conversion while B ingests must only see sealed files.
        let early = convert_and_read_ids(&staging);
        violations.extend(id_mismatch(
            "conversion while B is live",
            &early,
            &sorted_ids(&[0..1000, 5000..5100]),
        ));
        lifecycle_push_ids(&staging, 1750..2000);

        // Startup's forced flush finishes and publishes B.
        harness.release("startup", "StartupRecovered");
        startup.join().unwrap();
        let all = convert_and_read_ids(&staging);
        violations.extend(id_mismatch(
            "final conversion",
            &all,
            &sorted_ids(&[0..2000, 5000..5100]),
        ));
        if !part_files(&staging).is_empty() {
            violations.push(format!("parts left: {:?}", part_files(&staging)));
        }
        if !staging.arrow_files().is_empty() {
            violations.push(format!("arrows left: {:?}", staging.arrow_files()));
        }
        assert!(violations.is_empty(), "{violations:#?}");
    }

    /// Recovery that overlaps two shutdown-style flushes must wait until both
    /// have finished their detached writers, never selecting a detached
    /// writer's part, and must still not alter anything published.
    #[test]
    fn startup_recovery_waits_for_every_detached_shutdown_writer() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        let mut violations = Vec::new();
        let mut harness = LifecycleHarness::install(
            &staging,
            &[
                ("shutdown-1", "FlushDetached"),
                ("shutdown-2", "FlushDetached"),
                ("startup", "StartupRecovered"),
            ],
        );

        lifecycle_push_ids(&staging, 0..1000);
        let a_part = part_files(&staging).pop().unwrap();
        let first_flush = spawn_named("shutdown-1", {
            let staging = staging.clone();
            move || staging.flush(true).unwrap()
        });
        harness.wait_for("shutdown-1", &["FlushDetached"]);

        // A's part still exists, so C gets its own part for the same key.
        lifecycle_push_ids(&staging, 1000..1300);
        let c_part = part_files(&staging)
            .into_iter()
            .find(|path| *path != a_part)
            .unwrap();
        let second_flush = spawn_named("shutdown-2", {
            let staging = staging.clone();
            move || staging.flush(true).unwrap()
        });
        harness.wait_for("shutdown-2", &["FlushDetached"]);
        let a_bytes = fs::read(&a_part).unwrap();
        let c_bytes = fs::read(&c_part).unwrap();

        let startup = spawn_named("startup", {
            let staging = staging.clone();
            move || drop(staging.prepare_startup_sync(None).unwrap())
        });
        // Either recovery has finished, or it is waiting for the flushes.
        let first = harness.wait_for("startup", &["StartupRecovered", "RecoveryWaitingForFlush"]);
        for (part, bytes) in [(&a_part, &a_bytes), (&c_part, &c_bytes)] {
            if fs::read(part).ok().as_ref() != Some(bytes) {
                violations.push(format!(
                    "recovery took detached writer's part {part:?} before it finished"
                ));
            }
        }

        harness.release("shutdown-1", "FlushDetached");
        first_flush.join().unwrap();
        // The second flush still holds its detached writer.
        if fs::read(&c_part).ok().as_ref() != Some(&c_bytes) {
            violations.push(format!(
                "recovery took {c_part:?} while the second flush still owned it"
            ));
        }
        harness.release("shutdown-2", "FlushDetached");
        second_flush.join().unwrap();
        let published = published_files(&staging);

        if first != LifecyclePhase::StartupRecovered {
            harness.wait_for("startup", &["StartupRecovered"]);
        }
        for candidates in harness.candidates_seen() {
            for part in [&a_part, &c_part] {
                if candidates.contains(part) {
                    violations.push(format!("recovery selected detached writer's part {part:?}"));
                }
            }
        }
        harness.release("startup", "StartupRecovered");
        startup.join().unwrap();
        for path in changed_published(&published) {
            violations.push(format!("published {path:?} changed after publication"));
        }

        let all = convert_and_read_ids(&staging);
        violations.extend(id_mismatch(
            "final conversion",
            &all,
            &sorted_ids(&[0..1300]),
        ));
        if !part_files(&staging).is_empty() {
            violations.push(format!("parts left: {:?}", part_files(&staging)));
        }
        assert!(violations.is_empty(), "{violations:#?}");
    }

    /// The lifecycle lock guards no data. A recovery that panicked must not
    /// stop later flushes or recoveries, nor lose acknowledged rows.
    #[test]
    fn poisoned_part_lifecycle_lock_keeps_flush_and_recovery_working() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        lifecycle_push_ids(&staging, 0..100);
        let hook: LifecycleHook = Arc::new(|phase| {
            if matches!(phase, LifecyclePhase::RecoveryCandidates(_))
                && std::thread::current().name() == Some("poisoner")
            {
                panic!("injected recovery panic");
            }
        });
        *staging.lifecycle_hook.lock().unwrap() = Some(hook);
        let poisoner = spawn_named("poisoner", {
            let staging = staging.clone();
            move || staging.recover_orphan_part_files()
        });
        assert!(poisoner.join().is_err());
        assert!(staging.part_lifecycle.is_poisoned());

        lifecycle_push_ids(&staging, 100..200);
        staging.flush(true).unwrap();
        push_ids_at(&staging, 300..400, orphan_minute());
        std::mem::forget(staging.writer.lock().unwrap().take_flushable_disk(true));
        staging.recover_orphan_part_files();

        assert_eq!(
            convert_and_read_ids(&staging),
            sorted_ids(&[0..200, 300..400])
        );
        assert!(part_files(&staging).is_empty());
    }

    #[test]
    fn corrupt_orphan_part_is_removed() {
        let temp_dir = TempDir::new().unwrap();
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        });
        let staging = Stream::new(
            options,
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        fs::create_dir_all(&staging.data_path).unwrap();
        let part_path = staging.data_path.join("orphan.arrows.part");
        fs::write(&part_path, b"not an Arrow stream").unwrap();

        staging.recover_orphan_part_files();

        assert!(!part_path.exists());
    }

    #[test]
    fn corrupt_arrow_body_is_deferred_to_parquet_conversion() {
        let temp_dir = TempDir::new().unwrap();
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options,
            "test_stream",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        write_compressible_log(&staging, &schema, 1);

        let arrow_path = staging.arrow_files().pop().unwrap();
        let mut bytes = fs::read(&arrow_path).unwrap();
        let lz4_magic = [0x04, 0x22, 0x4d, 0x18];
        let magic_offset = bytes
            .windows(lz4_magic.len())
            .position(|window| window == lz4_magic)
            .expect("compressed Arrow body");
        bytes[magic_offset] ^= 0xff;
        fs::write(&arrow_path, bytes).unwrap();
        let part_path = arrow_path.with_extension("arrows.part");
        fs::rename(&arrow_path, &part_path).unwrap();

        staging.recover_orphan_part_files();

        assert!(!part_path.exists());
        assert_eq!(staging.arrow_files().len(), 1);

        let result = staging
            .convert_disk_files_to_parquet(None, None, false, true, &None)
            .unwrap();
        assert!(result.is_none());
        assert!(staging.inprocess_arrow_files().is_empty());
        assert!(staging.parquet_files().is_empty());
    }

    #[test]
    fn arrow_and_parquet_part_files_are_distinguished() {
        let temp_dir = TempDir::new().unwrap();
        let arrow_part = temp_dir.path().join("schema.data.arrows.part");
        let parquet_part = temp_dir.path().join("date=2026-09-09.data.parquet.part");
        let legacy_parquet_part = temp_dir.path().join("date=2026-09-09.data.part");
        fs::write(&arrow_part, b"PAR1").unwrap();
        fs::write(&parquet_part, b"not finalized").unwrap();
        fs::write(&legacy_parquet_part, b"PAR1").unwrap();

        assert!(!Stream::is_parquet_part_file(&arrow_part));
        assert!(Stream::is_parquet_part_file(&parquet_part));
        assert!(Stream::is_parquet_part_file(&legacy_parquet_part));
    }

    #[tokio::test]
    async fn miss_current_arrow_file_when_converting_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        // 2 logs in the previous minutes
        for i in 0..2 {
            write_log(&staging, &schema, i);
        }
        sleep(Duration::from_secs(60)).await;

        write_log(&staging, &schema, 0);

        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 3);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(
            options,
            stream_name,
            LogStreamMetadata::default(),
            None,
            &None,
        );
        let result = staging
            .convert_disk_files_to_parquet(None, None, false, false, &None)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow file left
        assert_eq!(staging.parquet_files().len(), 2);
        assert_eq!(staging.arrow_files().len(), 1);
    }

    fn create_test_file(dir: &TempDir, filename: &str) -> PathBuf {
        let file_path = dir.path().join(filename);
        let mut file = File::create(&file_path).expect("Failed to create test file");
        // Write some dummy content
        file.write_all(b"test content")
            .expect("Failed to write to test file");
        file_path
    }

    #[test]
    fn test_valid_arrow_path_conversion() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let filename = "12345abcde&key1=value1.date=2020-01-21.hour=10.minute=30.key1=value1.key2=value2.ee529ffc8e76.data.arrows";
        let file_path = create_test_file(&temp_dir, filename);
        let random_string = "random123";

        let result = arrow_path_to_parquet(&file_path, &file_path, random_string);

        assert!(result.is_some());
        let parquet_path = result.unwrap();
        assert_eq!(
            parquet_path.file_name().unwrap().to_str().unwrap(),
            "date=2020-01-21.hour=10.minute=30.key1=value1.key2=value2.ee529ffc8e76.data.random123.parquet"
        );
    }

    #[test]
    fn test_complex_path() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let nested_dir = temp_dir.path().join("nested/directory/structure");
        std::fs::create_dir_all(&nested_dir).expect("Failed to create nested directories");

        let filename = "20200201T1830f8a5fc1edc567d56&key1=value1&key2=value2.date=2020-01-21.hour=10.minute=30.region=us-west.ee529ffc8e76.data.arrows";
        let file_path = nested_dir.join(filename);

        let mut file = File::create(&file_path).expect("Failed to create test file");
        file.write_all(b"test content")
            .expect("Failed to write to test file");

        let random_string = "random456";

        let result = arrow_path_to_parquet(&file_path, &file_path, random_string);

        assert!(result.is_some());
        let parquet_path = result.unwrap();
        assert_eq!(
            parquet_path.file_name().unwrap().to_str().unwrap(),
            "date=2020-01-21.hour=10.minute=30.region=us-west.ee529ffc8e76.data.random456.parquet"
        );
    }

    #[test]
    fn get_or_create_returns_existing_stream() {
        let streams = Streams::default();
        let options = Arc::new(Options::default());
        let stream_name = "test_stream";
        let metadata = LogStreamMetadata::default();
        let ingestor_id = Some("test_ingestor".to_owned());

        // Create the stream first
        let stream1 = streams.get_or_create(
            options.clone(),
            stream_name.to_owned(),
            metadata.clone(),
            ingestor_id.clone(),
            &None,
        );

        // Call get_or_create again with the same stream_name
        let stream2 = streams.get_or_create(
            options.clone(),
            stream_name.to_owned(),
            metadata.clone(),
            ingestor_id.clone(),
            &None,
        );

        // Assert that both references point to the same stream
        assert!(Arc::ptr_eq(&stream1, &stream2));

        // Verify the map contains only one entry
        let guard = streams.read().expect("Failed to acquire read lock");
        assert_eq!(guard.len(), 1);
    }

    #[test]
    fn create_and_return_new_stream_when_name_does_not_exist() {
        let streams = Streams::default();
        let options = Arc::new(Options::default());
        let stream_name = "new_stream";
        let metadata = LogStreamMetadata::default();
        let ingestor_id = Some("new_ingestor".to_owned());

        // Assert the stream doesn't exist already
        let mut guard = streams.write().expect("Failed to acquire read lock");
        assert_eq!(guard.len(), 0);
        assert!(
            !guard
                .entry(DEFAULT_TENANT.to_string())
                .or_default()
                .contains_key(stream_name)
        );
        drop(guard);

        // Call get_or_create with a new stream_name
        let stream = streams.get_or_create(
            options.clone(),
            stream_name.to_owned(),
            metadata.clone(),
            ingestor_id.clone(),
            &None,
        );

        // verify created stream has the same ingestor_id
        assert_eq!(stream.ingestor_id, ingestor_id);

        // Assert that the stream is created
        let guard = streams.read().expect("Failed to acquire read lock");
        assert_eq!(guard.len(), 1);
        assert!(guard.get(DEFAULT_TENANT).unwrap().contains_key(stream_name));
    }

    #[test]
    fn get_or_create_stream_concurrently() {
        let streams = Arc::new(Streams::default());
        let options = Arc::new(Options::default());
        let stream_name = String::from("concurrent_stream");
        let metadata = LogStreamMetadata::default();
        let ingestor_id = Some(String::from("concurrent_ingestor"));

        // Barrier to synchronize threads
        let barrier = Arc::new(Barrier::new(2));

        // Clones for the first thread
        let streams1 = Arc::clone(&streams);
        let options1 = Arc::clone(&options);
        let barrier1 = Arc::clone(&barrier);
        let stream_name1 = stream_name.clone();
        let metadata1 = metadata.clone();
        let ingestor_id1 = ingestor_id.clone();

        // First thread
        let handle1 = spawn(move || {
            barrier1.wait();
            streams1.get_or_create(options1, stream_name1, metadata1, ingestor_id1, &None)
        });

        // Cloned for the second thread
        let streams2 = Arc::clone(&streams);

        // Second thread
        let handle2 = spawn(move || {
            barrier.wait();
            streams2.get_or_create(options, stream_name, metadata, ingestor_id, &None)
        });

        // Wait for both threads to complete and get their results
        let stream1 = handle1.join().expect("Thread 1 panicked");
        let stream2 = handle2.join().expect("Thread 2 panicked");

        // Assert that both references point to the same stream
        assert!(Arc::ptr_eq(&stream1, &stream2));

        // Verify the map contains only one entry
        let guard = streams.read().expect("Failed to acquire read lock");
        assert_eq!(guard.len(), 1);
    }

    type PhaseMatcher = Box<dyn Fn(&str, &LifecyclePhase) -> bool + Send>;

    enum GateAction {
        Block(std::sync::mpsc::Receiver<()>),
        Panic,
    }

    /// Like `LifecycleHarness`, but gates are added while threads run and
    /// match on phase contents, because parallel conversion groups run on
    static CONVERSION_HARNESS_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    /// unnamed Rayon workers. Each gate fires once. Dropping the harness
    /// releases every blocked thread. Timeouts only guard against a hang.
    struct ConversionHarness {
        _serial_guard: std::sync::MutexGuard<'static, ()>,
        events: std::sync::mpsc::Receiver<(String, LifecyclePhase)>,
        log: Vec<(String, LifecyclePhase)>,
        consumed: HashSet<usize>,
        gates: Arc<Mutex<Vec<(usize, PhaseMatcher, GateAction)>>>,
        releases: HashMap<usize, std::sync::mpsc::Sender<()>>,
        next_gate: usize,
    }

    impl ConversionHarness {
        fn install(staging: &StreamRef) -> Self {
            let serial_guard = CONVERSION_HARNESS_LOCK
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let (events_tx, events) = std::sync::mpsc::channel();
            let gates: Arc<Mutex<Vec<(usize, PhaseMatcher, GateAction)>>> = Arc::default();
            let hook_gates = Arc::clone(&gates);
            let hook: LifecycleHook = Arc::new(move |phase: LifecyclePhase| {
                let thread = std::thread::current().name().unwrap_or("").to_owned();
                let _ = events_tx.send((thread.clone(), phase.clone()));
                let action = {
                    let mut gates = hook_gates.lock().unwrap();
                    let index = gates
                        .iter()
                        .position(|(_, matches, _)| matches(&thread, &phase));
                    index.map(|index| gates.remove(index).2)
                };
                match action {
                    Some(GateAction::Block(gate)) => {
                        let _ = gate.recv();
                    }
                    Some(GateAction::Panic) => panic!("injected panic at {phase:?}"),
                    None => {}
                }
            });
            *staging.lifecycle_hook.lock().unwrap() = Some(hook);
            Self {
                _serial_guard: serial_guard,
                events,
                log: Vec::new(),
                consumed: HashSet::new(),
                gates,
                releases: HashMap::new(),
                next_gate: 0,
            }
        }

        /// Blocks the first future phase that `matches` until `release`.
        fn gate(&mut self, matches: PhaseMatcher) -> usize {
            let (release, gate) = std::sync::mpsc::channel();
            self.next_gate += 1;
            self.gates
                .lock()
                .unwrap()
                .push((self.next_gate, matches, GateAction::Block(gate)));
            self.releases.insert(self.next_gate, release);
            self.next_gate
        }

        /// Panics the thread reaching the first future phase that `matches`.
        fn panic_at(&mut self, matches: PhaseMatcher) {
            self.next_gate += 1;
            self.gates
                .lock()
                .unwrap()
                .push((self.next_gate, matches, GateAction::Panic));
        }

        fn release(&mut self, gate: usize) {
            let _ = self.releases.remove(&gate).expect("gate added").send(());
        }

        /// Returns the next unconsumed phase that `matches`, waiting for it.
        fn wait(
            &mut self,
            what: &str,
            matches: impl Fn(&str, &LifecyclePhase) -> bool,
        ) -> LifecyclePhase {
            let mut index = 0;
            loop {
                while index < self.log.len() {
                    let (thread, phase) = &self.log[index];
                    if !self.consumed.contains(&index) && matches(thread, phase) {
                        self.consumed.insert(index);
                        return phase.clone();
                    }
                    index += 1;
                }
                match self.events.recv_timeout(Duration::from_secs(60)) {
                    Ok(event) => self.log.push(event),
                    Err(err) => panic!("no {what} ({err}); seen {:#?}", self.log),
                }
            }
        }

        /// Every phase reported so far by `thread`.
        fn phases_of(&mut self, thread: &str) -> Vec<LifecyclePhase> {
            while let Ok(event) = self.events.try_recv() {
                self.log.push(event);
            }
            self.log
                .iter()
                .filter(|(name, _)| name == thread)
                .map(|(_, phase)| phase.clone())
                .collect()
        }

        /// Every Arrow source the conversions on `thread` have claimed.
        fn claimed_sources_of(&mut self, thread: &str) -> HashSet<PathBuf> {
            self.phases_of(thread)
                .into_iter()
                .filter_map(|phase| match phase {
                    LifecyclePhase::ConversionClaimed(groups) => Some(groups),
                    _ => None,
                })
                .flatten()
                .flat_map(|(_, sources)| sources)
                .collect()
        }

        /// Every part recovery on `thread` has selected as an orphan.
        fn recovery_candidates_of(&mut self, thread: &str) -> HashSet<PathBuf> {
            self.phases_of(thread)
                .into_iter()
                .filter_map(|phase| match phase {
                    LifecyclePhase::RecoveryCandidates(paths) => Some(paths),
                    _ => None,
                })
                .flatten()
                .collect()
        }
    }

    fn on_thread(thread: &'static str, kind: &'static str) -> PhaseMatcher {
        Box::new(move |name: &str, phase: &LifecyclePhase| {
            name == thread && phase_kind(phase) == kind
        })
    }

    fn readers_opened_for(outputs: Vec<PathBuf>) -> PhaseMatcher {
        Box::new(move |_: &str, phase: &LifecyclePhase| match phase {
            LifecyclePhase::ConversionReadersOpened { output, .. } => outputs.contains(output),
            _ => false,
        })
    }

    fn temp_output_phase(kind: &'static str, temp: PathBuf) -> PhaseMatcher {
        Box::new(move |_: &str, phase: &LifecyclePhase| {
            phase_kind(phase) == kind
                && matches!(
                    phase,
                    LifecyclePhase::ParquetPartCreated(path)
                        | LifecyclePhase::ParquetPartClosed(path)
                        | LifecyclePhase::ParquetPartWritten(path)
                        if *path == temp
                )
        })
    }

    fn claimed_groups(phase: LifecyclePhase) -> Vec<(PathBuf, Vec<PathBuf>)> {
        match phase {
            LifecyclePhase::ConversionClaimed(groups) => groups,
            phase => unreachable!("{phase:?}"),
        }
    }

    /// The temporary file a conversion writes `output` to before renaming it.
    fn temp_output(output: &Path) -> PathBuf {
        let mut temp = output.to_path_buf();
        temp.add_extension(PART_FILE_EXTENSION);
        temp
    }

    /// Simulates processing directories left by a crashed process: the given
    /// ids are published as Arrow files and moved into `name`.
    fn crash_orphan_dir(
        staging: &StreamRef,
        name: &str,
        batches: &[(std::ops::Range<i32>, NaiveDateTime)],
    ) -> PathBuf {
        for (ids, minute) in batches {
            push_ids_at(staging, ids.clone(), *minute);
        }
        staging.flush(true).unwrap();
        let dir = staging.data_path.join(name);
        fs::create_dir_all(&dir).unwrap();
        for file in staging.arrow_files() {
            fs::rename(&file, dir.join(file.file_name().unwrap())).unwrap();
        }
        dir
    }

    fn file_contents(paths: &HashSet<PathBuf>) -> HashMap<PathBuf, Option<Vec<u8>>> {
        paths
            .iter()
            .map(|path| (path.clone(), fs::read(path).ok()))
            .collect()
    }

    fn changed_files(before: &HashMap<PathBuf, Option<Vec<u8>>>) -> Vec<PathBuf> {
        let mut changed: Vec<PathBuf> = before
            .iter()
            .filter(|(path, bytes)| fs::read(path).ok() != **bytes)
            .map(|(path, _)| path.clone())
            .collect();
        changed.sort();
        changed
    }

    /// Every id in the published Parquet files, sorted, with multiplicity.
    fn published_ids(staging: &StreamRef) -> Vec<i32> {
        let mut ids = Vec::new();
        for path in staging.parquet_files() {
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let column = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                ids.extend(column.values().iter().copied());
            }
        }
        ids.sort_unstable();
        ids
    }

    /// Anything a finished, successful sync must not leave behind: parts,
    /// Arrow files, processing directories and Parquet files without a valid
    /// footer.
    fn staging_leftovers(staging: &StreamRef) -> Vec<PathBuf> {
        let valid: HashSet<PathBuf> = staging.parquet_files().into_iter().collect();
        let mut leftovers: Vec<PathBuf> = fs::read_dir(&staging.data_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                path.is_dir()
                    || name.ends_with(".part")
                    || name.ends_with(".arrows")
                    || (name.ends_with(".parquet") && !valid.contains(path))
            })
            .collect();
        leftovers.sort();
        leftovers
    }

    fn inode(path: &Path) -> Option<u64> {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(path).ok().map(|meta| meta.ino())
    }

    /// Shutdown sync converts its own processing directory while startup
    /// executes its plan for crash-orphaned directories and a second sync run
    /// converts newer files. Both shutdown groups convert in parallel and are
    /// held after opening their readers. Startup must claim only the orphans,
    /// must not delete the shutdown sources, and every id must be committed to
    /// exactly one Parquet file. The second run must not wait for the paused
    /// shutdown conversion.
    #[test]
    fn startup_plan_never_converts_sources_of_a_running_shutdown_conversion() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        let minute_a = lifecycle_minute() - TimeDelta::minutes(10);
        let minute_b = lifecycle_minute() - TimeDelta::minutes(11);
        // One orphan minute spans both directories.
        crash_orphan_dir(
            &staging,
            "processing_1",
            &[(9000..9100, minute_a), (9100..9200, minute_b)],
        );
        crash_orphan_dir(&staging, "processing_2", &[(9200..9300, minute_a)]);
        lifecycle_push_ids(&staging, 0..1000);
        push_ids_at(
            &staging,
            1000..1500,
            lifecycle_minute() - TimeDelta::minutes(2),
        );
        let mut violations = Vec::new();
        let mut harness = ConversionHarness::install(&staging);

        let claimed = harness.gate(on_thread("shutdown", "ConversionClaimed"));
        let shutdown = spawn_named("shutdown", {
            let staging = staging.clone();
            move || staging.flush_and_convert(false, true, &None)
        });
        let groups = claimed_groups(harness.wait("shutdown claim", |thread, phase| {
            thread == "shutdown" && phase_kind(phase) == "ConversionClaimed"
        }));
        assert_eq!(groups.len(), 2, "two minute groups: {groups:?}");
        let outputs: Vec<PathBuf> = groups.iter().map(|(output, _)| output.clone()).collect();
        let shutdown_sources: HashSet<PathBuf> = groups
            .iter()
            .flat_map(|(_, sources)| sources.clone())
            .collect();
        let readers = [
            harness.gate(readers_opened_for(outputs.clone())),
            harness.gate(readers_opened_for(outputs.clone())),
        ];
        harness.release(claimed);
        for _ in 0..2 {
            harness.wait("shutdown readers", |_, phase| {
                matches!(phase, LifecyclePhase::ConversionReadersOpened { output, .. } if outputs.contains(output))
            });
        }
        let shutdown_bytes = file_contents(&shutdown_sources);

        // Startup prepares and executes its plan while shutdown converts.
        let startup = spawn_named("startup", {
            let staging = staging.clone();
            move || staging.prepare_startup_sync(None).unwrap().execute()
        });
        if let Err(err) = startup.join().unwrap() {
            violations.push(format!("startup conversion failed: {err}"));
        }
        let stolen: Vec<_> = harness
            .claimed_sources_of("startup")
            .intersection(&shutdown_sources)
            .cloned()
            .collect();
        if !stolen.is_empty() {
            violations.push(format!(
                "startup claimed sources owned by the running shutdown conversion: {stolen:?}"
            ));
        }
        let changed = changed_files(&shutdown_bytes);
        if !changed.is_empty() {
            violations.push(format!(
                "sources changed or removed before their conversion published: {changed:?}"
            ));
        }
        violations.extend(id_mismatch(
            "published while shutdown converts",
            &published_ids(&staging),
            &sorted_ids(&[9000..9300]),
        ));

        // A second sync run converts newer files without waiting.
        push_ids_at(
            &staging,
            2000..2100,
            lifecycle_minute() - TimeDelta::minutes(3),
        );
        let second = spawn_named("shutdown-2", {
            let staging = staging.clone();
            move || staging.flush_and_convert(false, true, &None)
        });
        if let Err(err) = second.join().unwrap() {
            violations.push(format!("second sync run failed: {err}"));
        }
        let overlap: Vec<_> = harness
            .claimed_sources_of("shutdown-2")
            .intersection(&shutdown_sources)
            .cloned()
            .collect();
        if !overlap.is_empty() {
            violations.push(format!("second run claimed shutdown sources: {overlap:?}"));
        }
        violations.extend(id_mismatch(
            "published while shutdown still converts",
            &published_ids(&staging),
            &sorted_ids(&[2000..2100, 9000..9300]),
        ));

        for gate in readers {
            harness.release(gate);
        }
        if let Err(err) = shutdown.join().unwrap() {
            violations.push(format!("shutdown conversion failed: {err}"));
        }
        violations.extend(id_mismatch(
            "final",
            &published_ids(&staging),
            &sorted_ids(&[0..1500, 2000..2100, 9000..9300]),
        ));
        let leftovers = staging_leftovers(&staging);
        if !leftovers.is_empty() {
            violations.push(format!("left behind: {leftovers:?}"));
        }
        assert!(violations.is_empty(), "{violations:#?}");
    }

    /// Recovery runs (through the init entry or a startup plan) while a
    /// shutdown conversion holds its temporary Parquet output, either just
    /// created and empty or written and validated but not yet renamed. The
    /// output must not be selected, deleted or replaced, the conversion must
    /// publish it, and its sources must remain until that publication.
    #[test]
    fn recovery_never_removes_the_temporary_output_of_a_running_conversion() {
        for held_at in ["ParquetPartCreated", "ParquetPartWritten"] {
            for competitor in ["init", "startup"] {
                let case = format!("{competitor} during {held_at}");
                let temp_dir = TempDir::new().unwrap();
                let staging = lifecycle_stream(&temp_dir);
                crash_orphan_dir(
                    &staging,
                    "processing_1",
                    &[(9000..9100, lifecycle_minute() - TimeDelta::minutes(10))],
                );
                lifecycle_push_ids(&staging, 0..1000);
                let mut violations = Vec::new();
                let mut harness = ConversionHarness::install(&staging);

                let claimed = harness.gate(on_thread("shutdown", "ConversionClaimed"));
                let shutdown = spawn_named("shutdown", {
                    let staging = staging.clone();
                    move || staging.flush_and_convert(false, true, &None)
                });
                let groups = claimed_groups(harness.wait("shutdown claim", |thread, phase| {
                    thread == "shutdown" && phase_kind(phase) == "ConversionClaimed"
                }));
                assert_eq!(groups.len(), 1, "{case}: {groups:?}");
                let temp = temp_output(&groups[0].0);
                let shutdown_sources: HashSet<PathBuf> = groups[0].1.iter().cloned().collect();
                let shutdown_bytes = file_contents(&shutdown_sources);
                let held = harness.gate(temp_output_phase(held_at, temp.clone()));
                harness.release(claimed);
                harness.wait("held temporary output", |_, phase| {
                    phase_kind(phase) == held_at
                });
                let temp_inode = inode(&temp);
                let temp_bytes = fs::read(&temp).ok();
                if temp_inode.is_none() {
                    violations.push(format!("{case}: temporary output missing at {temp:?}"));
                }
                if changed_files(&shutdown_bytes) != Vec::<PathBuf>::new() {
                    violations.push(format!("{case}: sources changed before publication"));
                }

                let thread = competitor;
                let run = spawn_named(thread, {
                    let staging = staging.clone();
                    move || {
                        if competitor == "init" {
                            staging.flush_and_convert(true, false, &None)
                        } else {
                            staging.prepare_startup_sync(None).unwrap().execute()
                        }
                    }
                });
                if let Err(err) = run.join().unwrap() {
                    violations.push(format!("{case}: competitor failed: {err}"));
                }
                if harness.recovery_candidates_of(thread).contains(&temp) {
                    violations.push(format!("{case}: recovery selected live output {temp:?}"));
                }
                if inode(&temp) != temp_inode || fs::read(&temp).ok() != temp_bytes {
                    violations.push(format!(
                        "{case}: live output {temp:?} was removed or replaced"
                    ));
                }
                let stolen: Vec<_> = harness
                    .claimed_sources_of(thread)
                    .intersection(&shutdown_sources)
                    .cloned()
                    .collect();
                if !stolen.is_empty() {
                    violations.push(format!("{case}: competitor claimed {stolen:?}"));
                }
                let changed = changed_files(&shutdown_bytes);
                if !changed.is_empty() {
                    violations.push(format!(
                        "{case}: sources removed before publication: {changed:?}"
                    ));
                }

                harness.release(held);
                if let Err(err) = shutdown.join().unwrap() {
                    violations.push(format!("{case}: shutdown publication failed: {err}"));
                }
                if fs::metadata(&groups[0].0).is_err() {
                    violations.push(format!("{case}: {:?} was not published", groups[0].0));
                }
                violations.extend(id_mismatch(
                    &format!("{case}: final"),
                    &published_ids(&staging),
                    &sorted_ids(&[0..1000, 9000..9100]),
                ));
                let leftovers = staging_leftovers(&staging);
                if !leftovers.is_empty() {
                    violations.push(format!("{case}: left behind: {leftovers:?}"));
                }
                assert!(violations.is_empty(), "{violations:#?}");
            }
        }
    }

    /// A startup plan is prepared on one thread and executed later. Until it
    /// is executed or dropped, the direct init entries (`flush_and_convert`,
    /// `prepare_parquet`, `convert_disk_files_to_parquet`) and shutdown sync
    /// must not convert its sources. A dropped plan releases them for retry.
    #[test]
    fn deferred_startup_plan_owns_its_sources_until_executed_or_dropped() {
        for finish in ["execute", "drop"] {
            let temp_dir = TempDir::new().unwrap();
            let staging = lifecycle_stream(&temp_dir);
            crash_orphan_dir(
                &staging,
                "processing_1",
                &[
                    (9000..9100, lifecycle_minute() - TimeDelta::minutes(10)),
                    (9100..9200, lifecycle_minute() - TimeDelta::minutes(11)),
                ],
            );
            let mut violations = Vec::new();
            let mut harness = ConversionHarness::install(&staging);

            let plan = spawn_named("planner", {
                let staging = staging.clone();
                move || staging.prepare_startup_sync(None).unwrap()
            })
            .join()
            .unwrap();
            let plan_sources = harness.claimed_sources_of("planner");
            assert_eq!(plan_sources.len(), 2, "{finish}: {plan_sources:?}");
            let plan_bytes = file_contents(&plan_sources);

            // The init entry, held after opening readers if it took the plan's
            // sources, so an overlap converts both copies concurrently.
            let claimed = harness.gate(on_thread("init", "ConversionClaimed"));
            let init = spawn_named("init", {
                let staging = staging.clone();
                move || staging.flush_and_convert(true, false, &None)
            });
            let groups = claimed_groups(harness.wait("init claim", |thread, phase| {
                thread == "init" && phase_kind(phase) == "ConversionClaimed"
            }));
            let overlap = groups
                .iter()
                .any(|(_, sources)| sources.iter().any(|source| plan_sources.contains(source)));
            let outputs: Vec<PathBuf> = groups.iter().map(|(output, _)| output.clone()).collect();
            let mut held = Vec::new();
            if overlap {
                violations.push(format!("{finish}: init claimed the plan's sources"));
                for _ in &outputs {
                    held.push(harness.gate(readers_opened_for(outputs.clone())));
                }
            }
            harness.release(claimed);
            let init = if overlap {
                // Init converts its groups on Rayon workers.
                for _ in &outputs {
                    harness.wait("init readers", |_, phase| {
                        matches!(phase, LifecyclePhase::ConversionReadersOpened { output, .. } if outputs.contains(output))
                    });
                }
                Some(init)
            } else {
                if let Err(err) = init.join().unwrap() {
                    violations.push(format!("{finish}: init failed: {err}"));
                }
                None
            };

            if !overlap {
                for entry in ["prepare_parquet", "convert_disk_files_to_parquet"] {
                    let result = spawn_named(entry, {
                        let staging = staging.clone();
                        move || {
                            if entry == "prepare_parquet" {
                                staging.prepare_parquet(true, false, &None)
                            } else {
                                staging
                                    .convert_disk_files_to_parquet(None, None, true, false, &None)
                                    .map(drop)
                            }
                        }
                    })
                    .join()
                    .unwrap();
                    if let Err(err) = result {
                        violations.push(format!("{finish}: {entry} failed: {err}"));
                    }
                    if !harness.claimed_sources_of(entry).is_disjoint(&plan_sources) {
                        violations.push(format!("{finish}: {entry} claimed the plan's sources"));
                    }
                }
                // Shutdown sync converts new files meanwhile.
                lifecycle_push_ids(&staging, 0..100);
                if let Err(err) = staging.flush_and_convert(false, true, &None) {
                    violations.push(format!("{finish}: shutdown sync failed: {err}"));
                }
                let changed = changed_files(&plan_bytes);
                if !changed.is_empty() {
                    violations.push(format!(
                        "{finish}: plan sources changed while it was pending: {changed:?}"
                    ));
                }
                violations.extend(id_mismatch(
                    &format!("{finish}: published while the plan is pending"),
                    &published_ids(&staging),
                    &sorted_ids(&[0..100]),
                ));
            } else {
                lifecycle_push_ids(&staging, 0..100);
                staging.flush_and_convert(false, true, &None).unwrap();
            }

            if finish == "execute" {
                if let Err(err) = plan.execute() {
                    violations.push(format!("{finish}: plan failed: {err}"));
                }
            } else {
                drop(plan);
            }
            for gate in held {
                harness.release(gate);
            }
            if let Some(init) = init
                && let Err(err) = init.join().unwrap()
            {
                violations.push(format!("{finish}: init failed: {err}"));
            }
            if finish == "drop" {
                // The dropped plan's sources are free for the next init.
                staging.flush_and_convert(true, false, &None).unwrap();
            }
            violations.extend(id_mismatch(
                &format!("{finish}: final"),
                &published_ids(&staging),
                &sorted_ids(&[0..100, 9000..9200]),
            ));
            let leftovers = staging_leftovers(&staging);
            if !leftovers.is_empty() {
                violations.push(format!("{finish}: left behind: {leftovers:?}"));
            }
            assert!(violations.is_empty(), "{violations:#?}");
        }
    }

    /// A shutdown conversion fails to rename one output and fails to validate
    /// another; a startup plan panics while converting. Each must keep its
    /// sources, leave no temporary output, and release only what it claimed:
    /// the init entry must retry the failed shutdown directory while the
    /// pending plan's directories stay untouched, and a later startup retry
    /// converts those. Every id ends up committed exactly once.
    #[test]
    fn failed_conversions_retain_sources_and_release_only_their_own_claims() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        crash_orphan_dir(
            &staging,
            "processing_1",
            &[
                (9000..9100, lifecycle_minute() - TimeDelta::minutes(10)),
                (9100..9200, lifecycle_minute() - TimeDelta::minutes(11)),
            ],
        );
        let mut violations = Vec::new();
        let mut harness = ConversionHarness::install(&staging);
        let plan = spawn_named("planner", {
            let staging = staging.clone();
            move || staging.prepare_startup_sync(None).unwrap()
        })
        .join()
        .unwrap();
        let plan_sources = harness.claimed_sources_of("planner");
        assert_eq!(plan_sources.len(), 2, "{plan_sources:?}");
        let plan_bytes = file_contents(&plan_sources);

        lifecycle_push_ids(&staging, 0..100);
        push_ids_at(
            &staging,
            100..200,
            lifecycle_minute() - TimeDelta::minutes(2),
        );
        let claimed = harness.gate(on_thread("shutdown", "ConversionClaimed"));
        let shutdown = spawn_named("shutdown", {
            let staging = staging.clone();
            move || staging.flush_and_convert(false, true, &None)
        });
        let groups = claimed_groups(harness.wait("shutdown claim", |thread, phase| {
            thread == "shutdown" && phase_kind(phase) == "ConversionClaimed"
        }));
        assert_eq!(groups.len(), 2, "{groups:?}");
        let shutdown_sources: HashSet<PathBuf> = groups
            .iter()
            .flat_map(|(_, sources)| sources.clone())
            .collect();
        let shutdown_bytes = file_contents(&shutdown_sources);
        let rename_fails = temp_output(&groups[0].0);
        let validation_fails = temp_output(&groups[1].0);
        let written = harness.gate(temp_output_phase(
            "ParquetPartWritten",
            rename_fails.clone(),
        ));
        let closed = harness.gate(temp_output_phase(
            "ParquetPartClosed",
            validation_fails.clone(),
        ));
        harness.release(claimed);
        // The groups convert in parallel, so handle them in either order.
        for _ in 0..2 {
            let phase = harness.wait("injected failure point", |_, phase| {
                *phase == LifecyclePhase::ParquetPartWritten(rename_fails.clone())
                    || *phase == LifecyclePhase::ParquetPartClosed(validation_fails.clone())
            });
            if !changed_files(&shutdown_bytes).is_empty() {
                violations.push(format!("sources changed before publication at {phase:?}"));
            }
            if let LifecyclePhase::ParquetPartWritten(temp) = phase {
                // The final rename will fail.
                fs::remove_file(&temp).unwrap();
                harness.release(written);
            } else {
                // Validation will fail.
                File::create(&validation_fails).unwrap();
                harness.release(closed);
            }
        }
        if shutdown.join().unwrap().is_ok() {
            violations.push("shutdown reported success for a failed rename".to_owned());
        }
        let changed = changed_files(&shutdown_bytes);
        if !changed.is_empty() {
            violations.push(format!("failed conversion lost sources: {changed:?}"));
        }
        if !published_ids(&staging).is_empty() {
            violations.push("failed conversion published output".to_owned());
        }
        for temp in [&rename_fails, &validation_fails] {
            if temp.exists() {
                violations.push(format!("failed conversion left {temp:?}"));
            }
        }

        // Init retries the failed shutdown directory, not the pending plan's.
        let init = spawn_named("init", {
            let staging = staging.clone();
            move || staging.flush_and_convert(true, false, &None)
        });
        if let Err(err) = init.join().unwrap() {
            violations.push(format!("init retry failed: {err}"));
        }
        let init_sources = harness.claimed_sources_of("init");
        if init_sources != shutdown_sources {
            violations.push(format!(
                "init claimed {init_sources:?}, expected the released {shutdown_sources:?}"
            ));
        }
        let changed = changed_files(&plan_bytes);
        if !changed.is_empty() {
            violations.push(format!("pending plan sources changed: {changed:?}"));
        }
        violations.extend(id_mismatch(
            "after init retry",
            &published_ids(&staging),
            &sorted_ids(&[0..200]),
        ));
        let retried = published_files_by_name(&staging);

        // The plan panics in conversion; its claim must be released.
        harness.panic_at(on_thread("startup", "ConversionReadersOpened"));
        let startup = spawn_named("startup", move || plan.execute());
        if startup.join().is_ok() {
            violations.push("startup plan did not reach its injected panic".to_owned());
        }
        let changed = changed_files(&plan_bytes);
        if !changed.is_empty() {
            violations.push(format!("panicked plan lost sources: {changed:?}"));
        }
        if published_files_by_name(&staging) != retried {
            violations.push("panicked plan changed published outputs".to_owned());
        }

        let retry = spawn_named("startup-retry", {
            let staging = staging.clone();
            move || staging.prepare_startup_sync(None).unwrap().execute()
        });
        if let Err(err) = retry.join().unwrap() {
            violations.push(format!("startup retry failed: {err}"));
        }
        if harness.claimed_sources_of("startup-retry") != plan_sources {
            violations.push("startup retry did not reclaim the panicked plan's sources".to_owned());
        }
        violations.extend(id_mismatch(
            "final",
            &published_ids(&staging),
            &sorted_ids(&[0..200, 9000..9200]),
        ));
        let leftovers = staging_leftovers(&staging);
        if !leftovers.is_empty() {
            violations.push(format!("left behind: {leftovers:?}"));
        }
        assert!(violations.is_empty(), "{violations:#?}");
    }

    fn published_files_by_name(staging: &StreamRef) -> HashMap<PathBuf, Vec<u8>> {
        staging
            .parquet_files()
            .into_iter()
            .map(|path| {
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect()
    }

    /// Recovery publishes a legacy bare `.part` orphan under the `.arrows`
    /// name that a new writer for the same key also publishes to when it is
    /// dropped under the writer lock (as push does when a first write fails).
    /// Neither publication may replace the other.
    #[test]
    fn recovered_legacy_part_never_replaces_a_concurrent_publication() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        lifecycle_push_ids(&staging, 9000..9100);
        let orphan = part_files(&staging).pop().unwrap();
        std::mem::forget(staging.writer.lock().unwrap().take_flushable_disk(true));
        // `name.data.arrows.part` -> legacy `name.data.part`.
        let legacy = orphan
            .with_extension("")
            .with_extension(PART_FILE_EXTENSION);
        fs::rename(&orphan, &legacy).unwrap();
        let shared_name = orphan.with_extension("");
        let mut violations = Vec::new();
        let mut harness = ConversionHarness::install(&staging);

        let publishing = harness.gate(on_thread("recovery", "RecoveryPublishing"));
        let recovery = spawn_named("recovery", {
            let staging = staging.clone();
            move || staging.recover_orphan_part_files()
        });
        let chosen = harness.wait("recovery publishing", |thread, phase| {
            thread == "recovery" && phase_kind(phase) == "RecoveryPublishing"
        });
        assert_eq!(
            chosen,
            LifecyclePhase::RecoveryPublishing(shared_name.clone())
        );

        let publish_competitor = {
            let staging = staging.clone();
            let base = shared_name.clone();
            move || {
                let _writer = staging.writer.lock().unwrap();
                let schema = Schema::new(vec![
                    Field::new(
                        DEFAULT_TIMESTAMP_KEY,
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("id", DataType::Int32, false),
                    Field::new("value", DataType::Utf8, false),
                ]);
                let batch = RecordBatch::try_new(
                    Arc::new(schema.clone()),
                    vec![
                        Arc::new(TimestampMillisecondArray::from_iter_values(0..100)),
                        Arc::new(Int32Array::from_iter_values(0..100)),
                        Arc::new(StringArray::from_iter_values(
                            (0..100).map(|id| format!("payload-{id}")),
                        )),
                    ],
                )
                .unwrap();
                let range = TimeRange::granularity_range(
                    lifecycle_minute().and_local_timezone(Utc).unwrap(),
                    OBJECT_STORE_DATA_GRANULARITY,
                );
                let mut writer =
                    crate::parseable::staging::writer::DiskWriter::try_new(base, &schema, range)
                        .unwrap();
                writer.write(&batch).unwrap();
            }
        };
        let competitor = match staging.writer.try_lock() {
            // Recovery does not hold the writer lock across its name choice.
            Ok(guard) => {
                drop(guard);
                publish_competitor();
                None
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                Some(spawn_named("ingest", publish_competitor))
            }
            Err(err) => panic!("{err}"),
        };
        harness.release(publishing);
        recovery.join().unwrap();
        if let Some(competitor) = competitor {
            competitor.join().unwrap();
        }

        if staging.arrow_files().len() != 2 {
            violations.push(format!(
                "expected two published files, found {:?}",
                staging.arrow_files()
            ));
        }
        violations.extend(id_mismatch(
            "final",
            &convert_and_read_ids(&staging),
            &sorted_ids(&[0..100, 9000..9100]),
        ));
        let leftovers = staging_leftovers(&staging);
        if !leftovers.is_empty() {
            violations.push(format!("left behind: {leftovers:?}"));
        }
        assert!(violations.is_empty(), "{violations:#?}");
    }

    /// A claim releases only the entries still registered to it, and a
    /// claimed processing-directory name is never handed out again, even
    /// after its owner has removed the directory.
    #[test]
    fn conversion_claim_releases_only_its_own_entries() {
        let temp_dir = TempDir::new().unwrap();
        let staging = lifecycle_stream(&temp_dir);
        fs::create_dir_all(&staging.data_path).unwrap();
        let minute = 7;
        let preferred = Stream::inprocess_folder(&staging.data_path, minute);

        let mut first = ConversionClaim::new(&staging.conversion_claims);
        assert_eq!(
            staging
                .claim_new_inprocess_folder(&mut first, minute)
                .unwrap(),
            preferred
        );
        // The owner finished with the directory but still holds the claim.
        fs::remove_dir(&preferred).unwrap();
        let mut second = ConversionClaim::new(&staging.conversion_claims);
        let second_dir = staging
            .claim_new_inprocess_folder(&mut second, minute)
            .unwrap();
        assert_ne!(second_dir, preferred);
        // Startup cannot take a directory a running conversion owns.
        let mut startup = ConversionClaim::new(&staging.conversion_claims);
        assert!(
            staging
                .claim_unowned_inprocess_dirs(&mut startup, true)
                .is_empty()
        );

        // A stale entry for `first` that now belongs to `second` survives
        // `first` being dropped.
        let output = staging.data_path.join("x.data.ulid.parquet.part");
        first.outputs.push(output.clone());
        lock_conversion_claims(&staging.conversion_claims)
            .outputs
            .insert(output.clone(), second.id);
        drop(first);
        {
            let registry = lock_conversion_claims(&staging.conversion_claims);
            assert!(!registry.dirs.contains_key(&preferred));
            assert_eq!(registry.dirs.get(&second_dir), Some(&second.id));
            assert_eq!(registry.outputs.get(&output), Some(&second.id));
        }
        second.outputs.push(output.clone());
        drop(second);
        drop(startup);
        let registry = lock_conversion_claims(&staging.conversion_claims);
        assert!(registry.dirs.is_empty());
        assert!(registry.outputs.is_empty());
    }
}
