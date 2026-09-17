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
use chrono::{NaiveDate, NaiveDateTime, Timelike, Utc};
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
use relative_path::RelativePathBuf;
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
use tracing::{error, info, info_span, instrument, trace, warn};
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
        writer::Writer,
    },
};

static HOSTNAME: OnceCell<String> = OnceCell::new();
static METRIC_PARQUET_THREADPOOL: Lazy<rayon::ThreadPool> = Lazy::new(|| {
    rayon::ThreadPoolBuilder::new()
        .thread_name(|index| format!("metric-parquet-{index}"))
        .build()
        .expect("Metric Parquet thread pool should be constructible")
});

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

/// All state associated with a single logstream in Parseable.
pub struct Stream {
    pub stream_name: String,
    pub metadata: RwLock<LogStreamMetadata>,
    pub data_path: PathBuf,
    pub options: Arc<Options>,
    pub writer: Mutex<Writer>,
    schema_writer: Mutex<()>,
    pub ingestor_id: Option<String>,
}

/// Startup conversion owns this immutable processing-file snapshot. Periodic
/// sync may run while it is consumed because it only claims root Arrow files
/// and its newly-created processing directory.
pub(crate) struct StartupSyncPlan {
    stream: StreamRef,
    tenant_id: Option<String>,
    staging_files: Vec<(PathBuf, Vec<PathBuf>)>,
}

#[derive(Default)]
struct ArrowFileConversionOutcome {
    schema: Option<Schema>,
    first_error: Option<StagingError>,
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
        loop {
            let Some(plan) = self.plans.as_slice().first() else {
                break;
            };
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
        if let Some(err) = self.first_error {
            Err(err)
        } else {
            Ok(self.schema)
        }
    }
}

impl StartupSyncPlan {
    pub(crate) fn execute(self) -> Result<(), StagingError> {
        let time_partition = self.stream.get_time_partition();
        let custom_partition = self.stream.get_custom_partition();
        let outcome = self.stream.convert_arrow_file_groups_to_parquet(
            self.staging_files,
            time_partition.as_ref(),
            custom_partition.as_ref(),
            &self.tenant_id,
            ArrowGroupExecution::Sequential,
        )?;
        self.stream.finish_arrow_file_conversion(outcome)
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
            data_path,
            options,
            writer: Mutex::new(Writer::default()),
            schema_writer: Mutex::new(()),
            ingestor_id,
        })
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

            guard.push_disk(filename, record, file_path, range)?;
        }

        if let Some(mem) = guard.mem.as_mut() {
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

        dir.flatten()
            .map(|file| file.path())
            .filter(|file| {
                file.extension()
                    .is_some_and(|ext| ext.eq(ARROW_FILE_EXTENSION))
            })
            .filter_map(|f| {
                let modified = f.metadata().ok().and_then(|m| m.modified().ok());
                modified.map(|modified_time| (f, modified_time))
            })
            .sorted_by_key(|(_, modified_time)| *modified_time)
            .map(|(f, _)| f)
            .collect()
    }

    pub fn inprocess_arrow_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        // iterate through all the inprocess_ directories and collect all arrow files
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
        .collect::<Vec<_>>()
    }

    /// Groups arrow files which are to be included in one parquet
    ///
    /// Excludes the arrow file being written for the current minute (data is still being written to that one)
    ///
    /// Only includes ones starting from the previous minute
    pub fn arrow_files_grouped_exclude_time(
        &self,
        exclude: SystemTime,
        group_minute: u128,
        init_signal: bool,
        shutdown_signal: bool,
    ) -> Vec<(PathBuf, Vec<PathBuf>)> {
        if init_signal {
            // Startup owns only this one-time snapshot of processing files.
            // Root Arrow files are left for regular sync so startup never
            // races a live writer or a periodic cycle for the same file.
            self.group_inprocess_arrow_files(&Ulid::new().to_string())
        } else {
            let arrow_files = self.fetch_arrow_files_for_conversion(exclude, shutdown_signal);
            if arrow_files.is_empty() {
                return Vec::new();
            }

            // Never reuse a directory that may belong to the immutable startup
            // snapshot (possible when a process restarts within the same minute).
            let inprocess_dir = match self.create_inprocess_folder(group_minute) {
                Ok(inprocess_dir) => inprocess_dir,
                Err(e) => {
                    error!("Failed to create inprocess directory: {e}");
                    return Vec::new();
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
        }
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

    /// Takes one logical snapshot across every existing processing directory.
    /// Files sharing the same event-minute/output prefix are consolidated even
    /// when a crash left them in different processing directories.
    fn group_inprocess_arrow_files(&self, random_string: &str) -> Vec<(PathBuf, Vec<PathBuf>)> {
        let mut grouped: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for inprocess_file in self.inprocess_arrow_files() {
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
    ) -> Vec<PathBuf> {
        let mut arrow_files = self.arrow_files();
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
        for arrow_file_path in arrow_files {
            match arrow_file_path.metadata() {
                Ok(meta) if meta.len() == 0 => {
                    error!(
                        "Invalid arrow file {:?} detected for stream {}, removing it",
                        &arrow_file_path, self.stream_name
                    );
                    remove_file(&arrow_file_path).expect("File should be removed");
                }
                Ok(_) => {
                    let new_path = inprocess_dir.join(
                        arrow_file_path
                            .file_name()
                            .expect("Arrow file should have a name"),
                    );
                    if let Err(e) = fs::rename(&arrow_file_path, &new_path) {
                        error!(
                            "Failed to rename arrow file to inprocess directory: {} -> {}: {e}",
                            arrow_file_path.display(),
                            new_path.display()
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Could not get metadata for arrow file {}: {e}",
                        arrow_file_path.display()
                    );
                }
            }
        }
    }

    fn inprocess_folder(base: &Path, minute: u128) -> PathBuf {
        base.join(format!("{INPROCESS_DIR_PREFIX}{minute}"))
    }

    /// Atomically reserves a processing directory. Overlapping periodic
    /// cycles therefore never share one directory, even when they start in
    /// the same minute.
    fn create_inprocess_folder(&self, minute: u128) -> std::io::Result<PathBuf> {
        let preferred = Self::inprocess_folder(&self.data_path, minute);
        match fs::create_dir(&preferred) {
            Ok(()) => return Ok(preferred),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }

        loop {
            let unique = self
                .data_path
                .join(format!("{INPROCESS_DIR_PREFIX}{minute}_{}", Ulid::new()));
            match fs::create_dir(&unique) {
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
        )?;
        self.finish_arrow_file_conversion(outcome)
    }

    pub fn stage_schema_file(&self, mut schema: Schema) -> Result<(), StagingError> {
        let _schema_writer = self.schema_writer.lock().map_err(|poisoned| {
            StagingError::PoisonError(PoisonError::new(format!(
                "Schema writer lock poisoned while staging schema for stream {} - {}",
                self.stream_name, poisoned
            )))
        })?;

        // schema is dynamic, read from staging and merge if present
        fs::create_dir_all(&self.data_path)?;

        // need to add something before .schema to make the file have an extension of type `schema`
        let file_name = self.ingestor_id.as_ref().map_or_else(
            || ".schema".to_owned(),
            |id| format!(".ingestor.{id}.schema"),
        );
        let path = RelativePathBuf::from_iter([file_name]).to_path(&self.data_path);
        let tmp_path = path.with_extension("schema.tmp");

        let staging_schemas = self.get_schemas_if_present()?;
        if !staging_schemas.is_empty() {
            let mut staging_schemas = staging_schemas;
            staging_schemas.push(schema);
            schema = Schema::try_merge(staging_schemas)?;
        }

        // save the merged schema on staging disk
        // the path should be stream/.ingestor.{id}.schema
        info!("writing schema to path - {path:?}");
        write(&tmp_path, to_bytes(&schema))?;
        fs::rename(tmp_path, path)?;

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
        let _span = info_span!("flush", stream_name = %self.stream_name, forced).entered();
        // Swap out stale writers under the lock, drop them after releasing it.
        // DiskWriter::Drop does I/O (IPC finish + file rename) so dropping
        // outside the lock avoids blocking concurrent push() calls.
        let stale_writers = {
            let mut writer = self.writer.lock().map_err(|poisoned| {
                StagingError::PoisonError(PoisonError::new(format!(
                    "Writer lock poisoned while flushing data for stream {} - {}",
                    self.stream_name, poisoned
                )))
            })?;

            if let Some(mem) = writer.mem.as_mut() {
                mem.clear();
            }
            writer.take_flushable_disk(forced)
        };
        // DiskWriter::Drop I/O happens here, outside the lock
        drop(stale_writers);
        Ok(())
    }

    fn parquet_writer_props(
        &self,
        merged_schema: &Schema,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
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
            let bloom_filter_position = if PARSEABLE.options.bloom_filter_default_position {
                BloomFilterPosition::AfterRowGroup
            } else {
                BloomFilterPosition::End
            };
            props = props
                .set_column_bloom_filter_enabled(column_path.clone(), true)
                .set_column_bloom_filter_max_ndv(column_path, METRIC_NAME_BLOOM_FILTER_NDV)
                .set_bloom_filter_position(bloom_filter_position);
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

        // Set sorting columns
        props.set_sorting_columns(Some(sorting_column_vec)).build()
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
    ) -> Result<ArrowFileConversionOutcome, StagingError> {
        let span = info_span!(
            "convert_disk_files_to_parquet",
            stream_name = %self.stream_name,
            file_group_count = tracing::field::Empty,
        );
        let _guard = span.enter();

        let now = SystemTime::now();
        let group_minute = minute_from_system_time(now) - 1;
        let staging_files =
            self.arrow_files_grouped_exclude_time(now, group_minute, init_signal, shutdown_signal);
        span.record("file_group_count", staging_files.len());
        self.convert_arrow_file_groups_to_parquet(
            staging_files,
            time_partition,
            custom_partition,
            tenant_id,
            ArrowGroupExecution::Parallel,
        )
    }

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
                arrow_files,
                time_partition,
                custom_partition,
                tenant_id,
            );
            (parquet_path, result)
        };
        let results: Vec<_> = match execution {
            // Startup groups are ordered oldest-first and remain sequential so
            // recovery memory stays bounded.
            ArrowGroupExecution::Sequential => staging_files.into_iter().map(convert).collect(),
            // A periodic processing directory can contain many overdue minute
            // groups. Convert only two at a time so a single stream cannot
            // flood the shared Rayon pool with column-encoding work.
            ArrowGroupExecution::Parallel => {
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
            }
        };

        for (parquet_path, result) in results {
            match result {
                Ok(Some(schema)) => schemas.push(schema),
                Ok(None) => {}
                Err(err) => {
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

        let schema = if schemas.is_empty() {
            None
        } else {
            Some(Schema::try_merge(schemas)?)
        };
        Ok(ArrowFileConversionOutcome {
            schema,
            first_error,
        })
    }

    /// Stages schemas from successful groups before surfacing an unresolved
    /// group failure to the caller.
    fn finish_arrow_file_conversion(
        &self,
        outcome: ArrowFileConversionOutcome,
    ) -> Result<(), StagingError> {
        if let Some(schema) = outcome.schema
            && !self.get_static_schema_flag()
        {
            self.stage_schema_file(schema)?;
        }
        if let Some(err) = outcome.first_error {
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
        // Metrics sort every output row group explicitly, so reading their IPC
        // streams forward avoids reverse seeks and a full-column row reversal.
        let record_reader =
            ConversionRecordReader::for_stream(&arrow_files, self.is_otel_metrics());
        self.remove_invalid_arrow_files(record_reader.invalid_files(), tenant_id);
        let readable_arrow_files = record_reader.readable_files().to_vec();
        if record_reader.reader_count() == 0 {
            return Ok(None);
        }

        let merged_schema = record_reader.merged_schema();
        let props = self.parquet_writer_props(&merged_schema, time_partition, custom_partition);
        let schema = Arc::new(merged_schema.clone());
        let mut part_path = parquet_path.clone();
        part_path.add_extension(PART_FILE_EXTENSION);

        let write_result = self.write_parquet_part_file(
            &part_path,
            record_reader,
            &schema,
            &props,
            time_partition,
        );
        match write_result {
            Ok(true) => {}
            Ok(false) => {
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
        }

        if let Err(err) = std::fs::rename(&part_path, &parquet_path) {
            error!("Couldn't rename part file: {part_path:?} -> {parquet_path:?}, error = {err}");
            Self::remove_partial_parquet_file(&part_path);
            return Err(err.into());
        }

        self.cleanup_arrow_files_and_dir(&readable_arrow_files, tenant_id);
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
    ) -> Result<bool, StagingError> {
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
        // does pruning help with query?
        let sort_for_metric_pruning = self.is_otel_metrics();
        let time_partition_field = time_partition.map_or_else(
            || DEFAULT_TIMESTAMP_KEY.to_string(),
            |s| s.as_str().to_string(),
        );

        if sort_for_metric_pruning {
            let conversion_started = Instant::now();
            let source_arrow_files = record_reader.readable_files().len();
            let source_arrow_bytes = record_reader
                .readable_files()
                .iter()
                .filter_map(|path| path.metadata().ok().map(|metadata| metadata.len()))
                .sum::<u64>();
            let mut timings = MetricParquetPhaseTimings::default();
            let arrow_writer =
                ArrowWriter::try_new(&mut part_file, schema.clone(), Some(props.clone()))?;
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

            // Producer reads, decodes, concatenates, and sorts the next exact
            // row group while this thread encodes the current row group. The
            // bounded channel limits prepared batches retained in memory.
            let prepared_row_groups = Self::spawn_metric_row_group_pipeline(
                record_reader,
                schema.clone(),
                time_partition.cloned(),
                time_partition_field,
                target,
            );
            let mut preparation_complete = false;
            let mut next_row_group_index = writer.flushed_row_groups().len();
            let mut pending_encodings = VecDeque::with_capacity(*METRIC_ROW_GROUP_ENCODE_IN_FLIGHT);
            while !preparation_complete || !pending_encodings.is_empty() {
                if !preparation_complete
                    && pending_encodings.len() < *METRIC_ROW_GROUP_ENCODE_IN_FLIGHT
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
            writer.close()?;
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
        } else {
            let mut writer =
                ArrowWriter::try_new(&mut part_file, schema.clone(), Some(props.clone()))?;
            for record in record_reader.merged_iter(schema.clone(), time_partition.cloned()) {
                writer.write(&record?)?;
            }
            writer.close()?;
        }

        drop(part_file);

        if !Self::is_valid_parquet_file(part_path, &self.stream_name) {
            error!(
                "Invalid parquet file {part_path:?} detected for stream {stream_name}, removing it",
                stream_name = &self.stream_name
            );
            Self::remove_partial_parquet_file(part_path);
            return Ok(false);
        }
        trace!("Parquet file successfully constructed");
        Ok(true)
    }

    /// Removes a temporary Parquet output left by a failed conversion.
    fn remove_partial_parquet_file(part_path: &Path) {
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
        // First check file size as a quick validation
        match path.metadata() {
            Ok(meta) if meta.len() < FOOTER_SIZE as u64 => {
                error!(
                    "Invalid parquet file {path:?} detected for stream {stream_name}, size: {} bytes",
                    meta.len()
                );
                return false;
            }
            Err(e) => {
                error!(
                    "Cannot read metadata for parquet file {path:?} for stream {stream_name}: {e}"
                );
                return false;
            }
            _ => {} // File size is adequate, continue validation
        }

        // Try to open and read the parquet file metadata to verify it's valid
        match std::fs::File::open(path) {
            Ok(file) => match SerializedFileReader::new(file) {
                Ok(reader) => {
                    if reader.metadata().file_metadata().num_rows() == 0 {
                        error!("Invalid parquet file {path:?} for stream {stream_name}");
                        false
                    } else {
                        true
                    }
                }
                Err(e) => {
                    error!("Failed to read parquet file {path:?} for stream {stream_name}: {e}");
                    false
                }
            },
            Err(e) => {
                error!("Failed to open parquet file {path:?} for stream {stream_name}: {e}");
                false
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
        for parent_dir in processing_dirs {
            match fs::read_dir(&parent_dir) {
                Ok(mut entries) => {
                    if entries.next().is_none()
                        && let Err(err) = fs::remove_dir(&parent_dir)
                    {
                        warn!(
                            "Failed to remove inprocess directory {}: {err}",
                            parent_dir.display()
                        );
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    warn!(
                        "Failed to read inprocess directory {}: {err}",
                        parent_dir.display()
                    );
                }
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

    pub fn get_schema_raw(&self) -> HashMap<String, Arc<Field>> {
        self.metadata.read().expect(LOCK_EXPECT).schema.clone()
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
    fn recover_orphan_part_files(&self) {
        let Ok(dir) = self.data_path.read_dir() else {
            return;
        };

        for entry in dir.flatten() {
            let path = entry.path();
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
                                    if let Err(delete_err) = remove_file(&path) {
                                        error!(
                                            "Failed to remove invalid/corrupted .part file {:?}: {delete_err}",
                                            path
                                        );
                                    }
                                } else {
                                    // File has at least one structurally complete batch.
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

                                    // If arrow file with same name exists, generate a unique name
                                    if arrow_path.exists() {
                                        let file_name =
                                            arrow_path.file_name().unwrap().to_string_lossy();
                                        if let Some(date_pos) = file_name.find(".date") {
                                            let random_suffix = ulid::Ulid::new().to_string();
                                            let new_name = format!(
                                                "{}{}",
                                                random_suffix,
                                                &file_name[date_pos..]
                                            );
                                            arrow_path.set_file_name(new_name);
                                        }
                                    }

                                    info!(
                                        "Recovering orphaned .part file: {:?} -> {:?} for stream {}",
                                        path, arrow_path, self.stream_name
                                    );
                                    if let Err(e) = std::fs::rename(&path, &arrow_path) {
                                        error!(
                                            "Failed to rename .part file {:?} to {:?}: {e}",
                                            path, arrow_path
                                        );
                                    }
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

    /// Recovers and flushes startup writers, then snapshots only files already
    /// present in processing directories. Root Arrow files remain owned by
    /// periodic sync, which can start as soon as every stream is snapshotted.
    fn prepare_startup_sync(
        self: &Arc<Self>,
        tenant_id: Option<String>,
    ) -> Result<StartupSyncPlan, StagingError> {
        self.recover_orphan_part_files();

        let start_flush = Instant::now();
        self.flush(true)?;
        if self.get_stream_type().eq(&StreamType::UserDefined) {
            info!(
                "Startup flush for stream ({}) took: {}s",
                self.stream_name,
                start_flush.elapsed().as_secs_f64()
            );
        }

        let staging_files = self.group_inprocess_arrow_files(&Ulid::new().to_string());
        info!(
            "Captured {} startup parquet groups for stream {}",
            staging_files.len(),
            self.stream_name
        );
        Ok(StartupSyncPlan {
            stream: Arc::clone(self),
            tenant_id,
            staging_files,
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
        // On init, recover any orphaned .part files from previous interrupted runs
        if init_signal {
            self.recover_orphan_part_files();
        }

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

        self.prepare_parquet(init_signal, shutdown_signal, tenant_id)?;
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
mod tests {
    use std::{io::Write, sync::Barrier, thread::spawn, time::Duration};

    use arrow_array::{Int32Array, StringArray, TimestampMillisecondArray};
    use arrow_ipc::writer::StreamWriter as ArrowStreamWriter;
    use arrow_schema::{DataType, Field, TimeUnit};
    use chrono::{NaiveDate, TimeDelta, Utc};
    use temp_dir::TempDir;
    use tokio::time::sleep;

    use super::*;

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
                )
                .unwrap()
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

        assert!(outcome.schema.is_some());
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
}
