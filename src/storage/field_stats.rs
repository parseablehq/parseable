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

use crate::event::USER_AGENT_KEY;
use crate::event::format::EventFormat;
use crate::event::format::LogSource;
use crate::event::format::LogSourceEntry;
use crate::event::format::json;
use crate::handlers::TelemetryType;
use crate::handlers::http::cluster::send_query_request;
use crate::handlers::http::ingest::PostError;
use crate::handlers::http::middleware::CLUSTER_SECRET;
use crate::handlers::http::middleware::CLUSTER_SECRET_HEADER;
use crate::handlers::http::modal::utils::ingest_utils::json_byte_size;
use crate::handlers::http::query::Query;
use crate::handlers::http::query::QueryError;
use crate::handlers::http::query::query;
use crate::metadata::SchemaVersion;
use crate::option::Mode;
use crate::parseable::DEFAULT_TENANT;
use crate::parseable::PARSEABLE;
use crate::storage::ObjectStorageError;
use crate::storage::StreamType;
use crate::tenants::TENANT_METADATA;
use crate::utils::get_tenant_id_from_request;
use crate::utils::get_user_from_request;
use crate::utils::header_parsing::{
    HeaderBuildError, ParseHeaderError, generated_text_header_value,
};
use crate::utils::json::flatten::FlattenLimits;
use crate::utils::json::flatten::validate_custom_partition;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::body::MessageBody;
use actix_web::http::header::HeaderMap;
use actix_web::http::header::HeaderName;
use actix_web::http::header::HeaderValue;
use actix_web::web::Json;
use arrow_array::Array;
use arrow_array::BinaryArray;
use arrow_array::BinaryViewArray;
use arrow_array::BooleanArray;
use arrow_array::Date32Array;
use arrow_array::Float64Array;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_array::TimestampMillisecondArray;
use arrow_schema::DataType;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use dashmap::DashMap;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use regex::Regex;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

pub const DATASET_STATS_STREAM_NAME: &str = "pstats";
const DATASET_STATS_CUSTOM_PARTITION: &str = "dataset_name";
const MIN_TRACKED_DISTINCT_VALUES_PER_FIELD: usize = 1024;
const MAX_TRACKED_DISTINCT_VALUES_PER_FIELD: usize = 10_000;
const NULL_VALUE: &str = "NULL";
const HLL_PRECISION_BITS: u32 = 12;
const HLL_REGISTER_COUNT: usize = 1 << HLL_PRECISION_BITS;
static FIELD_STATS_RAYON_POOL: Lazy<ThreadPool> = Lazy::new(|| {
    ThreadPoolBuilder::new()
        .thread_name(|index| format!("field-stats-{index}"))
        .build()
        .expect("field stats rayon pool should initialize")
});

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct DistinctStat {
    pub distinct_value: String,
    pub count: i64,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FieldStat {
    pub field_name: String,
    pub count: i64,
    pub distinct_count: i64,
    pub distinct_stats: Vec<DistinctStat>,
}

const MAX_STAGED_FIELD_STATS_CACHE_ENTRIES: usize = 512;
const STAGED_FIELD_STATS_CACHE_TTL: Duration = Duration::from_secs(900);

struct CachedFieldStats {
    max_field_statistics: usize,
    stats: Vec<FieldStat>,
    inserted_at: Instant,
}

static STAGED_FIELD_STATS_CACHE: LazyLock<DashMap<PathBuf, CachedFieldStats>> =
    LazyLock::new(DashMap::new);

/// Caches precomputed per-field statistics for a newly staged Parquet file so
/// post-upload field stats publication can consume them in memory without
/// re-opening and re-decoding the Parquet file from disk.
pub fn cache_staged_field_stats(path: &Path, max_field_statistics: usize, stats: Vec<FieldStat>) {
    let now = Instant::now();
    if !STAGED_FIELD_STATS_CACHE.contains_key(path)
        && STAGED_FIELD_STATS_CACHE.len() >= MAX_STAGED_FIELD_STATS_CACHE_ENTRIES
    {
        STAGED_FIELD_STATS_CACHE.retain(|_, entry| {
            now.duration_since(entry.inserted_at) < STAGED_FIELD_STATS_CACHE_TTL
        });
        let mut attempts = 0usize;
        while STAGED_FIELD_STATS_CACHE.len() >= MAX_STAGED_FIELD_STATS_CACHE_ENTRIES && attempts < 8
        {
            attempts += 1;
            let mut oldest: Option<(Instant, PathBuf)> = None;
            for entry in STAGED_FIELD_STATS_CACHE.iter() {
                let ts = entry.value().inserted_at;
                if oldest.as_ref().is_none_or(|(min_ts, _)| ts < *min_ts) {
                    oldest = Some((ts, entry.key().clone()));
                }
            }
            let Some((_, key)) = oldest else {
                break;
            };
            STAGED_FIELD_STATS_CACHE.remove(&key);
        }
    }
    STAGED_FIELD_STATS_CACHE.insert(
        path.to_path_buf(),
        CachedFieldStats {
            max_field_statistics,
            stats,
            inserted_at: now,
        },
    );
}

/// Takes and removes cached field statistics for `path` if present, fresh, and computed
/// with the same `max_field_statistics` bound.
pub fn take_staged_field_stats(path: &Path, max_field_statistics: usize) -> Option<Vec<FieldStat>> {
    let (_, cached) = STAGED_FIELD_STATS_CACHE.remove(path)?;
    if cached.inserted_at.elapsed() < STAGED_FIELD_STATS_CACHE_TTL
        && cached.max_field_statistics == max_field_statistics
    {
        Some(cached.stats)
    } else {
        None
    }
}

/// Removes any cached field statistics for `path` (e.g. when a staged file is retired).
pub fn evict_staged_field_stats(path: &Path) {
    STAGED_FIELD_STATS_CACHE.remove(path);
}

fn filter_field_stats_for_schema(cached: Vec<FieldStat>, schema: &Schema) -> Vec<FieldStat> {
    let mut by_name: HashMap<String, FieldStat> = cached
        .into_iter()
        .map(|stat| (stat.field_name.clone(), stat))
        .collect();
    schema
        .fields()
        .iter()
        .filter_map(|field| by_name.remove(field.name()))
        .collect()
}

#[derive(Serialize, Debug)]
struct DatasetStats {
    dataset_name: String,
    stats_id: String,
    field_stats: Vec<FieldStat>,
}

impl DatasetStats {
    /// The rows of the stats dataset for these stats, borrowed from them: what generic and
    /// then structural flattening (with `_`) make of the stats as JSON. A row for every
    /// distinct value of every field, in order, and a row for a field without any.
    fn rows(&self) -> impl Iterator<Item = StatsRow<'_>> {
        self.field_stats.iter().flat_map(move |field| {
            // Generic flattening makes an empty `distinct_stats` array a null
            let null = DistinctColumns::Null {
                field_stats_distinct_stats: (),
            };
            let no_distinct_values = field.distinct_stats.is_empty().then_some(null);
            field
                .distinct_stats
                .iter()
                .map(|stat| DistinctColumns::Value {
                    field_stats_distinct_stats_distinct_value: &stat.distinct_value,
                    field_stats_distinct_stats_count: stat.count,
                })
                .chain(no_distinct_values)
                .map(move |distinct| StatsRow {
                    dataset_name: &self.dataset_name,
                    stats_id: &self.stats_id,
                    field_stats_field_name: &field.field_name,
                    field_stats_count: field.count,
                    field_stats_distinct_count: field.distinct_count,
                    distinct,
                })
        })
    }
}

/// A row of the stats dataset, see `DatasetStats::rows`. The fields are in the order of the
/// flattened row, which the JSON of the row keeps.
#[derive(Serialize, Debug)]
struct StatsRow<'a> {
    dataset_name: &'a str,
    stats_id: &'a str,
    field_stats_field_name: &'a str,
    field_stats_count: i64,
    field_stats_distinct_count: i64,
    #[serde(flatten)]
    distinct: DistinctColumns<'a>,
}

#[derive(Serialize, Debug)]
#[serde(untagged)]
enum DistinctColumns<'a> {
    Value {
        field_stats_distinct_stats_distinct_value: &'a str,
        field_stats_distinct_stats_count: i64,
    },
    /// `"field_stats_distinct_stats": null`, for a field without distinct values
    Null { field_stats_distinct_stats: () },
}

/// Calculates field statistics for the stream and pushes them to the internal stats dataset.
/// This function creates a new internal stream for stats if it doesn't exist.
/// It collects statistics for each field in the stream
pub async fn calculate_field_stats(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
    max_field_statistics: usize,
    tenant_id: &Option<String>,
) -> Result<bool, PostError> {
    let started_at = Instant::now();
    let result = calculate_field_stats_inner(
        stream_name,
        parquet_path,
        schema,
        max_field_statistics,
        tenant_id,
    )
    .await;
    log_stats_calculation_time(stream_name, parquet_path, started_at, &result);

    result
}

async fn calculate_field_stats_inner(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
    max_field_statistics: usize,
    tenant_id: &Option<String>,
) -> Result<bool, PostError> {
    // create datetime from timestamp present in parquet path
    let parquet_ts = extract_datetime_from_parquet_path_regex(parquet_path).map_err(|e| {
        PostError::Invalid(anyhow::anyhow!(
            "Failed to extract datetime from parquet path: {}",
            e
        ))
    })?;
    let field_stats =
        resolve_field_stats(stream_name, parquet_path, schema, max_field_statistics).await?;
    let mut stats_calculated = false;
    let stats = DatasetStats {
        dataset_name: stream_name.to_string(),
        stats_id: parquet_path.to_string_lossy().to_string(),
        field_stats,
    };
    if stats.field_stats.is_empty() {
        return Ok(stats_calculated);
    }
    stats_calculated = true;
    ensure_dataset_stats_stream(tenant_id).await?;
    let mut p_custom_fields = HashMap::new();
    p_custom_fields.insert(USER_AGENT_KEY.to_string(), "parseable".to_string());
    let stats_stream = PARSEABLE.get_stream(DATASET_STATS_STREAM_NAME, tenant_id)?;
    let max_row_bytes = FlattenLimits::from_config().max_bytes_per_request;
    for_each_stats_event(
        &stats,
        max_row_bytes,
        MAX_STATS_EVENT_BYTES,
        parquet_ts,
        || stats_stream.get_schema_raw(),
        &p_custom_fields,
        tenant_id,
        |event| {
            event.process()?;
            Ok(())
        },
    )?;
    Ok(stats_calculated)
}

/// The rows of one file staged as one event hold at most this much compact JSON. Most files
/// fit in one event. A file whose statistics carry many large distinct values is staged as
/// several, so the memory held and the size of one Arrow batch stay bounded.
const MAX_STATS_EVENT_BYTES: u64 = 8 * 1024 * 1024;

/// All rows of a file have one dataset partition and one timestamp. Convert them together,
/// so that a newly encountered schema is inferred and committed once and not for every row.
/// Rows are grouped, in order, into events of at most `max_event_bytes` of compact JSON; a
/// single row over that is an event of its own. `schema` is read again for each event, as
/// processing the one before it may have committed new fields.
#[allow(clippy::too_many_arguments)]
fn for_each_stats_event(
    stats: &DatasetStats,
    max_row_bytes: usize,
    max_event_bytes: u64,
    parquet_ts: DateTime<Utc>,
    schema: impl Fn() -> HashMap<String, std::sync::Arc<arrow_schema::Field>>,
    custom_fields: &HashMap<String, String>,
    tenant_id: &Option<String>,
    mut process: impl FnMut(crate::event::Event) -> Result<(), PostError>,
) -> Result<(), PostError> {
    let mut rows = Vec::new();
    let mut origin_size = 0u64;
    let mut flush =
        |rows: &mut Vec<serde_json::Value>, origin_size: &mut u64| -> Result<(), PostError> {
            if rows.is_empty() {
                return Ok(());
            }
            process(stats_event(
                std::mem::take(rows),
                std::mem::take(origin_size),
                &stats.dataset_name,
                parquet_ts,
                &schema(),
                custom_fields,
                tenant_id,
            )?)
        };
    for_each_stats_row(stats, max_row_bytes, |row, size| {
        if origin_size.saturating_add(size) > max_event_bytes {
            flush(&mut rows, &mut origin_size)?;
        }
        rows.push(row);
        origin_size += size;
        Ok(())
    })?;
    flush(&mut rows, &mut origin_size)
}

/// One event for `rows`, which are rows of the stats dataset for `dataset_name`.
fn stats_event(
    rows: Vec<serde_json::Value>,
    origin_size: u64,
    dataset_name: &str,
    parquet_ts: DateTime<Utc>,
    schema: &HashMap<String, std::sync::Arc<arrow_schema::Field>>,
    custom_fields: &HashMap<String, String>,
    tenant_id: &Option<String>,
) -> Result<crate::event::Event, PostError> {
    // into_event's generic partition extraction accepts only individual objects.
    // Every row was validated by for_each_stats_row and has dataset_name as its partition.
    let mut event = json::Event {
        json: serde_json::Value::Array(rows),
        p_timestamp: parquet_ts,
    }
    .into_event(
        DATASET_STATS_STREAM_NAME.to_string(),
        origin_size,
        schema,
        false,
        None,
        None,
        SchemaVersion::V1,
        StreamType::Internal,
        custom_fields,
        TelemetryType::Logs,
        tenant_id,
        false,
    )?;
    event.custom_partition_values.insert(
        DATASET_STATS_CUSTOM_PARTITION.to_string(),
        dataset_name.to_owned(),
    );
    Ok(event)
}

/// Hands the rows of the stats dataset for `stats` (see `DatasetStats::rows`) to `process`
/// one at a time, each with its size as compact JSON. A row is measured before it is built,
/// and only built when it is at most `max_row_bytes`, so this holds one row of at most that
/// size at a time, however many fields and distinct values the stats have. A row over
/// it, which takes a distinct value or a field name of about that size, is skipped with a
/// warning, and the rows after it are still processed. Returns the number of rows skipped.
fn for_each_stats_row(
    stats: &DatasetStats,
    max_row_bytes: usize,
    mut process: impl FnMut(serde_json::Value, u64) -> Result<(), PostError>,
) -> Result<usize, PostError> {
    let custom_partition = DATASET_STATS_CUSTOM_PARTITION.to_string();
    let mut skipped = 0;
    for row in stats.rows() {
        let size = json_byte_size(&row);
        if size > max_row_bytes as u64 {
            warn!(
                "Skipping a field stats row of field {} for parquet file {} of stream {}: its {size} bytes of JSON are over P_MAX_FLATTEN_BYTES_PER_REQUEST ({max_row_bytes})",
                shortened(row.field_stats_field_name),
                stats.stats_id,
                stats.dataset_name
            );
            skipped += 1;
            continue;
        }
        let json = serde_json::to_value(&row).map_err(|e| ObjectStorageError::Invalid(e.into()))?;
        // As flattening validated the custom partition of every row
        validate_custom_partition(
            json.as_object().expect("a stats row is a JSON object"),
            Some(&custom_partition),
        )?;
        process(json, size)?;
    }
    Ok(skipped)
}

/// The first 128 characters of `name`, which can be of any length, for a log message
fn shortened(name: &str) -> Cow<'_, str> {
    match name.char_indices().nth(128) {
        Some((end, _)) => Cow::Owned(format!("{}...", &name[..end])),
        None => Cow::Borrowed(name),
    }
}

fn log_stats_calculation_time(
    stream_name: &str,
    parquet_path: &Path,
    started_at: Instant,
    result: &Result<bool, PostError>,
) {
    let parquet_file = parquet_path
        .file_name()
        .and_then(|filename| filename.to_str())
        .unwrap_or("<unknown>");
    let elapsed_ms = started_at.elapsed().as_millis();
    if result.is_ok() {
        debug!(
            "Field stats calculation completed for parquet file {parquet_file} in {elapsed_ms} ms. stream={stream_name}"
        );
    } else {
        // The caller warns about the error, with the stream, tenant and path
        let err = result.as_ref().expect_err("result checked as error");
        debug!(
            "Field stats calculation failed for parquet file {parquet_file} after {elapsed_ms} ms. stream={stream_name}: {err}"
        );
    }
}

async fn ensure_dataset_stats_stream(tenant_id: &Option<String>) -> Result<(), PostError> {
    let log_source_entry = LogSourceEntry::new(LogSource::Json, HashSet::new());
    PARSEABLE
        .create_stream_if_not_exists(
            DATASET_STATS_STREAM_NAME,
            StreamType::Internal,
            Some(&DATASET_STATS_CUSTOM_PARTITION.to_string()),
            vec![log_source_entry],
            TelemetryType::Logs,
            tenant_id,
            vec![],
            vec![],
        )
        .await?;

    Ok(())
}

async fn resolve_field_stats(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
    max_field_statistics: usize,
) -> Result<Vec<FieldStat>, PostError> {
    if let Some(cached) = take_staged_field_stats(parquet_path, max_field_statistics) {
        return Ok(filter_field_stats_for_schema(cached, schema));
    }

    collect_all_field_stats_from_parquet(stream_name, parquet_path, schema, max_field_statistics)
        .await
}

async fn collect_all_field_stats_from_parquet(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
    max_field_statistics: usize,
) -> Result<Vec<FieldStat>, PostError> {
    let stream_name = stream_name.to_string();
    let parquet_path = parquet_path.to_path_buf();
    let schema = schema.clone();

    tokio::task::spawn_blocking(move || {
        collect_all_field_stats_from_parquet_blocking(
            &stream_name,
            &parquet_path,
            &schema,
            max_field_statistics,
        )
    })
    .await
    .map_err(|e| PostError::Invalid(anyhow::anyhow!("Field stats task failed: {}", e)))?
}

/// Incrementally accumulates per-field statistics from Arrow `RecordBatch`es
/// using the exact same chunked counting logic as Parquet-backed field stats.
pub struct FieldStatsAccumulator {
    stream_name: String,
    max_field_statistics: usize,
    field_counts: Vec<(String, FieldCountState)>,
}

impl FieldStatsAccumulator {
    pub fn new(stream_name: &str, schema: &Schema, max_field_statistics: usize) -> Self {
        let field_counts = schema
            .fields()
            .iter()
            .map(|field| {
                let name = field.name().clone();
                (
                    name.clone(),
                    FieldCountState::new(stream_name.to_string(), name, max_field_statistics),
                )
            })
            .collect();
        Self {
            stream_name: stream_name.to_string(),
            max_field_statistics,
            field_counts,
        }
    }

    pub fn update(&mut self, batch: &RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }
        let stream_name = self.stream_name.as_str();
        let max_field_statistics = self.max_field_statistics;
        FIELD_STATS_RAYON_POOL.install(|| {
            self.field_counts.par_iter_mut().for_each(|(name, total)| {
                if let Some(array) = batch.column_by_name(name) {
                    // Retain the old candidate-merge boundaries. Larger candidate states
                    // lose repeated tail values before they can be merged into the total.
                    // Decode and schedule in large batches, but merge in the same order as
                    // the former 1024-row Parquet reader, including across row groups.
                    for offset in (0..array.len()).step_by(1024) {
                        let chunk = array.slice(offset, (array.len() - offset).min(1024));
                        let state = collect_array_field_counts(
                            stream_name,
                            chunk.as_ref(),
                            name,
                            max_field_statistics,
                        );
                        total.merge_state(state);
                    }
                }
            });
        });
    }

    pub fn finish(self) -> Vec<FieldStat> {
        let max_field_statistics = self.max_field_statistics;
        self.field_counts
            .into_iter()
            .filter_map(|(field_name, state)| {
                if state.total_count == 0 {
                    return None;
                }
                Some(FieldStat {
                    field_name,
                    count: state.total_count,
                    distinct_count: state.distinct_count(),
                    distinct_stats: state.into_distinct_stats(max_field_statistics),
                })
            })
            .collect()
    }
}

/// Computes field statistics directly from in-memory Arrow `RecordBatch`es,
/// reusing the exact counting and merge logic of Parquet-backed field stats.
pub fn compute_field_stats_from_batches(
    batches: &[RecordBatch],
    stream_name: &str,
    schema: &Schema,
    max_field_statistics: usize,
) -> Vec<FieldStat> {
    let mut accumulator = FieldStatsAccumulator::new(stream_name, schema, max_field_statistics);
    for batch in batches {
        accumulator.update(batch);
    }
    accumulator.finish()
}

// 32K amortizes scheduling without retaining the whole file. At 120 fixed-width
// columns this is ~30 MiB of decoded values; counting/cache state stays bounded.
const FIELD_STATS_BATCH_ROWS: usize = 32 * 1024;

fn collect_all_field_stats_from_parquet_blocking(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
    max_field_statistics: usize,
) -> Result<Vec<FieldStat>, PostError> {
    collect_field_stats_with_batch_size(
        stream_name,
        parquet_path,
        schema,
        max_field_statistics,
        FIELD_STATS_BATCH_ROWS,
    )
}

fn collect_field_stats_with_batch_size(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
    max_field_statistics: usize,
    batch_rows: usize,
) -> Result<Vec<FieldStat>, PostError> {
    let file = File::open(parquet_path).map_err(|e| {
        PostError::Invalid(anyhow::anyhow!(
            "Failed to open parquet file for field stats: {}",
            e
        ))
    })?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| PostError::Invalid(anyhow::anyhow!(e)))?
        .with_batch_size(batch_rows)
        .build()
        .map_err(|e| PostError::Invalid(anyhow::anyhow!(e)))?;
    let mut accumulator = FieldStatsAccumulator::new(stream_name, schema, max_field_statistics);

    for batch in &mut reader {
        let batch = match batch {
            Ok(batch) => batch,
            Err(e) => {
                warn!(
                    "Skipping undecodable parquet batch while calculating field stats for {}: {}",
                    parquet_path.display(),
                    e
                );
                continue;
            }
        };

        accumulator.update(&batch);
    }

    Ok(accumulator.finish())
}

#[cfg(test)]
fn collect_field_counts(
    stream_name: &str,
    batch: &arrow_array::RecordBatch,
    field_name: &str,
    max_field_statistics: usize,
) -> Option<(String, FieldCountState)> {
    let array = batch.column_by_name(field_name)?;
    Some((
        field_name.to_string(),
        collect_array_field_counts(
            stream_name,
            array.as_ref(),
            field_name,
            max_field_statistics,
        ),
    ))
}

fn collect_array_field_counts(
    stream_name: &str,
    array: &dyn Array,
    field_name: &str,
    max_field_statistics: usize,
) -> FieldCountState {
    let mut field_count = FieldCountState::new_for_batch(
        stream_name.to_string(),
        field_name.to_string(),
        max_field_statistics,
    );

    // Count by native bits, never float equality (NaNs and signed zero matter).
    // Hash the same formatted bytes as before, preserving every HLL register.
    // Native counting is bounded to one logical chunk, including for wide files.
    macro_rules! native_counts {
        ($ty:ty, $key:expr) => {
            if let Some(arr) = array.as_any().downcast_ref::<$ty>() {
                if arr.len() <= MIN_TRACKED_DISTINCT_VALUES_PER_FIELD {
                    // A logical batch fits entirely below the cap. Count native values,
                    // then format/hash each distinct value once, in first-seen order so
                    // the resulting string map has the legacy insertion/merge order.
                    let mut positions: FxHashMap<Option<u64>, usize> = FxHashMap::default();
                    let mut values: Vec<(usize, i64)> = Vec::new();
                    let mut aggregate = true;
                    for i in 0..arr.len() {
                        // Unique numeric columns cannot amortize formatting. Probe only
                        // a short prefix, then use the original single-map path.
                        if i == 32 && values.len() >= 30 {
                            aggregate = false;
                            break;
                        }
                        let key = (!arr.is_null(i)).then(|| ($key)(arr.value(i)));
                        if let Some(&position) = positions.get(&key) {
                            values[position].1 += 1;
                        } else {
                            positions.insert(key, values.len());
                            values.push((i, 1));
                        }
                    }
                    if aggregate {
                        for (i, count) in values {
                            let text = format_arrow_value(array, i);
                            field_count.hll.add(&text);
                            field_count.total_count += count;
                            field_count.track_value(Cow::Owned(text), count);
                        }
                        return field_count;
                    }
                }
                for i in 0..arr.len() {
                    field_count.record_value(Cow::Owned(format_arrow_value(array, i)));
                }
                return field_count;
            }
        };
    }
    native_counts!(TimestampMillisecondArray, |v: i64| v as u64);
    native_counts!(Int64Array, |v: i64| v as u64);
    native_counts!(arrow_array::Int32Array, |v: i32| v as u64);
    native_counts!(arrow_array::Int16Array, |v: i16| v as u64);
    native_counts!(arrow_array::Int8Array, |v: i8| v as u64);
    native_counts!(arrow_array::UInt64Array, |v: u64| v);
    native_counts!(arrow_array::UInt32Array, |v: u32| v as u64);
    native_counts!(arrow_array::UInt16Array, |v: u16| v as u64);
    native_counts!(arrow_array::UInt8Array, |v: u8| v as u64);
    native_counts!(Float64Array, |v: f64| v.to_bits());
    native_counts!(arrow_array::Float32Array, |v: f32| v.to_bits() as u64);
    native_counts!(Date32Array, |v: i32| v as u64);
    native_counts!(BooleanArray, |v: bool| v as u64);

    // Fast paths for the string-like types that dominate log payloads. These read the value as a
    // borrowed `&str` straight out of the Arrow buffer, so a `String` is only allocated when the
    // value is actually inserted into the tracked-values map.
    match array.data_type() {
        DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                for row_index in 0..arr.len() {
                    if arr.is_null(row_index) {
                        field_count.record_value(Cow::Borrowed(NULL_VALUE));
                    } else {
                        field_count.record_value(Cow::Borrowed(arr.value(row_index)));
                    }
                }
                return field_count;
            }
        }
        DataType::Utf8View => {
            if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
                for row_index in 0..arr.len() {
                    if arr.is_null(row_index) {
                        field_count.record_value(Cow::Borrowed(NULL_VALUE));
                    } else {
                        field_count.record_value(Cow::Borrowed(arr.value(row_index)));
                    }
                }
                return field_count;
            }
        }
        _ => {}
    }

    for row_index in 0..array.len() {
        let value = format_arrow_value(array, row_index);
        field_count.record_value(Cow::Owned(value));
    }

    field_count
}

#[cfg(test)]
fn merge_field_counts(
    field_counts: &mut HashMap<String, FieldCountState>,
    field_name: &str,
    batch_field_count: FieldCountState,
) {
    let field_total = field_counts
        .get_mut(field_name)
        .expect("field_counts initialized for each field");

    field_total.merge_state(batch_field_count);
}

struct FieldCountState {
    stream_name: String,
    field_name: String,
    total_count: i64,
    counts: FxHashMap<String, i64>,
    hll: HyperLogLog,
    max_tracked_values: usize,
    /// Lower bound on the smallest count currently in `counts`. Tracked counts only ever grow, so a
    /// stale value stays a valid lower bound. Lets `track_value` reject candidates that cannot
    /// possibly evict anything without paying for a linear scan of `counts`.
    min_count_hint: i64,
    approximate: bool,
    log_cardinality_cap: bool,
}

impl FieldCountState {
    fn new(stream_name: String, field_name: String, max_field_statistics: usize) -> Self {
        Self::new_with_logging(stream_name, field_name, max_field_statistics, true)
    }

    fn new_for_batch(stream_name: String, field_name: String, max_field_statistics: usize) -> Self {
        Self::new_with_logging(stream_name, field_name, max_field_statistics, false)
    }

    fn new_with_logging(
        stream_name: String,
        field_name: String,
        max_field_statistics: usize,
        log_cardinality_cap: bool,
    ) -> Self {
        Self {
            stream_name,
            field_name,
            total_count: 0,
            counts: FxHashMap::default(),
            hll: HyperLogLog::new(),
            max_tracked_values: tracked_distinct_value_limit(max_field_statistics),
            min_count_hint: i64::MAX,
            approximate: false,
            log_cardinality_cap,
        }
    }

    fn record_value(&mut self, value: Cow<'_, str>) {
        self.hll.add(value.as_ref());
        self.total_count += 1;
        self.track_value(value, 1);
    }

    #[cfg(test)]
    fn merge_counts(&mut self, counts: HashMap<String, i64>) {
        for (value, count) in counts {
            self.hll.add(&value);
            self.total_count += count;
            self.track_value(Cow::Owned(value), count);
        }
    }

    fn merge_state(&mut self, state: FieldCountState) {
        self.hll.merge(&state.hll);
        self.total_count += state.total_count;

        if state.approximate {
            self.mark_approximate();
        }

        for (value, count) in state.counts {
            self.track_value(Cow::Owned(value), count);
        }
    }

    fn track_value(&mut self, value: Cow<'_, str>, count: i64) {
        if let Some(existing_count) = self.counts.get_mut(value.as_ref()) {
            *existing_count += count;
            return;
        }

        if self.counts.len() < self.max_tracked_values {
            self.min_count_hint = self.min_count_hint.min(count);
            self.counts.insert(value.into_owned(), count);
            return;
        }

        self.mark_approximate();

        // `min_count_hint` is a lower bound on the smallest tracked count, so a candidate that
        // cannot beat the hint cannot beat the real minimum either. This short-circuits the common
        // high-cardinality case (every row a fresh value with count 1) before the linear scan.
        if count <= self.min_count_hint {
            return;
        }

        if let Some((min_value, min_count)) = self.current_min_value() {
            // Every retained entry has a count >= the evicted minimum, and the inserted candidate
            // is strictly greater, so the old minimum remains a valid lower bound.
            self.min_count_hint = min_count;
            if count > min_count {
                self.counts.remove(&min_value);
                self.counts.insert(value.into_owned(), count);
            }
        }
    }

    fn mark_approximate(&mut self) {
        if !self.approximate {
            self.approximate = true;
            if self.log_cardinality_cap {
                debug!(
                    "Field stats cardinality cap reached for stream {} field {}. Tracking bounded top-value candidates with max_tracked_values={}",
                    self.stream_name, self.field_name, self.max_tracked_values
                );
            }
        }
    }

    fn current_min_value(&self) -> Option<(String, i64)> {
        self.counts
            .iter()
            .min_by(|(left_value, left_count), (right_value, right_count)| {
                left_count
                    .cmp(right_count)
                    .then_with(|| right_value.cmp(left_value))
            })
            .map(|(value, count)| (value.clone(), *count))
    }

    fn distinct_count(&self) -> i64 {
        if self.approximate {
            (self.hll.estimate().round() as i64).max(self.counts.len() as i64)
        } else {
            self.counts.len() as i64
        }
    }

    fn into_distinct_stats(self, max_field_statistics: usize) -> Vec<DistinctStat> {
        let mut distinct_stats = self
            .counts
            .into_iter()
            .map(|(distinct_value, count)| DistinctStat {
                distinct_value,
                count,
            })
            .collect::<Vec<_>>();
        distinct_stats.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.distinct_value.cmp(&right.distinct_value))
        });
        distinct_stats.truncate(max_field_statistics);
        distinct_stats
    }
}

fn tracked_distinct_value_limit(max_field_statistics: usize) -> usize {
    max_field_statistics.clamp(
        MIN_TRACKED_DISTINCT_VALUES_PER_FIELD,
        MAX_TRACKED_DISTINCT_VALUES_PER_FIELD,
    )
}

struct HyperLogLog {
    registers: Vec<u8>,
}

impl HyperLogLog {
    fn new() -> Self {
        Self {
            registers: vec![0; HLL_REGISTER_COUNT],
        }
    }

    fn add(&mut self, value: &str) {
        self.add_hash(hash_field_value(value));
    }

    fn add_hash(&mut self, hash: u64) {
        let register_index = (hash >> (u64::BITS - HLL_PRECISION_BITS)) as usize;
        let rank = ((hash << HLL_PRECISION_BITS).leading_zeros() + 1)
            .min(u64::BITS - HLL_PRECISION_BITS + 1) as u8;
        self.registers[register_index] = self.registers[register_index].max(rank);
    }

    fn merge(&mut self, other: &Self) {
        for (register, other_register) in self.registers.iter_mut().zip(other.registers.iter()) {
            *register = (*register).max(*other_register);
        }
    }

    fn estimate(&self) -> f64 {
        let register_count = HLL_REGISTER_COUNT as f64;
        let zero_registers = self
            .registers
            .iter()
            .filter(|register| **register == 0)
            .count();
        let harmonic_sum = self
            .registers
            .iter()
            .map(|register| 2_f64.powi(-(*register as i32)))
            .sum::<f64>();
        let raw_estimate = hll_alpha(register_count) * register_count.powi(2) / harmonic_sum;

        if raw_estimate <= 2.5 * register_count && zero_registers > 0 {
            register_count * (register_count / zero_registers as f64).ln()
        } else {
            raw_estimate
        }
    }
}

fn hll_alpha(register_count: f64) -> f64 {
    match HLL_REGISTER_COUNT {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / register_count),
    }
}

fn hash_field_value(value: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(value.as_bytes())
}

macro_rules! try_downcast {
    ($ty:ty, $arr:expr, $body:expr) => {
        if let Some(arr) = $arr.as_any().downcast_ref::<$ty>() {
            $body(arr)
        } else {
            warn!(
                "Expected {} for {:?}, but found {:?}",
                stringify!($ty),
                $arr.data_type(),
                $arr.data_type()
            );
            "UNSUPPORTED".to_string()
        }
    };
}

/// Function to format an Arrow value at a given index into a string.
/// Handles null values and different data types by downcasting the array to the appropriate type.
fn format_arrow_value(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Utf8 => try_downcast!(StringArray, array, |arr: &StringArray| arr
            .value(idx)
            .to_string()),
        DataType::Utf8View => try_downcast!(StringViewArray, array, |arr: &StringViewArray| arr
            .value(idx)
            .to_string()),
        DataType::Binary => try_downcast!(BinaryArray, array, |arr: &BinaryArray| {
            String::from_utf8_lossy(arr.value(idx)).to_string()
        }),
        DataType::BinaryView => try_downcast!(BinaryViewArray, array, |arr: &BinaryViewArray| {
            String::from_utf8_lossy(arr.value(idx)).to_string()
        }),
        DataType::Int64 => try_downcast!(Int64Array, array, |arr: &Int64Array| arr
            .value(idx)
            .to_string()),
        DataType::Int32 => try_downcast!(
            arrow_array::Int32Array,
            array,
            |arr: &arrow_array::Int32Array| arr.value(idx).to_string()
        ),
        DataType::Int16 => try_downcast!(
            arrow_array::Int16Array,
            array,
            |arr: &arrow_array::Int16Array| arr.value(idx).to_string()
        ),
        DataType::Int8 => try_downcast!(
            arrow_array::Int8Array,
            array,
            |arr: &arrow_array::Int8Array| arr.value(idx).to_string()
        ),
        DataType::UInt64 => try_downcast!(
            arrow_array::UInt64Array,
            array,
            |arr: &arrow_array::UInt64Array| arr.value(idx).to_string()
        ),
        DataType::UInt32 => try_downcast!(
            arrow_array::UInt32Array,
            array,
            |arr: &arrow_array::UInt32Array| arr.value(idx).to_string()
        ),
        DataType::UInt16 => try_downcast!(
            arrow_array::UInt16Array,
            array,
            |arr: &arrow_array::UInt16Array| arr.value(idx).to_string()
        ),
        DataType::UInt8 => try_downcast!(
            arrow_array::UInt8Array,
            array,
            |arr: &arrow_array::UInt8Array| arr.value(idx).to_string()
        ),
        DataType::Float64 => try_downcast!(Float64Array, array, |arr: &Float64Array| arr
            .value(idx)
            .to_string()),
        DataType::Float32 => try_downcast!(
            arrow_array::Float32Array,
            array,
            |arr: &arrow_array::Float32Array| arr.value(idx).to_string()
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => try_downcast!(
            TimestampMillisecondArray,
            array,
            |arr: &TimestampMillisecondArray| {
                let timestamp = arr.value(idx);
                chrono::DateTime::from_timestamp_millis(timestamp)
                    .map(|dt| dt.naive_utc().format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
                    .unwrap_or_else(|| "INVALID_TIMESTAMP".to_string())
            }
        ),
        DataType::Date32 => try_downcast!(Date32Array, array, |arr: &Date32Array| arr
            .value(idx)
            .to_string()),
        DataType::Boolean => try_downcast!(BooleanArray, array, |arr: &BooleanArray| if arr
            .value(idx)
        {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        DataType::List(_field) => try_downcast!(
            arrow_array::ListArray,
            array,
            |list_array: &arrow_array::ListArray| {
                let child_array = list_array.values();
                let offsets = list_array.value_offsets();
                let start = offsets[idx] as usize;
                let end = offsets[idx + 1] as usize;

                let formatted_values: Vec<String> = (start..end)
                    .map(|i| format_arrow_value(child_array.as_ref(), i))
                    .collect();

                format!("[{}]", formatted_values.join(", "))
            }
        ),
        DataType::Null => "NULL".to_string(),
        _ => {
            warn!(
                "Unsupported array type for statistics: {:?}",
                array.data_type()
            );
            "UNSUPPORTED".to_string()
        }
    }
}

static PARQUET_DATETIME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"date=(\d{4}-\d{2}-\d{2})\.hour=(\d{1,2})\.minute=(\d{1,2})")
        .expect("parquet datetime regex is valid")
});

#[inline(always)]
pub fn extract_datetime_from_parquet_path_regex(
    parquet_path: &Path,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    let filename = parquet_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("Invalid filename")?;

    if let Some(captures) = PARQUET_DATETIME_REGEX.captures(filename) {
        let date = &captures[1];
        let hour = &captures[2];
        let minute = &captures[3];

        // Create datetime string
        let datetime_str = format!("{} {}:{}:00", date, hour, minute);

        // Parse the datetime
        let naive_dt = NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M:%S")?;
        let datetime = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);

        Ok(datetime)
    } else {
        Err("Could not parse datetime from filename".into())
    }
}

/// Request for stats, received from API/SQL query.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataSetStatsRequest {
    /// Name of the stream to get stats for
    pub dataset_name: String,
    /// Included start time for stats query
    pub start_time: String,
    /// Excluded end time for stats query
    pub end_time: String,
    /// Fields list to fetch stats for
    /// If empty, stats for all fields will be returned
    #[serde(default)]
    pub fields: Vec<String>,
    /// Offset for pagination of distinct values (default: 0)
    pub offset: Option<u64>,
    /// Limit for number of distinct values per field (default: 5)
    pub limit: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FieldStats {
    field_count: f64,
    distinct_count: f64,
    distinct_values: IndexMap<String, f64>,
}

#[derive(Serialize, Deserialize)]
pub struct QueryRow {
    field_name: String,
    field_count: f64,
    distinct_count: f64,
    distinct_value: String,
    distinct_value_count: f64,
}

/// API handler to get the field stats for a dataset
/// If `fields` is empty, stats for all fields will be returned
/// If `fields` is provided, stats for those fields will be returned
pub async fn get_dataset_stats(
    req: HttpRequest,
    dataset_stats_request: Json<DataSetStatsRequest>,
) -> Result<impl Responder, QueryError> {
    let offset = dataset_stats_request.offset.unwrap_or(0);
    let limit = dataset_stats_request.limit.unwrap_or(5);
    let tenant_id = get_tenant_id_from_request(&req)?;
    let sql = if dataset_stats_request.fields.is_empty() {
        build_stats_sql(&dataset_stats_request.dataset_name, None, offset, limit)
    } else {
        build_stats_sql(
            &dataset_stats_request.dataset_name,
            Some(&dataset_stats_request.fields),
            offset,
            limit,
        )
    };

    // create query request
    let query_request = Query {
        query: sql,
        start_time: dataset_stats_request.start_time.clone(),
        end_time: dataset_stats_request.end_time.clone(),
        filter_tags: None,
        fields: false,
        streaming: false,
        send_null: false,
    };

    let rows: Vec<QueryRow> = match &PARSEABLE.options.mode {
        Mode::Query | Mode::All => {
            let response = query(req, query_request).await?;
            let body_bytes = response.into_body().try_into_bytes().map_err(|_| {
                QueryError::CustomError("error in converting response to bytes".to_string())
            })?;

            let body_str = std::str::from_utf8(&body_bytes).map_err(|_| {
                QueryError::CustomError("error in converting response bytes to string".to_string())
            })?;
            serde_json::from_str(body_str).map_err(|e| QueryError::CustomError(e.to_string()))?
        }
        Mode::Prism => {
            let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
            let userid = get_user_from_request(&req).map_err(|_| QueryError::Unauthorized)?;
            let auth = if let Some((_, hash)) = CLUSTER_SECRET.get() {
                Some(prism_forwarding_headers(
                    hash,
                    tenant,
                    &userid,
                    TENANT_METADATA.get_global_query_auth(tenant).as_deref(),
                )?)
            } else {
                Some(prism_auth_without_secret(req.headers())?)
            };

            let response = match send_query_request(auth, &query_request, &tenant_id).await {
                Ok((query_response, _)) => query_response,
                Err(err) => {
                    error!("{:?}", err);
                    return Err(err);
                }
            };
            serde_json::from_value(response).map_err(|e| QueryError::CustomError(e.to_string()))?
        }
        _ => {
            return Err(QueryError::CustomError(
                "Dataset Stats not allowed on this server mode".into(),
            ));
        }
    };

    let field_stats = transform_query_results(rows)?;
    let response = HttpResponse::Ok().json(field_stats);

    Ok(response)
}

/// Auth for forwarding a Prism dataset-stats query to a querier without the
/// cluster secret: the caller's `Authorization` header, so the querier checks
/// the caller's own access. Anything else is rejected. A session cookie is
/// only valid on this node, and leaving the auth empty would make
/// `send_query_request` use the querier's own token and skip those checks.
fn prism_auth_without_secret(headers: &HeaderMap) -> Result<HeaderMap, QueryError> {
    let auth = headers
        .get(actix_web::http::header::AUTHORIZATION)
        .ok_or_else(|| {
            QueryError::CustomError(
                "P_CLUSTER_SECRET is required to forward this request to a querier".to_string(),
            )
        })?;
    let mut map = HeaderMap::new();
    map.insert(actix_web::http::header::AUTHORIZATION, auth.clone());
    Ok(map)
}

/// Headers for forwarding a Prism dataset-stats query to a querier with the
/// cluster secret. All values are server-side (stored global query auth,
/// cluster secret hash, the authenticated user id) apart from the tenant,
/// which was already validated from the request header.
fn prism_forwarding_headers(
    hash: &str,
    tenant: &str,
    userid: &str,
    global_query_auth: Option<&str>,
) -> Result<HeaderMap, HeaderBuildError> {
    let mut map = HeaderMap::new();
    if let Some(header) = global_query_auth {
        map.insert(
            HeaderName::from_static("authorization"),
            generated_text_header_value(header, "global query auth")?,
        );
    }
    map.insert(
        HeaderName::from_static(CLUSTER_SECRET_HEADER),
        generated_text_header_value(hash, "cluster secret hash")?,
    );
    map.insert(
        HeaderName::from_static("intra-cluster-tenant"),
        HeaderValue::from_str(tenant)
            .map_err(|_| HeaderBuildError::Input(ParseHeaderError::InvalidTenantId))?,
    );
    map.insert(
        HeaderName::from_static("intra-cluster-userid"),
        generated_text_header_value(userid, "user id")?,
    );
    Ok(map)
}

pub fn transform_query_results(
    rows: Vec<QueryRow>,
) -> Result<HashMap<String, FieldStats>, QueryError> {
    let mut field_map: HashMap<String, FieldStats> = HashMap::new();

    for row in rows {
        let field_stats = field_map
            .entry(row.field_name.clone())
            .or_insert_with(|| FieldStats {
                field_count: row.field_count,
                distinct_count: row.distinct_count,
                distinct_values: IndexMap::new(),
            });

        field_stats
            .distinct_values
            .insert(row.distinct_value, row.distinct_value_count);
    }

    //sort field_stats distinct_values by distinct_value_count in descending order
    for field_stats in field_map.values_mut() {
        field_stats.distinct_values.sort_by(|_k1, v1, _k2, v2| {
            v2.partial_cmp(v1).unwrap_or(std::cmp::Ordering::Equal) // Descending order
        });
    }

    Ok(field_map)
}

/// Builds the SQL query to get field stats for a dataset.
/// If `fields` is `None`, stats for all fields will be returned.
/// If `fields` is `Some`, stats for those fields will be returned.
/// `offset` and `limit` control pagination of distinct values per field.
pub fn build_stats_sql(
    dataset_name: &str,
    fields: Option<&[String]>,
    offset: u64,
    limit: u64,
) -> String {
    let fields_filter = if let Some(fields) = fields {
        if !fields.is_empty() {
            let quoted_fields: Vec<String> = fields
                .iter()
                .map(|f| format!("'{}'", f.replace('\'', "''")))
                .collect();
            format!(
                "AND  field_stats_field_name IN ({})",
                quoted_fields.join(", ")
            )
        } else {
            String::default()
        }
    } else {
        String::default()
    };
    let dataset_name = dataset_name.replace('"', "\"\"");

    // Calculate the row number range based on offset and limit
    let rn_start = offset;
    let rn_end = offset + limit;

    format!(
        "WITH
  ranked_values AS (
    SELECT
      field_stats_field_name AS field_name,
      field_stats_distinct_stats_distinct_value AS distinct_value,
      SUM(field_stats_distinct_stats_count) AS distinct_value_count,
      ROW_NUMBER() OVER (
        PARTITION BY
          field_stats_field_name
        ORDER BY
          SUM(field_stats_distinct_stats_count) DESC,
          field_stats_distinct_stats_distinct_value ASC
      ) AS rn
    FROM
      {DATASET_STATS_STREAM_NAME}
    WHERE
      dataset_name = '{dataset_name}'
      AND field_stats_distinct_stats_distinct_value IS NOT NULL
      {fields_filter}
    GROUP BY
      field_stats_field_name,
      field_stats_distinct_stats_distinct_value
  ),
  top_values AS (
    SELECT
      *
    FROM
      ranked_values
    WHERE
      rn > {rn_start}
      AND rn <= {rn_end}
  ),
  field_distincts AS (
    SELECT
      field_name,
      COUNT(*) AS total_distinct_count
    FROM
      ranked_values
    GROUP BY
      field_name
  ),
  field_totals AS (
    SELECT
      field_stats_field_name,
      SUM(field_count) AS total_field_count
    FROM
      (
        SELECT
          field_stats_field_name,
          stats_id,
          p_timestamp,
          MAX(field_stats_count) AS field_count
        FROM
          {DATASET_STATS_STREAM_NAME}
        WHERE
          dataset_name = '{dataset_name}'
          {fields_filter}
        GROUP BY
          field_stats_field_name,
          stats_id,
          p_timestamp
      ) field_file_totals
    GROUP BY
      field_stats_field_name
  )
SELECT
  tv.field_name,
  ft.total_field_count AS field_count,
  fd.total_distinct_count AS distinct_count,
  tv.distinct_value,
  tv.distinct_value_count
FROM
  top_values tv
  JOIN field_totals ft ON tv.field_name = ft.field_stats_field_name
  JOIN field_distincts fd ON tv.field_name = fd.field_name
ORDER BY
  tv.field_name,
  tv.distinct_value_count DESC"
    )
}
#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs::OpenOptions, sync::Arc};

    use arrow::buffer::OffsetBuffer;
    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
        StringViewArray, TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use serde_json::json;
    use temp_dir::TempDir;

    use crate::handlers::http::modal::utils::ingest_utils::json_byte_size;
    use crate::storage::field_stats::{
        DATASET_STATS_CUSTOM_PARTITION, DatasetStats, DistinctStat, FieldCountState, FieldStat,
        NULL_VALUE, build_stats_sql, collect_all_field_stats_from_parquet_blocking,
        collect_field_counts, for_each_stats_row,
    };
    use crate::utils::json::apply_generic_flattening_for_partition;
    use crate::utils::json::flatten::{FlattenBudget, FlattenLimits, GENEROUS_LIMITS, legacy};

    #[test]
    fn differential_all_types_and_batches() {
        use super::*;
        use arrow_array::*;
        let n = FIELD_STATS_BATCH_ROWS * 2 + 137;
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();
        macro_rules! primitive {
            ($ty:ty, $value:expr) => {
                columns.push(Arc::new(<$ty>::from_iter(
                    (0..n).map(|i| if i % 31 == 0 { None } else { Some(($value)(i)) }),
                )));
            };
        }
        primitive!(Int8Array, |i| (i % 17) as i8 - 8);
        primitive!(Int16Array, |i| (i % 17) as i16 - 8);
        primitive!(Int32Array, |i| (i % 17) as i32 - 8);
        primitive!(Int64Array, |i| (i % 17) as i64 - 8);
        primitive!(UInt8Array, |i| (i % 17) as u8);
        primitive!(UInt16Array, |i| (i % 17) as u16);
        primitive!(UInt32Array, |i| (i % 17) as u32);
        primitive!(UInt64Array, |i| (i % 17) as u64);
        primitive!(Float64Array, |i| [
            0.,
            -0.,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.234
        ][i % 6]);
        primitive!(Float32Array, |i| [
            0.,
            -0.,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            1.234
        ][i % 6]);
        primitive!(TimestampMillisecondArray, |i| [
            i64::MIN,
            -1,
            0,
            1730000000123,
            i64::MAX
        ][i % 5]);
        primitive!(Date32Array, |i| (i % 17) as i32 - 8);
        primitive!(BooleanArray, |i| i % 2 == 0);
        let strings: Vec<_> = (0..n)
            .map(|i| [Some("NULL"), None, Some(""), Some("é\n")][i % 4])
            .collect();
        columns.push(Arc::new(StringArray::from(strings.clone())));
        columns.push(Arc::new(StringViewArray::from(strings)));
        let bytes: Vec<_> = (0..n)
            .map(|i| {
                [
                    Some(&b"abc"[..]),
                    None,
                    Some(&b"\xff"[..]),
                    Some(&b"\xfe"[..]),
                ][i % 4]
            })
            .collect();
        columns.push(Arc::new(BinaryArray::from(bytes.clone())));
        columns.push(Arc::new(BinaryViewArray::from(bytes)));
        columns.push(Arc::new(NullArray::new(n)));
        columns.push(Arc::new(ListArray::from_iter_primitive::<
            arrow_array::types::Int64Type,
            _,
            _,
        >((0..n).map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some(vec![Some((i % 7) as i64), None])
            }
        }))));
        // Above the cap: repeated high-cardinality app-like fields with known truth.
        primitive!(Int64Array, |i| (i % 3000) as i64);
        primitive!(Float64Array, |i| (i % 3000) as f64 / 7.);
        columns.push(Arc::new(StringArray::from_iter_values(
            (0..n).map(|i| format!("trace-{:08}", i % 3000)),
        )));
        let fields: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("f{i}"), a.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        // Memoization alone must preserve counts, candidate eviction and every HLL register.
        for field in schema.fields() {
            let (_, old) = legacy_collect_field_counts("test", &batch, field.name(), 50).unwrap();
            let (_, new) = collect_field_counts("test", &batch, field.name(), 50).unwrap();
            assert_eq!(old.counts, new.counts);
            assert_eq!(old.hll.registers, new.hll.registers);
        }
        for offset in (0..n).step_by(1024) {
            let chunk = batch.slice(offset, (n - offset).min(1024));
            for field in schema.fields() {
                let (_, old) =
                    legacy_collect_field_counts("test", &chunk, field.name(), 50).unwrap();
                let (_, new) = collect_field_counts("test", &chunk, field.name(), 50).unwrap();
                assert_eq!(old.counts, new.counts);
                assert_eq!(old.hll.registers, new.hll.registers);
            }
        }
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("types.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(19000))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let old = legacy_collect_all_field_stats_from_parquet_blocking("test", &path, &schema, 50)
            .unwrap();
        let new =
            collect_all_field_stats_from_parquet_blocking("test", &path, &schema, 50).unwrap();
        for (index, (a, b)) in old.iter().zip(&new).enumerate() {
            assert_eq!(a.count, b.count);
            assert_eq!(a.distinct_count, b.distinct_count, "HLL {index}");
            assert_eq!(
                serde_json::to_value(a).unwrap(),
                serde_json::to_value(b).unwrap(),
                "field {index}"
            );
            if index >= 19 {
                let mut errors = Vec::new();
                for stats in [a, b] {
                    let mut error = 0;
                    for value in &stats.distinct_stats {
                        let truth = (0..n)
                            .filter(|&i| {
                                format_arrow_value(batch.column(index).as_ref(), i)
                                    == value.distinct_value
                            })
                            .count() as i64;
                        assert!(value.count <= truth);
                        error += truth - value.count;
                    }
                    errors.push(error);
                }
                // Keeping the original merge boundaries preserves the same approximation.
                assert_eq!(errors[0], errors[1]);
                println!(
                    "APPROX field={index} old/new retained undercount={errors:?}, distinct={}",
                    b.distinct_count
                );
            }
        }
        let missing = dir.path().join("missing");
        assert_eq!(
            legacy_collect_all_field_stats_from_parquet_blocking("t", &missing, &schema, 50)
                .unwrap_err()
                .to_string(),
            collect_all_field_stats_from_parquet_blocking("t", &missing, &schema, 50)
                .unwrap_err()
                .to_string()
        );
    }

    /// Strict differential harness for the very Parquet files used by the server benchmark.
    #[test]
    #[ignore]
    fn differential_benchmark_parquet() {
        use super::*;
        let path = std::env::var("FIELD_STATS_PARQUET").expect("FIELD_STATS_PARQUET path");
        let path = Path::new(&path);
        let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .schema()
            .clone();
        let old =
            legacy_collect_all_field_stats_from_parquet_blocking("benchlogs", path, &schema, 50)
                .unwrap();
        let new =
            collect_all_field_stats_from_parquet_blocking("benchlogs", path, &schema, 50).unwrap();
        assert_eq!(
            serde_json::to_value(&old).unwrap(),
            serde_json::to_value(&new).unwrap()
        );
        // Ground truth for the reported top values, computed without cardinality caps.
        let mut truth: HashMap<String, HashMap<String, i64>> = HashMap::new();
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .with_batch_size(32768)
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                let counts = truth.entry(field.name().clone()).or_default();
                for i in 0..array.len() {
                    *counts
                        .entry(format_arrow_value(array.as_ref(), i))
                        .or_default() += 1;
                }
            }
        }
        for field in &new {
            let counts = &truth[&field.field_name];
            let error: i64 = field
                .distinct_stats
                .iter()
                .map(|v| counts[&v.distinct_value] - v.count)
                .sum();
            assert!(
                field
                    .distinct_stats
                    .iter()
                    .all(|v| counts[&v.distinct_value] >= v.count)
            );
            println!(
                "GROUND_TRUTH field={} distinct_estimate={} truth={} reported_count_underestimate={error}",
                field.field_name,
                field.distinct_count,
                counts.len()
            );
        }
    }

    #[test]
    fn batched_event_matches_per_row_events() {
        use super::*;
        let ts = DateTime::from_timestamp_millis(1730000000000).unwrap();
        for stats in [uniform_stats(&[4, 2, 3]), uniform_stats(&[0, 2, 0])] {
            let custom = HashMap::from([(USER_AGENT_KEY.to_string(), "parseable".to_string())]);
            // The events for `stats`, grouping rows up to `max_event_bytes`, and how many
            // times the schema was read for them
            let events = |max_row_bytes, max_event_bytes, schema: &HashMap<_, _>| {
                let mut events = Vec::new();
                let reads = std::cell::Cell::new(0);
                for_each_stats_event(
                    &stats,
                    max_row_bytes,
                    max_event_bytes,
                    ts,
                    || {
                        reads.set(reads.get() + 1);
                        schema.clone()
                    },
                    &custom,
                    &None,
                    |event| {
                        events.push(event);
                        Ok(())
                    },
                )
                .unwrap();
                (events, reads.get())
            };
            let (mut whole, reads) = events(usize::MAX, u64::MAX, &HashMap::new());
            assert_eq!((whole.len(), reads), (1, 1));
            let event = whole.pop().unwrap();
            assert!(event.is_first_event); // one process call => at most one schema write
            let mut total = 0;
            for_each_stats_row(&stats, usize::MAX, |row, size| {
                let old = json::Event {
                    json: row,
                    p_timestamp: ts,
                }
                .into_event(
                    DATASET_STATS_STREAM_NAME.to_string(),
                    size,
                    &HashMap::new(),
                    false,
                    Some(&DATASET_STATS_CUSTOM_PARTITION.to_string()),
                    None,
                    SchemaVersion::V1,
                    StreamType::Internal,
                    &custom,
                    TelemetryType::Logs,
                    &None,
                    false,
                )?;
                // Union schema: absent null-only columns are equivalent to nulls in the batch.
                for (i, field) in old.rb.schema().fields().iter().enumerate() {
                    let new = event
                        .rb
                        .column_by_name(field.name())
                        .unwrap()
                        .slice(total, 1);
                    if old.rb.column(i).data_type() == &DataType::Null {
                        assert!(new.is_null(0));
                    } else {
                        assert_eq!(old.rb.column(i).to_data(), new.to_data());
                    }
                }
                assert_eq!(old.custom_partition_values, event.custom_partition_values);
                assert_eq!(old.parsed_timestamp, event.parsed_timestamp);
                total += 1;
                Ok(())
            })
            .unwrap();
            assert_eq!(total, event.rb.num_rows());
            let schema = event
                .rb
                .schema()
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.clone()))
                .collect();
            let known = events(usize::MAX, u64::MAX, &schema).0.pop().unwrap();
            assert_eq!(known.rb, event.rb);
            assert!(events(1, u64::MAX, &schema).0.is_empty());

            // A byte bound splits the rows into several events, in order and without loss,
            // and the schema is read again for each of them.
            let mut sizes = Vec::new();
            for_each_stats_row(&stats, usize::MAX, |_, size| {
                sizes.push(size);
                Ok(())
            })
            .unwrap();
            assert!(total >= 3, "{total}");
            for (bound, single_rows) in [(1, true), (sizes[0] + sizes[1], false)] {
                let (parts, reads) = events(usize::MAX, bound, &schema);
                assert_eq!(reads, parts.len());
                if single_rows {
                    assert_eq!(parts.len(), total);
                } else {
                    assert_eq!(parts[0].rb.num_rows(), 2);
                    assert!(parts.len() > 1 && parts.len() < total, "{}", parts.len());
                }
                let mut offset = 0;
                for part in &parts {
                    let rows = part.rb.num_rows();
                    for (i, field) in part.rb.schema().fields().iter().enumerate() {
                        let expected = known
                            .rb
                            .column_by_name(field.name())
                            .unwrap()
                            .slice(offset, rows);
                        if part.rb.column(i).data_type() == &DataType::Null {
                            assert_eq!(expected.logical_null_count(), rows);
                        } else {
                            assert_eq!(part.rb.column(i).to_data(), expected.to_data());
                        }
                    }
                    assert_eq!(part.custom_partition_values, known.custom_partition_values);
                    assert_eq!(part.parsed_timestamp, known.parsed_timestamp);
                    assert_eq!(
                        part.origin_size,
                        sizes[offset..offset + rows].iter().sum::<u64>()
                    );
                    offset += rows;
                }
                assert_eq!(offset, total);
            }
        }
    }

    async fn create_test_parquet_with_data() -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_data.parquet");
        let schema = Arc::new(create_test_schema());

        // Create test data with various patterns
        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let name_array = StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            Some("Alice"),
            Some("Charlie"),
            Some("Alice"),
            Some("Bob"),
            Some("David"),
            None,
            Some("Eve"),
            Some("Frank"),
        ]);
        let score_array = Float64Array::from(vec![
            Some(95.5),
            Some(87.2),
            Some(95.5),
            Some(78.9),
            Some(92.1),
            Some(88.8),
            Some(91.0),
            None,
            Some(89.5),
            Some(94.2),
        ]);
        let active_array = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            None, // null value
            Some(false),
            Some(true),
        ]);
        let timestamp_array = TimestampMillisecondArray::from(vec![
            Some(1640995200000),
            Some(1640995260000),
            Some(1640995200000),
            Some(1640995320000),
            Some(1640995380000),
            Some(1640995440000),
            Some(1640995500000),
            None,
            Some(1640995560000),
            Some(1640995620000),
        ]);
        // Field with single value (all same)
        let single_value_array = StringArray::from(vec![
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
            Some("constant"),
        ]);

        // Create List<Int64> field
        let int_list_data = Int64Array::from(vec![
            1, 2, 3, 4, 5, 1, 2, 3, 6, 7, 8, 9, 1, 4, 5, 10, 11, 12, 13, 14, 1, 2, 12, 13, 14, 1, 2,
        ]);
        let int_list_offsets =
            OffsetBuffer::new(vec![0, 3, 5, 8, 12, 13, 15, 17, 17, 20, 22].into());
        let int_list_field = Arc::new(Field::new("item", DataType::Int64, false));
        let int_list_array = ListArray::new(
            int_list_field,
            int_list_offsets,
            Arc::new(int_list_data),
            None,
        );

        // Create List<Float64> field
        let float_list_data = Float64Array::from(vec![
            1.1, 2.2, 3.3, 4.4, 5.5, 1.1, 2.2, 6.6, 7.7, 8.8, 9.9, 3.3, 4.4, 5.5, 10.0, 11.1, 12.2,
            13.3,
        ]);
        let float_list_offsets =
            OffsetBuffer::new(vec![0, 2, 5, 7, 8, 11, 14, 15, 15, 17, 18].into());
        let float_list_field = Arc::new(Field::new("item", DataType::Float64, false));
        let float_list_array = ListArray::new(
            float_list_field,
            float_list_offsets,
            Arc::new(float_list_data),
            None,
        );

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(score_array),
                Arc::new(active_array),
                Arc::new(timestamp_array),
                Arc::new(single_value_array),
                Arc::new(int_list_array),
                Arc::new(float_list_array),
            ],
        )
        .unwrap();
        let props = WriterProperties::new();
        let mut parquet_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path.clone())
            .unwrap();
        let mut writer =
            ArrowWriter::try_new(&mut parquet_file, schema.clone(), Some(props.clone())).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        (temp_dir, file_path)
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("single_value", DataType::Utf8, true),
            Field::new_list(
                "int_list",
                Arc::new(Field::new("item", DataType::Int64, false)),
                true,
            ),
            Field::new_list(
                "float_list",
                Arc::new(Field::new("item", DataType::Float64, false)),
                true,
            ),
        ])
    }

    #[test]
    fn test_build_stats_sql_uses_file_ids_and_deduped_distinct_values() {
        let sql = build_stats_sql(
            "astronomy-shop-logs",
            Some(&["p_log_category".to_string()]),
            0,
            5,
        );

        assert!(sql.contains("COUNT(*) AS total_distinct_count"));
        assert!(sql.contains("stats_id"));
        assert!(sql.contains("MAX(field_stats_count) AS field_count"));
        assert!(sql.contains("SUM(field_count) AS total_field_count"));
        assert!(sql.contains("AND field_stats_distinct_stats_distinct_value IS NOT NULL"));
        assert!(sql.contains("field_stats_field_name IN ('p_log_category')"));
        assert!(sql.contains("fd.total_distinct_count AS distinct_count"));
        assert!(!sql.contains("SUM(field_stats_distinct_stats_count) AS total_field_count"));
        assert!(!sql.contains("SUM(field_stats_distinct_count)"));
    }

    #[test]
    fn test_collect_all_field_stats_from_parquet_single_pass() {
        let (_temp_dir, parquet_path) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(create_test_parquet_with_data());
        let schema = create_test_schema();

        let field_stats = collect_all_field_stats_from_parquet_blocking(
            "test_stream",
            &parquet_path,
            &schema,
            50,
        )
        .unwrap();
        let name_stats = field_stats
            .iter()
            .find(|stats| stats.field_name == "name")
            .unwrap();
        assert_eq!(name_stats.count, 10);
        assert_eq!(name_stats.distinct_count, 7);
        assert_eq!(name_stats.distinct_stats[0].distinct_value, "Alice");
        assert_eq!(name_stats.distinct_stats[0].count, 3);
        assert!(
            name_stats
                .distinct_stats
                .iter()
                .any(|stat| stat.distinct_value == "NULL" && stat.count == 1)
        );

        let active_stats = field_stats
            .iter()
            .find(|stats| stats.field_name == "active")
            .unwrap();
        assert_eq!(active_stats.count, 10);
        assert_eq!(active_stats.distinct_count, 3);
        assert_eq!(active_stats.distinct_stats[0].distinct_value, "true");
        assert_eq!(active_stats.distinct_stats[0].count, 6);

        let int_list_stats = field_stats
            .iter()
            .find(|stats| stats.field_name == "int_list")
            .unwrap();
        assert_eq!(int_list_stats.count, 10);
        assert_eq!(int_list_stats.distinct_count, 8);
        assert!(
            int_list_stats
                .distinct_stats
                .iter()
                .any(|stat| stat.distinct_value == "[]" && stat.count == 1)
        );
    }

    #[test]
    fn test_field_count_state_caps_high_cardinality_values() {
        let mut state =
            FieldCountState::new("test_stream".to_string(), "request_id".to_string(), 2);
        state.max_tracked_values = 8;

        for index in 0..100 {
            let mut counts = HashMap::new();
            counts.insert(format!("uuid-{index}"), 1);
            state.merge_counts(counts);
        }

        assert!(state.approximate);
        assert_eq!(state.total_count, 100);
        assert_eq!(state.counts.len(), 8);
        assert!(state.distinct_count() > 80);
        assert_eq!(state.into_distinct_stats(50).len(), 8);
    }

    #[test]
    fn test_field_count_state_only_evicts_for_stronger_candidate() {
        let mut state = FieldCountState::new("test_stream".to_string(), "status".to_string(), 3);
        state.max_tracked_values = 3;

        let mut initial_counts = HashMap::new();
        initial_counts.insert("A".to_string(), 100);
        initial_counts.insert("B".to_string(), 50);
        initial_counts.insert("C".to_string(), 30);
        state.merge_counts(initial_counts);

        let mut weak_candidate = HashMap::new();
        weak_candidate.insert("D".to_string(), 1);
        state.merge_counts(weak_candidate);

        assert!(state.counts.contains_key("C"));
        assert!(!state.counts.contains_key("D"));

        let mut strong_candidate = HashMap::new();
        strong_candidate.insert("E".to_string(), 31);
        state.merge_counts(strong_candidate);

        assert!(!state.counts.contains_key("C"));
        assert!(state.counts.contains_key("E"));
    }

    /// `track_value` skips the linear minimum scan when a candidate cannot beat `min_count_hint`.
    /// The hint is only a lower bound, so it must never reject a candidate that would really evict:
    /// here the tracked minimum climbs above the hint before the candidates arrive.
    #[test]
    fn test_field_count_state_eviction_survives_stale_min_hint() {
        let mut state = FieldCountState::new("test_stream".to_string(), "status".to_string(), 3);
        state.max_tracked_values = 3;

        for (value, count) in [("A", 1), ("B", 2), ("C", 3)] {
            state.merge_counts(HashMap::from([(value.to_string(), count)]));
        }
        // Grow the smallest tracked value so the real minimum (B = 2) exceeds the hint (1).
        state.merge_counts(HashMap::from([("A".to_string(), 9)]));
        assert_eq!(state.counts.get("A"), Some(&10));

        // Ties with the real minimum must not evict.
        state.merge_counts(HashMap::from([("D".to_string(), 2)]));
        assert!(!state.counts.contains_key("D"));
        assert!(state.counts.contains_key("B"));

        // A candidate above the real minimum must still evict, despite the stale hint.
        state.merge_counts(HashMap::from([("E".to_string(), 3)]));
        assert!(!state.counts.contains_key("B"));
        assert_eq!(state.counts.get("E"), Some(&3));
        assert_eq!(state.counts.len(), 3);
    }

    /// The `Utf8`/`Utf8View` fast paths in `collect_field_counts` bypass `format_arrow_value`, so
    /// they must agree with it on both real values and nulls.
    #[test]
    fn test_collect_field_counts_string_fast_path_matches_generic_path() {
        let values = vec![Some("alpha"), Some("beta"), None, Some("alpha")];
        let utf8: Arc<dyn Array> = Arc::new(StringArray::from(values.clone()));
        let utf8_view: Arc<dyn Array> = Arc::new(StringViewArray::from(values));

        for (name, array) in [("utf8", utf8), ("utf8_view", utf8_view)] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                name,
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![array]).expect("valid batch");
            let (_, state) =
                collect_field_counts("test_stream", &batch, name, 10).expect("column exists");

            assert_eq!(state.total_count, 4, "{name}");
            assert_eq!(state.counts.get("alpha"), Some(&2), "{name}");
            assert_eq!(state.counts.get("beta"), Some(&1), "{name}");
            assert_eq!(state.counts.get(NULL_VALUE), Some(&1), "{name}");
        }
    }

    #[test]
    #[ignore]
    fn bench_field_stats_high_cardinality() {
        use std::time::Instant;

        const ROWS: usize = 200_000;
        const HIGH_CARD_FIELDS: usize = 10;
        const LOW_CARD_FIELDS: usize = 10;

        let mut fields = Vec::new();
        for i in 0..HIGH_CARD_FIELDS {
            fields.push(Field::new(format!("trace_id_{i}"), DataType::Utf8, true));
        }
        for i in 0..LOW_CARD_FIELDS {
            fields.push(Field::new(format!("severity_{i}"), DataType::Utf8, true));
        }
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<Arc<dyn Array>> = Vec::new();
        for f in 0..HIGH_CARD_FIELDS {
            let vals: Vec<String> = (0..ROWS)
                .map(|r| format!("7f3a9c2e1b8d4f6a{f:02}{r:010}"))
                .collect();
            columns.push(Arc::new(StringArray::from(vals)));
        }
        for f in 0..LOW_CARD_FIELDS {
            let vals: Vec<String> = (0..ROWS).map(|r| format!("sev-{f}-{}", r % 8)).collect();
            columns.push(Arc::new(StringArray::from(vals)));
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).expect("valid batch");
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bench.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100_000))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        for rows in [1024, 8192, 32768, 65536] {
            let started = Instant::now();
            for _ in 0..3 {
                let stats =
                    super::collect_field_stats_with_batch_size("bench", &path, &schema, 50, rows)
                        .unwrap();
                assert_eq!(stats.len(), HIGH_CARD_FIELDS + LOW_CARD_FIELDS);
            }
            println!(
                "BATCH_BENCH rows={ROWS} fields={} batch={rows} mean_ms={:.3}",
                HIGH_CARD_FIELDS + LOW_CARD_FIELDS,
                started.elapsed().as_secs_f64() * 1000. / 3.
            );
        }
        let started = Instant::now();
        for _ in 0..3 {
            super::legacy_collect_all_field_stats_from_parquet_blocking(
                "bench", &path, &schema, 50,
            )
            .unwrap();
        }
        println!(
            "BATCH_BENCH legacy_mean_ms={:.3}",
            started.elapsed().as_secs_f64() * 1000. / 3.
        );
    }

    fn field_stat(name: &str, distinct_values: &[&str]) -> FieldStat {
        FieldStat {
            field_name: name.to_owned(),
            count: 10,
            distinct_count: distinct_values.len() as i64,
            distinct_stats: distinct_values
                .iter()
                .enumerate()
                .map(|(i, value)| DistinctStat {
                    distinct_value: (*value).to_owned(),
                    count: (distinct_values.len() - i) as i64,
                })
                .collect(),
        }
    }

    fn dataset_stats(field_stats: Vec<FieldStat>) -> DatasetStats {
        DatasetStats {
            dataset_name: "dataset".to_owned(),
            stats_id: "stats".to_owned(),
            field_stats,
        }
    }

    // `field{i}` with the distinct values `value0`, `value1`, ... up to `distinct_per_field[i]`
    fn uniform_stats(distinct_per_field: &[usize]) -> DatasetStats {
        dataset_stats(
            distinct_per_field
                .iter()
                .enumerate()
                .map(|(field, &distinct)| FieldStat {
                    field_name: format!("field{field}"),
                    count: 10,
                    distinct_count: distinct as i64,
                    distinct_stats: (0..distinct)
                        .map(|value| DistinctStat {
                            distinct_value: format!("value{value}"),
                            count: 1,
                        })
                        .collect(),
                })
                .collect(),
        )
    }

    // The rows `for_each_stats_row` hands over, with their sizes, and how many it skipped
    fn streamed_rows(
        stats: &DatasetStats,
        max_row_bytes: usize,
    ) -> (Vec<(serde_json::Value, u64)>, usize) {
        let mut rows = Vec::new();
        let skipped = for_each_stats_row(stats, max_row_bytes, |row, size| {
            rows.push((row, size));
            Ok(())
        })
        .unwrap();
        (rows, skipped)
    }

    // The rows the stats used to be ingested as: the stats as JSON, generically and then
    // structurally flattened by the oracle copies of those functions
    fn legacy_rows(stats: &DatasetStats) -> Vec<serde_json::Value> {
        let partition = DATASET_STATS_CUSTOM_PARTITION.to_string();
        legacy::generic_flattening(&serde_json::to_value(stats).unwrap())
            .unwrap()
            .into_iter()
            .map(|mut row| {
                legacy::flatten(&mut row, "_", Some(&partition), true).unwrap();
                row
            })
            .collect()
    }

    fn flatten_stats_payload(
        stats: &DatasetStats,
        limits: FlattenLimits,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        apply_generic_flattening_for_partition(
            serde_json::to_value(stats).unwrap(),
            None,
            None,
            Some(&DATASET_STATS_CUSTOM_PARTITION.to_string()),
            &mut FlattenBudget::new(limits),
        )
    }

    #[test]
    fn stats_rows_are_the_flattened_stats() {
        let cases = [
            dataset_stats(vec![field_stat("a", &["x", "y"])]),
            // A field without distinct values gets a row with a null `distinct_stats`
            dataset_stats(vec![
                field_stat("empty", &[]),
                field_stat("b", &["1"]),
                field_stat("also_empty", &[]),
            ]),
            dataset_stats(vec![field_stat(
                "esc\"aped\n",
                &["quo\"te", "back\\slash", "\u{1}ctl", "ünï", ""],
            )]),
            uniform_stats(&[3, 0, 2, 1]),
        ];
        for stats in cases {
            let (rows, skipped) =
                streamed_rows(&stats, FlattenLimits::DEFAULT.max_bytes_per_request);
            assert_eq!(skipped, 0);
            for (row, size) in &rows {
                assert_eq!(*size, json_byte_size(row));
            }
            let rows: Vec<_> = rows.into_iter().map(|(row, _)| row).collect();
            // Serialized, so that the order of the keys is compared too
            let expected = serde_json::to_string(&legacy_rows(&stats)).unwrap();
            assert_eq!(serde_json::to_string(&rows).unwrap(), expected);
            // The same as the stats flattened as one payload, which they were before
            let flattened = flatten_stats_payload(&stats, GENEROUS_LIMITS).unwrap();
            assert_eq!(serde_json::to_string(&flattened).unwrap(), expected);
        }

        let (rows, _) = streamed_rows(
            &dataset_stats(vec![field_stat("empty", &[]), field_stat("b", &["v"])]),
            FlattenLimits::DEFAULT.max_bytes_per_request,
        );
        let rows: Vec<_> = rows
            .iter()
            .map(|(row, _)| serde_json::to_string(row).unwrap())
            .collect();
        assert_eq!(
            rows,
            [
                r#"{"dataset_name":"dataset","stats_id":"stats","field_stats_field_name":"empty","field_stats_count":10,"field_stats_distinct_count":0,"field_stats_distinct_stats":null}"#,
                r#"{"dataset_name":"dataset","stats_id":"stats","field_stats_field_name":"b","field_stats_count":10,"field_stats_distinct_count":1,"field_stats_distinct_stats_distinct_value":"v","field_stats_distinct_stats_count":1}"#,
            ]
        );
    }

    #[test]
    fn stats_rows_keep_the_custom_partition_validation() {
        let mut stats = dataset_stats(vec![field_stat("a", &["x"])]);
        stats.dataset_name = "with.period".to_owned();
        let result = for_each_stats_row(
            &stats,
            FlattenLimits::DEFAULT.max_bytes_per_request,
            |_, _| unreachable!("a row with an invalid custom partition is not processed"),
        );
        assert!(result.is_err());
        assert!(flatten_stats_payload(&stats, GENEROUS_LIMITS).is_err());
    }

    #[test]
    fn stats_rows_over_one_byte_limit_are_all_processed() {
        // 1000 rows of about 225 bytes each, 225 KB together, against a limit of 512 bytes
        let stats = uniform_stats(&[25; 40]);
        let max_row_bytes = 512;
        let (rows, skipped) = streamed_rows(&stats, max_row_bytes);
        assert_eq!((rows.len(), skipped), (1000, 0));
        assert!(rows.iter().all(|(_, size)| *size <= max_row_bytes as u64));
        let total: u64 = rows.iter().map(|(_, size)| size).sum();
        assert!(total > 400 * max_row_bytes as u64, "{total}");
        assert_eq!(
            serde_json::to_string(&rows.into_iter().map(|(row, _)| row).collect::<Vec<_>>())
                .unwrap(),
            serde_json::to_string(&legacy_rows(&stats)).unwrap()
        );
        // Flattened as one payload under the same limit, nothing was ingested
        let limits = FlattenLimits {
            max_rows_per_event: 1000,
            max_rows_per_request: 1000,
            max_bytes_per_request: max_row_bytes,
        };
        assert!(flatten_stats_payload(&stats, limits).is_err());
    }

    #[test]
    fn stats_rows_skip_only_a_row_over_the_byte_limit() {
        let long_value = "v".repeat(600);
        let long_name = "f".repeat(600);
        let stats = dataset_stats(vec![
            field_stat("a", &["x", long_value.as_str(), "y"]),
            field_stat(&long_name, &["z"]),
            field_stat("b", &[]),
        ]);
        let (rows, skipped) = streamed_rows(&stats, 512);
        assert_eq!(skipped, 2);
        let kept: Vec<_> = rows
            .iter()
            .map(|(row, _)| {
                (
                    row["field_stats_field_name"].clone(),
                    row.get("field_stats_distinct_stats_distinct_value")
                        .cloned(),
                    row["field_stats_distinct_count"].clone(),
                )
            })
            .collect();
        assert_eq!(
            kept,
            [
                (json!("a"), Some(json!("x")), json!(3)),
                (json!("a"), Some(json!("y")), json!(3)),
                (json!("b"), None, json!(0)),
            ]
        );
        // The rows kept are those the stats were flattened into
        let legacy: Vec<_> = legacy_rows(&stats)
            .into_iter()
            .filter(|row| json_byte_size(row) <= 512)
            .collect();
        assert_eq!(
            serde_json::to_string(&rows.into_iter().map(|(row, _)| row).collect::<Vec<_>>())
                .unwrap(),
            serde_json::to_string(&legacy).unwrap()
        );
    }

    #[test]
    fn stats_rows_of_the_widest_payload_fit_the_default_byte_limit() {
        // Far more rows than the limits for ingested events allow, each processed on its own
        let fields = 300;
        let max_field_statistics = 50;
        let stats = uniform_stats(&[max_field_statistics; 300]);
        let mut rows = 0;
        let mut first = None;
        let skipped = for_each_stats_row(
            &stats,
            FlattenLimits::DEFAULT.max_bytes_per_request,
            |row, _| {
                if first.is_none() {
                    first = Some(row);
                }
                rows += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!((rows, skipped), (fields * max_field_statistics, 0));
        assert_eq!(
            first.unwrap()["field_stats_distinct_stats_distinct_value"],
            "value0"
        );
    }

    #[test]
    fn compute_field_stats_from_batches_matches_parquet_decode() {
        use super::{compute_field_stats_from_batches, extract_datetime_from_parquet_path_regex};

        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
            Field::new("latency", DataType::Float64, true),
            Field::new("ok", DataType::Boolean, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("trace_id", DataType::Utf8, true),
        ]));

        let make_batch = |start: usize, len: usize| -> RecordBatch {
            let levels = StringArray::from_iter(
                (start..start + len)
                    .map(|i| [Some("info"), Some("warn"), Some("error"), None][i % 4]),
            );
            let statuses = Int64Array::from_iter((start..start + len).map(|i| {
                if i % 11 == 0 {
                    None
                } else {
                    Some([200, 201, 404, 500][i % 4])
                }
            }));
            let latencies = Float64Array::from_iter((start..start + len).map(|i| {
                if i % 7 == 0 {
                    None
                } else {
                    Some((i % 13) as f64 * 0.5)
                }
            }));
            let oks = BooleanArray::from_iter(
                (start..start + len).map(|i| if i % 9 == 0 { None } else { Some(i % 2 == 0) }),
            );
            let ts = TimestampMillisecondArray::from_iter(
                (start..start + len).map(|i| Some(1_730_000_000_000 + (i % 5) as i64 * 1000)),
            );
            let trace_ids = StringArray::from_iter_values(
                (start..start + len).map(|i| format!("trace-{:05}", i % 1500)),
            );

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(levels),
                    Arc::new(statuses),
                    Arc::new(latencies),
                    Arc::new(oks),
                    Arc::new(ts),
                    Arc::new(trace_ids),
                ],
            )
            .unwrap()
        };

        let batches = vec![make_batch(0, 2500), make_batch(2500, 1750)];
        let dir = TempDir::new().unwrap();
        let path = dir
            .path()
            .join("date=2026-11-28.hour=09.minute=15.host.data.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2500))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();

        let from_batches = compute_field_stats_from_batches(&batches, "logs", &schema, 50);
        let from_parquet =
            collect_all_field_stats_from_parquet_blocking("logs", &path, &schema, 50).unwrap();

        assert_eq!(from_batches, from_parquet);
        assert_eq!(
            serde_json::to_value(&from_batches).unwrap(),
            serde_json::to_value(&from_parquet).unwrap()
        );

        // Exercise cached regex on the same filename.
        let dt = extract_datetime_from_parquet_path_regex(&path).unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-11-28T09:15:00+00:00");
    }

    #[tokio::test]
    async fn staged_field_stats_cache_handoff_and_schema_filtering() {
        use super::{
            cache_staged_field_stats, compute_field_stats_from_batches, resolve_field_stats,
        };

        let full_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            full_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("x")])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(1)])),
            ],
        )
        .unwrap();
        let expected = compute_field_stats_from_batches(&[batch], "logs", &full_schema, 50);

        // Non-existent path proves `resolve_field_stats` serves purely from memory on cache hit.
        let virtual_path = std::path::PathBuf::from(format!(
            "/nonexistent/date=2026-11-28.hour=10.minute=05.{}.parquet",
            uuid::Uuid::new_v4()
        ));
        cache_staged_field_stats(&virtual_path, 50, expected.clone());

        let reordered_schema = Schema::new(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("missing_in_file", DataType::Utf8, true),
            Field::new("a", DataType::Utf8, true),
        ]);
        let resolved = resolve_field_stats("logs", &virtual_path, &reordered_schema, 50)
            .await
            .unwrap();
        assert_eq!(resolved, vec![expected[1].clone(), expected[0].clone()]);

        // Entry is consumed on first read, so a second call falls back to disk and errors.
        assert!(
            resolve_field_stats("logs", &virtual_path, &full_schema, 50)
                .await
                .is_err()
        );

        // Mismatched max_field_statistics rejects cached stats and falls back to disk.
        cache_staged_field_stats(&virtual_path, 25, expected);
        assert!(
            resolve_field_stats("logs", &virtual_path, &full_schema, 50)
                .await
                .is_err()
        );
    }

    #[test]
    fn extract_datetime_from_parquet_path_regex_cached() {
        use super::extract_datetime_from_parquet_path_regex;

        let dt_padded = extract_datetime_from_parquet_path_regex(std::path::Path::new(
            "/staging/date=2026-07-04.hour=03.minute=09.node.data.parquet",
        ))
        .unwrap();
        assert_eq!(dt_padded.to_rfc3339(), "2026-07-04T03:09:00+00:00");

        let dt_single_digit = extract_datetime_from_parquet_path_regex(std::path::Path::new(
            "/staging/date=2026-07-04.hour=3.minute=9.node.data.parquet",
        ))
        .unwrap();
        assert_eq!(dt_single_digit, dt_padded);

        assert!(
            extract_datetime_from_parquet_path_regex(std::path::Path::new(
                "/staging/not_a_timestamped_file.parquet"
            ))
            .is_err()
        );
    }
}

#[cfg(test)]
mod forwarding_header_tests {
    use super::*;

    #[test]
    fn prism_auth_without_secret_forwards_only_authorization() {
        let mut headers = HeaderMap::new();
        headers.insert(
            actix_web::http::header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );
        headers.insert(
            actix_web::http::header::COOKIE,
            HeaderValue::from_static("session=abc"),
        );
        let map = prism_auth_without_secret(&headers).unwrap();
        assert_eq!(map.get("authorization").unwrap(), "Basic dXNlcjpwYXNz");
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn prism_auth_without_secret_rejects_cookie_and_api_key_callers() {
        for (name, value) in [("cookie", "session=abc"), ("x-api-key", "key")] {
            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
            assert!(matches!(
                prism_auth_without_secret(&headers),
                Err(QueryError::CustomError(_))
            ));
        }
    }

    #[test]
    fn prism_forwarding_headers_include_auth_secret_tenant_and_user() {
        let map = prism_forwarding_headers("hash", "acme", "alice", Some("Basic abc"))
            .expect("valid values build headers");
        assert_eq!(map.get("authorization").unwrap(), "Basic abc");
        assert_eq!(map.get(CLUSTER_SECRET_HEADER).unwrap(), "hash");
        assert_eq!(map.get("intra-cluster-tenant").unwrap(), "acme");
        assert_eq!(map.get("intra-cluster-userid").unwrap(), "alice");
    }

    #[test]
    fn prism_forwarding_headers_skip_missing_global_auth() {
        let map = prism_forwarding_headers("hash", "acme", "alice", None).unwrap();
        assert!(map.get("authorization").is_none());
    }

    #[test]
    fn prism_forwarding_headers_reject_invalid_stored_values_as_internal() {
        for (hash, user, auth, field) in [
            ("ha\nsh", "alice", None, "cluster secret hash"),
            ("hash", "al\nice", None, "user id"),
            ("hash", "alice", Some("Basic\r\nx"), "global query auth"),
        ] {
            let err = prism_forwarding_headers(hash, "acme", user, auth).unwrap_err();
            assert!(matches!(err, HeaderBuildError::Internal(f) if f == field));
            let status = actix_web::ResponseError::status_code(&QueryError::from(err));
            assert_eq!(status, actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    #[test]
    fn prism_forwarding_headers_reject_unreadable_stored_text_as_internal() {
        // `HeaderValue::from_str` accepts the non-ASCII values but the querier's `to_str` doesn't
        for value in ["josé", "用户", "ctl\u{1}"] {
            let auth = format!("Basic {value}");
            for (hash, user, auth, field) in [
                (value, "alice", None, "cluster secret hash"),
                ("hash", value, None, "user id"),
                ("hash", "alice", Some(auth.as_str()), "global query auth"),
            ] {
                let err = prism_forwarding_headers(hash, "acme", user, auth).unwrap_err();
                assert!(matches!(err, HeaderBuildError::Internal(f) if f == field));
                assert!(!err.to_string().contains(value));
                let status = actix_web::ResponseError::status_code(&QueryError::from(err));
                assert_eq!(status, actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    #[test]
    fn prism_forwarding_headers_reject_invalid_tenant_as_input() {
        let err = prism_forwarding_headers("hash", "ac\nme", "alice", None).unwrap_err();
        assert!(matches!(
            err,
            HeaderBuildError::Input(ParseHeaderError::InvalidTenantId)
        ));
        let status = actix_web::ResponseError::status_code(&QueryError::from(err));
        assert_eq!(status, actix_web::http::StatusCode::BAD_REQUEST);
    }
}

#[cfg(test)]
fn legacy_collect_all_field_stats_from_parquet_blocking(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
    max_field_statistics: usize,
) -> Result<Vec<FieldStat>, PostError> {
    let file = File::open(parquet_path).map_err(|e| {
        PostError::Invalid(anyhow::anyhow!(
            "Failed to open parquet file for field stats: {}",
            e
        ))
    })?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| PostError::Invalid(anyhow::anyhow!(e)))?
        .build()
        .map_err(|e| PostError::Invalid(anyhow::anyhow!(e)))?;
    let field_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut field_counts: HashMap<String, FieldCountState> = field_names
        .iter()
        .map(|field_name| {
            (
                field_name.clone(),
                FieldCountState::new(
                    stream_name.to_string(),
                    field_name.clone(),
                    max_field_statistics,
                ),
            )
        })
        .collect();

    for batch in &mut reader {
        let batch = match batch {
            Ok(batch) => batch,
            Err(e) => {
                warn!(
                    "Skipping undecodable parquet batch while calculating field stats for {}: {}",
                    parquet_path.display(),
                    e
                );
                continue;
            }
        };

        let batch_field_counts = legacy_collect_batch_field_counts_parallel(
            stream_name,
            &batch,
            &field_names,
            max_field_statistics,
        );

        for (field_name, batch_field_count) in batch_field_counts {
            merge_field_counts(&mut field_counts, &field_name, batch_field_count);
        }
    }

    Ok(field_names
        .into_iter()
        .filter_map(|field_name| {
            let state = field_counts.remove(&field_name)?;
            if state.total_count == 0 {
                return None;
            }

            Some(FieldStat {
                field_name,
                count: state.total_count,
                distinct_count: state.distinct_count(),
                distinct_stats: state.into_distinct_stats(max_field_statistics),
            })
        })
        .collect())
}

#[cfg(test)]
fn legacy_collect_batch_field_counts_parallel(
    stream_name: &str,
    batch: &arrow_array::RecordBatch,
    field_names: &[String],
    max_field_statistics: usize,
) -> Vec<(String, FieldCountState)> {
    FIELD_STATS_RAYON_POOL.install(|| {
        field_names
            .par_iter()
            .filter_map(|field_name| {
                legacy_collect_field_counts(stream_name, batch, field_name, max_field_statistics)
            })
            .collect()
    })
}

#[cfg(test)]
fn legacy_collect_field_counts(
    stream_name: &str,
    batch: &arrow_array::RecordBatch,
    field_name: &str,
    max_field_statistics: usize,
) -> Option<(String, FieldCountState)> {
    let array = batch.column_by_name(field_name)?;
    let mut field_count = FieldCountState::new_for_batch(
        stream_name.to_string(),
        field_name.to_string(),
        max_field_statistics,
    );

    // Fast paths for the string-like types that dominate log payloads. These read the value as a
    // borrowed `&str` straight out of the Arrow buffer, so a `String` is only allocated when the
    // value is actually inserted into the tracked-values map.
    match array.data_type() {
        DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                for row_index in 0..arr.len() {
                    if arr.is_null(row_index) {
                        field_count.record_value(Cow::Borrowed(NULL_VALUE));
                    } else {
                        field_count.record_value(Cow::Borrowed(arr.value(row_index)));
                    }
                }
                return Some((field_name.to_string(), field_count));
            }
        }
        DataType::Utf8View => {
            if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
                for row_index in 0..arr.len() {
                    if arr.is_null(row_index) {
                        field_count.record_value(Cow::Borrowed(NULL_VALUE));
                    } else {
                        field_count.record_value(Cow::Borrowed(arr.value(row_index)));
                    }
                }
                return Some((field_name.to_string(), field_count));
            }
        }
        _ => {}
    }

    for row_index in 0..array.len() {
        let value = format_arrow_value(array.as_ref(), row_index);
        field_count.record_value(Cow::Owned(value));
    }

    Some((field_name.to_string(), field_count))
}
