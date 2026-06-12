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
use crate::utils::create_intracluster_auth_headermap;
use crate::utils::get_tenant_id_from_request;
use crate::utils::get_user_from_request;
use crate::utils::json::apply_generic_flattening_for_partition;
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
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_array::TimestampMillisecondArray;
use arrow_schema::DataType;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::{debug, error, warn};

pub const DATASET_STATS_STREAM_NAME: &str = "pstats";
const DATASET_STATS_CUSTOM_PARTITION: &str = "dataset_name";
const MAX_CONCURRENT_FIELD_STATS: usize = 4;
const PARALLEL_FIELD_STATS_MIN_FIELDS: usize = 16;
const MIN_TRACKED_DISTINCT_VALUES_PER_FIELD: usize = 1024;
const MAX_TRACKED_DISTINCT_VALUES_PER_FIELD: usize = 10_000;
const HLL_PRECISION_BITS: u32 = 12;
const HLL_REGISTER_COUNT: usize = 1 << HLL_PRECISION_BITS;
static FIELD_STATS_QUERY_SEMAPHORE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_FIELD_STATS)));
static FIELD_STATS_RAYON_POOL: Lazy<ThreadPool> = Lazy::new(|| {
    ThreadPoolBuilder::new()
        .num_threads(MAX_CONCURRENT_FIELD_STATS)
        .thread_name(|index| format!("field-stats-{index}"))
        .build()
        .expect("field stats rayon pool should initialize")
});

#[derive(Serialize, Debug)]
struct DistinctStat {
    distinct_value: String,
    count: i64,
}

#[derive(Serialize, Debug)]
struct FieldStat {
    field_name: String,
    count: i64,
    distinct_count: i64,
    distinct_stats: Vec<DistinctStat>,
}

#[derive(Serialize, Debug)]
struct DatasetStats {
    dataset_name: String,
    stats_id: String,
    field_stats: Vec<FieldStat>,
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
    log_stats_calculation_time(stream_name, parquet_path, started_at, result.is_ok());

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
    let field_stats = collect_all_field_stats_from_parquet(
        stream_name,
        parquet_path,
        schema,
        max_field_statistics,
    )
    .await?;
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
    let stats_value =
        serde_json::to_value(&stats).map_err(|e| ObjectStorageError::Invalid(e.into()))?;
    ensure_dataset_stats_stream(tenant_id).await?;
    let vec_json = apply_generic_flattening_for_partition(
        stats_value,
        None,
        None,
        Some(&DATASET_STATS_CUSTOM_PARTITION.to_string()),
    )?;
    let mut p_custom_fields = HashMap::new();
    p_custom_fields.insert(USER_AGENT_KEY.to_string(), "parseable".to_string());
    let schema = PARSEABLE
        .get_stream(DATASET_STATS_STREAM_NAME, tenant_id)?
        .get_schema_raw();
    for json in vec_json {
        let origin_size = crate::handlers::http::modal::utils::ingest_utils::json_byte_size(&json);
        json::Event {
            json,
            p_timestamp: parquet_ts,
        }
        .into_event(
            DATASET_STATS_STREAM_NAME.to_string(),
            origin_size,
            &schema,
            false,
            Some(&DATASET_STATS_CUSTOM_PARTITION.to_string()),
            None,
            SchemaVersion::V1,
            StreamType::Internal,
            &p_custom_fields,
            TelemetryType::Logs,
            tenant_id,
            true,
        )?
        .process()?;
    }
    Ok(stats_calculated)
}

fn log_stats_calculation_time(
    stream_name: &str,
    parquet_path: &Path,
    started_at: Instant,
    success: bool,
) {
    let parquet_file = parquet_path
        .file_name()
        .and_then(|filename| filename.to_str())
        .unwrap_or("<unknown>");
    let elapsed_ms = started_at.elapsed().as_millis();
    if success {
        debug!(
            "Field stats calculation completed for parquet file {parquet_file} in {elapsed_ms} ms. stream={stream_name}"
        );
    } else {
        warn!(
            "Field stats calculation failed for parquet file {parquet_file} after {elapsed_ms} ms. stream={stream_name}"
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

async fn collect_all_field_stats_from_parquet(
    stream_name: &str,
    parquet_path: &Path,
    schema: &Schema,
    max_field_statistics: usize,
) -> Result<Vec<FieldStat>, PostError> {
    let permit = FIELD_STATS_QUERY_SEMAPHORE
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| {
            PostError::Invalid(anyhow::anyhow!(
                "Failed to acquire field stats query permit: {}",
                e
            ))
        })?;
    let stream_name = stream_name.to_string();
    let parquet_path = parquet_path.to_path_buf();
    let schema = schema.clone();

    tokio::task::spawn_blocking(move || {
        let _permit = permit;
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

fn collect_all_field_stats_from_parquet_blocking(
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

        let batch_counts = if field_names.len() >= PARALLEL_FIELD_STATS_MIN_FIELDS {
            collect_batch_field_counts_parallel(&batch, &field_names)
        } else {
            collect_batch_field_counts_serial(&batch, &field_names)
        };

        for (field_name, counts) in batch_counts {
            merge_field_counts(&mut field_counts, &field_name, counts);
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

fn collect_batch_field_counts_serial(
    batch: &arrow_array::RecordBatch,
    field_names: &[String],
) -> Vec<(String, HashMap<String, i64>)> {
    field_names
        .iter()
        .filter_map(|field_name| collect_field_counts(batch, field_name))
        .collect()
}

fn collect_batch_field_counts_parallel(
    batch: &arrow_array::RecordBatch,
    field_names: &[String],
) -> Vec<(String, HashMap<String, i64>)> {
    FIELD_STATS_RAYON_POOL.install(|| {
        field_names
            .par_iter()
            .filter_map(|field_name| collect_field_counts(batch, field_name))
            .collect()
    })
}

fn collect_field_counts(
    batch: &arrow_array::RecordBatch,
    field_name: &str,
) -> Option<(String, HashMap<String, i64>)> {
    let array = batch.column_by_name(field_name)?;
    let mut counts = HashMap::new();

    for row_index in 0..array.len() {
        let value = format_arrow_value(array.as_ref(), row_index);
        *counts.entry(value).or_default() += 1;
    }

    Some((field_name.to_string(), counts))
}

fn merge_field_counts(
    field_counts: &mut HashMap<String, FieldCountState>,
    field_name: &str,
    counts: HashMap<String, i64>,
) {
    let field_total = field_counts
        .get_mut(field_name)
        .expect("field_counts initialized for each field");

    field_total.merge_counts(counts);
}

struct FieldCountState {
    stream_name: String,
    field_name: String,
    total_count: i64,
    counts: HashMap<String, i64>,
    hll: HyperLogLog,
    max_tracked_values: usize,
    approximate: bool,
}

impl FieldCountState {
    fn new(stream_name: String, field_name: String, max_field_statistics: usize) -> Self {
        Self {
            stream_name,
            field_name,
            total_count: 0,
            counts: HashMap::new(),
            hll: HyperLogLog::new(),
            max_tracked_values: tracked_distinct_value_limit(max_field_statistics),
            approximate: false,
        }
    }

    fn merge_counts(&mut self, counts: HashMap<String, i64>) {
        for (value, count) in counts {
            self.hll.add(&value);
            self.total_count += count;

            if let Some(existing_count) = self.counts.get_mut(&value) {
                *existing_count += count;
                continue;
            }

            if self.counts.len() < self.max_tracked_values {
                self.counts.insert(value, count);
                continue;
            }

            if !self.approximate {
                self.approximate = true;
                warn!(
                    "Field stats cardinality cap reached for stream {} field {}. Tracking bounded top-value candidates with max_tracked_values={}",
                    self.stream_name, self.field_name, self.max_tracked_values
                );
            }

            if let Some((min_value, min_count)) = self.current_min_value()
                && count > min_count
            {
                self.counts.remove(&min_value);
                self.counts.insert(value, count);
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
        let hash = hash_field_value(value);
        let register_index = (hash >> (u64::BITS - HLL_PRECISION_BITS)) as usize;
        let rank = ((hash << HLL_PRECISION_BITS).leading_zeros() + 1)
            .min(u64::BITS - HLL_PRECISION_BITS + 1) as u8;
        self.registers[register_index] = self.registers[register_index].max(rank);
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

#[inline(always)]
pub fn extract_datetime_from_parquet_path_regex(
    parquet_path: &Path,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    let filename = parquet_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("Invalid filename")?;

    // Regex to match date=YYYY-MM-DD.hour=HH.minute=MM pattern
    let re = Regex::new(r"date=(\d{4}-\d{2}-\d{2})\.hour=(\d{1,2})\.minute=(\d{1,2})")?;

    if let Some(captures) = re.captures(filename) {
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
    let tenant_id = get_tenant_id_from_request(&req);
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
            let auth = if let Some((_, hash)) = CLUSTER_SECRET.get() {
                let mut map = actix_web::http::header::HeaderMap::new();
                if let Some(header) = TENANT_METADATA.get_global_query_auth(tenant) {
                    map.insert(
                        HeaderName::from_static("authorization"),
                        HeaderValue::from_str(&header).unwrap(),
                    );
                }
                let userid = get_user_from_request(&req).unwrap();
                map.insert(
                    HeaderName::from_static(CLUSTER_SECRET_HEADER),
                    HeaderValue::from_str(hash).unwrap(),
                );
                map.insert(
                    HeaderName::from_static("intra-cluster-tenant"),
                    HeaderValue::from_str(tenant).unwrap(),
                );
                map.insert(
                    HeaderName::from_static("intra-cluster-userid"),
                    HeaderValue::from_str(&userid).unwrap(),
                );
                Some(map)
            } else {
                let auth = create_intracluster_auth_headermap(
                    req.headers(),
                    "",
                    &get_user_from_request(&req).unwrap(),
                );
                let mut map = HeaderMap::new();

                for (key, value) in auth.iter() {
                    if let Ok(name) = HeaderName::from_bytes(key.as_str().as_bytes())
                        && let Ok(val) = HeaderValue::from_bytes(value.as_bytes())
                    {
                        map.insert(name, val);
                    }
                }
                Some(map)
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
        BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
        TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use temp_dir::TempDir;

    use crate::storage::field_stats::{
        FieldCountState, build_stats_sql, collect_all_field_stats_from_parquet_blocking,
    };

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
}
