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
use crate::handlers::http::ingest::PostError;
use crate::metadata::SchemaVersion;
use crate::parseable::PARSEABLE;
use crate::query::QUERY_SESSION_STATE;
use crate::storage::ObjectStorageError;
use crate::storage::StreamType;
use crate::utils::json::apply_generic_flattening_for_partition;
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
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use regex::Regex;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use tracing::trace;
use tracing::warn;
use ulid::Ulid;

pub const DATASET_STATS_STREAM_NAME: &str = "pstats";
const DATASET_STATS_CUSTOM_PARTITION: &str = "dataset_name";
const MAX_CONCURRENT_FIELD_STATS: usize = 10;

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
    //create datetime from timestamp present in parquet path
    let parquet_ts = extract_datetime_from_parquet_path_regex(parquet_path).map_err(|e| {
        PostError::Invalid(anyhow::anyhow!(
            "Failed to extract datetime from parquet path: {}",
            e
        ))
    })?;
    let field_stats = {
        let mut session_state = QUERY_SESSION_STATE.clone();
        session_state
            .config_mut()
            .options_mut()
            .catalog
            .default_schema = tenant_id.as_ref().map_or("public".into(), |v| v.to_owned());
        let ctx = SessionContext::new_with_state(session_state);
        let table_name = Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path
                .to_str()
                .ok_or_else(|| PostError::Invalid(anyhow::anyhow!("Invalid UTF-8 in path")))?,
            ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| PostError::Invalid(e.into()))?;

        collect_all_field_stats(&table_name, &ctx, schema, max_field_statistics).await
    };
    let mut stats_calculated = false;
    let stats = DatasetStats {
        dataset_name: stream_name.to_string(),
        field_stats,
    };
    if stats.field_stats.is_empty() {
        return Ok(stats_calculated);
    }
    stats_calculated = true;
    let stats_value =
        serde_json::to_value(&stats).map_err(|e| ObjectStorageError::Invalid(e.into()))?;
    let log_source_entry = LogSourceEntry::new(LogSource::Json, HashSet::new());
    PARSEABLE
        .create_stream_if_not_exists(
            DATASET_STATS_STREAM_NAME,
            StreamType::Internal,
            Some(&DATASET_STATS_CUSTOM_PARTITION.to_string()),
            vec![log_source_entry],
            TelemetryType::Logs,
            tenant_id,
        )
        .await?;
    let vec_json = apply_generic_flattening_for_partition(
        stats_value,
        None,
        None,
        Some(&DATASET_STATS_CUSTOM_PARTITION.to_string()),
    )?;
    let mut p_custom_fields = HashMap::new();
    p_custom_fields.insert(USER_AGENT_KEY.to_string(), "parseable".to_string());
    for json in vec_json {
        let origin_size = serde_json::to_vec(&json).unwrap().len() as u64; // string length need not be the same as byte length
        let schema = PARSEABLE
            .get_stream(DATASET_STATS_STREAM_NAME, tenant_id)?
            .get_schema_raw();
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
        )?
        .process()?;
    }
    Ok(stats_calculated)
}

/// Collects statistics for all fields in the stream.
/// Returns a vector of `FieldStat` for each field with non-zero count.
/// Uses `buffer_unordered` to run up to `MAX_CONCURRENT_FIELD_STATS` queries concurrently.
async fn collect_all_field_stats(
    stream_name: &str,
    ctx: &SessionContext,
    schema: &Schema,
    max_field_statistics: usize,
) -> Vec<FieldStat> {
    // Collect field names into an owned Vec<String> to avoid lifetime issues
    let field_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let field_futures = field_names.into_iter().map(|field_name| {
        let ctx = ctx.clone();
        async move {
            calculate_single_field_stats(ctx, stream_name, &field_name, max_field_statistics).await
        }
    });

    futures::stream::iter(field_futures)
        .buffer_unordered(MAX_CONCURRENT_FIELD_STATS)
        .filter_map(std::future::ready)
        .collect::<Vec<_>>()
        .await
}

/// This function is used to fetch distinct values and their counts for a field in the stream.
/// Returns a vector of `DistinctStat` containing distinct values and their counts.
/// The query groups by the field and orders by the count in descending order, limiting the results to `PARSEABLE.options.max_field_statistics`.
async fn calculate_single_field_stats(
    ctx: SessionContext,
    stream_name: &str,
    field_name: &str,
    max_field_statistics: usize,
) -> Option<FieldStat> {
    let mut total_count = 0;
    let mut distinct_count = 0;
    let mut distinct_stats = Vec::new();

    let combined_sql = get_stats_sql(stream_name, field_name, max_field_statistics);
    match ctx.sql(&combined_sql).await {
        Ok(df) => {
            let mut stream = match df.execute_stream().await {
                Ok(stream) => stream,
                Err(e) => {
                    trace!("Failed to execute distinct stats query: {e}");
                    return None; // Return empty if query fails
                }
            };
            while let Some(batch_result) = stream.next().await {
                let rb = match batch_result {
                    Ok(batch) => batch,
                    Err(e) => {
                        trace!("Failed to fetch batch in distinct stats query: {e}");
                        continue; // Skip this batch if there's an error
                    }
                };
                let total_count_array = rb.column(0).as_any().downcast_ref::<Int64Array>()?;
                let distinct_count_array = rb.column(1).as_any().downcast_ref::<Int64Array>()?;

                total_count = total_count_array.value(0);
                distinct_count = distinct_count_array.value(0);
                if distinct_count == 0 {
                    return None;
                }

                let field_value_array = rb.column(2).as_ref();
                let value_count_array = rb.column(3).as_any().downcast_ref::<Int64Array>()?;

                for i in 0..rb.num_rows() {
                    let value = format_arrow_value(field_value_array, i);
                    let count = value_count_array.value(i);

                    distinct_stats.push(DistinctStat {
                        distinct_value: value,
                        count,
                    });
                }
            }
        }
        Err(e) => {
            trace!("Failed to execute distinct stats query for field: {field_name}, error: {e}");
            return None;
        }
    }
    Some(FieldStat {
        field_name: field_name.to_string(),
        count: total_count,
        distinct_count,
        distinct_stats,
    })
}

fn get_stats_sql(stream_name: &str, field_name: &str, max_field_statistics: usize) -> String {
    let escaped_field_name = field_name.replace('"', "\"\"");
    let escaped_stream_name = stream_name.replace('"', "\"\"");

    format!(
        r#"
        WITH field_groups AS (
            SELECT 
                "{escaped_field_name}" as field_value,
                COUNT(*) as value_count
            FROM "{escaped_stream_name}"
            GROUP BY "{escaped_field_name}"
        ),
        field_summary AS (
            SELECT 
                field_value,
                value_count,
                SUM(value_count) OVER () as total_count,
                COUNT(*) OVER () as distinct_count,
                ROW_NUMBER() OVER (ORDER BY value_count DESC) as rn
            FROM field_groups
        )
        SELECT 
            total_count,
            distinct_count,
            field_value,
            value_count
        FROM field_summary 
        WHERE rn <= {max_field_statistics}
        ORDER BY value_count DESC
        "#
    )
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
                    .map(|dt| dt.to_string())
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

fn extract_datetime_from_parquet_path_regex(
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

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, sync::Arc};

    use arrow::buffer::OffsetBuffer;
    use arrow_array::{
        BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
        TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use temp_dir::TempDir;
    use ulid::Ulid;

    use crate::storage::field_stats::calculate_single_field_stats;

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

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_multiple_values() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let random_suffix = Ulid::new().to_string();
        let ctx = SessionContext::new();
        ctx.register_parquet(
            &random_suffix,
            parquet_path.to_str().expect("valid path"),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test name field with multiple distinct values and different frequencies
        let result = calculate_single_field_stats(ctx.clone(), &random_suffix, "name", 50).await;
        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "name");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 7);
        assert_eq!(stats.distinct_stats.len(), 7);

        // Verify ordering by count (descending)
        assert!(stats.distinct_stats[0].count >= stats.distinct_stats[1].count);
        assert!(stats.distinct_stats[1].count >= stats.distinct_stats[2].count);

        // Verify specific counts
        let alice_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "Alice");
        assert!(alice_stat.is_some());
        assert_eq!(alice_stat.unwrap().count, 3);

        let bob_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "Bob");
        assert!(bob_stat.is_some());
        assert_eq!(bob_stat.unwrap().count, 2);

        let charlie_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "Charlie");
        assert!(charlie_stat.is_some());
        assert_eq!(charlie_stat.unwrap().count, 1);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_numeric_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();

        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test score field (Float64)
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "score", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "score");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 9);

        // Verify that 95.5 appears twice (should be first due to highest count)
        let highest_count_stat = &stats.distinct_stats[0];
        assert_eq!(highest_count_stat.distinct_value, "95.5");
        assert_eq!(highest_count_stat.count, 2);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_boolean_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test active field (Boolean)
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "active", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "active");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 3);
        assert_eq!(stats.distinct_stats.len(), 3);

        assert_eq!(stats.distinct_stats[0].distinct_value, "true");
        assert_eq!(stats.distinct_stats[0].count, 6);
        assert_eq!(stats.distinct_stats[1].distinct_value, "false");
        assert_eq!(stats.distinct_stats[1].count, 3);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_timestamp_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test created_at field (Timestamp)
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "created_at", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "created_at");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 9);

        // Verify that the duplicate timestamp appears twice
        let duplicate_timestamp = &stats.distinct_stats[0];
        assert_eq!(duplicate_timestamp.count, 2);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_single_value_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test field with single distinct value
        let result =
            calculate_single_field_stats(ctx.clone(), &table_name, "single_value", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "single_value");
        assert_eq!(stats.count, 10);
        assert_eq!(stats.distinct_count, 1);
        assert_eq!(stats.distinct_stats.len(), 1);
        assert_eq!(stats.distinct_stats[0].distinct_value, "constant");
        assert_eq!(stats.distinct_stats[0].count, 10);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_nonexistent_table() {
        let ctx = SessionContext::new();

        // Test with non-existent table
        let result =
            calculate_single_field_stats(ctx.clone(), "non_existent_table", "field", 50).await;

        // Should return None due to SQL execution failure
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_nonexistent_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test with non-existent field
        let result =
            calculate_single_field_stats(ctx.clone(), &table_name, "non_existent_field", 50).await;

        // Should return None due to SQL execution failure
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_special_characters() {
        // Create a schema with field names containing special characters
        let schema = Arc::new(Schema::new(vec![
            Field::new("field with spaces", DataType::Utf8, true),
            Field::new("field\"with\"quotes", DataType::Utf8, true),
            Field::new("field'with'apostrophes", DataType::Utf8, true),
        ]));

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("special_chars.parquet");

        use parquet::arrow::AsyncArrowWriter;
        use tokio::fs::File;

        let space_array = StringArray::from(vec![Some("value1"), Some("value2"), Some("value1")]);
        let quote_array = StringArray::from(vec![Some("quote1"), Some("quote2"), Some("quote1")]);
        let apostrophe_array = StringArray::from(vec![Some("apos1"), Some("apos2"), Some("apos1")]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(space_array),
                Arc::new(quote_array),
                Arc::new(apostrophe_array),
            ],
        )
        .unwrap();

        let file = File::create(&file_path).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            file_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test field with spaces
        let result =
            calculate_single_field_stats(ctx.clone(), &table_name, "field with spaces", 50).await;
        assert!(result.is_some());
        let stats = result.unwrap();
        assert_eq!(stats.field_name, "field with spaces");
        assert_eq!(stats.count, 3);
        assert_eq!(stats.distinct_count, 2);

        // Test field with quotes
        let result =
            calculate_single_field_stats(ctx.clone(), &table_name, "field\"with\"quotes", 50).await;
        assert!(result.is_some());
        let stats = result.unwrap();
        assert_eq!(stats.field_name, "field\"with\"quotes");
        assert_eq!(stats.count, 3);
        assert_eq!(stats.distinct_count, 2);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_empty_table() {
        // Create empty table
        let schema = Arc::new(create_test_schema());
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty_data.parquet");

        use parquet::arrow::AsyncArrowWriter;
        use tokio::fs::File;

        let file = File::create(&file_path).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema.clone(), None).unwrap();

        // Create empty batch
        let empty_batch = RecordBatch::new_empty(schema.clone());
        writer.write(&empty_batch).await.unwrap();
        writer.close().await.unwrap();

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            file_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let result = calculate_single_field_stats(ctx.clone(), &table_name, "name", 50).await;
        assert!(result.unwrap().distinct_stats.is_empty());
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_streaming_behavior() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test that the function handles streaming properly by checking
        // that all data is collected correctly across multiple batches
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "name", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        // Verify that the streaming collected all the data
        let total_distinct_count: i64 = stats.distinct_stats.iter().map(|s| s.count).sum();
        assert_eq!(total_distinct_count, stats.count);

        // Verify that distinct_stats are properly ordered by count
        for i in 1..stats.distinct_stats.len() {
            assert!(stats.distinct_stats[i - 1].count >= stats.distinct_stats[i].count);
        }
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_large_dataset() {
        // Create a larger dataset to test streaming behavior
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, true),
        ]));

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("large_data.parquet");

        use parquet::arrow::AsyncArrowWriter;
        use tokio::fs::File;

        // Create 1000 rows with 10 distinct categories
        let ids: Vec<i64> = (0..1000).collect();
        let categories: Vec<Option<&str>> = (0..1000)
            .map(|i| {
                Some(match i % 10 {
                    0 => "cat_0",
                    1 => "cat_1",
                    2 => "cat_2",
                    3 => "cat_3",
                    4 => "cat_4",
                    5 => "cat_5",
                    6 => "cat_6",
                    7 => "cat_7",
                    8 => "cat_8",
                    _ => "cat_9",
                })
            })
            .collect();

        let id_array = Int64Array::from(ids);
        let category_array = StringArray::from(categories);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(category_array)],
        )
        .unwrap();

        let file = File::create(&file_path).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();
        ctx.register_parquet(
            &table_name,
            file_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let result = calculate_single_field_stats(ctx.clone(), &table_name, "category", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.count, 1000);
        assert_eq!(stats.distinct_count, 10);
        assert_eq!(stats.distinct_stats.len(), 10);

        // Each category should appear 100 times
        for distinct_stat in &stats.distinct_stats {
            assert_eq!(distinct_stat.count, 100);
        }
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_int_list_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();

        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test int_list field (List<Int64>)
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "int_list", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "int_list");
        assert_eq!(stats.count, 10);

        // Verify we have the expected distinct lists
        // Expected: [1, 2, 3], [4, 5], [6, 7, 8, 9], [1], [10, 11], [], [12, 13, 14], [1, 2]
        assert_eq!(stats.distinct_count, 8);

        // Check for duplicate lists - [1, 2, 3] appears twice, [4, 5] appears twice
        let list_123_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "[1, 2, 3]");
        assert!(list_123_stat.is_some());
        assert_eq!(list_123_stat.unwrap().count, 2);

        let list_45_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "[4, 5]");
        assert!(list_45_stat.is_some());
        assert_eq!(list_45_stat.unwrap().count, 2);

        // Check single occurrence lists
        let list_6789_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "[6, 7, 8, 9]");
        assert!(list_6789_stat.is_some());
        assert_eq!(list_6789_stat.unwrap().count, 1);

        let empty_list_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "[]");
        assert!(empty_list_stat.is_some());
        assert_eq!(empty_list_stat.unwrap().count, 1);
    }

    #[tokio::test]
    async fn test_calculate_single_field_stats_with_float_list_field() {
        let (_temp_dir, parquet_path) = create_test_parquet_with_data().await;

        let ctx = SessionContext::new();
        let table_name = ulid::Ulid::new().to_string();

        ctx.register_parquet(
            &table_name,
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // Test float_list field (List<Float64>)
        let result = calculate_single_field_stats(ctx.clone(), &table_name, "float_list", 50).await;

        assert!(result.is_some());
        let stats = result.unwrap();

        assert_eq!(stats.field_name, "float_list");
        assert_eq!(stats.count, 10);

        // Expected distinct lists: [1.1, 2.2], [3.3, 4.4, 5.5], [6.6], [7.7, 8.8, 9.9], [10.0], [], [11.1, 12.2], [13.3]
        assert_eq!(stats.distinct_count, 8);

        // Check for duplicate lists - [1.1, 2.2] appears twice, [3.3, 4.4, 5.5] appears twice
        let list_11_22_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "[1.1, 2.2]");
        assert!(list_11_22_stat.is_some());
        assert_eq!(list_11_22_stat.unwrap().count, 2);

        let list_33_44_55_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "[3.3, 4.4, 5.5]");
        assert!(list_33_44_55_stat.is_some());
        assert_eq!(list_33_44_55_stat.unwrap().count, 2);

        // Check single occurrence lists
        let list_66_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "[6.6]");
        assert!(list_66_stat.is_some());
        assert_eq!(list_66_stat.unwrap().count, 1);

        let empty_list_stat = stats
            .distinct_stats
            .iter()
            .find(|s| s.distinct_value == "[]");
        assert!(empty_list_stat.is_some());
        assert_eq!(empty_list_stat.unwrap().count, 1);
    }
}
