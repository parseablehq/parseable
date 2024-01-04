/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use chrono::{DateTime, Utc, TimeZone};
use datafusion::error::DataFusionError;
use datafusion::arrow::json::writer::record_batches_to_json_rows;
use datafusion::arrow::record_batch::RecordBatch;

use crate::metrics::{EVENTS_INGESTED, EVENTS_INGESTED_SIZE, STORAGE_SIZE};
use crate::query;
use crate::query::error::ExecuteError;

/// Helper struct type created by copying stats values from metadata
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub events: u64,
    pub ingestion: u64,
    pub storage: u64,
}

pub fn get_current_stats(stream_name: &str, format: &'static str) -> Option<Stats> {
    let event_labels = event_labels(stream_name, format);
    let storage_size_labels = storage_size_labels(stream_name);

    let events_ingested = EVENTS_INGESTED
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get();
    let ingestion_size = EVENTS_INGESTED_SIZE
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get();
    let storage_size = STORAGE_SIZE
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get();
    // this should be valid for all cases given that gauge must never go negative
    let ingestion_size = ingestion_size as u64;
    let storage_size = storage_size as u64;

    Some(Stats {
        events: events_ingested,
        ingestion: ingestion_size,
        storage: storage_size,
    })
}

pub fn delete_stats(stream_name: &str, format: &'static str) -> prometheus::Result<()> {
    let event_labels = event_labels(stream_name, format);
    let storage_size_labels = storage_size_labels(stream_name);

    EVENTS_INGESTED.remove_label_values(&event_labels)?;
    EVENTS_INGESTED_SIZE.remove_label_values(&event_labels)?;
    STORAGE_SIZE.remove_label_values(&storage_size_labels)?;

    Ok(())
}

fn event_labels<'a>(stream_name: &'a str, format: &'static str) -> [&'a str; 2] {
    [stream_name, format]
}

fn storage_size_labels(stream_name: &str) -> [&str; 3] {
    ["data", stream_name, "parquet"]
}

pub async fn get_first_event_stats(stream_name: &str) -> Result<Option<String>, QueryError> {
    let query_string = format!("SELECT p_timestamp FROM {} ORDER BY p_timestamp LIMIT 1", stream_name);
    let first_time: DateTime<Utc> = Utc.timestamp_opt(0, 0).single().expect("Failed to get the first UTC time");
    let now_time: DateTime<Utc> = Utc::now();
    let session_state = query::QUERY_SESSION.state();
    let logical_plan = session_state.create_logical_plan(&query_string).await?;
   
    let query = query::Query {
        raw_logical_plan: logical_plan,
        start: first_time,
        end: now_time,
        filter_tag: Some(Vec::new()),
    };

    let (records, _fields) = query.execute().await?;
    let records_itr: Vec<&RecordBatch> = records.iter().collect();
    let json_records = record_batches_to_json_rows(&records_itr).unwrap();

    if let Some(single_record) = json_records.get(0) {
        if let Some(p_timestamp_value) = single_record.get("p_timestamp") {
            let p_timestamp_str = p_timestamp_value.as_str().unwrap_or_default();
            return Ok(Some(p_timestamp_str.to_string()));
        }
    }

    Ok(None)
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("While generating times for 'now' failed to parse duration")]
    NotValidDuration(#[from] humantime::DurationError),
    #[error("Parsed duration out of range")]
    OutOfRange(#[from] chrono::OutOfRangeError),
    #[error("Datafusion Error: {0}")]
    Datafusion(#[from] DataFusionError),
    #[error("Query execution failed due to {0}")]
    Execute(#[from] ExecuteError),
}