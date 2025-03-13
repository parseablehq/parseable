/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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

use std::sync::Arc;

use actix_web::http::header::ContentType;
use arrow_schema::Schema;
use chrono::Utc;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::{
    handlers::http::{
        cluster::{
            fetch_stats_from_ingestors,
            utils::{merge_quried_stats, IngestionStats, QueriedStats, StorageStats},
        },
        logstream::error::StreamError,
        query::{into_query, update_schema_when_distributed, Query, QueryError},
    },
    hottier::{HotTierError, HotTierManager, StreamHotTier},
    parseable::{StreamNotFound, PARSEABLE},
    query::{error::ExecuteError, execute, CountsRequest, CountsResponse, QUERY_SESSION},
    rbac::{map::SessionKey, role::Action, Users},
    stats,
    storage::{retention::Retention, StreamInfo, StreamType},
    utils::{
        arrow::record_batches_to_json,
        time::{TimeParseError, TimeRange},
    },
    LOCK_EXPECT,
};

#[derive(Serialize)]
pub struct PrismLogstreamInfo {
    info: StreamInfo,
    schema: Arc<Schema>,
    stats: QueriedStats,
    retention: Retention,
}

pub async fn get_prism_logstream_info(
    stream_name: &str,
) -> Result<PrismLogstreamInfo, PrismLogstreamError> {
    // get StreamInfo
    let info = get_stream_info_helper(stream_name).await?;

    // get stream schema
    let schema = get_stream_schema_helper(stream_name).await?;

    // get stream stats
    let stats = get_stats(stream_name).await?;

    // get retention
    let retention = PARSEABLE
        .get_stream(stream_name)?
        .get_retention()
        .unwrap_or_default();

    Ok(PrismLogstreamInfo {
        info,
        schema,
        stats,
        retention,
    })
}

async fn get_stream_schema_helper(stream_name: &str) -> Result<Arc<Schema>, StreamError> {
    // Ensure parseable is aware of stream in distributed mode
    if PARSEABLE.check_or_load_stream(stream_name).await {
        return Err(StreamNotFound(stream_name.to_owned()).into());
    }

    let stream = PARSEABLE.get_stream(stream_name)?;
    match update_schema_when_distributed(&vec![stream_name.to_owned()]).await {
        Ok(_) => {
            let schema = stream.get_schema();
            Ok(schema)
        }
        Err(err) => Err(StreamError::Custom {
            msg: err.to_string(),
            status: StatusCode::EXPECTATION_FAILED,
        }),
    }
}

async fn get_stats(stream_name: &str) -> Result<QueriedStats, PrismLogstreamError> {
    let stats = stats::get_current_stats(stream_name, "json")
        .ok_or_else(|| StreamNotFound(stream_name.to_owned()))?;

    let ingestor_stats = if PARSEABLE
        .get_stream(stream_name)
        .is_ok_and(|stream| stream.get_stream_type() == StreamType::UserDefined)
    {
        Some(fetch_stats_from_ingestors(stream_name).await?)
    } else {
        None
    };

    let time = Utc::now();

    let stats = {
        let ingestion_stats = IngestionStats::new(
            stats.current_stats.events,
            stats.current_stats.ingestion,
            stats.lifetime_stats.events,
            stats.lifetime_stats.ingestion,
            stats.deleted_stats.events,
            stats.deleted_stats.ingestion,
            "json",
        );
        let storage_stats = StorageStats::new(
            stats.current_stats.storage,
            stats.lifetime_stats.storage,
            stats.deleted_stats.storage,
            "parquet",
        );

        QueriedStats::new(stream_name, time, ingestion_stats, storage_stats)
    };

    let stats = if let Some(mut ingestor_stats) = ingestor_stats {
        ingestor_stats.push(stats);
        merge_quried_stats(ingestor_stats)
    } else {
        stats
    };

    Ok(stats)
}

async fn get_stream_info_helper(stream_name: &str) -> Result<StreamInfo, StreamError> {
    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if PARSEABLE.check_or_load_stream(stream_name).await {
        return Err(StreamNotFound(stream_name.to_owned()).into());
    }

    let storage = PARSEABLE.storage.get_object_store();
    // if first_event_at is not found in memory map, check if it exists in the storage
    // if it exists in the storage, update the first_event_at in memory map
    let stream_first_event_at = if let Some(first_event_at) =
        PARSEABLE.get_stream(stream_name)?.get_first_event()
    {
        Some(first_event_at)
    } else if let Ok(Some(first_event_at)) = storage.get_first_event_from_storage(stream_name).await
    {
        PARSEABLE
            .update_first_event_at(stream_name, &first_event_at)
            .await
    } else {
        None
    };

    let hash_map = PARSEABLE.streams.read().unwrap();
    let stream_meta = hash_map
        .get(stream_name)
        .ok_or_else(|| StreamNotFound(stream_name.to_owned()))?
        .metadata
        .read()
        .expect(LOCK_EXPECT);

    let stream_info = StreamInfo {
        stream_type: stream_meta.stream_type,
        created_at: stream_meta.created_at.clone(),
        first_event_at: stream_first_event_at,
        time_partition: stream_meta.time_partition.clone(),
        time_partition_limit: stream_meta
            .time_partition_limit
            .map(|limit| limit.to_string()),
        custom_partition: stream_meta.custom_partition.clone(),
        static_schema_flag: stream_meta.static_schema_flag,
        log_source: stream_meta.log_source.clone(),
    };

    Ok(stream_info)
}

/// Response structure for Prism dataset queries.
/// Contains information about a stream, its statistics, retention policy,
/// and query results.
#[derive(Serialize)]
pub struct PrismDatasetResponse {
    /// Basic information about the stream
    info: StreamInfo,
    /// Statistics for the queried timeframe
    stats: QueriedStats,
    /// Retention policy details
    retention: Retention,
    /// Hot tier information if available
    hottier: Option<StreamHotTier>,
    /// Count of records in the specified time range
    counts: CountsResponse,
    /// Collection of distinct values for source identifiers
    distinct_sources: Value,
}

/// Request parameters for retrieving Prism dataset information.
/// Defines which streams to query and the time range for the query.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrismDatasetRequest {
    /// List of stream names to query
    #[serde(default)]
    streams: Vec<String>,
    /// ISO 8601 formatted start time or human-readable time expression
    start_time: String,
    /// ISO 8601 formatted end time or human-readable time expression
    end_time: String,
}

impl PrismDatasetRequest {
    /// Retrieves dataset information for all specified streams.
    ///
    /// Processes each stream in the request and compiles their information.
    /// Streams that don't exist or can't be accessed are skipped.
    ///
    /// # Returns
    /// - `Ok(Vec<PrismDatasetResponse>)`: List of responses for successfully processed streams
    /// - `Err(PrismLogstreamError)`: If a critical error occurs during processing
    ///
    /// # Note
    /// 1. This method won't fail if individual streams fail - it will only include
    ///    successfully processed streams in the result.
    /// 2. On receiving an empty stream list, we return for all streams the user is able to query for
    pub async fn get_datasets(
        mut self,
        key: SessionKey,
    ) -> Result<Vec<PrismDatasetResponse>, PrismLogstreamError> {
        if self.streams.is_empty() {
            self.streams = PARSEABLE.streams.list();
        }

        let mut responses = vec![];
        for stream in self.streams.iter() {
            if Users.authorize(key.clone(), Action::ListStream, Some(stream), None)
                != crate::rbac::Response::Authorized
            {
                warn!("Unauthorized access requested for stream: {stream}");
                continue;
            }

            if PARSEABLE.check_or_load_stream(stream).await {
                debug!("Stream not found: {stream}");
                continue;
            }

            let PrismLogstreamInfo {
                info,
                stats,
                retention,
                ..
            } = get_prism_logstream_info(stream).await?;

            let hottier = match HotTierManager::global() {
                Some(hot_tier_manager) => {
                    let stats = hot_tier_manager.get_hot_tier(stream).await?;
                    Some(stats)
                }
                _ => None,
            };
            let records = CountsRequest {
                stream: stream.clone(),
                start_time: self.start_time.clone(),
                end_time: self.end_time.clone(),
                num_bins: 1,
            }
            .get_bin_density()
            .await?;
            let counts = CountsResponse {
                fields: vec!["start_time".into(), "end_time".into(), "count".into()],
                records,
            };

            // Retrieve distinct values for source identifiers
            // Returns None if fields aren't present or if query fails
            let ips = self.get_distinct_entries(stream, "p_src_ip").await.ok();
            let user_agents = self
                .get_distinct_entries(stream, "p_user_agent")
                .await
                .ok();

            responses.push(PrismDatasetResponse {
                info,
                stats,
                retention,
                hottier,
                counts,
                distinct_sources: json!({
                    "ips": ips,
                    "user_agents": user_agents
                }),
            })
        }

        Ok(responses)
    }

    /// Retrieves distinct values for a specific field in a stream.
    ///
    /// # Parameters
    /// - `stream_name`: Name of the stream to query
    /// - `field`: Field name to get distinct values for
    ///
    /// # Returns
    /// - `Ok(Vec<String>)`: List of distinct values found for the field
    /// - `Err(QueryError)`: If the query fails or field doesn't exist
    async fn get_distinct_entries(
        &self,
        stream_name: &str,
        field: &str,
    ) -> Result<Vec<String>, QueryError> {
        let query = Query {
            query: format!("SELECT DISTINCT({field}) FOR {stream_name}"),
            start_time: self.start_time.clone(),
            end_time: self.end_time.clone(),
            send_null: false,
            filter_tags: None,
            fields: true,
        };
        let time_range = TimeRange::parse_human_time(&self.start_time, &self.end_time)?;

        let session_state = QUERY_SESSION.state();
        let query = into_query(&query, &session_state, time_range).await?;
        let (records, _) = execute(query, stream_name).await?;
        let response = record_batches_to_json(&records)?;
        // Extract field values from the JSON response
        let values = response
            .iter()
            .flat_map(|row| {
                row.get(field)
                    .and_then(|s| s.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        Ok(values)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrismLogstreamError {
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("StreamError: {0}")]
    StreamError(#[from] StreamError),
    #[error("StreamNotFound: {0}")]
    StreamNotFound(#[from] StreamNotFound),
    #[error("Hottier: {0}")]
    Hottier(#[from] HotTierError),
    #[error("Query: {0}")]
    Query(#[from] QueryError),
    #[error("TimeParse: {0}")]
    TimeParse(#[from] TimeParseError),
    #[error("Execute: {0}")]
    Execute(#[from] ExecuteError),
    #[error("Auth: {0}")]
    Auth(#[from] actix_web::Error),
}

impl actix_web::ResponseError for PrismLogstreamError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PrismLogstreamError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismLogstreamError::StreamError(e) => e.status_code(),
            PrismLogstreamError::StreamNotFound(_) => StatusCode::NOT_FOUND,
            PrismLogstreamError::Hottier(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismLogstreamError::Query(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismLogstreamError::TimeParse(_) => StatusCode::NOT_FOUND,
            PrismLogstreamError::Execute(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismLogstreamError::Auth(_) => StatusCode::UNAUTHORIZED,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
