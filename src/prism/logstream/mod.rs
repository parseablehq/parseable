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
use chrono::{TimeDelta, Utc};
use http::StatusCode;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::warn;

use crate::{
    LOCK_EXPECT,
    alerts::alert_structs::{ConditionConfig, Conditions},
    event::DEFAULT_TIMESTAMP_KEY,
    handlers::http::{
        cluster::{
            fetch_stats_from_ingestors,
            utils::{IngestionStats, QueriedStats, StorageStats, merge_queried_stats},
        },
        logstream::error::StreamError,
        query::{Query, QueryError, get_records_and_fields, update_schema_when_distributed},
    },
    hottier::{HotTierError, HotTierManager, StreamHotTier},
    parseable::{PARSEABLE, StreamNotFound},
    query::{CountConditions, CountsRequest, CountsResponse, error::ExecuteError},
    rbac::{Users, map::SessionKey, role::Action},
    stats,
    storage::{StreamInfo, StreamType, retention::Retention},
    utils::{
        arrow::record_batches_to_json,
        time::{TimeParseError, truncate_to_minute},
    },
    validator::error::HotTierValidationError,
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
    let (info, schema, stats) = tokio::join!(
        get_stream_info_helper(stream_name),
        get_stream_schema_helper(stream_name),
        get_stats(stream_name),
    );

    let info = info?;
    let schema = schema?;
    let stats = stats?;

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
    if !PARSEABLE.check_or_load_stream(stream_name).await {
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
        merge_queried_stats(ingestor_stats)
    } else {
        stats
    };

    Ok(stats)
}

async fn get_stream_info_helper(stream_name: &str) -> Result<StreamInfo, StreamError> {
    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(stream_name).await {
        return Err(StreamNotFound(stream_name.to_owned()).into());
    }

    let storage = PARSEABLE.storage().get_object_store();

    // Get first and latest event timestamps from storage
    let (stream_first_event_at, stream_latest_event_at) = match storage
        .get_first_and_latest_event_from_storage(stream_name)
        .await
    {
        Ok(result) => result,
        Err(err) => {
            warn!(
                "failed to fetch first/latest event timestamps from storage for stream {}: {}",
                stream_name, err
            );
            (None, None)
        }
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
        latest_event_at: stream_latest_event_at,
        time_partition: stream_meta.time_partition.clone(),
        time_partition_limit: stream_meta
            .time_partition_limit
            .map(|limit| limit.to_string()),
        custom_partition: stream_meta.custom_partition.clone(),
        static_schema_flag: stream_meta.static_schema_flag,
        log_source: stream_meta.log_source.clone(),
        telemetry_type: stream_meta.telemetry_type,
    };

    Ok(stream_info)
}

/// Response structure for Prism dataset queries.
/// Contains information about a stream, its statistics, retention policy,
/// and query results.
#[derive(Serialize)]
pub struct PrismDatasetResponse {
    /// Name of the stream
    stream: String,
    /// Basic information about the stream
    info: StreamInfo,
    /// Schema of the stream
    schema: Arc<Schema>,
    /// Statistics for the queried timeframe
    stats: QueriedStats,
    /// Retention policy details
    retention: Retention,
    /// Hot tier information if available
    hottier: Option<StreamHotTier>,
    /// Count of records in the specified time range
    counts: CountsResponse,
}

/// Request parameters for retrieving Prism dataset information.
/// Defines which streams to query
#[derive(Deserialize, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrismDatasetRequest {
    /// List of stream names to query
    #[serde(default)]
    streams: Vec<String>,
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

        // Process streams concurrently
        let results = futures::future::join_all(
            self.streams
                .iter()
                .map(|stream| self.process_stream(stream.clone(), key.clone())),
        )
        .await;

        // Collect successful responses and handle errors
        let mut responses = Vec::new();
        for result in results {
            match result {
                Ok(Some(response)) => responses.push(response),
                Ok(None) => {
                    warn!("Stream not found or unauthorized access");
                    continue;
                }
                Err(err) => {
                    warn!("error: {err}");
                    continue;
                }
            }
        }

        Ok(responses)
    }

    async fn process_stream(
        &self,
        stream: String,
        key: SessionKey,
    ) -> Result<Option<PrismDatasetResponse>, PrismLogstreamError> {
        // Skip unauthorized streams
        if !self.is_authorized(&stream, &key) {
            return Ok(None);
        }

        // Skip streams that don't exist
        if !PARSEABLE.check_or_load_stream(&stream).await {
            return Ok(None);
        }

        // Process stream data
        match get_prism_logstream_info(&stream).await {
            Ok(info) => Ok(Some(self.build_dataset_response(stream, info, &key).await?)),
            Err(err) => Err(err),
        }
    }

    fn is_authorized(&self, stream: &str, key: &SessionKey) -> bool {
        if Users.authorize(key.clone(), Action::ListStream, Some(stream), None)
            != crate::rbac::Response::Authorized
        {
            warn!("Unauthorized access requested for stream: {stream}");
            false
        } else {
            true
        }
    }

    async fn build_dataset_response(
        &self,
        stream: String,
        info: PrismLogstreamInfo,
        key: &SessionKey,
    ) -> Result<PrismDatasetResponse, PrismLogstreamError> {
        // Get hot tier info
        let hottier = self.get_hot_tier_info(&stream).await?;

        // Get counts
        let counts = self.get_counts(&stream, key).await?;

        Ok(PrismDatasetResponse {
            stream,
            info: info.info,
            schema: info.schema,
            stats: info.stats,
            retention: info.retention,
            hottier,
            counts,
        })
    }

    async fn get_hot_tier_info(
        &self,
        stream: &str,
    ) -> Result<Option<StreamHotTier>, PrismLogstreamError> {
        match HotTierManager::global() {
            Some(manager) => match manager.get_hot_tier(stream).await {
                Ok(stats) => Ok(Some(stats)),
                Err(HotTierError::HotTierValidationError(HotTierValidationError::NotFound(_))) => {
                    Ok(None)
                }
                Err(err) => Err(err.into()),
            },
            None => Ok(None),
        }
    }

    async fn get_counts(
        &self,
        stream: &str,
        key: &SessionKey,
    ) -> Result<CountsResponse, PrismLogstreamError> {
        let end = truncate_to_minute(Utc::now());
        let start = end - TimeDelta::hours(1);

        let conditions = if PARSEABLE.get_stream(stream)?.get_time_partition().is_some() {
            Some(CountConditions {
                conditions: Some(Conditions {
                    operator: Some(crate::alerts::LogicalOperator::And),
                    condition_config: vec![
                        ConditionConfig {
                            column: DEFAULT_TIMESTAMP_KEY.into(),
                            operator: crate::alerts::WhereConfigOperator::GreaterThanOrEqual,
                            value: Some(start.to_rfc3339()),
                        },
                        ConditionConfig {
                            column: DEFAULT_TIMESTAMP_KEY.into(),
                            operator: crate::alerts::WhereConfigOperator::LessThan,
                            value: Some(end.to_rfc3339()),
                        },
                    ],
                }),
                group_by: None,
            })
        } else {
            None
        };

        let count_request = CountsRequest {
            stream: stream.to_owned(),
            start_time: start.to_rfc3339(),
            end_time: end.to_rfc3339(),
            num_bins: 10,
            conditions,
        };

        if count_request.conditions.is_some() {
            // forward request to querier
            let query = count_request
                .get_df_sql(DEFAULT_TIMESTAMP_KEY.into())
                .await?;

            let query_request = Query {
                query,
                start_time: start.to_rfc3339(),
                end_time: end.to_rfc3339(),
                send_null: true,
                fields: true,
                streaming: false,
                filter_tags: None,
            };

            let (records, _) = get_records_and_fields(&query_request, key).await?;
            if let Some(records) = records {
                let json_records = record_batches_to_json(&records)?;
                let records = json_records.into_iter().map(Value::Object).collect_vec();

                let res = json!({
                    "fields": vec!["start_time", "end_time", "count"],
                    "records": records,
                });

                Ok(serde_json::from_value(res)?)
            } else {
                Err(PrismLogstreamError::Anyhow(anyhow::Error::msg(
                    "No data returned for counts SQL",
                )))
            }
        } else {
            let records = count_request.get_bin_density().await?;
            Ok(CountsResponse {
                fields: vec!["start_time".into(), "end_time".into(), "count".into()],
                records,
            })
        }
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
    #[error("SerdeError: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("ReqwestError: {0}")]
    ReqwestError(#[from] reqwest::Error),
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
            PrismLogstreamError::SerdeError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismLogstreamError::ReqwestError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismLogstreamError::Auth(_) => StatusCode::UNAUTHORIZED,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
