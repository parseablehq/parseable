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

use std::sync::Arc;

use actix_web::http::StatusCode;
use actix_web::http::header::ContentType;
use arrow_schema::Schema;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    handlers::http::{
        cluster::{
            fetch_stats_from_ingestors,
            utils::{IngestionStats, QueriedStats, StorageStats, merge_queried_stats},
        },
        logstream::error::StreamError,
        query::{QueryError, update_schema_when_distributed},
    },
    hottier::HotTierError,
    parseable::{PARSEABLE, StreamNotFound},
    query::{CountsRequest, CountsResponse, error::ExecuteError},
    rbac::{Users, map::SessionKey, role::Action},
    stats,
    storage::{StreamInfo, StreamType, retention::Retention},
    utils::{get_tenant_id_from_key, time::TimeParseError},
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
    tenant_id: &Option<String>,
) -> Result<PrismLogstreamInfo, PrismLogstreamError> {
    let (info, schema, stats) = tokio::join!(
        get_stream_info_helper(stream_name, tenant_id),
        get_stream_schema_helper(stream_name, tenant_id),
        get_stats(stream_name, tenant_id),
    );
    let info = info?;
    let schema = schema?;
    let stats = stats?;

    // get retention
    let retention = PARSEABLE
        .get_stream(stream_name, tenant_id)?
        .get_retention()
        .unwrap_or_default();

    Ok(PrismLogstreamInfo {
        info,
        schema,
        stats,
        retention,
    })
}

async fn get_stream_schema_helper(
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<Arc<Schema>, StreamError> {
    // Ensure parseable is aware of stream in distributed mode
    if !PARSEABLE.check_or_load_stream(stream_name, tenant_id).await {
        return Err(StreamNotFound(stream_name.to_owned()).into());
    }

    let stream = PARSEABLE.get_stream(stream_name, tenant_id)?;
    match update_schema_when_distributed(&vec![stream_name.to_owned()], tenant_id).await {
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

async fn get_stats(
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<QueriedStats, PrismLogstreamError> {
    let stats = stats::get_current_stats(stream_name, "json", tenant_id)
        .ok_or_else(|| StreamNotFound(stream_name.to_owned()))?;

    let ingestor_stats = if PARSEABLE
        .get_stream(stream_name, tenant_id)
        .is_ok_and(|stream| stream.get_stream_type() == StreamType::UserDefined)
    {
        Some(fetch_stats_from_ingestors(stream_name, tenant_id).await?)
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
        merge_queried_stats(ingestor_stats)?
    } else {
        stats
    };

    Ok(stats)
}

pub async fn get_stream_info_helper(
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<StreamInfo, StreamError> {
    // For query mode, if the stream not found in memory map,
    // check if it exists in the storage
    // create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(stream_name, tenant_id).await {
        return Err(StreamNotFound(stream_name.to_owned()).into());
    }

    let storage = PARSEABLE.storage().get_object_store();

    // Get first and latest event timestamps from storage
    let (stream_first_event_at, stream_latest_event_at) = match storage
        .get_first_and_latest_event_from_storage(stream_name, tenant_id)
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

    let stream = PARSEABLE.get_stream(stream_name, tenant_id)?;
    let stream_meta = stream.metadata.read().unwrap();

    let stream_info =
        StreamInfo::from_metadata(&stream_meta, stream_first_event_at, stream_latest_event_at);

    Ok(stream_info)
}

/// Response structure for Prism dataset queries.
/// Contains information about a stream, its statistics, retention policy,
/// and query results.
#[derive(Serialize, Debug)]
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
        let tenant_id = get_tenant_id_from_key(&key);
        if self.streams.is_empty() {
            self.streams = PARSEABLE.streams.list(&tenant_id);
        }

        // Process streams concurrently
        let results = futures::future::join_all(
            self.streams
                .iter()
                .map(|stream| self.process_stream(stream.clone(), key.clone(), &tenant_id)),
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
        tenant_id: &Option<String>,
    ) -> Result<Option<PrismDatasetResponse>, PrismLogstreamError> {
        // Skip unauthorized streams
        if !self.is_authorized(&stream, &key) {
            return Ok(None);
        }

        // Skip streams that don't exist
        if !PARSEABLE.check_or_load_stream(&stream, tenant_id).await {
            return Ok(None);
        }

        // exclude internal streams
        let is_internal = PARSEABLE
            .get_stream(&stream, tenant_id)
            .is_ok_and(|stream| {
                stream
                    .get_stream_type()
                    .eq(&crate::storage::StreamType::Internal)
            });
        if is_internal {
            return Ok(None);
        }

        // Process stream data
        match get_prism_logstream_info(&stream, tenant_id).await {
            Ok(info) => Ok(Some(
                self.build_dataset_response(stream, info, tenant_id).await?,
            )),
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
        tenant_id: &Option<String>,
    ) -> Result<PrismDatasetResponse, PrismLogstreamError> {
        // Get counts
        let counts = self.get_counts(&stream, tenant_id).await?;

        let res = PrismDatasetResponse {
            stream,
            info: info.info,
            schema: info.schema,
            stats: info.stats,
            retention: info.retention,
            counts,
        };

        Ok(res)
    }

    async fn get_counts(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<CountsResponse, PrismLogstreamError> {
        let count_request = CountsRequest {
            stream: stream.to_owned(),
            start_time: "1h".to_owned(),
            end_time: "now".to_owned(),
            num_bins: Some(10),
            conditions: None,
        };

        let records = count_request.get_bin_density(tenant_id).await?;
        Ok(CountsResponse {
            fields: vec!["start_time".into(), "end_time".into(), "count".into()],
            records,
        })
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
    fn status_code(&self) -> StatusCode {
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
