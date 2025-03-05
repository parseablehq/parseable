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
use serde::Serialize;

use crate::{
    handlers::http::{
        cluster::utils::{merge_quried_stats, IngestionStats, QueriedStats, StorageStats},
        logstream::error::StreamError,
    },
    parseable::{StreamNotFound, PARSEABLE},
    stats,
    storage::{retention::Retention, StreamInfo},
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
    match PARSEABLE
        .update_schema_when_distributed(&vec![stream_name.to_owned()])
        .await
    {
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

    let ingestor_stats: Option<Vec<QueriedStats>> = None;

    let time = Utc::now();

    let stats = {
        let ingestion_stats = IngestionStats::new(
            stats.current_stats.events,
            format!("{} {}", stats.current_stats.ingestion, "Bytes"),
            stats.lifetime_stats.events,
            format!("{} {}", stats.lifetime_stats.ingestion, "Bytes"),
            stats.deleted_stats.events,
            format!("{} {}", stats.deleted_stats.ingestion, "Bytes"),
            "json",
        );
        let storage_stats = StorageStats::new(
            format!("{} {}", stats.current_stats.storage, "Bytes"),
            format!("{} {}", stats.lifetime_stats.storage, "Bytes"),
            format!("{} {}", stats.deleted_stats.storage, "Bytes"),
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

#[derive(Debug, thiserror::Error)]
pub enum PrismLogstreamError {
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("StreamError: {0}")]
    StreamError(#[from] StreamError),
    #[error("StreamNotFound: {0}")]
    StreamNotFound(#[from] StreamNotFound),
}

impl actix_web::ResponseError for PrismLogstreamError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PrismLogstreamError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismLogstreamError::StreamError(e) => e.status_code(),
            PrismLogstreamError::StreamNotFound(_) => StatusCode::NOT_FOUND,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
