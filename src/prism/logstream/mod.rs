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
use http::StatusCode;
use serde::Serialize;

use crate::{
    handlers::http::{logstream::error::StreamError, query::update_schema_when_distributed},
    parseable::{StreamNotFound, PARSEABLE},
    storage::StreamInfo,
    LOCK_EXPECT,
};

#[derive(Serialize)]
pub struct PrismLogstreamInfo {
    info: StreamInfo,
    schema: Arc<Schema>,
}

pub async fn get_prism_logstream_info(
    stream_name: &str,
) -> Result<PrismLogstreamInfo, PrismLogstreamError> {
    // get StreamInfo
    let info = get_stream_info_helper(stream_name).await?;
    // get stream schema
    let schema = get_stream_schema_helper(stream_name).await?;

    Ok(PrismLogstreamInfo { info, schema })
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
}

impl actix_web::ResponseError for PrismLogstreamError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PrismLogstreamError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismLogstreamError::StreamError(e) => e.status_code(),
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
