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

use actix_web::{HttpRequest, Responder};
use bytes::Bytes;
use http::StatusCode;
use tracing::warn;

use crate::{
    catalog::remove_manifest_from_snapshot,
    event,
    handlers::http::{
        logstream::error::StreamError,
        modal::utils::logstream_utils::{
            create_stream_and_schema_from_storage, create_update_stream,
        },
    },
    metadata,
    option::CONFIG,
    stats,
};

pub async fn retention_cleanup(
    req: HttpRequest,
    body: Bytes,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let storage = CONFIG.storage().get_object_store();
    // if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !metadata::STREAM_INFO.stream_exists(&stream_name)
        && !create_stream_and_schema_from_storage(&stream_name)
            .await
            .unwrap_or(false)
    {
        return Err(StreamError::StreamNotFound(stream_name.clone()));
    }

    let date_list: Vec<String> = serde_json::from_slice(&body).unwrap();
    let res = remove_manifest_from_snapshot(storage.clone(), &stream_name, date_list).await;
    let first_event_at: Option<String> = res.unwrap_or_default();

    Ok((first_event_at, StatusCode::OK))
}

pub async fn delete(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    // if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !metadata::STREAM_INFO.stream_exists(&stream_name)
        && !create_stream_and_schema_from_storage(&stream_name)
            .await
            .unwrap_or(false)
    {
        return Err(StreamError::StreamNotFound(stream_name.clone()));
    }

    metadata::STREAM_INFO.delete_stream(&stream_name);
    event::STREAM_WRITERS.delete_stream(&stream_name);
    stats::delete_stats(&stream_name, "json")
        .unwrap_or_else(|e| warn!("failed to delete stats for stream {}: {:?}", stream_name, e));

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn put_stream(req: HttpRequest, body: Bytes) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    create_update_stream(&req, &body, &stream_name).await?;

    Ok(("Log stream created", StatusCode::OK))
}
