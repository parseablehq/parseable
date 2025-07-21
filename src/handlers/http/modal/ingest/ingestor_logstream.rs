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

use std::fs;

use actix_web::{
    HttpRequest, Responder,
    web::{Json, Path},
};
use bytes::Bytes;
use http::StatusCode;
use tracing::warn;

use crate::{
    catalog::remove_manifest_from_snapshot,
    handlers::http::logstream::error::StreamError,
    parseable::{PARSEABLE, StreamNotFound},
    stats,
};

pub async fn retention_cleanup(
    stream_name: Path<String>,
    Json(date_list): Json<Vec<String>>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let storage = PARSEABLE.storage.get_object_store();
    // if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.streams.contains(&stream_name)
        && !PARSEABLE
            .create_stream_and_schema_from_storage(&stream_name)
            .await
            .unwrap_or(false)
    {
        return Err(StreamNotFound(stream_name.clone()).into());
    }

    let res = remove_manifest_from_snapshot(storage.clone(), &stream_name, date_list).await;
    let first_event_at: Option<String> = res.unwrap_or_default();

    Ok((first_event_at, StatusCode::OK))
}

pub async fn delete(stream_name: Path<String>) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();

    // Delete from staging
    let stream_dir = PARSEABLE.get_stream(&stream_name)?;
    if let Err(err) = fs::remove_dir_all(&stream_dir.data_path) {
        warn!(
            "failed to delete local data for stream {} with error {err}. Clean {} manually",
            stream_name,
            stream_dir.data_path.to_string_lossy()
        )
    }

    // Delete from memory
    PARSEABLE.streams.delete(&stream_name);
    stats::delete_stats(&stream_name, "json")
        .unwrap_or_else(|e| warn!("failed to delete stats for stream {}: {:?}", stream_name, e));

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn put_stream(
    req: HttpRequest,
    stream_name: Path<String>,
    body: Bytes,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    PARSEABLE
        .create_update_stream(req.headers(), &body, &stream_name)
        .await?;

    Ok(("Log stream created", StatusCode::OK))
}
