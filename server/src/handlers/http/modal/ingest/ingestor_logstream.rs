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

use actix_web::{web, HttpRequest, Responder};
use bytes::Bytes;
use http::StatusCode;
use itertools::Itertools;

use crate::{
    catalog::remove_manifest_from_snapshot,
    event,
    handlers::http::{
        logstream::error::StreamError,
        modal::utils::logstream_utils::{
            create_stream_and_schema_from_storage, create_update_stream,
        },
    },
    metadata::{self, STREAM_INFO},
    option::CONFIG,
    stats,
    storage::LogStream,
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
    stats::delete_stats(&stream_name, "json").unwrap_or_else(|e| {
        log::warn!("failed to delete stats for stream {}: {:?}", stream_name, e)
    });

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn put_stream(req: HttpRequest, body: Bytes) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    create_update_stream(&req, &body, &stream_name).await?;

    Ok(("Log stream created", StatusCode::OK))
}

pub async fn put_enable_cache(
    req: HttpRequest,
    body: web::Json<bool>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let storage = CONFIG.storage().get_object_store();

    if CONFIG.parseable.local_cache_path.is_none() {
        return Err(StreamError::CacheNotEnabled(stream_name));
    }
    // here the ingest server has not found the stream
    // so it should check if the stream exists in storage
    let check = storage
        .list_streams()
        .await?
        .iter()
        .map(|stream| stream.name.clone())
        .contains(&stream_name);

    if !check {
        log::error!("Stream {} not found", stream_name.clone());
        return Err(StreamError::StreamNotFound(stream_name.clone()));
    }
    metadata::STREAM_INFO
        .upsert_stream_info(
            &*storage,
            LogStream {
                name: stream_name.clone().to_owned(),
            },
        )
        .await
        .map_err(|_| StreamError::StreamNotFound(stream_name.clone()))?;

    let enable_cache = body.into_inner();
    let mut stream_metadata = storage.get_object_store_format(&stream_name).await?;
    stream_metadata.cache_enabled = enable_cache;
    storage
        .put_stream_manifest(&stream_name, &stream_metadata)
        .await?;

    STREAM_INFO.set_cache_enabled(&stream_name, enable_cache)?;
    Ok((
        format!("Cache set to {enable_cache} for log stream {stream_name}"),
        StatusCode::OK,
    ))
}

pub async fn get_cache_enabled(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if CONFIG.parseable.local_cache_path.is_none() {
        return Err(StreamError::CacheNotEnabled(stream_name));
    }

    let cache_enabled = STREAM_INFO.get_cache_enabled(&stream_name)?;
    Ok((web::Json(cache_enabled), StatusCode::OK))
}
