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

use actix_web::{web::Path, HttpRequest, Responder};
use bytes::Bytes;
use http::StatusCode;
use tokio::sync::Mutex;
use tracing::{error, warn};

static CREATE_STREAM_LOCK: Mutex<()> = Mutex::const_new(());

use crate::{
    handlers::http::{
        base_path_without_preceding_slash,
        cluster::{self, sync_streams_with_ingestors},
        logstream::error::StreamError,
    },
    hottier::HotTierManager,
    parseable::{StreamNotFound, PARSEABLE},
    stats,
};

pub async fn delete(stream_name: Path<String>) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();

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

    let objectstore = PARSEABLE.storage.get_object_store();
    // Delete from storage
    objectstore.delete_stream(&stream_name).await?;
    let stream_dir = PARSEABLE.get_or_create_stream(&stream_name);
    if fs::remove_dir_all(&stream_dir.data_path).is_err() {
        warn!(
            "failed to delete local data for stream {}. Clean {} manually",
            stream_name,
            stream_dir.data_path.to_string_lossy()
        )
    }

    if let Some(hot_tier_manager) = HotTierManager::global() {
        if hot_tier_manager.check_stream_hot_tier_exists(&stream_name) {
            hot_tier_manager.delete_hot_tier(&stream_name).await?;
        }
    }

    let ingestor_metadata = cluster::get_ingestor_info().await.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        err
    })?;

    for ingestor in ingestor_metadata {
        let url = format!(
            "{}{}/logstream/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            stream_name
        );

        // delete the stream
        cluster::send_stream_delete_request(&url, ingestor.clone()).await?;
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
    let _ = CREATE_STREAM_LOCK.lock().await;
    let headers = PARSEABLE
        .create_update_stream(req.headers(), &body, &stream_name)
        .await?;

    sync_streams_with_ingestors(headers, body, &stream_name).await?;

    Ok(("Log stream created", StatusCode::OK))
}
