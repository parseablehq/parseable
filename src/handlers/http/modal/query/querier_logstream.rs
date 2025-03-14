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

use core::str;
use std::fs;

use actix_web::{
    web::{self, Path},
    HttpRequest, Responder,
};
use bytes::Bytes;
use chrono::Utc;
use http::StatusCode;
use relative_path::RelativePathBuf;
use tokio::sync::Mutex;
use tracing::{error, warn};

static CREATE_STREAM_LOCK: Mutex<()> = Mutex::const_new(());

use crate::{
    handlers::http::{
        base_path_without_preceding_slash,
        cluster::{
            self, fetch_daily_stats_from_ingestors, fetch_stats_from_ingestors,
            sync_streams_with_ingestors,
            utils::{merge_quried_stats, IngestionStats, QueriedStats, StorageStats},
        },
        logstream::{error::StreamError, get_stats_date},
    },
    hottier::HotTierManager,
    parseable::{StreamNotFound, PARSEABLE},
    stats::{self, Stats},
    storage::{ObjectStoreFormat, StreamType, STREAM_ROOT_DIRECTORY},
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

pub async fn get_stats(
    req: HttpRequest,
    stream_name: Path<String>,
) -> Result<impl Responder, StreamError> {
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

    let query_string = req.query_string();
    if !query_string.is_empty() {
        let date_key = query_string.split('=').collect::<Vec<&str>>()[0];
        let date_value = query_string.split('=').collect::<Vec<&str>>()[1];
        if date_key != "date" {
            return Err(StreamError::Custom {
                msg: "Invalid query parameter".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }

        if !date_value.is_empty() {
            let querier_stats = get_stats_date(&stream_name, date_value).await?;

            // this function requires all the ingestor stream jsons
            let path = RelativePathBuf::from_iter([&stream_name, STREAM_ROOT_DIRECTORY]);
            let obs = PARSEABLE
                .storage
                .get_object_store()
                .get_objects(
                    Some(&path),
                    Box::new(|file_name| {
                        file_name.starts_with(".ingestor") && file_name.ends_with("stream.json")
                    }),
                )
                .await?;

            let mut ingestor_stream_jsons = Vec::new();
            for ob in obs {
                let stream_metadata: ObjectStoreFormat = match serde_json::from_slice(&ob) {
                    Ok(d) => d,
                    Err(e) => {
                        error!("Failed to parse stream metadata: {:?}", e);
                        continue;
                    }
                };
                ingestor_stream_jsons.push(stream_metadata);
            }

            let ingestor_stats =
                fetch_daily_stats_from_ingestors(date_value, &ingestor_stream_jsons)?;

            let total_stats = Stats {
                events: querier_stats.events + ingestor_stats.events,
                ingestion: querier_stats.ingestion + ingestor_stats.ingestion,
                storage: querier_stats.storage + ingestor_stats.storage,
            };
            let stats = serde_json::to_value(total_stats)?;

            return Ok((web::Json(stats), StatusCode::OK));
        }
    }

    let stats = stats::get_current_stats(&stream_name, "json")
        .ok_or_else(|| StreamNotFound(stream_name.clone()))?;

    let ingestor_stats = if PARSEABLE
        .get_stream(&stream_name)
        .is_ok_and(|stream| stream.get_stream_type() == StreamType::UserDefined)
    {
        Some(fetch_stats_from_ingestors(&stream_name).await?)
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

        QueriedStats::new(&stream_name, time, ingestion_stats, storage_stats)
    };

    let stats = if let Some(mut ingestor_stats) = ingestor_stats {
        ingestor_stats.push(stats);
        merge_quried_stats(ingestor_stats)
    } else {
        stats
    };

    let stats = serde_json::to_value(stats)?;

    Ok((web::Json(stats), StatusCode::OK))
}
