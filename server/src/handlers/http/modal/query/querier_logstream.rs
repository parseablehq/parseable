use core::str;
use std::fs;

use actix_web::{web, HttpRequest, Responder};
use bytes::Bytes;
use chrono::Utc;
use http::StatusCode;
use tokio::sync::Mutex;

static CREATE_STREAM_LOCK: Mutex<()> = Mutex::const_new(());

use crate::{
    event,
    handlers::http::{
        base_path_without_preceding_slash,
        cluster::{
            self, fetch_daily_stats_from_ingestors, fetch_stats_from_ingestors,
            sync_streams_with_ingestors,
            utils::{merge_quried_stats, IngestionStats, QueriedStats, StorageStats},
        },
        logstream::{error::StreamError, get_stats_date},
        modal::utils::logstream_utils::{
            create_stream_and_schema_from_storage, create_update_stream,
        },
    },
    hottier::HotTierManager,
    metadata::{self, STREAM_INFO},
    option::CONFIG,
    stats::{self, Stats},
    storage::{StorageDir, StreamType},
};

pub async fn delete(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        create_stream_and_schema_from_storage(&stream_name).await?;
    }

    let objectstore = CONFIG.storage().get_object_store();

    objectstore.delete_stream(&stream_name).await?;
    let stream_dir = StorageDir::new(&stream_name);
    if fs::remove_dir_all(&stream_dir.data_path).is_err() {
        log::warn!(
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
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        StreamError::from(err)
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

    metadata::STREAM_INFO.delete_stream(&stream_name);
    event::STREAM_WRITERS.delete_stream(&stream_name);
    stats::delete_stats(&stream_name, "json").unwrap_or_else(|e| {
        log::warn!("failed to delete stats for stream {}: {:?}", stream_name, e)
    });

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn put_stream(req: HttpRequest, body: Bytes) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let _ = CREATE_STREAM_LOCK.lock().await;
    let headers = create_update_stream(&req, &body, &stream_name).await?;

    sync_streams_with_ingestors(headers, body, &stream_name).await?;

    Ok(("Log stream created", StatusCode::OK))
}

pub async fn get_stats(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        create_stream_and_schema_from_storage(&stream_name).await?;
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
            let ingestor_stats = fetch_daily_stats_from_ingestors(&stream_name, date_value).await?;
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
        .ok_or(StreamError::StreamNotFound(stream_name.clone()))?;

    let ingestor_stats = if STREAM_INFO.stream_type(&stream_name).unwrap()
        == Some(StreamType::UserDefined.to_string())
    {
        Some(fetch_stats_from_ingestors(&stream_name).await?)
    } else {
        None
    };

    let hash_map = STREAM_INFO.read().expect("Readable");
    let stream_meta = &hash_map
        .get(&stream_name)
        .ok_or(StreamError::StreamNotFound(stream_name.clone()))?;

    let time = Utc::now();

    let stats = match &stream_meta.first_event_at {
        Some(_) => {
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

            QueriedStats::new(&stream_name, time, ingestion_stats, storage_stats)
        }

        None => {
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

            QueriedStats::new(&stream_name, time, ingestion_stats, storage_stats)
        }
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

pub async fn put_enable_cache(
    req: HttpRequest,
    body: web::Json<bool>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let storage = CONFIG.storage().get_object_store();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

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

    let cache_enabled = STREAM_INFO.get_cache_enabled(&stream_name)?;
    Ok((web::Json(cache_enabled), StatusCode::OK))
}
