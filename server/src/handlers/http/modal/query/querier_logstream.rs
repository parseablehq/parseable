use actix_web::{web, HttpRequest, Responder};
use bytes::Bytes;
use chrono::Utc;
use http::StatusCode;

use crate::{handlers::http::{base_path_without_preceding_slash, cluster::{self, fetch_daily_stats_from_ingestors, fetch_stats_from_ingestors, sync_streams_with_ingestors, utils::{merge_quried_stats, IngestionStats, QueriedStats, StorageStats}}, logstream::{error::StreamError, get_stats_date}, modal::utils::logstream_utils::create_update_stream}, hottier::{HotTierManager, StreamHotTier, CURRENT_HOT_TIER_VERSION}, metadata::{self, STREAM_INFO}, option::CONFIG, stats::{self, Stats}, storage::StreamType, validator};


pub async fn put_stream(req: HttpRequest, body: Bytes) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let headers = create_update_stream(&req, &body, &stream_name).await?;
    sync_streams_with_ingestors(headers, body, &stream_name).await?;


    Ok(("Log stream created", StatusCode::OK))
}

pub async fn get_stats(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
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
            let ingestor_stats =
                fetch_daily_stats_from_ingestors(&stream_name, date_value).await?;
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

    let ingestor_stats = if STREAM_INFO.stream_type(&stream_name).unwrap() == Some(StreamType::UserDefined.to_string())
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
    let ingestor_metadata = cluster::get_ingestor_info().await.map_err(|err| {
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        StreamError::from(err)
    })?;
    for ingestor in ingestor_metadata {
        let url = format!(
            "{}{}/logstream/{}/cache",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            stream_name
        );

        cluster::sync_cache_with_ingestors(&url, ingestor.clone(), *body).await?;
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

pub async fn put_stream_hot_tier(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, StreamError> {

    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

    if STREAM_INFO.stream_type(&stream_name).unwrap() == Some(StreamType::Internal.to_string()) {
        return Err(StreamError::Custom {
            msg: "Hot tier can not be updated for internal stream".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    if CONFIG.parseable.hot_tier_storage_path.is_none() {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    }

    let body = body.into_inner();
    let mut hottier: StreamHotTier = match serde_json::from_value(body) {
        Ok(hottier) => hottier,
        Err(err) => return Err(StreamError::InvalidHotTierConfig(err)),
    };

    validator::hot_tier(&hottier.size.to_string())?;

    STREAM_INFO.set_hot_tier(&stream_name, true)?;
    if let Some(hot_tier_manager) = HotTierManager::global() {
        let existing_hot_tier_used_size = hot_tier_manager
            .validate_hot_tier_size(&stream_name, &hottier.size)
            .await?;
        hottier.used_size = Some(existing_hot_tier_used_size.to_string());
        hottier.available_size = Some(hottier.size.clone());
        hottier.version = Some(CURRENT_HOT_TIER_VERSION.to_string());
        hot_tier_manager
            .put_hot_tier(&stream_name, &mut hottier)
            .await?;
        let storage = CONFIG.storage().get_object_store();
        let mut stream_metadata = storage.get_object_store_format(&stream_name).await?;
        stream_metadata.hot_tier_enabled = Some(true);
        storage
            .put_stream_manifest(&stream_name, &stream_metadata)
            .await?;
    }

    Ok((
        format!("hot tier set for stream {stream_name}"),
        StatusCode::OK,
    ))
}

pub async fn get_stream_hot_tier(req: HttpRequest) -> Result<impl Responder, StreamError> {

    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

    if CONFIG.parseable.hot_tier_storage_path.is_none() {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    }

    if let Some(hot_tier_manager) = HotTierManager::global() {
        let mut hot_tier = hot_tier_manager.get_hot_tier(&stream_name).await?;
        hot_tier.size = format!("{} {}", hot_tier.size, "Bytes");
        hot_tier.used_size = Some(format!("{} {}", hot_tier.used_size.unwrap(), "Bytes"));
        hot_tier.available_size = Some(format!("{} {}", hot_tier.available_size.unwrap(), "Bytes"));
        Ok((web::Json(hot_tier), StatusCode::OK))
    } else {
        Err(StreamError::Custom {
            msg: format!("hot tier not initialised for stream {}", stream_name),
            status: (StatusCode::BAD_REQUEST),
        })
    }
}

pub async fn delete_stream_hot_tier(req: HttpRequest) -> Result<impl Responder, StreamError> {

    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }

    if CONFIG.parseable.hot_tier_storage_path.is_none() {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    }

    if STREAM_INFO.stream_type(&stream_name).unwrap() == Some(StreamType::Internal.to_string()) {
        return Err(StreamError::Custom {
            msg: "Hot tier can not be deleted for internal stream".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }

    if let Some(hot_tier_manager) = HotTierManager::global() {
        hot_tier_manager.delete_hot_tier(&stream_name).await?;
    }
    Ok((
        format!("hot tier deleted for stream {stream_name}"),
        StatusCode::OK,
    ))
}