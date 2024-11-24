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

use self::error::{CreateStreamError, StreamError};
use super::cluster::utils::{merge_quried_stats, IngestionStats, QueriedStats, StorageStats};
use super::cluster::{sync_streams_with_ingestors, INTERNAL_STREAM_NAME};
use super::ingest::create_stream_if_not_exists;
use super::modal::utils::logstream_utils::{
    create_stream_and_schema_from_storage, create_update_stream,
};
use crate::alerts::Alerts;
use crate::event::format::update_data_type_to_datetime;
use crate::handlers::STREAM_TYPE_KEY;
use crate::hottier::{HotTierManager, StreamHotTier, CURRENT_HOT_TIER_VERSION};
use crate::metadata::STREAM_INFO;
use crate::metrics::{EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE_DATE, EVENTS_STORAGE_SIZE_DATE};
use crate::option::{Mode, CONFIG};
use crate::stats::{event_labels_date, storage_size_labels_date, Stats};
use crate::storage::StreamType;
use crate::storage::{retention::Retention, StorageDir, StreamInfo};
use crate::{catalog, event, stats};

use crate::{metadata, validator};
use actix_web::http::header::{self, HeaderMap};
use actix_web::http::StatusCode;
use actix_web::{web, HttpRequest, Responder};
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_schema::{Field, Schema};
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderName, HeaderValue};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;

pub async fn delete(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
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

    metadata::STREAM_INFO.delete_stream(&stream_name);
    event::STREAM_WRITERS.delete_stream(&stream_name);
    stats::delete_stats(&stream_name, "json").unwrap_or_else(|e| {
        log::warn!("failed to delete stats for stream {}: {:?}", stream_name, e)
    });

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn list(_: HttpRequest) -> impl Responder {
    let res = CONFIG
        .storage()
        .get_object_store()
        .list_streams()
        .await
        .unwrap();

    web::Json(res)
}

pub async fn detect_schema(body: Bytes) -> Result<impl Responder, StreamError> {
    let body_val: Value = serde_json::from_slice(&body)?;
    let log_records: Vec<Value> = match body_val {
        Value::Array(arr) => arr,
        value @ Value::Object(_) => vec![value],
        _ => {
            return Err(StreamError::Custom {
                msg: "please send json events as part of the request".to_string(),
                status: StatusCode::BAD_REQUEST,
            })
        }
    };

    let mut schema = infer_json_schema_from_iterator(log_records.iter().map(Ok)).unwrap();
    for log_record in log_records {
        schema = update_data_type_to_datetime(schema, log_record, Vec::new());
    }
    Ok((web::Json(schema), StatusCode::OK))
}

pub async fn schema(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let schema = STREAM_INFO.schema(&stream_name)?;
    Ok((web::Json(schema), StatusCode::OK))
}

pub async fn get_alert(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let alerts = metadata::STREAM_INFO
        .read()
        .expect(metadata::LOCK_EXPECT)
        .get(&stream_name)
        .map(|metadata| {
            serde_json::to_value(&metadata.alerts).expect("alerts can serialize to valid json")
        });

    let mut alerts = match alerts {
        Some(alerts) => alerts,
        None => {
            let alerts = CONFIG
                .storage()
                .get_object_store()
                .get_alerts(&stream_name)
                .await?;

            if alerts.alerts.is_empty() {
                return Err(StreamError::NoAlertsSet);
            }

            serde_json::to_value(alerts).expect("alerts can serialize to valid json")
        }
    };

    remove_id_from_alerts(&mut alerts);

    Ok((web::Json(alerts), StatusCode::OK))
}

pub async fn put_stream(req: HttpRequest, body: Bytes) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    create_update_stream(&req, &body, &stream_name).await?;

    Ok(("Log stream created", StatusCode::OK))
}

pub async fn put_alert(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let mut body = body.into_inner();
    remove_id_from_alerts(&mut body);

    let alerts: Alerts = match serde_json::from_value(body) {
        Ok(alerts) => alerts,
        Err(err) => {
            return Err(StreamError::BadAlertJson {
                stream: stream_name,
                err,
            })
        }
    };

    validator::alert(&alerts)?;

    if !STREAM_INFO.stream_initialized(&stream_name)? {
        if CONFIG.parseable.mode == Mode::Query {
            create_stream_and_schema_from_storage(&stream_name).await?;
        } else {
            return Err(StreamError::UninitializedLogstream);
        }
    }

    let schema = STREAM_INFO.schema(&stream_name)?;
    for alert in &alerts.alerts {
        for column in alert.message.extract_column_names() {
            let is_valid = alert.message.valid(&schema, column);
            if !is_valid {
                return Err(StreamError::InvalidAlertMessage(
                    alert.name.to_owned(),
                    column.to_string(),
                ));
            }
            if !alert.rule.valid_for_schema(&schema) {
                return Err(StreamError::InvalidAlert(alert.name.to_owned()));
            }
        }
    }

    CONFIG
        .storage()
        .get_object_store()
        .put_alerts(&stream_name, &alerts)
        .await?;

    metadata::STREAM_INFO
        .set_alert(&stream_name, alerts)
        .expect("alerts set on existing stream");

    Ok((
        format!("set alert configuration for log stream {stream_name}"),
        StatusCode::OK,
    ))
}

pub async fn get_retention(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !STREAM_INFO.stream_exists(&stream_name) {
        if CONFIG.parseable.mode == Mode::Query {
            create_stream_and_schema_from_storage(&stream_name).await?;
        } else {
            return Err(StreamError::StreamNotFound(stream_name));
        }
    }
    let retention = STREAM_INFO.get_retention(&stream_name);

    match retention {
        Ok(retention) => {
            if let Some(retention) = retention {
                Ok((web::Json(retention), StatusCode::OK))
            } else {
                Ok((web::Json(Retention::default()), StatusCode::OK))
            }
        }
        Err(err) => Err(StreamError::from(err)),
    }
}

pub async fn put_retention(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let body = body.into_inner();

    let retention: Retention = match serde_json::from_value(body) {
        Ok(retention) => retention,
        Err(err) => return Err(StreamError::InvalidRetentionConfig(err)),
    };

    CONFIG
        .storage()
        .get_object_store()
        .put_retention(&stream_name, &retention)
        .await?;

    metadata::STREAM_INFO
        .set_retention(&stream_name, retention)
        .expect("retention set on existing stream");

    Ok((
        format!("set retention configuration for log stream {stream_name}"),
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

pub async fn put_enable_cache(
    req: HttpRequest,
    body: web::Json<bool>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let storage = CONFIG.storage().get_object_store();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        return Err(StreamError::StreamNotFound(stream_name));
    }
    if CONFIG.parseable.local_cache_path.is_none() {
        return Err(StreamError::CacheNotEnabled(stream_name));
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

pub async fn get_stats_date(stream_name: &str, date: &str) -> Result<Stats, StreamError> {
    let event_labels = event_labels_date(stream_name, "json", date);
    let storage_size_labels = storage_size_labels_date(stream_name, date);
    let events_ingested = EVENTS_INGESTED_DATE
        .get_metric_with_label_values(&event_labels)
        .unwrap()
        .get() as u64;
    let ingestion_size = EVENTS_INGESTED_SIZE_DATE
        .get_metric_with_label_values(&event_labels)
        .unwrap()
        .get() as u64;
    let storage_size = EVENTS_STORAGE_SIZE_DATE
        .get_metric_with_label_values(&storage_size_labels)
        .unwrap()
        .get() as u64;

    let stats = Stats {
        events: events_ingested,
        ingestion: ingestion_size,
        storage: storage_size,
    };
    Ok(stats)
}

pub async fn get_stats(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        if CONFIG.parseable.mode == Mode::Query {
            create_stream_and_schema_from_storage(&stream_name).await?;
        } else {
            return Err(StreamError::StreamNotFound(stream_name));
        }
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
            let stats = get_stats_date(&stream_name, date_value).await?;
            let stats = serde_json::to_value(stats)?;
            return Ok((web::Json(stats), StatusCode::OK));
        }
    }

    let stats = stats::get_current_stats(&stream_name, "json")
        .ok_or(StreamError::StreamNotFound(stream_name.clone()))?;

    let ingestor_stats: Option<Vec<QueriedStats>> = None;

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

// Check if the first_event_at is empty
#[allow(dead_code)]
pub fn first_event_at_empty(stream_name: &str) -> bool {
    let hash_map = STREAM_INFO.read().unwrap();
    if let Some(stream_info) = hash_map.get(stream_name) {
        if let Some(first_event_at) = &stream_info.first_event_at {
            return first_event_at.is_empty();
        }
    }
    true
}

fn remove_id_from_alerts(value: &mut Value) {
    if let Some(Value::Array(alerts)) = value.get_mut("alerts") {
        alerts
            .iter_mut()
            .map_while(|alert| alert.as_object_mut())
            .for_each(|map| {
                map.remove("id");
            });
    }
}

pub async fn create_stream(
    stream_name: String,
    time_partition: &str,
    time_partition_limit: &str,
    custom_partition: &str,
    static_schema_flag: &str,
    schema: Arc<Schema>,
    stream_type: &str,
) -> Result<(), CreateStreamError> {
    // fail to proceed if invalid stream name
    if stream_type != StreamType::Internal.to_string() {
        validator::stream_name(&stream_name, stream_type)?;
    }
    // Proceed to create log stream if it doesn't exist
    let storage = CONFIG.storage().get_object_store();

    match storage
        .create_stream(
            &stream_name,
            time_partition,
            time_partition_limit,
            custom_partition,
            static_schema_flag,
            schema.clone(),
            stream_type,
        )
        .await
    {
        Ok(created_at) => {
            let mut static_schema: HashMap<String, Arc<Field>> = HashMap::new();

            for (field_name, field) in schema
                .fields()
                .iter()
                .map(|field| (field.name().to_string(), field.clone()))
            {
                static_schema.insert(field_name, field);
            }

            metadata::STREAM_INFO.add_stream(
                stream_name.to_string(),
                created_at,
                time_partition.to_string(),
                time_partition_limit.to_string(),
                custom_partition.to_string(),
                static_schema_flag.to_string(),
                static_schema,
                stream_type,
            );
        }
        Err(err) => {
            return Err(CreateStreamError::Storage { stream_name, err });
        }
    }
    Ok(())
}

pub async fn get_stream_info(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        if CONFIG.parseable.mode == Mode::Query {
            create_stream_and_schema_from_storage(&stream_name).await?;
        } else {
            return Err(StreamError::StreamNotFound(stream_name));
        }
    }

    let store = CONFIG.storage().get_object_store();
    let dates: Vec<String> = Vec::new();
    if let Ok(Some(first_event_at)) = catalog::get_first_event(store, &stream_name, dates).await {
        if let Err(err) =
            metadata::STREAM_INFO.set_first_event_at(&stream_name, Some(first_event_at))
        {
            log::error!(
                "Failed to update first_event_at in streaminfo for stream {:?} {err:?}",
                stream_name
            );
        }
    }

    let hash_map = STREAM_INFO.read().unwrap();
    let stream_meta = &hash_map
        .get(&stream_name)
        .ok_or(StreamError::StreamNotFound(stream_name.clone()))?;

    let stream_info: StreamInfo = StreamInfo {
        stream_type: stream_meta.stream_type.clone(),
        created_at: stream_meta.created_at.clone(),
        first_event_at: stream_meta.first_event_at.clone(),
        time_partition: stream_meta.time_partition.clone(),
        time_partition_limit: stream_meta.time_partition_limit.clone(),
        custom_partition: stream_meta.custom_partition.clone(),
        cache_enabled: stream_meta.cache_enabled,
        static_schema_flag: stream_meta.static_schema_flag.clone(),
    };

    // get the other info from

    Ok((web::Json(stream_info), StatusCode::OK))
}

pub async fn put_stream_hot_tier(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if !metadata::STREAM_INFO.stream_exists(&stream_name) {
        if CONFIG.parseable.mode == Mode::Query {
            create_stream_and_schema_from_storage(&stream_name).await?;
        } else {
            return Err(StreamError::StreamNotFound(stream_name));
        }
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
        if CONFIG.parseable.mode == Mode::Query {
            create_stream_and_schema_from_storage(&stream_name).await?;
        } else {
            return Err(StreamError::StreamNotFound(stream_name));
        }
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
        if CONFIG.parseable.mode == Mode::Query {
            create_stream_and_schema_from_storage(&stream_name).await?;
        } else {
            return Err(StreamError::StreamNotFound(stream_name));
        }
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

pub async fn create_internal_stream_if_not_exists() -> Result<(), StreamError> {
    if let Ok(stream_exists) =
        create_stream_if_not_exists(INTERNAL_STREAM_NAME, &StreamType::Internal.to_string()).await
    {
        if stream_exists {
            return Ok(());
        }
        let mut header_map = HeaderMap::new();
        header_map.insert(
            HeaderName::from_str(STREAM_TYPE_KEY).unwrap(),
            HeaderValue::from_str(&StreamType::Internal.to_string()).unwrap(),
        );
        header_map.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        sync_streams_with_ingestors(header_map, Bytes::new(), INTERNAL_STREAM_NAME).await?;
    }
    Ok(())
}
#[allow(unused)]
fn classify_json_error(kind: serde_json::error::Category) -> StatusCode {
    match kind {
        serde_json::error::Category::Io => StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::error::Category::Syntax => StatusCode::BAD_REQUEST,
        serde_json::error::Category::Data => StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::error::Category::Eof => StatusCode::BAD_REQUEST,
    }
}

pub mod error {

    use actix_web::http::header::ContentType;
    use http::StatusCode;

    use crate::{
        hottier::HotTierError,
        metadata::error::stream_info::MetadataError,
        storage::ObjectStorageError,
        validator::error::{
            AlertValidationError, HotTierValidationError, StreamNameValidationError,
        },
    };

    #[allow(unused)]
    use super::classify_json_error;

    #[derive(Debug, thiserror::Error)]
    pub enum CreateStreamError {
        #[error("Stream name validation failed: {0}")]
        StreamNameValidation(#[from] StreamNameValidationError),
        #[error("failed to create log stream {stream_name} due to err: {err}")]
        Storage {
            stream_name: String,
            err: ObjectStorageError,
        },
        #[error("{msg}")]
        Custom { msg: String, status: StatusCode },
        #[error("Could not deserialize into JSON object, {0}")]
        SerdeError(#[from] serde_json::Error),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum StreamError {
        #[error("{0}")]
        CreateStream(#[from] CreateStreamError),
        #[error("Log stream {0} does not exist")]
        StreamNotFound(String),
        #[error(
            "Caching not enabled at Parseable server config. Can't enable cache for stream {0}"
        )]
        CacheNotEnabled(String),
        #[error("Log stream is not initialized, send an event to this logstream and try again")]
        UninitializedLogstream,
        #[error("Storage Error {0}")]
        Storage(#[from] ObjectStorageError),
        #[error("No alerts configured for this stream")]
        NoAlertsSet,
        #[error("failed to set alert configuration for log stream {stream} due to err: {err}")]
        BadAlertJson {
            stream: String,
            err: serde_json::Error,
        },
        #[error("Alert validation failed due to {0}")]
        AlertValidation(#[from] AlertValidationError),
        #[error("alert - \"{0}\" is invalid, please check if alert is valid according to this stream's schema and try again")]
        InvalidAlert(String),
        #[error(
            "alert - \"{0}\" is invalid, column \"{1}\" does not exist in this stream's schema"
        )]
        InvalidAlertMessage(String, String),
        #[error("failed to set retention configuration due to err: {0}")]
        InvalidRetentionConfig(serde_json::Error),
        #[error("{msg}")]
        Custom { msg: String, status: StatusCode },
        #[error("Error: {0}")]
        Anyhow(#[from] anyhow::Error),
        #[error("Network Error: {0}")]
        Network(#[from] reqwest::Error),
        #[error("Could not deserialize into JSON object, {0}")]
        SerdeError(#[from] serde_json::Error),
        #[error(
            "Hot tier is not enabled at the server config, cannot enable hot tier for stream {0}"
        )]
        HotTierNotEnabled(String),
        #[error("failed to enable hottier due to err: {0}")]
        InvalidHotTierConfig(serde_json::Error),
        #[error("Hot tier validation failed: {0}")]
        HotTierValidation(#[from] HotTierValidationError),
        #[error("{0}")]
        HotTierError(#[from] HotTierError),
    }

    impl actix_web::ResponseError for StreamError {
        fn status_code(&self) -> http::StatusCode {
            match self {
                StreamError::CreateStream(CreateStreamError::StreamNameValidation(_)) => {
                    StatusCode::BAD_REQUEST
                }
                StreamError::CreateStream(CreateStreamError::Storage { .. }) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                StreamError::CreateStream(CreateStreamError::Custom { .. }) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                StreamError::CreateStream(CreateStreamError::SerdeError(_)) => {
                    StatusCode::BAD_REQUEST
                }
                StreamError::CacheNotEnabled(_) => StatusCode::BAD_REQUEST,
                StreamError::StreamNotFound(_) => StatusCode::NOT_FOUND,
                StreamError::Custom { status, .. } => *status,
                StreamError::UninitializedLogstream => StatusCode::METHOD_NOT_ALLOWED,
                StreamError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
                StreamError::NoAlertsSet => StatusCode::NOT_FOUND,
                StreamError::BadAlertJson { .. } => StatusCode::BAD_REQUEST,
                StreamError::AlertValidation(_) => StatusCode::BAD_REQUEST,
                StreamError::InvalidAlert(_) => StatusCode::BAD_REQUEST,
                StreamError::InvalidAlertMessage(_, _) => StatusCode::BAD_REQUEST,
                StreamError::InvalidRetentionConfig(_) => StatusCode::BAD_REQUEST,
                StreamError::SerdeError(_) => StatusCode::BAD_REQUEST,
                StreamError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
                StreamError::Network(err) => {
                    err.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
                }
                StreamError::HotTierNotEnabled(_) => StatusCode::BAD_REQUEST,
                StreamError::InvalidHotTierConfig(_) => StatusCode::BAD_REQUEST,
                StreamError::HotTierValidation(_) => StatusCode::BAD_REQUEST,
                StreamError::HotTierError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }

    impl From<MetadataError> for StreamError {
        fn from(value: MetadataError) -> Self {
            match value {
                MetadataError::StreamMetaNotFound(s) => StreamError::StreamNotFound(s),
                MetadataError::StandaloneWithDistributed(s) => StreamError::Custom {
                    msg: s,
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::handlers::http::logstream::error::StreamError;
    use crate::handlers::http::logstream::get_stats;
    use actix_web::test::TestRequest;
    use anyhow::bail;

    #[actix_web::test]
    #[should_panic]
    async fn get_stats_panics_without_logstream() {
        let req = TestRequest::default().to_http_request();
        let _ = get_stats(req).await;
    }

    #[actix_web::test]
    async fn get_stats_stream_not_found_error_for_unknown_logstream() -> anyhow::Result<()> {
        let req = TestRequest::default()
            .param("logstream", "test")
            .to_http_request();

        match get_stats(req).await {
            Err(StreamError::StreamNotFound(_)) => Ok(()),
            _ => bail!("expected StreamNotFound error"),
        }
    }
}
