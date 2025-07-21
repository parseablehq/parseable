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

use self::error::StreamError;
use super::cluster::utils::{IngestionStats, QueriedStats, StorageStats};
use super::query::update_schema_when_distributed;
use crate::event::format::override_data_type;
use crate::hottier::{CURRENT_HOT_TIER_VERSION, HotTierManager, StreamHotTier};
use crate::metadata::SchemaVersion;
use crate::metrics::{EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE_DATE, EVENTS_STORAGE_SIZE_DATE};
use crate::parseable::{PARSEABLE, StreamNotFound};
use crate::rbac::Users;
use crate::rbac::role::Action;
use crate::stats::{Stats, event_labels_date, storage_size_labels_date};
use crate::storage::retention::Retention;
use crate::storage::{StreamInfo, StreamType};
use crate::utils::actix::extract_session_key_from_req;
use crate::utils::json::flatten::{
    self, convert_to_array, generic_flattening, has_more_than_max_allowed_levels,
};
use crate::{LOCK_EXPECT, stats, validator};

use actix_web::http::StatusCode;
use actix_web::web::{Json, Path};
use actix_web::{HttpRequest, Responder, web};
use arrow_json::reader::infer_json_schema_from_iterator;
use bytes::Bytes;
use chrono::Utc;
use itertools::Itertools;
use serde_json::{Value, json};
use std::fs;
use std::sync::Arc;
use tracing::warn;

pub async fn delete(stream_name: Path<String>) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    // Error out if stream doesn't exist in memory, or in the case of query node, in storage as well
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name).into());
    }

    let objectstore = PARSEABLE.storage.get_object_store();

    // Delete from storage
    objectstore.delete_stream(&stream_name).await?;
    // Delete from staging
    let stream_dir = PARSEABLE.get_or_create_stream(&stream_name);
    if let Err(err) = fs::remove_dir_all(&stream_dir.data_path) {
        warn!(
            "failed to delete local data for stream {} with error {err}. Clean {} manually",
            stream_name,
            stream_dir.data_path.to_string_lossy()
        )
    }

    if let Some(hot_tier_manager) = HotTierManager::global() {
        if hot_tier_manager.check_stream_hot_tier_exists(&stream_name) {
            hot_tier_manager.delete_hot_tier(&stream_name).await?;
        }
    }

    // Delete from memory
    PARSEABLE.streams.delete(&stream_name);
    stats::delete_stats(&stream_name, "json")
        .unwrap_or_else(|e| warn!("failed to delete stats for stream {}: {:?}", stream_name, e));

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn list(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let key = extract_session_key_from_req(&req)
        .map_err(|err| StreamError::Anyhow(anyhow::Error::msg(err.to_string())))?;

    // list all streams from storage
    let res = PARSEABLE
        .storage
        .get_object_store()
        .list_streams()
        .await
        .unwrap()
        .into_iter()
        .filter(|logstream| {
            Users.authorize(key.clone(), Action::ListStream, Some(logstream), None)
                == crate::rbac::Response::Authorized
        })
        .map(|name| json!({"name": name}))
        .collect_vec();

    Ok(web::Json(res))
}

pub async fn detect_schema(Json(json): Json<Value>) -> Result<impl Responder, StreamError> {
    // flatten before infer
    if !has_more_than_max_allowed_levels(&json, 1) {
        //perform generic flattening, return error if failed to flatten
        let mut flattened_json = match generic_flattening(&json) {
            Ok(flattened) => match convert_to_array(flattened) {
                Ok(array) => array,
                Err(e) => {
                    return Err(StreamError::Custom {
                        msg: format!("Failed to convert to array: {e}"),
                        status: StatusCode::BAD_REQUEST,
                    });
                }
            },
            Err(e) => {
                return Err(StreamError::Custom {
                    msg: e.to_string(),
                    status: StatusCode::BAD_REQUEST,
                });
            }
        };
        if let Err(err) = flatten::flatten(&mut flattened_json, "_", None, None, None, false) {
            return Err(StreamError::Custom {
                msg: err.to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }
        let flattened_json_arr = match flattened_json {
            Value::Array(arr) => arr,
            value @ Value::Object(_) => vec![value],
            _ => unreachable!("flatten would have failed beforehand"),
        };
        let mut schema = match infer_json_schema_from_iterator(flattened_json_arr.iter().map(Ok)) {
            Ok(schema) => Arc::new(schema),
            Err(e) => {
                return Err(StreamError::Custom {
                    msg: format!("Failed to infer schema: {e}"),
                    status: StatusCode::BAD_REQUEST,
                });
            }
        };
        for flattened_json in flattened_json_arr {
            schema = override_data_type(schema, flattened_json, SchemaVersion::V1);
        }
        Ok((web::Json(schema), StatusCode::OK))
    } else {
        // error out if the JSON is heavily nested
        Err(StreamError::Custom {
            msg: format!(
                "JSON is too deeply nested (exceeds level {}), cannot flatten",
                PARSEABLE.options.event_flatten_level
            ),
            status: StatusCode::BAD_REQUEST,
        })
    }
}

pub async fn get_schema(stream_name: Path<String>) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();

    // Ensure parseable is aware of stream in distributed mode
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name.clone()).into());
    }

    let stream = PARSEABLE.get_stream(&stream_name)?;
    match update_schema_when_distributed(&vec![stream_name.clone()]).await {
        Ok(_) => {
            let schema = stream.get_schema();
            Ok((web::Json(schema), StatusCode::OK))
        }
        Err(err) => Err(StreamError::Custom {
            msg: err.to_string(),
            status: StatusCode::EXPECTATION_FAILED,
        }),
    }
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

pub async fn get_retention(stream_name: Path<String>) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name.clone()).into());
    }

    let retention = PARSEABLE
        .get_stream(&stream_name)?
        .get_retention()
        .unwrap_or_default();
    Ok((web::Json(retention), StatusCode::OK))
}

pub async fn put_retention(
    stream_name: Path<String>,
    Json(retention): Json<Retention>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();

    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name).into());
    }

    PARSEABLE
        .storage
        .get_object_store()
        .put_retention(&stream_name, &retention)
        .await?;

    PARSEABLE.get_stream(&stream_name)?.set_retention(retention);

    Ok((
        format!("set retention configuration for log stream {stream_name}"),
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

pub async fn get_stats(
    req: HttpRequest,
    stream_name: Path<String>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();

    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name.clone()).into());
    }

    let query_string = req.query_string();
    if !query_string.is_empty() {
        let tokens = query_string.split('=').collect::<Vec<&str>>();
        let date_key = tokens[0];
        let date_value = tokens[1];
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
        .ok_or_else(|| StreamNotFound(stream_name.clone()))?;

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

    let stats = serde_json::to_value(stats)?;

    Ok((web::Json(stats), StatusCode::OK))
}

pub async fn get_stream_info(stream_name: Path<String>) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name.clone()).into());
    }

    let storage = PARSEABLE.storage.get_object_store();
    // if first_event_at is not found in memory map, check if it exists in the storage
    // if it exists in the storage, update the first_event_at in memory map
    let stream_first_event_at =
        if let Some(first_event_at) = PARSEABLE.get_stream(&stream_name)?.get_first_event() {
            Some(first_event_at)
        } else if let Ok(Some(first_event_at)) =
            storage.get_first_event_from_storage(&stream_name).await
        {
            PARSEABLE
                .update_first_event_at(&stream_name, &first_event_at)
                .await
        } else {
            None
        };

    let stream_log_source = storage
        .get_log_source_from_storage(&stream_name)
        .await
        .unwrap_or_default();
    PARSEABLE
        .update_log_source(&stream_name, stream_log_source)
        .await?;

    let hash_map = PARSEABLE.streams.read().unwrap();
    let stream_meta = hash_map
        .get(&stream_name)
        .ok_or_else(|| StreamNotFound(stream_name.clone()))?
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

    Ok((web::Json(stream_info), StatusCode::OK))
}

pub async fn put_stream_hot_tier(
    stream_name: Path<String>,
    Json(mut hottier): Json<StreamHotTier>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name).into());
    }

    let stream = PARSEABLE.get_stream(&stream_name)?;

    if stream.get_stream_type() == StreamType::Internal {
        return Err(StreamError::Custom {
            msg: "Hot tier can not be updated for internal stream".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }

    validator::hot_tier(&hottier.size.to_string())?;

    stream.set_hot_tier(true);
    let Some(hot_tier_manager) = HotTierManager::global() else {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    };
    let existing_hot_tier_used_size = hot_tier_manager
        .validate_hot_tier_size(&stream_name, hottier.size)
        .await?;
    hottier.used_size = existing_hot_tier_used_size;
    hottier.available_size = hottier.size;
    hottier.version = Some(CURRENT_HOT_TIER_VERSION.to_string());
    hot_tier_manager
        .put_hot_tier(&stream_name, &mut hottier)
        .await?;
    let storage = PARSEABLE.storage.get_object_store();
    let mut stream_metadata = storage.get_object_store_format(&stream_name).await?;
    stream_metadata.hot_tier_enabled = true;
    storage
        .put_stream_manifest(&stream_name, &stream_metadata)
        .await?;

    Ok((
        format!("hot tier set for stream {stream_name}"),
        StatusCode::OK,
    ))
}

pub async fn get_stream_hot_tier(stream_name: Path<String>) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();

    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name.clone()).into());
    }

    let Some(hot_tier_manager) = HotTierManager::global() else {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    };
    let meta = hot_tier_manager.get_hot_tier(&stream_name).await?;

    Ok((web::Json(meta), StatusCode::OK))
}

pub async fn delete_stream_hot_tier(
    stream_name: Path<String>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();

    // For query mode, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.check_or_load_stream(&stream_name).await {
        return Err(StreamNotFound(stream_name).into());
    }

    if PARSEABLE.get_stream(&stream_name)?.get_stream_type() == StreamType::Internal {
        return Err(StreamError::Custom {
            msg: "Hot tier can not be deleted for internal stream".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }

    let Some(hot_tier_manager) = HotTierManager::global() else {
        return Err(StreamError::HotTierNotEnabled(stream_name));
    };

    hot_tier_manager.delete_hot_tier(&stream_name).await?;

    Ok((
        format!("hot tier deleted for stream {stream_name}"),
        StatusCode::OK,
    ))
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
        parseable::StreamNotFound,
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
        #[error("{0}")]
        StreamNotFound(#[from] StreamNotFound),
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
        #[error(
            "alert - \"{0}\" is invalid, please check if alert is valid according to this stream's schema and try again"
        )]
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
        #[error("Hot tier validation failed: {0}")]
        HotTierValidation(#[from] HotTierValidationError),
        #[error("{0}")]
        HotTierError(#[from] HotTierError),
        #[error("Invalid query parameter: {0}")]
        InvalidQueryParameter(String),
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
                StreamError::HotTierNotEnabled(_) => StatusCode::FORBIDDEN,
                StreamError::HotTierValidation(_) => StatusCode::BAD_REQUEST,
                StreamError::HotTierError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                StreamError::InvalidQueryParameter(_) => StatusCode::BAD_REQUEST,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        event::format::LogSource, handlers::http::modal::utils::logstream_utils::PutStreamHeaders,
    };
    use actix_web::test::TestRequest;

    // TODO: Fix this test with routes
    // #[actix_web::test]
    // #[should_panic]
    // async fn get_stats_panics_without_logstream() {
    //     let req = TestRequest::default().to_http_request();
    //     let _ = get_stats(req).await;
    // }

    // #[actix_web::test]
    // async fn get_stats_stream_not_found_error_for_unknown_logstream() -> anyhow::Result<()> {
    //     let req = TestRequest::default().to_http_request();

    //     match get_stats(req, web::Path::from("test".to_string())).await {
    //         Err(StreamError::StreamNotFound(_)) => Ok(()),
    //         _ => bail!("expected StreamNotFound error"),
    //     }
    // }

    #[actix_web::test]
    async fn header_without_log_source() {
        let req = TestRequest::default().to_http_request();
        let PutStreamHeaders { log_source, .. } = req.headers().into();
        assert_eq!(log_source, LogSource::Json);
    }

    #[actix_web::test]
    async fn header_with_known_log_source() {
        let mut req = TestRequest::default()
            .insert_header(("X-P-Log-Source", "pmeta"))
            .to_http_request();
        let PutStreamHeaders { log_source, .. } = req.headers().into();
        assert_eq!(log_source, LogSource::Pmeta);

        req = TestRequest::default()
            .insert_header(("X-P-Log-Source", "otel-logs"))
            .to_http_request();
        let PutStreamHeaders { log_source, .. } = req.headers().into();
        assert_eq!(log_source, LogSource::OtelLogs);

        req = TestRequest::default()
            .insert_header(("X-P-Log-Source", "kinesis"))
            .to_http_request();
        let PutStreamHeaders { log_source, .. } = req.headers().into();
        assert_eq!(log_source, LogSource::Kinesis);
    }

    #[actix_web::test]
    async fn header_with_unknown_log_source() {
        let req = TestRequest::default()
            .insert_header(("X-P-Log-Source", "teststream"))
            .to_http_request();
        let PutStreamHeaders { log_source, .. } = req.headers().into();
        matches!(
            log_source,
            LogSource::Custom(src) if src == "teststream"
        );
    }
}
