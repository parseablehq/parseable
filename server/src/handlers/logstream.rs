/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use actix_web::http::StatusCode;
use actix_web::{web, HttpRequest, Responder};
use chrono::Utc;
use serde_json::Value;

use crate::alerts::Alerts;
use crate::event;
use crate::option::CONFIG;
use crate::storage::{ObjectStorageError, StorageDir};
use crate::{metadata, validator};

use self::error::StreamError;

pub async fn delete(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    validator::stream_name(&stream_name)?;

    let objectstore = CONFIG.storage().get_object_store();

    if objectstore.get_schema(&stream_name).await.is_err() {
        return Err(StreamError::StreamNotFound(stream_name.to_string()));
    }

    objectstore.delete_stream(&stream_name).await?;
    metadata::STREAM_INFO.delete_stream(&stream_name);

    if event::STREAM_WRITERS::delete_entry(&stream_name).is_err() {
        log::warn!(
            "failed to delete log stream event writers for stream {}",
            stream_name
        )
    }

    let stream_dir = StorageDir::new(&stream_name);
    if fs::remove_dir_all(&stream_dir.data_path).is_err() {
        log::warn!(
            "failed to delete local data for stream {}. Clean {} manually",
            stream_name,
            stream_dir.data_path.to_string_lossy()
        )
    }

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn list(_: HttpRequest) -> impl Responder {
    let body = CONFIG
        .storage()
        .get_object_store()
        .list_streams()
        .await
        .unwrap();
    web::Json(body)
}

pub async fn schema(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    match metadata::STREAM_INFO.schema(&stream_name) {
        Ok(schema) => Ok((web::Json(schema), StatusCode::OK)),
        Err(_) => match CONFIG
            .storage()
            .get_object_store()
            .get_schema(&stream_name)
            .await
        {
            Ok(Some(schema)) => Ok((web::Json(Some(schema)), StatusCode::OK)),
            Ok(None) => Err(StreamError::UninitializedLogstream),
            Err(ObjectStorageError::NoSuchKey(_)) => Err(StreamError::StreamNotFound(stream_name)),
            Err(err) => Err(err.into()),
        },
    }
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

pub async fn put_stream(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    if metadata::STREAM_INFO.stream_exists(&stream_name) {
        // Error if the log stream already exists
        return Err(StreamError::Custom {
            msg: format!(
                "log stream {stream_name} already exists, please create a new log stream with unique name"
            ),
            status: StatusCode::BAD_REQUEST,
        });
    } else {
        create_stream(stream_name).await?;
    }

    Ok(("log stream created", StatusCode::OK))
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

    match metadata::STREAM_INFO.schema(&stream_name) {
        Ok(Some(schema)) => {
            let invalid_alert = alerts
                .alerts
                .iter()
                .find(|alert| !alert.rule.valid_for_schema(&schema));

            if let Some(alert) = invalid_alert {
                return Err(StreamError::InvalidAlert(alert.name.to_string()));
            }
        }
        Ok(None) => return Err(StreamError::UninitializedLogstream),
        Err(_) => return Err(StreamError::StreamNotFound(stream_name)),
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

pub async fn get_stats(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let stats = match metadata::STREAM_INFO.get_stats(&stream_name) {
        Ok(stats) => stats,
        Err(_) => return Err(StreamError::StreamNotFound(stream_name)),
    };

    let time = Utc::now();

    let stats = serde_json::json!({
        "stream": stream_name,
        "time": time,
        "ingestion": {
            "count": stats.events,
            "size": format!("{} {}", stats.ingestion, "Bytes"),
            "format": "json"
        },
        "storage": {
            "size": format!("{} {}", stats.storage, "Bytes"),
            "format": "parquet"
        }
    });

    Ok((web::Json(stats), StatusCode::OK))
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

// Check if the stream exists and create a new stream if doesn't exist
pub async fn create_stream_if_not_exists(stream_name: &str) -> Result<(), StreamError> {
    if metadata::STREAM_INFO.stream_exists(stream_name) {
        return Ok(());
    }

    create_stream(stream_name.to_string()).await
}

pub async fn create_stream(stream_name: String) -> Result<(), StreamError> {
    // fail to proceed if invalid stream name
    validator::stream_name(&stream_name)?;

    // Proceed to create log stream if it doesn't exist
    let storage = CONFIG.storage().get_object_store();
    if let Err(e) = storage.create_stream(&stream_name).await {
        // Fail if unable to create log stream on object store backend
        return Err(StreamError::Custom {
            msg: format!("failed to create log stream {stream_name} due to err: {e}"),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        });
    }
    metadata::STREAM_INFO.add_stream(stream_name.to_string(), None, Alerts::default());

    Ok(())
}

pub mod error {

    use actix_web::http::header::ContentType;
    use http::StatusCode;

    use crate::{
        storage::ObjectStorageError,
        validator::error::{AlertValidationError, StreamNameValidationError},
    };

    #[derive(Debug, thiserror::Error)]
    pub enum StreamError {
        #[error("Stream name validation failed due to {0}")]
        StreamNameValidation(#[from] StreamNameValidationError),
        #[error("Log stream {0} does not exist")]
        StreamNotFound(String),
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
        #[error("{msg}")]
        Custom { msg: String, status: StatusCode },
    }

    impl actix_web::ResponseError for StreamError {
        fn status_code(&self) -> http::StatusCode {
            match self {
                StreamError::StreamNameValidation(_) => StatusCode::BAD_REQUEST,
                StreamError::StreamNotFound(_) => StatusCode::NOT_FOUND,
                StreamError::Custom { status, .. } => *status,
                StreamError::UninitializedLogstream => StatusCode::METHOD_NOT_ALLOWED,
                StreamError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
                StreamError::NoAlertsSet => StatusCode::NOT_FOUND,
                StreamError::BadAlertJson { .. } => StatusCode::BAD_REQUEST,
                StreamError::AlertValidation(_) => StatusCode::BAD_REQUEST,
                StreamError::InvalidAlert(_) => StatusCode::BAD_REQUEST,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }
}
