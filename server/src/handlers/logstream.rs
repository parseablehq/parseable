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
use actix_web::{web, HttpRequest, HttpResponse, Responder};
use chrono::Utc;
use serde_json::Value;

use crate::alerts::Alerts;
use crate::option::CONFIG;
use crate::storage::StorageDir;
use crate::{event, response};
use crate::{metadata, validator};

pub async fn delete(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if let Err(e) = validator::stream_name(&stream_name) {
        // fail to proceed if there is an error in log stream name validation
        return response::ServerResponse {
            msg: format!("failed to get log stream schema due to err: {}", e),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    let objectstore = CONFIG.storage().get_object_store();

    if objectstore.get_schema(&stream_name).await.is_err() {
        return response::ServerResponse {
            msg: format!("log stream {} does not exist", stream_name),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    if let Err(e) = objectstore.delete_stream(&stream_name).await {
        return response::ServerResponse {
            msg: format!(
                "failed to delete log stream {} due to err: {}",
                stream_name, e
            ),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http();
    }

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

    response::ServerResponse {
        msg: format!("log stream {} deleted", stream_name),
        code: StatusCode::OK,
    }
    .to_http()
}

pub async fn list(_: HttpRequest) -> impl Responder {
    response::list_response(
        CONFIG
            .storage()
            .get_object_store()
            .list_streams()
            .await
            .unwrap(),
    )
}

pub async fn schema(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    match metadata::STREAM_INFO.schema(&stream_name) {
        Ok(schema) => response::ServerResponse {
            msg: schema
                .and_then(|ref schema| serde_json::to_string(schema).ok())
                .unwrap_or_default(),
            code: StatusCode::OK,
        }
        .to_http(),
        Err(_) => match CONFIG
            .storage()
            .get_object_store()
            .get_schema(&stream_name)
            .await
        {
            Ok(None) => response::ServerResponse {
                msg: "log stream is not initialized, please post an event before fetching schema"
                    .to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http(),
            Ok(Some(ref schema)) => response::ServerResponse {
                msg: serde_json::to_string(schema).unwrap(),
                code: StatusCode::OK,
            }
            .to_http(),
            Err(_) => response::ServerResponse {
                msg: "failed to get log stream schema, because log stream doesn't exist"
                    .to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http(),
        },
    }
}

pub async fn get_alert(req: HttpRequest) -> HttpResponse {
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
        None => match CONFIG
            .storage()
            .get_object_store()
            .get_alerts(&stream_name)
            .await
        {
            Ok(alerts) if alerts.alerts.is_empty() => {
                return response::ServerResponse {
                    msg: "alert configuration not set for log stream {}".to_string(),
                    code: StatusCode::BAD_REQUEST,
                }
                .to_http()
            }
            Ok(alerts) => serde_json::to_value(alerts).expect("alerts can serialize to valid json"),
            Err(_) => {
                return response::ServerResponse {
                    msg: "alert doesn't exist".to_string(),
                    code: StatusCode::BAD_REQUEST,
                }
                .to_http()
            }
        },
    };

    remove_id_from_alerts(&mut alerts);

    response::ServerResponse {
        msg: alerts.to_string(),
        code: StatusCode::OK,
    }
    .to_http()
}

pub async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    create_stream_if_not_exists(stream_name).await
}

pub async fn put_alert(req: HttpRequest, body: web::Json<serde_json::Value>) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let mut body = body.into_inner();
    remove_id_from_alerts(&mut body);

    let alerts: Alerts = match serde_json::from_value(body) {
        Ok(alerts) => alerts,
        Err(e) => {
            return response::ServerResponse {
                msg: format!(
                    "failed to set alert configuration for log stream {} due to err: {}",
                    stream_name, e
                ),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
    };

    if let Err(e) = validator::alert(&alerts) {
        return response::ServerResponse {
            msg: format!(
                "failed to set alert configuration for log stream {} due to err: {}",
                stream_name, e
            ),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    match metadata::STREAM_INFO.schema(&stream_name) {
        Ok(Some(schema)) => {
            let invalid_alert = alerts
                .alerts
                .iter()
                .find(|alert| !alert.rule.valid_for_schema(&schema));

            if let Some(alert) = invalid_alert {
                return response::ServerResponse {
                    msg:
                        format!("alert - \"{}\" is invalid, please check if alert is valid according to this stream's schema and try again", alert.name),
                    code: StatusCode::BAD_REQUEST,
                }
                .to_http();
            }
        }
        Ok(None) => {
            return response::ServerResponse {
                msg: "log stream is not initialized, please post an event before setting up alerts"
                    .to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
        Err(_) => {
            return response::ServerResponse {
                msg: "log stream is not found".to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
    }

    if let Err(e) = CONFIG
        .storage()
        .get_object_store()
        .put_alerts(&stream_name, &alerts)
        .await
    {
        return response::ServerResponse {
            msg: format!(
                "failed to set alert configuration for log stream {} due to err: {}",
                stream_name, e
            ),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http();
    }

    if let Err(e) = metadata::STREAM_INFO.set_alert(&stream_name, alerts) {
        return response::ServerResponse {
            msg: format!(
                "failed to set alert configuration for log stream {} due to err: {}",
                stream_name, e
            ),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http();
    }

    response::ServerResponse {
        msg: format!("set alert configuration for log stream {}", stream_name),
        code: StatusCode::OK,
    }
    .to_http()
}

pub async fn get_stats(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let stats = match metadata::STREAM_INFO.get_stats(&stream_name) {
        Ok(stats) => stats,
        Err(e) => {
            return response::ServerResponse {
                msg: format!("Could not return stats due to error: {}", e),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
    };

    let time = Utc::now();

    let stats = serde_json::json!({
        "stream": stream_name,
        "time": time,
        "ingestion": {
            "size": format!("{} {}", stats.ingestion, "Bytes"),
            "format": "json"
        },
        "storage": {
            "size": format!("{} {}", stats.storage, "Bytes"),
            "format": "parquet"
        }
    });

    response::ServerResponse {
        msg: stats.to_string(),
        code: StatusCode::OK,
    }
    .to_http()
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
pub async fn create_stream_if_not_exists(stream_name: String) -> HttpResponse {
    if metadata::STREAM_INFO.stream_exists(stream_name.as_str()) {
        // Error if the log stream already exists
        response::ServerResponse {
            msg: format!(
                "log stream {} already exists, please create a new log stream with unique name",
                stream_name
            ),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    // fail to proceed if invalid stream name
    if let Err(e) = validator::stream_name(&stream_name) {
        response::ServerResponse {
            msg: format!("failed to create log stream due to err: {}", e),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    // Proceed to create log stream if it doesn't exist
    let storage = CONFIG.storage().get_object_store();
    if let Err(e) = storage.create_stream(&stream_name).await {
        // Fail if unable to create log stream on object store backend
        response::ServerResponse {
            msg: format!(
                "failed to create log stream {} due to err: {}",
                stream_name, e
            ),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http();
    }
    metadata::STREAM_INFO.add_stream(stream_name.to_string(), None, Alerts::default());
    response::ServerResponse {
        msg: format!("created log stream {}", stream_name),
        code: StatusCode::OK,
    }
    .to_http()
}
