/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

use actix_web::http::StatusCode;
use actix_web::{web, HttpRequest, HttpResponse, Responder};

use crate::alerts::Alerts;
use crate::response;
use crate::s3::S3;
use crate::storage::ObjectStorage;
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

    let s3 = S3::new();

    if s3.get_schema(&stream_name).await.is_err() {
        return response::ServerResponse {
            msg: format!("log stream {} does not exist", stream_name),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    if let Err(e) = s3.delete_stream(&stream_name).await {
        return response::ServerResponse {
            msg: format!(
                "failed to delete log stream {} due to err: {}",
                stream_name, e
            ),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http();
    }

    if let Err(e) = metadata::STREAM_INFO.delete_stream(&stream_name) {
        return response::ServerResponse {
            msg: format!(
                "failed to delete log stream {} from metadata due to err: {}",
                stream_name, e
            ),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http();
    }

    response::ServerResponse {
        msg: format!("log stream {} deleted", stream_name),
        code: StatusCode::OK,
    }
    .to_http()
}

pub async fn list(_: HttpRequest) -> impl Responder {
    response::list_response(S3::new().list_streams().await.unwrap())
}

pub async fn schema(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    match metadata::STREAM_INFO.schema(&stream_name) {
        Ok(schema) => response::ServerResponse {
            msg: schema,
            code: StatusCode::OK,
        }
        .to_http(),
        Err(_) => match S3::new().get_schema(&stream_name).await {
            Ok(schema) if schema.is_empty() => response::ServerResponse {
                msg: "log stream is not initialized, please post an event before fetching schema"
                    .to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http(),
            Ok(schema) => {
                let buf = schema.as_ref();
                response::ServerResponse {
                    msg: String::from_utf8(buf.to_vec()).unwrap(),
                    code: StatusCode::OK,
                }
                .to_http()
            }
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

    match metadata::STREAM_INFO.alert(stream_name.clone()) {
        Ok(alerts) => response::ServerResponse {
            msg: serde_json::to_string(&alerts).unwrap(),
            code: StatusCode::OK,
        }
        .to_http(),
        Err(_) => match S3::new().get_alerts(&stream_name).await {
            Ok(alerts) if alerts.alerts.is_empty() => response::ServerResponse {
                msg: "alert configuration not set for log stream {}".to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http(),
            Ok(alerts) => response::ServerResponse {
                msg: serde_json::to_string(&alerts).unwrap(),
                code: StatusCode::OK,
            }
            .to_http(),
            Err(_) => response::ServerResponse {
                msg: "alert doesn't exist".to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http(),
        },
    }
}

pub async fn put(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    // fail to proceed if there is an error in log stream name validation
    if let Err(e) = validator::stream_name(&stream_name) {
        return response::ServerResponse {
            msg: format!("failed to create log stream due to err: {}", e),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    let s3 = S3::new();

    // Proceed to create log stream if it doesn't exist
    if s3.get_schema(&stream_name).await.is_err() {
        if let Err(e) = metadata::STREAM_INFO.add_stream(
            stream_name.to_string(),
            "".to_string(),
            Default::default(),
        ) {
            return response::ServerResponse {
                msg: format!(
                    "failed to create log stream {} due to error: {}",
                    stream_name, e
                ),
                code: StatusCode::INTERNAL_SERVER_ERROR,
            }
            .to_http();
        }
        // Fail if unable to create log stream on object store backend
        if let Err(e) = s3.create_stream(&stream_name).await {
            // delete the stream from metadata because we couldn't create it on object store backend
            metadata::STREAM_INFO.delete_stream(&stream_name).unwrap();
            return response::ServerResponse {
                msg: format!(
                    "failed to create log stream {} due to err: {}",
                    stream_name, e
                ),
                code: StatusCode::INTERNAL_SERVER_ERROR,
            }
            .to_http();
        }
        return response::ServerResponse {
            msg: format!("created log stream {}", stream_name),
            code: StatusCode::OK,
        }
        .to_http();
    }

    // Error if the log stream already exists
    response::ServerResponse {
        msg: format!(
            "log stream {} already exists, please create a new log stream with unique name",
            stream_name
        ),
        code: StatusCode::BAD_REQUEST,
    }
    .to_http()
}

pub async fn put_alert(req: HttpRequest, body: web::Json<serde_json::Value>) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let alerts: Alerts = match serde_json::from_value(body.clone()) {
        Ok(alerts) => alerts,
        Err(e) => {
            return response::ServerResponse {
                msg: format!(
                    "failed to set alert configuration for log stream {} due to err: {}",
                    stream_name, e
                ),
                code: StatusCode::INTERNAL_SERVER_ERROR,
            }
            .to_http()
        }
    };
    match validator::alert(serde_json::to_string(&body.as_object()).unwrap()) {
        Ok(_) => match S3::new().put_alerts(&stream_name, alerts.clone()).await {
            Ok(_) => {
                if let Err(e) = metadata::STREAM_INFO.set_alert(stream_name.to_string(), alerts) {
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
            Err(e) => response::ServerResponse {
                msg: format!(
                    "failed to set alert configuration for log stream {} due to err: {}",
                    stream_name, e
                ),
                code: StatusCode::INTERNAL_SERVER_ERROR,
            }
            .to_http(),
        },
        Err(e) => response::ServerResponse {
            msg: format!(
                "failed to set alert configuration for log stream {} due to err: {}",
                stream_name, e
            ),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http(),
    }
}
