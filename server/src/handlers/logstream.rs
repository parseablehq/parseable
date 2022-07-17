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

use crate::metadata;
use crate::response;
use crate::s3::S3;
use crate::storage::ObjectStorage;
use crate::validator;

pub async fn delete_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if let Err(e) = validator::stream_name(&stream_name) {
        // fail to proceed if there is an error in log stream name validation
        return response::ServerResponse {
            msg: format!("Failed to get log stream schema due to err: {}", e),
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
                "Failed to delete log stream {} from Object Storage due to err: {}",
                stream_name, e
            ),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http();
    }

    if let Err(e) = metadata::STREAM_INFO.delete_stream(stream_name.to_string()) {
        return response::ServerResponse {
            msg: format!(
                "Failed to delete log stream {} from metadata due to err: {}",
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

pub async fn list_streams(_: HttpRequest) -> impl Responder {
    response::list_response(S3::new().list_streams().await.unwrap())
}

pub async fn get_schema(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    // fail to proceed if there is an error in log stream name validation
    if let Err(e) = validator::stream_name(&stream_name) {
        return response::ServerResponse {
            msg: format!("Failed to get log stream schema due to err: {}", e),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    match S3::new().get_schema(&stream_name).await {
        Ok(schema) if schema.is_empty() => {
            response::ServerResponse {
                msg: "Failed to get log stream schema, because log stream is not initialized yet. Please post an event before fetching schema".to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
        Ok(schema) => {
            let buf = schema.as_ref();
            response::ServerResponse {
                msg: String::from_utf8(buf.to_vec()).unwrap(),
                code: StatusCode::OK,
            }
            .to_http()
        }
        Err(_) => response::ServerResponse {
            msg: "Failed to get log stream schema, because log stream doesn't exist".to_string(),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http(),
    }
}

pub async fn get_alert(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if let Err(e) = validator::stream_name(&stream_name) {
        // fail to proceed if there is an error in log stream name validation
        return response::ServerResponse {
            msg: format!("Failed to get log stream schema due to err: {}", e),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    match S3::new().get_alert(&stream_name).await {
        Ok(alert) if alert.is_empty() => {
            if let Err(e) = validator::stream_name(&stream_name) {
                return response::ServerResponse {
                    msg: format!("Failed to get Alert due to err: {}", e),
                    code: StatusCode::BAD_REQUEST,
                }
                .to_http();
            }
            response::ServerResponse {
                msg: "Failed to get Alert".to_string(),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
        Ok(schema) => {
            let buf = schema.as_ref();
            response::ServerResponse {
                msg: String::from_utf8(buf.to_vec()).unwrap(),
                code: StatusCode::OK,
            }
            .to_http()
        }
        Err(_) => response::ServerResponse {
            msg: "Failed to get Alert, because Alert doesn't exist".to_string(),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http(),
    }
}

pub async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    // fail to proceed if there is an error in log stream name validation
    if let Err(e) = validator::stream_name(&stream_name) {
        return response::ServerResponse {
            msg: format!("Failed to create log stream due to err: {}", e),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    let s3 = S3::new();

    // Proceed to create log stream if it doesn't exist
    if s3.get_schema(&stream_name).await.is_err() {
        // Fail if unable to create log stream on object store backend
        if let Err(e) = s3.create_stream(&stream_name).await {
            return response::ServerResponse {
                msg: format!("Failed to create log stream due to err: {}", e),
                code: StatusCode::INTERNAL_SERVER_ERROR,
            }
            .to_http();
        }

        if let Err(e) = metadata::STREAM_INFO.add_stream(
            stream_name.to_string(),
            "".to_string(),
            "".to_string(),
        ) {
            return response::ServerResponse {
                msg: format!(
                    "Failed to add log stream {} to metadata due to err: {}",
                    stream_name, e
                ),
                code: StatusCode::INTERNAL_SERVER_ERROR,
            }
            .to_http();
        }

        return response::ServerResponse {
            msg: format!("Created log stream {}", stream_name),
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
    let alert_config = body.clone();
    match validator::alert(serde_json::to_string(&body.as_object()).unwrap()) {
        Ok(_) => match S3::new()
            .create_alert(&stream_name, alert_config.to_string())
            .await
        {
            Ok(_) => {
                if let Err(e) = metadata::STREAM_INFO
                    .set_alert(stream_name.to_string(), alert_config.to_string())
                {
                    return response::ServerResponse {
                        msg: format!(
                            "Failed to set Alert for log stream {} due to err: {}",
                            stream_name, e
                        ),
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                    }
                    .to_http();
                }
                response::ServerResponse {
                    msg: format!("Set alert configuration for log stream {}", stream_name),
                    code: StatusCode::OK,
                }
                .to_http()
            }
            Err(e) => response::ServerResponse {
                msg: format!(
                    "Failed to set alert configuration for log stream {} due to err: {}",
                    stream_name, e
                ),
                code: StatusCode::INTERNAL_SERVER_ERROR,
            }
            .to_http(),
        },
        Err(e) => {
            return response::ServerResponse {
                msg: format!(
                    "Failed to set alert configuration for log stream {} due to err: {}",
                    stream_name, e
                ),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http();
        }
    }
}
