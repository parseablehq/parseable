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
use serde_json::Value;
use sysinfo::{System, SystemExt};

use crate::event;
use crate::metadata;
use crate::query::Query;
use crate::response;
use crate::s3::S3;
use crate::storage::ObjectStorage;
use crate::utils;
use crate::validator;

const META_LABEL: &str = "x-p-meta";

pub async fn liveness() -> HttpResponse {
    // If the available memory is less than 100MiB, return a 503 error.
    // As liveness check fails, Kubelet will restart the server.
    if System::new_all().available_memory() < 100 * 1024 * 1024 {
        return HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE);
    }

    HttpResponse::new(StatusCode::OK)
}

pub async fn readiness() -> HttpResponse {
    if S3::new().is_available().await {
        return HttpResponse::new(StatusCode::OK);
    }

    HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE)
}

//
// **** Query related handlers ****
//
pub async fn query(_req: HttpRequest, json: web::Json<Value>) -> HttpResponse {
    let json = json.into_inner();
    let query = match Query::parse(json) {
        Ok(s) => s,
        Err(crate::Error::JsonQuery(e)) => {
            return response::ServerResponse {
                msg: format!("Bad Request: missing \"{}\" field in query payload", e),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
        Err(e) => {
            return response::ServerResponse {
                msg: format!("Failed to execute query due to err: {}", e),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
    };

    let storage = S3::new();

    if storage.get_schema(&query.stream_name).await.is_err() {
        return response::ServerResponse {
            msg: format!("log stream {} does not exist", query.stream_name),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    match query.execute(&storage).await {
        Ok(results) => response::QueryResponse {
            body: results,
            code: StatusCode::OK,
        }
        .to_http(),
        Err(e) => response::ServerResponse {
            msg: e.to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http(),
    }
}

//
// **** Stream related handlers ****
//
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

//
// **** Steam metadata related handlers ****
//
// Get log stream schema
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

    match S3::new().alert_exists(&stream_name).await {
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

pub async fn post_event(req: HttpRequest, body: web::Json<serde_json::Value>) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let labels = collect_labels(&req);

    if let Err(e) = metadata::STREAM_INFO.schema(stream_name.clone()) {
        // if stream doesn't exist, fail to post data
        return response::ServerResponse {
            msg: format!(
                "Failed to post event. Log stream {} does not exist. Error: {}",
                stream_name, e
            ),
            code: StatusCode::NOT_FOUND,
        }
        .to_http();
    };

    let s3 = S3::new();

    if let Some(array) = body.as_array() {
        let mut i = 0;

        for body in array {
            let body = utils::flatten_json_body(web::Json(body.clone()), labels.clone()).unwrap();
            let e = event::Event {
                body,
                stream_name: stream_name.clone(),
            };

            if let Err(e) = e.process(&s3).await {
                return response::ServerResponse {
                    msg: format!("Failed to process event at index {} due to err: {}", i, e),
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                }
                .to_http();
            }

            i += 1;
        }

        return response::ServerResponse {
            msg: format!("Successfully posted {} events", i),
            code: StatusCode::OK,
        }
        .to_http();
    }
    let e = event::Event {
        body: utils::flatten_json_body(body, labels).unwrap(),
        stream_name,
    };
    if let Err(e) = e.process(&s3).await {
        return response::ServerResponse {
            msg: format!("Failed to process event due to err: {}", e),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http();
    }

    response::ServerResponse {
        msg: "Successfully posted event".to_string(),
        code: StatusCode::OK,
    }
    .to_http()
}

/// collect labels passed from http headers
/// format: labels = "app=k8s, cloud=gcp"
fn collect_labels(req: &HttpRequest) -> Option<String> {
    let keys = req.headers().keys().cloned().collect::<Vec<_>>();

    let mut labels_vec = Vec::with_capacity(100);
    for key in keys {
        if key.to_string().to_lowercase().starts_with(META_LABEL) {
            let value = req.headers().get(&key)?.to_str().ok();
            let remove_meta_char = format!("{}-", META_LABEL);
            let kv = format! {"{}={}", key.to_string().replace(&remove_meta_char.to_string(), ""), value.unwrap()};
            labels_vec.push(kv);
        }
    }

    Some(labels_vec.join(","))
}
