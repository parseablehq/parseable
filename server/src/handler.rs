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
use bytes::Bytes;
use sysinfo::{System, SystemExt};

use crate::event;
use crate::metadata;
use crate::option;
use crate::query;
use crate::response;
use crate::storage;
use crate::utils;
use crate::validator;

const META_LABEL: &str = "x-p-meta";

pub async fn liveness() -> HttpResponse {
    // If the available memroy is less than 100MiB, return a 503 error.
    // As liveness check fails, Kubelet will restart the server.
    if System::new_all().available_memory() < 100 * 1024 * 1024 {
        return HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE);
    }
    HttpResponse::new(StatusCode::OK)
}

pub async fn readiness() -> HttpResponse {
    match storage::is_available() {
        Ok(_) => HttpResponse::new(StatusCode::OK),
        Err(_) => HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE),
    }
}

//
// **** Query related handlers ****
//
pub async fn cache_query(_req: HttpRequest, query: web::Json<query::Query>) -> HttpResponse {
    let stream_name = match query.parse() {
        Ok(stream_name) => stream_name,
        Err(e) => {
            return response::ServerResponse {
                msg: format!("Failed to execute query due to err: {}", e),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
        }
    };

    if storage::stream_exists(&stream_name).is_err() {
        return response::ServerResponse {
            msg: format!("log stream {} does not exist", stream_name),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    match query.execute(&stream_name) {
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

    if storage::stream_exists(&stream_name).is_err() {
        return response::ServerResponse {
            msg: format!("log stream {} does not exist", stream_name),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http();
    }

    if let Err(e) = storage::delete_stream(&stream_name) {
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
    response::list_response(storage::list_streams().unwrap())
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

    match storage::stream_exists(&stream_name) {
        Ok(schema) if schema.is_empty() => {
            // fail to proceed if there is an error in log stream name validation
            if let Err(e) = validator::stream_name(&stream_name) {
                return response::ServerResponse {
                    msg: format!("Failed to get log stream schema due to err: {}", e),
                    code: StatusCode::BAD_REQUEST,
                }
                .to_http();
            }

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

    match storage::alert_exists(&stream_name) {
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

    // Proceed to create log stream if it doesn't exist
    if storage::stream_exists(&stream_name).is_err() {
        // Fail if unable to create log stream on object store backend
        if let Err(e) = storage::create_stream(&stream_name) {
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
        Ok(_) => match storage::create_alert(&stream_name, alert_config.to_string()) {
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

    let schema = match storage::stream_exists(&stream_name) {
        // empty schema file is created in object store at the time of Put log stream.
        // If that file is successfully received, we assume that to be indicating that
        // log stream already exists.
        Ok(schema) => schema,
        Err(e) => {
            return response::ServerResponse {
                msg: format!("log stream {} Does not Exist, Error: {}", stream_name, e),
                code: StatusCode::NOT_FOUND,
            }
            .to_http()
        }
    };

    if let Some(array) = body.as_array() {
        let mut i = 0;

        for body in array {
            let body = utils::flatten_json_body(web::Json(body.clone()), labels.clone()).unwrap();
            let schema = utils::read_schema_from_file(&stream_name).unwrap();
            let e = event::Event {
                body,
                path: option::get_opts().local_disk_path,
                stream_name: stream_name.clone(),
                schema: Bytes::from(schema),
            };

            if let Err(e) = e.process() {
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
        path: option::get_opts().local_disk_path,
        stream_name,
        schema,
    };
    if let Err(e) = e.process() {
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
