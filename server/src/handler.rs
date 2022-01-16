/*
 * Parseable Server (C) 2022 Parseable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use actix_web::http::StatusCode;
use actix_web::{web, HttpRequest, HttpResponse};

use crate::event;
use crate::option;
use crate::response;
use crate::storage;
use crate::utils;
use crate::query;

pub async fn cache_query(_req: HttpRequest, query: web::Json<query::Query>) -> HttpResponse {
     match query.parse() {
         Ok(stream_name) => {
            match storage::stream_exists(&stream_name) {
                Ok(_) => {
                    let results = query.execute(&stream_name);
                    response::QueryResponse {
                        body: results,
                        code: StatusCode::OK,
                    }
                    .to_http()
                }
                Err(_) => {
                    response::ServerResponse {
                        msg: format!("Stream {} does not exist", stream_name.to_string()),
                        code: StatusCode::BAD_REQUEST,
                    }
                    .to_http()
                 }
            }
         },
         Err(e) => {
            response::ServerResponse {
                msg: format!("Failed to execute query due to err: {}", e.to_string()),
                code: StatusCode::BAD_REQUEST,
            }
            .to_http()
         }
     }
}

pub async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    match utils::validate_stream_name(&stream_name) {
        Ok(_) => {
            match storage::stream_exists(&stream_name) {
                // Error if the stream already exists
                Ok(_) => response::ServerResponse {
                    msg: format!(
                        "Stream {} already exists, please create a new Stream with unique name",
                        stream_name
                    ),
                    code: StatusCode::BAD_REQUEST,
                }
                .to_http(),
                // Proceed to create stream if it doesn't exist
                Err(_) => match storage::create_stream(&stream_name) {
                    Ok(_) => response::ServerResponse {
                        msg: format!("Created Stream {}", stream_name),
                        code: StatusCode::OK,
                    }
                    .to_http(),
                    // Fail if unable to create stream on object store backend
                    Err(e) => response::ServerResponse {
                        msg: format!("Failed to create Stream due to err: {}", e.to_string()),
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                    }
                    .to_http(),
                },
            }
        }
        // fail to proceed if there is an error in stream name validation
        Err(e) => response::ServerResponse {
            msg: format!("Failed to create Stream due to err: {}", e.to_string()),
            code: StatusCode::BAD_REQUEST,
        }
        .to_http(),
    }
}

pub async fn post_event(req: HttpRequest, body: web::Json<serde_json::Value>) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();

    match storage::stream_exists(&stream_name) {
        // empty schema file is created in object store at the time of Put Stream.
        // If that file is successfully received, we assume that to be indicating that
        // stream already exists.
        Ok(schema) => {
            let e = event::Event {
                body: utils::flatten_json_body(body),
                path: option::get_opts().local_disk_path,
                stream_name: stream_name.clone(),
                schema: schema,
            };
            // If the .schema file is still empty, this is the first event in this stream.
            if e.schema.is_empty() {
                // 1. Parse the arrow schema, upload it to <bucket>/<stream_prefix>/.schema file
                // 2. Also store the event on local cache
                match e.initial_event() {
                    Ok(res) => response::ServerResponse {
                        msg: res.msg,
                        code: StatusCode::OK,
                    }
                    .to_http(),
                    Err(res) => response::ServerResponse {
                        msg: res.msg,
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                    }
                    .to_http(),
                }
            } else {
                match e.next_event() {
                    Ok(res) => response::ServerResponse {
                        msg: res.msg,
                        code: StatusCode::OK,
                    }
                    .to_http(),
                    Err(res) => response::ServerResponse {
                        msg: res.msg,
                        code: StatusCode::BAD_REQUEST,
                    }
                    .to_http(),
                }
            }
        }
        Err(e) => response::ServerResponse {
            msg: format!("Stream {} Does not Exist, Error: {}", stream_name, e),
            code: StatusCode::NOT_FOUND,
        }
        .to_http(),
    }
}
