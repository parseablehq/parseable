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
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::event;
use crate::mem_store;
use crate::option;
use crate::response;
use crate::storage;
use crate::utils;

pub async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    match storage::stream_exists(&stream_name) {
        Ok(_) => response::ServerResponse {
            msg: format!(
                "Stream {} already exists, please create a Stream with unique name",
                stream_name
            ),
            code: StatusCode::BAD_REQUEST,
        }
        .server_response(),
        Err(_) => match storage::create_stream(&stream_name) {
            Ok(_) => response::ServerResponse {
                msg: format!("Created Stream {}", stream_name),
                code: StatusCode::OK,
            }
            .server_response(),
            Err(e) => response::ServerResponse {
                msg: format!("Failed to create Stream due to err: {}", e.to_string()),
                code: StatusCode::INTERNAL_SERVER_ERROR,
            }
            .server_response(),
        },
    }
}

pub async fn post_event(req: HttpRequest, body: web::Json<serde_json::Value>) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();

    match storage::stream_exists(&stream_name) {
        Ok(schema) => {
            let e = event::Event {
                body: utils::flatten_json_body(body),
                path: option::get_opts().local_disk_path,
                stream_name: stream_name.clone(),
                schema: schema,
            };
            // If the schema is empty, this is the first event in this stream.
            // Parse the arrow schema, upload it to <bucket>/<stream_prefix>/.schema file
            if e.schema.is_empty() {
                match e.initial_event() {
                    Ok(res) => {
                        // Store record batch to Parquet file on local cache
                        e.convert_arrow_parquet(res.rb.unwrap());
                        response::ServerResponse {
                            msg: res.msg,
                            code: StatusCode::OK,
                        }
                        .server_response()
                    }
                    Err(res) => response::ServerResponse {
                        msg: res.msg,
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                    }
                    .server_response(),
                }
            } else {
                let rb = mem_store::MEM_STREAMS::get_rb(stream_name.clone());
                match e.next_event() {
                    Ok(res) => {
                        let vec = vec![res.rb.unwrap(), rb];
                        let new_batch =
                            RecordBatch::concat(&Arc::new(res.schema.unwrap().clone()), &vec)
                                .unwrap();
                        mem_store::MEM_STREAMS::put(
                            stream_name.clone(),
                            mem_store::Stream {
                                stream_schema: Some(mem_store::MEM_STREAMS::get_schema(
                                    stream_name,
                                )),
                                rb: Some(new_batch.clone()),
                            },
                        );
                        e.convert_arrow_parquet(new_batch);
                        response::ServerResponse {
                            msg: res.msg,
                            code: StatusCode::OK,
                        }
                        .server_response()
                    }
                    Err(res) => response::ServerResponse {
                        msg: res.msg,
                        code: StatusCode::BAD_REQUEST,
                    }
                    .server_response(),
                }
            }
        }
        Err(e) => response::ServerResponse {
            msg: format!("Stream {} Does not Exist, Error: {}", &stream_name, e),
            code: StatusCode::NOT_FOUND,
        }
        .server_response(),
    }
}
