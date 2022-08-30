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

use std::collections::HashMap;

use actix_web::http::StatusCode;
use actix_web::{web, HttpRequest, HttpResponse, ResponseError};
use serde_json::Value;

use crate::event;
use crate::metadata;
use crate::query::Query;
use crate::response::{self, EventResponse};
use crate::s3::S3;
use crate::storage::ObjectStorage;
use crate::utils::header_parsing::collect_labelled_headers;
use crate::utils::{self, merge};

const PREFIX_TAGS: &str = "x-p-tags-";
const PREFIX_META: &str = "x-p-meta-";
const SEPARATOR: char = ',';

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

pub async fn post_event(req: HttpRequest, body: web::Json<serde_json::Value>) -> HttpResponse {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let tags = match collect_labelled_headers(&req, PREFIX_TAGS, SEPARATOR) {
        Ok(tags) => HashMap::from([("p_tags".to_string(), tags)]),
        Err(e) => return e.error_response(),
    };

    let metadata = match collect_labelled_headers(&req, PREFIX_META, SEPARATOR) {
        Ok(metadata) => HashMap::from([("p_metadata".to_string(), metadata)]),
        Err(e) => return e.error_response(),
    };

    if let Err(e) = metadata::STREAM_INFO.schema(&stream_name) {
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
            let body = merge(body.clone(), metadata.clone());
            let body = merge(body, tags.clone());
            let body = utils::flatten_json_body(web::Json(body)).unwrap();

            let e = event::Event {
                body,
                stream_name: stream_name.clone(),
            };

            if let Err(e) = e.process(&s3).await {
                return response::ServerResponse {
                    msg: format!("failed to process event because {}", e),
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

    let body = merge(body.clone(), metadata);
    let body = merge(body, tags);

    let event = event::Event {
        body: utils::flatten_json_body(web::Json(body)).unwrap(),
        stream_name,
    };

    match event.process(&s3).await {
        Ok(EventResponse { msg }) => response::ServerResponse {
            msg,
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http(),
        Err(e) => response::ServerResponse {
            msg: format!("Failed to process event due to err: {}", e),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
        .to_http(),
    }
}
