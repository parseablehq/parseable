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

use actix_web::{web, HttpRequest, HttpResponse};
use serde_json::Value;

use crate::event;
use crate::query::Query;
use crate::response::QueryResponse;
use crate::s3::S3;
use crate::utils::header_parsing::collect_labelled_headers;
use crate::utils::{self, flatten_json_body, merge};

use self::error::{PostError, QueryError};

const PREFIX_TAGS: &str = "x-p-tag-";
const PREFIX_META: &str = "x-p-meta-";
const SEPARATOR: char = '^';

pub async fn query(_req: HttpRequest, json: web::Json<Value>) -> Result<HttpResponse, QueryError> {
    let json = json.into_inner();
    let query = Query::parse(json)?;

    let storage = S3::new();

    let query_result = query.execute(&storage).await;

    query_result
        .map(Into::<QueryResponse>::into)
        .map(|response| response.to_http())
        .map_err(|e| e.into())
}

pub async fn post_event(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<HttpResponse, PostError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();

    let tags = HashMap::from([(
        "p_tags".to_string(),
        collect_labelled_headers(&req, PREFIX_TAGS, SEPARATOR)?,
    )]);

    let metadata = HashMap::from([(
        "p_metadata".to_string(),
        collect_labelled_headers(&req, PREFIX_META, SEPARATOR)?,
    )]);

    if let Some(array) = body.as_array() {
        for body in array {
            let body = merge(body.clone(), metadata.clone());
            let body = merge(body, tags.clone());
            let body = flatten_json_body(web::Json(body)).unwrap();

            let event = event::Event {
                body,
                stream_name: stream_name.clone(),
            };

            event.process().await?;
        }
    } else {
        let body = merge(body.clone(), metadata);
        let body = merge(body, tags);

        let event = event::Event {
            body: utils::flatten_json_body(web::Json(body)).unwrap(),
            stream_name,
        };

        event.process().await?;
    }

    Ok(HttpResponse::Ok().finish())
}

pub mod error {
    use actix_web::http::header::ContentType;
    use http::StatusCode;

    use crate::{
        event::error::EventError,
        query::error::{ExecuteError, ParseError},
        utils::header_parsing::ParseHeaderError,
    };

    #[derive(Debug, thiserror::Error)]
    pub enum QueryError {
        #[error("Bad request: {0}")]
        Parse(#[from] ParseError),
        #[error("Query execution failed due to {0}")]
        Execute(#[from] ExecuteError),
    }

    impl actix_web::ResponseError for QueryError {
        fn status_code(&self) -> http::StatusCode {
            match self {
                QueryError::Parse(_) => StatusCode::BAD_REQUEST,
                QueryError::Execute(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum PostError {
        #[error("Header Error: {0}")]
        Header(#[from] ParseHeaderError),
        #[error("Event Error: {0}")]
        Event(#[from] EventError),
    }

    impl actix_web::ResponseError for PostError {
        fn status_code(&self) -> http::StatusCode {
            match self {
                PostError::Header(_) => StatusCode::BAD_REQUEST,
                PostError::Event(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }
}
