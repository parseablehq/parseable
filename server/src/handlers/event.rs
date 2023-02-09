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

use actix_web::{web, HttpRequest, HttpResponse};
use serde_json::Value;

use crate::event;
use crate::option::CONFIG;
use crate::query::Query;
use crate::response::QueryResponse;
use crate::utils::header_parsing::{collect_labelled_headers, ParseHeaderError};
use crate::utils::json::{flatten_json_body, merge};

use self::error::{PostError, QueryError};

const PREFIX_TAGS: &str = "x-p-tag-";
const PREFIX_META: &str = "x-p-meta-";
const STREAM_NAME_HEADER_KEY: &str = "x-p-stream";
const SEPARATOR: char = '^';

pub async fn query(_req: HttpRequest, json: web::Json<Value>) -> Result<HttpResponse, QueryError> {
    let json = json.into_inner();
    let query = Query::parse(json)?;

    let storage = CONFIG.storage().get_object_store();

    let query_result = query.execute(storage).await;

    query_result
        .map(Into::<QueryResponse>::into)
        .map(|response| response.to_http())
        .map_err(|e| e.into())
}

// Handler for POST /api/v1/ingest
// ingests events into the specified logstream in the header
// if the logstream does not exist, it is created
pub async fn ingest(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<HttpResponse, PostError> {
    if let Some((_, stream_name)) = req
        .headers()
        .iter()
        .find(|&(key, _)| key == STREAM_NAME_HEADER_KEY)
    {
        let stream_name = stream_name.to_str().unwrap().to_owned();
        if let Err(e) = super::logstream::create_stream_if_not_exists(&stream_name).await {
            return Err(PostError::CreateStream(e.into()));
        }
        push_logs(stream_name, req, body).await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        Err(PostError::Header(ParseHeaderError::MissingStreamName))
    }
}

// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
pub async fn post_event(
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<HttpResponse, PostError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    push_logs(stream_name, req, body).await?;
    Ok(HttpResponse::Ok().finish())
}

async fn push_logs(
    stream_name: String,
    req: HttpRequest,
    body: web::Json<serde_json::Value>,
) -> Result<(), PostError> {
    let tags_n_metadata = [
        (
            "p_tags".to_string(),
            Value::String(collect_labelled_headers(&req, PREFIX_TAGS, SEPARATOR)?),
        ),
        (
            "p_metadata".to_string(),
            Value::String(collect_labelled_headers(&req, PREFIX_META, SEPARATOR)?),
        ),
    ];

    match body.0 {
        Value::Array(array) => {
            for mut body in array {
                merge(&mut body, tags_n_metadata.clone().into_iter());
                let body = flatten_json_body(&body).unwrap();
                let schema_key = event::get_schema_key(&body);

                let event = event::Event {
                    body,
                    stream_name: stream_name.clone(),
                    schema_key,
                };

                event.process().await?;
            }
        }
        mut body @ Value::Object(_) => {
            merge(&mut body, tags_n_metadata.into_iter());
            let body = flatten_json_body(&body).unwrap();
            let schema_key = event::get_schema_key(&body);
            let event = event::Event {
                body,
                stream_name,
                schema_key,
            };

            event.process().await?;
        }
        _ => return Err(PostError::Invalid),
    }

    Ok(())
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
        #[error("Invalid Request")]
        Invalid,
        #[error("Failed to create stream due to {0}")]
        CreateStream(Box<dyn std::error::Error + Send + Sync>),
    }

    impl actix_web::ResponseError for PostError {
        fn status_code(&self) -> http::StatusCode {
            match self {
                PostError::Header(_) => StatusCode::BAD_REQUEST,
                PostError::Event(_) => StatusCode::INTERNAL_SERVER_ERROR,
                PostError::Invalid => StatusCode::BAD_REQUEST,
                PostError::CreateStream(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }
}
