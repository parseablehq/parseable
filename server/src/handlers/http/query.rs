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

use actix_web::http::header::ContentType;
use actix_web::{web, HttpRequest, Responder};
use http::StatusCode;
use serde_json::Value;
use std::time::Instant;

use crate::handlers::FILL_NULL_OPTION_KEY;
use crate::metrics::QUERY_EXECUTE_TIME;
use crate::option::CONFIG;
use crate::query::error::{ExecuteError, ParseError};
use crate::query::Query;
use crate::response::QueryResponse;

pub async fn query(
    _req: HttpRequest,
    json: web::Json<Value>,
) -> Result<impl Responder, QueryError> {
    let time = Instant::now();
    let json = json.into_inner();

    let fill_null = json
        .as_object()
        .and_then(|map| map.get(FILL_NULL_OPTION_KEY))
        .and_then(|value| value.as_bool())
        .unwrap_or_default();

    let query = Query::parse(json)?;

    let storage = CONFIG.storage().get_object_store();
    let query_result = query.execute(storage).await;
    let query_result = query_result
        .map(|(records, fields)| QueryResponse::new(records, fields, fill_null))
        .map(|response| response.to_http())
        .map_err(|e| e.into());

    let time = time.elapsed().as_secs_f64();
    QUERY_EXECUTE_TIME
        .with_label_values(&[query.stream_name.as_str()])
        .observe(time);

    query_result
}

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
