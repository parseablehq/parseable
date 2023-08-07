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

use actix_web::error::ErrorUnauthorized;
use actix_web::http::header::ContentType;
use actix_web::web::{self, Json};
use actix_web::{FromRequest, HttpRequest, Responder};
use actix_web_httpauth::extractors::basic::BasicAuth;
use futures_util::Future;
use http::StatusCode;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Instant;

use crate::handlers::FILL_NULL_OPTION_KEY;
use crate::metrics::QUERY_EXECUTE_TIME;
use crate::option::CONFIG;
use crate::query::error::{ExecuteError, ParseError};
use crate::query::Query;
use crate::rbac::role::{Action, Permission};
use crate::rbac::Users;
use crate::response::QueryResponse;

pub async fn query(
    query: Query,
    web::Query(params): web::Query<HashMap<String, bool>>,
) -> Result<impl Responder, QueryError> {
    let time = Instant::now();

    // format output json to include field names
    let with_fields = params.get("withFields").cloned().unwrap_or(false);
    // Fill missing columns with null
    let fill_null = params
        .get("fillNull")
        .cloned()
        .or(Some(query.fill_null))
        .unwrap_or(false);

    let storage = CONFIG.storage().get_object_store();
    let (records, fields) = query.execute(storage).await?;
    let response = QueryResponse {
        records,
        fields,
        fill_null,
        with_fields,
    }
    .to_http();

    let time = time.elapsed().as_secs_f64();
    QUERY_EXECUTE_TIME
        .with_label_values(&[query.stream_name.as_str()])
        .observe(time);

    Ok(response)
}

impl FromRequest for Query {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let creds = BasicAuth::extract(req)
            .into_inner()
            .expect("expects basic auth");
        // Extract username and password from the request using basic auth extractor.
        let username = creds.user_id().trim().to_owned();
        let password = creds.password().unwrap_or("").trim().to_owned();
        let permissions = Users.get_permissions(username, password);
        let json = Json::<Value>::from_request(req, payload);

        let fut = async move {
            let json = json.await?.into_inner();
            // maybe move this option to query param instead so that it can simply be extracted from request
            let fill_null = json
                .as_object()
                .and_then(|map| map.get(FILL_NULL_OPTION_KEY))
                .and_then(|value| value.as_bool())
                .unwrap_or_default();

            let mut query = Query::parse(json).map_err(QueryError::Parse)?;
            query.fill_null = fill_null;

            let mut authorized = false;
            let mut tags = Vec::new();

            // in permission check if user can run query on the stream.
            // also while iterating add any filter tags for this stream
            for permission in permissions {
                match permission {
                    Permission::Stream(Action::All, _) => authorized = true,
                    Permission::StreamWithTag(Action::Query, stream, tag)
                        if stream == query.stream_name || stream == "*" =>
                    {
                        authorized = true;
                        if let Some(tag) = tag {
                            tags.push(tag)
                        }
                    }
                    _ => (),
                }
            }

            if !authorized {
                return Err(ErrorUnauthorized("Not authorized"));
            }

            if !tags.is_empty() {
                query.filter_tag = Some(tags)
            }

            Ok(query)
        };

        Box::pin(fut)
    }
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
