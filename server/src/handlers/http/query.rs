/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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
use actix_web::web::{self, Json};
use actix_web::{FromRequest, HttpRequest, Responder};
use chrono::{DateTime, Utc};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use futures_util::Future;
use http::StatusCode;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Instant;

use crate::metrics::QUERY_EXECUTE_TIME;
use crate::query::error::ExecuteError;
use crate::query::QUERY_SESSION;
use crate::rbac::role::{Action, Permission};
use crate::rbac::Users;
use crate::response::QueryResponse;
use crate::utils::actix::extract_session_key_from_req;

/// Query Request through http endpoint.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    query: String,
    start_time: String,
    end_time: String,
    #[serde(default)]
    send_null: bool,
    #[serde(skip)]
    fields: bool,
    #[serde(skip)]
    filter_tags: Option<Vec<String>>,
}

pub async fn query(req: HttpRequest, query_request: Query) -> Result<impl Responder, QueryError> {
    let creds = extract_session_key_from_req(&req).expect("expects basic auth");
    let permissions = Users.get_permissions(&creds);
    let session_state = QUERY_SESSION.state();
    let mut query = into_query(&query_request, &session_state).await?;

    // check authorization of this query if it references physical table;
    let table_name = query.table_name();
    if let Some(ref table) = table_name {
        let mut authorized = false;
        let mut tags = Vec::new();

        // in permission check if user can run query on the stream.
        // also while iterating add any filter tags for this stream
        for permission in permissions {
            match permission {
                Permission::Stream(Action::All, _) => {
                    authorized = true;
                    break;
                }
                Permission::StreamWithTag(Action::Query, ref stream, tag)
                    if stream == table || stream == "*" =>
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
            return Err(QueryError::Unauthorized);
        }

        if !tags.is_empty() {
            query.filter_tag = Some(tags)
        }
    }

    let time = Instant::now();

    let (records, fields) = query.execute().await?;
    let response = QueryResponse {
        records,
        fields,
        fill_null: query_request.send_null,
        with_fields: query_request.fields,
    }
    .to_http();

    if let Some(table) = table_name {
        let time = time.elapsed().as_secs_f64();
        QUERY_EXECUTE_TIME
            .with_label_values(&[&table])
            .observe(time);
    }

    Ok(response)
}

impl FromRequest for Query {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let query = Json::<Query>::from_request(req, payload);
        let params = web::Query::<HashMap<String, bool>>::from_request(req, payload)
            .into_inner()
            .map(|x| x.0)
            .unwrap_or_default();

        let fut = async move {
            let mut query = query.await?.into_inner();
            // format output json to include field names
            query.fields = params.get("fields").cloned().unwrap_or(false);

            if !query.send_null {
                query.send_null = params.get("sendNull").cloned().unwrap_or(false);
            }

            Ok(query)
        };

        Box::pin(fut)
    }
}

async fn into_query(
    query: &Query,
    session_state: &SessionState,
) -> Result<crate::query::Query, QueryError> {
    if query.query.is_empty() {
        return Err(QueryError::EmptyQuery);
    }

    if query.start_time.is_empty() {
        return Err(QueryError::EmptyStartTime);
    }

    if query.end_time.is_empty() {
        return Err(QueryError::EmptyEndTime);
    }

    let start: DateTime<Utc>;
    let end: DateTime<Utc>;

    if query.end_time == "now" {
        end = Utc::now();
        start = end - chrono::Duration::from_std(humantime::parse_duration(&query.start_time)?)?;
    } else {
        start = DateTime::parse_from_rfc3339(&query.start_time)
            .map_err(|_| QueryError::StartTimeParse)?
            .into();
        end = DateTime::parse_from_rfc3339(&query.end_time)
            .map_err(|_| QueryError::EndTimeParse)?
            .into();
    };

    if start.timestamp() > end.timestamp() {
        return Err(QueryError::StartTimeAfterEndTime);
    }

    Ok(crate::query::Query {
        raw_logical_plan: session_state.create_logical_plan(&query.query).await?,
        start,
        end,
        filter_tag: query.filter_tags.clone(),
    })
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("Query cannot be empty")]
    EmptyQuery,
    #[error("Start time cannot be empty")]
    EmptyStartTime,
    #[error("End time cannot be empty")]
    EmptyEndTime,
    #[error("Could not parse start time correctly")]
    StartTimeParse,
    #[error("Could not parse end time correctly")]
    EndTimeParse,
    #[error("While generating times for 'now' failed to parse duration")]
    NotValidDuration(#[from] humantime::DurationError),
    #[error("Parsed duration out of range")]
    OutOfRange(#[from] chrono::OutOfRangeError),
    #[error("Start time cannot be greater than the end time")]
    StartTimeAfterEndTime,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Datafusion Error: {0}")]
    Datafusion(#[from] DataFusionError),
    #[error("Execution Error: {0}")]
    Execute(#[from] ExecuteError),
}

impl actix_web::ResponseError for QueryError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            QueryError::Execute(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
