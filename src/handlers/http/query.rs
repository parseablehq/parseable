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

use std::{future::Future, pin::Pin, sync::Arc, time::Instant};

use actix_web::{
    http::header::ContentType, web::Json, FromRequest, HttpRequest, HttpResponse, Responder,
};
use chrono::{DateTime, Utc};
use datafusion::{common::tree_node::TreeNode, error::DataFusionError};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    event::{commit_schema, error::EventError},
    metrics::QUERY_EXECUTE_TIME,
    option::Mode,
    parseable::{StreamNotFound, PARSEABLE},
    query::{
        error::ExecuteError, execute, CountsRequest, CountsResponse, Query, TableScanVisitor,
        QUERY_SESSION,
    },
    rbac::{map::SessionKey, Users},
    response::QueryResponse,
    storage::{object_storage::commit_schema_to_storage, ObjectStorageError},
    utils::{
        actix::extract_session_key_from_req,
        time::{TimeParseError, TimeRange},
        user_auth_for_query,
    },
};

use super::fetch_schema;

/// Can be optionally be accepted as query params in the query request
/// NOTE: ensure that the fields param is not set based on request body
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(default)]
    pub fields: bool,
    #[serde(default)]
    pub send_null: bool,
}

/// Query Request in json format.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    pub query: String,
    pub start_time: String,
    pub end_time: String,
    #[serde(default, flatten)]
    pub params: QueryParams,
    #[serde(skip)]
    pub filter_tags: Option<Vec<String>>,
}

impl QueryRequest {
    // fields param is set based only on query param and send_null is set to true if either body or query param is true
    fn update_params(&mut self, QueryParams { fields, send_null }: QueryParams) {
        self.params.fields = fields;
        self.params.send_null |= send_null;
    }

    // Constructs a query from the http request
    pub async fn into_query(&self, key: &SessionKey) -> Result<Query, QueryError> {
        if self.query.is_empty() {
            return Err(QueryError::EmptyQuery);
        }

        if self.start_time.is_empty() {
            return Err(QueryError::EmptyStartTime);
        }

        if self.end_time.is_empty() {
            return Err(QueryError::EmptyEndTime);
        }

        let session_state = QUERY_SESSION.state();
        let plan = if let Ok(plan) = session_state.create_logical_plan(&self.query).await {
            plan
        } else {
            //if logical plan creation fails, create streams and try again
            create_streams_for_querier().await;
            session_state.create_logical_plan(&self.query).await?
        };

        // create a visitor to extract the table names present in query
        let mut visitor = TableScanVisitor::default();
        let _ = plan.visit(&mut visitor);
        let stream_names = visitor.into_inner();
        let permissions = Users.get_permissions(key);
        user_auth_for_query(&permissions, &stream_names)?;

        update_schema_when_distributed(&stream_names).await?;

        let time_range = TimeRange::parse_human_time(&self.start_time, &self.end_time)?;

        Ok(Query {
            plan,
            time_range,
            filter_tag: self.filter_tags.clone(),
            stream_names,
        })
    }
}

pub async fn query(
    req: HttpRequest,
    query_request: QueryRequest,
) -> Result<impl Responder, QueryError> {
    let key = extract_session_key_from_req(&req)?;
    let query = query_request.into_query(&key).await?;
    let first_stream_name = query
        .first_stream_name()
        .ok_or_else(|| QueryError::MalformedQuery("No table name found in query"))?;
    let histogram = QUERY_EXECUTE_TIME.with_label_values(&[first_stream_name]);

    let time = Instant::now();

    // Intercept `count(*)`` queries and use the counts API
    if let Some(column_name) = query.is_logical_plan_count_without_filters() {
        let counts_req = CountsRequest {
            stream: first_stream_name.to_owned(),
            start_time: query_request.start_time.clone(),
            end_time: query_request.end_time.clone(),
            num_bins: 1,
        };
        let count_records = counts_req.get_bin_density().await?;
        // NOTE: this should not panic, since there is atleast one bin, always
        let count = count_records[0].count;
        let response = if query_request.params.fields {
            json!({
                "fields": [&column_name],
                "records": [json!({column_name: count})]
            })
        } else {
            Value::Array(vec![json!({column_name: count})])
        };

        let time = time.elapsed().as_secs_f64();

        QUERY_EXECUTE_TIME
            .with_label_values(&[first_stream_name])
            .observe(time);

        return Ok(HttpResponse::Ok().json(response));
    }

    let stream_name = query.first_stream_name().cloned().unwrap_or_default();
    let (records, fields) = execute(query, &stream_name).await?;
    let response = QueryResponse {
        records,
        fields,
        fill_null: query_request.params.send_null,
        with_fields: query_request.params.fields,
    }
    .to_http()?;

    let time = time.elapsed().as_secs_f64();

    histogram.observe(time);

    Ok(response)
}

pub async fn get_counts(
    req: HttpRequest,
    counts_request: Json<CountsRequest>,
) -> Result<impl Responder, QueryError> {
    let creds = extract_session_key_from_req(&req)?;
    let permissions = Users.get_permissions(&creds);

    // does user have access to table?
    user_auth_for_query(&permissions, &[counts_request.stream.clone()])?;

    let records = counts_request.get_bin_density().await?;

    Ok(Json(CountsResponse {
        fields: vec!["start_time".into(), "end_time".into(), "count".into()],
        records,
    }))
}

pub async fn update_schema_when_distributed(tables: &Vec<String>) -> Result<(), EventError> {
    if PARSEABLE.options.mode == Mode::Query {
        for table in tables {
            if let Ok(new_schema) = fetch_schema(table).await {
                // commit schema merges the schema internally and updates the schema in storage.
                commit_schema_to_storage(table, new_schema.clone()).await?;

                commit_schema(table, Arc::new(new_schema))?;
            }
        }
    }

    Ok(())
}

/// Create streams for querier if they do not exist
/// get list of streams from memory and storage
/// create streams for memory from storage if they do not exist
pub async fn create_streams_for_querier() {
    let querier_streams = PARSEABLE.streams.list();
    for stream_name in PARSEABLE
        .storage
        .get_object_store()
        .list_streams()
        .await
        .unwrap()
    {
        if !querier_streams.contains(&stream_name) {
            let _ = PARSEABLE
                .create_stream_and_schema_from_storage(&stream_name)
                .await;
        }
    }
}

impl FromRequest for QueryRequest {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let query = Json::<QueryRequest>::from_request(req, payload);
        let params = actix_web::web::Query::<QueryParams>::from_request(req, payload)
            .into_inner()
            .map(|x| x.0)
            .unwrap_or_default();

        let fut = async move {
            let mut query = query.await?.into_inner();
            query.update_params(params);

            Ok(query)
        };

        Box::pin(fut)
    }
}

/// unused for now, might need it in the future
#[allow(unused)]
fn transform_query_for_ingestor(query: &QueryRequest) -> Option<QueryRequest> {
    if query.query.is_empty() {
        return None;
    }

    if query.start_time.is_empty() {
        return None;
    }

    if query.end_time.is_empty() {
        return None;
    }

    let end_time = if query.end_time == "now" {
        Utc::now()
    } else {
        DateTime::parse_from_rfc3339(&query.end_time)
            .ok()?
            .with_timezone(&Utc)
    };

    let start_time = end_time - chrono::Duration::minutes(1);
    // when transforming the query, the ingestors are forced to return an array of values
    let mut q = query.clone();
    q.params.fields = false;
    q.start_time = start_time.to_rfc3339();
    q.end_time = end_time.to_rfc3339();

    Some(q)
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("Query cannot be empty")]
    EmptyQuery,
    #[error("Start time cannot be empty")]
    EmptyStartTime,
    #[error("End time cannot be empty")]
    EmptyEndTime,
    #[error("Error while parsing provided time range: {0}")]
    TimeParse(#[from] TimeParseError),
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Datafusion Error: {0}")]
    Datafusion(#[from] DataFusionError),
    #[error("Execution Error: {0}")]
    Execute(#[from] ExecuteError),
    #[error("ObjectStorage Error: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Event Error: {0}")]
    EventError(#[from] EventError),
    #[error("Error: {0}")]
    MalformedQuery(&'static str),
    #[allow(unused)]
    #[error(
        r#"Error: Failed to Parse Record Batch into Json
Description: {0}"#
    )]
    JsonParse(String),
    #[error("Error: {0}")]
    ActixError(#[from] actix_web::Error),
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("Error: {0}")]
    StreamNotFound(#[from] StreamNotFound),
}

impl actix_web::ResponseError for QueryError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            QueryError::Execute(_) | QueryError::JsonParse(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

impl From<reqwest::Error> for QueryError {
    fn from(value: reqwest::Error) -> Self {
        QueryError::Anyhow(anyhow::Error::msg(value.to_string()))
    }
}
