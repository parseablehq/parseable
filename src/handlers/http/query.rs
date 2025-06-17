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

use crate::event::error::EventError;
use crate::handlers::http::fetch_schema;
use actix_web::http::header::ContentType;
use actix_web::web::{self, Json};
use actix_web::{Either, FromRequest, HttpRequest, HttpResponse, Responder};
use arrow_array::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::common::tree_node::TreeNode;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use futures::stream::once;
use futures::{future, Stream, StreamExt};
use futures_util::Future;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::error;

use crate::event::commit_schema;
use crate::metrics::QUERY_EXECUTE_TIME;
use crate::option::Mode;
use crate::parseable::{StreamNotFound, PARSEABLE};
use crate::query::error::ExecuteError;
use crate::query::{execute, CountsRequest, CountsResponse, Query as LogicalQuery};
use crate::query::{TableScanVisitor, QUERY_SESSION};
use crate::rbac::Users;
use crate::response::QueryResponse;
use crate::storage::ObjectStorageError;
use crate::utils::actix::extract_session_key_from_req;
use crate::utils::time::{TimeParseError, TimeRange};
use crate::utils::user_auth_for_datasets;

const TIME_ELAPSED_HEADER: &str = "p-time-elapsed";
/// Query Request through http endpoint.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub query: String,
    pub start_time: String,
    pub end_time: String,
    #[serde(default)]
    pub send_null: bool,
    #[serde(skip)]
    pub fields: bool,
    #[serde(skip)]
    pub streaming: bool,
    #[serde(skip)]
    pub filter_tags: Option<Vec<String>>,
}

/// A function to execute the query and fetch QueryResponse
/// This won't look in the cache
/// TODO: Improve this function and make this a part of the query API
pub async fn get_records_and_fields(
    query_request: &Query,
    req: &HttpRequest,
) -> Result<(Option<Vec<RecordBatch>>, Option<Vec<String>>), QueryError> {
    let session_state = QUERY_SESSION.state();

    // get the logical plan and extract the table name
    let raw_logical_plan = session_state
        .create_logical_plan(&query_request.query)
        .await?;

    let time_range =
        TimeRange::parse_human_time(&query_request.start_time, &query_request.end_time)?;
    // create a visitor to extract the table name
    let mut visitor = TableScanVisitor::default();
    let _ = raw_logical_plan.visit(&mut visitor);

    let tables = visitor.into_inner();
    update_schema_when_distributed(&tables).await?;
    let query: LogicalQuery = into_query(query_request, &session_state, time_range).await?;

    let creds = extract_session_key_from_req(req)?;
    let permissions = Users.get_permissions(&creds);

    let table_name = query
        .first_table_name()
        .ok_or_else(|| QueryError::MalformedQuery("No table name found in query"))?;

    user_auth_for_datasets(&permissions, &tables)?;

    let (records, fields) = execute(query, &table_name, false).await?;

    let records = match records {
        Either::Left(vec_rb) => vec_rb,
        Either::Right(_) => {
            return Err(QueryError::CustomError("Reject streaming response".into()))
        }
    };

    Ok((Some(records), Some(fields)))
}

pub async fn query(req: HttpRequest, query_request: Query) -> Result<HttpResponse, QueryError> {
    let session_state = QUERY_SESSION.state();
    let raw_logical_plan = match session_state
        .create_logical_plan(&query_request.query)
        .await
    {
        Ok(raw_logical_plan) => raw_logical_plan,
        Err(_) => {
            create_streams_for_querier().await;
            session_state
                .create_logical_plan(&query_request.query)
                .await?
        }
    };
    let time_range =
        TimeRange::parse_human_time(&query_request.start_time, &query_request.end_time)?;

    let mut visitor = TableScanVisitor::default();
    let _ = raw_logical_plan.visit(&mut visitor);
    let tables = visitor.into_inner();
    update_schema_when_distributed(&tables).await?;
    let query: LogicalQuery = into_query(&query_request, &session_state, time_range).await?;

    let creds = extract_session_key_from_req(&req)?;
    let permissions = Users.get_permissions(&creds);

    let table_name = query
        .first_table_name()
        .ok_or_else(|| QueryError::MalformedQuery("No table name found in query"))?;

    user_auth_for_datasets(&permissions, &tables)?;

    let time = Instant::now();

    // if the query is `select count(*) from <dataset>`
    // we use the `get_bin_density` method to get the count of records in the dataset
    // instead of executing the query using datafusion
    if let Some(column_name) = query.is_logical_plan_count_without_filters() {
        return handle_count_query(&query_request, &table_name, column_name, time).await;
    }

    // if the query request has streaming = false (default)
    // we use datafusion's `execute` method to get the records
    if !query_request.streaming {
        return handle_non_streaming_query(query, &table_name, &query_request, time).await;
    }

    // if the query request has streaming = true
    // we use datafusion's `execute_stream` method to get the records
    handle_streaming_query(query, &table_name, &query_request, time).await
}

/// Handles count queries (e.g., `SELECT COUNT(*) FROM <dataset-name>`)
///
/// Instead of executing the query through DataFusion, this function uses the
/// `CountsRequest::get_bin_density` method to quickly retrieve the count of records
/// in the specified dataset and time range.
///
/// # Arguments
/// - `query_request`: The original query request from the client.
/// - `table_name`: The name of the table/dataset to count records in.
/// - `column_name`: The column being counted (usually `*`).
/// - `time`: The timer for measuring query execution time.
///
/// # Returns
/// - `HttpResponse` with the count result as JSON, including fields if requested.
async fn handle_count_query(
    query_request: &Query,
    table_name: &str,
    column_name: &str,
    time: Instant,
) -> Result<HttpResponse, QueryError> {
    let counts_req = CountsRequest {
        stream: table_name.to_string(),
        start_time: query_request.start_time.clone(),
        end_time: query_request.end_time.clone(),
        num_bins: 1,
    };
    let count_records = counts_req.get_bin_density().await?;
    let count = count_records[0].count;
    let response = if query_request.fields {
        json!({
            "fields": [column_name],
            "records": [json!({column_name: count})]
        })
    } else {
        serde_json::Value::Array(vec![json!({column_name: count})])
    };

    let total_time = format!("{:?}", time.elapsed());
    let time = time.elapsed().as_secs_f64();

    QUERY_EXECUTE_TIME
        .with_label_values(&[table_name])
        .observe(time);

    Ok(HttpResponse::Ok()
        .insert_header((TIME_ELAPSED_HEADER, total_time.as_str()))
        .json(response))
}

/// Handles standard (non-streaming) queries, returning all results in a single JSON response.
///
/// Executes the logical query using DataFusion's batch execution, collects all results,
/// and serializes them into a single JSON object. The response includes the records,
/// field names, and other metadata as specified in the query request.
///
/// # Arguments
/// - `query`: The logical query to execute.
/// - `table_name`: The name of the table/dataset being queried.
/// - `query_request`: The original query request from the client.
/// - `time`: The timer for measuring query execution time.
///
/// # Returns
/// - `HttpResponse` with the full query result as a JSON object.
async fn handle_non_streaming_query(
    query: LogicalQuery,
    table_name: &str,
    query_request: &Query,
    time: Instant,
) -> Result<HttpResponse, QueryError> {
    let (records, fields) = execute(query, table_name, query_request.streaming).await?;
    let records = match records {
        Either::Left(rbs) => rbs,
        Either::Right(_) => {
            return Err(QueryError::MalformedQuery(
                "Expected batch results, got stream",
            ))
        }
    };
    let total_time = format!("{:?}", time.elapsed());
    let time = time.elapsed().as_secs_f64();

    QUERY_EXECUTE_TIME
        .with_label_values(&[table_name])
        .observe(time);
    let response = QueryResponse {
        records,
        fields,
        fill_null: query_request.send_null,
        with_fields: query_request.fields,
    }
    .to_json()?;
    Ok(HttpResponse::Ok()
        .insert_header((TIME_ELAPSED_HEADER, total_time.as_str()))
        .json(response))
}

/// Handles streaming queries, returning results as newline-delimited JSON (NDJSON).
///
/// Executes the logical query using DataFusion's streaming execution. If the `fields`
/// flag is set, the first chunk of the response contains the field names as a JSON object.
/// Each subsequent chunk contains a record batch as a JSON object, separated by newlines.
/// This allows clients to start processing results before the entire query completes.
///
/// # Arguments
/// - `query`: The logical query to execute.
/// - `table_name`: The name of the table/dataset being queried.
/// - `query_request`: The original query request from the client.
/// - `time`: The timer for measuring query execution time.
///
/// # Returns
/// - `HttpResponse` streaming the query results as NDJSON, optionally prefixed with the fields array.
async fn handle_streaming_query(
    query: LogicalQuery,
    table_name: &str,
    query_request: &Query,
    time: Instant,
) -> Result<HttpResponse, QueryError> {
    let (records_stream, fields) = execute(query, table_name, query_request.streaming).await?;
    let records_stream = match records_stream {
        Either::Left(_) => {
            return Err(QueryError::MalformedQuery(
                "Expected stream results, got batch",
            ))
        }
        Either::Right(stream) => stream,
    };
    let total_time = format!("{:?}", time.elapsed());
    let time = time.elapsed().as_secs_f64();
    QUERY_EXECUTE_TIME
        .with_label_values(&[table_name])
        .observe(time);

    let send_null = query_request.send_null;
    let with_fields = query_request.fields;

    let stream = if with_fields {
        // send the fields json as an initial chunk
        let fields_json = serde_json::json!({
            "fields": fields
        })
        .to_string();

        // stream the records without fields
        let mut batch_processor = create_batch_processor(send_null);
        let records_stream = records_stream.map(move |batch_result| {
            let batch_result = batch_result.map_err(QueryError::from);
            batch_processor(batch_result)
        });

        // Combine the initial fields chunk with the records stream
        let fields_chunk = once(future::ok::<_, actix_web::Error>(Bytes::from(format!(
            "{}\n",
            fields_json
        ))));
        Box::pin(fields_chunk.chain(records_stream))
            as Pin<Box<dyn Stream<Item = Result<Bytes, actix_web::Error>>>>
    } else {
        let mut batch_processor = create_batch_processor(send_null);
        let stream = records_stream
            .map(move |batch_result| batch_processor(batch_result.map_err(QueryError::from)));
        Box::pin(stream) as Pin<Box<dyn Stream<Item = Result<Bytes, actix_web::Error>>>>
    };

    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .insert_header((TIME_ELAPSED_HEADER, total_time.as_str()))
        .streaming(stream))
}

fn create_batch_processor(
    send_null: bool,
) -> impl FnMut(Result<RecordBatch, QueryError>) -> Result<Bytes, actix_web::Error> {
    move |batch_result| match batch_result {
        Ok(batch) => {
            let response = QueryResponse {
                records: vec![batch],
                fields: Vec::new(),
                fill_null: send_null,
                with_fields: false,
            }
            .to_json()
            .map_err(|e| {
                error!("Failed to parse record batch into JSON: {}", e);
                actix_web::error::ErrorInternalServerError(e)
            })?;
            Ok(Bytes::from(format!("{}\n", response)))
        }
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}
pub async fn get_counts(
    req: HttpRequest,
    counts_request: Json<CountsRequest>,
) -> Result<impl Responder, QueryError> {
    let creds = extract_session_key_from_req(&req)?;
    let permissions = Users.get_permissions(&creds);

    // does user have access to table?
    user_auth_for_datasets(&permissions, &[counts_request.stream.clone()])?;

    let records = counts_request.get_bin_density().await?;

    Ok(web::Json(CountsResponse {
        fields: vec!["start_time".into(), "end_time".into(), "count".into()],
        records,
    }))
}

pub async fn update_schema_when_distributed(tables: &Vec<String>) -> Result<(), EventError> {
    // if the mode is query or prism, we need to update the schema in memory
    // no need to commit schema to storage
    // as the schema is read from memory everytime
    if PARSEABLE.options.mode == Mode::Query || PARSEABLE.options.mode == Mode::Prism {
        for table in tables {
            if let Ok(new_schema) = fetch_schema(table).await {
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
    let store = PARSEABLE.storage.get_object_store();
    let storage_streams = store.list_streams().await.unwrap();
    for stream_name in storage_streams {
        if !querier_streams.contains(&stream_name) {
            let _ = PARSEABLE
                .create_stream_and_schema_from_storage(&stream_name)
                .await;
        }
    }
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

            if !query.streaming {
                query.streaming = params.get("streaming").cloned().unwrap_or(false);
            }

            Ok(query)
        };

        Box::pin(fut)
    }
}

pub async fn into_query(
    query: &Query,
    session_state: &SessionState,
    time_range: TimeRange,
) -> Result<LogicalQuery, QueryError> {
    if query.query.is_empty() {
        return Err(QueryError::EmptyQuery);
    }

    if query.start_time.is_empty() {
        return Err(QueryError::EmptyStartTime);
    }

    if query.end_time.is_empty() {
        return Err(QueryError::EmptyEndTime);
    }

    Ok(crate::query::Query {
        raw_logical_plan: session_state.create_logical_plan(&query.query).await?,
        time_range,
        filter_tag: query.filter_tags.clone(),
    })
}

/// unused for now, might need it in the future
#[allow(unused)]
fn transform_query_for_ingestor(query: &Query) -> Option<Query> {
    if query.query.is_empty() {
        return None;
    }

    if query.start_time.is_empty() {
        return None;
    }

    if query.end_time.is_empty() {
        return None;
    }

    let end_time: DateTime<Utc> = if query.end_time == "now" {
        Utc::now()
    } else {
        DateTime::parse_from_rfc3339(&query.end_time)
            .ok()?
            .with_timezone(&Utc)
    };

    let start_time = end_time - chrono::Duration::minutes(1);
    // when transforming the query, the ingestors are forced to return an array of values
    let q = Query {
        query: query.query.clone(),
        fields: false,
        filter_tags: query.filter_tags.clone(),
        send_null: query.send_null,
        start_time: start_time.to_rfc3339(),
        end_time: end_time.to_rfc3339(),
        streaming: query.streaming,
    };

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
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("CustomError: {0}")]
    CustomError(String),
    #[error("No available queriers found")]
    NoAvailableQuerier,
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
