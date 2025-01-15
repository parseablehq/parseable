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
use actix_web::{FromRequest, HttpRequest, HttpResponse, Responder};
use chrono::{DateTime, Utc};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::Column;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Aggregate, LogicalPlan, Projection};
use datafusion::prelude::Expr;
use futures_util::Future;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::error;

use crate::event::error::EventError;
use crate::handlers::http::fetch_schema;
use crate::metadata::STREAM_INFO;

use crate::event::commit_schema;
use crate::metrics::QUERY_EXECUTE_TIME;
use crate::option::{Mode, CONFIG};
use crate::query::error::ExecuteError;
use crate::query::{DateBinRecord, DateBinRequest, Query as LogicalQuery};
use crate::query::{TableScanVisitor, QUERY_SESSION};
use crate::rbac::Users;
use crate::response::QueryResponse;
use crate::storage::object_storage::commit_schema_to_storage;
use crate::storage::ObjectStorageError;
use crate::utils::actix::extract_session_key_from_req;
use crate::utils::time::{TimeParseError, TimeRange};
use crate::utils::user_auth_for_query;

use super::modal::utils::logstream_utils::create_stream_and_schema_from_storage;

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
    pub filter_tags: Option<Vec<String>>,
}
/// DateBin Response.
#[derive(Debug, Serialize, Clone)]
pub struct DateBinResponse {
    pub fields: Vec<String>,
    pub records: Vec<DateBinRecord>,
}

pub async fn query(req: HttpRequest, query_request: Query) -> Result<HttpResponse, QueryError> {
    let session_state = QUERY_SESSION.state();
    let raw_logical_plan = match session_state
        .create_logical_plan(&query_request.query)
        .await
    {
        Ok(raw_logical_plan) => raw_logical_plan,
        Err(_) => {
            //if logical plan creation fails, create streams and try again
            create_streams_for_querier().await;
            session_state
                .create_logical_plan(&query_request.query)
                .await?
        }
    };
    let time_range =
        TimeRange::parse_human_time(&query_request.start_time, &query_request.end_time)?;

    // Create a visitor to extract the table names present in query
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
    let time = Instant::now();

    if let (true, column_name) = is_logical_plan_aggregate_without_filters(&raw_logical_plan) {
        let date_bin_request = DateBinRequest {
            stream: table_name.clone(),
            start_time: query_request.start_time.clone(),
            end_time: query_request.end_time.clone(),
            num_bins: 1,
        };
        let date_bin_records = date_bin_request.get_bin_density().await?;
        let response = if query_request.fields {
            json!({
                "fields": vec![&column_name],
                "records": vec![json!({&column_name: date_bin_records[0].log_count})]
            })
        } else {
            Value::Array(vec![json!({&column_name: date_bin_records[0].log_count})])
        };

        let time = time.elapsed().as_secs_f64();

        QUERY_EXECUTE_TIME
            .with_label_values(&[&table_name])
            .observe(time);

        return Ok(HttpResponse::Ok().json(response));
    }

    user_auth_for_query(&permissions, &tables)?;

    let (records, fields) = query.execute(table_name.clone()).await?;

    let response = QueryResponse {
        records,
        fields,
        fill_null: query_request.send_null,
        with_fields: query_request.fields,
    }
    .to_http()?;

    let time = time.elapsed().as_secs_f64();

    QUERY_EXECUTE_TIME
        .with_label_values(&[&table_name])
        .observe(time);

    Ok(response)
}

pub async fn get_date_bin(
    req: HttpRequest,
    date_bin: Json<DateBinRequest>,
) -> Result<impl Responder, QueryError> {
    let creds = extract_session_key_from_req(&req)?;
    let permissions = Users.get_permissions(&creds);

    // does user have access to table?
    user_auth_for_query(&permissions, &[date_bin.stream.clone()])?;

    let date_bin_records = date_bin.get_bin_density().await?;

    Ok(web::Json(DateBinResponse {
        fields: vec!["date_bin_timestamp".into(), "log_count".into()],
        records: date_bin_records,
    }))
}

pub async fn update_schema_when_distributed(tables: &Vec<String>) -> Result<(), QueryError> {
    if CONFIG.options.mode == Mode::Query {
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
    let querier_streams = STREAM_INFO.list_streams();
    let store = CONFIG.storage().get_object_store();
    let storage_streams = store.list_streams().await.unwrap();
    for stream in storage_streams {
        let stream_name = stream.name;

        if !querier_streams.contains(&stream_name) {
            let _ = create_stream_and_schema_from_storage(&stream_name).await;
        }
    }
}

fn is_logical_plan_aggregate_without_filters(plan: &LogicalPlan) -> (bool, String) {
    match plan {
        LogicalPlan::Projection(Projection { input, expr, .. }) => {
            if let LogicalPlan::Aggregate(Aggregate { input, .. }) = &**input {
                if matches!(&**input, LogicalPlan::TableScan { .. }) && expr.len() == 1 {
                    return match &expr[0] {
                        Expr::Column(Column { name, .. }) => (name == "count(*)", name.clone()),
                        Expr::Alias(Alias {
                            expr: inner_expr,
                            name,
                            ..
                        }) => {
                            let alias_name = name;
                            if let Expr::Column(Column { name, .. }) = &**inner_expr {
                                (name == "count(*)", alias_name.to_string())
                            } else {
                                (false, "".to_string())
                            }
                        }
                        _ => (false, "".to_string()),
                    };
                }
            }
        }
        _ => return (false, "".to_string()),
    }
    (false, "".to_string())
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
    #[error("Evern Error: {0}")]
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
