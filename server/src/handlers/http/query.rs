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
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use datafusion::common::tree_node::TreeNode;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use futures_util::Future;
use http::StatusCode;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use crate::event::error::EventError;
use crate::handlers::http::fetch_schema;
use crate::metadata::STREAM_INFO;
use arrow_array::RecordBatch;

use crate::event::commit_schema;
use crate::handlers::{CACHE_RESULTS_HEADER_KEY, CACHE_VIEW_HEADER_KEY, USER_ID_HEADER_KEY};
use crate::localcache::CacheError;
use crate::metrics::QUERY_EXECUTE_TIME;
use crate::option::{Mode, CONFIG};
use crate::query::error::ExecuteError;
use crate::query::Query as LogicalQuery;
use crate::query::{TableScanVisitor, QUERY_SESSION};
use crate::querycache::{CacheMetadata, QueryCacheManager};
use crate::rbac::role::{Action, Permission};
use crate::rbac::Users;
use crate::response::QueryResponse;
use crate::storage::object_storage::commit_schema_to_storage;
use crate::storage::ObjectStorageError;
use crate::utils::actix::extract_session_key_from_req;

use super::modal::utils::logstream_utils::create_stream_and_schema_from_storage;

/// Query Request through http endpoint.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
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

pub async fn query(req: HttpRequest, query_request: Query) -> Result<impl Responder, QueryError> {
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
    // create a visitor to extract the table name
    let mut visitor = TableScanVisitor::default();
    let _ = raw_logical_plan.visit(&mut visitor);
    let stream = visitor
        .top()
        .ok_or_else(|| QueryError::MalformedQuery("Table Name not found in SQL"))?;

    let query_cache_manager = QueryCacheManager::global(CONFIG.parseable.query_cache_size)
        .await
        .unwrap_or(None);

    let cache_results = req
        .headers()
        .get(CACHE_RESULTS_HEADER_KEY)
        .and_then(|value| value.to_str().ok());
    let show_cached = req
        .headers()
        .get(CACHE_VIEW_HEADER_KEY)
        .and_then(|value| value.to_str().ok());
    let user_id = req
        .headers()
        .get(USER_ID_HEADER_KEY)
        .and_then(|value| value.to_str().ok());

    // deal with cached data
    if let Ok(results) = get_results_from_cache(
        show_cached,
        query_cache_manager,
        stream,
        user_id,
        &query_request.start_time,
        &query_request.end_time,
        &query_request.query,
        query_request.send_null,
        query_request.fields,
    )
    .await
    {
        return results.to_http();
    };

    let tables = visitor.into_inner();
    update_schema_when_distributed(tables).await?;
    let mut query: LogicalQuery = into_query(&query_request, &session_state).await?;

    let creds = extract_session_key_from_req(&req)?;
    let permissions = Users.get_permissions(&creds);

    let table_name = query
        .first_table_name()
        .ok_or_else(|| QueryError::MalformedQuery("No table name found in query"))?;

    authorize_and_set_filter_tags(&mut query, permissions, &table_name)?;

    let time = Instant::now();
    let (records, fields) = query.execute(table_name.clone()).await?;
    // deal with cache saving
    if let Err(err) = put_results_in_cache(
        cache_results,
        user_id,
        query_cache_manager,
        &table_name,
        &records,
        query.start.to_rfc3339(),
        query.end.to_rfc3339(),
        query_request.query,
    )
    .await
    {
        log::error!("{}", err);
    };

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

pub async fn update_schema_when_distributed(tables: Vec<String>) -> Result<(), QueryError> {
    if CONFIG.parseable.mode == Mode::Query {
        for table in tables {
            if let Ok(new_schema) = fetch_schema(&table).await {
                // commit schema merges the schema internally and updates the schema in storage.
                commit_schema_to_storage(&table, new_schema.clone()).await?;

                commit_schema(&table, Arc::new(new_schema))?;
            }
        }
    }

    Ok(())
}

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

#[allow(clippy::too_many_arguments)]
pub async fn put_results_in_cache(
    cache_results: Option<&str>,
    user_id: Option<&str>,
    query_cache_manager: Option<&QueryCacheManager>,
    stream: &str,
    records: &[RecordBatch],
    start: String,
    end: String,
    query: String,
) -> Result<(), QueryError> {
    match (cache_results, query_cache_manager) {
        (Some(_), None) => {
            log::warn!(
                "Instructed to cache query results but Query Caching is not Enabled in Server"
            );

            Ok(())
        }
        // do cache
        (Some(should_cache), Some(query_cache_manager)) => {
            if should_cache != "true" {
                log::error!("value of cache results header is false");
                return Err(QueryError::CacheError(CacheError::Other(
                    "should not cache results",
                )));
            }

            let user_id = user_id.ok_or(CacheError::Other("User Id not provided"))?;
            let mut cache = query_cache_manager.get_cache(stream, user_id).await?;

            let cache_key = CacheMetadata::new(query.clone(), start.clone(), end.clone());

            // guard to stop multiple caching of the same content
            if let Some(path) = cache.get_file(&cache_key) {
                log::info!("File already exists in cache, Removing old file");
                cache.delete(&cache_key, path).await?;
            }

            if let Err(err) = query_cache_manager
                .create_parquet_cache(stream, records, user_id, start, end, query)
                .await
            {
                log::error!("Error occured while caching query results: {:?}", err);
                if query_cache_manager
                    .clear_cache(stream, user_id)
                    .await
                    .is_err()
                {
                    log::error!("Error Clearing Unwanted files from cache dir");
                }
            }
            // fallthrough
            Ok(())
        }
        (None, _) => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn get_results_from_cache(
    show_cached: Option<&str>,
    query_cache_manager: Option<&QueryCacheManager>,
    stream: &str,
    user_id: Option<&str>,
    start_time: &str,
    end_time: &str,
    query: &str,
    send_null: bool,
    send_fields: bool,
) -> Result<QueryResponse, QueryError> {
    match (show_cached, query_cache_manager) {
        (Some(_), None) => {
            log::warn!(
                "Instructed to show cached results but Query Caching is not Enabled on Server"
            );
            None
        }
        (Some(should_show), Some(query_cache_manager)) => {
            if should_show != "true" {
                log::error!("value of show cached header is false");
                return Err(QueryError::CacheError(CacheError::Other(
                    "should not return cached results",
                )));
            }

            let user_id =
                user_id.ok_or_else(|| QueryError::Anyhow(anyhow!("User Id not provided")))?;

            let mut query_cache = query_cache_manager.get_cache(stream, user_id).await?;

            let (start, end) = parse_human_time(start_time, end_time)?;

            let file_path = query_cache.get_file(&CacheMetadata::new(
                query.to_string(),
                start.to_rfc3339(),
                end.to_rfc3339(),
            ));
            if let Some(file_path) = file_path {
                let (records, fields) = query_cache.get_cached_records(&file_path).await?;
                let response = QueryResponse {
                    records,
                    fields,
                    fill_null: send_null,
                    with_fields: send_fields,
                };

                Some(Ok(response))
            } else {
                None
            }
        }
        (_, _) => None,
    }
    .map_or_else(|| Err(QueryError::CacheMiss), |ret_val| ret_val)
}

pub fn authorize_and_set_filter_tags(
    query: &mut LogicalQuery,
    permissions: Vec<Permission>,
    table_name: &str,
) -> Result<(), QueryError> {
    // check authorization of this query if it references physical table;
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
                if stream == table_name || stream == "*" =>
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

    Ok(())
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

    let (start, end) = parse_human_time(&query.start_time, &query.end_time)?;

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

fn parse_human_time(
    start_time: &str,
    end_time: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>), QueryError> {
    let start: DateTime<Utc>;
    let end: DateTime<Utc>;

    if end_time == "now" {
        end = Utc::now();
        start = end - chrono::Duration::from_std(humantime::parse_duration(start_time)?)?;
    } else {
        start = DateTime::parse_from_rfc3339(start_time)
            .map_err(|_| QueryError::StartTimeParse)?
            .into();
        end = DateTime::parse_from_rfc3339(end_time)
            .map_err(|_| QueryError::EndTimeParse)?
            .into();
    };

    Ok((start, end))
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
    #[error("ObjectStorage Error: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Cache Error: {0}")]
    CacheError(#[from] CacheError),
    #[error("")]
    CacheMiss,
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
