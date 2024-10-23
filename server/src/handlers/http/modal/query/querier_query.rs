
use actix_web::http::header::ContentType;
use actix_web::web::{self, Json};
use actix_web::{FromRequest, HttpRequest, Responder};
use anyhow::anyhow;
use arrow_flight::{FlightClient, Ticket};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::common::tree_node::TreeNode;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use futures::TryStreamExt;
use futures_util::Future;
use http::{header, StatusCode};
use itertools::Itertools;
use rand::seq::SliceRandom;
use reqwest::Client;
use serde_json::{json, Value};
use tonic::transport::{Channel, Uri};
use tonic::Status;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use crate::event::error::EventError;
use crate::handlers::http::fetch_schema;
use crate::handlers::http::modal::query_server::QUERY_ROUTING;
use crate::handlers::http::modal::{QuerierMetadata, LEADER};
use crate::handlers::http::query::{authorize_and_set_filter_tags, get_results_from_cache, into_query, put_results_in_cache, update_schema_when_distributed, Query, QueryError};
use crate::rbac::map::SessionKey;
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


/// If leader, decide which querier will respond to the query
/// if no querier is free or if only leader is alive, leader processes query 
pub async fn query_coordinator(req: HttpRequest, body: Bytes) -> Result<impl Responder, QueryError> {
    let query_request: Query = serde_json::from_slice(&body)
        .map_err(|err| {
            log::error!("Error while converting bytes to Query");
            QueryError::Anyhow(anyhow::Error::msg(err.to_string()))
        })?;

    if LEADER.lock().is_leader() {
        // if leader, then fetch a query node to reroute the query to
        QUERY_ROUTING.lock().await.check_liveness().await;
        let node = QUERY_ROUTING.lock().await.get_query_node().await;
        log::warn!("Selected node- {node:?}");
        let fill_null = query_request.send_null.clone();
        let with_fields = query_request.fields.clone();

        match query_helper(query_request, node.clone()).await {
            Ok(records) => {
                match records.get(0) {
                    Some(record) => {
                        let fields = record
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name())
                            .cloned()
                            .collect_vec();
    
                            QUERY_ROUTING.lock().await.reinstate_node(node);
                        
                        return Ok(
                            QueryResponse {
                                records,
                                fields,
                                fill_null,
                                with_fields
                            }.to_http()?
                        )
                    },
                    None => {
                        QUERY_ROUTING.lock().await.reinstate_node(node);
                        return Err(QueryError::Anyhow(anyhow::Error::msg("no records")))
                    },
                }
            },
            Err(e) => {
                QUERY_ROUTING.lock().await.reinstate_node(node);
                return Err(QueryError::Anyhow(anyhow::Error::msg(e.to_string())))
            }
        }
    } else {
        // this should not happen
        Err(QueryError::Anyhow(anyhow::Error::msg("Query should be a flight request")))
    }
    
}


async fn query_helper(query_request: Query, node: QuerierMetadata) -> Result<Vec<RecordBatch>, Status> {

    let sql = query_request.query;
    let start_time = query_request.start_time;
    let end_time = query_request.end_time;
    let out_ticket = json!({
        "query": sql,
        "startTime": start_time,
        "endTime": end_time
    })
    .to_string();

    let mut hasher = DefaultHasher::new();
    out_ticket.hash(&mut hasher);
    let hashed_query = hasher.finish();

    // send a poll_info request
    let url = node
        .domain_name
        .rsplit_once(':')
        .ok_or(Status::failed_precondition(
            "Ingestor metadata is courupted",
        ))
        .unwrap()
        .0;
    let url = format!("{}:{}", url, node.flight_port);
    let url = url
        .parse::<Uri>()
        .map_err(|_| Status::failed_precondition("querier metadata is courupted")).unwrap();
    let channel = Channel::builder(url)
        .connect()
        .await
        .map_err(|err| Status::failed_precondition(err.to_string())).unwrap();

    let client = FlightClient::new(channel);

    let inn = client
    .into_inner()
    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
    .max_decoding_message_size(usize::MAX)
    .max_encoding_message_size(usize::MAX);

    let mut client = FlightClient::new_from_inner(inn);

    client.add_header("authorization", &node.token).unwrap();

    // let mut descriptor = FlightDescriptor{
    //     cmd: out_ticket.into(),
    //     path: vec![hashed_query.to_string()],
    //     ..Default::default()
    // };
    // let mut response = Vec::new();

    // loop {
    //     match client.poll_flight_info(descriptor.clone()).await {
    //         Ok(poll_info) => {
    //             // in case we got no error, we either have the result or the server is still working on the result
    //             match poll_info.info {
    //                 Some(info) => {
    //                     // this means query is complete, expect FlightEndpoint
    //                     let endpoints = info.endpoint;

                        
    //                     for endpoint in endpoints {
    //                         match client.do_get(endpoint.ticket.unwrap()).await {
    //                             Ok(batch_stream) => response.push(batch_stream),
    //                             Err(err) => {
    //                                 // there was an error, depending upon its type decide whether we need to re-try the request
    //                                 log::error!("do_get error- {err:?}");
    //                             },
    //                         }
    //                     }
    //                 },
    //                 None => {
    //                     // this means query is still running, expect FlightDescriptor
    //                     descriptor = poll_info.flight_descriptor.unwrap();
    //                 },
    //             }
    //         },
    //         Err(err) => {
    //             log::error!("poll_info error- {err:?}");
    //         },
    //     }
    // }

    Ok(client.do_get(Ticket{
        ticket: out_ticket.into()
    }).await?
    .try_collect()
    .await?)
}


pub async fn querier_query_helper(
    query_request: Query,
    cache_results: Option<&str>,
    show_cached: Option<&str>,
    user_id: Option<&str>,
    creds: SessionKey
) -> Result<QueryResponse, QueryError> {
    let session_state = QUERY_SESSION.state();

    // get the logical plan and extract the table name
    let raw_logical_plan = session_state
        .create_logical_plan(&query_request.query)
        .await?;

    // create a visitor to extract the table name
    let mut visitor = TableScanVisitor::default();
    let _ = raw_logical_plan.visit(&mut visitor);
    let stream = visitor
        .top()
        .ok_or_else(|| QueryError::MalformedQuery("Table Name not found in SQL"))?;

    let query_cache_manager = QueryCacheManager::global(CONFIG.parseable.query_cache_size)
        .await
        .unwrap_or(None);

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
        return Ok(results);
    };

    let tables = visitor.into_inner();
    update_schema_when_distributed(tables).await?;
    let mut query: LogicalQuery = into_query(&query_request, &session_state).await?;

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
    };

    let time = time.elapsed().as_secs_f64();

    QUERY_EXECUTE_TIME
        .with_label_values(&[&table_name])
        .observe(time);

    Ok(response)
}