use std::time::Instant;

use actix_web::{HttpRequest, Responder};
use datafusion::common::tree_node::TreeNode;

use crate::{handlers::{http::query::{authorize_and_set_filter_tags, get_results_from_cache, into_query, put_results_in_cache, update_schema_when_distributed, Query, QueryError}, CACHE_RESULTS_HEADER_KEY, CACHE_VIEW_HEADER_KEY, USER_ID_HEADER_KEY}, metrics::QUERY_EXECUTE_TIME, option::CONFIG, query::{TableScanVisitor, QUERY_SESSION}, querycache::QueryCacheManager, rbac::Users, response::QueryResponse, utils::actix::extract_session_key_from_req};


pub async fn query(req: HttpRequest, query_request: Query) -> Result<impl Responder, QueryError> {
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