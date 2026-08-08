/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

use actix_web::{
    HttpRequest, HttpResponse,
    http::{
        StatusCode,
        header::{HeaderMap, HeaderName, HeaderValue},
    },
    web::Json,
};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, warn};

use crate::{
    alerts::alerts_utils::get_filter_string,
    event::{DEFAULT_TIMESTAMP_KEY, format::LogSource},
    handlers::http::{
        cluster::send_query_request,
        middleware::{CLUSTER_SECRET, CLUSTER_SECRET_HEADER},
        query::{Query, QueryError, create_streams_for_distributed, get_records_and_fields},
    },
    option::Mode,
    parseable::{DEFAULT_TENANT, PARSEABLE, StreamNotFound},
    query::CountConditions,
    rbac::map::SessionKey,
    tenants::TENANT_METADATA,
    utils::{
        actix::extract_session_key_from_req,
        arrow::record_batches_to_json,
        get_tenant_id_from_request, get_user_from_request,
        time::{TimeRange, count_api_bin_interval},
    },
};

const DEFAULT_TRACE_LIMIT: usize = 500;
const MAX_TRACE_LIMIT: usize = 1000;
const TRACE_LIST_REQUIRED_FIELDS: &[&str] = &[
    "service.name",
    "span_name",
    "span_duration_ns",
    "span_trace_id",
    "span_span_id",
    "span_start_time_unix_nano",
    "span_start_time_unix_nano_epoch",
    "span_status_code",
    "span_parent_span_id",
    "p_timestamp",
];
const TRACE_DETAIL_REQUIRED_FIELDS: &[&str] = TRACE_LIST_REQUIRED_FIELDS;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraceListRequest {
    pub dataset: String,
    pub service_name: Option<String>,
    pub start_time: String,
    pub end_time: String,
    pub sort_by: Option<TraceSortBy>,
    pub conditions: Option<CountConditions>,
    #[serde(alias = "option")]
    pub options: Option<TraceListOption>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TraceSortBy {
    #[default]
    MostRecent,
    LeastRecent,
    LongestFirst,
    ShortestFirst,
    MostSpans,
    LeastSpans,
}

impl TraceSortBy {
    fn order_by(self) -> (&'static str, &'static str) {
        match self {
            Self::MostRecent => ("span_start_time_unix_nano_epoch", "DESC"),
            Self::LeastRecent => ("span_start_time_unix_nano_epoch", "ASC"),
            Self::LongestFirst => ("span_duration_ns", "DESC"),
            Self::ShortestFirst => ("span_duration_ns", "ASC"),
            Self::MostSpans => ("total_span_count", "DESC"),
            Self::LeastSpans => ("total_span_count", "ASC"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TraceListOption {
    Traces,
    ErrorSpans,
    #[default]
    Spans,
}

impl TraceListOption {
    fn result_filter(self) -> &'static str {
        match self {
            Self::Traces => "COALESCE(span_parent_span_id, '') = ''",
            Self::ErrorSpans => "error_count > 0 AND span_status_code = 2",
            Self::Spans => "1=1",
        }
    }

    fn count_filter(self, alias: &str) -> String {
        match self {
            Self::Traces => format!("COALESCE({alias}.\"span_parent_span_id\", '') = ''"),
            Self::ErrorSpans => format!("{alias}.\"span_status_code\" = 2"),
            Self::Spans => "1=1".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraceDetailRequest {
    pub dataset: String,
    pub trace_id: String,
    pub start_time: String,
    pub end_time: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TraceListResponse {
    count: u64,
    offset: usize,
    limit: usize,
    records: Vec<Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TraceDetailResponse {
    start_time: String,
    end_time: String,
    records: Vec<Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum TraceError {
    #[error("Query error: {0}")]
    Query(#[from] QueryError),
    #[error("{0}")]
    TimeParse(String),
    #[error("{0}")]
    StreamNotFound(#[from] StreamNotFound),
    #[error("{0}")]
    BadRequest(String),
    #[error("Trace not found: {0}")]
    TraceNotFound(String),
}

impl actix_web::ResponseError for TraceError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Query(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::TimeParse(_) | Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::StreamNotFound(_) | Self::TraceNotFound(_) => StatusCode::NOT_FOUND,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .json(serde_json::json!({ "error": self.to_string() }))
    }
}

pub async fn list_traces(
    req: HttpRequest,
    Json(body): Json<TraceListRequest>,
) -> Result<HttpResponse, TraceError> {
    let limit = body.limit.unwrap_or(DEFAULT_TRACE_LIMIT);
    if limit == 0 || limit > MAX_TRACE_LIMIT {
        return Err(TraceError::BadRequest(format!(
            "limit must be between 1 and {MAX_TRACE_LIMIT}, got {limit}"
        )));
    }
    let offset = body.offset.unwrap_or(0);
    let service_name = body
        .service_name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty());
    if body.service_name.is_some() && service_name.is_none() {
        return Err(TraceError::BadRequest(
            "serviceName must not be empty when provided".to_string(),
        ));
    }

    let tenant_id = get_tenant_id_from_request(&req);
    create_streams_for_distributed(vec![body.dataset.clone()], &tenant_id)
        .await
        .map_err(|error| TraceError::BadRequest(error.to_string()))?;
    let time_range = parse_time_range(&body.start_time, &body.end_time)?;
    let dataset_info =
        validate_trace_dataset(&body.dataset, &tenant_id, TRACE_LIST_REQUIRED_FIELDS)?;
    let context = TraceSqlContext::new(
        &body.dataset,
        &dataset_info.time_column,
        &time_range,
        service_name,
    );
    let conditions = build_conditions_filter(body.conditions.as_ref())?;
    let option = body.options.unwrap_or_default();
    let sort_by = body.sort_by.unwrap_or_default();
    let target = query_target(&req, &tenant_id)?;
    let start_time = time_range.start.to_rfc3339();
    let end_time = time_range.end.to_rfc3339();

    let (records, count_records) = tokio::try_join!(
        execute_trace_query(
            "traces/list",
            target.clone(),
            build_trace_list_sql(&context, &conditions, option, sort_by, offset, limit),
            &start_time,
            &end_time,
            &tenant_id,
        ),
        execute_trace_query(
            "traces/list/count",
            target,
            build_trace_count_sql(&context, &conditions, option),
            &start_time,
            &end_time,
            &tenant_id,
        ),
    )?;
    let count = count_records
        .first()
        .and_then(|record| record.get("count"))
        .and_then(json_u64)
        .unwrap_or(0);

    Ok(HttpResponse::Ok().json(TraceListResponse {
        count,
        offset,
        limit,
        records,
    }))
}

pub async fn get_trace_detail(
    req: HttpRequest,
    Json(body): Json<TraceDetailRequest>,
) -> Result<HttpResponse, TraceError> {
    let trace_id = body.trace_id.trim();
    if trace_id.is_empty() {
        return Err(TraceError::BadRequest("traceId is required".to_string()));
    }

    let tenant_id = get_tenant_id_from_request(&req);
    create_streams_for_distributed(vec![body.dataset.clone()], &tenant_id)
        .await
        .map_err(|error| TraceError::BadRequest(error.to_string()))?;
    let discovery_range = parse_time_range(&body.start_time, &body.end_time)?;
    let dataset_info =
        validate_trace_dataset(&body.dataset, &tenant_id, TRACE_DETAIL_REQUIRED_FIELDS)?;
    let target = query_target(&req, &tenant_id)?;

    let bounds = execute_trace_query(
        "traces/detail/bounds",
        target.clone(),
        build_trace_bounds_sql(&body.dataset, trace_id),
        &discovery_range.start.to_rfc3339(),
        &discovery_range.end.to_rfc3339(),
        &tenant_id,
    )
    .await?;
    let bounds = bounds.first().ok_or_else(|| {
        TraceError::TraceNotFound(format!("{trace_id} in dataset '{}'", body.dataset))
    })?;
    let start_time = bounds
        .get("start_time")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            TraceError::TraceNotFound(format!("{trace_id} in dataset '{}'", body.dataset))
        })?;
    let end_time = bounds
        .get("end_time")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            TraceError::TraceNotFound(format!("{trace_id} in dataset '{}'", body.dataset))
        })?;
    let start_time = parse_timestamp(start_time)?;
    let end_time = parse_timestamp(end_time)?;

    // Query ranges are minute-aligned internally. Include the minute containing
    // the final span so traces contained in one minute are not truncated.
    let records = execute_trace_query(
        "traces/detail",
        target,
        build_trace_detail_sql(&body.dataset, trace_id, dataset_info.has_event_name),
        &start_time.to_rfc3339(),
        &(end_time + Duration::minutes(1)).to_rfc3339(),
        &tenant_id,
    )
    .await?;

    Ok(HttpResponse::Ok().json(TraceDetailResponse {
        start_time: start_time.to_rfc3339(),
        end_time: end_time.to_rfc3339(),
        records,
    }))
}

fn parse_time_range(start_time: &str, end_time: &str) -> Result<TimeRange, TraceError> {
    TimeRange::parse_human_time(start_time, end_time)
        .map_err(|error| TraceError::TimeParse(error.to_string()))
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, TraceError> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(timestamp.and_utc());
        }
    }
    Err(TraceError::TimeParse(format!(
        "Invalid timestamp returned for trace: {value}"
    )))
}

fn build_conditions_filter(
    conditions: Option<&CountConditions>,
) -> Result<Option<String>, TraceError> {
    conditions
        .and_then(|conditions| conditions.conditions.as_ref())
        .map(get_filter_string)
        .transpose()
        .map_err(TraceError::BadRequest)
}

fn json_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| value.try_into().ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

struct TraceDatasetInfo {
    time_column: String,
    has_event_name: bool,
}

fn validate_trace_dataset(
    dataset: &str,
    tenant_id: &Option<String>,
    required_fields: &[&str],
) -> Result<TraceDatasetInfo, TraceError> {
    let stream = PARSEABLE.get_stream(dataset, tenant_id)?;
    if !stream
        .get_log_source()
        .iter()
        .any(|source| source.log_source_format == LogSource::OtelTraces)
    {
        return Err(TraceError::BadRequest(format!(
            "Dataset '{dataset}' must have log source 'otel-traces'"
        )));
    }

    let time_column = stream
        .get_time_partition()
        .unwrap_or_else(|| DEFAULT_TIMESTAMP_KEY.to_string());
    let schema = stream.get_schema_raw();
    let mut missing: Vec<&str> = required_fields
        .iter()
        .filter(|field| !schema.contains_key::<str>(field))
        .copied()
        .collect();
    if !schema.contains_key(&time_column) {
        missing.push(&time_column);
    }
    if !missing.is_empty() {
        return Err(TraceError::BadRequest(format!(
            "Dataset '{dataset}' is missing required fields for the traces API: {}",
            missing.join(", ")
        )));
    }
    Ok(TraceDatasetInfo {
        time_column,
        has_event_name: schema.contains_key("event_name"),
    })
}

struct TraceSqlContext {
    table: String,
    time_column: String,
    start_time: String,
    end_time: String,
    service_name: Option<String>,
}

impl TraceSqlContext {
    fn new(
        dataset: &str,
        time_column: &str,
        time_range: &TimeRange,
        service_name: Option<&str>,
    ) -> Self {
        // Validate the range through the shared count interval helper as the
        // query APIs use the same supported bounds.
        let _ = count_api_bin_interval(&time_range.start, &time_range.end);
        Self {
            table: quote_identifier(dataset),
            time_column: quote_identifier(time_column),
            start_time: time_range.start.to_rfc3339(),
            end_time: time_range.end.to_rfc3339(),
            service_name: service_name.map(escape_sql_string_literal),
        }
    }

    fn filter(&self, alias: &str) -> String {
        let prefix = if alias.is_empty() {
            String::new()
        } else {
            format!("{alias}.")
        };
        let mut filter = format!(
            "{prefix}{} >= TIMESTAMP '{}' AND {prefix}{} <= TIMESTAMP '{}'",
            self.time_column, self.start_time, self.time_column, self.end_time
        );
        if let Some(service_name) = &self.service_name {
            filter.push_str(&format!(" AND {prefix}\"service.name\" = '{service_name}'"));
        }
        filter
    }
}

fn build_trace_source_filter(
    context: &TraceSqlContext,
    conditions: &Option<String>,
    alias: &str,
) -> String {
    let mut filter = format!(
        "{} AND {alias}.\"service.name\" IS NOT NULL",
        context.filter(alias)
    );
    if let Some(conditions) = conditions {
        filter.push_str(&format!(" AND ({conditions})"));
    }
    filter
}

fn build_trace_list_sql(
    context: &TraceSqlContext,
    conditions: &Option<String>,
    option: TraceListOption,
    sort_by: TraceSortBy,
    offset: usize,
    limit: usize,
) -> String {
    let table = &context.table;
    let source_filter = build_trace_source_filter(context, conditions, "t");
    let result_filter = option.result_filter();
    let (sort_column, sort_direction) = sort_by.order_by();
    format!(
        r#"WITH trace_stats AS (
  SELECT
    t."service.name",
    t."span_name",
    t."span_duration_ns",
    t."span_trace_id",
    t."span_span_id",
    t."span_start_time_unix_nano",
    t."span_start_time_unix_nano_epoch",
    t."p_timestamp",
    t."span_status_code",
    t."span_parent_span_id",
    COUNT(DISTINCT t."span_span_id") OVER (
      PARTITION BY t."span_trace_id"
    ) AS total_span_count,
    COUNT(DISTINCT CASE WHEN t."span_status_code" = 2 THEN t."span_span_id" END) OVER (
      PARTITION BY t."span_trace_id"
    ) AS error_count
  FROM {table} t
  WHERE {source_filter}
)
SELECT DISTINCT
  "service.name",
  "span_name",
  "span_duration_ns",
  "span_trace_id",
  "span_span_id",
  "span_start_time_unix_nano",
  "span_start_time_unix_nano_epoch",
  "p_timestamp",
  total_span_count,
  error_count
FROM trace_stats
WHERE {result_filter}
ORDER BY {sort_column} {sort_direction}, "span_start_time_unix_nano_epoch" DESC, "span_span_id" ASC
OFFSET {offset}
LIMIT {limit}"#
    )
}

fn build_trace_count_sql(
    context: &TraceSqlContext,
    conditions: &Option<String>,
    option: TraceListOption,
) -> String {
    let table = &context.table;
    let source_filter = build_trace_source_filter(context, conditions, "t");
    let option_filter = option.count_filter("t");
    format!(
        r#"SELECT COUNT(DISTINCT t."span_span_id") AS count
FROM {table} t
WHERE {source_filter} AND {option_filter}"#
    )
}

fn build_trace_bounds_sql(dataset: &str, trace_id: &str) -> String {
    let table = quote_identifier(dataset);
    let trace_id = escape_sql_string_literal(trace_id);
    format!(
        r#"SELECT
  MIN("p_timestamp") AS start_time,
  MAX("p_timestamp") AS end_time
FROM {table}
WHERE "span_trace_id" = '{trace_id}'"#
    )
}

fn build_trace_detail_sql(dataset: &str, trace_id: &str, has_event_name: bool) -> String {
    let table = quote_identifier(dataset);
    let trace_id = escape_sql_string_literal(trace_id);
    let event_name_projection = if has_event_name {
        "\"event_name\",\n    "
    } else {
        ""
    };
    let event_count_expr = if has_event_name {
        "SUM(CASE WHEN \"event_name\" IS NOT NULL THEN 1 ELSE 0 END)"
    } else {
        "0"
    };
    format!(
        r#"WITH RECURSIVE
trace_spans AS (
  SELECT
    "span_span_id",
    "span_parent_span_id",
    "service.name",
    "span_name",
    "span_duration_ns",
    "span_start_time_unix_nano",
    "span_start_time_unix_nano_epoch",
    "span_trace_id",
    "span_status_code",
    {event_name_projection}"p_timestamp"
  FROM {table}
  WHERE "span_trace_id" = '{trace_id}'
),
deduped AS (
  SELECT
    "span_span_id",
    "span_parent_span_id",
    "service.name",
    "span_name",
    "span_duration_ns",
    "span_start_time_unix_nano",
    "span_start_time_unix_nano_epoch",
    "span_trace_id",
    CAST(MAX(CASE WHEN "span_status_code" = 2 THEN 1 ELSE 0 END) AS BOOLEAN) AS has_error,
    {event_count_expr} AS event_count,
    MIN("p_timestamp") AS p_timestamp
  FROM trace_spans
  GROUP BY
    "span_span_id",
    "span_parent_span_id",
    "service.name",
    "span_name",
    "span_duration_ns",
    "span_start_time_unix_nano",
    "span_start_time_unix_nano_epoch",
    "span_trace_id"
),
all_span_ids AS (
  SELECT DISTINCT "span_span_id" FROM deduped
),
span_hierarchy AS (
  SELECT "span_span_id", p_timestamp, 0 AS level
  FROM deduped
  WHERE COALESCE("span_parent_span_id", '') = ''
    OR "span_parent_span_id" NOT IN (SELECT "span_span_id" FROM all_span_ids)
  UNION ALL
  SELECT s."span_span_id", s.p_timestamp, sh.level + 1
  FROM deduped s
  INNER JOIN span_hierarchy sh ON s."span_parent_span_id" = sh."span_span_id"
),
span_levels AS (
  SELECT "span_span_id", MIN(level) AS level
  FROM span_hierarchy
  GROUP BY "span_span_id"
)
SELECT
  d."span_span_id",
  d."span_parent_span_id",
  d."service.name",
  d."span_name",
  d."span_duration_ns",
  d."span_start_time_unix_nano",
  d."span_start_time_unix_nano_epoch",
  d."span_trace_id",
  d.has_error,
  sl.level,
  COUNT(*) OVER () AS total_span_count,
  d.event_count
FROM deduped d
INNER JOIN span_levels sl ON d."span_span_id" = sl."span_span_id"
ORDER BY sl.level, d."span_start_time_unix_nano""#
    )
}

#[derive(Clone)]
enum TraceQueryTarget {
    Local(SessionKey),
    Remote(Option<HeaderMap>),
}

fn query_target(
    req: &HttpRequest,
    tenant_id: &Option<String>,
) -> Result<TraceQueryTarget, TraceError> {
    match PARSEABLE.options.mode {
        Mode::All | Mode::Query => Ok(TraceQueryTarget::Local(
            extract_session_key_from_req(req).map_err(QueryError::ActixError)?,
        )),
        Mode::Prism => Ok(TraceQueryTarget::Remote(build_auth_headers(req, tenant_id))),
        mode => Err(TraceError::BadRequest(format!(
            "Trace queries are not available in {mode:?} mode"
        ))),
    }
}

async fn execute_trace_query(
    query_name: &'static str,
    target: TraceQueryTarget,
    sql: String,
    start_time: &str,
    end_time: &str,
    tenant_id: &Option<String>,
) -> Result<Vec<Value>, TraceError> {
    let request = Query {
        query: sql,
        start_time: start_time.to_string(),
        end_time: end_time.to_string(),
        send_null: true,
        fields: false,
        streaming: false,
        filter_tags: None,
    };

    match target {
        TraceQueryTarget::Local(credentials) => {
            let (records, _) = get_records_and_fields(&request, &credentials, tenant_id)
                .await
                .map_err(|error| {
                    error!("traces/{query_name} query failed: {error:?}");
                    TraceError::Query(error)
                })?;
            let records = records.unwrap_or_default();
            let records = record_batches_to_json(&records)
                .map_err(|error| TraceError::BadRequest(error.to_string()))?;
            Ok(records.into_iter().map(Value::Object).collect())
        }
        TraceQueryTarget::Remote(auth) => {
            let (response, _) = send_query_request(auth, &request, tenant_id)
                .await
                .map_err(|error| {
                    error!("traces/{query_name} query failed: {error:?}");
                    TraceError::Query(error)
                })?;
            if let Some(records) = response.as_array() {
                return Ok(records.clone());
            }
            if let Some(records) = response
                .as_object()
                .and_then(|object| object.get("records"))
                .and_then(Value::as_array)
            {
                return Ok(records.clone());
            }
            warn!("traces/{query_name} unexpected response: {response}");
            Err(TraceError::BadRequest(format!(
                "Unexpected query response: {response}"
            )))
        }
    }
}

fn build_auth_headers(req: &HttpRequest, tenant_id: &Option<String>) -> Option<HeaderMap> {
    let (_, hash) = CLUSTER_SECRET.get()?;
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let mut headers = HeaderMap::new();
    if let Some(auth) = TENANT_METADATA.get_global_query_auth(tenant)
        && let Ok(value) = HeaderValue::from_str(&auth)
    {
        headers.insert(HeaderName::from_static("authorization"), value);
    }
    if let Ok(value) = HeaderValue::from_str(hash) {
        headers.insert(HeaderName::from_static(CLUSTER_SECRET_HEADER), value);
    }
    if let Ok(value) = HeaderValue::from_str(tenant) {
        headers.insert(HeaderName::from_static("intra-cluster-tenant"), value);
    }
    if let Ok(user) = get_user_from_request(req)
        && let Ok(value) = HeaderValue::from_str(&user)
    {
        headers.insert(HeaderName::from_static("intra-cluster-userid"), value);
    }
    Some(headers)
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn escape_sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(service_name: Option<&str>) -> TraceSqlContext {
        let range =
            TimeRange::parse_human_time("2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z").unwrap();
        TraceSqlContext::new("traces", "p_timestamp", &range, service_name)
    }

    #[test]
    fn trace_list_applies_filter_sort_and_pagination() {
        let sql = build_trace_list_sql(
            &context(Some("checkout")),
            &Some("\"deployment\" = 'prod'".to_string()),
            TraceListOption::ErrorSpans,
            TraceSortBy::MostSpans,
            20,
            10,
        );
        assert!(sql.contains("\"service.name\" = 'checkout'"));
        assert!(sql.contains("\"deployment\" = 'prod'"));
        assert!(sql.contains("error_count > 0 AND span_status_code = 2"));
        assert!(sql.contains("ORDER BY total_span_count DESC"));
        assert!(sql.contains("OFFSET 20\nLIMIT 10"));
    }

    #[test]
    fn trace_count_matches_selected_option() {
        let traces = build_trace_count_sql(&context(None), &None, TraceListOption::Traces);
        assert!(traces.contains("COUNT(DISTINCT t.\"span_span_id\") AS count"));
        assert!(traces.contains("COALESCE(t.\"span_parent_span_id\", '') = ''"));

        let errors = build_trace_count_sql(&context(None), &None, TraceListOption::ErrorSpans);
        assert!(errors.contains("t.\"span_status_code\" = 2"));
    }

    #[test]
    fn trace_detail_sql_escapes_id_and_builds_hierarchy() {
        let bounds = build_trace_bounds_sql("traces", "abc' OR 1=1 --");
        assert!(bounds.contains("\"span_trace_id\" = 'abc'' OR 1=1 --'"));

        let detail = build_trace_detail_sql("traces", "abc", true);
        assert!(detail.contains("WITH RECURSIVE"));
        assert!(detail.contains("\"event_name\""));
        assert!(detail.contains("INNER JOIN span_hierarchy"));
        assert!(detail.contains("COUNT(*) OVER () AS total_span_count"));
    }

    #[test]
    fn trace_detail_sql_omits_unavailable_event_name() {
        let detail = build_trace_detail_sql("traces", "abc", false);
        assert!(!detail.contains("\"event_name\""));
        assert!(detail.contains("0 AS event_count"));
    }

    #[test]
    fn trace_detail_request_requires_time_range() {
        let request: TraceDetailRequest = serde_json::from_value(serde_json::json!({
            "dataset": "traces",
            "traceId": "abc",
            "startTime": "1h",
            "endTime": "now"
        }))
        .unwrap();
        assert_eq!(request.start_time, "1h");
        assert_eq!(request.end_time, "now");

        assert!(
            serde_json::from_value::<TraceDetailRequest>(serde_json::json!({
                "dataset": "traces",
                "traceId": "abc"
            }))
            .is_err()
        );
    }

    #[test]
    fn trace_timestamp_parser_accepts_arrow_text_format() {
        let timestamp = parse_timestamp("2026-01-01 12:34:56.123").unwrap();
        assert_eq!(timestamp.to_rfc3339(), "2026-01-01T12:34:56.123+00:00");
    }

    #[test]
    fn dataset_name_is_quoted_as_identifier() {
        let range =
            TimeRange::parse_human_time("2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z").unwrap();
        let context = TraceSqlContext::new("traces\"archive", "p_timestamp", &range, None);
        assert_eq!(context.table, "\"traces\"\"archive\"");
    }
}
