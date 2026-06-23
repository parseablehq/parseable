/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use actix_web::web::{self, Json};
use actix_web::{HttpRequest, Responder};
use arrow_schema::DataType;
use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::{Span, debug, info, warn};

use crate::alerts::{alert_structs::Conditions, alerts_utils::get_filter_string};
use crate::event::DEFAULT_TIMESTAMP_KEY;
use crate::handlers::http::query::{
    Query, QueryError, create_streams_for_distributed, get_records_and_fields_for_authorized_query,
};
use crate::metrics::increment_query_calls_by_date;
use crate::parseable::{DEFAULT_TENANT, PARSEABLE};
use crate::rbac::Users;
use crate::utils::actix::extract_session_key_from_req;
use crate::utils::arrow::record_batches_to_json;
use crate::utils::time::truncate_to_minute;
use crate::utils::{get_tenant_id_from_request, user_auth_for_datasets};

const DEFAULT_LOG_CONTEXT_PAGE_SIZE: u64 = 500;
const LOG_CONTEXT_ANCHORED_DUPLICATE: &str = "first";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogContextRequest {
    pub dataset: String,
    pub context_window: Option<String>,
    pub context_start_time: Option<String>,
    pub context_end_time: Option<String>,
    pub p_timestamp: String,
    pub log: Option<String>,
    pub body: Option<String>,
    pub message: Option<String>,
    pub conditions: Option<Conditions>,
    pub page_size: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogContextResponse {
    pub scope: LogContextScope,
    pub context_start_time: String,
    pub context_end_time: String,
    pub limit: u64,
    pub anchor_index: u64,
    pub duplicate_anchor_count: u64,
    pub anchored_duplicate: &'static str,
    pub records: Vec<Value>,
    pub queries: LogContextQueries,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LogContextScope {
    ContextWindow,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogContextQueries {
    pub previous: Option<LogContextQueryPayload>,
    pub next: Option<LogContextQueryPayload>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogContextQueryPayload {
    pub query: String,
    pub start_time: String,
    pub end_time: String,
    pub send_null: bool,
    pub reverse_records: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogContextMatchField {
    name: String,
    value: String,
}

#[derive(Debug, Clone)]
struct LogContextCursor {
    timestamp: DateTime<Utc>,
    match_field: LogContextMatchField,
}

#[tracing::instrument(
    name = "query_context",
    skip(req, context_request),
    fields(
        dataset = tracing::field::Empty,
        tenant = tracing::field::Empty,
        page_size = tracing::field::Empty,
        scope = tracing::field::Empty
    )
)]
pub async fn query_context(
    req: HttpRequest,
    context_request: Json<LogContextRequest>,
) -> Result<impl Responder, QueryError> {
    let context_request = context_request.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let span = Span::current();
    span.record("dataset", tracing::field::display(&context_request.dataset));
    span.record("tenant", tracing::field::debug(&tenant_id));
    info!(
        has_log = context_request.log.is_some(),
        has_body = context_request.body.is_some(),
        has_message = context_request.message.is_some(),
        has_conditions = context_request.conditions.is_some(),
        "query context request received"
    );

    let creds = extract_session_key_from_req(&req)?;
    let permissions = Users.get_permissions(&creds);

    create_streams_for_distributed(vec![context_request.dataset.clone()], &tenant_id).await?;
    let authorized_datasets = vec![context_request.dataset.clone()];
    user_auth_for_datasets(
        &permissions,
        std::slice::from_ref(&context_request.dataset),
        &tenant_id,
    )
    .await?;

    let page_size = normalize_log_context_page_size(context_request.page_size)?;
    span.record("page_size", page_size);
    let anchor_timestamp = parse_log_context_timestamp(&context_request.p_timestamp)?;
    let (context_start_time, context_end_time) = resolve_log_context_bounds(
        anchor_timestamp,
        context_request.context_window.as_deref(),
        context_request.context_start_time.as_deref(),
        context_request.context_end_time.as_deref(),
    )?;
    validate_log_context_anchor_in_bounds(anchor_timestamp, context_start_time, context_end_time)?;
    debug!(
        page_size,
        anchor_timestamp = %anchor_timestamp,
        context_start_time = %context_start_time,
        context_end_time = %context_end_time,
        "query context request normalized"
    );

    let dataset = PARSEABLE.get_stream(&context_request.dataset, &tenant_id)?;
    let schema = dataset.get_schema();
    validate_log_context_schema(schema.as_ref(), DEFAULT_TIMESTAMP_KEY)?;
    let match_fields = normalize_log_context_match_fields(
        &context_request.log,
        &context_request.body,
        &context_request.message,
        schema.as_ref(),
    )?;
    let additional_filter = log_context_additional_filter(&context_request.conditions)?;

    let context_start_time_str = format_log_context_api_time(context_start_time);
    let context_end_time_str = format_log_context_api_time(context_end_time);

    let anchor_count_query = build_log_context_anchor_count_query(
        &context_request.dataset,
        anchor_timestamp,
        context_start_time,
        context_end_time,
        &match_fields,
        additional_filter.as_deref(),
    );
    let duplicate_anchor_count = execute_log_context_anchor_count(
        &anchor_count_query,
        &authorized_datasets,
        &context_start_time_str,
        &context_end_time_str,
        &tenant_id,
    )
    .await?;
    debug!(
        duplicate_anchor_count,
        "query context anchor count resolved"
    );

    let scope = LogContextScope::ContextWindow;
    span.record("scope", tracing::field::debug(&scope));
    info!(
        scope = ?scope,
        duplicate_anchor_count,
        "query context scope selected"
    );

    if duplicate_anchor_count == 0 {
        warn!(
            scope = ?scope,
            "query context anchor row not found"
        );
        return Err(QueryError::CustomError(
            "No log row matched the provided pTimestamp and log/body/message value".to_string(),
        ));
    }

    let fetch_limit = page_size;
    let newer_payload = build_log_context_newer_query_payload(
        &context_request.dataset,
        anchor_timestamp,
        context_start_time,
        context_end_time,
        &match_fields,
        additional_filter.as_deref(),
        &context_start_time_str,
        &context_end_time_str,
        fetch_limit,
    );
    let older_payload = build_log_context_anchor_and_older_query_payload(
        &context_request.dataset,
        anchor_timestamp,
        context_start_time,
        context_end_time,
        &match_fields,
        additional_filter.as_deref(),
        &context_start_time_str,
        &context_end_time_str,
        fetch_limit,
    );

    let (newer_records, older_records) = tokio::try_join!(
        execute_log_context_rows(&newer_payload, &authorized_datasets, &tenant_id),
        execute_log_context_rows(&older_payload, &authorized_datasets, &tenant_id),
    )?;
    let (records, anchor_index) =
        build_log_context_records_window(newer_records, older_records, page_size)?;

    let queries = build_log_context_cursor_queries(
        &context_request.dataset,
        context_start_time,
        context_end_time,
        &match_fields,
        additional_filter.as_deref(),
        &context_start_time_str,
        &context_end_time_str,
        page_size,
        &records,
    )?;

    info!(
        scope = ?scope,
        anchor_index,
        duplicate_anchor_count,
        record_count = records.len(),
        "query context rows fetched"
    );

    let current_date = chrono::Utc::now().date_naive().to_string();
    increment_query_calls_by_date(
        &current_date,
        tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
    );

    Ok(web::Json(LogContextResponse {
        scope,
        context_start_time: context_start_time_str,
        context_end_time: context_end_time_str,
        limit: page_size,
        anchor_index,
        duplicate_anchor_count,
        anchored_duplicate: LOG_CONTEXT_ANCHORED_DUPLICATE,
        records,
        queries,
    }))
}

fn normalize_log_context_page_size(page_size: Option<u64>) -> Result<u64, QueryError> {
    let page_size = page_size.unwrap_or(DEFAULT_LOG_CONTEXT_PAGE_SIZE);
    if page_size == 0 {
        return Err(QueryError::CustomError(
            "pageSize must be greater than 0".to_string(),
        ));
    }

    Ok(page_size.min(DEFAULT_LOG_CONTEXT_PAGE_SIZE))
}

fn parse_log_context_timestamp(raw: &str) -> Result<DateTime<Utc>, QueryError> {
    parse_log_context_time_field(raw, "pTimestamp")
}

fn parse_log_context_time_field(raw: &str, field_name: &str) -> Result<DateTime<Utc>, QueryError> {
    let raw = raw.trim();
    let timestamp = DateTime::parse_from_rfc3339(raw)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|rfc3339_err| {
            parse_log_context_naive_utc_timestamp(raw)
                .map(|timestamp| DateTime::from_naive_utc_and_offset(timestamp, Utc))
                .map_err(|_| {
                    QueryError::CustomError(format!("Invalid {field_name}: {rfc3339_err}"))
                })
        })?;

    DateTime::from_timestamp_millis(timestamp.timestamp_millis()).ok_or_else(|| {
        QueryError::CustomError(format!("{field_name} is outside the supported range"))
    })
}

fn parse_log_context_naive_utc_timestamp(raw: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f"))
}

fn resolve_log_context_bounds(
    anchor_timestamp: DateTime<Utc>,
    context_window: Option<&str>,
    context_start_time: Option<&str>,
    context_end_time: Option<&str>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), QueryError> {
    match (context_window, context_start_time, context_end_time) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => Err(QueryError::CustomError(
            "Request must include either contextWindow or contextStartTime/contextEndTime, not both"
                .to_string(),
        )),
        (Some(context_window), None, None) => {
            log_context_window_bounds(anchor_timestamp, context_window)
        }
        (None, Some(context_start_time), Some(context_end_time)) => {
            log_context_explicit_bounds(context_start_time, context_end_time)
        }
        (None, Some(_), None) | (None, None, Some(_)) => Err(QueryError::CustomError(
            "contextStartTime and contextEndTime must be provided together".to_string(),
        )),
        (None, None, None) => Err(QueryError::CustomError(
            "Request must include either contextWindow or contextStartTime/contextEndTime".to_string(),
        )),
    }
}

fn log_context_window_bounds(
    anchor_timestamp: DateTime<Utc>,
    context_window: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>), QueryError> {
    let duration = humantime::parse_duration(context_window)
        .map_err(|err| QueryError::CustomError(format!("Invalid contextWindow: {err}")))?;
    if duration.is_zero() {
        return Err(QueryError::CustomError(
            "contextWindow must be greater than 0".to_string(),
        ));
    }

    let duration = chrono::Duration::from_std(duration)
        .map_err(|err| QueryError::CustomError(format!("Invalid contextWindow: {err}")))?;
    let start = truncate_to_minute(anchor_timestamp - duration);
    let mut end = truncate_to_minute(anchor_timestamp + duration);

    if start >= end {
        end = start + chrono::Duration::minutes(1);
    }

    Ok((start, end))
}

fn log_context_explicit_bounds(
    context_start_time: &str,
    context_end_time: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>), QueryError> {
    let start = parse_log_context_time_field(context_start_time, "contextStartTime")?;
    let end = parse_log_context_time_field(context_end_time, "contextEndTime")?;

    if start >= end {
        return Err(QueryError::CustomError(
            "contextStartTime must be before contextEndTime".to_string(),
        ));
    }

    Ok((start, end))
}

fn validate_log_context_anchor_in_bounds(
    anchor_timestamp: DateTime<Utc>,
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
) -> Result<(), QueryError> {
    if anchor_timestamp >= context_start_time && anchor_timestamp < context_end_time {
        return Ok(());
    }

    Err(QueryError::CustomError(
        "pTimestamp must be greater than or equal to contextStartTime and less than contextEndTime"
            .to_string(),
    ))
}

fn normalize_log_context_match_fields(
    log: &Option<String>,
    body: &Option<String>,
    message: &Option<String>,
    schema: &arrow_schema::Schema,
) -> Result<Vec<LogContextMatchField>, QueryError> {
    let fields = [("log", log), ("body", body), ("message", message)]
        .into_iter()
        .filter_map(|(field_name, value)| value.as_ref().map(|value| (field_name, value)))
        .map(|(name, value)| {
            validate_log_context_match_field_schema(schema, name)?;
            Ok(LogContextMatchField {
                name: name.to_string(),
                value: value.clone(),
            })
        })
        .collect::<Result<Vec<_>, QueryError>>()?;

    if fields.is_empty() {
        return Err(QueryError::CustomError(
            "Request must include exactly one of log, body, or message".to_string(),
        ));
    }

    if fields.len() > 1 {
        return Err(QueryError::CustomError(
            "Request must include exactly one of log, body, or message".to_string(),
        ));
    }

    Ok(fields)
}

fn validate_log_context_schema(
    schema: &arrow_schema::Schema,
    field_name: &str,
) -> Result<(), QueryError> {
    if schema
        .fields()
        .iter()
        .any(|field| field.name() == field_name)
    {
        return Ok(());
    }

    Err(QueryError::CustomError(format!(
        "Field '{field_name}' does not exist in dataset schema"
    )))
}

fn validate_log_context_match_field_schema(
    schema: &arrow_schema::Schema,
    field_name: &str,
) -> Result<(), QueryError> {
    let field = schema
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or_else(|| {
            QueryError::CustomError(format!(
                "Field '{field_name}' does not exist in dataset schema"
            ))
        })?;

    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(()),
        data_type => Err(QueryError::CustomError(format!(
            "Field '{field_name}' must be a string column for log context matching; found {data_type}"
        ))),
    }
}

fn log_context_additional_filter(
    conditions: &Option<Conditions>,
) -> Result<Option<String>, QueryError> {
    conditions
        .as_ref()
        .map(|conditions| {
            get_filter_string(conditions)
                .map(|filter| format!("({filter})"))
                .map_err(|err| QueryError::CustomError(format!("Invalid conditions: {err}")))
        })
        .transpose()
}

fn build_log_context_anchor_count_query(
    dataset: &str,
    anchor_timestamp: DateTime<Utc>,
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
    match_fields: &[LogContextMatchField],
    additional_filter: Option<&str>,
) -> String {
    let scope_filter =
        build_log_context_scope_filter(context_start_time, context_end_time, additional_filter);
    let anchor_match_predicate =
        build_log_context_anchor_match_predicate(anchor_timestamp, match_fields);
    let dataset = quote_sql_identifier(dataset);

    format!(
        r#"SELECT
  COUNT(*) AS duplicate_anchor_count
FROM {dataset}
WHERE {scope_filter} AND ({anchor_match_predicate})"#
    )
}

#[allow(clippy::too_many_arguments)]
fn build_log_context_newer_query_payload(
    dataset: &str,
    anchor_timestamp: DateTime<Utc>,
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
    match_fields: &[LogContextMatchField],
    additional_filter: Option<&str>,
    start_time: &str,
    end_time: &str,
    limit: u64,
) -> LogContextQueryPayload {
    LogContextQueryPayload {
        query: build_log_context_neighbor_query(
            dataset,
            context_start_time,
            context_end_time,
            &LogContextCursor {
                timestamp: anchor_timestamp,
                match_field: match_fields[0].clone(),
            },
            LogContextSeekDirection::Newer,
            match_fields,
            additional_filter,
            Some(limit),
            None,
        ),
        start_time: start_time.to_string(),
        end_time: end_time.to_string(),
        send_null: false,
        reverse_records: false,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_log_context_anchor_and_older_query_payload(
    dataset: &str,
    anchor_timestamp: DateTime<Utc>,
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
    match_fields: &[LogContextMatchField],
    additional_filter: Option<&str>,
    start_time: &str,
    end_time: &str,
    limit: u64,
) -> LogContextQueryPayload {
    LogContextQueryPayload {
        query: build_log_context_anchor_and_older_query(
            dataset,
            anchor_timestamp,
            context_start_time,
            context_end_time,
            match_fields,
            additional_filter,
            limit,
        ),
        start_time: start_time.to_string(),
        end_time: end_time.to_string(),
        send_null: false,
        reverse_records: false,
    }
}

#[derive(Debug, Clone, Copy)]
enum LogContextSeekDirection {
    Newer,
    Older,
}

fn build_log_context_anchor_and_older_query(
    dataset: &str,
    anchor_timestamp: DateTime<Utc>,
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
    match_fields: &[LogContextMatchField],
    additional_filter: Option<&str>,
    limit: u64,
) -> String {
    let anchor_cursor = LogContextCursor {
        timestamp: anchor_timestamp,
        match_field: match_fields[0].clone(),
    };
    let scope_filter =
        build_log_context_scope_filter(context_start_time, context_end_time, additional_filter);
    let predicate = build_log_context_anchor_and_older_predicate(&anchor_cursor);
    let order_by = build_log_context_order_by(match_fields);
    let dataset = quote_sql_identifier(dataset);

    format!(
        "SELECT * FROM {dataset} WHERE ({scope_filter}) AND ({predicate}) {order_by} LIMIT {limit}"
    )
}

#[allow(clippy::too_many_arguments)]
fn build_log_context_neighbor_query(
    dataset: &str,
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
    cursor: &LogContextCursor,
    direction: LogContextSeekDirection,
    match_fields: &[LogContextMatchField],
    additional_filter: Option<&str>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> String {
    let scope_filter =
        build_log_context_scope_filter(context_start_time, context_end_time, additional_filter);
    let predicate = build_log_context_cursor_predicate(cursor, direction);
    let order_by = match direction {
        LogContextSeekDirection::Newer => build_log_context_reverse_order_by(match_fields),
        LogContextSeekDirection::Older => build_log_context_order_by(match_fields),
    };
    let dataset = quote_sql_identifier(dataset);
    let limit_clause = limit
        .map(|limit| format!(" LIMIT {limit}"))
        .unwrap_or_default();
    let offset_clause = offset
        .map(|offset| format!(" OFFSET {offset}"))
        .unwrap_or_default();

    format!(
        "SELECT * FROM {dataset} WHERE ({scope_filter}) AND ({predicate}) {order_by}{limit_clause}{offset_clause}"
    )
}

#[allow(clippy::too_many_arguments)]
fn build_log_context_cursor_query_payload(
    dataset: &str,
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
    match_fields: &[LogContextMatchField],
    additional_filter: Option<&str>,
    start_time: &str,
    end_time: &str,
    limit: u64,
    cursor: &LogContextCursor,
    direction: LogContextSeekDirection,
) -> LogContextQueryPayload {
    let reverse_records = matches!(direction, LogContextSeekDirection::Newer);
    LogContextQueryPayload {
        query: build_log_context_neighbor_query(
            dataset,
            context_start_time,
            context_end_time,
            cursor,
            direction,
            match_fields,
            additional_filter,
            Some(limit),
            Some(0),
        ),
        start_time: start_time.to_string(),
        end_time: end_time.to_string(),
        send_null: false,
        reverse_records,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_log_context_cursor_queries(
    dataset: &str,
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
    match_fields: &[LogContextMatchField],
    additional_filter: Option<&str>,
    start_time: &str,
    end_time: &str,
    limit: u64,
    records: &[Value],
) -> Result<LogContextQueries, QueryError> {
    let previous = records
        .first()
        .map(|record| log_context_cursor_from_record(record, &match_fields[0]))
        .transpose()?
        .map(|cursor| {
            build_log_context_cursor_query_payload(
                dataset,
                context_start_time,
                context_end_time,
                match_fields,
                additional_filter,
                start_time,
                end_time,
                limit,
                &cursor,
                LogContextSeekDirection::Newer,
            )
        });
    let next = records
        .last()
        .map(|record| log_context_cursor_from_record(record, &match_fields[0]))
        .transpose()?
        .map(|cursor| {
            build_log_context_cursor_query_payload(
                dataset,
                context_start_time,
                context_end_time,
                match_fields,
                additional_filter,
                start_time,
                end_time,
                limit,
                &cursor,
                LogContextSeekDirection::Older,
            )
        });

    Ok(LogContextQueries { previous, next })
}

fn build_log_context_records_window(
    newer_records: Vec<Value>,
    older_records: Vec<Value>,
    page_size: u64,
) -> Result<(Vec<Value>, u64), QueryError> {
    if older_records.is_empty() {
        return Err(QueryError::CustomError(
            "Anchor row was not returned by the context query".to_string(),
        ));
    }

    let page_size = usize::try_from(page_size)
        .map_err(|_| QueryError::CustomError("pageSize is too large".to_string()))?;
    let (newer_take, older_take) =
        log_context_window_counts(newer_records.len(), older_records.len(), page_size);
    let mut newer_records = newer_records
        .into_iter()
        .take(newer_take)
        .collect::<Vec<_>>();
    newer_records.reverse();

    let anchor_index = newer_records.len() as u64;
    let mut records = newer_records;
    records.extend(older_records.into_iter().take(older_take));

    Ok((records, anchor_index))
}

fn log_context_window_counts(
    newer_len: usize,
    older_len: usize,
    page_size: usize,
) -> (usize, usize) {
    let target_newer = page_size / 2;
    let mut newer_take = newer_len.min(target_newer);
    let older_take = older_len.min(page_size.saturating_sub(newer_take));
    if newer_take + older_take < page_size {
        newer_take = newer_len.min(newer_take + page_size - newer_take - older_take);
    }

    (newer_take, older_take)
}

fn build_log_context_scope_filter(
    context_start_time: DateTime<Utc>,
    context_end_time: DateTime<Utc>,
    additional_filter: Option<&str>,
) -> String {
    let timestamp_column = quote_sql_identifier(DEFAULT_TIMESTAMP_KEY);
    let time_filter = format!(
        "{timestamp_column} >= {} AND {timestamp_column} < {}",
        timestamp_sql_literal(context_start_time),
        timestamp_sql_literal(context_end_time)
    );

    match additional_filter {
        Some(filter) => format!("({time_filter}) AND {filter}"),
        None => time_filter,
    }
}

fn build_log_context_anchor_match_predicate(
    anchor_timestamp: DateTime<Utc>,
    match_fields: &[LogContextMatchField],
) -> String {
    let mut predicates = vec![format!(
        "{} = {}",
        quote_sql_identifier(DEFAULT_TIMESTAMP_KEY),
        timestamp_sql_literal(anchor_timestamp)
    )];

    predicates.extend(match_fields.iter().map(|field| {
        format!(
            "{} = {}",
            quote_sql_identifier(&field.name),
            quote_sql_string_literal(&field.value)
        )
    }));

    predicates.join(" AND ")
}

fn build_log_context_anchor_and_older_predicate(cursor: &LogContextCursor) -> String {
    let timestamp_column = quote_sql_identifier(DEFAULT_TIMESTAMP_KEY);
    let timestamp = timestamp_sql_literal(cursor.timestamp);
    let field_column = quote_sql_identifier(&cursor.match_field.name);
    let field_value = quote_sql_string_literal(&cursor.match_field.value);

    format!(
        "{timestamp_column} < {timestamp} OR ({timestamp_column} = {timestamp} AND {field_column} >= {field_value})"
    )
}

fn build_log_context_cursor_predicate(
    cursor: &LogContextCursor,
    direction: LogContextSeekDirection,
) -> String {
    let timestamp_column = quote_sql_identifier(DEFAULT_TIMESTAMP_KEY);
    let timestamp = timestamp_sql_literal(cursor.timestamp);
    let field_column = quote_sql_identifier(&cursor.match_field.name);
    let field_value = quote_sql_string_literal(&cursor.match_field.value);

    match direction {
        LogContextSeekDirection::Newer => format!(
            "{timestamp_column} > {timestamp} OR ({timestamp_column} = {timestamp} AND {field_column} < {field_value})"
        ),
        LogContextSeekDirection::Older => format!(
            "{timestamp_column} < {timestamp} OR ({timestamp_column} = {timestamp} AND {field_column} > {field_value})"
        ),
    }
}

fn build_log_context_order_by(match_fields: &[LogContextMatchField]) -> String {
    let mut order_columns = vec![format!(
        "{} DESC",
        quote_sql_identifier(DEFAULT_TIMESTAMP_KEY)
    )];
    order_columns.extend(
        match_fields
            .iter()
            .map(|field| format!("{} ASC", quote_sql_identifier(&field.name))),
    );

    format!("ORDER BY {}", order_columns.into_iter().join(", "))
}

fn build_log_context_reverse_order_by(match_fields: &[LogContextMatchField]) -> String {
    let mut order_columns = vec![format!(
        "{} ASC",
        quote_sql_identifier(DEFAULT_TIMESTAMP_KEY)
    )];
    order_columns.extend(
        match_fields
            .iter()
            .map(|field| format!("{} DESC", quote_sql_identifier(&field.name))),
    );

    format!("ORDER BY {}", order_columns.into_iter().join(", "))
}

#[tracing::instrument(
    name = "query_context.execute_anchor_count",
    skip(sql, authorized_datasets, tenant_id),
    fields(
        tenant = tracing::field::Empty,
        start_time = %start_time,
        end_time = %end_time,
        sql_len = sql.len(),
        authorized_dataset_count = authorized_datasets.len()
    )
)]
async fn execute_log_context_anchor_count(
    sql: &str,
    authorized_datasets: &[String],
    start_time: &str,
    end_time: &str,
    tenant_id: &Option<String>,
) -> Result<u64, QueryError> {
    Span::current().record("tenant", tracing::field::debug(tenant_id));
    debug!("query context anchor count query starting");

    let query_request = Query {
        query: sql.to_string(),
        start_time: start_time.to_string(),
        end_time: end_time.to_string(),
        send_null: false,
        fields: false,
        streaming: false,
        filter_tags: None,
    };

    let (records, _) =
        get_records_and_fields_for_authorized_query(&query_request, authorized_datasets, tenant_id)
            .await?;
    let records = records.unwrap_or_default();
    let rows = record_batches_to_json(&records)?;
    let row = rows
        .first()
        .ok_or_else(|| QueryError::CustomError("No anchor count returned".to_string()))?;
    let duplicate_anchor_count = json_u64_field(row, "duplicate_anchor_count")?;
    debug!(
        duplicate_anchor_count,
        "query context anchor count query completed"
    );

    Ok(duplicate_anchor_count)
}

#[tracing::instrument(
    name = "query_context.execute_rows",
    skip(payload, authorized_datasets, tenant_id),
    fields(
        tenant = tracing::field::Empty,
        start_time = %payload.start_time,
        end_time = %payload.end_time,
        sql_len = payload.query.len(),
        authorized_dataset_count = authorized_datasets.len()
    )
)]
async fn execute_log_context_rows(
    payload: &LogContextQueryPayload,
    authorized_datasets: &[String],
    tenant_id: &Option<String>,
) -> Result<Vec<Value>, QueryError> {
    Span::current().record("tenant", tracing::field::debug(tenant_id));
    debug!("query context row query starting");

    let query_request = Query {
        query: payload.query.clone(),
        start_time: payload.start_time.clone(),
        end_time: payload.end_time.clone(),
        send_null: payload.send_null,
        fields: false,
        streaming: false,
        filter_tags: None,
    };

    let (records, _) =
        get_records_and_fields_for_authorized_query(&query_request, authorized_datasets, tenant_id)
            .await?;
    let records = records.unwrap_or_default();
    let records: Vec<Value> = record_batches_to_json(&records)?
        .into_iter()
        .map(Value::Object)
        .collect();

    debug!(
        record_count = records.len(),
        "query context row query completed"
    );

    Ok(records)
}

fn json_u64_field(row: &Map<String, Value>, field: &str) -> Result<u64, QueryError> {
    let value = row
        .get(field)
        .ok_or_else(|| QueryError::CustomError(format!("Missing field '{field}'")))?;

    match value {
        Value::Number(number) => {
            if let Some(value) = number.as_u64() {
                return Ok(value);
            }
            if let Some(value) = number.as_i64()
                && value >= 0
            {
                return Ok(value as u64);
            }
            Err(QueryError::CustomError(format!(
                "Field '{field}' is not a valid unsigned integer"
            )))
        }
        Value::Null => Ok(0),
        _ => Err(QueryError::CustomError(format!(
            "Field '{field}' is not a valid unsigned integer"
        ))),
    }
}

fn log_context_cursor_from_record(
    record: &Value,
    match_field: &LogContextMatchField,
) -> Result<LogContextCursor, QueryError> {
    let record = record.as_object().ok_or_else(|| {
        QueryError::CustomError("Log context record is not an object".to_string())
    })?;
    let timestamp = record
        .get(DEFAULT_TIMESTAMP_KEY)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            QueryError::CustomError(format!("Missing field '{DEFAULT_TIMESTAMP_KEY}' in record"))
        })
        .and_then(parse_log_context_timestamp)?;
    let value = record
        .get(&match_field.name)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            QueryError::CustomError(format!("Missing field '{}' in record", match_field.name))
        })?
        .to_string();

    Ok(LogContextCursor {
        timestamp,
        match_field: LogContextMatchField {
            name: match_field.name.clone(),
            value,
        },
    })
}

fn format_log_context_api_time(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn timestamp_sql_literal(timestamp: DateTime<Utc>) -> String {
    format!(
        "TIMESTAMP '{}'",
        timestamp.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f")
    )
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    fn schema_with(fields: &[&str]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|field| {
                    if *field == DEFAULT_TIMESTAMP_KEY {
                        Field::new(
                            *field,
                            DataType::Timestamp(TimeUnit::Millisecond, None),
                            true,
                        )
                    } else {
                        Field::new(*field, DataType::Utf8, true)
                    }
                })
                .collect::<Vec<_>>(),
        )
    }

    fn anchor_timestamp() -> DateTime<Utc> {
        parse_log_context_timestamp("2026-06-17T10:15:42.123456Z").unwrap()
    }

    #[test]
    fn log_context_timestamp_parser_accepts_request_and_query_result_formats() {
        assert_eq!(
            parse_log_context_timestamp("2026-06-18T07:39:59.995Z").unwrap(),
            parse_log_context_timestamp("2026-06-18T07:39:59.995").unwrap()
        );
        assert_eq!(
            parse_log_context_timestamp("2026-06-18T07:39:59.995Z").unwrap(),
            parse_log_context_timestamp("2026-06-18 07:39:59.995").unwrap()
        );
    }

    #[test]
    fn log_context_window_counts_center_anchor_and_fill_edges() {
        assert_eq!(log_context_window_counts(250, 250, 500), (250, 250));
        assert_eq!(log_context_window_counts(10, 1000, 500), (10, 490));
        assert_eq!(log_context_window_counts(1000, 10, 500), (490, 10));
        assert_eq!(log_context_window_counts(10, 10, 500), (10, 10));
    }

    #[test]
    fn log_context_page_size_defaults_and_clamps() {
        assert_eq!(
            normalize_log_context_page_size(None).unwrap(),
            DEFAULT_LOG_CONTEXT_PAGE_SIZE
        );
        assert_eq!(normalize_log_context_page_size(Some(1)).unwrap(), 1);
        assert_eq!(
            normalize_log_context_page_size(Some(DEFAULT_LOG_CONTEXT_PAGE_SIZE + 1)).unwrap(),
            DEFAULT_LOG_CONTEXT_PAGE_SIZE
        );
        assert!(normalize_log_context_page_size(Some(0)).is_err());
    }

    #[test]
    fn log_context_explicit_bounds_accept_start_and_end_times() {
        let (start, end) =
            log_context_explicit_bounds("2026-06-17T10:14:00Z", "2026-06-17T10:16:00Z").unwrap();
        assert_eq!(format_log_context_api_time(start), "2026-06-17T10:14:00Z");
        assert_eq!(format_log_context_api_time(end), "2026-06-17T10:16:00Z");

        let (start, end) =
            log_context_explicit_bounds("2026-06-17T10:15:42.100Z", "2026-06-17T10:15:42.900Z")
                .unwrap();
        assert_eq!(
            format_log_context_api_time(start),
            "2026-06-17T10:15:42.100Z"
        );
        assert_eq!(format_log_context_api_time(end), "2026-06-17T10:15:42.900Z");

        assert!(
            log_context_explicit_bounds("2026-06-17T10:16:00Z", "2026-06-17T10:16:00Z").is_err()
        );
        assert!(
            log_context_explicit_bounds("2026-06-17T10:17:00Z", "2026-06-17T10:16:00Z").is_err()
        );
    }

    #[test]
    fn log_context_window_bounds_apply_window_and_truncate_to_minute() {
        let (start, end) = log_context_window_bounds(anchor_timestamp(), "1m").unwrap();
        assert_eq!(format_log_context_api_time(start), "2026-06-17T10:14:00Z");
        assert_eq!(format_log_context_api_time(end), "2026-06-17T10:16:00Z");

        let (start, end) = log_context_window_bounds(anchor_timestamp(), "5s").unwrap();
        assert_eq!(format_log_context_api_time(start), "2026-06-17T10:15:00Z");
        assert_eq!(format_log_context_api_time(end), "2026-06-17T10:16:00Z");
    }

    #[test]
    fn log_context_bounds_resolver_accepts_one_mode_only() {
        let anchor = anchor_timestamp();
        assert!(resolve_log_context_bounds(anchor, Some("1m"), None, None).is_ok());
        assert!(
            resolve_log_context_bounds(
                anchor,
                None,
                Some("2026-06-17T10:14:00Z"),
                Some("2026-06-17T10:16:00Z"),
            )
            .is_ok()
        );
        assert!(
            resolve_log_context_bounds(anchor, Some("1m"), Some("2026-06-17T10:14:00Z"), None,)
                .is_err()
        );
        assert!(
            resolve_log_context_bounds(anchor, None, Some("2026-06-17T10:14:00Z"), None).is_err()
        );
        assert!(resolve_log_context_bounds(anchor, None, None, None).is_err());
    }

    #[test]
    fn log_context_anchor_must_be_inside_context_bounds() {
        let (start, end) =
            log_context_explicit_bounds("2026-06-17T10:14:00Z", "2026-06-17T10:16:00Z").unwrap();

        validate_log_context_anchor_in_bounds(
            parse_log_context_timestamp("2026-06-17T10:14:00Z").unwrap(),
            start,
            end,
        )
        .unwrap();
        validate_log_context_anchor_in_bounds(anchor_timestamp(), start, end).unwrap();
        assert!(
            validate_log_context_anchor_in_bounds(
                parse_log_context_timestamp("2026-06-17T10:13:59.999Z").unwrap(),
                start,
                end,
            )
            .is_err()
        );
        assert!(
            validate_log_context_anchor_in_bounds(
                parse_log_context_timestamp("2026-06-17T10:16:00Z").unwrap(),
                start,
                end,
            )
            .is_err()
        );
    }

    #[test]
    fn log_context_match_fields_accept_exactly_one_anchor_field() {
        let schema = schema_with(&[DEFAULT_TIMESTAMP_KEY, "body", "log", "message"]);

        let log_fields = normalize_log_context_match_fields(
            &Some("log value".to_string()),
            &None,
            &None,
            &schema,
        )
        .unwrap();
        assert_eq!(
            log_fields,
            vec![LogContextMatchField {
                name: "log".to_string(),
                value: "log value".to_string(),
            }]
        );

        let body_fields = normalize_log_context_match_fields(
            &None,
            &Some("body value".to_string()),
            &None,
            &schema,
        )
        .unwrap();
        assert_eq!(
            body_fields,
            vec![LogContextMatchField {
                name: "body".to_string(),
                value: "body value".to_string(),
            }]
        );

        let message_fields = normalize_log_context_match_fields(
            &None,
            &None,
            &Some("message value".to_string()),
            &schema,
        )
        .unwrap();
        assert_eq!(
            message_fields,
            vec![LogContextMatchField {
                name: "message".to_string(),
                value: "message value".to_string(),
            }]
        );
    }

    #[test]
    fn log_context_match_fields_reject_missing_multiple_and_absent_schema_fields() {
        let full_schema = schema_with(&[DEFAULT_TIMESTAMP_KEY, "body", "log", "message"]);
        let log_only_schema = schema_with(&[DEFAULT_TIMESTAMP_KEY, "log"]);

        assert!(normalize_log_context_match_fields(&None, &None, &None, &full_schema).is_err());
        assert!(
            normalize_log_context_match_fields(
                &Some("log value".to_string()),
                &Some("body value".to_string()),
                &None,
                &full_schema,
            )
            .is_err()
        );
        assert!(
            normalize_log_context_match_fields(
                &None,
                &Some("body value".to_string()),
                &None,
                &log_only_schema
            )
            .is_err()
        );

        let numeric_message_schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("message", DataType::Int64, true),
        ]);
        assert!(
            normalize_log_context_match_fields(
                &None,
                &None,
                &Some("message value".to_string()),
                &numeric_message_schema,
            )
            .is_err()
        );
    }

    #[test]
    fn log_context_anchor_count_sql_only_counts_anchor_duplicates() {
        let anchor = anchor_timestamp();
        let (start, end) =
            log_context_explicit_bounds("2026-06-17T10:14:00Z", "2026-06-17T10:16:00Z").unwrap();
        let match_fields = vec![LogContextMatchField {
            name: "message".to_string(),
            value: "alpha".to_string(),
        }];

        let sql =
            build_log_context_anchor_count_query("logs", anchor, start, end, &match_fields, None);

        assert!(sql.contains("COUNT(*) AS duplicate_anchor_count"));
        assert!(!sql.contains("rows_before"));
        assert!(!sql.contains("COUNT(*) AS total"));
        assert!(sql.contains("\"p_timestamp\" = TIMESTAMP '2026-06-17 10:15:42.123'"));
        assert!(sql.contains("\"message\" = 'alpha'"));
        assert!(!sql.contains("\"log\" = 'alpha' AND \"body\""));
    }

    #[test]
    fn log_context_cursor_sql_builds_previous_and_next_pages() {
        let anchor = anchor_timestamp();
        let (start, end) =
            log_context_explicit_bounds("2026-06-17T10:14:00Z", "2026-06-17T10:16:00Z").unwrap();
        let match_fields = vec![LogContextMatchField {
            name: "message".to_string(),
            value: "alpha".to_string(),
        }];
        let cursor = LogContextCursor {
            timestamp: anchor,
            match_field: match_fields[0].clone(),
        };

        let previous_sql = build_log_context_neighbor_query(
            "logs",
            start,
            end,
            &cursor,
            LogContextSeekDirection::Newer,
            &match_fields,
            None,
            None,
            None,
        );
        assert!(previous_sql.contains("\"p_timestamp\" > TIMESTAMP '2026-06-17 10:15:42.123'"));
        assert!(previous_sql.contains("\"message\" < 'alpha'"));
        assert!(previous_sql.ends_with("ORDER BY \"p_timestamp\" ASC, \"message\" DESC"));
        assert!(!previous_sql.contains("LIMIT"));
        assert!(!previous_sql.contains("OFFSET"));

        let next_sql = build_log_context_neighbor_query(
            "logs",
            start,
            end,
            &cursor,
            LogContextSeekDirection::Older,
            &match_fields,
            None,
            None,
            None,
        );
        assert!(next_sql.contains("\"p_timestamp\" < TIMESTAMP '2026-06-17 10:15:42.123'"));
        assert!(next_sql.contains("\"message\" > 'alpha'"));
        assert!(next_sql.ends_with("ORDER BY \"p_timestamp\" DESC, \"message\" ASC"));
        assert!(!next_sql.contains("LIMIT"));
        assert!(!next_sql.contains("OFFSET"));

        let previous_payload = build_log_context_cursor_query_payload(
            "logs",
            start,
            end,
            &match_fields,
            None,
            "2026-06-17T10:14:00Z",
            "2026-06-17T10:16:00Z",
            500,
            &cursor,
            LogContextSeekDirection::Newer,
        );
        assert!(previous_payload.query.ends_with("LIMIT 500 OFFSET 0"));
        assert!(previous_payload.reverse_records);

        let next_payload = build_log_context_cursor_query_payload(
            "logs",
            start,
            end,
            &match_fields,
            None,
            "2026-06-17T10:14:00Z",
            "2026-06-17T10:16:00Z",
            500,
            &cursor,
            LogContextSeekDirection::Older,
        );
        assert!(next_payload.query.ends_with("LIMIT 500 OFFSET 0"));
        assert!(!next_payload.reverse_records);
    }
}
