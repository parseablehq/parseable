use std::{collections::HashMap, ops::ControlFlow};

use actix_web::{HttpRequest, HttpResponse, body::to_bytes_limited, http::header::HeaderMap, web};
use chrono::{DateTime, Duration, LocalResult, TimeZone, Utc};
use chrono_tz::Tz;
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use ulid::Ulid;

use crate::{
    datafusion::sql::{
        parser::{DFParser, Statement as DFStatement},
        sqlparser::ast::{
            DataType as SqlDataType, Expr as SqlExpr, SelectItem, SetExpr,
            Statement as SqlStatement, visit_expressions,
        },
    },
    handlers::http::{
        alerts, cluster, health_check, logstream,
        modal::query::querier_logstream,
        query, rbac, role, targets,
        users::{dashboards, filters},
    },
    parseable::PARSEABLE,
    prism::logstream::get_prism_logstream_info,
    rbac::{Response, Users, map::SessionKey, role::Action},
    tool_catalog::{ToolCallRequest, ToolCallResult, oss_read_tool_specs, oss_tool_specs},
    utils::{actix::extract_session_key_from_req, get_tenant_id_from_request, time::TimeRange},
};

fn required_string<'a>(arguments: &'a Value, name: &str) -> Result<&'a str, String> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing required string argument `{name}`"))
}

fn bounded_integer(arguments: &Value, name: &str, default: u64, min: u64, max: u64) -> u64 {
    arguments
        .get(name)
        .and_then(Value::as_u64)
        .unwrap_or(default)
        .clamp(min, max)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

const MAX_TOOL_ROWS: u64 = 500;
const MAX_TOOL_COLUMNS: usize = 10;
const DATASET_DISCOVERY_CONCURRENCY: usize = 8;

pub fn validate_tool_sql(query: &str) -> Result<(), String> {
    let statements =
        DFParser::parse_sql(query.trim()).map_err(|error| format!("invalid SQL: {error}"))?;
    if statements.len() != 1 {
        return Err("exactly one SQL statement is required".to_owned());
    }
    let Some(DFStatement::Statement(statement)) = statements.front() else {
        return Err("only SQL SELECT queries are allowed".to_owned());
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return Err("only SQL SELECT queries are allowed".to_owned());
    };
    let unsupported_cast = match visit_expressions(query, |expression| {
        let SqlExpr::Cast { data_type, .. } = expression else {
            return ControlFlow::Continue(());
        };
        let replacement = match data_type {
            SqlDataType::Int64 | SqlDataType::UInt64 => "BIGINT",
            SqlDataType::Float64 => "DOUBLE",
            _ => return ControlFlow::Continue(()),
        };
        ControlFlow::Break((data_type.to_string(), replacement))
    }) {
        ControlFlow::Break(unsupported) => Some(unsupported),
        ControlFlow::Continue(()) => None,
    };
    if let Some((unsupported, replacement)) = unsupported_cast {
        return Err(format!(
            "unsupported DataFusion SQL cast type `{unsupported}`; use `{replacement}` instead"
        ));
    }
    fn validate_projection(body: &SetExpr) -> Result<(), String> {
        match body {
            SetExpr::Select(select) => {
                if select.projection.len() > MAX_TOOL_COLUMNS {
                    return Err(format!(
                        "query selects {} columns; the tool limit is {MAX_TOOL_COLUMNS}",
                        select.projection.len()
                    ));
                }
                if select.projection.iter().any(|item| {
                    matches!(
                        item,
                        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                    )
                }) {
                    return Err(format!(
                        "wildcard selection is not allowed; select at most {MAX_TOOL_COLUMNS} explicit columns"
                    ));
                }
                Ok(())
            }
            SetExpr::Query(query) => validate_projection(query.body.as_ref()),
            SetExpr::SetOperation { left, right, .. } => {
                validate_projection(left)?;
                validate_projection(right)
            }
            _ => Err("unsupported SQL query form".to_owned()),
        }
    }
    validate_projection(query.body.as_ref())
}

pub fn bounded_tool_sql(query: &str) -> Result<String, String> {
    validate_tool_sql(query)?;
    Ok(format!(
        "SELECT * FROM ({}) AS \"__parseable_tool_result\" LIMIT {MAX_TOOL_ROWS}",
        query.trim().trim_end_matches(';')
    ))
}

pub fn enforce_tool_tabular_limits(mut value: Value) -> Result<Value, String> {
    let rows = match &mut value {
        Value::Array(rows) => rows,
        Value::Object(object) => object
            .get_mut("records")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| "query response is not tabular".to_owned())?,
        _ => return Err("query response is not tabular".to_owned()),
    };
    rows.truncate(MAX_TOOL_ROWS as usize);
    if let Some(column_count) = rows
        .iter()
        .filter_map(Value::as_object)
        .map(serde_json::Map::len)
        .max()
        && column_count > MAX_TOOL_COLUMNS
    {
        return Err(format!(
            "query selected {column_count} columns; the tool limit is {MAX_TOOL_COLUMNS}"
        ));
    }
    Ok(value)
}

fn bounded_select(query: &str) -> Result<String, String> {
    bounded_tool_sql(query)
}

fn visible_datasets(tenant_id: &Option<String>, session_key: &SessionKey) -> Vec<String> {
    let internal_datasets = PARSEABLE
        .streams
        .list_internal_streams(tenant_id)
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    let mut datasets = PARSEABLE
        .streams
        .list(tenant_id)
        .into_iter()
        .filter(|dataset| !internal_datasets.contains(dataset))
        .filter(|dataset| {
            Users.authorize(session_key.clone(), Action::ListStream, Some(dataset), None)
                == Response::Authorized
        })
        .collect::<Vec<_>>();
    datasets.sort_unstable();
    datasets
}

fn resolve_time_range(arguments: &Value) -> Result<Value, String> {
    resolve_time_range_at(arguments, Utc::now())
}

pub fn resolve_time_range_at(arguments: &Value, reference: DateTime<Utc>) -> Result<Value, String> {
    let expression = required_string(arguments, "expression")?;
    let timezone_name = arguments
        .get("timezone")
        .and_then(Value::as_str)
        .unwrap_or("UTC");
    let timezone = timezone_name
        .parse::<Tz>()
        .map_err(|_| format!("invalid IANA timezone `{timezone_name}`"))?;
    let normalized = expression.trim().to_ascii_lowercase();
    let range = if normalized == "today" || normalized == "yesterday" {
        let date = reference.with_timezone(&timezone).date_naive()
            - Duration::days(i64::from(normalized == "yesterday"));
        let local_start = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| "could not construct local day boundary".to_owned())?;
        let start = match timezone.from_local_datetime(&local_start) {
            LocalResult::Single(start) | LocalResult::Ambiguous(start, _) => {
                start.with_timezone(&Utc)
            }
            LocalResult::None => return Err("local day boundary does not exist".to_owned()),
        };
        let end = if normalized == "today" {
            reference
        } else {
            let next_local_start = (date + Duration::days(1))
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| "could not construct local day boundary".to_owned())?;
            match timezone.from_local_datetime(&next_local_start) {
                LocalResult::Single(end) | LocalResult::Ambiguous(end, _) => {
                    end.with_timezone(&Utc)
                }
                LocalResult::None => return Err("local day boundary does not exist".to_owned()),
            }
        };
        TimeRange::new(start, end)
    } else if let Some(duration) = normalized
        .strip_prefix("last ")
        .or_else(|| normalized.strip_prefix("past "))
        .or_else(|| normalized.strip_prefix("now-"))
    {
        let duration = duration.trim();
        let duration_with_quantity =
            (!duration.chars().any(|character| character.is_ascii_digit())).then(|| {
                let unit = duration
                    .strip_prefix("an ")
                    .or_else(|| duration.strip_prefix("a "))
                    .unwrap_or(duration);
                format!("1 {unit}")
            });
        let duration = duration_with_quantity.as_deref().unwrap_or(duration);
        let duration = humantime::parse_duration(duration)
            .map_err(|error| format!("invalid time duration: {error}"))?;
        let duration = Duration::from_std(duration)
            .map_err(|error| format!("time duration is too large: {error}"))?;
        TimeRange::new(reference - duration, reference)
    } else if let Some((start, end)) = expression.split_once(" to ") {
        TimeRange::parse_human_time(start.trim(), end.trim()).map_err(|error| error.to_string())?
    } else if DateTime::parse_from_rfc3339(expression).is_ok() {
        TimeRange::parse_human_time(expression, &reference.to_rfc3339())
            .map_err(|error| error.to_string())?
    } else {
        TimeRange::parse_human_time(expression, "now").map_err(|error| error.to_string())?
    };
    Ok(json!({
        "expression": expression,
        "timezone": timezone_name,
        "startTime": range.start.to_rfc3339(),
        "endTime": range.end.to_rfc3339(),
    }))
}

fn nested_string<'a>(value: &'a Value, name: &str) -> Option<&'a str> {
    match value {
        Value::Object(object) => object
            .get(name)
            .and_then(Value::as_str)
            .or_else(|| object.values().find_map(|value| nested_string(value, name))),
        Value::Array(array) => array.iter().find_map(|value| nested_string(value, name)),
        _ => None,
    }
}

async fn response_value(response: HttpResponse) -> Result<Value, String> {
    let status = response.status();
    let bytes = to_bytes_limited(response.into_body(), 1024 * 1024)
        .await
        .map_err(|error| format!("tool response body error: {error:?}"))?
        .map_err(|error| format!("tool response exceeded 1 MiB: {error}"))?;
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    if status.is_success() {
        Ok(value)
    } else {
        Err(format!("handler returned HTTP {status}: {value}"))
    }
}

fn authorize(
    session_key: &SessionKey,
    action: Action,
    resource: Option<&str>,
    user: Option<&str>,
) -> Result<(), String> {
    if !Users.session_exists(session_key) {
        return Err(
            "Your session has expired or is no longer valid. Please re-authenticate to access this resource."
                .to_owned(),
        );
    }
    match Users.authorize(session_key.clone(), action, resource, user) {
        Response::Authorized => Ok(()),
        Response::Suspended(message) => Err(message),
        Response::UnAuthorized | Response::ReloadRequired => {
            Err(format!("Caller is not authorized to perform {action:?}"))
        }
    }
}

pub fn resolve_dataset_candidates(payload: Value, arguments: &Value) -> Value {
    let query = arguments
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let limit = bounded_integer(arguments, "limit", 5, 1, 20) as usize;
    let Some(datasets) = payload.as_array() else {
        return payload;
    };

    let requested_dataset = arguments
        .get("selectedDataset")
        .or_else(|| arguments.get("contextDataset"))
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty());
    if let Some(requested_dataset) = requested_dataset {
        let selected = datasets.iter().find(|dataset| {
            dataset
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| name.eq_ignore_ascii_case(requested_dataset))
        });
        return match selected {
            Some(dataset) => json!({
                "status": "resolved",
                "datasets": [dataset],
                "selectedDatasets": [dataset.get("name").and_then(Value::as_str).unwrap_or(requested_dataset)],
                "confidence": 1.0,
                "reason": if arguments.get("selectedDataset").is_some() {
                    "explicit candidate selection validated against caller-visible datasets"
                } else {
                    "trusted page-context dataset validated against caller-visible datasets"
                }
            }),
            None => json!({
                "status": "error",
                "message": format!("Dataset `{requested_dataset}` is not visible to this caller"),
            }),
        };
    }

    if datasets.len() == 1 {
        let dataset = &datasets[0];
        return json!({
            "status": "resolved",
            "datasets": [dataset],
            "selectedDatasets": [dataset.get("name").and_then(Value::as_str).unwrap_or_default()],
            "confidence": 1.0,
            "reason": "only caller-visible dataset"
        });
    }

    let terms = query
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|term| term.len() > 2)
        .collect::<Vec<_>>();
    let telemetry_type = arguments
        .get("telemetryType")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let mut scored = datasets
        .iter()
        .filter_map(|dataset| {
            let original_name = dataset.get("name")?.as_str()?;
            let name = original_name.to_ascii_lowercase();
            let mut score = 0_u64;
            let mut reasons = Vec::new();
            if !query.is_empty() && name == query {
                score += 1_000;
                reasons.push("exact dataset name");
            } else if !query.is_empty() && query.contains(&name) {
                score += 750;
                reasons.push("dataset name explicitly mentioned in the question");
            } else if !query.is_empty() && name.contains(&query) {
                score += 500;
                reasons.push("query matches the dataset name");
            }
            let term_matches = terms.iter().filter(|term| name.contains(**term)).count() as u64;
            if term_matches > 0 {
                score += term_matches * 25;
                reasons.push("dataset-name token match");
            }
            if let Some(telemetry_type) = telemetry_type.as_deref()
                && serde_json::to_string(dataset)
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains(telemetry_type)
            {
                score += 15;
                reasons.push("telemetry type match");
            }
            Some((score, original_name.to_owned(), reasons, dataset.clone()))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));

    if let Some((score, name, _, dataset)) = scored.first()
        && *score >= 500
        && scored.get(1).is_none_or(|second| *score > second.0)
    {
        return json!({
            "status": "resolved",
            "datasets": [dataset],
            "selectedDatasets": [name],
            "confidence": 0.95,
            "reason": "unique exact dataset-name match"
        });
    }

    let candidates = scored
        .into_iter()
        .take(limit)
        .map(|(score, name, reasons, dataset)| {
            json!({
                "name": name,
                "score": score,
                "reasons": reasons,
                "dataset": dataset,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "status": if candidates.is_empty() { "not_found" } else { "ambiguous" },
        "datasets": [],
        "candidates": candidates,
        "query": query,
        "message": if datasets.is_empty() {
            "No caller-visible datasets were found."
        } else {
            "Dataset resolution is ambiguous. Inspect candidate schemas, then call resolve_dataset again with selectedDataset."
        }
    })
}

#[derive(Clone, Copy)]
struct OssToolContext<'a> {
    session_key: &'a SessionKey,
    tenant_id: &'a Option<String>,
    query_headers: &'a HeaderMap,
}

fn execute_dataset_resolution_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    match name {
        "list_datasets" => Ok(visible_datasets(context.tenant_id, context.session_key)
            .into_iter()
            .map(|name| json!({ "name": name }))
            .collect()),
        "resolve_dataset" => {
            let datasets = visible_datasets(context.tenant_id, context.session_key)
                .into_iter()
                .map(|name| json!({ "name": name }))
                .collect();
            Ok(resolve_dataset_candidates(datasets, arguments))
        }
        "resolve_time_range" => resolve_time_range(arguments),
        _ => unreachable!("dataset resolution dispatcher only receives known tools"),
    }
}

enum DatasetActivity {
    Skipped,
    Count { name: String, events: u64 },
    Error { name: String, message: String },
}

async fn discover_datasets(
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    let start_time = required_string(arguments, "startTime")?.to_owned();
    let end_time = required_string(arguments, "endTime")?.to_owned();
    TimeRange::parse_human_time(&start_time, &end_time).map_err(|error| error.to_string())?;
    let telemetry_type = required_string(arguments, "telemetryType")?.to_ascii_lowercase();
    if !matches!(telemetry_type.as_str(), "logs" | "metrics" | "traces") {
        return Err("telemetryType must be logs, metrics, or traces".to_owned());
    }
    let limit = bounded_integer(arguments, "limit", 50, 1, 500) as usize;
    let datasets = visible_datasets(context.tenant_id, context.session_key);
    let visible_datasets = datasets.len();

    let activity = stream::iter(datasets)
        .map(|name| {
            let start_time = start_time.clone();
            let end_time = end_time.clone();
            let telemetry_type = telemetry_type.clone();
            async move {
                if Users.authorize(
                    context.session_key.clone(),
                    Action::Query,
                    Some(&name),
                    None,
                ) != Response::Authorized
                {
                    return DatasetActivity::Skipped;
                }
                if !PARSEABLE
                    .check_or_load_stream(&name, context.tenant_id)
                    .await
                {
                    return DatasetActivity::Error {
                        name,
                        message: "dataset metadata is unavailable".to_owned(),
                    };
                }
                let actual_telemetry_type = match PARSEABLE.get_stream(&name, context.tenant_id) {
                    Ok(stream) => match stream.metadata.read() {
                        Ok(metadata) => metadata.telemetry_type.to_string(),
                        Err(error) => {
                            return DatasetActivity::Error {
                                name,
                                message: error.to_string(),
                            };
                        }
                    },
                    Err(error) => {
                        return DatasetActivity::Error {
                            name,
                            message: error.to_string(),
                        };
                    }
                };
                if actual_telemetry_type != telemetry_type {
                    return DatasetActivity::Skipped;
                }

                let request = crate::query::CountsRequest {
                    stream: name.clone(),
                    start_time,
                    end_time,
                    num_bins: Some(1),
                    conditions: None,
                };
                match request.get_bin_density(context.tenant_id).await {
                    Ok(counts) => DatasetActivity::Count {
                        name,
                        events: counts.into_iter().map(|record| record.count).sum(),
                    },
                    Err(error) => DatasetActivity::Error {
                        name,
                        message: error.to_string(),
                    },
                }
            }
        })
        .buffer_unordered(DATASET_DISCOVERY_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut active = Vec::new();
    let mut queried_datasets = 0_usize;
    let mut errors = Vec::new();
    for result in activity {
        match result {
            DatasetActivity::Skipped => {}
            DatasetActivity::Count { name, events } => {
                queried_datasets += 1;
                if events > 0 {
                    active.push((name, events));
                }
            }
            DatasetActivity::Error { name, message } => {
                errors.push(json!({ "name": name, "message": message }));
            }
        }
    }
    active.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let unavailable_datasets = errors.len();

    if active.is_empty() && !errors.is_empty() {
        tracing::warn!(
            telemetry_type,
            visible_datasets,
            queried_datasets,
            unavailable_datasets,
            "dataset activity discovery was incomplete"
        );
        return Ok(json!({
            "status": "error",
            "message": "Dataset activity could not be determined reliably for the requested telemetry type and time range.",
            "telemetryType": telemetry_type,
            "startTime": start_time,
            "endTime": end_time,
            "visibleDatasets": visible_datasets,
            "queriedDatasets": queried_datasets,
            "unavailableDatasets": errors.into_iter().take(20).collect::<Vec<_>>(),
        }));
    }

    let active_count = active.len();
    let selected = active
        .into_iter()
        .take(limit)
        .map(|(name, events)| json!({ "name": name, "eventsInRange": events }))
        .collect::<Vec<_>>();
    let selected_names = selected
        .iter()
        .filter_map(|dataset| dataset.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    Ok(json!({
        "status": if selected.is_empty() { "not_found" } else { "resolved" },
        "selectionSource": "activity_in_resolved_time_range",
        "telemetryType": telemetry_type,
        "startTime": start_time,
        "endTime": end_time,
        "queriedDatasets": queried_datasets,
        "visibleDatasets": visible_datasets,
        "activeDatasetCount": active_count,
        "truncated": active_count > selected.len(),
        "datasets": selected,
        "selectedDatasets": selected_names,
        "unavailableDatasets": errors.into_iter().take(20).collect::<Vec<_>>(),
    }))
}

async fn execute_dataset_metadata_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    let dataset = required_string(arguments, "dataset")?;
    match name {
        "get_dataset_schema" => {
            authorize(context.session_key, Action::GetSchema, Some(dataset), None)?;
            let schema = logstream::get_schema_internal(dataset, context.tenant_id)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Value::String(logstream::compact_schema(&schema)))
        }
        "get_dataset_info" => {
            authorize(
                context.session_key,
                Action::GetStreamInfo,
                Some(dataset),
                None,
            )?;
            serde_json::to_value(
                get_prism_logstream_info(dataset, context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_dataset_stats" => {
            authorize(context.session_key, Action::GetStats, Some(dataset), None)?;
            querier_logstream::get_stats_internal(dataset, context.tenant_id, None)
                .await
                .map_err(|error| error.to_string())
        }
        _ => unreachable!("dataset metadata dispatcher only receives known tools"),
    }
}

pub fn sample_events_query(
    arguments: &Value,
    fields: &[String],
    reference: DateTime<Utc>,
) -> Result<query::Query, String> {
    let dataset = required_string(arguments, "dataset")?;
    let limit = bounded_integer(arguments, "limit", 10, 1, 100);
    let (start, end) = match (
        arguments.get("startTime").and_then(Value::as_str),
        arguments.get("endTime").and_then(Value::as_str),
    ) {
        (Some(start), Some(end)) => {
            let range =
                TimeRange::parse_human_time(start, end).map_err(|error| error.to_string())?;
            (range.start, range.end)
        }
        (None, None) => {
            let minutes = bounded_integer(arguments, "minutes", 60, 1, 1440);
            (reference - Duration::minutes(minutes as i64), reference)
        }
        _ => return Err("startTime and endTime must be provided together".to_owned()),
    };
    let preferred = [
        "p_timestamp",
        "service.name",
        "service_name",
        "severity_text",
        "severity_number",
        "body",
        "message",
        "trace_id",
        "span_id",
        "duration_nano",
    ];
    let mut selected = preferred
        .iter()
        .filter(|candidate| fields.iter().any(|field| field == **candidate))
        .map(|field| (*field).to_owned())
        .collect::<Vec<_>>();
    if selected.is_empty() {
        selected.extend(fields.iter().take(MAX_TOOL_COLUMNS).cloned());
    }
    if selected.is_empty() {
        return Err("dataset schema has no fields".to_owned());
    }
    let order = if selected.iter().any(|field| field == "p_timestamp") {
        " ORDER BY \"p_timestamp\" DESC"
    } else {
        ""
    };
    Ok(query::Query {
        query: format!(
            "SELECT {} FROM {}{order} LIMIT {limit}",
            selected
                .iter()
                .map(|field| quote_identifier(field))
                .collect::<Vec<_>>()
                .join(", "),
            quote_identifier(dataset),
        ),
        start_time: start.to_rfc3339(),
        end_time: end.to_rfc3339(),
        send_null: true,
        fields: false,
        streaming: false,
        filter_tags: None,
    })
}

pub async fn dataset_field_names(
    dataset: &str,
    tenant_id: &Option<String>,
) -> Result<Vec<String>, String> {
    let schema = logstream::get_schema_internal(dataset, tenant_id)
        .await
        .map_err(|error| error.to_string())?;
    Ok(schema
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect())
}

async fn sample_events(arguments: &Value, context: OssToolContext<'_>) -> Result<Value, String> {
    let dataset = required_string(arguments, "dataset")?;
    authorize(context.session_key, Action::Query, Some(dataset), None)?;
    let fields = dataset_field_names(dataset, context.tenant_id).await?;
    let response = query::query_internal(
        sample_events_query(arguments, &fields, Utc::now())?,
        context.session_key,
        context.tenant_id,
    )
    .await
    .map_err(|error| error.to_string())?;
    enforce_tool_tabular_limits(response_value(response).await?)
}

fn normalized_log_context_bounds(
    arguments: &Value,
    anchor: DateTime<Utc>,
) -> Result<(String, String), String> {
    let start_time = arguments
        .get("contextStartTime")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| (anchor - Duration::minutes(5)).to_rfc3339());
    let end_time = arguments
        .get("contextEndTime")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| (anchor + Duration::minutes(5)).to_rfc3339());
    let time_range = TimeRange::parse_human_time(&start_time, &end_time)
        .map_err(|error| format!("invalid log context time range: {error}"))?;
    Ok((time_range.start.to_rfc3339(), time_range.end.to_rfc3339()))
}

pub fn log_context_request(arguments: &Value) -> Result<Value, String> {
    let dataset = required_string(arguments, "dataset")?;
    let timestamp = required_string(arguments, "pTimestamp")?;
    let mut body = serde_json::Map::from_iter([
        ("dataset".to_owned(), json!(dataset)),
        ("pTimestamp".to_owned(), json!(timestamp)),
    ]);
    let mut has_match_field = false;
    for name in ["log", "body", "message"] {
        if let Some(value) = arguments.get(name).and_then(Value::as_str) {
            body.insert(name.to_owned(), json!(value));
            has_match_field = true;
        }
    }
    if !has_match_field {
        return Err(
            "get_log_context requires at least one exact `log`, `body`, or `message` value from the anchor row"
                .to_owned(),
        );
    }
    for name in ["contextWindow", "contextStartTime", "contextEndTime"] {
        if let Some(value) = arguments.get(name).and_then(Value::as_str) {
            body.insert(name.to_owned(), json!(value));
        }
    }
    if !body.contains_key("contextWindow")
        && !body.contains_key("contextStartTime")
        && !body.contains_key("contextEndTime")
    {
        body.insert("contextWindow".to_owned(), json!("5 minutes"));
    }
    if let Some(conditions) = arguments.get("conditions") {
        body.insert("conditions".to_owned(), conditions.clone());
    }
    body.insert(
        "pageSize".to_owned(),
        json!(bounded_integer(arguments, "pageSize", 100, 1, 500)),
    );
    Ok(Value::Object(body))
}

pub fn project_log_context_result(mut value: Value) -> Value {
    const PREFERRED_FIELDS: [&str; 10] = [
        "p_timestamp",
        "service.name",
        "service_name",
        "severity_text",
        "severity_description",
        "body",
        "message",
        "log",
        "trace_id",
        "span_id",
    ];
    let rows = match &mut value {
        Value::Array(rows) => Some(rows),
        Value::Object(object) => object.get_mut("records").and_then(Value::as_array_mut),
        _ => None,
    };
    if let Some(rows) = rows {
        rows.truncate(MAX_TOOL_ROWS as usize);
        for row in rows {
            let Some(object) = row.as_object_mut() else {
                continue;
            };
            let mut selected = PREFERRED_FIELDS
                .iter()
                .filter(|field| object.contains_key(**field))
                .take(MAX_TOOL_COLUMNS)
                .map(|field| (*field).to_owned())
                .collect::<Vec<_>>();
            if selected.is_empty() {
                selected.extend(object.keys().take(MAX_TOOL_COLUMNS).cloned());
            }
            object.retain(|field, _| selected.contains(field));
        }
    }
    value
}

async fn get_log_context(arguments: &Value, context: OssToolContext<'_>) -> Result<Value, String> {
    log_context_request(arguments)?;
    let dataset = required_string(arguments, "dataset")?;
    authorize(context.session_key, Action::Query, Some(dataset), None)?;
    let anchor = DateTime::parse_from_rfc3339(required_string(arguments, "pTimestamp")?)
        .map_err(|error| error.to_string())?
        .with_timezone(&Utc);
    let (start_time, end_time) = normalized_log_context_bounds(arguments, anchor)?;
    let limit = bounded_integer(arguments, "pageSize", 100, 1, 500);
    let schema = logstream::get_schema_internal(dataset, context.tenant_id)
        .await
        .map_err(|error| error.to_string())?;
    let fields = schema
        .fields()
        .iter()
        .take(10)
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Err("dataset schema has no fields".to_owned());
    }
    let response = query::query_internal(
        query::Query {
            query: format!(
                "SELECT {} FROM {} WHERE {} >= {} AND {} <= {} ORDER BY {} LIMIT {limit}",
                fields.join(", "),
                quote_identifier(dataset),
                quote_identifier("p_timestamp"),
                quote_literal(&start_time),
                quote_identifier("p_timestamp"),
                quote_literal(&end_time),
                quote_identifier("p_timestamp"),
            ),
            start_time,
            end_time,
            send_null: true,
            fields: false,
            streaming: false,
            filter_tags: None,
        },
        context.session_key,
        context.tenant_id,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(project_log_context_result(response_value(response).await?))
}

fn first_present_field(fields: &[String], candidates: &[&str]) -> Option<String> {
    candidates
        .iter()
        .find(|candidate| fields.iter().any(|field| field == **candidate))
        .map(|field| (*field).to_owned())
}

pub fn list_log_errors_query(
    dataset: &str,
    fields: &[String],
    limit: u64,
) -> Result<String, String> {
    let service = first_present_field(fields, &["service.name", "service_name"]).map_or_else(
        || "'unknown'".to_owned(),
        |field| {
            format!(
                "COALESCE(CAST({} AS VARCHAR), 'unknown')",
                quote_identifier(&field)
            )
        },
    );
    let severity_text = first_present_field(fields, &["severity_text", "severity_description"]);
    let exception_type = first_present_field(
        fields,
        &["event_exception.type", "exception.type", "error.type"],
    );
    let error_type = exception_type
        .as_ref()
        .or(severity_text.as_ref())
        .map_or_else(
            || "'error'".to_owned(),
            |field| {
                format!(
                    "COALESCE(CAST({} AS VARCHAR), 'error')",
                    quote_identifier(field)
                )
            },
        );
    let message = first_present_field(
        fields,
        &[
            "event_exception.message",
            "exception.message",
            "error.message",
            "body",
            "message",
            "log",
        ],
    );
    let sample_message = message.as_ref().map_or_else(
        || "CAST(NULL AS VARCHAR)".to_owned(),
        |field| format!("MAX(CAST({} AS VARCHAR))", quote_identifier(field)),
    );
    let last_seen = first_present_field(fields, &["p_timestamp", "time_unix_nano"]).map_or_else(
        || "CAST(NULL AS VARCHAR)".to_owned(),
        |field| format!("MAX({})", quote_identifier(&field)),
    );

    let mut predicates = Vec::new();
    if fields.iter().any(|field| field == "severity_number") {
        predicates.push("CAST(\"severity_number\" AS BIGINT) >= 17".to_owned());
    }
    if let Some(field) = &severity_text {
        predicates.push(format!(
            "UPPER(CAST({} AS VARCHAR)) IN ('ERROR', 'FATAL', 'CRITICAL')",
            quote_identifier(field)
        ));
    }
    if let Some(field) = &exception_type {
        predicates.push(format!(
            "{} IS NOT NULL AND CAST({} AS VARCHAR) <> ''",
            quote_identifier(field),
            quote_identifier(field)
        ));
    }
    if predicates.is_empty()
        && let Some(field) = &message
    {
        let field = quote_identifier(field);
        predicates.push(format!(
            "(LOWER(CAST({field} AS VARCHAR)) LIKE '%error%' OR LOWER(CAST({field} AS VARCHAR)) LIKE '%exception%' OR LOWER(CAST({field} AS VARCHAR)) LIKE '%failed%' OR LOWER(CAST({field} AS VARCHAR)) LIKE '%fatal%' OR LOWER(CAST({field} AS VARCHAR)) LIKE '%panic%')"
        ));
    }
    if predicates.is_empty() {
        return Err(
            "log dataset has no supported severity, exception, or message field".to_owned(),
        );
    }

    Ok(format!(
        "SELECT {service} AS \"serviceName\", {error_type} AS \"errorType\", {sample_message} AS \"sampleMessage\", COUNT(*) AS \"occurrences\", {last_seen} AS \"lastSeen\" FROM {} WHERE ({}) GROUP BY {service}, {error_type} ORDER BY \"occurrences\" DESC LIMIT {}",
        quote_identifier(dataset),
        predicates.join(" OR "),
        limit.clamp(1, 500),
    ))
}

async fn list_log_errors(arguments: &Value, context: OssToolContext<'_>) -> Result<Value, String> {
    let dataset = required_string(arguments, "dataset")?;
    authorize(context.session_key, Action::Query, Some(dataset), None)?;
    let info = serde_json::to_value(
        get_prism_logstream_info(dataset, context.tenant_id)
            .await
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    if !nested_string(&info, "telemetryType").is_some_and(|value| value == "logs") {
        return Err(format!(
            "tool requires a logs dataset, but `{dataset}` is not logs telemetry"
        ));
    }
    let schema = logstream::get_schema_internal(dataset, context.tenant_id)
        .await
        .map_err(|error| error.to_string())?;
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    let limit = bounded_integer(arguments, "limit", 50, 1, 500);
    let query_text = bounded_select(&list_log_errors_query(dataset, &fields, limit)?)?;
    let response = query::query_internal(
        query::Query {
            query: query_text,
            start_time: required_string(arguments, "startTime")?.to_owned(),
            end_time: required_string(arguments, "endTime")?.to_owned(),
            send_null: true,
            fields: false,
            streaming: false,
            filter_tags: None,
        },
        context.session_key,
        context.tenant_id,
    )
    .await
    .map_err(|error| error.to_string())?;
    let records = response_value(response).await?;
    let total = records.as_array().map_or(0, Vec::len);
    Ok(json!({
        "total": total,
        "offset": 0,
        "limit": limit,
        "records": records,
    }))
}

async fn execute_sql_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    authorize(context.session_key, Action::Query, None, None)?;
    let query_text = required_string(arguments, "query")?;
    let query_text = if name == "explain_query" {
        format!("EXPLAIN {}", bounded_select(query_text)?)
    } else {
        bounded_select(query_text)?
    };
    let response = query::query_internal(
        query::Query {
            query: query_text,
            start_time: required_string(arguments, "startTime")?.to_owned(),
            end_time: required_string(arguments, "endTime")?.to_owned(),
            send_null: true,
            fields: false,
            streaming: false,
            filter_tags: None,
        },
        context.session_key,
        context.tenant_id,
    )
    .await
    .map_err(|error| error.to_string())?;
    response_value(response).await
}

async fn execute_alert_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    match name {
        "list_alerts" => {
            authorize(context.session_key, Action::GetAlert, None, None)?;
            serde_json::to_value(
                alerts::list_internal(context.session_key.clone(), &HashMap::new())
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_alert" => {
            authorize(context.session_key, Action::GetAlert, None, None)?;
            let id = Ulid::from_string(required_string(arguments, "alert_id")?)
                .map_err(|error| error.to_string())?;
            serde_json::to_value(
                alerts::get_internal(context.session_key, id, context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "list_alert_tags" => {
            authorize(context.session_key, Action::GetAlert, None, None)?;
            serde_json::to_value(
                alerts::list_tags_internal(context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "enable_alert" | "disable_alert" | "evaluate_alert" => {
            authorize(context.session_key, Action::PutAlert, None, None)?;
            let id = Ulid::from_string(required_string(arguments, "alert_id")?)
                .map_err(|error| error.to_string())?;
            let response = match name {
                "enable_alert" => {
                    alerts::enable_alert_internal(context.session_key, id, context.tenant_id).await
                }
                "disable_alert" => {
                    alerts::disable_alert_internal(context.session_key, id, context.tenant_id).await
                }
                _ => {
                    alerts::evaluate_alert_internal(context.session_key, id, context.tenant_id)
                        .await
                }
            }
            .map_err(|error| error.to_string())?;
            serde_json::to_value(response).map_err(|error| error.to_string())
        }
        "create_alert" => create_alert(arguments, context).await,
        _ => unreachable!("alert dispatcher only receives known tools"),
    }
}

async fn create_alert(arguments: &Value, context: OssToolContext<'_>) -> Result<Value, String> {
    authorize(context.session_key, Action::PutAlert, None, None)?;
    let request = serde_json::from_value(
        arguments
            .get("spec")
            .cloned()
            .ok_or_else(|| "missing required object argument `spec`".to_owned())?,
    )
    .map_err(|error| format!("invalid alert spec: {error}"))?;
    serde_json::to_value(
        alerts::post_internal(request, context.session_key, context.tenant_id)
            .await
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

async fn execute_alert_target_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    match name {
        "list_alert_targets" => {
            authorize(context.session_key, Action::GetAlert, None, None)?;
            serde_json::to_value(
                targets::list_internal(context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_alert_target" => {
            authorize(context.session_key, Action::GetAlert, None, None)?;
            let id = Ulid::from_string(required_string(arguments, "target_id")?)
                .map_err(|error| error.to_string())?;
            targets::get_internal(id, context.tenant_id)
                .await
                .map_err(|error| error.to_string())
        }
        "create_alert_target" => {
            authorize(context.session_key, Action::PutAlert, None, None)?;
            let target = serde_json::from_value(
                arguments
                    .get("spec")
                    .cloned()
                    .ok_or_else(|| "missing required object argument `spec`".to_owned())?,
            )
            .map_err(|error| format!("invalid alert target spec: {error}"))?;
            targets::post_internal(target, context.tenant_id)
                .await
                .map_err(|error| error.to_string())
        }
        _ => unreachable!("alert target dispatcher only receives known tools"),
    }
}

async fn execute_trace_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    let datasets = [required_string(arguments, "dataset")?.to_owned()];
    let start_time = required_string(arguments, "startTime")?.to_owned();
    let end_time = required_string(arguments, "endTime")?.to_owned();
    let default_limit = if name == "list_trace_errors" { 50 } else { 100 };
    let limit = bounded_integer(arguments, "limit", default_limit, 1, 500) as usize;
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for dataset in datasets {
        if let Err(message) = authorize(context.session_key, Action::Query, Some(&dataset), None) {
            errors.push(json!({ "dataset": dataset, "message": message }));
            continue;
        }
        let response = if name == "get_trace" {
            crate::handlers::http::traces::get_trace_detail_internal(
                crate::handlers::http::traces::TraceDetailRequest {
                    dataset: dataset.clone(),
                    trace_id: required_string(arguments, "traceId")?.to_owned(),
                    start_time: start_time.clone(),
                    end_time: end_time.clone(),
                },
                context.tenant_id,
                context.session_key,
                Some(context.query_headers.clone()),
            )
            .await
        } else {
            crate::handlers::http::traces::list_traces_internal(
                crate::handlers::http::traces::TraceListRequest {
                    dataset: dataset.clone(),
                    service_name: arguments
                        .get("serviceName")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    start_time: start_time.clone(),
                    end_time: end_time.clone(),
                    sort_by: None,
                    conditions: None,
                    options: (name == "list_trace_errors")
                        .then_some(crate::handlers::http::traces::TraceListOption::ErrorSpans),
                    limit: Some(limit),
                    offset: Some(0),
                },
                context.tenant_id,
                context.session_key,
                Some(context.query_headers.clone()),
            )
            .await
        };
        match response {
            Ok(response) => results.push(json!({ "dataset": dataset, "response": response })),
            Err(error) => errors.push(json!({
                "dataset": dataset,
                "message": error.to_string(),
            })),
        }
    }
    if results.is_empty() {
        return Err(format!("all selected datasets failed: {errors:?}"));
    }
    Ok(json!({ "results": results, "errors": errors }))
}

fn time_bucket_interval(start: &str, end: &str, bucket_count: u64) -> String {
    let seconds = TimeRange::parse_human_time(start, end)
        .map(|range| (range.end - range.start).num_seconds().max(1) as u64)
        .unwrap_or(3_600);
    format!("{} seconds", (seconds / bucket_count.max(1)).max(1))
}

fn trace_activity_query(
    dataset: &str,
    time_field: &str,
    interval: &str,
    group_by: &[String],
    clauses: &[String],
) -> String {
    let bucket = format!(
        "CAST(DATE_BIN({}, {}, TIMESTAMP '1970-01-01T00:00:00Z') AS TEXT)",
        quote_literal(interval),
        quote_identifier(time_field),
    );
    let mut select = vec![format!("{bucket} AS \"time\"")];
    select.extend(group_by.iter().map(|field| quote_identifier(field)));
    select.push("COUNT(DISTINCT \"span_span_id\") AS \"span_count\"".to_owned());
    select.push("COUNT(DISTINCT \"span_trace_id\") AS \"trace_count\"".to_owned());
    let groups = (1..=group_by.len() + 1)
        .map(|position| position.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    format!(
        "SELECT {} FROM {}{} GROUP BY {} ORDER BY 1 LIMIT 500",
        select.join(", "),
        quote_identifier(dataset),
        where_clause,
        groups,
    )
}

async fn execute_trace_activity_tool(
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    let dataset = required_string(arguments, "dataset")?;
    let start_time = required_string(arguments, "startTime")?;
    let end_time = required_string(arguments, "endTime")?;
    authorize(context.session_key, Action::Query, Some(dataset), None)?;
    if !PARSEABLE
        .check_or_load_stream(dataset, context.tenant_id)
        .await
    {
        return Err(format!("dataset `{dataset}` metadata is unavailable"));
    }
    let stream = PARSEABLE
        .get_stream(dataset, context.tenant_id)
        .map_err(|error| error.to_string())?;
    let telemetry_type = stream
        .metadata
        .read()
        .map_err(|error| error.to_string())?
        .telemetry_type
        .to_string();
    if telemetry_type != "traces" {
        return Err(format!(
            "dataset `{dataset}` has telemetry type `{telemetry_type}`; expected `traces`"
        ));
    }
    let schema = logstream::get_schema_internal(dataset, context.tenant_id)
        .await
        .map_err(|error| error.to_string())?;
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    for required in ["span_span_id", "span_trace_id"] {
        if !fields.iter().any(|field| field == required) {
            return Err(format!(
                "trace dataset `{dataset}` is missing required field `{required}`"
            ));
        }
    }
    let time_field = stream
        .get_time_partition()
        .unwrap_or_else(|| "p_timestamp".to_owned());
    if !fields.contains(&time_field) {
        return Err(format!(
            "trace dataset `{dataset}` is missing time field `{time_field}`"
        ));
    }
    let group_by = arguments
        .get("groupBy")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "groupBy entries must be strings".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if group_by.len() > 5 {
        return Err("groupBy may contain at most 5 fields".to_owned());
    }
    for field in &group_by {
        if !fields.contains(field) {
            return Err(format!("groupBy field `{field}` does not exist"));
        }
    }
    let mut clauses = Vec::new();
    for (field, value) in arguments
        .get("filters")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        if !fields.iter().any(|candidate| candidate == field) {
            return Err(format!("filter field `{field}` does not exist"));
        }
        match value {
            Value::String(value) => clauses.push(format!(
                "{} = {}",
                quote_identifier(field),
                quote_literal(value)
            )),
            Value::Number(value) => clauses.push(format!("{} = {value}", quote_identifier(field))),
            Value::Bool(value) => clauses.push(format!("{} = {value}", quote_identifier(field))),
            Value::Null => clauses.push(format!("{} IS NULL", quote_identifier(field))),
            _ => return Err("filter values must be JSON scalars".to_owned()),
        }
    }
    let bucket_count = bounded_integer(arguments, "bucketCount", 60, 1, 100);
    let interval = time_bucket_interval(start_time, end_time, bucket_count);
    let query = trace_activity_query(dataset, &time_field, &interval, &group_by, &clauses);
    let response = query::query_internal(
        query::Query {
            query: bounded_select(&query)?,
            start_time: start_time.to_owned(),
            end_time: end_time.to_owned(),
            send_null: true,
            fields: false,
            streaming: false,
            filter_tags: None,
        },
        context.session_key,
        context.tenant_id,
    )
    .await
    .map_err(|error| error.to_string())?;
    let rows = response_value(response).await?;
    Ok(json!({
        "dataset": dataset,
        "queryType": "sql",
        "query": query,
        "bucketInterval": interval,
        "semantics": {
            "span_count": "COUNT(DISTINCT span_span_id)",
            "trace_count": "COUNT(DISTINCT span_trace_id)"
        },
        "rows": rows,
    }))
}

async fn execute_identity_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    match name {
        "list_users" => {
            authorize(context.session_key, Action::ListUser, None, None)?;
            Ok(rbac::list_users_internal(context.tenant_id))
        }
        "get_user" => {
            let user = required_string(arguments, "userid")?;
            authorize(context.session_key, Action::GetUserRoles, None, Some(user))?;
            serde_json::to_value(
                rbac::get_user_internal(user, context.tenant_id)
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_user_roles" => {
            let user = required_string(arguments, "userid")?;
            authorize(context.session_key, Action::GetUserRoles, None, Some(user))?;
            rbac::get_role_internal(user, context.tenant_id).map_err(|error| error.to_string())
        }
        "list_roles" => {
            authorize(context.session_key, Action::ListRole, None, None)?;
            serde_json::to_value(
                role::list_internal(context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_role" => {
            authorize(context.session_key, Action::GetRole, None, None)?;
            serde_json::to_value(
                role::get_internal(required_string(arguments, "name")?, context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_default_role" => {
            authorize(context.session_key, Action::GetRole, None, None)?;
            Ok(role::get_default_internal(context.tenant_id))
        }
        _ => unreachable!("identity dispatcher only receives known tools"),
    }
}

async fn execute_system_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    match name {
        "ping" => {
            authorize(context.session_key, Action::GetAbout, None, None)?;
            Ok(json!({
                "about": crate::handlers::http::about::about().await.into_inner(),
                "liveness": { "status": health_check::liveness_internal().as_u16() },
                "readiness": {
                    "status": health_check::readiness_internal(context.tenant_id).await.as_u16()
                },
            }))
        }
        "get_cluster_status" => {
            authorize(context.session_key, Action::ListCluster, None, None)?;
            serde_json::to_value(
                cluster::get_cluster_info_internal(context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_cluster_metrics" => {
            authorize(context.session_key, Action::ListClusterMetrics, None, None)?;
            serde_json::to_value(
                cluster::get_cluster_metrics_internal(context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_retention" => {
            let dataset = required_string(arguments, "dataset")?;
            authorize(
                context.session_key,
                Action::GetRetention,
                Some(dataset),
                None,
            )?;
            serde_json::to_value(
                logstream::get_retention_internal(dataset, context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "get_hot_tier_config" => get_hot_tier_config(arguments, context).await,
        _ => unreachable!("system dispatcher only receives known tools"),
    }
}

async fn get_hot_tier_config(
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    let dataset = required_string(arguments, "dataset")?;
    authorize(
        context.session_key,
        Action::GetHotTierEnabled,
        Some(dataset),
        None,
    )?;
    serde_json::to_value(
        logstream::get_stream_hot_tier_internal(dataset, context.tenant_id)
            .await
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

async fn execute_saved_object_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    match name {
        "list_filters" => {
            authorize(context.session_key, Action::ListFilter, None, None)?;
            serde_json::to_value(filters::list_internal(context.session_key).await)
                .map_err(|error| error.to_string())
        }
        "get_filter" => {
            authorize(context.session_key, Action::GetFilter, None, None)?;
            serde_json::to_value(
                filters::get_internal(
                    required_string(arguments, "id")?,
                    context.session_key,
                    context.tenant_id,
                )
                .await
                .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "list_dashboard_tags" => {
            authorize(context.session_key, Action::ListDashboard, None, None)?;
            serde_json::to_value(dashboards::list_tags_internal(context.tenant_id).await)
                .map_err(|error| error.to_string())
        }
        "get_dashboard" => {
            authorize(context.session_key, Action::GetDashboard, None, None)?;
            let id = Ulid::from_string(required_string(arguments, "dashboardId")?)
                .map_err(|error| error.to_string())?;
            serde_json::to_value(
                dashboards::get_dashboard_internal(id, context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
        }
        "list_dashboards" => {
            authorize(context.session_key, Action::ListDashboard, None, None)?;
            let limit = bounded_integer(arguments, "limit", 100, 1, 500) as usize;
            let query = arguments
                .get("query")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase);
            let fetch_limit = if query.is_some() { 0 } else { limit };
            Ok(Value::Array(
                dashboards::list_dashboards_internal(fetch_limit, None, context.tenant_id)
                    .await
                    .into_iter()
                    .filter(|dashboard| {
                        query.as_ref().is_none_or(|query| {
                            dashboard
                                .get("title")
                                .and_then(Value::as_str)
                                .is_some_and(|title| title.to_ascii_lowercase().contains(query))
                                || dashboard.get("tags").and_then(Value::as_array).is_some_and(
                                    |tags| {
                                        tags.iter()
                                            .filter_map(Value::as_str)
                                            .any(|tag| tag.to_ascii_lowercase().contains(query))
                                    },
                                )
                        })
                    })
                    .take(limit)
                    .collect(),
            ))
        }
        _ => unreachable!("saved object dispatcher only receives known tools"),
    }
}

async fn execute_oss_tool_inner(
    name: &str,
    arguments: &Value,
    session_key: &SessionKey,
    tenant_id: &Option<String>,
    query_headers: &HeaderMap,
) -> Result<Value, String> {
    let context = OssToolContext {
        session_key,
        tenant_id,
        query_headers,
    };
    match name {
        "list_datasets" | "resolve_dataset" | "resolve_time_range" => {
            execute_dataset_resolution_tool(name, arguments, context)
        }
        "discover_datasets" => discover_datasets(arguments, context).await,
        "get_dataset_schema" | "get_dataset_info" | "get_dataset_stats" => {
            execute_dataset_metadata_tool(name, arguments, context).await
        }
        "sample_events" => sample_events(arguments, context).await,
        "get_log_context" => get_log_context(arguments, context).await,
        "list_log_errors" => list_log_errors(arguments, context).await,
        "query_sql" | "explain_query" => execute_sql_tool(name, arguments, context).await,
        "list_alerts" | "get_alert" | "list_alert_tags" | "enable_alert" | "disable_alert"
        | "evaluate_alert" | "create_alert" => execute_alert_tool(name, arguments, context).await,
        "list_alert_targets" | "get_alert_target" | "create_alert_target" => {
            execute_alert_target_tool(name, arguments, context).await
        }
        "get_traces" | "get_trace" | "list_trace_errors" => {
            execute_trace_tool(name, arguments, context).await
        }
        "get_trace_activity" => execute_trace_activity_tool(arguments, context).await,
        "ping"
        | "get_cluster_status"
        | "get_cluster_metrics"
        | "get_retention"
        | "get_hot_tier_config" => execute_system_tool(name, arguments, context).await,
        "list_users" | "get_user" | "get_user_roles" | "list_roles" | "get_role"
        | "get_default_role" => execute_identity_tool(name, arguments, context).await,
        "list_filters"
        | "get_filter"
        | "list_dashboard_tags"
        | "get_dashboard"
        | "list_dashboards" => execute_saved_object_tool(name, arguments, context).await,
        _ => Err(format!(
            "tool `{name}` is registered but does not yet have an OSS internal executor"
        )),
    }
}

/// Execute a registered OSS tool in-process. Enterprise uses this only after
/// applying its own agent policy and action-confirmation checks, so catalog
/// validation and the underlying platform implementation remain canonical.
pub async fn execute_oss_tool(
    name: &str,
    arguments: &Value,
    session_key: &SessionKey,
    tenant_id: &Option<String>,
    request_headers: &HeaderMap,
) -> Result<Value, String> {
    execute_oss_tool_with_query_headers(name, arguments, session_key, tenant_id, request_headers)
        .await
}

/// Execute a registered OSS tool with separate headers for internal query-node
/// requests. Prism callers use this to preserve their server-authenticated
/// routing headers without changing the external MCP execution path.
pub async fn execute_oss_tool_with_query_headers(
    name: &str,
    arguments: &Value,
    session_key: &SessionKey,
    tenant_id: &Option<String>,
    query_headers: &HeaderMap,
) -> Result<Value, String> {
    let spec = oss_tool_specs()
        .into_iter()
        .find(|tool| tool.name == name)
        .ok_or_else(|| format!("unknown OSS tool `{name}`"))?;
    spec.validate_arguments(arguments)?;
    execute_oss_tool_inner(name, arguments, session_key, tenant_id, query_headers).await
}

/// Generic execution endpoint used by external MCP adapters.
pub async fn call_tool_registry(
    req: HttpRequest,
    web::Json(call): web::Json<ToolCallRequest>,
) -> HttpResponse {
    if !call.arguments.is_object() {
        return HttpResponse::BadRequest().json(ToolCallResult::error(
            "tool arguments must be a JSON object",
        ));
    }
    let session_key = match extract_session_key_from_req(&req) {
        Ok(session_key) => session_key,
        Err(error) => {
            return HttpResponse::Unauthorized().json(ToolCallResult::error(error.to_string()));
        }
    };
    if !Users.session_exists(&session_key) {
        return HttpResponse::Unauthorized().json(ToolCallResult::error(
            "Your session has expired or is no longer valid. Please re-authenticate to access this resource.",
        ));
    }
    let Some(spec) = oss_read_tool_specs()
        .into_iter()
        .find(|tool| tool.name == call.name)
    else {
        return HttpResponse::NotFound().json(ToolCallResult::error("unknown tool"));
    };
    if let Err(error) = spec.validate_arguments(&call.arguments) {
        return HttpResponse::BadRequest().json(ToolCallResult::error(error));
    }
    let tenant_id = get_tenant_id_from_request(&req);
    match execute_oss_tool(
        spec.name,
        &call.arguments,
        &session_key,
        &tenant_id,
        req.headers(),
    )
    .await
    {
        Ok(result) => HttpResponse::Ok().json(ToolCallResult::success(result)),
        Err(message) => HttpResponse::Ok().json(ToolCallResult::error(message)),
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test, web};
    use chrono::{DateTime, Duration, Utc};
    use serde_json::{Value, json};

    use super::{
        bounded_tool_sql, call_tool_registry, list_log_errors_query, log_context_request,
        normalized_log_context_bounds, project_log_context_result, resolve_dataset_candidates,
        resolve_time_range_at, sample_events_query, trace_activity_query,
    };

    #[actix_web::test]
    async fn shared_sql_limits_reject_wildcards_and_excess_columns() {
        assert!(bounded_tool_sql("SELECT a, b FROM logs").is_ok());
        assert!(bounded_tool_sql("SELECT * FROM logs").is_err());
        assert!(bounded_tool_sql("SELECT a,b,c,d,e,f,g,h,i,j,k FROM logs").is_err());
    }

    #[actix_web::test]
    async fn shared_dataset_resolution_does_not_select_an_arbitrary_fallback() {
        let datasets = json!([
            { "name": "application-logs" },
            { "name": "otel-metrics" },
            { "name": "otel-traces" }
        ]);
        let resolved =
            resolve_dataset_candidates(datasets.clone(), &json!({ "query": "show otel-metrics" }));
        assert_eq!(resolved["selectedDatasets"], json!(["otel-metrics"]));

        let ambiguous = resolve_dataset_candidates(
            datasets,
            &json!({ "query": "checkout errors", "limit": 2 }),
        );
        assert_eq!(ambiguous["status"], "ambiguous");
        assert_eq!(ambiguous["candidates"].as_array().unwrap().len(), 2);
    }

    #[actix_web::test]
    async fn shared_time_resolution_honors_timezone_and_reference() {
        let reference = DateTime::parse_from_rfc3339("2026-10-01T12:00:00Z")
            .unwrap()
            .to_utc();
        let resolved = resolve_time_range_at(
            &json!({
                "expression": "last 30 minutes",
                "timezone": "Asia/Kolkata"
            }),
            reference,
        )
        .unwrap();
        assert_eq!(resolved["startTime"], "2026-10-01T11:30:00+00:00");
        assert_eq!(resolved["endTime"], "2026-10-01T12:00:00+00:00");
    }

    #[actix_web::test]
    async fn log_context_projection_prefers_observability_fields() {
        let projected = project_log_context_result(json!([{
            "p_timestamp": "2026-10-22T00:00:00Z",
            "service.name": "checkout",
            "body": "failed",
            "unrelated": "drop"
        }]));
        assert!(projected[0].get("p_timestamp").is_some());
        assert!(projected[0].get("service.name").is_some());
        assert!(projected[0].get("unrelated").is_none());
    }

    #[actix_web::test]
    async fn sample_events_prefers_observability_fields_and_explicit_range() {
        let query = sample_events_query(
            &json!({
                "dataset": "logs",
                "startTime": "2026-10-22T00:00:00Z",
                "endTime": "2026-10-22T01:00:00Z",
                "limit": 5
            }),
            &[
                "unused".to_owned(),
                "p_timestamp".to_owned(),
                "service.name".to_owned(),
                "body".to_owned(),
            ],
            Utc::now(),
        )
        .unwrap();

        assert_eq!(query.start_time, "2026-10-22T00:00:00+00:00");
        assert!(
            query
                .query
                .contains("\"p_timestamp\", \"service.name\", \"body\"")
        );
        assert!(!query.query.contains("\"unused\""));
        assert!(query.query.ends_with("LIMIT 5"));
    }

    #[actix_web::test]
    async fn log_context_requires_an_exact_anchor_value() {
        assert!(
            log_context_request(&json!({
                "dataset": "logs",
                "pTimestamp": "2026-10-22T00:00:00Z"
            }))
            .is_err()
        );
    }

    #[actix_web::test]
    async fn trace_activity_uses_distinct_span_and_trace_identifiers() {
        let query = trace_activity_query(
            "astronomy-shop-traces",
            "p_timestamp",
            "60 seconds",
            &["service.name".to_owned()],
            &[],
        );

        assert!(query.contains("COUNT(DISTINCT \"span_span_id\") AS \"span_count\""));
        assert!(query.contains("COUNT(DISTINCT \"span_trace_id\") AS \"trace_count\""));
        assert!(query.contains("\"service.name\""));
        assert!(!query.contains("COUNT(*)"));
    }

    #[actix_web::test]
    async fn log_error_query_uses_otel_severity_fields() {
        let query = list_log_errors_query(
            "app-logs",
            &[
                "p_timestamp".to_owned(),
                "service.name".to_owned(),
                "severity_number".to_owned(),
                "severity_text".to_owned(),
                "body".to_owned(),
            ],
            5,
        )
        .unwrap();

        assert!(query.contains("CAST(\"severity_number\" AS BIGINT) >= 17"));
        assert!(query.contains("'ERROR', 'FATAL', 'CRITICAL'"));
        assert!(query.contains("LIMIT 5"));
    }

    #[actix_web::test]
    async fn log_context_bounds_normalize_relative_times() {
        let (start, end) = normalized_log_context_bounds(
            &json!({
                "contextStartTime": "now-15m",
                "contextEndTime": "now",
            }),
            Utc::now(),
        )
        .expect("relative bounds must be accepted");
        let start = start.parse::<DateTime<Utc>>().expect("valid start time");
        let end = end.parse::<DateTime<Utc>>().expect("valid end time");

        assert_eq!(end - start, Duration::minutes(15));
    }

    #[actix_web::test]
    async fn invocation_endpoint_requires_authentication() {
        let app =
            test::init_service(App::new().route("/tools/call", web::post().to(call_tool_registry)))
                .await;
        let request = test::TestRequest::post()
            .uri("/tools/call")
            .set_json(json!({ "name": "list_datasets", "arguments": {} }))
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn arguments_must_be_an_object() {
        let app =
            test::init_service(App::new().route("/tools/call", web::post().to(call_tool_registry)))
                .await;
        let request = test::TestRequest::post()
            .uri("/tools/call")
            .set_json(json!({ "name": "list_datasets", "arguments": [] }))
            .to_request();
        let response: Value = test::call_and_read_body_json(&app, request).await;

        assert!(
            response["isError"]
                .as_bool()
                .expect("isError must be a boolean")
        );
        assert_eq!(response["content"][0]["type"], "text");
        assert_eq!(
            response["content"][0]["text"],
            "tool arguments must be a JSON object"
        );
    }
}
