use std::{collections::HashMap, time::Instant};

use actix_web::{HttpRequest, HttpResponse, body::to_bytes_limited, http::header::HeaderMap, web};
use chrono::{DateTime, Duration, Utc};
use serde_json::{Value, json};
use ulid::Ulid;

use crate::{
    handlers::http::{
        alerts, cluster, health_check, logstream,
        modal::query::querier_logstream,
        query, rbac, role, targets,
        users::{dashboards, filters},
    },
    parseable::PARSEABLE,
    prism::logstream::get_prism_logstream_info,
    rbac::{Response, Users, map::SessionKey, role::Action},
    tool_catalog::{ToolCallRequest, ToolCallResult, ToolEffect, oss_tool_specs},
    utils::{actix::extract_session_key_from_req, get_tenant_id_from_request},
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

fn bounded_select(query: &str) -> Result<String, String> {
    let query = query.trim();
    let query = query.strip_suffix(';').unwrap_or(query);
    if query.contains(';') {
        return Err("exactly one SQL statement is required".to_owned());
    }
    let normalized = query.to_ascii_lowercase();
    if !(normalized.starts_with("select ") || normalized.starts_with("with ")) {
        return Err("only SQL SELECT queries are allowed".to_owned());
    }
    Ok(format!(
        "SELECT * FROM ({query}) AS __parseable_tool_query LIMIT 500"
    ))
}

fn visible_datasets(tenant_id: &Option<String>, session_key: &SessionKey) -> Vec<String> {
    let mut datasets = PARSEABLE
        .streams
        .list(tenant_id)
        .into_iter()
        .filter(|dataset| {
            Users.authorize(session_key.clone(), Action::ListStream, Some(dataset), None)
                == Response::Authorized
        })
        .collect::<Vec<_>>();
    datasets.sort_unstable();
    datasets
}

fn resolve_time_range(arguments: &Value) -> Result<Value, String> {
    let expression = required_string(arguments, "expression")?;
    let timezone = arguments
        .get("timezone")
        .and_then(Value::as_str)
        .unwrap_or("UTC");
    if !timezone.eq_ignore_ascii_case("utc") {
        return Err("OSS time resolution currently requires timezone `UTC`".to_owned());
    }
    let now = Utc::now();
    let normalized = expression.trim().to_ascii_lowercase();
    let (start, end) = match normalized.as_str() {
        "today" => (
            now.date_naive()
                .and_hms_opt(0, 0, 0)
                .expect("midnight is valid")
                .and_utc(),
            now,
        ),
        "yesterday" => {
            let today = now
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .expect("midnight is valid")
                .and_utc();
            (today - Duration::days(1), today)
        }
        _ => {
            let duration = normalized
                .strip_prefix("last ")
                .or_else(|| normalized.strip_prefix("past "))
                .unwrap_or(normalized.as_str());
            let duration = if duration.chars().any(|character| character.is_ascii_digit()) {
                duration.to_owned()
            } else {
                format!("1 {duration}")
            };
            let duration = humantime::parse_duration(&duration)
                .map_err(|error| format!("invalid time expression `{expression}`: {error}"))?;
            let duration = Duration::from_std(duration).map_err(|error| error.to_string())?;
            (now - duration, now)
        }
    };
    Ok(json!({
        "expression": expression,
        "timezone": "UTC",
        "startTime": start.to_rfc3339(),
        "endTime": end.to_rfc3339(),
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

#[tracing::instrument(
    name = "llm.tool.execute",
    skip_all,
    fields(
        otel.name = %format!("execute_tool {name}"),
        otel.kind = "internal",
        gen_ai.operation.name = "execute_tool",
        gen_ai.tool.name = %name,
        gen_ai.tool.type = "function",
        gen_ai.tool.call.arguments = %arguments,
        gen_ai.tool.call.result = tracing::field::Empty,
        error.type = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    )
)]
async fn execute_oss_tool(
    name: &str,
    arguments: &Value,
    session_key: &SessionKey,
    tenant_id: &Option<String>,
    request_headers: &HeaderMap,
) -> Result<Value, String> {
    let result =
        execute_oss_tool_inner(name, arguments, session_key, tenant_id, request_headers).await;
    let span = tracing::Span::current();
    match &result {
        Ok(result) => {
            span.record("gen_ai.tool.call.result", result.to_string());
        }
        Err(_) => {
            span.record("error.type", "_OTHER");
            span.record("otel.status_code", "ERROR");
        }
    }
    result
}

#[derive(Clone, Copy)]
struct OssToolContext<'a> {
    session_key: &'a SessionKey,
    tenant_id: &'a Option<String>,
    request_headers: &'a HeaderMap,
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
            let datasets = visible_datasets(context.tenant_id, context.session_key);
            let selected = arguments
                .get("selectedDataset")
                .or_else(|| arguments.get("contextDataset"))
                .and_then(Value::as_str)
                .or_else(|| arguments.get("query").and_then(Value::as_str));
            if let Some(selected) = selected
                && let Some(dataset) = datasets.iter().find(|dataset| dataset.as_str() == selected)
            {
                return Ok(json!({
                    "status": "resolved",
                    "datasets": [{ "name": dataset }],
                    "selectedDatasets": [dataset],
                    "confidence": 1.0,
                    "reason": "exact caller-visible dataset name",
                }));
            }
            Ok(json!({
                "status": if datasets.is_empty() { "not_found" } else { "ambiguous" },
                "datasets": [],
                "candidates": datasets.into_iter().map(|name| json!({ "name": name })).collect::<Vec<_>>(),
                "message": "Select one exact caller-visible dataset.",
            }))
        }
        "resolve_time_range" => resolve_time_range(arguments),
        _ => unreachable!("dataset resolution dispatcher only receives known tools"),
    }
}

async fn discover_datasets(
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    let telemetry_type = required_string(arguments, "telemetryType")?;
    if !matches!(telemetry_type, "logs" | "metrics" | "traces") {
        return Err("telemetryType must be logs, metrics, or traces".to_owned());
    }
    let start = DateTime::parse_from_rfc3339(required_string(arguments, "startTime")?)
        .map_err(|error| error.to_string())?;
    let end = DateTime::parse_from_rfc3339(required_string(arguments, "endTime")?)
        .map_err(|error| error.to_string())?;
    let limit = bounded_integer(arguments, "limit", 50, 1, 500) as usize;
    let mut datasets = Vec::new();
    let mut errors = Vec::new();
    for dataset in visible_datasets(context.tenant_id, context.session_key) {
        match get_prism_logstream_info(&dataset, context.tenant_id).await {
            Ok(info) => {
                let info = serde_json::to_value(info).map_err(|error| error.to_string())?;
                let matches_type = nested_string(&info, "telemetryType")
                    .is_some_and(|value| value.eq_ignore_ascii_case(telemetry_type));
                let active = nested_string(&info, "latestEventAt")
                    .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                    .is_some_and(|latest| latest >= start && latest <= end);
                if matches_type && active {
                    datasets.push(json!({ "name": dataset, "info": info }));
                    if datasets.len() == limit {
                        break;
                    }
                }
            }
            Err(error) => errors.push(json!({
                "dataset": dataset,
                "message": error.to_string(),
            })),
        }
    }
    Ok(json!({
        "telemetryType": telemetry_type,
        "startTime": start.to_rfc3339(),
        "endTime": end.to_rfc3339(),
        "datasets": datasets,
        "errors": errors,
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
            serde_json::to_value(
                logstream::get_schema_internal(dataset, context.tenant_id)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
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

async fn sample_events(arguments: &Value, context: OssToolContext<'_>) -> Result<Value, String> {
    let dataset = required_string(arguments, "dataset")?;
    authorize(context.session_key, Action::Query, Some(dataset), None)?;
    let limit = bounded_integer(arguments, "limit", 10, 1, 100);
    let minutes = bounded_integer(arguments, "minutes", 60, 1, 1440);
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
    let end = Utc::now();
    let response = query::query_internal(
        query::Query {
            query: format!(
                "SELECT {} FROM {} LIMIT {limit}",
                fields.join(", "),
                quote_identifier(dataset)
            ),
            start_time: (end - Duration::minutes(minutes as i64)).to_rfc3339(),
            end_time: end.to_rfc3339(),
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

async fn get_log_context(arguments: &Value, context: OssToolContext<'_>) -> Result<Value, String> {
    let dataset = required_string(arguments, "dataset")?;
    authorize(context.session_key, Action::Query, Some(dataset), None)?;
    let anchor = DateTime::parse_from_rfc3339(required_string(arguments, "pTimestamp")?)
        .map_err(|error| error.to_string())?;
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
    response_value(response).await
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

fn trace_datasets(arguments: &Value, context: OssToolContext<'_>) -> Vec<String> {
    arguments
        .get("datasets")
        .and_then(Value::as_array)
        .map(|datasets| {
            datasets
                .iter()
                .filter_map(Value::as_str)
                .take(10)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|datasets| !datasets.is_empty())
        .unwrap_or_else(|| {
            visible_datasets(context.tenant_id, context.session_key)
                .into_iter()
                .take(10)
                .collect()
        })
}

async fn execute_trace_tool(
    name: &str,
    arguments: &Value,
    context: OssToolContext<'_>,
) -> Result<Value, String> {
    let datasets = trace_datasets(arguments, context);
    let start_time = required_string(arguments, "startTime")?.to_owned();
    let end_time = required_string(arguments, "endTime")?.to_owned();
    let limit = bounded_integer(arguments, "limit", 100, 1, 500) as usize;
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for dataset in datasets {
        authorize(context.session_key, Action::Query, Some(&dataset), None)?;
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
                Some(context.request_headers.clone()),
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
                    options: None,
                    limit: Some(limit),
                    offset: Some(0),
                },
                context.tenant_id,
                context.session_key,
                Some(context.request_headers.clone()),
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
            Ok(
                dashboards::list_dashboards_internal(limit, None, context.tenant_id)
                    .await
                    .into(),
            )
        }
        _ => unreachable!("saved object dispatcher only receives known tools"),
    }
}

async fn execute_oss_tool_inner(
    name: &str,
    arguments: &Value,
    session_key: &SessionKey,
    tenant_id: &Option<String>,
    request_headers: &HeaderMap,
) -> Result<Value, String> {
    let context = OssToolContext {
        session_key,
        tenant_id,
        request_headers,
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
        "query_sql" | "explain_query" => execute_sql_tool(name, arguments, context).await,
        "list_alerts" | "get_alert" | "list_alert_tags" | "enable_alert" | "disable_alert"
        | "evaluate_alert" | "create_alert" => execute_alert_tool(name, arguments, context).await,
        "list_alert_targets" | "get_alert_target" | "create_alert_target" => {
            execute_alert_target_tool(name, arguments, context).await
        }
        "get_traces" | "get_trace" => execute_trace_tool(name, arguments, context).await,
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
    let Some(spec) = oss_tool_specs()
        .into_iter()
        .find(|tool| tool.name == call.name)
    else {
        return HttpResponse::NotFound().json(ToolCallResult::error("unknown tool"));
    };
    if let Err(error) = spec.validate_arguments(&call.arguments) {
        return HttpResponse::BadRequest().json(ToolCallResult::error(error));
    }
    if matches!(
        spec.effect(),
        ToolEffect::Mutation | ToolEffect::ExternalSideEffect
    ) && !call.confirmed
    {
        return HttpResponse::Conflict().json(ToolCallResult::error(
            "tool execution requires explicit confirmation",
        ));
    }
    let tenant_id = get_tenant_id_from_request(&req);
    let started = Instant::now();
    match execute_oss_tool(
        spec.name,
        &call.arguments,
        &session_key,
        &tenant_id,
        req.headers(),
    )
    .await
    {
        Ok(result) => {
            tracing::info!(
                target: "parseable::llm::tools",
                tool = spec.name,
                duration_ms = started.elapsed().as_millis(),
                "tool execution completed"
            );
            HttpResponse::Ok().json(ToolCallResult::success(result))
        }
        Err(message) => {
            tracing::warn!(
                target: "parseable::llm::tools",
                tool = spec.name,
                duration_ms = started.elapsed().as_millis(),
                error = %message,
                "tool execution failed"
            );
            HttpResponse::Ok().json(ToolCallResult::error(message))
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test, web};
    use serde_json::{Value, json};

    use super::call_tool_registry;

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

        assert_eq!(response["isError"], true);
        assert_eq!(response["content"][0]["type"], "text");
        assert_eq!(
            response["content"][0]["text"],
            "tool arguments must be a JSON object"
        );
    }
}
