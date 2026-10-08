use actix_web::{
    HttpRequest, HttpResponse,
    http::header::{CACHE_CONTROL, ETAG, IF_NONE_MATCH},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::utils::get_hash;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallRequest {
    pub name: String,
    #[serde(default = "empty_arguments")]
    pub arguments: Value,
    #[serde(default)]
    pub confirmed: bool,
}

fn empty_arguments() -> Value {
    json!({})
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallResult {
    pub content: Vec<ToolContent>,
    pub structured_content: Value,
    pub is_error: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ToolContent {
    #[serde(rename = "type")]
    pub content_type: &'static str,
    pub text: String,
}

impl ToolCallResult {
    pub fn success(result: Value) -> Self {
        let text = match &result {
            Value::String(text) => text.clone(),
            result => serde_json::to_string(result).unwrap_or_else(|_| "null".to_owned()),
        };
        Self {
            content: vec![ToolContent {
                content_type: "text",
                text,
            }],
            structured_content: json!({ "result": result }),
            is_error: false,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            content: vec![ToolContent {
                content_type: "text",
                text: message.clone(),
            }],
            structured_content: json!({ "error": { "message": message } }),
            is_error: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolEffect {
    ReadOnly,
    ExpensiveRead,
    Mutation,
    ExternalSideEffect,
}

#[derive(Clone, Debug)]
pub struct ToolSpec {
    pub name: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

impl ToolSpec {
    fn new(
        name: &'static str,
        title: &'static str,
        description: &'static str,
        input_schema: Value,
    ) -> Self {
        Self {
            name,
            title,
            description,
            input_schema,
        }
    }

    pub fn effect(&self) -> ToolEffect {
        match self.name {
            "get_dataset_stats"
            | "sample_events"
            | "get_log_context"
            | "query_sql"
            | "query_promql"
            | "get_cluster_metrics"
            | "explain_query"
            | "discover_datasets"
            | "get_traces"
            | "get_trace" => ToolEffect::ExpensiveRead,
            "create_alert"
            | "enable_alert"
            | "disable_alert"
            | "create_alert_target"
            | "update_alert"
            | "delete_alert"
            | "update_alert_target"
            | "delete_alert_target"
            | "save_tile"
            | "create_dashboard"
            | "update_dashboard"
            | "delete_dashboard" => ToolEffect::Mutation,
            "evaluate_alert" => ToolEffect::ExternalSideEffect,
            _ => ToolEffect::ReadOnly,
        }
    }

    pub fn validate_arguments(&self, arguments: &Value) -> Result<(), String> {
        validate_tool_arguments(&self.input_schema, arguments)
    }
}

pub fn validate_tool_arguments(schema: &Value, arguments: &Value) -> Result<(), String> {
    validate_schema_value(schema, arguments, "arguments")
}

fn validate_schema_value(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    if let Some(result) = validate_any_of(schema, value, path) {
        return result;
    }

    validate_enum(schema, value, path)?;
    validate_type(schema, value, path)?;
    validate_object(schema, value, path)?;
    validate_array(schema, value, path)?;
    validate_string(schema, value, path)?;
    validate_number(schema, value, path)
}

fn validate_any_of(schema: &Value, value: &Value, path: &str) -> Option<Result<(), String>> {
    schema
        .get("anyOf")
        .and_then(Value::as_array)
        .map(|variants| {
            variants
                .iter()
                .any(|variant| validate_schema_value(variant, value, path).is_ok())
                .then_some(())
                .ok_or_else(|| format!("{path} does not match any allowed schema"))
        })
}

fn validate_enum(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        return Err(format!("{path} must be one of {allowed:?}"));
    }
    Ok(())
}

fn validate_type(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let Some(expected_type) = schema.get("type").and_then(Value::as_str) else {
        return Ok(());
    };
    let type_matches = match expected_type {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => true,
    };
    match type_matches {
        true => Ok(()),
        false => Err(format!("{path} must be a {expected_type}")),
    }
}

fn validate_object(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    let properties = schema.get("properties").and_then(Value::as_object);
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for name in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(name) {
                return Err(format!("{path}.{name} is required"));
            }
        }
    }
    if schema.get("additionalProperties").and_then(Value::as_bool) == Some(false)
        && let Some(properties) = properties
        && let Some(name) = object.keys().find(|name| !properties.contains_key(*name))
    {
        return Err(format!("{path}.{name} is not allowed"));
    }
    if let Some(properties) = properties {
        for (name, value) in object {
            if let Some(property_schema) = properties.get(name) {
                validate_schema_value(property_schema, value, &format!("{path}.{name}"))?;
            }
        }
    }
    Ok(())
}

fn validate_array(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let Some(array) = value.as_array() else {
        return Ok(());
    };
    if let Some(minimum) = schema.get("minItems").and_then(Value::as_u64)
        && array.len() < minimum as usize
    {
        return Err(format!("{path} must contain at least {minimum} items"));
    }
    if let Some(maximum) = schema.get("maxItems").and_then(Value::as_u64)
        && array.len() > maximum as usize
    {
        return Err(format!("{path} must contain at most {maximum} items"));
    }
    if let Some(item_schema) = schema.get("items") {
        for (index, item) in array.iter().enumerate() {
            validate_schema_value(item_schema, item, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn validate_string(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    if let Some(string) = value.as_str()
        && let Some(minimum) = schema.get("minLength").and_then(Value::as_u64)
        && string.chars().count() < minimum as usize
    {
        return Err(format!("{path} must contain at least {minimum} characters"));
    }
    Ok(())
}

fn validate_number(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let Some(number) = value.as_f64() else {
        return Ok(());
    };
    if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64)
        && number < minimum
    {
        return Err(format!("{path} must be at least {minimum}"));
    }
    if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64)
        && number > maximum
    {
        return Err(format!("{path} must be at most {maximum}"));
    }
    Ok(())
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn empty_schema() -> Value {
    object_schema(json!({}), &[])
}

fn string_property(description: &str) -> Value {
    json!({ "type": "string", "minLength": 1, "description": description })
}

fn dataset_schema() -> Value {
    object_schema(
        json!({ "dataset": string_property("Dataset (log stream) name.") }),
        &["dataset"],
    )
}

fn alert_schema() -> Value {
    object_schema(
        json!({ "alert_id": string_property("Alert ID. Use list_alerts to discover.") }),
        &["alert_id"],
    )
}

fn target_schema() -> Value {
    object_schema(
        json!({ "target_id": string_property("Alert target ID. Use list_alert_targets to discover.") }),
        &["target_id"],
    )
}

/// Tools implemented by both Parseable OSS and Enterprise.
///
/// Enterprise may extend this catalog, but an OSS server only advertises this
/// list so MCP clients never discover enterprise-only capabilities.
pub fn oss_tool_specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec::new(
            "list_datasets",
            "List datasets",
            "List all log datasets available on this Parseable server.",
            empty_schema(),
        ),
        ToolSpec::new(
            "discover_datasets",
            "Discover active datasets",
            "Find RBAC-visible datasets that contain logs, metrics, or traces in a resolved time range. Use this for broad telemetry analysis when the user did not name an exact dataset.",
            object_schema(
                json!({
                    "telemetryType": { "type": "string", "enum": ["logs", "metrics", "traces"] },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 50 },
                    "startTime": string_property("Resolved RFC3339 start time."),
                    "endTime": string_property("Resolved RFC3339 end time.")
                }),
                &["telemetryType", "startTime", "endTime"],
            ),
        ),
        ToolSpec::new(
            "resolve_dataset",
            "Resolve dataset",
            "Validate an exact user-named or trusted-context dataset against the caller-visible dataset list.",
            object_schema(
                json!({
                    "query": string_property("User request or dataset search text."),
                    "selectedDataset": { "type": "string" },
                    "contextDataset": { "type": "string" }
                }),
                &["query"],
            ),
        ),
        ToolSpec::new(
            "resolve_time_range",
            "Resolve time range",
            "Resolve a user time expression into an absolute RFC3339 start and end time.",
            object_schema(
                json!({
                    "expression": string_property("User time expression, such as `last hour` or `today`."),
                    "timezone": { "type": "string", "default": "UTC" }
                }),
                &["expression"],
            ),
        ),
        ToolSpec::new(
            "get_dataset_schema",
            "Get dataset schema",
            "Get columns and data types for a dataset.",
            dataset_schema(),
        ),
        ToolSpec::new(
            "get_dataset_info",
            "Get dataset info",
            "Get metadata for a dataset, including event times and storage configuration.",
            dataset_schema(),
        ),
        ToolSpec::new(
            "get_dataset_stats",
            "Get dataset stats",
            "Get event count, ingested bytes, and storage bytes for a dataset.",
            dataset_schema(),
        ),
        ToolSpec::new(
            "sample_events",
            "Sample recent events",
            "Return recent events from a dataset.",
            object_schema(
                json!({
                    "dataset": string_property("Dataset name."),
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "default": 10 },
                    "minutes": { "type": "integer", "minimum": 1, "maximum": 1440, "default": 60 },
                }),
                &["dataset"],
            ),
        ),
        ToolSpec::new(
            "get_log_context",
            "Get surrounding log context",
            "Fetch records around an exact anchor log row.",
            object_schema(
                json!({
                    "dataset": string_property("Dataset containing the anchor row."),
                    "pTimestamp": string_property("Exact p_timestamp from the anchor row."),
                    "log": { "type": "string" },
                    "body": { "type": "string" },
                    "message": { "type": "string" },
                    "contextWindow": { "type": "string" },
                    "contextStartTime": { "type": "string" },
                    "contextEndTime": { "type": "string" },
                    "conditions": { "type": "object", "additionalProperties": true },
                    "pageSize": { "type": "integer", "minimum": 1, "maximum": 500, "default": 100 },
                }),
                &["dataset", "pTimestamp"],
            ),
        ),
        ToolSpec::new(
            "query_sql",
            "Run SQL query",
            "Execute a read-only SQL query over a time window.",
            object_schema(
                json!({
                    "query": string_property("Read-only DataFusion SQL SELECT."),
                    "startTime": string_property("Start of time window."),
                    "endTime": string_property("End of time window."),
                }),
                &["query", "startTime", "endTime"],
            ),
        ),
        ToolSpec::new(
            "query_promql",
            "Run PromQL query",
            "Execute a PromQL instant or range query.",
            object_schema(
                json!({
                    "query": string_property("PromQL expression."),
                    "stream": string_property("Metrics dataset name."),
                    "start": { "type": "string" },
                    "end": { "type": "string" },
                    "step": { "type": "string" },
                    "time": { "type": "string" },
                    "timeout": { "type": "number", "minimum": 0 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 500 },
                    "timestamp_format": { "type": "string", "enum": ["rfc3339", "unix"] },
                }),
                &["query", "stream"],
            ),
        ),
        ToolSpec::new(
            "list_alerts",
            "List alerts",
            "List alerts configured on this Parseable server.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_alert",
            "Get alert",
            "Get the full configuration for one alert.",
            alert_schema(),
        ),
        ToolSpec::new(
            "list_alert_tags",
            "List alert tags",
            "List tags used across alerts.",
            empty_schema(),
        ),
        ToolSpec::new(
            "enable_alert",
            "Enable alert",
            "Enable a disabled alert.",
            alert_schema(),
        ),
        ToolSpec::new(
            "disable_alert",
            "Disable alert",
            "Disable an active alert.",
            alert_schema(),
        ),
        ToolSpec::new(
            "evaluate_alert",
            "Evaluate alert now",
            "Evaluate an alert immediately. This may trigger notifications.",
            alert_schema(),
        ),
        ToolSpec::new(
            "create_alert",
            "Create alert",
            "Create an alert from a complete Parseable alert specification.",
            object_schema(
                json!({ "spec": { "type": "object", "additionalProperties": true } }),
                &["spec"],
            ),
        ),
        ToolSpec::new(
            "list_alert_targets",
            "List alert targets",
            "List configured alert notification targets.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_alert_target",
            "Get alert target",
            "Get the configuration for one alert target.",
            target_schema(),
        ),
        ToolSpec::new(
            "create_alert_target",
            "Create alert target",
            "Create an alert notification target.",
            object_schema(
                json!({ "spec": { "type": "object", "additionalProperties": true } }),
                &["spec"],
            ),
        ),
        ToolSpec::new(
            "get_traces",
            "Get traces",
            "List bounded traces or spans from caller-visible trace datasets.",
            object_schema(
                json!({
                    "datasets": { "type": "array", "minItems": 1, "maxItems": 10, "items": { "type": "string" } },
                    "serviceName": { "type": "string" },
                    "startTime": string_property("RFC3339 range start."),
                    "endTime": string_property("RFC3339 range end."),
                    "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 100 }
                }),
                &["startTime", "endTime"],
            ),
        ),
        ToolSpec::new(
            "get_trace",
            "Get trace",
            "Get the bounded span details for one trace ID.",
            object_schema(
                json!({
                    "traceId": string_property("Trace ID."),
                    "datasets": { "type": "array", "minItems": 1, "maxItems": 10, "items": { "type": "string" } },
                    "startTime": string_property("RFC3339 range start."),
                    "endTime": string_property("RFC3339 range end.")
                }),
                &["traceId", "startTime", "endTime"],
            ),
        ),
        ToolSpec::new(
            "ping",
            "Ping Parseable server",
            "Check connectivity and server health.",
            empty_schema(),
        ),
        ToolSpec::new(
            "explain_query",
            "Explain SQL query plan",
            "Explain a read-only SQL query without executing it.",
            object_schema(
                json!({
                    "query": string_property("SQL SELECT to explain."),
                    "startTime": string_property("Start of time window."),
                    "endTime": string_property("End of time window."),
                }),
                &["query", "startTime", "endTime"],
            ),
        ),
        ToolSpec::new(
            "list_users",
            "List users",
            "List registered users.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_user",
            "Get user",
            "Get one user's profile, direct roles, group roles, and group membership.",
            object_schema(
                json!({ "userid": string_property("Exact user ID.") }),
                &["userid"],
            ),
        ),
        ToolSpec::new(
            "get_user_roles",
            "Get user roles",
            "Get roles assigned to a user.",
            object_schema(
                json!({ "userid": string_property("User ID.") }),
                &["userid"],
            ),
        ),
        ToolSpec::new(
            "list_roles",
            "List roles",
            "List roles defined on this Parseable server.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_role",
            "Get role",
            "Get the privileges assigned to a role.",
            object_schema(json!({ "name": string_property("Role name.") }), &["name"]),
        ),
        ToolSpec::new(
            "get_default_role",
            "Get default role",
            "Get the role assigned to new users by default.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_cluster_status",
            "Get cluster status",
            "List nodes and their status in a distributed deployment.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_cluster_metrics",
            "Get cluster metrics",
            "Get aggregated metrics for a distributed deployment.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_retention",
            "Get dataset retention policy",
            "Get the retention policy for a dataset.",
            dataset_schema(),
        ),
        ToolSpec::new(
            "get_hot_tier_config",
            "Get hot-tier configuration",
            "Get hot-tier configuration and utilization for a dataset.",
            dataset_schema(),
        ),
        ToolSpec::new(
            "list_filters",
            "List saved filters",
            "List saved filters visible to the caller.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_filter",
            "Get saved filter",
            "Get one saved filter by ID when visible to the caller.",
            object_schema(
                json!({ "id": string_property("Saved-filter ID.") }),
                &["id"],
            ),
        ),
        ToolSpec::new(
            "list_dashboard_tags",
            "List dashboard tags",
            "List unique dashboard tags in the caller's tenant.",
            empty_schema(),
        ),
        ToolSpec::new(
            "get_dashboard",
            "Get dashboard",
            "Retrieve one caller-visible dashboard with its tiles.",
            object_schema(
                json!({ "dashboardId": string_property("Dashboard ID.") }),
                &["dashboardId"],
            ),
        ),
        ToolSpec::new(
            "list_dashboards",
            "List dashboards",
            "List caller-visible dashboards with compact metadata.",
            object_schema(
                json!({
                    "query": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 100 }
                }),
                &[],
            ),
        ),
        ToolSpec::new(
            "update_alert",
            "Update alert",
            "Update an alert using an explicit validated patch or complete replacement after confirmation.",
            object_schema(
                json!({
                    "alertId": string_property("Alert ID to update."),
                    "patch": { "type": "object", "additionalProperties": true, "description": "Explicit camelCase alert patch." },
                    "replacement": { "type": "object", "additionalProperties": true, "description": "Optional complete validated alert replacement." },
                    "idempotencyKey": string_property("Server-scoped idempotency key.")
                }),
                &["alertId", "idempotencyKey"],
            ),
        ),
        ToolSpec::new(
            "delete_alert",
            "Delete alert",
            "Delete one alert after explicit confirmation.",
            object_schema(
                json!({
                    "alertId": string_property("Alert ID to delete."),
                    "idempotencyKey": string_property("Server-scoped idempotency key.")
                }),
                &["alertId", "idempotencyKey"],
            ),
        ),
        ToolSpec::new(
            "update_alert_target",
            "Update alert target",
            "Update an alert target using an explicit patch or replacement after confirmation.",
            object_schema(
                json!({
                    "targetId": string_property("Alert-target ID."),
                    "patch": { "type": "object", "additionalProperties": true },
                    "replacement": { "type": "object", "additionalProperties": true },
                    "idempotencyKey": string_property("Server-scoped idempotency key.")
                }),
                &["targetId", "idempotencyKey"],
            ),
        ),
        ToolSpec::new(
            "delete_alert_target",
            "Delete alert target",
            "Delete one alert target after explicit confirmation.",
            object_schema(
                json!({
                    "targetId": string_property("Alert-target ID."),
                    "idempotencyKey": string_property("Server-scoped idempotency key.")
                }),
                &["targetId", "idempotencyKey"],
            ),
        ),
        ToolSpec::new(
            "save_tile",
            "Save tile",
            "Persist a validated tile in an existing dashboard after confirmation.",
            object_schema(
                json!({
                    "dashboardId": string_property("Dashboard ID."),
                    "tile": { "type": "object", "additionalProperties": true },
                    "idempotencyKey": string_property("Server-scoped idempotency key.")
                }),
                &["dashboardId", "tile", "idempotencyKey"],
            ),
        ),
        ToolSpec::new(
            "create_dashboard",
            "Create dashboard",
            "Persist a validated dashboard after confirmation.",
            object_schema(
                json!({
                    "dashboard": { "type": "object", "additionalProperties": true },
                    "idempotencyKey": string_property("Server-scoped idempotency key.")
                }),
                &["dashboard", "idempotencyKey"],
            ),
        ),
        ToolSpec::new(
            "update_dashboard",
            "Update dashboard",
            "Update a dashboard using an explicit patch or validated replacement after confirmation.",
            object_schema(
                json!({
                    "dashboardId": string_property("Dashboard ID."),
                    "patch": { "type": "object", "additionalProperties": true },
                    "replacement": { "type": "object", "additionalProperties": true },
                    "idempotencyKey": string_property("Server-scoped idempotency key.")
                }),
                &["dashboardId", "idempotencyKey"],
            ),
        ),
        ToolSpec::new(
            "delete_dashboard",
            "Delete dashboard",
            "Delete one dashboard after explicit confirmation.",
            object_schema(
                json!({
                    "dashboardId": string_property("Dashboard ID."),
                    "idempotencyKey": string_property("Server-scoped idempotency key.")
                }),
                &["dashboardId", "idempotencyKey"],
            ),
        ),
    ]
}

pub fn is_oss_tool(name: &str) -> bool {
    oss_tool_specs().iter().any(|tool| tool.name == name)
}

pub fn registry_http_response(request: &HttpRequest, tools: Vec<Value>) -> HttpResponse {
    let payload = json!({ "tools": tools });
    let body = serde_json::to_string(&payload).expect("tool registry is JSON serializable");
    let etag = format!("\"{}\"", get_hash(&body));
    let cache_control = "private, max-age=300, must-revalidate";
    if request
        .headers()
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        == Some(etag.as_str())
    {
        return HttpResponse::NotModified()
            .insert_header((ETAG, etag))
            .insert_header((CACHE_CONTROL, cache_control))
            .finish();
    }

    HttpResponse::Ok()
        .insert_header((ETAG, etag))
        .insert_header((CACHE_CONTROL, cache_control))
        .content_type("application/json")
        .body(body)
}

/// `GET /api/prism/v1/llm/tools/list`
pub async fn list_tool_registry(request: HttpRequest) -> HttpResponse {
    let tools = oss_tool_specs()
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "title": tool.title,
                "description": tool.description,
                "inputSchema": tool.input_schema,
            })
        })
        .collect::<Vec<_>>();

    registry_http_response(&request, tools)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use actix_web::{App, http::header, test, web};
    use serde_json::Value;

    use super::{ToolCallRequest, ToolCallResult, list_tool_registry, oss_tool_specs};

    #[actix_web::test]
    async fn oss_catalog_has_unique_names() {
        let tools = oss_tool_specs();
        let names = tools.iter().map(|tool| tool.name).collect::<HashSet<_>>();

        assert_eq!(tools.len(), 48);
        assert_eq!(tools.len(), names.len());
    }

    #[actix_web::test]
    async fn registry_api_returns_only_oss_tools() {
        let app =
            test::init_service(App::new().route("/tools/list", web::get().to(list_tool_registry)))
                .await;
        let response: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/tools/list").to_request(),
        )
        .await;

        assert_eq!(response["tools"].as_array().unwrap().len(), 48);
        assert_eq!(response["tools"][0]["name"], "list_datasets");
        assert_eq!(response["tools"][0]["inputSchema"]["type"], "object");
    }

    #[actix_web::test]
    async fn registry_api_supports_etag_revalidation() {
        let app =
            test::init_service(App::new().route("/tools/list", web::get().to(list_tool_registry)))
                .await;
        let first = test::call_service(
            &app,
            test::TestRequest::get().uri("/tools/list").to_request(),
        )
        .await;
        let etag = first.headers().get(header::ETAG).unwrap().clone();
        assert_eq!(
            first.headers().get(header::CACHE_CONTROL).unwrap(),
            "private, max-age=300, must-revalidate"
        );

        let second = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/tools/list")
                .insert_header((header::IF_NONE_MATCH, etag))
                .to_request(),
        )
        .await;
        assert_eq!(second.status(), actix_web::http::StatusCode::NOT_MODIFIED);
    }

    #[actix_web::test]
    async fn promql_execution_limits_use_numeric_schema_types() {
        let tools = oss_tool_specs();
        let promql = tools
            .iter()
            .find(|tool| tool.name == "query_promql")
            .unwrap();
        let properties = &promql.input_schema["properties"];

        assert_eq!(properties["timeout"]["type"], "number");
        assert_eq!(properties["limit"]["type"], "integer");
        assert_eq!(properties["limit"]["maximum"], 500);
    }

    #[actix_web::test]
    async fn tool_call_defaults_to_empty_arguments_without_confirmation() {
        let call: ToolCallRequest = serde_json::from_value(serde_json::json!({
            "name": "list_datasets"
        }))
        .unwrap();

        assert_eq!(call.arguments, serde_json::json!({}));
        assert!(!call.confirmed);
    }

    #[actix_web::test]
    async fn tool_schema_validation_rejects_missing_and_extra_arguments() {
        let schema_tool = oss_tool_specs()
            .into_iter()
            .find(|tool| tool.name == "get_dataset_schema")
            .unwrap();

        assert!(
            schema_tool
                .validate_arguments(&serde_json::json!({}))
                .is_err()
        );
        assert!(
            schema_tool
                .validate_arguments(&serde_json::json!({
                    "dataset": "logs",
                    "unexpected": true
                }))
                .is_err()
        );
        assert!(
            schema_tool
                .validate_arguments(&serde_json::json!({ "dataset": "logs" }))
                .is_ok()
        );
    }

    #[actix_web::test]
    async fn tool_results_use_mcp_call_tool_result_shape() {
        let result = serde_json::to_value(ToolCallResult::success(serde_json::json!({
            "rows": []
        })))
        .unwrap();

        assert_eq!(result["content"][0]["type"], "text");
        assert_eq!(
            result["structuredContent"]["result"]["rows"],
            serde_json::json!([])
        );
        assert!(
            !result["isError"]
                .as_bool()
                .expect("isError must be a boolean")
        );
    }
}
