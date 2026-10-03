use actix_web::HttpResponse;
use serde_json::{Value, json};

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
    ]
}

pub fn is_oss_tool(name: &str) -> bool {
    oss_tool_specs().iter().any(|tool| tool.name == name)
}

/// `GET /api/prism/v1/llm/tools`
pub async fn list_tool_registry() -> HttpResponse {
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

    HttpResponse::Ok().json(json!({ "tools": tools }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use actix_web::{App, test, web};
    use serde_json::Value;

    use super::{list_tool_registry, oss_tool_specs};

    #[actix_web::test]
    async fn oss_catalog_has_unique_names() {
        let tools = oss_tool_specs();
        let names = tools.iter().map(|tool| tool.name).collect::<HashSet<_>>();

        assert_eq!(tools.len(), 28);
        assert_eq!(tools.len(), names.len());
    }

    #[actix_web::test]
    async fn registry_api_returns_only_oss_tools() {
        let app =
            test::init_service(App::new().route("/tools", web::get().to(list_tool_registry))).await;
        let response: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/tools").to_request(),
        )
        .await;

        assert_eq!(response["tools"].as_array().unwrap().len(), 28);
        assert_eq!(response["tools"][0]["name"], "list_datasets");
        assert_eq!(response["tools"][0]["inputSchema"]["type"], "object");
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
}
