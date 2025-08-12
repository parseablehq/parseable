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
use arrow_schema::{ArrowError, DataType, Schema};
use async_trait::async_trait;
use chrono::Utc;
use datafusion::logical_expr::{LogicalPlan, Projection};
use datafusion::prelude::Expr;
use datafusion::sql::sqlparser::parser::ParserError;
use derive_more::FromStrError;
use http::StatusCode;
use serde_json::{Error as SerdeError, Value as JsonValue};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::thread;
// use std::time::Duration;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, trace, warn};
use ulid::Ulid;

pub mod alert_enums;
pub mod alert_structs;
pub mod alert_traits;
pub mod alert_types;
pub mod alerts_utils;
pub mod target;

pub use crate::alerts::alert_enums::{
    AggregateFunction, AlertOperator, AlertState, AlertTask, AlertType, AlertVersion, EvalConfig,
    LogicalOperator, NotificationState, Severity, WhereConfigOperator,
};
pub use crate::alerts::alert_structs::{
    AlertConfig, AlertInfo, AlertRequest, Alerts, AlertsInfo, AlertsInfoByState, AlertsSummary,
    BasicAlertFields, Context, DeploymentInfo, RollingWindow, ThresholdConfig,
};
use crate::alerts::alert_traits::{AlertManagerTrait, AlertTrait};
use crate::alerts::alert_types::ThresholdAlert;
use crate::alerts::target::{NotificationConfig, TARGETS};
use crate::handlers::http::fetch_schema;
// use crate::handlers::http::query::create_streams_for_distributed;
// use crate::option::Mode;
use crate::parseable::{PARSEABLE, StreamNotFound};
use crate::query::{QUERY_SESSION, resolve_stream_names};
use crate::rbac::map::SessionKey;
use crate::storage;
use crate::storage::{ALERTS_ROOT_DIRECTORY, ObjectStorageError};
use crate::sync::alert_runtime;
use crate::utils::user_auth_for_query;

// these types describe the scheduled task for an alert
pub type ScheduledTaskHandlers = (JoinHandle<()>, Receiver<()>, Sender<()>);

pub const CURRENT_ALERTS_VERSION: &str = "v2";

pub static ALERTS: RwLock<Option<Arc<dyn AlertManagerTrait>>> = RwLock::const_new(None);

pub async fn get_alert_manager() -> Arc<dyn AlertManagerTrait> {
    let guard = ALERTS.read().await;
    if let Some(manager) = guard.as_ref() {
        manager.clone()
    } else {
        drop(guard);
        let mut write_guard = ALERTS.write().await;
        if write_guard.is_none() {
            *write_guard = Some(Arc::new(create_default_alerts_manager()));
        }
        write_guard.as_ref().unwrap().clone()
    }
}

pub async fn set_alert_manager(manager: Arc<dyn AlertManagerTrait>) {
    *ALERTS.write().await = Some(manager);
}

pub fn create_default_alerts_manager() -> Alerts {
    let (tx, rx) = mpsc::channel::<AlertTask>(10);
    let alerts = Alerts {
        alerts: RwLock::new(HashMap::new()),
        sender: tx,
    };
    thread::spawn(|| alert_runtime(rx));
    alerts
}

impl AlertConfig {
    /// Migration function to convert v1 alerts to v2 structure
    pub async fn migrate_from_v1(
        alert_json: &JsonValue,
        store: &dyn crate::storage::ObjectStorage,
    ) -> Result<AlertConfig, AlertError> {
        let basic_fields = Self::parse_basic_fields(alert_json)?;
        let alert_info = format!("Alert '{}' (ID: {})", basic_fields.title, basic_fields.id);

        let query = Self::build_query_from_v1(alert_json, &alert_info).await?;
        let datasets = resolve_stream_names(&query)?;
        let threshold_config = Self::extract_threshold_config(alert_json, &alert_info)?;
        let eval_config = Self::extract_eval_config(alert_json, &alert_info)?;
        let targets = Self::extract_targets(alert_json, &alert_info)?;
        let state = Self::extract_state(alert_json);

        // Create the migrated v2 alert
        let migrated_alert = AlertConfig {
            version: AlertVersion::V2,
            id: basic_fields.id,
            severity: basic_fields.severity,
            title: basic_fields.title,
            query,
            datasets,
            alert_type: AlertType::Threshold,
            threshold_config,
            eval_config,
            targets,
            state,
            notification_state: NotificationState::Notify,
            notification_config: NotificationConfig::default(),
            created: Utc::now(),
            tags: None,
        };

        // Save the migrated alert back to storage
        store.put_alert(basic_fields.id, &migrated_alert).await?;

        Ok(migrated_alert)
    }

    /// Parse basic fields common between v1 and v2 alerts
    fn parse_basic_fields(alert_json: &JsonValue) -> Result<BasicAlertFields, AlertError> {
        let id: Ulid = alert_json["id"]
            .as_str()
            .ok_or_else(|| AlertError::CustomError("Missing id in v1 alert".to_string()))?
            .parse()
            .map_err(|_| AlertError::CustomError("Invalid id format in v1 alert".to_string()))?;

        let title = alert_json["title"]
            .as_str()
            .ok_or_else(|| {
                AlertError::CustomError(format!("Missing title in v1 alert (ID: {id})"))
            })?
            .to_string();

        let severity_str = alert_json["severity"].as_str().ok_or_else(|| {
            AlertError::CustomError(format!("Missing severity in v1 alert '{title}' (ID: {id})"))
        })?;

        let severity = match severity_str.to_lowercase().as_str() {
            "critical" => Severity::Critical,
            "high" => Severity::High,
            "medium" => Severity::Medium,
            "low" => Severity::Low,
            _ => Severity::Medium, // default
        };

        Ok(BasicAlertFields {
            id,
            title,
            severity,
        })
    }

    /// Build SQL query from v1 alert structure
    async fn build_query_from_v1(
        alert_json: &JsonValue,
        alert_info: &str,
    ) -> Result<String, AlertError> {
        let stream = alert_json["stream"].as_str().ok_or_else(|| {
            AlertError::CustomError(format!("Missing stream in v1 alert for {alert_info}"))
        })?;

        let aggregates = &alert_json["aggregates"];
        let aggregate_config = &aggregates["aggregateConfig"][0];

        let aggregate_function = Self::parse_aggregate_function(aggregate_config, alert_info)?;
        let base_query =
            Self::build_base_query(&aggregate_function, aggregate_config, stream, alert_info)?;
        let final_query =
            Self::add_where_conditions(base_query, aggregate_config, stream, alert_info).await?;

        Ok(final_query)
    }

    /// Parse aggregate function from v1 config
    fn parse_aggregate_function(
        aggregate_config: &JsonValue,
        alert_info: &str,
    ) -> Result<AggregateFunction, AlertError> {
        let aggregate_function_str =
            aggregate_config["aggregateFunction"]
                .as_str()
                .ok_or_else(|| {
                    AlertError::CustomError(format!(
                        "Missing aggregateFunction in v1 alert for {alert_info}"
                    ))
                })?;

        match aggregate_function_str.to_lowercase().as_str() {
            "avg" => Ok(AggregateFunction::Avg),
            "count" => Ok(AggregateFunction::Count),
            "countdistinct" => Ok(AggregateFunction::CountDistinct),
            "min" => Ok(AggregateFunction::Min),
            "max" => Ok(AggregateFunction::Max),
            "sum" => Ok(AggregateFunction::Sum),
            _ => Err(AlertError::CustomError(format!(
                "Unsupported aggregate function: {aggregate_function_str} for {alert_info}"
            ))),
        }
    }

    /// Build base SQL query without WHERE conditions
    fn build_base_query(
        aggregate_function: &AggregateFunction,
        aggregate_config: &JsonValue,
        stream: &str,
        _alert_info: &str,
    ) -> Result<String, AlertError> {
        let column = aggregate_config["column"].as_str().unwrap_or("*");

        let query = match aggregate_function {
            AggregateFunction::CountDistinct => {
                if column == "*" {
                    format!("SELECT COUNT(*) as alert_value FROM \"{stream}\"")
                } else {
                    format!("SELECT COUNT(DISTINCT \"{column}\") as alert_value FROM \"{stream}\"")
                }
            }
            _ => {
                if column == "*" {
                    format!(
                        "SELECT {}(*) as alert_value FROM \"{stream}\"",
                        aggregate_function.to_string().to_uppercase()
                    )
                } else if matches!(aggregate_function, AggregateFunction::Count) && column != "*" {
                    // COUNT with specific column should handle NULLs differently
                    format!("SELECT COUNT(\"{column}\") as alert_value FROM \"{stream}\"")
                } else {
                    format!(
                        "SELECT {}(\"{column}\") as alert_value FROM \"{stream}\"",
                        aggregate_function.to_string().to_uppercase()
                    )
                }
            }
        };
        Ok(query)
    }

    /// Add WHERE conditions to the base query with data type conversion
    async fn add_where_conditions(
        base_query: String,
        aggregate_config: &JsonValue,
        stream: &str,
        alert_info: &str,
    ) -> Result<String, AlertError> {
        let Some(conditions) = aggregate_config["conditions"].as_object() else {
            return Ok(base_query);
        };

        let Some(condition_config) = conditions["conditionConfig"].as_array() else {
            return Ok(base_query);
        };

        if condition_config.is_empty() {
            return Ok(base_query);
        }

        // Fetch the stream schema for data type conversion
        let schema = match fetch_schema(stream).await {
            Ok(schema) => schema,
            Err(e) => {
                return Err(AlertError::CustomError(format!(
                    "Failed to fetch schema for stream '{stream}' during migration of {alert_info}: {e}. Migration cannot proceed without schema information."
                )));
            }
        };

        let mut where_clauses = Vec::new();
        for condition in condition_config {
            let column = condition["column"].as_str().unwrap_or("");
            if column.is_empty() {
                warn!("Skipping WHERE condition with empty column name for {alert_info}");
                continue;
            }
            let operator_str = condition["operator"].as_str().unwrap_or("=");
            let value = condition["value"].as_str().unwrap_or("");

            let operator = Self::parse_where_operator(operator_str);
            let where_clause = Self::format_where_clause_with_types(
                column, &operator, value, &schema, alert_info,
            )?;
            where_clauses.push(where_clause);
        }

        let logical_op = conditions["operator"].as_str().unwrap_or("and");
        let where_clause = where_clauses.join(&format!(" {} ", logical_op.to_uppercase()));

        Ok(format!("{base_query} WHERE {where_clause}"))
    }

    /// Parse WHERE operator from string
    fn parse_where_operator(operator_str: &str) -> WhereConfigOperator {
        match operator_str {
            "=" => WhereConfigOperator::Equal,
            "!=" => WhereConfigOperator::NotEqual,
            "<" => WhereConfigOperator::LessThan,
            ">" => WhereConfigOperator::GreaterThan,
            "<=" => WhereConfigOperator::LessThanOrEqual,
            ">=" => WhereConfigOperator::GreaterThanOrEqual,
            "is null" => WhereConfigOperator::IsNull,
            "is not null" => WhereConfigOperator::IsNotNull,
            "ilike" => WhereConfigOperator::ILike,
            "contains" => WhereConfigOperator::Contains,
            "begins with" => WhereConfigOperator::BeginsWith,
            "ends with" => WhereConfigOperator::EndsWith,
            "does not contain" => WhereConfigOperator::DoesNotContain,
            "does not begin with" => WhereConfigOperator::DoesNotBeginWith,
            "does not end with" => WhereConfigOperator::DoesNotEndWith,
            _ => WhereConfigOperator::Equal, // default fallback
        }
    }

    /// Format a single WHERE clause with proper data type conversion
    fn format_where_clause_with_types(
        column: &str,
        operator: &WhereConfigOperator,
        value: &str,
        schema: &Schema,
        alert_info: &str,
    ) -> Result<String, AlertError> {
        match operator {
            WhereConfigOperator::IsNull | WhereConfigOperator::IsNotNull => {
                Ok(format!("\"{column}\" {}", operator.as_str()))
            }
            WhereConfigOperator::Contains => Ok(format!(
                "\"{column}\" LIKE '%{}%'",
                value.replace('\'', "''")
            )),
            WhereConfigOperator::BeginsWith => Ok(format!(
                "\"{column}\" LIKE '{}%'",
                value.replace('\'', "''")
            )),
            WhereConfigOperator::EndsWith => Ok(format!(
                "\"{column}\" LIKE '%{}'",
                value.replace('\'', "''")
            )),
            WhereConfigOperator::DoesNotContain => Ok(format!(
                "\"{column}\" NOT LIKE '%{}%'",
                value.replace('\'', "''")
            )),
            WhereConfigOperator::DoesNotBeginWith => Ok(format!(
                "\"{column}\" NOT LIKE '{}%'",
                value.replace('\'', "''")
            )),
            WhereConfigOperator::DoesNotEndWith => Ok(format!(
                "\"{column}\" NOT LIKE '%{}'",
                value.replace('\'', "''")
            )),
            WhereConfigOperator::ILike => Ok(format!(
                "\"{column}\" ILIKE '{}'",
                value.replace('\'', "''")
            )),
            _ => {
                // Standard operators: =, !=, <, >, <=, >=
                let formatted_value =
                    Self::convert_value_by_data_type(column, value, schema, alert_info)?;
                Ok(format!(
                    "\"{column}\" {} {formatted_value}",
                    operator.as_str()
                ))
            }
        }
    }

    /// Convert string value to appropriate data type based on schema
    fn convert_value_by_data_type(
        column: &str,
        value: &str,
        schema: &Schema,
        alert_info: &str,
    ) -> Result<String, AlertError> {
        // Find the field in the schema
        let field = schema.fields().iter().find(|f| f.name() == column);
        let Some(field) = field else {
            // Column not found in schema, fail migration
            return Err(AlertError::CustomError(format!(
                "Column '{column}' not found in stream schema during migration of {alert_info}. Available columns: [{}]",
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        };

        match field.data_type() {
            DataType::Float64 => {
                match value.parse::<f64>() {
                    Ok(float_val) => Ok(float_val.to_string()), // Raw number without quotes
                    Err(_) => Err(AlertError::CustomError(format!(
                        "Failed to parse value '{value}' as float64 for column '{column}' during migration of {alert_info}",
                    ))),
                }
            }
            DataType::Int64 => {
                match value.parse::<i64>() {
                    Ok(int_val) => Ok(int_val.to_string()), // Raw number without quotes
                    Err(_) => Err(AlertError::CustomError(format!(
                        "Failed to parse value '{value}' as int64 for column '{column}' during migration of {alert_info}",
                    ))),
                }
            }
            DataType::Boolean => {
                match value.to_lowercase().parse::<bool>() {
                    Ok(bool_val) => Ok(bool_val.to_string()), // Raw boolean without quotes
                    Err(_) => Err(AlertError::CustomError(format!(
                        "Failed to parse value '{value}' as boolean for column '{column}' during migration of {alert_info}",
                    ))),
                }
            }
            DataType::Date32 | DataType::Date64 => {
                // For date types, try to validate the format but keep as quoted string in SQL
                match chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                    Ok(_) => Ok(format!("'{}'", value.replace('\'', "''"))),
                    Err(_) => {
                        // Try ISO format
                        match value.parse::<chrono::DateTime<chrono::Utc>>() {
                            Ok(_) => Ok(format!("'{}'", value.replace('\'', "''"))),
                            Err(_) => Err(AlertError::CustomError(format!(
                                "Failed to parse value '{value}' as date for column '{column}' during migration of {alert_info}",
                            ))),
                        }
                    }
                }
            }
            DataType::Timestamp(..) => {
                // For timestamp types, try to validate but keep as quoted string in SQL
                match value.parse::<chrono::DateTime<chrono::Utc>>() {
                    Ok(_) => Ok(format!("'{}'", value.replace('\'', "''"))),
                    Err(_) => Err(AlertError::CustomError(format!(
                        "Failed to parse value '{value}' as timestamp for column '{column}' during migration of {alert_info}",
                    ))),
                }
            }
            _ => {
                // For all other data types (string, binary, etc.), use string with quotes
                Ok(format!("'{}'", value.replace('\'', "''")))
            }
        }
    }

    /// Extract threshold configuration from v1 alert
    fn extract_threshold_config(
        alert_json: &JsonValue,
        alert_info: &str,
    ) -> Result<ThresholdConfig, AlertError> {
        let aggregates = &alert_json["aggregates"];
        let aggregate_config = &aggregates["aggregateConfig"][0];

        let threshold_operator = aggregate_config["operator"].as_str().ok_or_else(|| {
            AlertError::CustomError(format!("Missing operator in v1 alert for {alert_info}"))
        })?;

        let threshold_value = aggregate_config["value"].as_f64().ok_or_else(|| {
            AlertError::CustomError(format!("Missing value in v1 alert for {alert_info}"))
        })?;

        let operator = match threshold_operator {
            ">" => AlertOperator::GreaterThan,
            "<" => AlertOperator::LessThan,
            "=" => AlertOperator::Equal,
            "!=" => AlertOperator::NotEqual,
            ">=" => AlertOperator::GreaterThanOrEqual,
            "<=" => AlertOperator::LessThanOrEqual,
            _ => AlertOperator::GreaterThan, // default
        };

        Ok(ThresholdConfig {
            operator,
            value: threshold_value,
        })
    }

    /// Extract evaluation configuration from v1 alert
    fn extract_eval_config(
        alert_json: &JsonValue,
        alert_info: &str,
    ) -> Result<EvalConfig, AlertError> {
        let rolling_window = &alert_json["evalConfig"]["rollingWindow"];

        let eval_start = rolling_window["evalStart"]
            .as_str()
            .ok_or_else(|| {
                AlertError::CustomError(format!("Missing evalStart in v1 alert for {alert_info}"))
            })?
            .to_string();

        let eval_end = rolling_window["evalEnd"]
            .as_str()
            .ok_or_else(|| {
                AlertError::CustomError(format!("Missing evalEnd in v1 alert for {alert_info}"))
            })?
            .to_string();

        let eval_frequency = rolling_window["evalFrequency"].as_u64().ok_or_else(|| {
            AlertError::CustomError(format!(
                "Missing evalFrequency in v1 alert for {alert_info}"
            ))
        })?;

        Ok(EvalConfig::RollingWindow(RollingWindow {
            eval_start,
            eval_end,
            eval_frequency,
        }))
    }

    /// Extract target IDs from v1 alert
    fn extract_targets(alert_json: &JsonValue, alert_info: &str) -> Result<Vec<Ulid>, AlertError> {
        let targets: Result<Vec<Ulid>, _> = alert_json["targets"]
            .as_array()
            .ok_or_else(|| {
                AlertError::CustomError(format!("Missing targets in v1 alert for {alert_info}"))
            })?
            .iter()
            .map(|t| {
                t.as_str()
                    .ok_or_else(|| {
                        AlertError::CustomError(format!("Invalid target format for {alert_info}"))
                    })?
                    .parse()
                    .map_err(|_| {
                        AlertError::CustomError(format!(
                            "Invalid target ID format for {alert_info}"
                        ))
                    })
            })
            .collect();

        targets
    }

    /// Extract alert state from v1 alert
    fn extract_state(alert_json: &JsonValue) -> AlertState {
        let state_str = alert_json["state"].as_str().unwrap_or("resolved");
        match state_str.to_lowercase().as_str() {
            "triggered" => AlertState::Triggered,
            // "silenced" => AlertState::Silenced,
            "resolved" => AlertState::NotTriggered,
            _ => AlertState::NotTriggered,
        }
    }

    pub fn get_eval_frequency(&self) -> u64 {
        match &self.eval_config {
            EvalConfig::RollingWindow(rolling_window) => rolling_window.eval_frequency,
        }
    }
    pub fn get_eval_window(&self) -> String {
        match &self.eval_config {
            EvalConfig::RollingWindow(rolling_window) => format!(
                "Start={}\tEnd={}",
                rolling_window.eval_start, rolling_window.eval_end
            ),
        }
    }

    fn get_context(&self) -> Context {
        let deployment_instance = format!(
            "{}://{}",
            PARSEABLE.options.get_scheme(),
            PARSEABLE.options.address
        );
        let deployment_id = storage::StorageMetadata::global().deployment_id;
        let deployment_mode = storage::StorageMetadata::global().mode.to_string();

        Context::new(
            AlertInfo::new(
                self.id,
                self.title.clone(),
                self.state,
                alert_enums::NotificationState::Notify,
                self.severity.clone().to_string(),
            ),
            DeploymentInfo::new(deployment_instance, deployment_id, deployment_mode),
            self.notification_config.clone(),
            String::default(),
        )
    }

    pub async fn trigger_notifications(&self, message: String) -> Result<(), AlertError> {
        let mut context = self.get_context();
        context.message = message;
        for target_id in &self.targets {
            let target = TARGETS.get_target_by_id(target_id).await?;
            trace!("Target (trigger_notifications)-\n{target:?}");
            target.call(context.clone());
        }
        Ok(())
    }

    /// create a summary of the dashboard
    /// used for listing dashboards
    pub fn to_summary(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::new();

        map.insert(
            "title".to_string(),
            serde_json::Value::String(self.title.clone()),
        );

        map.insert(
            "created".to_string(),
            serde_json::Value::String(self.created.to_string()),
        );

        map.insert(
            "alertType".to_string(),
            serde_json::Value::String(self.alert_type.to_string()),
        );

        map.insert(
            "notificationState".to_string(),
            serde_json::Value::String(self.notification_state.to_string()),
        );

        map.insert(
            "id".to_string(),
            serde_json::Value::String(self.id.to_string()),
        );

        map.insert(
            "severity".to_string(),
            serde_json::Value::String(self.severity.to_string()),
        );

        map.insert(
            "state".to_string(),
            serde_json::Value::String(self.state.to_string()),
        );

        if let Some(tags) = &self.tags {
            map.insert(
                "tags".to_string(),
                serde_json::Value::Array(
                    tags.iter()
                        .map(|tag| serde_json::Value::String(tag.clone()))
                        .collect(),
                ),
            );
        }

        map.insert(
            "datasets".to_string(),
            serde_json::Value::Array(
                self.datasets
                    .iter()
                    .map(|dataset| serde_json::Value::String(dataset.clone()))
                    .collect(),
            ),
        );

        map
    }
}

/// Check if a query is an aggregate query that returns a single value without executing it
pub async fn get_number_of_agg_exprs(query: &str) -> Result<usize, AlertError> {
    let session_state = QUERY_SESSION.state();

    // Parse the query into a logical plan
    let logical_plan = session_state
        .create_logical_plan(query)
        .await
        .map_err(|err| AlertError::CustomError(format!("Failed to parse query: {err}")))?;

    // Check if the plan structure indicates an aggregate query
    _get_number_of_agg_exprs(&logical_plan)
}

/// Extract the projection which deals with aggregation
pub async fn get_aggregate_projection(query: &str) -> Result<String, AlertError> {
    let session_state = QUERY_SESSION.state();

    // Parse the query into a logical plan
    let logical_plan = session_state
        .create_logical_plan(query)
        .await
        .map_err(|err| AlertError::CustomError(format!("Failed to parse query: {err}")))?;

    // Check if the plan structure indicates an aggregate query
    _get_aggregate_projection(&logical_plan)
}

fn _get_aggregate_projection(plan: &LogicalPlan) -> Result<String, AlertError> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            // let fields = exprlist_to_fields(&agg.aggr_expr, &agg.input)?;
            match &agg.aggr_expr[0] {
                datafusion::prelude::Expr::Alias(alias) => Ok(alias.name.clone()),
                _ => Ok(agg.aggr_expr[0].name_for_alias()?),
            }
        }
        // Projection over aggregate: SELECT COUNT(*) as total, SELECT AVG(col) as average
        LogicalPlan::Projection(Projection { input, .. }) => _get_aggregate_projection(input),
        // Do not consider any aggregates inside a subquery or recursive CTEs
        LogicalPlan::Subquery(_) | LogicalPlan::RecursiveQuery(_) => {
            Err(AlertError::InvalidAlertQuery("Subquery not allowed".into()))
        }
        // Recursively check wrapped plans (Filter, Limit, Sort, etc.)
        _ => {
            // Use inputs() method to get all input plans and recursively search
            for input in plan.inputs() {
                if let Ok(result) = _get_aggregate_projection(input) {
                    return Ok(result);
                }
            }
            Err(AlertError::InvalidAlertQuery(
                "No aggregate projection found".into(),
            ))
        }
    }
}

/// Extracts aliases for aggregate functions from a DataFusion logical plan
pub fn extract_aggregate_aliases(plan: &LogicalPlan) -> Vec<(String, Option<String>)> {
    let mut aliases = Vec::new();

    if let LogicalPlan::Projection(projection) = plan {
        // Check if this projection contains aliased aggregates
        for expr in &projection.expr {
            if let Some((agg_name, alias)) = extract_alias_from_expr(expr) {
                aliases.push((agg_name, alias));
            }
        }
    }

    // Recursively check child plans
    for input in plan.inputs() {
        aliases.extend(extract_aggregate_aliases(input));
    }

    aliases
}

/// Extracts aggregate function name and alias from an expression
fn extract_alias_from_expr(expr: &Expr) -> Option<(String, Option<String>)> {
    match expr {
        Expr::Alias(alias_expr) => {
            // This is an aliased expression
            let alias_name = alias_expr.name.clone();

            match alias_expr.expr.as_ref() {
                Expr::AggregateFunction(agg_func) => {
                    let agg_name = format!("{:?}", agg_func.func);
                    Some((agg_name, Some(alias_name)))
                }
                // Handle other aggregate expressions like Count, etc.
                _ => {
                    // Check if the inner expression is an aggregate
                    let expr_str = format!("{:?}", alias_expr.expr);
                    if expr_str.contains("count")
                        || expr_str.contains("sum")
                        || expr_str.contains("avg")
                        || expr_str.contains("min")
                        || expr_str.contains("max")
                    {
                        Some((expr_str, Some(alias_name)))
                    } else {
                        None
                    }
                }
            }
        }
        Expr::AggregateFunction(agg_func) => {
            // Unaliased aggregate function
            let agg_name = format!("{:?}", agg_func.func);
            Some((agg_name, None))
        }
        Expr::Column(column_expr) => {
            // This might be an un-aliased aggregate expression
            if column_expr.name().contains("count")
                || column_expr.name().contains("sum")
                || column_expr.name().contains("avg")
                || column_expr.name().contains("min")
                || column_expr.name().contains("max")
            {
                Some((column_expr.name.clone(), None))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Analyze a logical plan to determine if it represents an aggregate query
///
/// Returns the number of aggregate expressions found in the plan
fn _get_number_of_agg_exprs(plan: &LogicalPlan) -> Result<usize, AlertError> {
    match plan {
        // Direct aggregate: SELECT COUNT(*), AVG(col), etc.
        LogicalPlan::Aggregate(agg) => Ok(agg.aggr_expr.len()),

        // Projection over aggregate: SELECT COUNT(*) as total, SELECT AVG(col) as average
        LogicalPlan::Projection(Projection { input, .. }) => _get_number_of_agg_exprs(input),

        // Do not consider any aggregates inside a subquery or recursive CTEs
        LogicalPlan::Subquery(_) | LogicalPlan::RecursiveQuery(_) => {
            Err(AlertError::InvalidAlertQuery("Subquery not allowed".into()))
        }

        // Recursively check wrapped plans (Filter, Limit, Sort, etc.)
        _ => {
            // Use inputs() method to get all input plans
            plan.inputs()
                .iter()
                .map(|input| _get_number_of_agg_exprs(input))
                .sum()
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AlertError {
    #[error("Storage Error: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Serde Error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Cannot perform this operation: {0}")]
    Metadata(&'static str),
    #[error("User is not authorized to run this query")]
    Unauthorized,
    #[error("ActixError: {0}")]
    Error(#[from] actix_web::Error),
    #[error("DataFusion Error: {0}")]
    DatafusionError(#[from] datafusion::error::DataFusionError),
    #[error("Error: {0}")]
    CustomError(String),
    #[error("Invalid State Change: {0}")]
    InvalidStateChange(String),
    #[error("{0}")]
    StreamNotFound(#[from] StreamNotFound),
    #[error("{0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("No alert request body provided")]
    InvalidAlertModifyRequest,
    #[error("{0}")]
    FromStrError(#[from] FromStrError),
    #[error("Invalid Target ID- {0}")]
    InvalidTargetID(String),
    #[error("Invalid target modification request: {0}")]
    InvalidTargetModification(String),
    #[error("Can't delete a Target which is being used")]
    TargetInUse,
    #[error("{0}")]
    ParserError(#[from] ParserError),
    #[error("Invalid alert query: {0}")]
    InvalidAlertQuery(String),
    #[error("Invalid query parameter")]
    InvalidQueryParameter,
    #[error("{0}")]
    ArrowError(#[from] ArrowError),
    #[error("Upgrade to Parseable Enterprise for {0} type alerts")]
    NotPresentInOSS(&'static str),
    #[error("{0}")]
    Unimplemented(String),
    #[error("{0}")]
    ValidationFailure(String),
}

impl actix_web::ResponseError for AlertError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::BAD_REQUEST,
            Self::Error(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::DatafusionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::CustomError(_) => StatusCode::BAD_REQUEST,
            Self::InvalidStateChange(_) => StatusCode::BAD_REQUEST,
            Self::StreamNotFound(_) => StatusCode::NOT_FOUND,
            Self::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidAlertModifyRequest => StatusCode::BAD_REQUEST,
            Self::FromStrError(_) => StatusCode::BAD_REQUEST,
            Self::InvalidTargetID(_) => StatusCode::BAD_REQUEST,
            Self::InvalidTargetModification(_) => StatusCode::BAD_REQUEST,
            Self::TargetInUse => StatusCode::CONFLICT,
            Self::ParserError(_) => StatusCode::BAD_REQUEST,
            Self::InvalidAlertQuery(_) => StatusCode::BAD_REQUEST,
            Self::InvalidQueryParameter => StatusCode::BAD_REQUEST,
            Self::ValidationFailure(_) => StatusCode::BAD_REQUEST,
            Self::ArrowError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unimplemented(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotPresentInOSS(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

#[async_trait]
impl AlertManagerTrait for Alerts {
    /// Loads alerts from disk, blocks
    async fn load(&self) -> anyhow::Result<()> {
        let mut map = self.alerts.write().await;
        let store = PARSEABLE.storage.get_object_store();

        // Get alerts path and read raw bytes for migration handling
        let relative_path = relative_path::RelativePathBuf::from(ALERTS_ROOT_DIRECTORY);

        let raw_objects = store
            .get_objects(
                Some(&relative_path),
                Box::new(|file_name| file_name.ends_with(".json")),
            )
            .await
            .unwrap_or_default();

        for raw_bytes in raw_objects {
            // First, try to parse as JSON Value to check version
            let json_value: JsonValue = match serde_json::from_slice(&raw_bytes) {
                Ok(val) => val,
                Err(e) => {
                    error!("Failed to parse alert JSON: {e}");
                    continue;
                }
            };

            // Check version and handle migration
            let alert = if let Some(version_str) = json_value["version"].as_str() {
                if version_str == "v1"
                    || json_value["query"].is_null()
                    || json_value.get("stream").is_some()
                {
                    // This is a v1 alert that needs migration
                    match AlertConfig::migrate_from_v1(&json_value, store.as_ref()).await {
                        Ok(migrated) => migrated,
                        Err(e) => {
                            error!("Failed to migrate v1 alert: {e}");
                            continue;
                        }
                    }
                } else {
                    // Try to parse as v2
                    match serde_json::from_value::<AlertConfig>(json_value) {
                        Ok(alert) => alert,
                        Err(e) => {
                            error!("Failed to parse v2 alert: {e}");
                            continue;
                        }
                    }
                }
            } else {
                // No version field, assume v1 and migrate
                warn!("Found alert without version field, assuming v1 and migrating");
                match AlertConfig::migrate_from_v1(&json_value, store.as_ref()).await {
                    Ok(migrated) => migrated,
                    Err(e) => {
                        error!("Failed to migrate alert without version: {e}");
                        continue;
                    }
                }
            };

            let alert: Box<dyn AlertTrait> = match &alert.alert_type {
                AlertType::Threshold => {
                    Box::new(ThresholdAlert::from(alert)) as Box<dyn AlertTrait>
                }
                AlertType::Anomaly(_) => {
                    return Err(anyhow::Error::msg(
                        AlertError::NotPresentInOSS("anomaly").to_string(),
                    ));
                }
                AlertType::Forecast(_) => {
                    return Err(anyhow::Error::msg(
                        AlertError::NotPresentInOSS("forecast").to_string(),
                    ));
                }
            };

            // Create alert task iff alert's state is not paused
            if alert.get_state().eq(&AlertState::Disabled) {
                map.insert(*alert.get_id(), alert);
                continue;
            }

            match self.sender.send(AlertTask::Create(alert.clone_box())).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to create alert task: {e}\nRetrying...");
                    // Retry sending the task
                    match self.sender.send(AlertTask::Create(alert.clone_box())).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to create alert task: {e}");
                            continue;
                        }
                    }
                }
            };

            map.insert(*alert.get_id(), alert);
        }

        Ok(())
    }

    /// Returns a list of alerts that the user has access to (based on query auth)
    async fn list_alerts_for_user(
        &self,
        session: SessionKey,
        tags: Vec<String>,
    ) -> Result<Vec<AlertConfig>, AlertError> {
        // First, collect all alerts without performing auth checks to avoid holding the lock
        let all_alerts: Vec<AlertConfig> = {
            let alerts_guard = self.alerts.read().await;
            alerts_guard
                .values()
                .map(|alert| alert.to_alert_config())
                .collect()
        };
        // Lock is released here, now perform expensive auth checks

        let authorized_alerts = if tags.is_empty() {
            // Parallelize authorization checks
            let futures: Vec<_> = all_alerts
                .into_iter()
                .map(|alert| async {
                    if user_auth_for_query(&session.clone(), &alert.query)
                        .await
                        .is_ok()
                    {
                        Some(alert)
                    } else {
                        None
                    }
                })
                .collect();

            futures::future::join_all(futures)
                .await
                .into_iter()
                .flatten()
                .collect()
        } else {
            // Parallelize authorization checks and then filter by tags
            let futures: Vec<_> = all_alerts
                .into_iter()
                .map(|alert| async {
                    if user_auth_for_query(&session, &alert.query).await.is_ok() {
                        Some(alert)
                    } else {
                        None
                    }
                })
                .collect();

            futures::future::join_all(futures)
                .await
                .into_iter()
                .flatten()
                .filter(|alert| {
                    if let Some(alert_tags) = &alert.tags {
                        alert_tags.iter().any(|tag| tags.contains(tag))
                    } else {
                        false
                    }
                })
                .collect()
        };

        Ok(authorized_alerts)
    }

    /// Returns a single alert that the user has access to (based on query auth)
    async fn get_alert_by_id(&self, id: Ulid) -> Result<Box<dyn AlertTrait>, AlertError> {
        let read_access = self.alerts.read().await;
        if let Some(alert) = read_access.get(&id) {
            Ok(alert.clone_box())
        } else {
            Err(AlertError::CustomError(format!(
                "No alert found for the given ID- {id}"
            )))
        }
    }

    /// Update the in-mem vector of alerts
    async fn update(&self, alert: &dyn AlertTrait) {
        self.alerts
            .write()
            .await
            .insert(*alert.get_id(), alert.clone_box());
    }

    /// Update the state of alert
    async fn update_state(
        &self,
        alert_id: Ulid,
        new_state: AlertState,
        trigger_notif: Option<String>,
    ) -> Result<(), AlertError> {
        // let store = PARSEABLE.storage.get_object_store();

        // read and modify alert
        let mut write_access = self.alerts.write().await;
        let mut alert: Box<dyn AlertTrait> = if let Some(alert) = write_access.get(&alert_id) {
            match &alert.get_alert_type() {
                AlertType::Threshold => {
                    Box::new(ThresholdAlert::from(alert.to_alert_config())) as Box<dyn AlertTrait>
                }
                AlertType::Anomaly(_) => {
                    return Err(AlertError::NotPresentInOSS("anomaly"));
                }
                AlertType::Forecast(_) => {
                    return Err(AlertError::NotPresentInOSS("forecast"));
                }
            }
        } else {
            return Err(AlertError::CustomError(format!(
                "No alert found for the given ID- {alert_id}"
            )));
        };

        // if new state is Disabled then ensure that the task is removed from list
        if new_state.eq(&AlertState::Disabled) {
            if alert.get_state().eq(&AlertState::Disabled) {
                return Err(AlertError::InvalidStateChange(
                    "Can't disable an alert which is currently disabled".into(),
                ));
            }

            self.sender
                .send(AlertTask::Delete(alert_id))
                .await
                .map_err(|e| AlertError::CustomError(e.to_string()))?;
        }
        // user has resumed evals for this alert
        else if alert.get_state().eq(&AlertState::Disabled)
            && new_state.eq(&AlertState::NotTriggered)
        {
            self.sender
                .send(AlertTask::Create(alert.clone_box()))
                .await
                .map_err(|e| AlertError::CustomError(e.to_string()))?;
        }
        alert.update_state(new_state, trigger_notif).await?;
        write_access.insert(*alert.get_id(), alert.clone_box());
        Ok(())
    }

    /// Update the notification state of alert
    async fn update_notification_state(
        &self,
        alert_id: Ulid,
        new_notification_state: NotificationState,
    ) -> Result<(), AlertError> {
        // let store = PARSEABLE.storage.get_object_store();

        // read and modify alert
        let mut write_access = self.alerts.write().await;
        let mut alert: Box<dyn AlertTrait> = if let Some(alert) = write_access.get(&alert_id) {
            match &alert.get_alert_type() {
                AlertType::Threshold => {
                    Box::new(ThresholdAlert::from(alert.to_alert_config())) as Box<dyn AlertTrait>
                }
                AlertType::Anomaly(_) => {
                    return Err(AlertError::NotPresentInOSS("anomaly"));
                }
                AlertType::Forecast(_) => {
                    return Err(AlertError::NotPresentInOSS("forecast"));
                }
            }
        } else {
            return Err(AlertError::CustomError(format!(
                "No alert found for the given ID- {alert_id}"
            )));
        };

        alert
            .update_notification_state(new_notification_state)
            .await?;
        write_access.insert(*alert.get_id(), alert.clone_box());

        Ok(())
    }

    /// Remove alert and scheduled task from disk and memory
    async fn delete(&self, alert_id: Ulid) -> Result<(), AlertError> {
        if self.alerts.write().await.remove(&alert_id).is_some() {
            trace!("removed alert from memory");
        } else {
            warn!("Alert ID- {alert_id} not found in memory!");
        }
        Ok(())
    }

    /// Get state of alert using alert_id
    async fn get_state(&self, alert_id: Ulid) -> Result<AlertState, AlertError> {
        let read_access = self.alerts.read().await;

        if let Some(alert) = read_access.get(&alert_id) {
            Ok(*alert.get_state())
        } else {
            let msg = format!("No alert present for ID- {alert_id}");
            Err(AlertError::CustomError(msg))
        }
    }

    /// Start a scheduled alert task
    async fn start_task(&self, alert: Box<dyn AlertTrait>) -> Result<(), AlertError> {
        self.sender
            .send(AlertTask::Create(alert))
            .await
            .map_err(|e| AlertError::CustomError(e.to_string()))?;
        Ok(())
    }

    /// Remove a scheduled alert task
    async fn delete_task(&self, alert_id: Ulid) -> Result<(), AlertError> {
        self.sender
            .send(AlertTask::Delete(alert_id))
            .await
            .map_err(|e| AlertError::CustomError(e.to_string()))?;

        Ok(())
    }

    /// List tags from all alerts
    /// This function returns a list of unique tags from all alerts
    async fn list_tags(&self) -> Vec<String> {
        let alerts = self.alerts.read().await;
        let mut tags = alerts
            .iter()
            .filter_map(|(_, alert)| alert.get_tags().as_ref())
            .flat_map(|t| t.iter().cloned())
            .collect::<Vec<String>>();
        tags.sort();
        tags.dedup();
        tags
    }

    async fn get_all_alerts(&self) -> HashMap<Ulid, Box<dyn AlertTrait>> {
        let alerts = self.alerts.read().await;
        alerts.iter().map(|(k, v)| (*k, v.clone_box())).collect()
    }
}

// TODO: add RBAC
pub async fn get_alerts_summary() -> Result<AlertsSummary, AlertError> {
    let guard = ALERTS.read().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts.get_all_alerts().await
    } else {
        return Err(AlertError::CustomError("No AlertManager registered".into()));
    };

    let total = alerts.len() as u64;

    let mut triggered = 0;
    let mut not_triggered = 0;
    let mut disabled = 0;
    let mut triggered_alerts: Vec<AlertsInfo> = Vec::new();
    let mut disabled_alerts: Vec<AlertsInfo> = Vec::new();
    let mut not_triggered_alerts: Vec<AlertsInfo> = Vec::new();

    // find total alerts for each state
    // get title, id and state of each alert for that state
    for (_, alert) in alerts.iter() {
        match alert.get_state() {
            AlertState::Triggered => {
                triggered += 1;
                triggered_alerts.push(AlertsInfo {
                    title: alert.get_title().to_string(),
                    id: *alert.get_id(),
                    severity: alert.get_severity().clone(),
                });
            }
            AlertState::Disabled => {
                disabled += 1;
                disabled_alerts.push(AlertsInfo {
                    title: alert.get_title().to_string(),
                    id: *alert.get_id(),
                    severity: alert.get_severity().clone(),
                });
            }
            AlertState::NotTriggered => {
                not_triggered += 1;
                not_triggered_alerts.push(AlertsInfo {
                    title: alert.get_title().to_string(),
                    id: *alert.get_id(),
                    severity: alert.get_severity().clone(),
                });
            }
        }
    }

    // Sort and limit to top 5 for each state by severity priority
    triggered_alerts.sort_by_key(|alert| get_severity_priority(&alert.severity));
    triggered_alerts.truncate(5);

    disabled_alerts.sort_by_key(|alert| get_severity_priority(&alert.severity));
    disabled_alerts.truncate(5);

    not_triggered_alerts.sort_by_key(|alert| get_severity_priority(&alert.severity));
    not_triggered_alerts.truncate(5);

    let alert_summary = AlertsSummary {
        total,
        triggered: AlertsInfoByState {
            total: triggered,
            alert_info: triggered_alerts,
        },
        disabled: AlertsInfoByState {
            total: disabled,
            alert_info: disabled_alerts,
        },
        not_triggered: AlertsInfoByState {
            total: not_triggered,
            alert_info: not_triggered_alerts,
        },
    };
    Ok(alert_summary)
}

fn get_severity_priority(severity: &Severity) -> u8 {
    match severity {
        Severity::Critical => 0,
        Severity::High => 1,
        Severity::Medium => 2,
        Severity::Low => 3,
    }
}
