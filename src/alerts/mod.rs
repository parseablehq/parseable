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
use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::logical_expr::{LogicalPlan, Projection};
use datafusion::sql::sqlparser::parser::ParserError;
use derive_more::FromStrError;
use derive_more::derive::FromStr;
use http::StatusCode;
use once_cell::sync::Lazy;
use serde::Serialize;
use serde_json::{Error as SerdeError, Value as JsonValue};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::thread;
use std::time::Duration;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, trace, warn};
use ulid::Ulid;

pub mod alerts_utils;
pub mod target;

use crate::alerts::target::TARGETS;
use crate::handlers::http::fetch_schema;
use crate::handlers::http::query::create_streams_for_distributed;
use crate::parseable::{PARSEABLE, StreamNotFound};
use crate::query::{QUERY_SESSION, resolve_stream_names};
use crate::rbac::map::SessionKey;
use crate::storage;
use crate::storage::{ALERTS_ROOT_DIRECTORY, ObjectStorageError};
use crate::sync::alert_runtime;
use crate::utils::user_auth_for_query;

/// Helper struct for basic alert fields during migration
struct BasicAlertFields {
    id: Ulid,
    title: String,
    severity: Severity,
}

// these types describe the scheduled task for an alert
pub type ScheduledTaskHandlers = (JoinHandle<()>, Receiver<()>, Sender<()>);

pub const CURRENT_ALERTS_VERSION: &str = "v2";

pub static ALERTS: Lazy<Alerts> = Lazy::new(|| {
    let (tx, rx) = mpsc::channel::<AlertTask>(10);
    let alerts = Alerts {
        alerts: RwLock::new(HashMap::new()),
        sender: tx,
    };

    thread::spawn(|| alert_runtime(rx));

    alerts
});

#[derive(Debug)]
pub struct Alerts {
    pub alerts: RwLock<HashMap<Ulid, AlertConfig>>,
    pub sender: mpsc::Sender<AlertTask>,
}

pub enum AlertTask {
    Create(Box<AlertConfig>),
    Delete(Ulid),
}

#[derive(Default, Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AlertVerison {
    V1,
    #[default]
    V2,
}

impl From<&str> for AlertVerison {
    fn from(value: &str) -> Self {
        match value {
            "v1" => Self::V1,
            "v2" => Self::V2,
            _ => Self::V2, // default to v2
        }
    }
}

#[async_trait]
pub trait CallableTarget {
    async fn call(&self, payload: &Context);
}

#[derive(Debug, Clone)]
pub struct Context {
    alert_info: AlertInfo,
    deployment_info: DeploymentInfo,
    message: String,
}

impl Context {
    pub fn new(alert_info: AlertInfo, deployment_info: DeploymentInfo, message: String) -> Self {
        Self {
            alert_info,
            deployment_info,
            message,
        }
    }

    fn default_alert_string(&self) -> String {
        format!(
            "AlertName: {}\nTriggered TimeStamp: {}\nSeverity: {}\n{}",
            self.alert_info.alert_name,
            Utc::now().to_rfc3339(),
            self.alert_info.severity,
            self.message
        )
    }

    fn default_resolved_string(&self) -> String {
        format!("{} is now resolved ", self.alert_info.alert_name)
    }

    fn default_silenced_string(&self) -> String {
        format!(
            "Notifications for {} have been silenced ",
            self.alert_info.alert_name
        )
    }
}

#[derive(Debug, Clone)]
pub struct AlertInfo {
    alert_id: Ulid,
    alert_name: String,
    // message: String,
    // reason: String,
    alert_state: AlertState,
    severity: String,
}

impl AlertInfo {
    pub fn new(
        alert_id: Ulid,
        alert_name: String,
        alert_state: AlertState,
        severity: String,
    ) -> Self {
        Self {
            alert_id,
            alert_name,
            alert_state,
            severity,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeploymentInfo {
    deployment_instance: String,
    deployment_id: Ulid,
    deployment_mode: String,
}

impl DeploymentInfo {
    pub fn new(deployment_instance: String, deployment_id: Ulid, deployment_mode: String) -> Self {
        Self {
            deployment_instance,
            deployment_id,
            deployment_mode,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AlertType {
    Threshold,
}

impl Display for AlertType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertType::Threshold => write!(f, "threshold"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AlertOperator {
    #[serde(rename = ">")]
    GreaterThan,
    #[serde(rename = "<")]
    LessThan,
    #[serde(rename = "=")]
    Equal,
    #[serde(rename = "!=")]
    NotEqual,
    #[serde(rename = ">=")]
    GreaterThanOrEqual,
    #[serde(rename = "<=")]
    LessThanOrEqual,
}

impl Display for AlertOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertOperator::GreaterThan => write!(f, ">"),
            AlertOperator::LessThan => write!(f, "<"),
            AlertOperator::Equal => write!(f, "="),
            AlertOperator::NotEqual => write!(f, "!="),
            AlertOperator::GreaterThanOrEqual => write!(f, ">="),
            AlertOperator::LessThanOrEqual => write!(f, "<="),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, FromStr, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum WhereConfigOperator {
    #[serde(rename = "=")]
    Equal,
    #[serde(rename = "!=")]
    NotEqual,
    #[serde(rename = "<")]
    LessThan,
    #[serde(rename = ">")]
    GreaterThan,
    #[serde(rename = "<=")]
    LessThanOrEqual,
    #[serde(rename = ">=")]
    GreaterThanOrEqual,
    #[serde(rename = "is null")]
    IsNull,
    #[serde(rename = "is not null")]
    IsNotNull,
    #[serde(rename = "ilike")]
    ILike,
    #[serde(rename = "contains")]
    Contains,
    #[serde(rename = "begins with")]
    BeginsWith,
    #[serde(rename = "ends with")]
    EndsWith,
    #[serde(rename = "does not contain")]
    DoesNotContain,
    #[serde(rename = "does not begin with")]
    DoesNotBeginWith,
    #[serde(rename = "does not end with")]
    DoesNotEndWith,
}

impl WhereConfigOperator {
    /// Convert the enum value to its string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::GreaterThan => ">",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThanOrEqual => ">=",
            Self::IsNull => "is null",
            Self::IsNotNull => "is not null",
            Self::ILike => "ilike",
            Self::Contains => "contains",
            Self::BeginsWith => "begins with",
            Self::EndsWith => "ends with",
            Self::DoesNotContain => "does not contain",
            Self::DoesNotBeginWith => "does not begin with",
            Self::DoesNotEndWith => "does not end with",
        }
    }
}

impl Display for WhereConfigOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // We can reuse our as_str method to get the string representation
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AggregateFunction {
    Avg,
    Count,
    CountDistinct,
    Min,
    Max,
    Sum,
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Avg => write!(f, "Avg"),
            AggregateFunction::Count => write!(f, "Count"),
            AggregateFunction::CountDistinct => write!(f, "CountDistinct"),
            AggregateFunction::Min => write!(f, "Min"),
            AggregateFunction::Max => write!(f, "Max"),
            AggregateFunction::Sum => write!(f, "Sum"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct OperationConfig {
    pub column: String,
    pub operator: Option<String>,
    pub value: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FilterConfig {
    pub conditions: Vec<Conditions>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ConditionConfig {
    pub column: String,
    pub operator: WhereConfigOperator,
    pub value: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Conditions {
    pub operator: Option<LogicalOperator>,
    pub condition_config: Vec<ConditionConfig>,
}

impl Conditions {
    pub fn generate_filter_message(&self) -> String {
        match &self.operator {
            Some(op) => match op {
                LogicalOperator::And | LogicalOperator::Or => {
                    let expr1 = &self.condition_config[0];
                    let expr2 = &self.condition_config[1];
                    let expr1_msg = if expr1.value.as_ref().is_some_and(|v| !v.is_empty()) {
                        format!(
                            "{} {} {}",
                            expr1.column,
                            expr1.operator,
                            expr1.value.as_ref().unwrap()
                        )
                    } else {
                        format!("{} {}", expr1.column, expr1.operator)
                    };

                    let expr2_msg = if expr2.value.as_ref().is_some_and(|v| !v.is_empty()) {
                        format!(
                            "{} {} {}",
                            expr2.column,
                            expr2.operator,
                            expr2.value.as_ref().unwrap()
                        )
                    } else {
                        format!("{} {}", expr2.column, expr2.operator)
                    };

                    format!("[{expr1_msg} {op} {expr2_msg}]")
                }
            },
            None => {
                let expr = &self.condition_config[0];
                if let Some(val) = &expr.value {
                    format!("{} {} {}", expr.column, expr.operator, val)
                } else {
                    format!("{} {}", expr.column, expr.operator)
                }
            }
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GroupBy {
    pub columns: Vec<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ThresholdConfig {
    pub operator: AlertOperator,
    pub value: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RollingWindow {
    // x minutes (25m)
    pub eval_start: String,
    // should always be "now"
    pub eval_end: String,
    // x minutes (5m)
    pub eval_frequency: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum EvalConfig {
    RollingWindow(RollingWindow),
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlertEval {}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Default, FromStr)]
#[serde(rename_all = "camelCase")]
pub enum AlertState {
    Triggered,
    Silenced,
    #[default]
    Resolved,
}

impl Display for AlertState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertState::Triggered => write!(f, "Triggered"),
            AlertState::Silenced => write!(f, "Silenced"),
            AlertState::Resolved => write!(f, "Resolved"),
        }
    }
}

impl AlertState {
    pub async fn update_state(
        &self,
        new_state: AlertState,
        alert_id: Ulid,
    ) -> Result<(), AlertError> {
        match self {
            AlertState::Triggered => {
                if new_state == AlertState::Triggered {
                    let msg = format!("Not allowed to manually go from Triggered to {new_state}");
                    return Err(AlertError::InvalidStateChange(msg));
                } else {
                    // update state on disk and in memory
                    ALERTS
                        .update_state(alert_id, new_state, Some("".into()))
                        .await?;
                }
            }
            AlertState::Silenced => {
                // from here, the user can only go to Resolved
                if new_state == AlertState::Resolved {
                    // update state on disk and in memory
                    ALERTS
                        .update_state(alert_id, new_state, Some("".into()))
                        .await?;
                } else {
                    let msg = format!("Not allowed to manually go from Silenced to {new_state}");
                    return Err(AlertError::InvalidStateChange(msg));
                }
            }
            AlertState::Resolved => {
                // user shouldn't logically be changing states if current state is Resolved
                let msg = format!("Not allowed to go manually from Resolved to {new_state}");
                return Err(AlertError::InvalidStateChange(msg));
            }
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub enum Severity {
    Critical,
    High,
    #[default]
    Medium,
    Low,
}

impl Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Critical => write!(f, "Critical (P0)"),
            Severity::High => write!(f, "High (P1)"),
            Severity::Medium => write!(f, "Medium (P2)"),
            Severity::Low => write!(f, "Low (P3)"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum LogicalOperator {
    And,
    Or,
}

impl Display for LogicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalOperator::And => write!(f, "AND"),
            LogicalOperator::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlertRequest {
    #[serde(default = "Severity::default")]
    pub severity: Severity,
    pub title: String,
    pub query: String,
    pub alert_type: AlertType,
    pub threshold_config: ThresholdConfig,
    pub eval_config: EvalConfig,
    pub targets: Vec<Ulid>,
    pub tags: Option<Vec<String>>,
}

impl AlertRequest {
    pub async fn into(self) -> Result<AlertConfig, AlertError> {
        // Validate that all target IDs exist
        for id in &self.targets {
            TARGETS.get_target_by_id(id).await?;
        }
        let config = AlertConfig {
            version: AlertVerison::from(CURRENT_ALERTS_VERSION),
            id: Ulid::new(),
            severity: self.severity,
            title: self.title,
            query: self.query,
            alert_type: self.alert_type,
            threshold_config: self.threshold_config,
            eval_config: self.eval_config,
            targets: self.targets,
            state: AlertState::default(),
            created: Utc::now(),
            tags: self.tags,
        };
        Ok(config)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlertConfig {
    pub version: AlertVerison,
    #[serde(default)]
    pub id: Ulid,
    pub severity: Severity,
    pub title: String,
    pub query: String,
    pub alert_type: AlertType,
    pub threshold_config: ThresholdConfig,
    pub eval_config: EvalConfig,
    pub targets: Vec<Ulid>,
    // for new alerts, state should be resolved
    #[serde(default)]
    pub state: AlertState,
    pub created: DateTime<Utc>,
    pub tags: Option<Vec<String>>,
}

impl AlertConfig {
    /// Migration function to convert v1 alerts to v2 structure
    async fn migrate_from_v1(
        alert_json: &JsonValue,
        store: &dyn crate::storage::ObjectStorage,
    ) -> Result<AlertConfig, AlertError> {
        let basic_fields = Self::parse_basic_fields(alert_json)?;
        let alert_info = format!("Alert '{}' (ID: {})", basic_fields.title, basic_fields.id);

        let query = Self::build_query_from_v1(alert_json, &alert_info).await?;
        let threshold_config = Self::extract_threshold_config(alert_json, &alert_info)?;
        let eval_config = Self::extract_eval_config(alert_json, &alert_info)?;
        let targets = Self::extract_targets(alert_json, &alert_info)?;
        let state = Self::extract_state(alert_json);

        // Create the migrated v2 alert
        let migrated_alert = AlertConfig {
            version: AlertVerison::V2,
            id: basic_fields.id,
            severity: basic_fields.severity,
            title: basic_fields.title,
            query,
            alert_type: AlertType::Threshold,
            threshold_config,
            eval_config,
            targets,
            state,
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
            "silenced" => AlertState::Silenced,
            "resolved" => AlertState::Resolved,
            _ => AlertState::Resolved,
        }
    }

    pub async fn modify(&mut self, alert: AlertRequest) -> Result<(), AlertError> {
        // Validate that all target IDs exist
        for id in &alert.targets {
            TARGETS.get_target_by_id(id).await?;
        }
        self.title = alert.title;
        self.query = alert.query;
        self.alert_type = alert.alert_type;
        self.threshold_config = alert.threshold_config;
        self.eval_config = alert.eval_config;
        self.targets = alert.targets;
        self.state = AlertState::default();
        Ok(())
    }

    /// Validations
    pub async fn validate(&self, session_key: SessionKey) -> Result<(), AlertError> {
        // validate evalType
        let eval_frequency = match &self.eval_config {
            EvalConfig::RollingWindow(rolling_window) => {
                if humantime::parse_duration(&rolling_window.eval_start).is_err() {
                    return Err(AlertError::Metadata(
                        "evalStart should be of type humantime",
                    ));
                }
                rolling_window.eval_frequency
            }
        };

        // validate that target repeat notifs !> eval_frequency
        for target_id in &self.targets {
            let target = TARGETS.get_target_by_id(target_id).await?;
            match &target.notification_config.times {
                target::Retry::Infinite => {}
                target::Retry::Finite(repeat) => {
                    let notif_duration =
                        Duration::from_secs(60 * target.notification_config.interval)
                            * *repeat as u32;
                    if (notif_duration.as_secs_f64()).gt(&((eval_frequency * 60) as f64)) {
                        return Err(AlertError::Metadata(
                            "evalFrequency should be greater than target repetition  interval",
                        ));
                    }
                }
            }
        }

        // validate that the query is valid
        if self.query.is_empty() {
            return Err(AlertError::InvalidAlertQuery);
        }

        let tables = resolve_stream_names(&self.query)?;
        if tables.is_empty() {
            return Err(AlertError::InvalidAlertQuery);
        }
        create_streams_for_distributed(tables)
            .await
            .map_err(|_| AlertError::InvalidAlertQuery)?;

        // validate that the user has access to the tables mentioned in the query
        user_auth_for_query(&session_key, &self.query).await?;

        // validate that the alert query is valid and can be evaluated
        if !Self::is_query_aggregate(&self.query).await? {
            return Err(AlertError::InvalidAlertQuery);
        }
        Ok(())
    }

    /// Check if a query is an aggregate query that returns a single value without executing it
    async fn is_query_aggregate(query: &str) -> Result<bool, AlertError> {
        let session_state = QUERY_SESSION.state();

        // Parse the query into a logical plan
        let logical_plan = session_state
            .create_logical_plan(query)
            .await
            .map_err(|err| AlertError::CustomError(format!("Failed to parse query: {err}")))?;

        // Check if the plan structure indicates an aggregate query
        Ok(Self::is_logical_plan_aggregate(&logical_plan))
    }

    /// Analyze a logical plan to determine if it represents an aggregate query
    fn is_logical_plan_aggregate(plan: &LogicalPlan) -> bool {
        match plan {
            // Direct aggregate: SELECT COUNT(*), AVG(col), etc.
            LogicalPlan::Aggregate(_) => true,

            // Projection over aggregate: SELECT COUNT(*) as total, SELECT AVG(col) as average
            LogicalPlan::Projection(Projection { input, expr, .. }) => {
                // Check if input contains an aggregate and we have exactly one expression
                let is_aggregate_input = Self::is_logical_plan_aggregate(input);
                let single_expr = expr.len() == 1;
                is_aggregate_input && single_expr
            }

            // Recursively check wrapped plans (Filter, Limit, Sort, etc.)
            _ => {
                // Use inputs() method to get all input plans
                plan.inputs()
                    .iter()
                    .any(|input| Self::is_logical_plan_aggregate(input))
            }
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
                self.severity.clone().to_string(),
            ),
            DeploymentInfo::new(deployment_instance, deployment_id, deployment_mode),
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

        map
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
    #[error("Invalid alert query")]
    InvalidAlertQuery,
    #[error("Invalid query parameter")]
    InvalidQueryParameter,
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
            Self::InvalidAlertQuery => StatusCode::BAD_REQUEST,
            Self::InvalidQueryParameter => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

impl Alerts {
    /// Loads alerts from disk, blocks
    pub async fn load(&self) -> anyhow::Result<()> {
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

            // Create alert task
            match self
                .sender
                .send(AlertTask::Create(Box::new(alert.clone())))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to create alert task: {e}\nRetrying...");
                    // Retry sending the task
                    match self
                        .sender
                        .send(AlertTask::Create(Box::new(alert.clone())))
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to create alert task: {e}");
                            continue;
                        }
                    }
                }
            };

            map.insert(alert.id, alert);
        }

        Ok(())
    }

    /// Returns a list of alerts that the user has access to (based on query auth)
    pub async fn list_alerts_for_user(
        &self,
        session: SessionKey,
        tags: Vec<String>,
    ) -> Result<Vec<AlertConfig>, AlertError> {
        let mut alerts: Vec<AlertConfig> = Vec::new();
        for (_, alert) in self.alerts.read().await.iter() {
            // filter based on whether the user can execute this query or not
            if user_auth_for_query(&session, &alert.query).await.is_ok() {
                alerts.push(alert.to_owned());
            }
        }
        if tags.is_empty() {
            return Ok(alerts);
        }
        // filter alerts based on tags
        alerts.retain(|alert| {
            if let Some(alert_tags) = &alert.tags {
                alert_tags.iter().any(|tag| tags.contains(tag))
            } else {
                false
            }
        });

        Ok(alerts)
    }

    /// Returns a sigle alert that the user has access to (based on query auth)
    pub async fn get_alert_by_id(&self, id: Ulid) -> Result<AlertConfig, AlertError> {
        let read_access = self.alerts.read().await;
        if let Some(alert) = read_access.get(&id) {
            Ok(alert.clone())
        } else {
            Err(AlertError::CustomError(format!(
                "No alert found for the given ID- {id}"
            )))
        }
    }

    /// Update the in-mem vector of alerts
    pub async fn update(&self, alert: &AlertConfig) {
        self.alerts.write().await.insert(alert.id, alert.clone());
    }

    /// Update the state of alert
    pub async fn update_state(
        &self,
        alert_id: Ulid,
        new_state: AlertState,
        trigger_notif: Option<String>,
    ) -> Result<(), AlertError> {
        let store = PARSEABLE.storage.get_object_store();

        // read and modify alert
        let mut alert = self.get_alert_by_id(alert_id).await?;
        trace!("get alert state by id-\n{}", alert.state);

        alert.state = new_state;

        trace!("new state-\n{}", alert.state);

        // save to disk
        store.put_alert(alert_id, &alert).await?;

        // modify in memory
        let mut writer = self.alerts.write().await;
        if let Some(alert) = writer.get_mut(&alert_id) {
            trace!("in memory alert-\n{}", alert.state);
            alert.state = new_state;
            trace!("in memory updated alert-\n{}", alert.state);
        };
        drop(writer);

        if trigger_notif.is_some() {
            trace!("trigger notif on-\n{}", alert.state);
            alert.trigger_notifications(trigger_notif.unwrap()).await?;
        }

        Ok(())
    }

    /// Remove alert and scheduled task from disk and memory
    pub async fn delete(&self, alert_id: Ulid) -> Result<(), AlertError> {
        if self.alerts.write().await.remove(&alert_id).is_some() {
            trace!("removed alert from memory");
        } else {
            warn!("Alert ID- {alert_id} not found in memory!");
        }
        Ok(())
    }

    /// Get state of alert using alert_id
    pub async fn get_state(&self, alert_id: Ulid) -> Result<AlertState, AlertError> {
        let read_access = self.alerts.read().await;

        if let Some(alert) = read_access.get(&alert_id) {
            Ok(alert.state)
        } else {
            let msg = format!("No alert present for ID- {alert_id}");
            Err(AlertError::CustomError(msg))
        }
    }

    /// Start a scheduled alert task
    pub async fn start_task(&self, alert: AlertConfig) -> Result<(), AlertError> {
        self.sender
            .send(AlertTask::Create(Box::new(alert)))
            .await
            .map_err(|e| AlertError::CustomError(e.to_string()))?;
        Ok(())
    }

    /// Remove a scheduled alert task
    pub async fn delete_task(&self, alert_id: Ulid) -> Result<(), AlertError> {
        self.sender
            .send(AlertTask::Delete(alert_id))
            .await
            .map_err(|e| AlertError::CustomError(e.to_string()))?;

        Ok(())
    }

    /// List tags from all alerts
    /// This function returns a list of unique tags from all alerts
    pub async fn list_tags(&self) -> Vec<String> {
        let alerts = self.alerts.read().await;
        let mut tags = alerts
            .iter()
            .filter_map(|(_, alert)| alert.tags.as_ref())
            .flat_map(|t| t.iter().cloned())
            .collect::<Vec<String>>();
        tags.sort();
        tags.dedup();
        tags
    }
}

#[derive(Debug, Serialize)]
pub struct AlertsSummary {
    total: u64,
    triggered: AlertsInfoByState,
    silenced: AlertsInfoByState,
    resolved: AlertsInfoByState,
}

#[derive(Debug, Serialize)]
pub struct AlertsInfoByState {
    total: u64,
    alert_info: Vec<AlertsInfo>,
}

#[derive(Debug, Serialize)]
pub struct AlertsInfo {
    title: String,
    id: Ulid,
    severity: Severity,
}

// TODO: add RBAC
pub async fn get_alerts_summary() -> Result<AlertsSummary, AlertError> {
    let alerts = ALERTS.alerts.read().await;
    let total = alerts.len() as u64;
    let mut triggered = 0;
    let mut resolved = 0;
    let mut silenced = 0;
    let mut triggered_alerts: Vec<AlertsInfo> = Vec::new();
    let mut silenced_alerts: Vec<AlertsInfo> = Vec::new();
    let mut resolved_alerts: Vec<AlertsInfo> = Vec::new();

    // find total alerts for each state
    // get title, id and state of each alert for that state
    for (_, alert) in alerts.iter() {
        match alert.state {
            AlertState::Triggered => {
                triggered += 1;
                triggered_alerts.push(AlertsInfo {
                    title: alert.title.clone(),
                    id: alert.id,
                    severity: alert.severity.clone(),
                });
            }
            AlertState::Silenced => {
                silenced += 1;
                silenced_alerts.push(AlertsInfo {
                    title: alert.title.clone(),
                    id: alert.id,
                    severity: alert.severity.clone(),
                });
            }
            AlertState::Resolved => {
                resolved += 1;
                resolved_alerts.push(AlertsInfo {
                    title: alert.title.clone(),
                    id: alert.id,
                    severity: alert.severity.clone(),
                });
            }
        }
    }

    // Sort and limit to top 5 for each state by severity priority
    triggered_alerts.sort_by_key(|alert| get_severity_priority(&alert.severity));
    triggered_alerts.truncate(5);

    silenced_alerts.sort_by_key(|alert| get_severity_priority(&alert.severity));
    silenced_alerts.truncate(5);

    resolved_alerts.sort_by_key(|alert| get_severity_priority(&alert.severity));
    resolved_alerts.truncate(5);

    let alert_summary = AlertsSummary {
        total,
        triggered: AlertsInfoByState {
            total: triggered,
            alert_info: triggered_alerts,
        },
        silenced: AlertsInfoByState {
            total: silenced,
            alert_info: silenced_alerts,
        },
        resolved: AlertsInfoByState {
            total: resolved,
            alert_info: resolved_alerts,
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
