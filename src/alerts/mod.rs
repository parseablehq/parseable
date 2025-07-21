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
use async_trait::async_trait;
use chrono::Utc;
use datafusion::sql::sqlparser::parser::ParserError;
use derive_more::FromStrError;
use derive_more::derive::FromStr;
use http::StatusCode;
use once_cell::sync::Lazy;
use serde::Serialize;
use serde_json::Error as SerdeError;
use std::collections::{HashMap, HashSet};
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
use crate::parseable::{PARSEABLE, StreamNotFound};
use crate::rbac::map::SessionKey;
use crate::storage;
use crate::storage::ObjectStorageError;
use crate::sync::alert_runtime;
use crate::utils::user_auth_for_query;

// these types describe the scheduled task for an alert
pub type ScheduledTaskHandlers = (JoinHandle<()>, Receiver<()>, Sender<()>);

pub const CURRENT_ALERTS_VERSION: &str = "v1";

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
    Create(AlertConfig),
    Delete(Ulid),
}

#[derive(Default, Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AlertVerison {
    #[default]
    V1,
}

impl From<&str> for AlertVerison {
    fn from(value: &str) -> Self {
        match value {
            "v1" => Self::V1,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub struct AggregateResult {
    result: bool,
    message: Option<String>,
    config: AggregateConfig,
    value: f64,
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
pub struct AggregateConfig {
    pub aggregate_function: AggregateFunction,
    pub conditions: Option<Conditions>,
    pub group_by: Option<GroupBy>,
    pub column: String,
    pub operator: AlertOperator,
    pub value: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Aggregates {
    pub operator: Option<LogicalOperator>,
    pub aggregate_config: Vec<AggregateConfig>,
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
pub struct AlertRequest {
    #[serde(default = "Severity::default")]
    pub severity: Severity,
    pub title: String,
    pub stream: String,
    pub alert_type: AlertType,
    pub aggregates: Aggregates,
    pub eval_config: EvalConfig,
    pub targets: Vec<Ulid>,
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
            stream: self.stream,
            alert_type: self.alert_type,
            aggregates: self.aggregates,
            eval_config: self.eval_config,
            targets: self.targets,
            state: AlertState::default(),
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
    pub stream: String,
    pub alert_type: AlertType,
    pub aggregates: Aggregates,
    pub eval_config: EvalConfig,
    pub targets: Vec<Ulid>,
    // for new alerts, state should be resolved
    #[serde(default)]
    pub state: AlertState,
}

impl AlertConfig {
    pub async fn modify(&mut self, alert: AlertRequest) -> Result<(), AlertError> {
        // Validate that all target IDs exist
        for id in &alert.targets {
            TARGETS.get_target_by_id(id).await?;
        }
        self.title = alert.title;
        self.stream = alert.stream;
        self.alert_type = alert.alert_type;
        self.aggregates = alert.aggregates;
        self.eval_config = alert.eval_config;
        self.targets = alert.targets;
        self.state = AlertState::default();
        Ok(())
    }

    pub fn get_base_query(&self) -> String {
        format!("SELECT * FROM \"{}\"", self.stream)
    }

    /// Validations
    pub async fn validate(&self) -> Result<(), AlertError> {
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

        // validate aggregateConfig and conditionConfig
        self.validate_configs()?;

        // validate the presence of columns
        let columns = self.get_agg_config_cols();

        let schema = PARSEABLE.get_stream(&self.stream)?.get_schema();

        let schema_columns = schema
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<HashSet<&String>>();

        for col in columns {
            if !schema_columns.contains(col) {
                return Err(AlertError::CustomError(format!(
                    "Column {} not found in stream {}",
                    col, self.stream
                )));
            }
        }

        Ok(())
    }

    fn validate_configs(&self) -> Result<(), AlertError> {
        fn validate_condition_config(config: &Option<Conditions>) -> Result<(), AlertError> {
            if config.is_none() {
                return Ok(());
            }
            let config = config.as_ref().unwrap();
            match &config.operator {
                Some(_) => {
                    // only two aggregate conditions should be present
                    if config.condition_config.len() != 2 {
                        return Err(AlertError::CustomError(
                            "While using AND/OR, only two conditions must be used".to_string(),
                        ));
                    }
                }
                None => {
                    // only one aggregate condition should be present
                    if config.condition_config.len() != 1 {
                        return Err(AlertError::CustomError(
                            "While not using AND/OR, only one condition must be used".to_string(),
                        ));
                    }
                }
            }

            // validate that the value should be None in case of `is null` and `is not null`
            for condition in config.condition_config.iter() {
                let needs_no_value = matches!(
                    condition.operator,
                    WhereConfigOperator::IsNull | WhereConfigOperator::IsNotNull
                );

                if needs_no_value && condition.value.as_ref().is_some_and(|v| !v.is_empty()) {
                    return Err(AlertError::CustomError(
                        "value must be null when operator is either `is null` or `is not null`"
                            .into(),
                    ));
                }
                if !needs_no_value && condition.value.as_ref().is_none_or(|v| v.is_empty()) {
                    return Err(AlertError::CustomError(
                            "value must not be null when operator is neither `is null` nor `is not null`"
                                .into(),
                        ));
                }
            }
            Ok(())
        }

        // validate aggregate config(s)
        match &self.aggregates.operator {
            Some(_) => {
                // only two aggregate conditions should be present
                if self.aggregates.aggregate_config.len() != 2 {
                    return Err(AlertError::CustomError(
                        "While using AND/OR, only two aggregate conditions must be used"
                            .to_string(),
                    ));
                }

                // validate condition config
                let agg1 = &self.aggregates.aggregate_config[0];
                let agg2 = &self.aggregates.aggregate_config[1];

                validate_condition_config(&agg1.conditions)?;
                validate_condition_config(&agg2.conditions)?;
            }
            None => {
                // only one aggregate condition should be present
                if self.aggregates.aggregate_config.len() != 1 {
                    return Err(AlertError::CustomError(
                        "While not using AND/OR, only one aggregate condition must be used"
                            .to_string(),
                    ));
                }

                let agg = &self.aggregates.aggregate_config[0];
                validate_condition_config(&agg.conditions)?;
            }
        }
        Ok(())
    }

    fn get_agg_config_cols(&self) -> HashSet<&String> {
        let mut columns: HashSet<&String> = HashSet::new();
        match &self.aggregates.operator {
            Some(op) => match op {
                LogicalOperator::And | LogicalOperator::Or => {
                    let agg1 = &self.aggregates.aggregate_config[0];
                    let agg2 = &self.aggregates.aggregate_config[1];

                    columns.insert(&agg1.column);
                    columns.insert(&agg2.column);

                    if let Some(condition) = &agg1.conditions {
                        columns.extend(self.get_condition_cols(condition));
                    }
                }
            },
            None => {
                let agg = &self.aggregates.aggregate_config[0];
                columns.insert(&agg.column);

                if let Some(condition) = &agg.conditions {
                    columns.extend(self.get_condition_cols(condition));
                }
            }
        }
        columns
    }

    fn get_condition_cols<'a>(&'a self, condition: &'a Conditions) -> HashSet<&'a String> {
        let mut columns: HashSet<&String> = HashSet::new();
        match &condition.operator {
            Some(op) => match op {
                LogicalOperator::And | LogicalOperator::Or => {
                    let c1 = &condition.condition_config[0];
                    let c2 = &condition.condition_config[1];
                    columns.insert(&c1.column);
                    columns.insert(&c2.column);
                }
            },
            None => {
                let c = &condition.condition_config[0];
                columns.insert(&c.column);
            }
        }
        columns
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

        for alert in store.get_alerts().await.unwrap_or_default() {
            match self.sender.send(AlertTask::Create(alert.clone())).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to create alert task: {e}\nRetrying...");
                    // Retry sending the task
                    match self.sender.send(AlertTask::Create(alert.clone())).await {
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
    ) -> Result<Vec<AlertConfig>, AlertError> {
        let mut alerts: Vec<AlertConfig> = Vec::new();
        for (_, alert) in self.alerts.read().await.iter() {
            // filter based on whether the user can execute this query or not
            let query = alert.get_base_query();
            if user_auth_for_query(&session, &query).await.is_ok() {
                alerts.push(alert.to_owned());
            }
        }

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
            .send(AlertTask::Create(alert))
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
