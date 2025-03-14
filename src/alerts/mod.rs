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
use alerts_utils::user_auth_for_query;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::common::tree_node::TreeNode;
use http::StatusCode;
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::Serialize;
use serde_json::Error as SerdeError;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{trace, warn};
use ulid::Ulid;

pub mod alerts_utils;
pub mod target;

use crate::parseable::{StreamNotFound, PARSEABLE};
use crate::query::{TableScanVisitor, QUERY_SESSION};
use crate::rbac::map::SessionKey;
use crate::storage;
use crate::storage::ObjectStorageError;
use crate::sync::schedule_alert_task;
use crate::utils::time::TimeRange;

use self::target::Target;

// these types describe the scheduled task for an alert
pub type ScheduledTaskHandlers = (JoinHandle<()>, Receiver<()>, Sender<()>);

pub const CURRENT_ALERTS_VERSION: &str = "v1";

pub static ALERTS: Lazy<Alerts> = Lazy::new(Alerts::default);

#[derive(Debug, Default)]
pub struct Alerts {
    pub alerts: RwLock<HashMap<Ulid, AlertConfig>>,
    pub scheduled_tasks: RwLock<HashMap<Ulid, ScheduledTaskHandlers>>,
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
            "AlertName: {}, Triggered TimeStamp: {}, Severity: {}, Message: {}",
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
    EqualTo,
    #[serde(rename = "<>")]
    NotEqualTo,
    #[serde(rename = ">=")]
    GreaterThanEqualTo,
    #[serde(rename = "<=")]
    LessThanEqualTo,
    #[serde(rename = "like")]
    Like,
    #[serde(rename = "not like")]
    NotLike,
}

impl Display for AlertOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertOperator::GreaterThan => write!(f, ">"),
            AlertOperator::LessThan => write!(f, "<"),
            AlertOperator::EqualTo => write!(f, "="),
            AlertOperator::NotEqualTo => write!(f, "<>"),
            AlertOperator::GreaterThanEqualTo => write!(f, ">="),
            AlertOperator::LessThanEqualTo => write!(f, "<="),
            AlertOperator::Like => write!(f, "like"),
            AlertOperator::NotLike => write!(f, "not like"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AggregateOperation {
    Avg,
    Count,
    Min,
    Max,
    Sum,
}

impl Display for AggregateOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateOperation::Avg => write!(f, "Avg"),
            AggregateOperation::Count => write!(f, "Count"),
            AggregateOperation::Min => write!(f, "Min"),
            AggregateOperation::Max => write!(f, "Max"),
            AggregateOperation::Sum => write!(f, "Sum"),
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
    pub operator: AlertOperator,
    pub value: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Conditions {
    pub operator: Option<AggregateCondition>,
    pub conditions: Vec<ConditionConfig>,
}

// #[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
// pub enum Conditions {
//     AND((ConditionConfig, ConditionConfig)),
//     OR((ConditionConfig, ConditionConfig)),
//     Condition(ConditionConfig),
// }

impl Conditions {
    pub fn generate_filter_message(&self) -> String {
        match &self.operator {
            Some(op) => match op {
                AggregateCondition::And | AggregateCondition::Or => {
                    let expr1 = &self.conditions[0];
                    let expr2 = &self.conditions[1];
                    format!(
                        "[{} {} {} AND {} {} {}]",
                        expr1.column,
                        expr1.operator,
                        expr1.value,
                        expr2.column,
                        expr2.operator,
                        expr2.value
                    )
                }
            },
            None => {
                let expr = &self.conditions[0];
                format!("[{} {} {}]", expr.column, expr.operator, expr.value)
            }
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AggregateConfig {
    pub agg: AggregateOperation,
    pub condition_config: Option<Conditions>,
    pub column: String,
    pub operator: AlertOperator,
    pub value: f64,
}

// #[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
// pub enum Aggregations {
//     AND((AggregateConfig, AggregateConfig)),
//     OR((AggregateConfig, AggregateConfig)),
//     Single(AggregateConfig),
// }

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Aggregations {
    pub operator: Option<AggregateCondition>,
    pub aggregate_conditions: Vec<AggregateConfig>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub enum AggregateCondition {
    And,
    Or,
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

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Default)]
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
    pub query: String,
    pub alert_type: AlertType,
    pub aggregate_config: Aggregations,
    pub eval_type: EvalConfig,
    pub targets: Vec<Target>,
}

impl From<AlertRequest> for AlertConfig {
    fn from(val: AlertRequest) -> AlertConfig {
        AlertConfig {
            version: AlertVerison::from(CURRENT_ALERTS_VERSION),
            id: Ulid::new(),
            severity: val.severity,
            title: val.title,
            query: val.query,
            alert_type: val.alert_type,
            aggregate_config: val.aggregate_config,
            eval_type: val.eval_type,
            targets: val.targets,
            state: AlertState::default(),
        }
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
    pub aggregate_config: Aggregations,
    pub eval_type: EvalConfig,
    pub targets: Vec<Target>,
    // for new alerts, state should be resolved
    #[serde(default)]
    pub state: AlertState,
}

impl AlertConfig {
    pub fn modify(&mut self, alert: AlertRequest) {
        self.title = alert.title;
        self.query = alert.query;
        self.alert_type = alert.alert_type;
        self.aggregate_config = alert.aggregate_config;
        self.eval_type = alert.eval_type;
        self.targets = alert.targets;
        self.state = AlertState::default();
    }

    /// Validations
    pub async fn validate(&self) -> Result<(), AlertError> {
        // validate evalType
        let eval_frequency = match &self.eval_type {
            EvalConfig::RollingWindow(rolling_window) => {
                if rolling_window.eval_end != "now" {
                    return Err(AlertError::Metadata("evalEnd should be now"));
                }

                if humantime::parse_duration(&rolling_window.eval_start).is_err() {
                    return Err(AlertError::Metadata(
                        "evalStart should be of type humantime",
                    ));
                }
                rolling_window.eval_frequency
            }
        };

        // validate that target repeat notifs !> eval_frequency
        for target in &self.targets {
            match &target.timeout.times {
                target::Retry::Infinite => {}
                target::Retry::Finite(repeat) => {
                    let notif_duration = target.timeout.interval * *repeat as u32;
                    if (notif_duration.as_secs_f64()).gt(&((eval_frequency * 60) as f64)) {
                        return Err(AlertError::Metadata(
                            "evalFrequency should be greater than target repetition  interval",
                        ));
                    }
                }
            }
        }

        // validate aggregateCnnfig and conditionConfig
        self.validate_configs()?;

        let session_state = QUERY_SESSION.state();
        let raw_logical_plan = session_state.create_logical_plan(&self.query).await?;

        // create a visitor to extract the table names present in query
        let mut visitor = TableScanVisitor::default();
        let _ = raw_logical_plan.visit(&mut visitor);

        let table = visitor.into_inner().first().unwrap().to_owned();

        let lowercase = self.query.split(&table).collect_vec()[0].to_lowercase();

        if lowercase
            .strip_prefix(" ")
            .unwrap_or(&lowercase)
            .strip_suffix(" ")
            .unwrap_or(&lowercase)
            .ne("select * from")
        {
            return Err(AlertError::Metadata(
                "Query needs to be select * from <logstream>",
            ));
        }

        // TODO: Filter tags should be taken care of!!!
        let time_range = TimeRange::parse_human_time("1m", "now")
            .map_err(|err| AlertError::CustomError(err.to_string()))?;

        let query = crate::query::Query {
            raw_logical_plan,
            time_range,
            filter_tag: None,
        };

        // for now proceed in a similar fashion as we do in query
        // TODO: in case of multiple table query does the selection of time partition make a difference? (especially when the tables don't have overlapping data)
        let Some(stream_name) = query.first_table_name() else {
            return Err(AlertError::CustomError(format!(
                "Table name not found in query- {}",
                self.query
            )));
        };

        let time_partition = PARSEABLE.get_stream(&stream_name)?.get_time_partition();
        let base_df = query
            .get_dataframe(time_partition.as_ref())
            .await
            .map_err(|err| AlertError::CustomError(err.to_string()))?;

        // now that we have base_df, verify that it has
        // columns from aggregate config
        let columns = self.get_agg_config_cols();

        base_df.select_columns(columns.iter().map(|c| c.as_str()).collect_vec().as_slice())?;
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
                    if config.conditions.len() != 2 {
                        return Err(AlertError::CustomError(
                            "While using AND/OR, two conditions must be used".to_string(),
                        ));
                    }
                }
                None => {
                    // only one aggregate condition should be present
                    if config.conditions.len() != 1 {
                        return Err(AlertError::CustomError(
                            "While not using AND/OR, one conditions must be used".to_string(),
                        ));
                    }
                }
            }
            Ok(())
        }

        // validate aggregate config(s)
        match &self.aggregate_config.operator {
            Some(_) => {
                // only two aggregate conditions should be present
                if self.aggregate_config.aggregate_conditions.len() != 2 {
                    return Err(AlertError::CustomError(
                        "While using AND/OR, two aggregateConditions must be used".to_string(),
                    ));
                }

                // validate condition config
                let agg1 = &self.aggregate_config.aggregate_conditions[0];
                let agg2 = &self.aggregate_config.aggregate_conditions[0];

                validate_condition_config(&agg1.condition_config)?;
                validate_condition_config(&agg2.condition_config)?;
            }
            None => {
                // only one aggregate condition should be present
                if self.aggregate_config.aggregate_conditions.len() != 1 {
                    return Err(AlertError::CustomError(
                        "While not using AND/OR, one aggregateConditions must be used".to_string(),
                    ));
                }

                let agg = &self.aggregate_config.aggregate_conditions[0];
                validate_condition_config(&agg.condition_config)?;
            }
        }
        Ok(())
    }

    fn get_agg_config_cols(&self) -> HashSet<&String> {
        let mut columns: HashSet<&String> = HashSet::new();
        match &self.aggregate_config.operator {
            Some(op) => match op {
                AggregateCondition::And | AggregateCondition::Or => {
                    let agg1 = &self.aggregate_config.aggregate_conditions[0];
                    let agg2 = &self.aggregate_config.aggregate_conditions[1];

                    columns.insert(&agg1.column);
                    columns.insert(&agg2.column);

                    if let Some(condition) = &agg1.condition_config {
                        columns.extend(self.get_condition_cols(condition));
                    }
                }
            },
            None => {
                let agg = &self.aggregate_config.aggregate_conditions[0];
                columns.insert(&agg.column);

                if let Some(condition) = &agg.condition_config {
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
                AggregateCondition::And | AggregateCondition::Or => {
                    let c1 = &condition.conditions[0];
                    let c2 = &condition.conditions[1];
                    columns.insert(&c1.column);
                    columns.insert(&c2.column);
                }
            },
            None => {
                let c = &condition.conditions[0];
                columns.insert(&c.column);
            }
        }
        columns
    }

    pub fn get_eval_frequency(&self) -> u64 {
        match &self.eval_type {
            EvalConfig::RollingWindow(rolling_window) => rolling_window.eval_frequency,
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

        // let additional_labels =
        //     serde_json::to_value(rule).expect("rule is perfectly deserializable");
        // let flatten_additional_labels =
        //     utils::json::flatten::flatten_with_parent_prefix(additional_labels, "rule", "_")
        //         .expect("can be flattened");

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
        for target in &self.targets {
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
    pub async fn load(&self) -> Result<(), AlertError> {
        let mut map = self.alerts.write().await;
        let store = PARSEABLE.storage.get_object_store();

        for alert in store.get_alerts().await.unwrap_or_default() {
            let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
            let (inbox_tx, inbox_rx) = oneshot::channel::<()>();
            let handle = schedule_alert_task(
                alert.get_eval_frequency(),
                alert.clone(),
                inbox_rx,
                outbox_tx,
            )?;

            self.update_task(alert.id, handle, outbox_rx, inbox_tx)
                .await;

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
            let query = &alert.query;
            if user_auth_for_query(&session, query).await.is_ok() {
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

    /// Update the scheduled alert tasks in-memory map
    pub async fn update_task(
        &self,
        id: Ulid,
        handle: JoinHandle<()>,
        rx: Receiver<()>,
        tx: Sender<()>,
    ) {
        self.scheduled_tasks
            .write()
            .await
            .insert(id, (handle, rx, tx));
    }

    /// Remove a scheduled alert task
    pub async fn delete_task(&self, alert_id: Ulid) -> Result<(), AlertError> {
        if self
            .scheduled_tasks
            .write()
            .await
            .remove(&alert_id)
            .is_none()
        {
            trace!("Alert task {alert_id} not found in hashmap");
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct AlertsInfo {
    total: u64,
    silenced: u64,
    resolved: u64,
    triggered: u64,
    low: u64,
    medium: u64,
    high: u64,
}

// TODO: add RBAC
pub async fn get_alerts_info() -> Result<AlertsInfo, AlertError> {
    let alerts = ALERTS.alerts.read().await;
    let mut total = 0;
    let mut silenced = 0;
    let mut resolved = 0;
    let mut triggered = 0;
    let mut low = 0;
    let mut medium = 0;
    let mut high = 0;

    for (_, alert) in alerts.iter() {
        total += 1;
        match alert.state {
            AlertState::Silenced => silenced += 1,
            AlertState::Resolved => resolved += 1,
            AlertState::Triggered => triggered += 1,
        }

        match alert.severity {
            Severity::Low => low += 1,
            Severity::Medium => medium += 1,
            Severity::High => high += 1,
            _ => {}
        }
    }

    Ok(AlertsInfo {
        total,
        silenced,
        resolved,
        triggered,
        low,
        medium,
        high,
    })
}
