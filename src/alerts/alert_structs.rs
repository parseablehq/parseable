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

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, mpsc};
use ulid::Ulid;

use crate::{
    alerts::{
        AlertError, CURRENT_ALERTS_VERSION,
        alert_enums::{
            AlertOperator, AlertState, AlertTask, AlertType, AlertVersion, EvalConfig,
            LogicalOperator, NotificationState, Severity, WhereConfigOperator,
        },
        alert_traits::AlertTrait,
        target::{NotificationConfig, TARGETS},
    },
    query::resolve_stream_names,
};

/// Helper struct for basic alert fields during migration
pub struct BasicAlertFields {
    pub id: Ulid,
    pub title: String,
    pub severity: Severity,
}

#[derive(Debug)]
pub struct Alerts {
    pub alerts: RwLock<HashMap<Ulid, Box<dyn AlertTrait>>>,
    pub sender: mpsc::Sender<AlertTask>,
}

#[derive(Debug, Clone)]
pub struct Context {
    pub alert_info: AlertInfo,
    pub deployment_info: DeploymentInfo,
    pub message: String,
    pub notification_config: NotificationConfig,
}

impl Context {
    pub fn new(
        alert_info: AlertInfo,
        deployment_info: DeploymentInfo,
        notification_config: NotificationConfig,
        message: String,
    ) -> Self {
        Self {
            alert_info,
            deployment_info,
            message,
            notification_config,
        }
    }

    pub(crate) fn default_alert_string(&self) -> String {
        format!(
            "AlertName: {}\nTriggered TimeStamp: {}\nSeverity: {}\n{}",
            self.alert_info.alert_name,
            Utc::now().to_rfc3339(),
            self.alert_info.severity,
            self.message
        )
    }

    pub(crate) fn default_resolved_string(&self) -> String {
        format!("{} is now `not-triggered` ", self.alert_info.alert_name)
    }

    pub(crate) fn default_disabled_string(&self) -> String {
        format!(
            "{} is now `disabled`. No more evals will be run till the sate is `disabled`.",
            self.alert_info.alert_name
        )
    }
}

#[derive(Debug, Clone)]
pub struct AlertInfo {
    pub alert_id: Ulid,
    pub alert_name: String,
    // message: String,
    // reason: String,
    pub alert_state: AlertState,
    pub notification_state: NotificationState,
    pub severity: String,
}

impl AlertInfo {
    pub fn new(
        alert_id: Ulid,
        alert_name: String,
        alert_state: AlertState,
        notification_state: NotificationState,
        severity: String,
    ) -> Self {
        Self {
            alert_id,
            alert_name,
            alert_state,
            notification_state,
            severity,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeploymentInfo {
    pub deployment_instance: String,
    pub deployment_id: Ulid,
    pub deployment_mode: String,
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

impl Default for RollingWindow {
    fn default() -> Self {
        Self {
            eval_start: "10m".into(),
            eval_end: "now".into(),
            eval_frequency: 10,
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
    pub alert_type: String,
    pub anomaly_config: Option<AnomalyConfig>,
    pub forecast_config: Option<ForecastConfig>,
    pub threshold_config: ThresholdConfig,
    #[serde(default)]
    pub notification_config: NotificationConfig,
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
        let datasets = resolve_stream_names(&self.query)?;
        let config = AlertConfig {
            version: AlertVersion::from(CURRENT_ALERTS_VERSION),
            id: Ulid::new(),
            severity: self.severity,
            title: self.title,
            query: self.query,
            datasets,
            alert_type: {
                match self.alert_type.as_str() {
                    "anomaly" => {
                        if let Some(conf) = self.anomaly_config {
                            AlertType::Anomaly(conf)
                        } else {
                            return Err(AlertError::Metadata(
                                "anomalyConfig is required for anomaly type alerts",
                            ));
                        }
                    }
                    "forecast" => {
                        if let Some(conf) = self.forecast_config {
                            AlertType::Forecast(conf)
                        } else {
                            return Err(AlertError::Metadata(
                                "forecastConfig is required for forecast type alerts",
                            ));
                        }
                    }
                    "threshold" => AlertType::Threshold,
                    _ => return Err(AlertError::Metadata("Invalid alert type provided")),
                }
            },
            threshold_config: self.threshold_config,
            eval_config: self.eval_config,
            targets: self.targets,
            state: AlertState::default(),
            notification_state: NotificationState::Notify,
            notification_config: self.notification_config,
            created: Utc::now(),
            tags: self.tags,
        };
        Ok(config)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlertConfig {
    pub version: AlertVersion,
    #[serde(default)]
    pub id: Ulid,
    pub severity: Severity,
    pub title: String,
    pub query: String,
    pub datasets: Vec<String>,
    pub alert_type: AlertType,
    pub threshold_config: ThresholdConfig,
    pub eval_config: EvalConfig,
    pub targets: Vec<Ulid>,
    // for new alerts, state should be resolved
    #[serde(default)]
    pub state: AlertState,
    pub notification_state: NotificationState,
    pub notification_config: NotificationConfig,
    pub created: DateTime<Utc>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlertConfigResponse {
    pub version: AlertVersion,
    #[serde(default)]
    pub id: Ulid,
    pub severity: Severity,
    pub title: String,
    pub query: String,
    pub datasets: Vec<String>,
    pub alert_type: &'static str,
    pub anomaly_config: Option<AnomalyConfig>,
    pub forecast_config: Option<ForecastConfig>,
    pub threshold_config: ThresholdConfig,
    pub eval_config: EvalConfig,
    pub targets: Vec<Ulid>,
    // for new alerts, state should be resolved
    #[serde(default)]
    pub state: AlertState,
    pub notification_state: NotificationState,
    pub notification_config: NotificationConfig,
    pub created: DateTime<Utc>,
    pub tags: Option<Vec<String>>,
}

impl AlertConfig {
    pub fn to_response(self) -> AlertConfigResponse {
        AlertConfigResponse {
            version: self.version,
            id: self.id,
            severity: self.severity,
            title: self.title,
            query: self.query,
            datasets: self.datasets,
            alert_type: {
                match self.alert_type {
                    AlertType::Threshold => "threshold",
                    AlertType::Anomaly(_) => "anomaly",
                    AlertType::Forecast(_) => "forecast",
                }
            },
            anomaly_config: {
                match &self.alert_type {
                    AlertType::Anomaly(conf) => Some(conf.clone()),
                    _ => None,
                }
            },
            forecast_config: {
                match self.alert_type {
                    AlertType::Forecast(conf) => Some(conf),
                    _ => None,
                }
            },
            threshold_config: self.threshold_config,
            eval_config: self.eval_config,
            targets: self.targets,
            state: self.state,
            notification_state: self.notification_state,
            notification_config: self.notification_config,
            created: self.created,
            tags: self.tags,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertsSummary {
    pub total: u64,
    pub triggered: AlertsInfoByState,
    pub disabled: AlertsInfoByState,
    pub not_triggered: AlertsInfoByState,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertsInfoByState {
    pub total: u64,
    pub alert_info: Vec<AlertsInfo>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertsInfo {
    pub title: String,
    pub id: Ulid,
    pub severity: Severity,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ForecastConfig {
    pub historic_duration: String,
    pub forecast_duration: String,
}

impl ForecastConfig {
    pub fn calculate_eval_window(&self) -> Result<String, AlertError> {
        let parsed_historic_duration =
            if let Ok(historic_duration) = humantime::parse_duration(&self.historic_duration) {
                historic_duration
            } else {
                return Err(AlertError::Metadata(
                    "historicDuration should be of type humantime",
                ));
            };

        let eval_window = if parsed_historic_duration.lt(&Duration::from_secs(60 * 60 * 24 * 3)) {
            // less than 3 days = 10 mins
            "10m"
        } else {
            "30m"
        };

        Ok(eval_window.into())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AnomalyConfig {
    pub historic_duration: String,
}

impl AnomalyConfig {
    pub fn calculate_eval_window(&self) -> Result<String, AlertError> {
        let parsed_historic_duration =
            if let Ok(historic_duration) = humantime::parse_duration(&self.historic_duration) {
                historic_duration
            } else {
                return Err(AlertError::Metadata(
                    "historicDuration should be of type humantime",
                ));
            };

        let eval_window = if parsed_historic_duration.lt(&Duration::from_secs(60 * 60 * 24 * 3)) {
            // less than 3 days = 10 mins
            "10m"
        } else {
            "30m"
        };

        Ok(eval_window.into())
    }
}

/// Result structure for alert query execution with group support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertQueryResult {
    /// List of group results, each containing group values and the aggregate value
    pub groups: Vec<GroupResult>,
    /// True if this is a simple query without GROUP BY (single group with empty group_values)
    pub is_simple_query: bool,
}

/// Result for a single group in a GROUP BY query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupResult {
    /// The group-by column values (empty for non-GROUP BY queries)
    pub group_values: HashMap<String, String>,
    /// The aggregate function value for this group
    pub aggregate_value: f64,
}

impl AlertQueryResult {
    /// Get the single aggregate value for simple queries (backward compatibility)
    pub fn get_single_value(&self) -> f64 {
        if self.is_simple_query && !self.groups.is_empty() {
            self.groups[0].aggregate_value
        } else {
            0.0
        }
    }
}

#[derive(serde::Deserialize)]
pub struct NotificationStateRequest {
    pub state: String,
}
