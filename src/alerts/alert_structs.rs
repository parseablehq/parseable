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

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    metastore::metastore_traits::MetastoreObject,
    query::resolve_stream_names,
    storage::object_storage::{alert_json_path, alert_state_json_path, mttr_json_path},
};

const RESERVED_FIELDS: &[&str] = &[
    "version",
    "id",
    "severity",
    "title",
    "query",
    "datasets",
    "alertType",
    "alert_type",
    "anomalyConfig",
    "anomaly_config",
    "forecastConfig",
    "forecast_config",
    "thresholdConfig",
    "threshold_config",
    "evalConfig",
    "eval_config",
    "targets",
    "state",
    "notificationState",
    "notification_state",
    "notificationConfig",
    "notification_config",
    "created",
    "tags",
    "lastTriggeredAt",
    "last_triggered_at",
];

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
    #[serde(rename = "type")]
    #[serde(default)]
    pub column_type: Option<String>,
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
    #[serde(flatten)]
    pub other_fields: Option<serde_json::Map<String, Value>>,
}

impl AlertRequest {
    pub async fn into(self) -> Result<AlertConfig, AlertError> {
        // Validate that other_fields doesn't contain reserved field names
        let other_fields = if let Some(mut other_fields) = self.other_fields {
            // Limit other_fields to maximum 10 fields
            if other_fields.len() > 10 {
                return Err(AlertError::ValidationFailure(format!(
                    "other_fields can contain at most 10 fields, found {}",
                    other_fields.len()
                )));
            }

            for reserved in RESERVED_FIELDS {
                if other_fields.remove(*reserved).is_some() {
                    tracing::warn!("Removed reserved field '{}' from other_fields", reserved);
                }
            }

            if other_fields.is_empty() {
                None
            } else {
                Some(other_fields)
            }
        } else {
            None
        };

        // Validate that all target IDs exist
        for id in &self.targets {
            TARGETS.get_target_by_id(id).await?;
        }
        let datasets = resolve_stream_names(&self.query)?;

        if datasets.len() != 1 {
            return Err(AlertError::ValidationFailure(format!(
                "Query should include only one dataset. Found: {datasets:?}"
            )));
        }

        let created_timestamp = Utc::now();

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
            created: created_timestamp,
            tags: self.tags,
            last_triggered_at: None,
            other_fields,
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
    pub last_triggered_at: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub other_fields: Option<serde_json::Map<String, Value>>,
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
    pub last_triggered_at: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub other_fields: Option<serde_json::Map<String, Value>>,
}

impl AlertConfig {
    /// Filters out reserved field names from other_fields
    /// This prevents conflicts when flattening other_fields during serialization
    pub fn sanitize_other_fields(&mut self) {
        if let Some(ref mut other_fields) = self.other_fields {
            for reserved in RESERVED_FIELDS {
                if other_fields.remove(*reserved).is_some() {
                    tracing::warn!(
                        "Removed reserved field '{}' from other_fields during sanitization",
                        reserved
                    );
                }
            }

            if other_fields.is_empty() {
                self.other_fields = None;
            }
        }
    }

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
            last_triggered_at: self.last_triggered_at,
            other_fields: self.other_fields,
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

#[derive(Deserialize)]
pub struct NotificationStateRequest {
    pub state: String,
}

/// MTTR (Mean Time To Recovery) statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MTTRStats {
    /// Total number of incidents (triggered -> not-triggered cycles)
    pub total_incidents: usize,
    /// Mean recovery time in seconds
    pub mean_seconds: f64,
    /// Median recovery time in seconds
    pub median_seconds: f64,
    /// Minimum recovery time in seconds
    pub min_seconds: f64,
    /// Maximum recovery time in seconds
    pub max_seconds: f64,
    /// All individual recovery times in seconds
    pub recovery_times_seconds: Vec<i64>,
}

impl MTTRStats {
    /// Check if there are no incidents recorded
    pub fn is_empty(&self) -> bool {
        self.total_incidents == 0
    }

    /// Create MTTRStats from a list of recovery times
    pub fn from_recovery_times(recovery_times: Vec<i64>) -> MTTRStats {
        if recovery_times.is_empty() {
            return MTTRStats::default();
        }

        let total_incidents = recovery_times.len();
        let total_recovery_time: i64 = recovery_times.iter().sum();
        let mean_seconds = total_recovery_time as f64 / total_incidents as f64;

        let min_seconds = *recovery_times.iter().min().unwrap() as f64;
        let max_seconds = *recovery_times.iter().max().unwrap() as f64;

        // Calculate median
        let median_seconds = if total_incidents == 1 {
            recovery_times[0] as f64
        } else {
            let mut sorted_times = recovery_times.clone();
            sorted_times.sort_unstable();

            if total_incidents.is_multiple_of(2) {
                let mid = total_incidents / 2;
                (sorted_times[mid - 1] + sorted_times[mid]) as f64 / 2.0
            } else {
                sorted_times[total_incidents / 2] as f64
            }
        };

        MTTRStats {
            total_incidents,
            mean_seconds,
            median_seconds,
            min_seconds,
            max_seconds,
            recovery_times_seconds: recovery_times,
        }
    }
}

impl Default for MTTRStats {
    fn default() -> Self {
        Self {
            total_incidents: 0,
            mean_seconds: 0.0,
            median_seconds: 0.0,
            min_seconds: 0.0,
            max_seconds: 0.0,
            recovery_times_seconds: Vec::new(),
        }
    }
}

/// Aggregated MTTR statistics across multiple alerts
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedMTTRStats {
    /// Overall MTTR statistics
    pub overall: MTTRStats,
    /// Number of alerts included in the calculation
    pub total_alerts: usize,
    /// Number of alerts that had incidents
    pub alerts_with_incidents: usize,
    /// Per-alert breakdown (optional, for detailed analysis)
    pub per_alert_stats: HashMap<String, MTTRStats>,
}

/// Daily MTTR statistics for a specific date
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DailyMTTRStats {
    /// Date in YYYY-MM-DD format
    pub date: NaiveDate,
    /// Aggregated MTTR statistics for this date
    pub stats: AggregatedMTTRStats,
}

/// MTTR history containing array of daily MTTR objects
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MTTRHistory {
    /// Array of daily MTTR statistics
    pub daily_stats: Vec<DailyMTTRStats>,
}

/// Query parameters for MTTR API endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MTTRQueryParams {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

impl AggregatedMTTRStats {
    /// Calculate aggregated MTTR stats from multiple alert state entries
    pub fn from_alert_states(alert_states: Vec<AlertStateEntry>) -> Self {
        let mut all_recovery_times = Vec::new();
        let mut per_alert_stats = HashMap::new();
        let mut alerts_with_incidents = 0;

        for alert_state in &alert_states {
            let alert_stats = alert_state.get_mttr_stats();

            if !alert_stats.is_empty() {
                alerts_with_incidents += 1;
                all_recovery_times.extend(alert_stats.recovery_times_seconds.iter());

                per_alert_stats.insert(alert_state.alert_id.to_string(), alert_stats);
            }
        }

        let overall = MTTRStats::from_recovery_times(all_recovery_times);

        Self {
            overall,
            total_alerts: alert_states.len(),
            alerts_with_incidents,
            per_alert_stats,
        }
    }
}

/// Represents a single state transition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    /// The alert state
    pub state: AlertState,
    /// Timestamp when this state was set/updated
    pub last_updated_at: DateTime<Utc>,
    /// The previous alert state before this transition, if any
    pub previous_alert_state: Option<AlertState>,
    /// Duration in seconds
    pub previous_state_duration: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertStateEntry {
    /// The unique identifier for the alert
    pub alert_id: Ulid,
    pub states: Vec<StateTransition>,
}

impl StateTransition {
    /// Creates a new state transition with the current timestamp
    pub fn new(
        state: AlertState,
        previous_alert_state: Option<AlertState>,
        previous_alert_time: Option<DateTime<Utc>>,
    ) -> Self {
        let now = Utc::now();
        // calculate duration if previous alert time is provided
        let previous_state_duration =
            previous_alert_time.map(|alert_time| (now - alert_time).num_seconds());
        Self {
            state,
            last_updated_at: now,
            previous_alert_state,
            previous_state_duration,
        }
    }
}

impl AlertStateEntry {
    /// Creates a new alert state entry with an initial state
    pub fn new(alert_id: Ulid, initial_state: AlertState) -> Self {
        Self {
            alert_id,
            states: vec![StateTransition::new(initial_state, None, None)],
        }
    }

    /// Updates the state (only adds new entry if state has changed)
    /// Returns true if the state was changed, false if it remained the same
    pub fn update_state(&mut self, new_state: AlertState) -> bool {
        match self.states.last() {
            Some(last_transition) => {
                if last_transition.state != new_state {
                    // State changed - add new transition
                    self.states.push(StateTransition::new(
                        new_state,
                        Some(last_transition.state),
                        Some(last_transition.last_updated_at),
                    ));
                    true
                } else {
                    // If state hasn't changed, do nothing - preserve the original timestamp
                    false
                }
            }
            None => {
                // No previous states - add the first one
                self.states
                    .push(StateTransition::new(new_state, None, None));
                true
            }
        }
    }

    /// Gets the current (latest) state
    pub fn current_state(&self) -> Option<&StateTransition> {
        self.states.last()
    }

    /// Get all recovery times (in seconds) from triggered to not-triggered
    /// Returns recovery times in chronological order
    pub fn get_recovery_times(&self) -> Vec<i64> {
        let mut recovery_times = Vec::new();
        let mut trigger_time: Option<DateTime<Utc>> = None;

        // Create a sorted view without mutating the original
        let mut sorted_states = self.states.clone();
        sorted_states.sort_by(|a, b| a.last_updated_at.cmp(&b.last_updated_at));

        for transition in &sorted_states {
            match transition.state {
                AlertState::Triggered => {
                    // Record when alert was triggered
                    trigger_time = Some(transition.last_updated_at);
                }
                AlertState::NotTriggered => {
                    // If we have a trigger time, calculate recovery time
                    if let Some(triggered_at) = trigger_time {
                        let recovery_duration = transition
                            .last_updated_at
                            .signed_duration_since(triggered_at);
                        let recovery_seconds = recovery_duration.num_seconds();

                        // Only include positive durations (validation against clock issues)
                        if recovery_seconds > 0 {
                            recovery_times.push(recovery_seconds);
                        } else {
                            tracing::warn!(
                                "Negative or zero recovery time detected: {} seconds. Triggered at: {}, Recovered at: {}",
                                recovery_seconds,
                                triggered_at,
                                transition.last_updated_at
                            );
                        }
                        trigger_time = None; // Reset for next cycle
                    }
                }
                AlertState::Disabled => {
                    // Ignore disabled state - it doesn't affect MTTR calculation
                    // until it's explicitly resolved (moves to not-triggered)
                }
            }
        }

        recovery_times
    }

    /// This is the method that is used for MTTR statistics
    pub fn get_mttr_stats(&self) -> MTTRStats {
        let recovery_times = self.get_recovery_times();
        MTTRStats::from_recovery_times(recovery_times)
    }
}

impl MetastoreObject for AlertStateEntry {
    fn get_object_id(&self) -> String {
        self.alert_id.to_string()
    }

    fn get_object_path(&self) -> String {
        alert_state_json_path(self.alert_id).to_string()
    }
}

impl MetastoreObject for AlertConfig {
    fn get_object_id(&self) -> String {
        self.id.to_string()
    }

    fn get_object_path(&self) -> String {
        alert_json_path(self.id).to_string()
    }
}

impl MetastoreObject for MTTRHistory {
    fn get_object_id(&self) -> String {
        "mttr".to_string()
    }

    fn get_object_path(&self) -> String {
        mttr_json_path().to_string()
    }
}
