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

use std::{str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use tonic::async_trait;
use tracing::{info, trace, warn};
use ulid::Ulid;

use crate::{
    alerts::{
        AlertConfig, AlertError, AlertState, AlertType, AlertVersion, EvalConfig, Severity,
        ThresholdConfig,
        alert_enums::NotificationState,
        alert_traits::{AlertTrait, MessageCreation},
        alerts_utils::{evaluate_condition, execute_alert_query, extract_time_range},
        is_query_aggregate,
        target::{self, NotificationConfig},
    },
    handlers::http::query::create_streams_for_distributed,
    option::Mode,
    parseable::PARSEABLE,
    query::resolve_stream_names,
    rbac::map::SessionKey,
    utils::user_auth_for_query,
};

/// Struct which defines the threshold type alerts
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ThresholdAlert {
    pub version: AlertVersion,
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
    pub notification_state: NotificationState,
    pub notification_config: NotificationConfig,
    pub created: DateTime<Utc>,
    pub tags: Option<Vec<String>>,
    pub datasets: Vec<String>,
}

#[async_trait]
impl AlertTrait for ThresholdAlert {
    async fn eval_alert(&self) -> Result<Option<String>, AlertError> {
        let time_range = extract_time_range(&self.eval_config)?;
        let final_value = execute_alert_query(self.get_query(), &time_range).await?;
        let result = evaluate_condition(
            &self.threshold_config.operator,
            final_value,
            self.threshold_config.value,
        );

        let message = if result {
            // generate message
            Some(self.create_threshold_message(final_value)?)
        } else {
            None
        };
        Ok(message)
    }

    async fn validate(&self, session_key: &SessionKey) -> Result<(), AlertError> {
        // validate alert type
        // Anomaly is only allowed in Prism
        if self.alert_type.eq(&AlertType::Anomaly) && PARSEABLE.options.mode != Mode::Prism {
            return Err(AlertError::CustomError(
                "Anomaly alert is only allowed on Prism mode".into(),
            ));
        }

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
        match &self.notification_config.times {
            target::Retry::Infinite => {}
            target::Retry::Finite(repeat) => {
                let notif_duration =
                    Duration::from_secs(60 * self.notification_config.interval) * *repeat as u32;
                if (notif_duration.as_secs_f64()).gt(&((eval_frequency * 60) as f64)) {
                    return Err(AlertError::Metadata(
                        "evalFrequency should be greater than target repetition  interval",
                    ));
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
        user_auth_for_query(session_key, &self.query).await?;

        // validate that the alert query is valid and can be evaluated
        if !is_query_aggregate(&self.query).await? {
            return Err(AlertError::InvalidAlertQuery);
        }
        Ok(())
    }

    async fn update_notification_state(
        &mut self,
        new_notification_state: NotificationState,
    ) -> Result<(), AlertError> {
        let store = PARSEABLE.storage.get_object_store();
        // update state in memory
        self.notification_state = new_notification_state;
        // update on disk
        store.put_alert(self.id, &self.to_alert_config()).await?;

        Ok(())
    }

    async fn update_state(
        &mut self,
        new_state: AlertState,
        trigger_notif: Option<String>,
    ) -> Result<(), AlertError> {
        let store = PARSEABLE.storage.get_object_store();
        if self.state.eq(&AlertState::Paused) {
            warn!(
                "Alert- {} has been Paused. No evals will be done till it is unpaused.",
                self.id
            );
            // update state in memory
            self.state = new_state;

            // update on disk
            store.put_alert(self.id, &self.to_alert_config()).await?;
            // The task should have already been removed from the list of running tasks
            return Ok(());
        }

        match &mut self.notification_state {
            NotificationState::Notify => {}
            NotificationState::Snoozed(till_time) => {
                // if now > till_time, modify notif state to notify and proceed
                let now = Utc::now();
                let till = DateTime::<Utc>::from_str(till_time)
                    .map_err(|e| AlertError::CustomError(e.to_string()))?;
                if now > till {
                    info!(
                        "Modifying alert notif state from snoozed to notify- Now= {now}, Snooze till= {till}"
                    );
                    self.notification_state = NotificationState::Notify;
                }
            }
        }

        // update state in memory
        self.state = new_state;

        // update on disk
        store.put_alert(self.id, &self.to_alert_config()).await?;

        if trigger_notif.is_some() && self.notification_state.eq(&NotificationState::Notify) {
            trace!("trigger notif on-\n{}", self.state);
            self.to_alert_config()
                .trigger_notifications(trigger_notif.unwrap())
                .await?;
        }
        Ok(())
    }

    fn get_id(&self) -> &Ulid {
        &self.id
    }

    fn get_query(&self) -> &str {
        &self.query
    }

    fn get_severity(&self) -> &Severity {
        &self.severity
    }

    fn get_title(&self) -> &str {
        &self.title
    }

    fn get_alert_type(&self) -> &AlertType {
        &self.alert_type
    }

    fn get_threshold_config(&self) -> &ThresholdConfig {
        &self.threshold_config
    }

    fn get_eval_config(&self) -> &EvalConfig {
        &self.eval_config
    }

    fn get_targets(&self) -> &Vec<Ulid> {
        &self.targets
    }

    fn get_state(&self) -> &AlertState {
        &self.state
    }

    fn get_eval_frequency(&self) -> u64 {
        match &self.eval_config {
            EvalConfig::RollingWindow(rolling_window) => rolling_window.eval_frequency,
        }
    }

    fn get_eval_window(&self) -> String {
        match &self.eval_config {
            EvalConfig::RollingWindow(rolling_window) => rolling_window.eval_start.clone(),
        }
    }

    fn get_created(&self) -> String {
        self.created.to_string()
    }

    fn get_tags(&self) -> &Option<Vec<String>> {
        &self.tags
    }

    fn get_datasets(&self) -> &Vec<String> {
        &self.datasets
    }

    fn to_alert_config(&self) -> AlertConfig {
        let clone = self.clone();
        clone.into()
    }

    fn clone_box(&self) -> Box<dyn AlertTrait> {
        Box::new(self.clone())
    }
}

impl MessageCreation for ThresholdAlert {
    fn create_threshold_message(&self, actual_value: f64) -> Result<String, AlertError> {
        Ok(format!(
            "Alert Triggered: {}\n\nThreshold: ({} {})\nCurrent Value: {}\nEvaluation Window: {} | Frequency: {}\n\nQuery:\n{}",
            self.get_id(),
            self.get_threshold_config().operator,
            self.get_threshold_config().value,
            actual_value,
            self.get_eval_window(),
            self.get_eval_frequency(),
            self.get_query()
        ))
    }

    fn create_anomaly_message(
        &self,
        _forecast_value: f64,
        _lower_bound: f64,
        _upper_bound: f64,
    ) -> Result<String, AlertError> {
        Err(AlertError::Unimplemented(
            "Anomaly message creation is not allowed for Threshold alert".into(),
        ))
    }

    fn create_forecast_message(
        &self,
        _forecast_time: DateTime<Utc>,
        _forecast_value: f64,
    ) -> Result<String, AlertError> {
        Err(AlertError::Unimplemented(
            "Forecast message creation is not allowed for Threshold alert".into(),
        ))
    }
}

impl From<AlertConfig> for ThresholdAlert {
    fn from(value: AlertConfig) -> Self {
        Self {
            version: value.version,
            id: value.id,
            severity: value.severity,
            title: value.title,
            query: value.query,
            alert_type: value.alert_type,
            threshold_config: value.threshold_config,
            eval_config: value.eval_config,
            targets: value.targets,
            state: value.state,
            notification_state: value.notification_state,
            notification_config: value.notification_config,
            created: value.created,
            tags: value.tags,
            datasets: value.datasets,
        }
    }
}

impl From<ThresholdAlert> for AlertConfig {
    fn from(val: ThresholdAlert) -> Self {
        AlertConfig {
            version: val.version,
            id: val.id,
            severity: val.severity,
            title: val.title,
            query: val.query,
            alert_type: val.alert_type,
            threshold_config: val.threshold_config,
            eval_config: val.eval_config,
            targets: val.targets,
            state: val.state,
            notification_state: val.notification_state,
            notification_config: val.notification_config,
            created: val.created,
            tags: val.tags,
            datasets: val.datasets,
        }
    }
}
