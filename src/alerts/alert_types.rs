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

use std::time::Duration;

use chrono::{DateTime, Utc};
use tonic::async_trait;
use ulid::Ulid;

use crate::{
    alerts::{
        AlertConfig, AlertError, AlertState, AlertType, AlertVerison, EvalConfig, Severity,
        ThresholdConfig,
        alerts_utils::{evaluate_condition, execute_alert_query, extract_time_range},
        is_query_aggregate,
        target::{self, TARGETS},
        traits::AlertTrait,
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
    pub datasets: Vec<String>,
}

#[async_trait]
impl AlertTrait for ThresholdAlert {
    async fn eval_alert(&self) -> Result<(bool, f64), AlertError> {
        let time_range = extract_time_range(&self.eval_config)?;
        let final_value = execute_alert_query(self.get_query(), &time_range).await?;
        let result = evaluate_condition(
            &self.threshold_config.operator,
            final_value,
            self.threshold_config.value,
        );
        Ok((result, final_value))
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
        user_auth_for_query(session_key, &self.query).await?;

        // validate that the alert query is valid and can be evaluated
        if !is_query_aggregate(&self.query).await? {
            return Err(AlertError::InvalidAlertQuery);
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

    fn set_state(&mut self, new_state: AlertState) {
        self.state = new_state
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
            created: val.created,
            tags: val.tags,
            datasets: val.datasets,
        }
    }
}
