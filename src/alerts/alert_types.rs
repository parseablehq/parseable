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

use std::{str::FromStr, time::Duration};

use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tonic::async_trait;
use tracing::{info, trace, warn};
use ulid::Ulid;

use crate::{
    alerts::{
        AlertConfig, AlertError, AlertState, AlertType, AlertVersion, EvalConfig, Severity,
        ThresholdConfig,
        alert_enums::NotificationState,
        alert_structs::{AlertStateEntry, GroupResult},
        alert_traits::{AlertTrait, MessageCreation},
        alerts_utils::{evaluate_condition, execute_alert_query, extract_time_range},
        get_number_of_agg_exprs,
        target::{self, NotificationConfig},
    },
    handlers::http::query::create_streams_for_distributed,
    metastore::metastore_traits::MetastoreObject,
    parseable::PARSEABLE,
    query::resolve_stream_names,
    rbac::map::SessionKey,
    storage::object_storage::alert_json_path,
    tenants::TENANT_METADATA,
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
    pub last_triggered_at: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub other_fields: Option<serde_json::Map<String, Value>>,
    pub tenant_id: Option<String>,
}

impl MetastoreObject for ThresholdAlert {
    fn get_object_path(&self) -> String {
        alert_json_path(self.id, &self.tenant_id).to_string()
    }

    fn get_object_id(&self) -> String {
        self.id.to_string()
    }
}

#[async_trait]
impl AlertTrait for ThresholdAlert {
    async fn eval_alert(&self) -> Result<Option<String>, AlertError> {
        let time_range = extract_time_range(&self.eval_config)?;
        let auth = if let Some(tenant) = self.tenant_id.as_ref()
            && let Some(header) = TENANT_METADATA.get_global_query_auth(tenant)
        {
            let mut map = HeaderMap::new();
            map.insert(
                HeaderName::from_static("authorization"),
                HeaderValue::from_str(&header).unwrap(),
            );
            Some(map)
        } else {
            None
        };
        let query_result =
            execute_alert_query(auth, self.get_query(), &time_range, &self.tenant_id).await?;

        if query_result.is_simple_query {
            // Handle simple queries
            let final_value = query_result.get_single_value();
            let result = evaluate_condition(
                &self.threshold_config.operator,
                final_value,
                self.threshold_config.value,
            );

            let message = if result {
                Some(self.create_threshold_message(final_value)?)
            } else {
                None
            };
            Ok(message)
        } else {
            // Handle GROUP BY queries - evaluate each group
            let mut breached_groups = Vec::new();

            for group in &query_result.groups {
                let result = evaluate_condition(
                    &self.threshold_config.operator,
                    group.aggregate_value,
                    self.threshold_config.value,
                );

                if result {
                    breached_groups.push(group.clone());
                }
            }

            let message = if !breached_groups.is_empty() {
                Some(self.create_group_message(&breached_groups)?)
            } else {
                None
            };
            Ok(message)
        }
    }

    async fn validate(&self, session_key: &SessionKey) -> Result<(), AlertError> {
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
            return Err(AlertError::InvalidAlertQuery("Empty query".into()));
        }

        let tables = resolve_stream_names(&self.query)?;
        if tables.is_empty() {
            return Err(AlertError::InvalidAlertQuery(
                "No tables found in query".into(),
            ));
        }
        create_streams_for_distributed(tables, &self.tenant_id)
            .await
            .map_err(|_| AlertError::InvalidAlertQuery("Invalid tables".into()))?;

        // validate that the user has access to the tables mentioned in the query
        user_auth_for_query(session_key, &self.query).await?;

        // validate that the alert query is valid and can be evaluated
        let num_aggrs = get_number_of_agg_exprs(&self.query).await?;
        if num_aggrs != 1 {
            return Err(AlertError::InvalidAlertQuery(format!(
                "Found {num_aggrs} aggregate expressions, only 1 allowed"
            )));
        }
        Ok(())
    }

    async fn update_notification_state(
        &mut self,
        new_notification_state: NotificationState,
    ) -> Result<(), AlertError> {
        // update state in memory
        self.notification_state = new_notification_state;

        // update on disk
        PARSEABLE
            .metastore
            .put_alert(&self.to_alert_config(), &self.tenant_id)
            .await?;
        Ok(())
    }

    async fn update_state(
        &mut self,
        new_state: AlertState,
        trigger_notif: Option<String>,
    ) -> Result<(), AlertError> {
        if self.state.eq(&AlertState::Disabled) {
            warn!(
                "Alert- {} is currently Disabled. Updating state to {new_state}.",
                self.id
            );
            // update state in memory
            self.state = new_state;

            // if new state is `Triggered`, change triggered at
            if new_state.eq(&AlertState::Triggered) {
                self.last_triggered_at = Some(Utc::now());
            }

            // update on disk
            PARSEABLE
                .metastore
                .put_alert(&self.to_alert_config(), &self.tenant_id)
                .await?;
            let state_entry = AlertStateEntry::new(self.id, self.state);
            PARSEABLE
                .metastore
                .put_alert_state(&state_entry as &dyn MetastoreObject, &self.tenant_id)
                .await?;
            return Ok(());
        }

        match &mut self.notification_state {
            NotificationState::Notify => {}
            NotificationState::Mute(till_time) => {
                // if now > till_time, modify notif state to notify and proceed
                let now = Utc::now();
                let till = match till_time.as_str() {
                    "indefinite" => DateTime::<Utc>::MAX_UTC,
                    _ => DateTime::<Utc>::from_str(till_time)
                        .map_err(|e| AlertError::CustomError(e.to_string()))?,
                };
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

        // if new state is `Triggered`, change triggered at
        if new_state.eq(&AlertState::Triggered) {
            self.last_triggered_at = Some(Utc::now());
        }

        // update on disk
        PARSEABLE
            .metastore
            .put_alert(&self.to_alert_config(), &self.tenant_id)
            .await?;
        let state_entry = AlertStateEntry::new(self.id, self.state);

        PARSEABLE
            .metastore
            .put_alert_state(&state_entry as &dyn MetastoreObject, &self.tenant_id)
            .await?;

        if let Some(trigger_notif) = trigger_notif
            && self.notification_state.eq(&NotificationState::Notify)
        {
            trace!("trigger notif on-\n{}", self.state);
            self.to_alert_config()
                .trigger_notifications(trigger_notif)
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

    fn get_targets(&self) -> &[Ulid] {
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

    fn get_eval_window(&self) -> &str {
        match &self.eval_config {
            EvalConfig::RollingWindow(rolling_window) => rolling_window.eval_start.as_str(),
        }
    }

    fn get_created(&self) -> String {
        self.created.to_rfc3339()
    }

    fn get_tags(&self) -> &Option<Vec<String>> {
        &self.tags
    }

    fn get_datasets(&self) -> &[String] {
        &self.datasets
    }

    fn get_tenant_id(&self) -> &Option<String> {
        &self.tenant_id
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
    fn get_message_header(&self) -> Result<String, AlertError> {
        Ok(format!(
            "Alert Name:         {}\nAlert Type:         Threshold alert\nSeverity:           {}\nTriggered at:       {}\nThreshold:          {}\nAlert ID:           {}\nEvaluation Window:  {}\nFrequency:          {}\n\nValues crossing the threshold:",
            self.title,
            self.severity,
            Utc::now().to_rfc3339(),
            format_args!(
                "{} {}",
                self.threshold_config.operator, self.threshold_config.value
            ),
            self.id,
            self.get_eval_window(),
            self.get_eval_frequency()
        ))
    }
    fn create_threshold_message(&self, actual_value: f64) -> Result<String, AlertError> {
        let header = self.get_message_header()?;
        Ok(format!(
            "{header}\nValue: {}\n\nQuery:\n{}",
            actual_value,
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
            last_triggered_at: value.last_triggered_at,
            other_fields: value.other_fields,
            tenant_id: value.tenant_id,
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
            last_triggered_at: val.last_triggered_at,
            other_fields: val.other_fields,
            tenant_id: val.tenant_id,
        }
    }
}

impl ThresholdAlert {
    fn create_group_message(&self, breached_groups: &[GroupResult]) -> Result<String, AlertError> {
        let header = self.get_message_header()?;
        let mut message = format!("{header}\n");

        message.push_str(&format!(
            "Alerting Groups ({} total):\n",
            breached_groups.len()
        ));

        for (index, group) in breached_groups.iter().enumerate() {
            message.push_str(&format!("{}. ", index + 1));

            if group.group_values.is_empty() {
                message.push_str("[No GROUP BY]");
            } else {
                let group_desc = group
                    .group_values
                    .iter()
                    .map(|(key, value)| format!("{}: {}", key, value))
                    .collect::<Vec<_>>()
                    .join(", ");
                message.push_str(&group_desc);
            }

            message.push_str(&format!(" → Value: {}\n", group.aggregate_value));
        }

        message.push_str(&format!("\nQuery:\n{}", self.get_query()));

        Ok(message)
    }
}
