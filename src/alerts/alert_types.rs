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

use actix_web::http::header::{HeaderName, HeaderValue};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde_json::Value;
use tonic::async_trait;
use tracing::{info, trace, warn};
use ulid::Ulid;

use crate::{
    alerts::{
        AlertConfig, AlertError, AlertQueryType, AlertState, AlertType, AlertVersion, EvalConfig,
        Severity, ThresholdConfig,
        alert_enums::NotificationState,
        alert_structs::{AlertStateEntry, GroupResult},
        alert_traits::{AlertTrait, MessageCreation},
        alerts_utils::{evaluate_condition, execute_alert_query, extract_time_range},
        get_number_of_agg_exprs,
        target::{self, NotificationConfig},
    },
    handlers::http::{
        middleware::{CLUSTER_SECRET, CLUSTER_SECRET_HEADER},
        query::create_streams_for_distributed,
    },
    metastore::metastore_traits::MetastoreObject,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    query::resolve_stream_names,
    rbac::map::SessionKey,
    storage::object_storage::alert_json_path,
    tenants::TENANT_METADATA,
    utils::user_auth_for_query,
};

/// What this process last wrote for each alert, keyed by tenant and alert id.
///
/// An evaluation that leaves an alert as it was (a quiet alert staying
/// `NotTriggered`) used to rewrite its config and state objects every time.
/// A write is skipped only when this process itself wrote exactly the same
/// bytes (or state) for the alert, successfully, within `PERSIST_REFRESH`.
/// The record is dropped before every write and set only after it succeeds,
/// so a failed or partial write is never mistaken for a persisted one; it is
/// empty after a restart, so the first evaluation always writes; and every
/// write is repeated at least every `PERSIST_REFRESH`, which bounds how long
/// an object changed behind this process's back can go uncorrected. Other
/// writers of an alert's objects call `forget_persisted_alert`.
static PERSISTED_ALERTS: Lazy<DashMap<(String, Ulid), PersistedAlert>> = Lazy::new(DashMap::new);
const PERSIST_REFRESH: chrono::TimeDelta = chrono::TimeDelta::minutes(15);

#[derive(Debug, Default, Clone)]
struct PersistedAlert {
    config: Option<(Vec<u8>, DateTime<Utc>)>,
    state: Option<(AlertState, DateTime<Utc>)>,
}

impl PersistedAlert {
    fn is_fresh(at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        let elapsed = now - at;
        elapsed >= chrono::TimeDelta::zero() && elapsed < PERSIST_REFRESH
    }

    fn has_config(&self, bytes: &[u8], now: DateTime<Utc>) -> bool {
        self.config
            .as_ref()
            .is_some_and(|(written, at)| written == bytes && Self::is_fresh(*at, now))
    }

    fn has_state(&self, state: AlertState, now: DateTime<Utc>) -> bool {
        self.state
            .as_ref()
            .is_some_and(|(written, at)| *written == state && Self::is_fresh(*at, now))
    }
}

fn persisted_key(id: Ulid, tenant_id: &Option<String>) -> (String, Ulid) {
    (
        tenant_id.as_deref().unwrap_or(DEFAULT_TENANT).to_owned(),
        id,
    )
}

/// For code that writes an alert's config or state objects other than
/// through `ThresholdAlert`: the next state update writes both again.
pub(crate) fn forget_persisted_alert(id: Ulid, tenant_id: &Option<String>) {
    PERSISTED_ALERTS.remove(&persisted_key(id, tenant_id));
}

/// Whether the config serialized as `bytes` was written by this process
/// within the refresh window. Never holds a map guard past the call.
fn config_already_persisted(key: &(String, Ulid), bytes: &[u8], now: DateTime<Utc>) -> bool {
    PERSISTED_ALERTS
        .get(key)
        .is_some_and(|persisted| persisted.has_config(bytes, now))
}

fn state_already_persisted(key: &(String, Ulid), state: AlertState, now: DateTime<Utc>) -> bool {
    PERSISTED_ALERTS
        .get(key)
        .is_some_and(|persisted| persisted.has_state(state, now))
}

fn set_persisted_config(key: (String, Ulid), written: Option<(Vec<u8>, DateTime<Utc>)>) {
    PERSISTED_ALERTS.entry(key).or_default().config = written;
}

fn set_persisted_state(key: (String, Ulid), written: Option<(AlertState, DateTime<Utc>)>) {
    PERSISTED_ALERTS.entry(key).or_default().state = written;
}

/// Struct which defines the threshold type alerts
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ThresholdAlert {
    pub version: AlertVersion,
    #[serde(default)]
    pub id: Ulid,
    pub severity: Severity,
    pub title: String,
    pub query: String,
    #[serde(default)]
    pub query_type: AlertQueryType,
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
        self.validate_oss_query_type()?;
        if self.query_type == AlertQueryType::Promql {
            super::promql_alerts::authorize_execution(&self.to_alert_config())
                .map_err(AlertError::InvalidAlertQuery)?;
            let samples = crate::handlers::http::promql::execute_alert_instant(
                &self.datasets[0],
                &self.query,
                &self.tenant_id,
            )
            .await
            .map_err(|e| AlertError::InvalidAlertQuery(e.to_string()))?;
            return samples
                .into_iter()
                .find(|(_, value)| {
                    evaluate_condition(
                        &self.threshold_config.operator,
                        *value,
                        self.threshold_config.value,
                    )
                })
                .map(|(_, value)| self.create_threshold_message(value))
                .transpose();
        }

        let time_range = extract_time_range(&self.eval_config)?;

        let tenant = self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let auth = if let Some((_, hash)) = CLUSTER_SECRET.get() {
            let mut map = actix_web::http::header::HeaderMap::new();
            if let Some(header) = TENANT_METADATA.get_global_query_auth(tenant) {
                map.insert(
                    HeaderName::from_static("authorization"),
                    HeaderValue::from_str(&header).unwrap(),
                );
            }
            map.insert(
                HeaderName::from_static(CLUSTER_SECRET_HEADER),
                HeaderValue::from_str(hash).unwrap(),
            );
            map.insert(
                HeaderName::from_static("intra-cluster-tenant"),
                HeaderValue::from_str(tenant).unwrap(),
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

        self.validate_oss_query_type()?;

        if self.query_type == AlertQueryType::Promql {
            super::user_auth_for_alert_config(session_key, &self.to_alert_config()).await?;
            if self.get_eval_frequency() == 0 || self.get_eval_frequency() > 1440 {
                return Err(AlertError::ValidationFailure(
                    "PromQL evaluation frequency must be between 1 and 1440 minutes".into(),
                ));
            }
            if !self.threshold_config.value.is_finite() {
                return Err(AlertError::ValidationFailure(
                    "Threshold must be finite".into(),
                ));
            }
            if self.targets.len() > 20 {
                return Err(AlertError::ValidationFailure(
                    "PromQL alerts support at most 20 notification targets".into(),
                ));
            }
            super::promql_alerts::hold_duration(&self.to_alert_config())?;
            crate::handlers::http::promql::validate_alert_expression(&self.query)
                .map_err(AlertError::InvalidAlertQuery)?;
            return Ok(());
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
        let num_aggrs = get_number_of_agg_exprs(&self.query, &self.tenant_id).await?;
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

        // update on disk; an explicit change is always written
        self.persist_config(true).await
    }

    async fn update_state(
        &mut self,
        new_state: AlertState,
        trigger_notif: Option<String>,
    ) -> Result<(), AlertError> {
        if self.query_type == AlertQueryType::Promql && new_state == AlertState::Disabled {
            super::promql_alerts::reset_runtime_for_disable(&mut self.other_fields);
        }
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

            // update on disk; leaving `Disabled` is always a change
            self.persist_config(true).await?;
            self.persist_state(true).await?;
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

        // update on disk, unless this process already wrote exactly this
        // config and state (an evaluation that changed nothing)
        self.persist_config(false).await?;
        self.persist_state(false).await?;

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

    fn get_query_type(&self) -> AlertQueryType {
        self.query_type
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
            query_type: value.query_type,
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
            query_type: val.query_type,
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
    /// Writes the alert's config, skipped (unless `force`) when this process
    /// already wrote the same bytes recently; see `PERSISTED_ALERTS`.
    async fn persist_config(&self, force: bool) -> Result<(), AlertError> {
        let config = self.to_alert_config();
        let key = persisted_key(self.id, &self.tenant_id);
        let bytes = serde_json::to_vec(&config)?;
        let now = Utc::now();
        if !force && config_already_persisted(&key, &bytes, now) {
            return Ok(());
        }
        // A write that fails may still have landed, so it is not recorded.
        set_persisted_config(key.clone(), None);
        PARSEABLE
            .metastore
            .put_alert(&config, &self.tenant_id)
            .await?;
        set_persisted_config(key, Some((bytes, now)));
        Ok(())
    }

    /// Records the alert's current state, skipped (unless `force`) when this
    /// process already recorded the same state recently.
    async fn persist_state(&self, force: bool) -> Result<(), AlertError> {
        let key = persisted_key(self.id, &self.tenant_id);
        let now = Utc::now();
        if !force && state_already_persisted(&key, self.state, now) {
            return Ok(());
        }
        set_persisted_state(key.clone(), None);
        let state_entry = AlertStateEntry::new(self.id, self.state, self.tenant_id.clone());
        PARSEABLE
            .metastore
            .put_alert_state(&state_entry as &dyn MetastoreObject, &self.tenant_id)
            .await?;
        set_persisted_state(key, Some((self.state, now)));
        Ok(())
    }

    pub(crate) fn validate_oss_query_type(&self) -> Result<(), AlertError> {
        if self.query_type == AlertQueryType::Promql
            && !matches!(
                PARSEABLE.options.mode,
                crate::option::Mode::All | crate::option::Mode::Query
            )
        {
            return Err(AlertError::ValidationFailure(
                "PromQL alerts require All or Query deployment mode".into(),
            ));
        }

        Ok(())
    }

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

#[cfg(test)]
mod persist_tests {
    use super::*;

    fn alert() -> ThresholdAlert {
        let config: AlertConfig = serde_json::from_value(serde_json::json!({
            "version":"v2","id":Ulid::new(),"severity":"high","title":"Quiet alert",
            "queryType":"sql","query":"SELECT COUNT(*) FROM logs","datasets":["logs"],
            "alertType":"threshold","thresholdConfig":{"operator":">","value":8.0},
            "evalConfig":{"rollingWindow":{"evalStart":"5m","evalEnd":"now","evalFrequency":1}},
            "targets":[],"notificationState":"notify","notificationConfig":{"interval":1},
            "created":Utc::now(),"tenantId":null
        }))
        .unwrap();
        ThresholdAlert::from(config)
    }

    #[test]
    fn records_expire_and_match_only_identical_content() {
        let now = Utc::now();
        let persisted = PersistedAlert {
            config: Some((b"config".to_vec(), now)),
            state: Some((AlertState::NotTriggered, now)),
        };
        assert!(persisted.has_config(b"config", now));
        assert!(!persisted.has_config(b"changed", now));
        assert!(persisted.has_state(AlertState::NotTriggered, now));
        assert!(!persisted.has_state(AlertState::Triggered, now));
        // Rewritten at least once per refresh window.
        let later = now + PERSIST_REFRESH;
        assert!(!persisted.has_config(b"config", later));
        assert!(!persisted.has_state(AlertState::NotTriggered, later));
        assert!(!PersistedAlert::default().has_config(b"config", now));
        assert!(!PersistedAlert::default().has_state(AlertState::NotTriggered, now));
    }

    async fn stored_config(alert: &ThresholdAlert) -> Option<AlertConfig> {
        PARSEABLE
            .metastore
            .get_alerts()
            .await
            .unwrap()
            .into_values()
            .flatten()
            .filter_map(|bytes| serde_json::from_slice::<AlertConfig>(&bytes).ok())
            .find(|config| config.id == alert.id)
    }

    async fn stored_state(alert: &ThresholdAlert) -> Option<AlertState> {
        PARSEABLE
            .metastore
            .get_alert_state_entry(&alert.id, &alert.tenant_id)
            .await
            .unwrap()
            .and_then(|entry| entry.current_state().map(|transition| transition.state))
    }

    /// Removes both stored objects, so a later write shows up as their
    /// return. Either may already be gone.
    async fn erase(alert: &ThresholdAlert) {
        let config = alert.to_alert_config();
        let _ = PARSEABLE
            .metastore
            .delete_alert(&config, &alert.tenant_id)
            .await;
        let state = AlertStateEntry::new(alert.id, alert.state, alert.tenant_id.clone());
        let _ = PARSEABLE
            .metastore
            .delete_alert_state(&state, &alert.tenant_id)
            .await;
        assert!(stored_config(alert).await.is_none());
        assert!(stored_state(alert).await.is_none());
    }

    #[actix_web::test]
    async fn unchanged_state_updates_skip_rewrites_until_something_changes() {
        crate::parseable::install_default_test_parseable();
        let mut alert = alert();

        // The first update after start always writes.
        alert
            .update_state(AlertState::NotTriggered, None)
            .await
            .unwrap();
        assert_eq!(
            stored_config(&alert).await.map(|config| config.state),
            Some(AlertState::NotTriggered)
        );
        assert_eq!(stored_state(&alert).await, Some(AlertState::NotTriggered));

        // An evaluation that changes nothing writes nothing.
        erase(&alert).await;
        alert
            .update_state(AlertState::NotTriggered, None)
            .await
            .unwrap();
        assert!(stored_config(&alert).await.is_none());
        assert!(stored_state(&alert).await.is_none());

        // Another writer forgetting the record makes the next update write.
        forget_persisted_alert(alert.id, &alert.tenant_id);
        alert
            .update_state(AlertState::NotTriggered, None)
            .await
            .unwrap();
        assert!(stored_config(&alert).await.is_some());
        assert_eq!(stored_state(&alert).await, Some(AlertState::NotTriggered));

        // A state change is written.
        alert
            .update_state(AlertState::Triggered, None)
            .await
            .unwrap();
        let triggered_at = alert.last_triggered_at;
        assert!(triggered_at.is_some());
        let stored = stored_config(&alert).await.unwrap();
        assert_eq!(stored.state, AlertState::Triggered);
        assert_eq!(stored.last_triggered_at, triggered_at);
        assert_eq!(stored_state(&alert).await, Some(AlertState::Triggered));

        // Staying triggered moves `last_triggered_at`, so the config is
        // written again while the unchanged state is not.
        erase(&alert).await;
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        alert
            .update_state(AlertState::Triggered, None)
            .await
            .unwrap();
        let stored = stored_config(&alert).await.unwrap();
        assert_ne!(stored.last_triggered_at, triggered_at);
        assert_eq!(stored.last_triggered_at, alert.last_triggered_at);
        assert!(stored_state(&alert).await.is_none());

        // An expired snooze is lifted and written, though the state stays.
        alert
            .update_state(AlertState::NotTriggered, None)
            .await
            .unwrap();
        let expired = (Utc::now() - chrono::TimeDelta::seconds(1)).to_rfc3339();
        alert
            .update_notification_state(NotificationState::Mute(expired))
            .await
            .unwrap();
        erase(&alert).await;
        alert
            .update_state(AlertState::NotTriggered, None)
            .await
            .unwrap();
        assert_eq!(alert.notification_state, NotificationState::Notify);
        let stored = stored_config(&alert).await.unwrap();
        assert_eq!(stored.notification_state, NotificationState::Notify);
        assert!(stored_state(&alert).await.is_none());

        // An explicit notification change is always written, even when it
        // matches what was last written.
        erase(&alert).await;
        alert
            .update_notification_state(NotificationState::Notify)
            .await
            .unwrap();
        assert!(stored_config(&alert).await.is_some());

        // Leaving `Disabled` is always written.
        erase(&alert).await;
        alert.state = AlertState::Disabled;
        alert
            .update_state(AlertState::NotTriggered, None)
            .await
            .unwrap();
        assert_eq!(
            stored_config(&alert).await.map(|config| config.state),
            Some(AlertState::NotTriggered)
        );
        assert_eq!(stored_state(&alert).await, Some(AlertState::NotTriggered));
    }
}
