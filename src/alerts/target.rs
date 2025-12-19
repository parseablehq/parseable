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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
use itertools::Itertools;
use once_cell::sync::Lazy;
use reqwest::ClientBuilder;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing::{error, trace, warn};
use ulid::Ulid;
use url::Url;

use crate::{
    alerts::{AlertError, AlertState, Context, alert_traits::CallableTarget},
    metastore::metastore_traits::MetastoreObject,
    parseable::PARSEABLE,
    storage::object_storage::target_json_path,
};

use super::ALERTS;

pub static TARGETS: Lazy<TargetConfigs> = Lazy::new(|| TargetConfigs {
    target_configs: RwLock::new(HashMap::new()),
});

#[derive(Debug)]
pub struct TargetConfigs {
    pub target_configs: RwLock<HashMap<Ulid, Target>>,
}

impl TargetConfigs {
    /// Loads alerts from disk, blocks
    pub async fn load(&self) -> anyhow::Result<()> {
        let targets = PARSEABLE.metastore.get_targets().await?;
        let mut map = self.target_configs.write().await;
        for target in targets {
            map.insert(target.id, target);
        }

        Ok(())
    }

    pub async fn update(&self, target: Target) -> Result<(), AlertError> {
        PARSEABLE.metastore.put_target(&target).await?;
        let mut map = self.target_configs.write().await;
        map.insert(target.id, target.clone());
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<Target>, AlertError> {
        let targets = self
            .target_configs
            .read()
            .await
            .values()
            .cloned()
            .collect_vec();
        Ok(targets)
    }

    pub async fn get_target_by_id(&self, target_id: &Ulid) -> Result<Target, AlertError> {
        let target = self
            .target_configs
            .read()
            .await
            .get(target_id)
            .ok_or(AlertError::InvalidTargetID(target_id.to_string()))
            .cloned()?;

        Ok(target)
    }

    pub async fn delete(&self, target_id: &Ulid) -> Result<Target, AlertError> {
        // ensure that the target is not being used by any alert
        let guard = ALERTS.read().await;
        let alerts = if let Some(alerts) = guard.as_ref() {
            alerts
        } else {
            return Err(AlertError::CustomError("No AlertManager set".into()));
        };

        for (_, alert) in alerts.get_all_alerts().await.iter() {
            if alert.get_targets().contains(target_id) {
                return Err(AlertError::TargetInUse);
            }
        }
        let target = self
            .target_configs
            .write()
            .await
            .remove(target_id)
            .ok_or(AlertError::InvalidTargetID(target_id.to_string()))?;
        PARSEABLE.metastore.delete_target(&target).await?;
        Ok(target)
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum Retry {
    Infinite,
    Finite(usize),
}

impl Default for Retry {
    fn default() -> Self {
        Retry::Finite(1)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(try_from = "TargetVerifier")]
pub struct Target {
    pub name: String,
    #[serde(flatten)]
    pub target: TargetType,
    #[serde(default = "Ulid::new")]
    pub id: Ulid,
}

impl Target {
    pub fn mask(self) -> Value {
        match self.target {
            TargetType::Slack(slack_web_hook) => {
                let endpoint = slack_web_hook.endpoint.to_string();
                let masked_endpoint = if endpoint.len() > 20 {
                    format!("{}********", &endpoint[..20])
                } else {
                    "********".to_string()
                };
                json!({
                   "name":self.name,
                   "type":"slack",
                   "endpoint":masked_endpoint,
                   "id":self.id
                })
            }
            TargetType::Other(other_web_hook) => {
                let endpoint = other_web_hook.endpoint.to_string();
                let masked_endpoint = if endpoint.len() > 20 {
                    format!("{}********", &endpoint[..20])
                } else {
                    "********".to_string()
                };
                json!({
                    "name":self.name,
                    "type":"webhook",
                    "endpoint":masked_endpoint,
                    "headers":other_web_hook.headers,
                    "skipTlsCheck":other_web_hook.skip_tls_check,
                    "id":self.id
                })
            }
            TargetType::AlertManager(alert_manager) => {
                let endpoint = alert_manager.endpoint.to_string();
                let masked_endpoint = if endpoint.len() > 20 {
                    format!("{}********", &endpoint[..20])
                } else {
                    "********".to_string()
                };
                if let Some(auth) = alert_manager.auth {
                    let password = "********";
                    json!({
                        "name":self.name,
                        "type":"webhook",
                        "endpoint":masked_endpoint,
                        "username":auth.username,
                        "password":password,
                        "skipTlsCheck":alert_manager.skip_tls_check,
                        "id":self.id
                    })
                } else {
                    json!({
                        "name":self.name,
                        "type":"webhook",
                        "endpoint":masked_endpoint,
                        "username":Value::Null,
                        "password":Value::Null,
                        "skipTlsCheck":alert_manager.skip_tls_check,
                        "id":self.id
                    })
                }
            }
        }
    }

    pub fn call(&self, context: Context) {
        trace!("target.call context- {context:?}");
        let timeout = context.notification_config.clone();
        let resolves = context.alert_info.alert_state;
        let mut state = timeout.state.lock().unwrap();
        trace!("target.call state- {state:?}");
        state.alert_state = resolves;

        match resolves {
            AlertState::Triggered => {
                if !state.timed_out {
                    // call once and then start sleeping
                    // reduce repeats by 1
                    call_target(self.target.clone(), context.clone());
                    // set state
                    state.timed_out = true;
                    state.awaiting_resolve = true;
                    drop(state);
                    self.spawn_timeout_task(&timeout, context.clone());
                }
            }
            alert_state @ AlertState::NotTriggered => {
                state.alert_state = alert_state;
                if state.timed_out {
                    // if in timeout and resolve came in, only process if it's the first one ( awaiting resolve )
                    if state.awaiting_resolve {
                        state.awaiting_resolve = false;
                    } else {
                        // no further resolve will be considered in timeout period
                        return;
                    }
                }

                call_target(self.target.clone(), context);
            }
            // do not send out any notifs
            // (an eval should not have run!)
            AlertState::Disabled => {}
        }
    }

    fn spawn_timeout_task(&self, target_timeout: &NotificationConfig, alert_context: Context) {
        trace!("repeat-\n{target_timeout:?}");
        let state = Arc::clone(&target_timeout.state);
        let retry = target_timeout.times;
        let timeout = target_timeout.interval;
        let target = self.target.clone();
        let alert_id = alert_context.alert_info.alert_id;

        let sleep_and_check_if_call =
            move |timeout_state: Arc<Mutex<TimeoutState>>, current_state: AlertState| async move {
                tokio::time::sleep(Duration::from_secs(timeout * 60)).await;

                let mut state = timeout_state.lock().unwrap();

                if current_state == AlertState::Triggered {
                    state.awaiting_resolve = true;
                    true
                } else {
                    state.timed_out = false;
                    false
                }
            };

        trace!("Spawning retry task");
        tokio::spawn(async move {
            // Get alerts manager reference once at the start
            let alerts = {
                let guard = ALERTS.read().await;
                if let Some(alerts) = guard.as_ref() {
                    alerts.clone()
                } else {
                    error!("No AlertManager set for alert_id: {alert_id}, stopping timeout task");
                    *state.lock().unwrap() = TimeoutState::default();
                    return;
                }
            }; // Lock released immediately

            match retry {
                Retry::Infinite => loop {
                    let current_state = if let Ok(state) = alerts.get_state(alert_id).await {
                        state
                    } else {
                        *state.lock().unwrap() = TimeoutState::default();
                        warn!(
                            "Unable to fetch state for given alert_id- {alert_id}, stopping target notifs"
                        );
                        return;
                    };

                    let should_call =
                        sleep_and_check_if_call(Arc::clone(&state), current_state).await;
                    if should_call {
                        call_target(target.clone(), alert_context.clone())
                    }
                },
                Retry::Finite(times) => {
                    for _ in 0..(times - 1) {
                        let current_state = if let Ok(state) = alerts.get_state(alert_id).await {
                            state
                        } else {
                            *state.lock().unwrap() = TimeoutState::default();
                            warn!(
                                "Unable to fetch state for given alert_id- {alert_id}, stopping target notifs"
                            );
                            return;
                        };

                        let should_call =
                            sleep_and_check_if_call(Arc::clone(&state), current_state).await;
                        if should_call {
                            call_target(target.clone(), alert_context.clone())
                        }
                    }
                }
            }
            *state.lock().unwrap() = TimeoutState::default();
        });
    }
}

impl MetastoreObject for Target {
    fn get_object_path(&self) -> String {
        target_json_path(&self.id).to_string()
    }

    fn get_object_id(&self) -> String {
        self.id.to_string()
    }
}

fn call_target(target: TargetType, context: Context) {
    trace!("Calling target with context- {context:?}");
    tokio::spawn(async move { target.call(&context).await });
}

#[derive(Debug, serde::Deserialize)]
pub struct NotificationConfigVerifier {
    interval: Option<u64>,
    times: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetVerifier {
    pub name: String,
    #[serde(flatten)]
    pub target: TargetType,
    #[serde(default)]
    pub notification_config: Option<NotificationConfigVerifier>,
    #[serde(default = "Ulid::new")]
    pub id: Ulid,
}

impl TryFrom<TargetVerifier> for Target {
    type Error = String;

    fn try_from(value: TargetVerifier) -> Result<Self, Self::Error> {
        let mut timeout = NotificationConfig::default();

        // Default is Infinite in case of alertmanager
        if matches!(value.target, TargetType::AlertManager(_)) {
            timeout.times = Retry::Infinite
        }

        if let Some(notification_config) = value.notification_config {
            let interval = notification_config.interval.map(|ref interval| *interval);

            if let Some(interval) = interval {
                timeout.interval = interval
            }

            if let Some(times) = notification_config.times {
                timeout.times = Retry::Finite(times)
            }
        }

        Ok(Target {
            name: value.name,
            target: value.target,
            id: value.id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
#[serde(deny_unknown_fields)]
pub enum TargetType {
    #[serde(rename = "slack")]
    Slack(SlackWebHook),
    #[serde(rename = "webhook")]
    Other(OtherWebHook),
    #[serde(rename = "alertManager")]
    AlertManager(AlertManager),
}

impl TargetType {
    pub async fn call(&self, payload: &Context) {
        match self {
            TargetType::Slack(target) => target.call(payload).await,
            TargetType::Other(target) => target.call(payload).await,
            TargetType::AlertManager(target) => target.call(payload).await,
        }
    }
}

fn default_client_builder() -> ClientBuilder {
    ClientBuilder::new()
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlackWebHook {
    endpoint: Url,
}

#[async_trait]
impl CallableTarget for SlackWebHook {
    async fn call(&self, payload: &Context) {
        let client = default_client_builder()
            .build()
            .expect("Client can be constructed on this system");

        let alert = match payload.alert_info.alert_state {
            AlertState::Triggered => {
                serde_json::json!({ "text": payload.message })
            }
            AlertState::NotTriggered => {
                serde_json::json!({ "text": payload.default_resolved_string() })
            }
            AlertState::Disabled => {
                serde_json::json!({ "text": payload.default_disabled_string() })
            }
        };

        if let Err(e) = client.post(self.endpoint.clone()).json(&alert).send().await {
            error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtherWebHook {
    endpoint: Url,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    skip_tls_check: bool,
}

#[async_trait]
impl CallableTarget for OtherWebHook {
    async fn call(&self, payload: &Context) {
        let mut builder = default_client_builder();
        if self.skip_tls_check {
            builder = builder.danger_accept_invalid_certs(true)
        }

        let client = builder
            .build()
            .expect("Client can be constructed on this system");

        let alert = match payload.alert_info.alert_state {
            AlertState::Triggered => payload.message.clone(),
            AlertState::NotTriggered => payload.default_resolved_string(),
            AlertState::Disabled => payload.default_disabled_string(),
        };

        let request = client
            .post(self.endpoint.clone())
            .headers((&self.headers).try_into().expect("valid_headers"));

        if let Err(e) = request.body(alert).send().await {
            error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertManager {
    endpoint: Url,
    #[serde(default)]
    skip_tls_check: bool,
    #[serde(flatten)]
    auth: Option<Auth>,
}

#[async_trait]
impl CallableTarget for AlertManager {
    async fn call(&self, payload: &Context) {
        let mut builder = default_client_builder();

        if self.skip_tls_check {
            builder = builder.danger_accept_invalid_certs(true)
        }

        if let Some(Auth { username, password }) = &self.auth {
            let basic_auth_value = "Basic ".to_string()
                + &base64::prelude::BASE64_STANDARD.encode(format!("{username}:{password}"));
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::try_from(basic_auth_value).expect("valid value"),
            )]);
            builder = builder.default_headers(headers)
        }

        let client = builder
            .build()
            .expect("Client can be constructed on this system");

        let mut alerts = serde_json::json!([{
          "labels": {
            "alertname": payload.alert_info.alert_name,
            // "stream": payload.stream,
            "deployment_instance": payload.deployment_info.deployment_instance,
            "deployment_id": payload.deployment_info.deployment_id,
            "deployment_mode": payload.deployment_info.deployment_mode
            },
          "annotations": {
            "message": "MESSAGE",
            "reason": "REASON"
          }
        }]);

        let alert = &mut alerts[0];

        // fill in status label accordingly
        match payload.alert_info.alert_state {
            AlertState::Triggered => alert["labels"]["status"] = "triggered".into(),
            AlertState::NotTriggered => {
                alert["labels"]["status"] = "not-triggered".into();
                alert["annotations"]["reason"] =
                    serde_json::Value::String(payload.default_resolved_string());
                alert["endsAt"] = Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                    .into();
            }
            AlertState::Disabled => alert["labels"]["status"] = "disabled".into(),
        };

        if let Err(e) = client
            .post(self.endpoint.clone())
            .json(&alerts)
            .send()
            .await
        {
            error!("Couldn't make call to alertmanager, error: {}", e)
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct NotificationConfig {
    pub interval: u64,
    #[serde(skip)]
    pub times: Retry,
    #[serde(skip)]
    pub state: Arc<Mutex<TimeoutState>>,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            interval: 1,
            times: Retry::default(),
            state: Arc::<Mutex<TimeoutState>>::default(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TimeoutState {
    pub alert_state: AlertState,
    pub timed_out: bool,
    pub awaiting_resolve: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Auth {
    username: String,
    password: String,
}
