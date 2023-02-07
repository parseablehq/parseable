/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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
    sync::{Arc, Mutex},
    time::Duration,
};

use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};

use super::{AlertState, CallableTarget, Context};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(try_from = "TargetVerifier")]
pub struct Target {
    #[serde(flatten)]
    pub target: TargetType,
    #[serde(flatten)]
    pub timeout: Option<Timeout>,
}

impl Target {
    pub fn call(&self, context: Context) {
        if let Some(ref timeout) = self.timeout {
            let resolves = context.alert_state;
            let mut state = timeout.state.lock().unwrap();

            match resolves {
                AlertState::SetToFiring => {
                    state.alert_state = AlertState::Firing;

                    if !state.timed_out {
                        // set state
                        state.timed_out = true;
                        state.awaiting_resolve = true;
                        drop(state);

                        self.spawn_timeout_task(timeout, context.clone());
                        call_target(self.target.clone(), context)
                    }
                }
                AlertState::Resolved => {
                    state.alert_state = AlertState::Listening;
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
                _ => unreachable!(),
            }
        } else {
            // Without timeout there is no alert state to consider other than the one returned on `resolves`
            call_target(self.target.clone(), context);
        }
    }

    fn spawn_timeout_task(&self, timeout: &Timeout, alert_context: Context) {
        let state = Arc::clone(&timeout.state);
        let timeout = timeout.timeout;
        let target = self.target.clone();

        actix_web::rt::spawn(async move {
            const RETRIES: usize = 10;
            // sleep for timeout period
            for _ in 0..RETRIES {
                tokio::time::sleep(timeout).await;
                let mut state = state.lock().unwrap();
                if state.alert_state == AlertState::Firing {
                    // it is still firing .. sleep more and come back
                    state.awaiting_resolve = true;

                    call_target(target.clone(), alert_context.clone())
                } else {
                    state.timed_out = false;
                    return;
                }
            }

            // fallback for if this task only observed FIRING on all RETRIES
            // Stream might be dead and sending too many alerts is not great
            // Send and alert stating that this alert will only work once it has seen a RESOLVE
            state.lock().unwrap().timed_out = false;
            let mut context = alert_context;
            context.message = format!(
                "Triggering alert did not resolve itself after {RETRIES} retries, This alert is paused until it resolves",
            );
            // Send and exit this task.
            call_target(target, context);
        });
    }
}

fn call_target(target: TargetType, context: Context) {
    actix_web::rt::spawn(async move {
        target.call(&context);
    });
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetVerifier {
    #[serde(flatten)]
    pub target: TargetType,
    #[serde(alias = "repeat")]
    pub timeout: Option<String>,
}

impl TryFrom<TargetVerifier> for Target {
    type Error = humantime::DurationError;

    fn try_from(value: TargetVerifier) -> Result<Self, Self::Error> {
        let timeout = value
            .timeout
            .map(|ref dur| humantime::parse_duration(dur))
            .transpose()?;

        Ok(Target {
            target: value.target,
            timeout: timeout.map(|duration| Timeout {
                timeout: duration,
                state: Default::default(),
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type", content = "config")]
#[serde(deny_unknown_fields)]
pub enum TargetType {
    Slack(SlackWebHook),
    #[serde(rename = "webhook")]
    Other(OtherWebHook),
}

impl TargetType {
    pub fn call(&self, payload: &Context) {
        match self {
            TargetType::Slack(target) => target.call(payload),
            TargetType::Other(target) => target.call(payload),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackWebHook {
    url: String,
}

impl CallableTarget for SlackWebHook {
    fn call(&self, payload: &Context) {
        let alert = match payload.alert_state {
            AlertState::SetToFiring => ureq::json!({ "text": payload.default_alert_string() }),
            AlertState::Resolved => ureq::json!({ "text": payload.default_resolved_string() }),
            _ => unreachable!(),
        };

        if let Err(e) = ureq::post(&self.url)
            .set("Content-Type", "application/json")
            .send_json(alert)
        {
            log::error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OtherWebHook {
    #[serde(rename_all = "camelCase")]
    ApiKey {
        url: String,
        api_key: String,
    },
    Simple {
        url: String,
    },
}

impl CallableTarget for OtherWebHook {
    fn call(&self, payload: &Context) {
        let alert = match payload.alert_state {
            AlertState::SetToFiring => payload.default_alert_string(),
            AlertState::Resolved => payload.default_resolved_string(),
            _ => unreachable!(),
        };

        let res = match self {
            OtherWebHook::Simple { url } => ureq::post(url)
                .set("Content-Type", "text/plain; charset=iso-8859-1")
                .send_string(&alert),
            OtherWebHook::ApiKey { url, api_key } => ureq::post(url)
                .set("Content-Type", "text/plain; charset=iso-8859-1")
                .set("X-API-Key", api_key)
                .send_string(&alert),
        };

        if let Err(e) = res {
            log::error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Timeout {
    #[serde(with = "humantime_serde")]
    #[serde(rename = "repeat")]
    pub timeout: Duration,
    #[serde(skip)]
    pub state: Arc<Mutex<TimeoutState>>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TimeoutState {
    pub alert_state: AlertState,
    pub timed_out: bool,
    pub awaiting_resolve: bool,
}
