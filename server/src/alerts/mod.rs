/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod rule;
pub mod target;

pub use self::rule::Rule;
use self::target::Target;

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alerts {
    pub alerts: Vec<Alert>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alert {
    #[serde(default = "crate::utils::uuid::gen")]
    pub id: Uuid,
    pub name: String,
    pub message: String,
    pub rule: Rule,
    pub targets: Vec<Target>,
    // timeout is only defined if duration is given
    #[serde(flatten)]
    pub timeout: Option<Timeout>,
}

impl Alert {
    pub fn check_alert(&self, stream_name: String, event_json: &serde_json::Value) {
        let resolves = self.rule.resolves(event_json);

        // return early, no state change
        if let AlertState::Firing | AlertState::Listening = resolves {
            return;
        }

        if let Some(ref timeout) = self.timeout {
            let mut state = timeout.state.lock().unwrap();

            match resolves {
                AlertState::SetToFiring => {
                    state.alert_state = AlertState::Firing;

                    if !state.timed_out {
                        // set state
                        state.timed_out = true;
                        state.awaiting_resolve = true;
                        drop(state);

                        let context = self.get_context(stream_name, AlertState::SetToFiring);
                        self.spawn_timeout_task(timeout, Arc::clone(&context));
                        self.send_to_targets(context)
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

                    let context = self.get_context(stream_name, AlertState::Resolved);
                    self.send_to_targets(context);
                }
                _ => unreachable!(),
            }
        } else {
            // Without timeout there is no alert state to consider other than the one returned on `resolves`
            let context = self.get_context(stream_name, resolves);
            self.send_to_targets(context);
        }
    }

    fn spawn_timeout_task(&self, timeout: &Timeout, alert_context: Arc<Context>) {
        let state = Arc::clone(&timeout.state);
        let timeout = timeout.timeout;
        let targets = self.targets.clone();

        actix_web::rt::spawn(async move {
            const RETRIES: usize = 10;
            // sleep for timeout period
            for _ in 0..RETRIES {
                let targets = targets.clone();
                let alert_context = Arc::clone(&alert_context);
                tokio::time::sleep(timeout).await;
                let mut state = state.lock().unwrap();
                if state.alert_state == AlertState::Firing {
                    // it is still firing .. sleep more and come back
                    state.awaiting_resolve = true;

                    Self::_send_to_targets(targets, alert_context)
                } else {
                    state.timed_out = false;
                    return;
                }
            }

            // fallback for if this task only observed FIRING on all RETRIES
            // Stream might be dead and sending too many alerts is not great
            // Send and alert stating that this alert will only work once it has seen a RESOLVE
            state.lock().unwrap().timed_out = false;
            let mut context = alert_context.as_ref().clone();
            context.message = format!(
                "Triggering alert did not resolve itself after {} retries, This alert is paused until it resolves", RETRIES
            );
            // Send and exit this task.
            Self::_send_to_targets(targets, Arc::new(context));
        });
    }

    fn send_to_targets(&self, context: Arc<Context>) {
        Self::_send_to_targets(self.targets.clone(), context)
    }

    fn _send_to_targets(targets: Vec<Target>, context: Arc<Context>) {
        log::info!("Calling targets for stream {}", context.alert_name);
        for target in targets {
            let context = Arc::clone(&context);
            actix_web::rt::spawn(async move {
                target.call(context);
            });
        }
    }

    fn get_context(&self, stream_name: String, alert_state: AlertState) -> Arc<Context> {
        Arc::new(Context::new(
            stream_name,
            self.name.clone(),
            self.message.clone(),
            self.rule.trigger_reason(),
            alert_state,
        ))
    }
}

pub trait CallableTarget {
    fn call(&self, payload: &Context);
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Timeout {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(skip)]
    pub state: Arc<Mutex<TimeoutState>>,
}

#[derive(Debug, Clone, Copy)]
pub struct TimeoutState {
    pub alert_state: AlertState,
    pub timed_out: bool,
    pub awaiting_resolve: bool,
}

impl Default for TimeoutState {
    fn default() -> Self {
        Self {
            alert_state: AlertState::Listening,
            timed_out: false,
            awaiting_resolve: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Context {
    stream: String,
    alert_name: String,
    message: String,
    reason: String,
    alert_state: AlertState,
}

impl Context {
    pub fn new(
        stream: String,
        alert_name: String,
        message: String,
        reason: String,
        alert_state: AlertState,
    ) -> Self {
        Self {
            stream,
            alert_name,
            message,
            reason,
            alert_state,
        }
    }

    fn default_alert_string(&self) -> String {
        format!(
            "{} triggered on {}\nMessage: {}\nFailing Condition: {}",
            self.alert_name, self.stream, self.message, self.reason
        )
    }

    fn default_resolved_string(&self) -> String {
        format!("{} on {} is now resolved ", self.alert_name, self.stream)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AlertState {
    Listening,
    SetToFiring,
    Firing,
    Resolved,
}

impl Default for AlertState {
    fn default() -> Self {
        Self::Listening
    }
}
