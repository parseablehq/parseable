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

use serde::{Deserialize, Serialize};

pub mod rule;
pub mod target;

use crate::utils::uid::Uid;

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
    #[serde(default = "crate::utils::uid::gen")]
    pub id: Uid,
    pub name: String,
    pub message: String,
    pub rule: Rule,
    pub targets: Vec<Target>,
}

impl Alert {
    pub fn check_alert(&self, stream_name: String, event_json: &serde_json::Value) {
        let resolves = self.rule.resolves(event_json);

        match resolves {
            AlertState::Listening | AlertState::Firing => (),
            alert_state @ (AlertState::SetToFiring | AlertState::Resolved) => {
                let context = self.get_context(stream_name, alert_state);
                for target in &self.targets {
                    target.call(context.clone());
                }
            }
        }
    }

    fn get_context(&self, stream_name: String, alert_state: AlertState) -> Context {
        Context::new(
            stream_name,
            self.name.clone(),
            self.message.clone(),
            self.rule.trigger_reason(),
            alert_state,
        )
    }
}

pub trait CallableTarget {
    fn call(&self, payload: &Context);
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
