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

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod rule;
pub mod target;

pub use self::rule::Rule;
use self::target::Target;
use crate::event::Event;

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
}

impl Alert {
    pub async fn check_alert(&self, event: &Event) -> Result<(), ()> {
        let event_json: serde_json::Value = serde_json::from_str(&event.body).map_err(|_| ())?;

        if self.rule.resolves(&event_json) {
            log::info!("Alert triggered for stream {}", self.name);
            for target in self.targets.clone() {
                let context = Context::new(
                    event.stream_name.clone(),
                    self.name.clone(),
                    self.message.clone(),
                    self.rule.trigger_reason(),
                );
                actix_web::rt::spawn(async move {
                    target.call(&context);
                });
            }
        }

        Ok(())
    }
}

pub trait CallableTarget {
    fn call(&self, payload: &Context);
}

pub struct Context {
    stream: String,
    alert_name: String,
    message: String,
    reason: String,
}

impl Context {
    pub fn new(stream: String, alert_name: String, message: String, reason: String) -> Self {
        Self {
            stream,
            alert_name,
            message,
            reason,
        }
    }

    fn default_alert_string(&self) -> String {
        format!(
            "{} triggered on {}\nMessage: {}\nFailing Condition: {}",
            self.alert_name, self.stream, self.message, self.reason
        )
    }
}
