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

use log::{error, info};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

use crate::error::Error;

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alerts {
    pub alerts: Vec<Alert>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alert {
    pub name: String,
    pub message: String,
    pub rule: Rule,
    pub targets: Vec<Target>,
}

impl Alert {
    // TODO: spawn async tasks to call webhooks if alert rules are met
    // This is done to ensure that threads aren't blocked by calls to the webhook
    pub async fn check_alert(&mut self, event: &serde_json::Value) -> Result<(), Error> {
        if self.rule.resolves(event).await {
            info!("Alert triggered; name: {}", self.name);

            let msg = self.message.clone();
            let targets = self.targets.clone();
            actix_web::rt::spawn(async move {
                for target in targets {
                    target.call(&msg);
                }
            });
        }

        Ok(())
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    /// Determines what field the alert is to compare against
    pub field: String,
    /// Determines what comparison operator is to be used
    #[serde(default)]
    pub operator: Operator,
    /// Value compared against incoming data
    pub value: serde_json::Value,
    /// Number of times the rule is expected to meet before resolving
    pub repeats: u32,
    /// Number of times the rule has been met as of now
    #[serde(skip)]
    repeated: u32,
    /// Seconds after first event that meets rule that can trigger alert
    pub within: u64,
    /// Instant after which timer for resolution is reset
    #[serde(skip)]
    period_end: Option<Instant>,
}

impl Rule {
    pub async fn resolves(&mut self, event: &serde_json::Value) -> bool {
        let comparison = match self.operator {
            Operator::EqualTo => event.get(&self.field).unwrap() == &self.value,
            // TODO: currently this is a hack, ensure checks are performed in the right way
            Operator::GreaterThan => {
                event.get(&self.field).unwrap().as_f64().unwrap() > (self.value).as_f64().unwrap()
            }
            Operator::LessThan => {
                event.get(&self.field).unwrap().as_f64().unwrap() < (self.value).as_f64().unwrap()
            }
        };

        // Reset rule if last timer passed or it is currently untimed
        match self.period_end {
            Some(instant) if Instant::now() > instant => self.reset(),
            None => self.reset(),
            _ => {}
        }

        // If truthy, increment count of repeated
        if comparison {
            self.repeated += 1;
        }

        // If enough repetitions made set repeated to 0 and remove timer before returning true
        if self.repeated >= self.repeats {
            self.repeated = 0;
            self.period_end.take();
            return true;
        }

        false
    }

    fn reset(&mut self) {
        self.period_end = Some(Instant::now() + Duration::from_secs(self.within));
        self.repeated = 0;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Operator {
    EqualTo,
    GreaterThan,
    LessThan,
}

impl Default for Operator {
    fn default() -> Self {
        Self::EqualTo
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    pub name: String,
    #[serde(rename = "server_url")]
    pub server_url: String,
    #[serde(rename = "api_key")]
    pub api_key: String,
}

impl Target {
    pub fn call(&self, msg: &str) {
        if let Err(e) = ureq::post(&self.server_url)
            .set("Content-Type", "text/plain; charset=iso-8859-1")
            .set("X-API-Key", &self.api_key)
            .send_string(msg)
        {
            error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}
