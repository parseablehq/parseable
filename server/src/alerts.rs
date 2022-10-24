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

use std::sync::atomic::{AtomicU32, Ordering};

use log::{error, info};
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alerts {
    pub alerts: Vec<Alert>,
}

#[derive(Debug, Serialize, Deserialize)]
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
    pub async fn check_alert(&self, event: &serde_json::Value) -> Result<(), ()> {
        if self.rule.resolves(event) {
            info!("Alert triggered; name: {}", self.name);
            for target in self.targets.clone() {
                let msg = self.message.clone();
                actix_web::rt::spawn(async move {
                    target.call(&msg);
                });
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Rule {
    Numeric(NumericRule),
}

impl Rule {
    fn resolves(&self, event: &serde_json::Value) -> bool {
        match self {
            Rule::Numeric(rule) => rule.resolves(event),
        }
    }

    pub fn valid_for_schema(&self, schema: &arrow_schema::Schema) -> bool {
        match self {
            Rule::Numeric(NumericRule { field, .. }) => match schema.column_with_name(field) {
                Some((_, field)) => matches!(
                    field.data_type(),
                    arrow_schema::DataType::Int8
                        | arrow_schema::DataType::Int16
                        | arrow_schema::DataType::Int32
                        | arrow_schema::DataType::Int64
                        | arrow_schema::DataType::UInt8
                        | arrow_schema::DataType::UInt16
                        | arrow_schema::DataType::UInt32
                        | arrow_schema::DataType::UInt64
                        | arrow_schema::DataType::Float16
                        | arrow_schema::DataType::Float32
                        | arrow_schema::DataType::Float64
                ),
                None => false,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NumericRule {
    pub field: String,
    /// Field that determines what comparison operator is to be used
    #[serde(default)]
    pub operator: NumericOperator,
    pub value: serde_json::Number,
    pub repeats: u32,
    #[serde(skip)]
    repeated: AtomicU32,
}

impl NumericRule {
    // TODO: utilise `within` to set a range for validity of rule to trigger alert
    fn resolves(&self, event: &serde_json::Value) -> bool {
        let number = match event.get(&self.field).expect("field exists") {
            serde_json::Value::Number(number) => number,
            _ => unreachable!("right rule is set for right column type"),
        };

        let comparison = match self.operator {
            NumericOperator::EqualTo => number == &self.value,
            NumericOperator::NotEqualTo => number != &self.value,
            NumericOperator::GreaterThan => number.as_f64().unwrap() > self.value.as_f64().unwrap(),
            NumericOperator::GreaterThanEquals => {
                number.as_f64().unwrap() >= self.value.as_f64().unwrap()
            }
            NumericOperator::LessThan => number.as_f64().unwrap() < self.value.as_f64().unwrap(),
            NumericOperator::LessThanEquals => {
                number.as_f64().unwrap() <= self.value.as_f64().unwrap()
            }
        };

        // If truthy, increment count of repeated
        // acquire lock and load
        let mut repeated = self.repeated.load(Ordering::Acquire);

        if comparison {
            repeated += 1
        }

        // If enough repetitions made, return true
        let ret = if repeated >= self.repeats {
            repeated = 0;
            true
        } else {
            false
        };
        // store the value back to repeated and release
        self.repeated.store(repeated, Ordering::Release);

        ret
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NumericOperator {
    #[serde(alias = "=")]
    EqualTo,
    #[serde(alias = "!=")]
    NotEqualTo,
    #[serde(alias = ">")]
    GreaterThan,
    #[serde(alias = ">=")]
    GreaterThanEquals,
    #[serde(alias = "<")]
    LessThan,
    #[serde(alias = "<=")]
    LessThanEquals,
}

impl Default for NumericOperator {
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
