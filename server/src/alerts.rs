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

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    // TODO: spawn async tasks to call webhooks if alert rules are met
    // This is done to ensure that threads aren't blocked by calls to the webhook
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
            Rule::Numeric(NumericRule { column, .. }) => match schema.column_with_name(column) {
                Some((_, column)) => matches!(
                    column.data_type(),
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

    pub fn trigger_reason(&self) -> String {
        match self {
            Rule::Numeric(NumericRule {
                column,
                operator,
                value,
                repeats,
                ..
            }) => match operator {
                NumericOperator::EqualTo => format!(
                    "{} column was equal to {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::NotEqualTo => format!(
                    "{} column was not equal to {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::GreaterThan => format!(
                    "{} column was greater than {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::GreaterThanEquals => format!(
                    "{} column was greater than or equal to {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::LessThan => format!(
                    "{} column was less than {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::LessThanEquals => format!(
                    "{} column was less than or equal to {}, {} times",
                    column, value, repeats
                ),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NumericRule {
    pub column: String,
    /// Field that determines what comparison operator is to be used
    #[serde(default)]
    pub operator: NumericOperator,
    pub value: serde_json::Number,
    pub repeats: u32,
    #[serde(skip)]
    repeated: AtomicU32,
}

impl NumericRule {
    fn resolves(&self, event: &serde_json::Value) -> bool {
        let number = match event.get(&self.column).expect("column exists") {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum Target {
    Slack(targets::slack::SlackWebHook),
    #[serde(alias = "webhook")]
    Other(targets::other::OtherWebHook),
}

impl Target {
    pub fn call(&self, payload: &Context) {
        match self {
            Target::Slack(target) => target.call(payload),
            Target::Other(target) => target.call(payload),
        }
    }
}

pub trait CallableTarget {
    fn call(&self, payload: &Context);
}

pub mod targets {
    pub mod slack {
        use serde::{Deserialize, Serialize};

        use crate::alerts::{CallableTarget, Context};

        #[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        pub struct SlackWebHook {
            #[serde(rename = "server_url")]
            server_url: String,
        }

        impl CallableTarget for SlackWebHook {
            fn call(&self, payload: &Context) {
                if let Err(e) = ureq::post(&self.server_url)
                    .set("Content-Type", "application/json")
                    .send_json(ureq::json!({ "text": payload.default_alert_string() }))
                {
                    log::error!("Couldn't make call to webhook, error: {}", e)
                }
            }
        }
    }

    pub mod other {
        use serde::{Deserialize, Serialize};

        use crate::alerts::{CallableTarget, Context};

        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(untagged)]
        pub enum OtherWebHook {
            ApiKey {
                #[serde(rename = "server_url")]
                server_url: String,
                #[serde(rename = "api_key")]
                api_key: String,
            },
            Simple {
                #[serde(rename = "server_url")]
                server_url: String,
            },
        }

        impl CallableTarget for OtherWebHook {
            fn call(&self, payload: &Context) {
                let res = match self {
                    OtherWebHook::Simple { server_url } => ureq::post(server_url)
                        .set("Content-Type", "text/plain; charset=iso-8859-1")
                        .send_string(&payload.default_alert_string()),
                    OtherWebHook::ApiKey {
                        server_url,
                        api_key,
                    } => ureq::post(server_url)
                        .set("Content-Type", "text/plain; charset=iso-8859-1")
                        .set("X-API-Key", api_key)
                        .send_string(&payload.default_alert_string()),
                };

                if let Err(e) = res {
                    log::error!("Couldn't make call to webhook, error: {}", e)
                }
            }
        }
    }
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

    // <Alert_Name> Triggered on <Log_stream>
    // Message: Ting
    // Failing Condition: Status column was equal to 500, 5 times
    fn default_alert_string(&self) -> String {
        format!(
            "{} triggered on {}\nMessage: {}\nFailing Condition: {}",
            self.alert_name, self.stream, self.message, self.reason
        )
    }
}
