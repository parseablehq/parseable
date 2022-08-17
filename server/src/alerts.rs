use log::error;
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alerts {
    pub alerts: Vec<Alert>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    pub field: String,
    /// Field that determines what comparison operator is to be used
    #[serde(default)]
    pub operator: Operator,
    pub value: String,
    pub repeats: u32,
    #[serde(skip)]
    repeated: u32,
    pub within: String,
}

impl Rule {
    // TODO: utilise `within` to set a range for validity of rule to trigger alert
    pub async fn resolves(&mut self, event: &serde_json::Value) -> bool {
        let comparison = match self.operator {
            Operator::EqualTo => event.get(&self.field).unwrap() == &serde_json::json!(self.value),
            // TODO: currently this is a hack, ensure checks are performed in the right way
            Operator::GreaterThan => {
                event.get(&self.field).unwrap().as_f64().unwrap()
                    > serde_json::json!(self.value).as_f64().unwrap()
            }
            Operator::LessThan => {
                event.get(&self.field).unwrap().as_f64().unwrap()
                    < serde_json::json!(self.value).as_f64().unwrap()
            }
        };

        // If truthy, increment count of repeated
        if comparison {
            self.repeated += 1;
        }

        // If enough repetitions made, return true
        if self.repeated >= self.repeats {
            self.repeated = 0;
            return true;
        }

        false
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
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

pub fn alert(body: String) -> Result<(), Error> {
    let alerts: Alerts = serde_json::from_str(body.as_str())?;
    for alert in alerts.alerts {
        if alert.name.is_empty() {
            return Err(Error::InvalidAlert(
                "alert name cannot be empty".to_string(),
            ));
        }
        if alert.message.is_empty() {
            return Err(Error::InvalidAlert(
                "alert message cannot be empty".to_string(),
            ));
        }
        if alert.rule.value.is_empty() {
            return Err(Error::InvalidAlert(
                "rule.value cannot be empty".to_string(),
            ));
        }
        if alert.rule.field.is_empty() {
            return Err(Error::InvalidAlert("rule.field must be set".to_string()));
        }
        if alert.rule.within.is_empty() {
            return Err(Error::InvalidAlert("rule.within must be set".to_string()));
        }
        if alert.rule.repeats == 0 {
            return Err(Error::InvalidAlert(
                "rule.repeats can't be set to 0".to_string(),
            ));
        }
        if alert.targets.is_empty() {
            return Err(Error::InvalidAlert(
                "alert must have at least one target".to_string(),
            ));
        }
    }
    Ok(())
}
