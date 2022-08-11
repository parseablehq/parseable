use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{error::Error, event::Event};

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
    pub async fn parse_event(&self, event: &Event) -> Result<(), Error> {
        if self.rule.matches(&event) {
            for _ in self.targets.clone() {
                actix_web::rt::spawn(async move {});
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
    pub comparator: Comparator,
    pub value: Value,
    pub repeats: u32,
    pub within: String,
}

impl Rule {
    pub fn matches(&self, _event: &Event) -> bool {
        true
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Comparator {
    EqualTo,
    GreaterThan,
    LessThan,
}

impl Default for Comparator {
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
        if alert.rule.value.is_number() {
            return Err(Error::InvalidAlert(
                "rule.value must be a numerical value".to_string(),
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
