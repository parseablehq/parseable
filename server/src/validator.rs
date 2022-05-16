use serde_derive::Deserialize;
use serde_derive::Serialize;

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
    pub target: Vec<Target>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    pub field: String,
    pub contains: String,
    pub repeats: String,
    pub within: String,
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

/// TODO: unwrap result
pub fn alert_validator(body: String) {
    let _p: Alerts = serde_json::from_str(&body).unwrap();
}
