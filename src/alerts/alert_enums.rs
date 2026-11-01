/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::fmt::{self, Display};

use chrono::{DateTime, Utc};
use derive_more::derive::FromStr;
use ulid::Ulid;

use crate::alerts::{
    alert_structs::{AnomalyConfig, ForecastConfig, RollingWindow},
    alert_traits::AlertTrait,
};

pub enum AlertTask {
    Create(Box<dyn AlertTrait>),
    Delete(Ulid),
}

#[derive(Default, Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AlertVersion {
    V1,
    #[default]
    V2,
}

impl From<&str> for AlertVersion {
    fn from(value: &str) -> Self {
        match value {
            "v1" => Self::V1,
            "v2" => Self::V2,
            _ => Self::V2, // default to v2
        }
    }
}

#[derive(
    derive_more::Display,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Default,
    FromStr,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
)]
#[serde(rename_all = "camelCase")]
pub enum Severity {
    Critical,
    High,
    #[default]
    Medium,
    Low,
}

#[derive(derive_more::Display, Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum LogicalOperator {
    #[display("AND")]
    And,
    #[display("OR")]
    Or,
}

#[derive(derive_more::Display, Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum AlertType {
    #[display("threshold")]
    Threshold,
    #[display("anomaly")]
    Anomaly(AnomalyConfig),
    #[display("forecast")]
    Forecast(ForecastConfig),
}

#[derive(
    derive_more::Display,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
)]
#[serde(rename_all = "camelCase")]
pub enum AlertQueryType {
    #[default]
    #[display("builder")]
    Builder,
    #[serde(alias = "sql")]
    #[display("code")]
    Code,
    #[display("promql")]
    Promql,
}

#[derive(derive_more::Display, Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AlertOperator {
    #[serde(rename = ">")]
    #[display(">")]
    GreaterThan,
    #[serde(rename = "<")]
    #[display("<")]
    LessThan,
    #[serde(rename = "=")]
    #[display("=")]
    Equal,
    #[serde(rename = "!=")]
    #[display("!=")]
    NotEqual,
    #[serde(rename = ">=")]
    #[display(">=")]
    GreaterThanOrEqual,
    #[serde(rename = "<=")]
    #[display("<=")]
    LessThanOrEqual,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, FromStr, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum WhereConfigOperator {
    #[serde(rename = "=")]
    Equal,
    #[serde(rename = "!=")]
    NotEqual,
    #[serde(rename = "<")]
    LessThan,
    #[serde(rename = ">")]
    GreaterThan,
    #[serde(rename = "<=")]
    LessThanOrEqual,
    #[serde(rename = ">=")]
    GreaterThanOrEqual,
    #[serde(rename = "is null")]
    IsNull,
    #[serde(rename = "is not null")]
    IsNotNull,
    #[serde(rename = "ilike")]
    ILike,
    #[serde(rename = "contains")]
    Contains,
    #[serde(rename = "begins with")]
    BeginsWith,
    #[serde(rename = "ends with")]
    EndsWith,
    #[serde(rename = "does not contain")]
    DoesNotContain,
    #[serde(rename = "does not begin with")]
    DoesNotBeginWith,
    #[serde(rename = "does not end with")]
    DoesNotEndWith,
}

impl WhereConfigOperator {
    /// Convert the enum value to its string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::GreaterThan => ">",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThanOrEqual => ">=",
            Self::IsNull => "is null",
            Self::IsNotNull => "is not null",
            Self::ILike => "ilike",
            Self::Contains => "contains",
            Self::BeginsWith => "begins with",
            Self::EndsWith => "ends with",
            Self::DoesNotContain => "does not contain",
            Self::DoesNotBeginWith => "does not begin with",
            Self::DoesNotEndWith => "does not end with",
        }
    }
}

impl Display for WhereConfigOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // We can reuse our as_str method to get the string representation
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::AlertQueryType;

    #[test]
    fn alert_query_type_deserializes_supported_modes() {
        assert_eq!(
            serde_json::from_str::<AlertQueryType>("\"builder\"").unwrap(),
            AlertQueryType::Builder
        );
        assert_eq!(
            serde_json::from_str::<AlertQueryType>("\"code\"").unwrap(),
            AlertQueryType::Code
        );
        assert_eq!(
            serde_json::from_str::<AlertQueryType>("\"promql\"").unwrap(),
            AlertQueryType::Promql
        );
    }

    #[test]
    fn alert_query_type_accepts_legacy_sql_as_code() {
        assert_eq!(
            serde_json::from_str::<AlertQueryType>("\"sql\"").unwrap(),
            AlertQueryType::Code
        );
    }

    #[test]
    fn alert_query_type_rejects_unknown_mode() {
        assert!(serde_json::from_str::<AlertQueryType>("\"rawSql\"").is_err());
    }

    #[test]
    fn alert_request_deserializes_promql_query_type() {
        let request: crate::alerts::alert_structs::AlertRequest =
            serde_json::from_value(serde_json::json!({
                "severity": "high",
                "title": "Test alert",
                "alertType": "threshold",
                "queryType": "promql",
                "query": "sum({\"k8s.pod.cpu.usage\"}) by (\"k8s.namespace.name\")",
                "thresholdConfig": {"operator": ">", "value": -1.0},
                "evalConfig": {
                    "rollingWindow": {
                        "evalStart": "10 minutes",
                        "evalEnd": "now",
                        "evalFrequency": 10
                    }
                },
                "targets": [],
                "notificationConfig": {"interval": 1},
                "datasets": ["azure-prod-cluster-metrics"]
            }))
            .unwrap();

        assert_eq!(request.query_type, AlertQueryType::Promql);
        assert_eq!(request.datasets, vec!["azure-prod-cluster-metrics"]);
    }
}

#[derive(derive_more::Display, Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AggregateFunction {
    Avg,
    Count,
    CountDistinct,
    Min,
    Max,
    Sum,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum EvalConfig {
    RollingWindow(RollingWindow),
}

#[derive(
    derive_more::Display,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    PartialEq,
    Default,
    FromStr,
    Eq,
    PartialOrd,
    Ord,
)]
#[serde(rename_all = "camelCase")]
pub enum AlertState {
    #[display("triggered")]
    Triggered,
    #[default]
    #[serde(rename = "not-triggered")]
    #[display("not-triggered")]
    NotTriggered,
    #[display("disabled")]
    Disabled,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub enum NotificationState {
    #[default]
    Notify,
    /// Mute means the alert will evaluate but no notifications will be sent out
    ///
    /// It is a state which can only be set manually
    ///
    /// user needs to pass the timestamp or the duration (in human time) till which the alert is silenced
    Mute(String),
}

impl Display for NotificationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationState::Notify => write!(f, "notify"),
            NotificationState::Mute(till_time) => {
                let till = match till_time.as_str() {
                    "indefinite" => &DateTime::<Utc>::MAX_UTC.to_rfc3339(),
                    _ => till_time,
                };
                write!(f, "{till}")
            }
        }
    }
}
