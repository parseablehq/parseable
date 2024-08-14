/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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

use arrow_array::cast::as_string_array;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use async_trait::async_trait;
use datafusion::arrow::compute::kernels::cast;
use datafusion::arrow::datatypes::Schema;
use regex::Regex;
use serde::{Deserialize, Serialize};

use std::fmt;
use utoipa::ToSchema;

pub mod parser;
pub mod rule;
pub mod target;

use crate::metrics::ALERTS_STATES;
use crate::utils::arrow::get_field;
use crate::utils::uid;
use crate::CONFIG;
use crate::{storage, utils};

pub use self::rule::Rule;
use self::target::Target;

#[derive(Default, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Alerts {
    pub version: AlertVersion,
    pub alerts: Vec<Alert>,
}

#[derive(Default, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum AlertVersion {
    #[default]
    V1,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Alert {
    #[serde(default = "crate::utils::uid::gen")]
    #[schema(value_type = String)]
    pub id: uid::Uid,
    pub name: String,
    #[serde(flatten)]
    #[schema(value_type = String)]
    pub message: Message,
    #[schema(value_type = String)]
    pub rule: Rule,
    #[schema(value_type = Vec<Object>)]
    pub targets: Vec<Target>,
}

impl Alert {
    pub fn check_alert(&self, stream_name: &str, events: RecordBatch) {
        let resolves = self.rule.resolves(events.clone());

        for (index, state) in resolves.into_iter().enumerate() {
            match state {
                AlertState::Listening | AlertState::Firing => (),
                alert_state @ (AlertState::SetToFiring | AlertState::Resolved) => {
                    let context = self.get_context(
                        stream_name.to_owned(),
                        alert_state,
                        &self.rule,
                        events.slice(index, 1),
                    );
                    ALERTS_STATES
                        .with_label_values(&[
                            context.stream.as_str(),
                            context.alert_info.alert_name.as_str(),
                            context.alert_info.alert_state.to_string().as_str(),
                        ])
                        .inc();
                    for target in &self.targets {
                        target.call(context.clone());
                    }
                }
            }
        }
    }

    fn get_context(
        &self,
        stream_name: String,
        alert_state: AlertState,
        rule: &Rule,
        event_row: RecordBatch,
    ) -> Context {
        let deployment_instance = format!(
            "{}://{}",
            CONFIG.parseable.get_scheme(),
            CONFIG.parseable.address
        );
        let deployment_id = storage::StorageMetadata::global().deployment_id;
        let deployment_mode = storage::StorageMetadata::global().mode.to_string();
        let additional_labels =
            serde_json::to_value(rule).expect("rule is perfectly deserializable");
        let flatten_additional_labels =
            utils::json::flatten::flatten_with_parent_prefix(additional_labels, "rule", "_")
                .expect("can be flattened");
        Context::new(
            stream_name,
            AlertInfo::new(
                self.name.clone(),
                self.message.get(event_row),
                rule.trigger_reason(),
                alert_state,
            ),
            DeploymentInfo::new(deployment_instance, deployment_id, deployment_mode),
            flatten_additional_labels,
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub message: String,
}

impl Message {
    // checks if message (with a column name) is valid (i.e. the column name is present in the schema)
    pub fn valid(&self, schema: &Schema, column: &str) -> bool {
        return get_field(&schema.fields, column).is_some();
    }

    pub fn extract_column_names(&self) -> Vec<&str> {
        // the message can have either no column name ({column_name} not present) or any number of {column_name} present
        Regex::new(r"\{(.*?)\}")
            .unwrap()
            .captures_iter(self.message.as_str())
            .map(|cap| cap.get(1).unwrap().as_str())
            .collect()
    }

    /// Returns the message with the column names replaced with the values in the column.
    fn get(&self, event: RecordBatch) -> String {
        let mut replace_message = self.message.clone();
        for column in self.extract_column_names() {
            if let Some(value) = event.column_by_name(column) {
                let arr = cast(value, &DataType::Utf8).unwrap();
                let value = as_string_array(&arr).value(0);

                replace_message =
                    replace_message.replace(&format!("{{{column}}}"), value.to_string().as_str());
            }
        }
        replace_message
    }
}

#[async_trait]
pub trait CallableTarget {
    async fn call(&self, payload: &Context);
}

#[derive(Debug, Clone)]
pub struct Context {
    stream: String,
    alert_info: AlertInfo,
    deployment_info: DeploymentInfo,
    additional_labels: serde_json::Value,
}

impl Context {
    pub fn new(
        stream: String,
        alert_info: AlertInfo,
        deployment_info: DeploymentInfo,
        additional_labels: serde_json::Value,
    ) -> Self {
        Self {
            stream,
            alert_info,
            deployment_info,
            additional_labels,
        }
    }

    fn default_alert_string(&self) -> String {
        format!(
            "{} triggered on {}\nMessage: {}\nFailing Condition: {}",
            self.alert_info.alert_name,
            self.stream,
            self.alert_info.message,
            self.alert_info.reason
        )
    }

    fn default_resolved_string(&self) -> String {
        format!(
            "{} on {} is now resolved ",
            self.alert_info.alert_name, self.stream
        )
    }
}

#[derive(Debug, Clone)]
pub struct AlertInfo {
    alert_name: String,
    message: String,
    reason: String,
    alert_state: AlertState,
}

impl AlertInfo {
    pub fn new(
        alert_name: String,
        message: String,
        reason: String,
        alert_state: AlertState,
    ) -> Self {
        Self {
            alert_name,
            message,
            reason,
            alert_state,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeploymentInfo {
    deployment_instance: String,
    deployment_id: uid::Uid,
    deployment_mode: String,
}

impl DeploymentInfo {
    pub fn new(
        deployment_instance: String,
        deployment_id: uid::Uid,
        deployment_mode: String,
    ) -> Self {
        Self {
            deployment_instance,
            deployment_id,
            deployment_mode,
        }
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

impl fmt::Display for AlertState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AlertState::Listening => write!(f, "Listening"),
            AlertState::SetToFiring => write!(f, "SetToFiring"),
            AlertState::Firing => write!(f, "Firing"),
            AlertState::Resolved => write!(f, "Resolved"),
        }
    }
}
