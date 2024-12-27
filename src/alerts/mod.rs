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

use actix_web::http::header::ContentType;
use actix_web::web::Json;
use actix_web::{FromRequest, HttpRequest};
use alerts_utils::{evaluate_alert, user_auth_for_query};
use async_trait::async_trait;
use http::StatusCode;
use once_cell::sync::Lazy;
use serde_json::Error as SerdeError;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{trace, warn};
use ulid::Ulid;

pub mod alerts_utils;
pub mod target;

use crate::option::CONFIG;
use crate::rbac::map::SessionKey;
use crate::storage;
use crate::storage::ObjectStorageError;
use crate::sync::schedule_alert_task;
use crate::utils::uid;
use crate::utils::uid::Uid;

use self::target::Target;

// these types describe the scheduled task for an alert
pub type ScheduledTaskHandlers = (JoinHandle<()>, Receiver<()>, Sender<()>);
pub type ScheduledTasks = RwLock<HashMap<Uid, ScheduledTaskHandlers>>;

pub static ALERTS: Lazy<Alerts> = Lazy::new(Alerts::default);

#[derive(Debug, Default)]
pub struct Alerts {
    pub alerts: RwLock<Vec<AlertConfig>>,
    pub scheduled_tasks: ScheduledTasks,
}

#[derive(Default, Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AlertVerison {
    #[default]
    V1,
}

#[async_trait]
pub trait CallableTarget {
    async fn call(&self, payload: &Context);
}

#[derive(Debug, Clone)]
pub struct Context {
    alert_info: AlertInfo,
    deployment_info: DeploymentInfo,
}

impl Context {
    pub fn new(alert_info: AlertInfo, deployment_info: DeploymentInfo) -> Self {
        Self {
            alert_info,
            deployment_info,
        }
    }

    fn default_alert_string(&self) -> String {
        format!(
            "triggered on {}",
            self.alert_info.alert_name,
            // self.alert_info.message,
            // self.alert_info.reason
        )
    }

    fn default_resolved_string(&self) -> String {
        format!("{} is now resolved ", self.alert_info.alert_name)
    }

    fn default_silenced_string(&self) -> String {
        format!(
            "Notifications for {} have been silenced ",
            self.alert_info.alert_name
        )
    }
}

#[derive(Debug, Clone)]
pub struct AlertInfo {
    alert_id: String,
    alert_name: String,
    // message: String,
    // reason: String,
    alert_state: AlertState,
}

impl AlertInfo {
    pub fn new(alert_id: String, alert_name: String, alert_state: AlertState) -> Self {
        Self {
            alert_id,
            alert_name,
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

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AlertType {
    Threshold,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AlertOperator {
    GreaterThan,
    LessThan,
    EqualTo,
    NotEqualTo,
    GreaterThanEqualTo,
    LessThanEqualTo,
    Like,
    NotLike,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum Aggregate {
    Avg,
    Count,
    Min,
    Max,
    Sum,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ThresholdConfig {
    pub agg: Aggregate,
    pub column: String,
    pub operator: AlertOperator,
    pub value: f32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RollingWindow {
    // x minutes (25m)
    pub eval_start: String,
    // should always be "now"
    pub eval_end: String,
    // x minutes (5m)
    pub eval_frequency: u32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum EvalConfig {
    RollingWindow(RollingWindow),
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlertEval {}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub enum AlertState {
    Triggered,
    Silenced,
    #[default]
    Resolved,
}

impl Display for AlertState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertState::Triggered => write!(f, "Triggered"),
            AlertState::Silenced => write!(f, "Silenced"),
            AlertState::Resolved => write!(f, "Resolved"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlertRequest {
    pub version: AlertVerison,
    pub title: String,
    pub query: String,
    pub alert_type: AlertType,
    pub thresholds: Vec<ThresholdConfig>,
    pub eval_type: EvalConfig,
    pub targets: Vec<Target>,
}

impl FromRequest for AlertRequest {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let body = Json::<AlertRequest>::from_request(req, payload);
        let fut = async move {
            let body = body.await?.into_inner();
            Ok(body)
        };

        Box::pin(fut)
    }
}

impl AlertRequest {
    pub fn modify(self, alert: AlertConfig) -> AlertConfig {
        AlertConfig {
            version: self.version,
            id: alert.id,
            title: self.title,
            query: self.query,
            alert_type: self.alert_type,
            thresholds: self.thresholds,
            eval_type: self.eval_type,
            targets: self.targets,
            state: alert.state,
        }
    }
}

impl From<AlertRequest> for AlertConfig {
    fn from(val: AlertRequest) -> AlertConfig {
        AlertConfig {
            version: val.version,
            id: crate::utils::uid::gen(),
            title: val.title,
            query: val.query,
            alert_type: val.alert_type,
            thresholds: val.thresholds,
            eval_type: val.eval_type,
            targets: val.targets,
            state: AlertState::default(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlertConfig {
    pub version: AlertVerison,
    #[serde(default = "crate::utils::uid::gen")]
    pub id: uid::Uid,
    pub title: String,
    pub query: String,
    pub alert_type: AlertType,
    pub thresholds: Vec<ThresholdConfig>,
    pub eval_type: EvalConfig,
    pub targets: Vec<Target>,
    // for new alerts, state should be resolved
    #[serde(default = "AlertState::default")]
    pub state: AlertState,
}

impl AlertConfig {
    pub fn get_eval_frequency(&self) -> u32 {
        match &self.eval_type {
            EvalConfig::RollingWindow(rolling_window) => rolling_window.eval_frequency,
        }
    }

    fn get_context(&self, alert_state: AlertState) -> Context {
        let deployment_instance = format!(
            "{}://{}",
            CONFIG.parseable.get_scheme(),
            CONFIG.parseable.address
        );
        let deployment_id = storage::StorageMetadata::global().deployment_id;
        let deployment_mode = storage::StorageMetadata::global().mode.to_string();

        // let additional_labels =
        //     serde_json::to_value(rule).expect("rule is perfectly deserializable");
        // let flatten_additional_labels =
        //     utils::json::flatten::flatten_with_parent_prefix(additional_labels, "rule", "_")
        //         .expect("can be flattened");

        Context::new(
            AlertInfo::new(self.id.to_string(), self.title.clone(), alert_state),
            DeploymentInfo::new(deployment_instance, deployment_id, deployment_mode),
        )
    }

    pub async fn trigger_notifications(&self) -> Result<(), AlertError> {
        let context = self.get_context(self.state);
        for target in &self.targets {
            target.call(context.clone());
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AlertError {
    #[error("Storage Error: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Serde Error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Cannot perform this operation: {0}")]
    Metadata(&'static str),
    #[error("User is not authorized to run this query")]
    Unauthorized,
    #[error("ActixError: {0}")]
    Error(#[from] actix_web::Error),
    #[error("DataFusion Error: {0}")]
    DatafusionError(#[from] datafusion::error::DataFusionError),
    #[error("Error: {0}")]
    CustomError(String),
    #[error("Invalid State Change: {0}")]
    InvalidStateChange(String),
}

impl actix_web::ResponseError for AlertError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::BAD_REQUEST,
            Self::Error(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::DatafusionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::CustomError(_) => StatusCode::BAD_REQUEST,
            Self::InvalidStateChange(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

impl Alerts {
    /// Loads alerts from disk
    /// spawn scheduled tasks
    /// Evaluate
    pub async fn load(&self) -> Result<(), AlertError> {
        let mut this = vec![];
        let store = CONFIG.storage().get_object_store();
        let all_alerts = store.get_alerts().await.unwrap_or_default();

        for alert in all_alerts {
            if alert.is_empty() {
                continue;
            }

            let alert: AlertConfig = serde_json::from_slice(&alert)?;

            let (handle, rx, tx) =
                schedule_alert_task(alert.get_eval_frequency(), alert.clone()).await?;

            self.update_task(alert.id, handle, rx, tx).await;

            this.push(alert);
        }

        let mut s = self.alerts.write().await;
        s.append(&mut this.clone());
        drop(s);

        // run eval task once for each alert
        for alert in this.iter() {
            evaluate_alert(alert).await?;
        }

        Ok(())
    }

    /// Returns a list of alerts that the user has access to (based on query auth)
    pub async fn list_alerts_for_user(
        &self,
        session: SessionKey,
    ) -> Result<Vec<AlertConfig>, AlertError> {
        let mut alerts: Vec<AlertConfig> = Vec::new();
        for alert in self.alerts.read().await.iter() {
            // filter based on whether the user can execute this query or not
            let query = &alert.query;
            if user_auth_for_query(&session, query).await.is_ok() {
                alerts.push(alert.to_owned());
            }
        }

        Ok(alerts)
    }

    /// Returns a sigle alert that the user has access to (based on query auth)
    pub async fn get_alert_by_id(&self, id: &str) -> Result<AlertConfig, AlertError> {
        let read_access = self.alerts.read().await;
        let alert = read_access.iter().find(|a| a.id.to_string() == id);

        if let Some(alert) = alert {
            Ok(alert.clone())
        } else {
            Err(AlertError::CustomError(format!(
                "No alert found for the given ID- {id}"
            )))
        }
    }

    /// Update the in-mem vector of alerts
    pub async fn update(&self, alert: &AlertConfig) {
        let mut s = self.alerts.write().await;
        s.retain(|a| a.id != alert.id);
        s.push(alert.clone());
    }

    /// Update the state of alert
    pub async fn update_state(
        &self,
        alert_id: &str,
        new_state: AlertState,
        trigger_notif: bool,
    ) -> Result<(), AlertError> {
        let store = CONFIG.storage().get_object_store();

        // read and modify alert
        let mut alert = self.get_alert_by_id(alert_id).await?;

        alert.state = new_state;

        // save to disk
        store.put_alert(alert_id, &alert).await?;

        // modify in memory
        let mut writer = self.alerts.write().await;
        let alert_to_update = writer
            .iter_mut()
            .find(|alert| alert.id.to_string() == alert_id);
        if let Some(alert) = alert_to_update {
            alert.state = new_state;
        };
        drop(writer);

        if trigger_notif {
            alert.trigger_notifications().await?;
        }

        Ok(())
    }

    /// Remove alert and scheduled task from disk and memory
    pub async fn delete(&self, alert_id: &str) -> Result<(), AlertError> {
        // delete from memory
        let read_access = self.alerts.read().await;

        let index = read_access
            .iter()
            .enumerate()
            .find(|(_, alert)| alert.id.to_string() == alert_id)
            .to_owned();

        if let Some((index, _)) = index {
            // drop the read access in order to get exclusive write access
            drop(read_access);
            self.alerts.write().await.remove(index);
            trace!("removed alert from memory");
        } else {
            warn!("Alert ID- {alert_id} not found in memory!");
        }
        Ok(())
    }

    /// Get state of alert using alert_id
    pub async fn get_state(&self, alert_id: &str) -> Result<AlertState, AlertError> {
        let read_access = self.alerts.read().await;
        let alert = read_access.iter().find(|a| a.id.to_string() == alert_id);

        if let Some(alert) = alert {
            Ok(alert.state)
        } else {
            let msg = format!("No alert present for ID- {alert_id}");
            Err(AlertError::CustomError(msg))
        }
    }

    /// Update the scheduled alert tasks in-memory map
    pub async fn update_task(
        &self,
        id: Uid,
        handle: JoinHandle<()>,
        rx: Receiver<()>,
        tx: Sender<()>,
    ) {
        let mut s = self.scheduled_tasks.write().await;
        s.insert(id, (handle, rx, tx));
    }

    /// Remove a scheduled alert task
    pub async fn delete_task(&self, alert_id: &str) -> Result<(), AlertError> {
        let read_access = self.scheduled_tasks.read().await;

        let hashed_object = read_access
            .iter()
            .find(|(id, _)| id.to_string() == alert_id);

        if hashed_object.is_some() {
            // drop the read access in order to get exclusive write access
            drop(read_access);

            // now delete from hashmap
            let removed =
                self.scheduled_tasks
                    .write()
                    .await
                    .remove(&Ulid::from_string(alert_id).map_err(|_| {
                        AlertError::CustomError("Unable to decode Ulid".to_owned())
                    })?);

            if removed.is_none() {
                trace!("Unable to remove alert task {alert_id} from hashmap");
            }
        } else {
            trace!("Alert task {alert_id} not found in hashmap");
        }

        Ok(())
    }
}
