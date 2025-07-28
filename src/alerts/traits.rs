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

use crate::{
    alerts::{
        AlertConfig, AlertError, AlertState, AlertType, EvalConfig, Severity, ThresholdConfig,
    },
    rbac::map::SessionKey,
};
use std::{collections::HashMap, fmt::Debug};
use tonic::async_trait;
use ulid::Ulid;

#[async_trait]
pub trait AlertTrait: Debug + Send + Sync {
    async fn eval_alert(&self) -> Result<(bool, f64), AlertError>;
    async fn validate(&self, session_key: &SessionKey) -> Result<(), AlertError>;
    fn get_id(&self) -> &Ulid;
    fn get_severity(&self) -> &Severity;
    fn get_title(&self) -> &str;
    fn get_query(&self) -> &str;
    fn get_alert_type(&self) -> &AlertType;
    fn get_threshold_config(&self) -> &ThresholdConfig;
    fn get_eval_config(&self) -> &EvalConfig;
    fn get_targets(&self) -> &Vec<Ulid>;
    fn get_state(&self) -> &AlertState;
    fn get_eval_window(&self) -> String;
    fn get_eval_frequency(&self) -> u64;
    fn get_created(&self) -> String;
    fn get_tags(&self) -> &Option<Vec<String>>;
    fn get_datasets(&self) -> &Vec<String>;
    fn to_alert_config(&self) -> AlertConfig;
    fn clone_box(&self) -> Box<dyn AlertTrait>;
    fn set_state(&mut self, new_state: AlertState);
}

#[async_trait]
pub trait AlertManagerTrait: Send + Sync {
    async fn load(&self) -> anyhow::Result<()>;
    async fn list_alerts_for_user(
        &self,
        session: SessionKey,
        tags: Vec<String>,
    ) -> Result<Vec<AlertConfig>, AlertError>;
    async fn get_alert_by_id(&self, id: Ulid) -> Result<AlertConfig, AlertError>;
    async fn update(&self, alert: &dyn AlertTrait);
    async fn update_state(
        &self,
        alert_id: Ulid,
        new_state: AlertState,
        trigger_notif: Option<String>,
    ) -> Result<(), AlertError>;
    async fn delete(&self, alert_id: Ulid) -> Result<(), AlertError>;
    async fn get_state(&self, alert_id: Ulid) -> Result<AlertState, AlertError>;
    async fn start_task(&self, alert: Box<dyn AlertTrait>) -> Result<(), AlertError>;
    async fn delete_task(&self, alert_id: Ulid) -> Result<(), AlertError>;
    async fn list_tags(&self) -> Vec<String>;
    async fn get_all_alerts(&self) -> HashMap<Ulid, Box<dyn AlertTrait>>;
}
