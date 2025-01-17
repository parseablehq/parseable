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

use std::collections::{HashMap, HashSet};

use actix_web::{http::header::ContentType, Error};
use chrono::Utc;
use datafusion::error::DataFusionError;
use http::StatusCode;
use itertools::Itertools;
use once_cell::sync::Lazy;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use tokio::sync::RwLock;
use tracing::error;

use crate::{
    handlers::http::{
        rbac::RBACError,
        users::{CORRELATION_DIR, USERS_ROOT_DIR},
    },
    option::CONFIG,
    query::QUERY_SESSION,
    rbac::{map::SessionKey, Users},
    storage::ObjectStorageError,
    users::filters::FilterQuery,
    utils::{get_hash, user_auth_for_query},
};

pub static CORRELATIONS: Lazy<Correlation> = Lazy::new(Correlation::default);

type CorrelationMap = HashMap<CorrelationId, CorrelationConfig>;

#[derive(Debug, Default, derive_more::Deref)]
pub struct Correlation(RwLock<HashMap<UserId, CorrelationMap>>);

impl Correlation {
    // Load correlations from storage
    pub async fn load(&self) -> anyhow::Result<()> {
        let store = CONFIG.storage().get_object_store();
        let all_correlations = store.get_all_correlations().await.unwrap_or_default();

        for correlations_bytes in all_correlations.values().flatten() {
            let Ok(correlation) = serde_json::from_slice::<CorrelationConfig>(correlations_bytes)
                .inspect_err(|e| {
                    error!("Unable to load correlation file : {e}");
                })
            else {
                continue;
            };

            self.write()
                .await
                .entry(correlation.user_id.to_owned())
                .or_insert_with(HashMap::new)
                .insert(correlation.id.to_owned(), correlation);
        }

        Ok(())
    }

    pub async fn list_correlations_for_user(
        &self,
        session_key: &SessionKey,
        user_id: &str,
    ) -> Result<Vec<CorrelationConfig>, CorrelationError> {
        let Some(correlations) = self.read().await.get(user_id).cloned() else {
            return Err(CorrelationError::AnyhowError(anyhow::Error::msg(format!(
                "Unable to find correlations for user - {user_id}"
            ))));
        };

        let mut user_correlations = vec![];
        let permissions = Users.get_permissions(session_key);

        for correlation in correlations.values() {
            let tables = &correlation
                .table_configs
                .iter()
                .map(|t| t.table_name.clone())
                .collect_vec();
            if user_auth_for_query(&permissions, tables).is_ok() && correlation.user_id == user_id {
                user_correlations.push(correlation.clone());
            }
        }

        Ok(user_correlations)
    }

    pub async fn get_correlation(
        &self,
        correlation_id: &str,
        user_id: &str,
    ) -> Result<CorrelationConfig, CorrelationError> {
        self.read()
            .await
            .get(user_id)
            .and_then(|correlations| correlations.get(correlation_id))
            .cloned()
            .ok_or_else(|| {
                CorrelationError::AnyhowError(anyhow::Error::msg(format!(
                    "Unable to find correlation with ID- {correlation_id}"
                )))
            })
    }

    /// Insert new or replace existing correlation for the user and with the same ID
    pub async fn update(&self, correlation: &CorrelationConfig) -> Result<(), CorrelationError> {
        // Update in storage
        let correlation_bytes = serde_json::to_vec(&correlation)?.into();
        let path = correlation.path();
        CONFIG
            .storage()
            .get_object_store()
            .put_object(&path, correlation_bytes)
            .await?;

        // Update in memory
        self.write()
            .await
            .entry(correlation.user_id.to_owned())
            .or_insert_with(HashMap::new)
            .insert(correlation.id.to_owned(), correlation.clone());

        Ok(())
    }

    /// Delete correlation from memory and storage
    pub async fn delete(&self, correlation: &CorrelationConfig) -> Result<(), CorrelationError> {
        // Delete from memory
        self.write()
            .await
            .entry(correlation.user_id.to_owned())
            .and_modify(|correlations| {
                correlations.remove(&correlation.id);
            });

        // Delete from storage
        let path = correlation.path();
        CONFIG
            .storage()
            .get_object_store()
            .delete_object(&path)
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CorrelationVersion {
    V1,
}

type CorrelationId = String;
type UserId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelationConfig {
    pub version: CorrelationVersion,
    pub title: String,
    pub id: CorrelationId,
    pub user_id: UserId,
    pub table_configs: Vec<TableConfig>,
    pub join_config: JoinConfig,
    pub filter: Option<FilterQuery>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
}

impl CorrelationConfig {
    pub fn path(&self) -> RelativePathBuf {
        RelativePathBuf::from_iter([
            USERS_ROOT_DIR,
            &self.user_id,
            CORRELATION_DIR,
            &format!("{}.json", self.id),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelationRequest {
    pub title: String,
    pub table_configs: Vec<TableConfig>,
    pub join_config: JoinConfig,
    pub filter: Option<FilterQuery>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
}

impl From<CorrelationRequest> for CorrelationConfig {
    fn from(val: CorrelationRequest) -> Self {
        Self {
            version: CorrelationVersion::V1,
            title: val.title,
            id: get_hash(Utc::now().timestamp_micros().to_string().as_str()),
            user_id: String::default(),
            table_configs: val.table_configs,
            join_config: val.join_config,
            filter: val.filter,
            start_time: val.start_time,
            end_time: val.end_time,
        }
    }
}

impl CorrelationRequest {
    pub fn generate_correlation_config(self, id: String, user_id: String) -> CorrelationConfig {
        CorrelationConfig {
            version: CorrelationVersion::V1,
            title: self.title,
            id,
            user_id,
            table_configs: self.table_configs,
            join_config: self.join_config,
            filter: self.filter,
            start_time: self.start_time,
            end_time: self.end_time,
        }
    }

    /// This function will validate the TableConfigs, JoinConfig, and user auth
    pub async fn validate(&self, session_key: &SessionKey) -> Result<(), CorrelationError> {
        let ctx = &QUERY_SESSION;

        let h1: HashSet<&String> = self.table_configs.iter().map(|t| &t.table_name).collect();
        let h2: HashSet<&String> = self
            .join_config
            .join_conditions
            .iter()
            .map(|j| &j.table_name)
            .collect();

        // check if table config tables are the same
        if h1.len() != 2 {
            return Err(CorrelationError::Metadata(
                "Must provide config for two unique tables",
            ));
        }

        // check that the tables mentioned in join config are
        // the same as those in table config
        if h1 != h2 {
            return Err(CorrelationError::Metadata(
                "Must provide same tables for join config and table config",
            ));
        }

        // check if user has access to table
        let permissions = Users.get_permissions(session_key);
        let tables = &self
            .table_configs
            .iter()
            .map(|t| t.table_name.clone())
            .collect_vec();

        user_auth_for_query(&permissions, tables)?;

        // to validate table config, we need to check whether the mentioned fields
        // are present in the table or not
        for table_config in self.table_configs.iter() {
            // table config check
            let df = ctx.table(&table_config.table_name).await?;

            let mut selected_fields = table_config
                .selected_fields
                .iter()
                .map(|c| c.as_str())
                .collect_vec();

            // unwrap because we have determined that the tables in table config are the same as those in join config
            let condition = self
                .join_config
                .join_conditions
                .iter()
                .find(|j| j.table_name == table_config.table_name)
                .unwrap();
            let join_field = condition.field.as_str();

            if !selected_fields.contains(&join_field) {
                selected_fields.push(join_field);
            }

            // if this errors out then the table config is incorrect or join config is incorrect
            df.select_columns(selected_fields.as_slice())?;
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CorrelationError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Serde Error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Cannot perform this operation: {0}")]
    Metadata(&'static str),
    #[error("User does not exist")]
    UserDoesNotExist(#[from] RBACError),
    #[error("Error: {0}")]
    AnyhowError(#[from] anyhow::Error),
    #[error("Unauthorized")]
    Unauthorized,
    #[error("DataFusion Error: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("{0}")]
    ActixError(#[from] Error),
}

impl actix_web::ResponseError for CorrelationError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
            Self::AnyhowError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthorized => StatusCode::BAD_REQUEST,
            Self::DataFusion(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ActixError(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableConfig {
    pub selected_fields: Vec<String>,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinCondition {
    pub table_name: String,
    pub field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinConfig {
    pub join_conditions: Vec<JoinCondition>,
}
