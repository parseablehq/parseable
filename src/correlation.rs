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

use std::collections::{HashMap, HashSet};

use actix_web::http::StatusCode;
use actix_web::{Error, http::header::ContentType};
use chrono::Utc;
use datafusion::error::DataFusionError;
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
    metastore::{MetastoreError, metastore_traits::MetastoreObject},
    parseable::{DEFAULT_TENANT, PARSEABLE},
    query::QUERY_SESSION,
    rbac::{Users, map::SessionKey},
    storage::ObjectStorageError,
    users::filters::FilterQuery,
    utils::{get_hash, get_tenant_id_from_key, user_auth_for_datasets},
};

pub static CORRELATIONS: Lazy<Correlations> = Lazy::new(Correlations::default);

type CorrelationMap = HashMap<CorrelationId, CorrelationConfig>;

#[derive(Debug, Default, derive_more::Deref)]
pub struct Correlations(RwLock<HashMap<String, CorrelationMap>>);

impl Correlations {
    // Load correlations from storage
    pub async fn load(&self) -> anyhow::Result<()> {
        let all_correlations = PARSEABLE.metastore.get_correlations().await?;

        let mut guard = self.write().await;

        for (tenant_id, correlations_bytes) in all_correlations {
            let mut corrs = HashMap::new();
            for corr in correlations_bytes {
                let correlation = match serde_json::from_slice::<CorrelationConfig>(&corr) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Unable to load correlation file : {e}");
                        continue;
                    }
                };
                corrs.insert(correlation.id.clone(), correlation);
            }

            guard.insert(tenant_id, corrs);
        }

        Ok(())
    }

    pub async fn list_correlations(
        &self,
        session_key: &SessionKey,
    ) -> Result<Vec<CorrelationConfig>, CorrelationError> {
        let mut user_correlations = vec![];
        let permissions = Users.get_permissions(session_key);
        let tenant_id = get_tenant_id_from_key(session_key);
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(corrs) = self.read().await.get(tenant) {
            for correlation in corrs.values() {
                let tables = &correlation
                    .table_configs
                    .iter()
                    .map(|t| t.table_name.clone())
                    .collect_vec();
                if user_auth_for_datasets(&permissions, tables, &tenant_id)
                    .await
                    .is_ok()
                {
                    user_correlations.push(correlation.clone());
                }
            }
        }

        Ok(user_correlations)
    }

    pub async fn get_correlation(
        &self,
        correlation_id: &str,
        tenant_id: &Option<String>,
    ) -> Result<CorrelationConfig, CorrelationError> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(corrs) = self.read().await.get(tenant_id) {
            corrs.get(correlation_id).cloned().ok_or_else(|| {
                CorrelationError::AnyhowError(anyhow::Error::msg(format!(
                    "Unable to find correlation with ID- {correlation_id}"
                )))
            })
        } else {
            Err(CorrelationError::AnyhowError(anyhow::Error::msg(format!(
                "Unable to find correlation with ID- {correlation_id}"
            ))))
        }
    }

    /// Create correlation associated with the user
    pub async fn create(
        &self,
        mut correlation: CorrelationConfig,
        session_key: &SessionKey,
    ) -> Result<CorrelationConfig, CorrelationError> {
        correlation.id = get_hash(Utc::now().timestamp_micros().to_string().as_str());
        correlation.validate(session_key).await?;
        let tenant_id = get_tenant_id_from_key(session_key);
        // Update in metastore
        PARSEABLE
            .metastore
            .put_correlation(&correlation, &tenant_id)
            .await?;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        // Update in memory
        if let Some(corrs) = self.write().await.get_mut(tenant) {
            corrs.insert(correlation.id.to_owned(), correlation.clone());
        }

        Ok(correlation)
    }

    /// Update existing correlation for the user and with the same ID
    pub async fn update(
        &self,
        mut updated_correlation: CorrelationConfig,
        session_key: &SessionKey,
    ) -> Result<CorrelationConfig, CorrelationError> {
        let tenant_id = get_tenant_id_from_key(session_key);
        // validate whether user has access to this correlation object or not
        let correlation = self
            .get_correlation(&updated_correlation.id, &tenant_id)
            .await?;
        if correlation.user_id != updated_correlation.user_id {
            return Err(CorrelationError::AnyhowError(anyhow::Error::msg(format!(
                r#"User "{}" isn't authorized to update correlation with ID - {}"#,
                updated_correlation.user_id, correlation.id
            ))));
        }

        correlation.validate(session_key).await?;
        updated_correlation.update(correlation);

        // Update in metastore
        PARSEABLE
            .metastore
            .put_correlation(&updated_correlation, &tenant_id)
            .await?;

        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        // Update in memory
        if let Some(corrs) = self.write().await.get_mut(tenant) {
            corrs.insert(
                updated_correlation.id.to_owned(),
                updated_correlation.clone(),
            );
        }

        Ok(updated_correlation)
    }

    /// Delete correlation from memory and storage
    pub async fn delete(
        &self,
        correlation_id: &str,
        user_id: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), CorrelationError> {
        let correlation = CORRELATIONS
            .get_correlation(correlation_id, tenant_id)
            .await?;
        if correlation.user_id != user_id {
            return Err(CorrelationError::AnyhowError(anyhow::Error::msg(format!(
                r#"User "{user_id}" isn't authorized to delete correlation with ID - {correlation_id}"#
            ))));
        }

        // Delete from storage
        PARSEABLE
            .metastore
            .delete_correlation(&correlation, tenant_id)
            .await?;

        // Delete from memory
        self.write().await.remove(&correlation.id);

        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CorrelationVersion {
    #[default]
    V1,
}

type CorrelationId = String;
type UserId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelationConfig {
    #[serde(default)]
    pub version: CorrelationVersion,
    pub title: String,
    #[serde(default)]
    pub id: CorrelationId,
    #[serde(default)]
    pub user_id: UserId,
    pub table_configs: Vec<TableConfig>,
    pub join_config: JoinConfig,
    pub filter: Option<FilterQuery>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
}

impl MetastoreObject for CorrelationConfig {
    fn get_object_path(&self) -> String {
        self.path().to_string()
    }

    fn get_object_id(&self) -> String {
        self.id.clone()
    }
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

    pub fn update(&mut self, update: Self) {
        self.title = update.title;
        self.table_configs = update.table_configs;
        self.join_config = update.join_config;
        self.filter = update.filter;
        self.start_time = update.start_time;
        self.end_time = update.end_time;
    }

    /// This function will validate the TableConfigs, JoinConfig, and user auth
    pub async fn validate(&self, session_key: &SessionKey) -> Result<(), CorrelationError> {
        let ctx = &QUERY_SESSION.get_ctx();
        let tenant_id = get_tenant_id_from_key(session_key);
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

        user_auth_for_datasets(&permissions, tables, &tenant_id).await?;

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
    #[error(transparent)]
    MetastoreError(#[from] MetastoreError),
}

impl actix_web::ResponseError for CorrelationError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
            Self::AnyhowError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthorized => StatusCode::BAD_REQUEST,
            Self::DataFusion(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ActixError(_) => StatusCode::BAD_REQUEST,
            Self::MetastoreError(e) => e.status_code(),
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        match self {
            CorrelationError::MetastoreError(e) => {
                actix_web::HttpResponse::build(self.status_code())
                    .insert_header(ContentType::json())
                    .json(e.to_detail())
            }
            _ => actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string()),
        }
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
