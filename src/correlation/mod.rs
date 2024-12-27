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

use std::collections::HashSet;

use actix_web::http::header::ContentType;
use chrono::Utc;
use correlation_utils::user_auth_for_query;
use datafusion::error::DataFusionError;
use http::StatusCode;
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use tokio::sync::RwLock;
use tracing::{error, trace, warn};

use crate::{
    handlers::http::rbac::RBACError, option::CONFIG, query::QUERY_SESSION, rbac::map::SessionKey,
    storage::ObjectStorageError, users::filters::FilterQuery, utils::get_hash,
};

pub mod correlation_utils;

pub static CORRELATIONS: Lazy<Correlation> = Lazy::new(Correlation::default);

#[derive(Debug, Default)]
pub struct Correlation(RwLock<Vec<CorrelationConfig>>);

impl Correlation {
    pub async fn load(&self) -> Result<(), CorrelationError> {
        // lead correlations from storage
        let store = CONFIG.storage().get_object_store();
        let all_correlations = store.get_correlations().await.unwrap_or_default();

        let mut correlations = vec![];
        for corr in all_correlations {
            if corr.is_empty() {
                continue;
            }

            let correlation: CorrelationConfig = match serde_json::from_slice(&corr) {
                Ok(c) => c,
                Err(e) => {
                    error!("Unable to load correlation- {e}");
                    continue;
                },
            };

            correlations.push(correlation);
        }

        let mut s = self.0.write().await;
        s.append(&mut correlations.clone());
        Ok(())
    }

    pub async fn list_correlations_for_user(
        &self,
        session_key: &SessionKey,
    ) -> Result<Vec<CorrelationConfig>, CorrelationError> {
        let correlations = self.0.read().await.iter().cloned().collect_vec();

        let mut user_correlations = vec![];
        for c in correlations {
            if user_auth_for_query(session_key, &c.table_configs)
                .await
                .is_ok()
            {
                user_correlations.push(c);
            }
        }
        Ok(user_correlations)
    }

    pub async fn get_correlation_by_id(
        &self,
        correlation_id: &str,
    ) -> Result<CorrelationConfig, CorrelationError> {
        let read = self.0.read().await;
        let correlation = read.iter().find(|c| c.id == correlation_id).cloned();

        if let Some(c) = correlation {
            Ok(c)
        } else {
            Err(CorrelationError::AnyhowError(anyhow::Error::msg(format!(
                "Unable to find correlation with ID- {correlation_id}"
            ))))
        }
    }

    pub async fn update(&self, correlation: &CorrelationConfig) -> Result<(), CorrelationError> {
        // save to memory
        let mut s = self.0.write().await;
        s.retain(|c| c.id != correlation.id);
        s.push(correlation.clone());
        Ok(())
    }

    pub async fn delete(&self, correlation_id: &str) -> Result<(), CorrelationError> {
        // now delete from memory
        let read_access = self.0.read().await;

        let index = read_access
            .iter()
            .enumerate()
            .find(|(_, c)| c.id == correlation_id)
            .to_owned();

        if let Some((index, _)) = index {
            // drop the read access in order to get exclusive write access
            drop(read_access);
            self.0.write().await.remove(index);
            trace!("removed correlation from memory");
        } else {
            warn!("Correlation ID- {correlation_id} not found in memory!");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CorrelationVersion {
    V1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelationConfig {
    pub version: CorrelationVersion,
    pub id: String,
    pub table_configs: Vec<TableConfig>,
    pub join_config: JoinConfig,
    pub filter: Option<FilterQuery>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
}

impl CorrelationConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelationRequest {
    pub version: CorrelationVersion,
    pub table_configs: Vec<TableConfig>,
    pub join_config: JoinConfig,
    pub filter: Option<FilterQuery>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
}

impl From<CorrelationRequest> for CorrelationConfig {
    fn from(val: CorrelationRequest) -> Self {
        Self {
            version: val.version,
            id: get_hash(Utc::now().timestamp_micros().to_string().as_str()),
            table_configs: val.table_configs,
            join_config: val.join_config,
            filter: val.filter,
            start_time: val.start_time,
            end_time: val.end_time,
        }
    }
}

impl CorrelationRequest {
    pub fn generate_correlation_config(self, id: String) -> CorrelationConfig {
        CorrelationConfig {
            version: self.version,
            id,
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
        user_auth_for_query(session_key, &self.table_configs).await?;

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
