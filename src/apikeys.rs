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

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use ulid::Ulid;

use crate::{
    metastore::metastore_traits::MetastoreObject,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    storage::object_storage::apikey_json_path,
};

pub static API_KEYS: Lazy<ApiKeyStore> = Lazy::new(|| ApiKeyStore {
    keys: RwLock::new(HashMap::new()),
});

#[derive(Debug)]
pub struct ApiKeyStore {
    pub keys: RwLock<HashMap<String, HashMap<Ulid, ApiKey>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKey {
    pub key_id: Ulid,
    pub api_key: String,
    pub key_name: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    #[serde(default)]
    pub tenant: Option<String>,
}

/// Request body for creating a new API key
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateApiKeyRequest {
    pub key_name: String,
}

/// Response for list keys (api_key masked to last 4 chars)
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyListEntry {
    pub key_id: Ulid,
    pub api_key: String,
    pub key_name: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
}

impl ApiKey {
    pub fn new(key_name: String, created_by: String, tenant: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            key_id: Ulid::new(),
            api_key: uuid::Uuid::new_v4().to_string(),
            key_name,
            created_by,
            created_at: now,
            modified_at: now,
            tenant,
        }
    }

    pub fn to_list_entry(&self) -> ApiKeyListEntry {
        let masked = if self.api_key.len() >= 4 {
            let last4 = &self.api_key[self.api_key.len() - 4..];
            format!("****{last4}")
        } else {
            "****".to_string()
        };
        ApiKeyListEntry {
            key_id: self.key_id,
            api_key: masked,
            key_name: self.key_name.clone(),
            created_by: self.created_by.clone(),
            created_at: self.created_at,
            modified_at: self.modified_at,
        }
    }
}

impl MetastoreObject for ApiKey {
    fn get_object_path(&self) -> String {
        apikey_json_path(&self.key_id, &self.tenant).to_string()
    }

    fn get_object_id(&self) -> String {
        self.key_id.to_string()
    }
}

impl ApiKeyStore {
    /// Load API keys from object store into memory
    pub async fn load(&self) -> anyhow::Result<()> {
        let api_keys = PARSEABLE.metastore.get_api_keys().await?;
        let mut map = self.keys.write().await;
        for (tenant_id, keys) in api_keys {
            let inner = keys
                .into_iter()
                .map(|mut k| {
                    k.tenant = Some(tenant_id.clone());
                    (k.key_id, k)
                })
                .collect();
            map.insert(tenant_id, inner);
        }
        Ok(())
    }

    /// Create a new API key
    pub async fn create(&self, api_key: ApiKey) -> Result<(), ApiKeyError> {
        let tenant = api_key.tenant.as_deref().unwrap_or(DEFAULT_TENANT);

        // Hold write lock for the entire operation to prevent TOCTOU race
        // on duplicate name check
        let mut map = self.keys.write().await;
        if let Some(tenant_keys) = map.get(tenant)
            && tenant_keys.values().any(|k| k.key_name == api_key.key_name)
        {
            return Err(ApiKeyError::DuplicateKeyName(api_key.key_name));
        }

        PARSEABLE
            .metastore
            .put_api_key(&api_key, &api_key.tenant)
            .await?;

        map.entry(tenant.to_owned())
            .or_default()
            .insert(api_key.key_id, api_key);
        Ok(())
    }

    /// Delete an API key by key_id
    pub async fn delete(
        &self,
        key_id: &Ulid,
        tenant_id: &Option<String>,
    ) -> Result<ApiKey, ApiKeyError> {
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);

        // Read the key first without removing
        let api_key = {
            let map = self.keys.read().await;
            let tenant_keys = map
                .get(tenant)
                .ok_or_else(|| ApiKeyError::KeyNotFound(key_id.to_string()))?;
            tenant_keys
                .get(key_id)
                .cloned()
                .ok_or_else(|| ApiKeyError::KeyNotFound(key_id.to_string()))?
        };

        // Delete from storage first
        PARSEABLE
            .metastore
            .delete_api_key(&api_key, tenant_id)
            .await?;

        // Remove from memory only after successful storage deletion
        {
            let mut map = self.keys.write().await;
            if let Some(tenant_keys) = map.get_mut(tenant) {
                tenant_keys.remove(key_id);
            }
        }

        Ok(api_key)
    }

    /// List all API keys for a tenant (returns masked entries)
    pub async fn list(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<ApiKeyListEntry>, ApiKeyError> {
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let map = self.keys.read().await;
        let entries = if let Some(tenant_keys) = map.get(tenant) {
            tenant_keys.values().map(|k| k.to_list_entry()).collect()
        } else {
            vec![]
        };
        Ok(entries)
    }

    /// Get a specific API key by key_id (returns full key)
    pub async fn get(
        &self,
        key_id: &Ulid,
        tenant_id: &Option<String>,
    ) -> Result<ApiKey, ApiKeyError> {
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let map = self.keys.read().await;
        let tenant_keys = map
            .get(tenant)
            .ok_or_else(|| ApiKeyError::KeyNotFound(key_id.to_string()))?;
        tenant_keys
            .get(key_id)
            .cloned()
            .ok_or_else(|| ApiKeyError::KeyNotFound(key_id.to_string()))
    }

    /// Validate an API key for ingestion. Returns true if the key is valid.
    /// For multi-tenant: checks the key belongs to the specified tenant.
    /// For single-tenant: checks the key exists globally.
    pub async fn validate_key(&self, api_key_value: &str, tenant_id: &Option<String>) -> bool {
        let map = self.keys.read().await;
        if let Some(tenant_id) = tenant_id {
            // Multi-tenant: check keys for the specific tenant
            if let Some(tenant_keys) = map.get(tenant_id) {
                return tenant_keys.values().any(|k| k.api_key == api_key_value);
            }
            false
        } else {
            // Single-tenant: check keys under DEFAULT_TENANT
            if let Some(tenant_keys) = map.get(DEFAULT_TENANT) {
                return tenant_keys.values().any(|k| k.api_key == api_key_value);
            }
            false
        }
    }

    /// Insert an API key directly into memory (used for sync from prism)
    pub async fn sync_put(&self, api_key: ApiKey) {
        let tenant = api_key
            .tenant
            .as_deref()
            .unwrap_or(DEFAULT_TENANT)
            .to_owned();
        let mut map = self.keys.write().await;
        map.entry(tenant)
            .or_default()
            .insert(api_key.key_id, api_key);
    }

    /// Remove an API key from memory (used for sync from prism)
    pub async fn sync_delete(&self, key_id: &Ulid, tenant_id: &Option<String>) {
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let mut map = self.keys.write().await;
        if let Some(tenant_keys) = map.get_mut(tenant) {
            tenant_keys.remove(key_id);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApiKeyError {
    #[error("API key not found: {0}")]
    KeyNotFound(String),

    #[error("Duplicate key name: {0}")]
    DuplicateKeyName(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("{0}")]
    MetastoreError(#[from] crate::metastore::MetastoreError),

    #[error("{0}")]
    AnyhowError(#[from] anyhow::Error),
}

impl actix_web::ResponseError for ApiKeyError {
    fn status_code(&self) -> actix_web::http::StatusCode {
        match self {
            ApiKeyError::KeyNotFound(_) => actix_web::http::StatusCode::NOT_FOUND,
            ApiKeyError::DuplicateKeyName(_) => actix_web::http::StatusCode::CONFLICT,
            ApiKeyError::Unauthorized(_) => actix_web::http::StatusCode::FORBIDDEN,
            ApiKeyError::MetastoreError(_) | ApiKeyError::AnyhowError(_) => {
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}
