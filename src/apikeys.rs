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

//! API key primitives.
//!
//! API keys are persisted as a third `UserType` variant (`UserType::ApiKey`)
//! inside `parseable.json`, alongside native and OAuth users. This module
//! defines the key's type enum (which determines the user's global privileges)
//! and the request/response shapes used by the enterprise CRUD handlers.

use serde::{Deserialize, Serialize};

use crate::rbac::role::{Permission, RoleBuilder, model::DefaultPrivilege};

/// Type of API key, determining the global privileges the backing user gets
/// across the org/tenant. Mirrors the built-in `DefaultPrivilege` levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyType {
    /// Global ingestion across all streams in the tenant/org.
    #[default]
    Ingestion,
    /// Global query access across all streams in the tenant/org.
    Query,
    /// Full admin privileges within the tenant/org.
    Admin,
    /// Editor privileges within the tenant/org (same set granted to editor users).
    Editor,
}

impl KeyType {
    /// Map the key type to the equivalent `DefaultPrivilege`. Permissions for
    /// an API-key-backed user are derived from this privilege rather than from
    /// role-name lookup, so no role needs to exist per-tenant.
    pub fn default_privilege(&self) -> DefaultPrivilege {
        match self {
            KeyType::Ingestion => DefaultPrivilege::Ingestor { resource: None },
            KeyType::Query => DefaultPrivilege::Reader { resource: None },
            KeyType::Admin => DefaultPrivilege::Admin,
            KeyType::Editor => DefaultPrivilege::Editor,
        }
    }

    /// Build the flat permission set granted by this key type.
    pub fn permissions(&self) -> Vec<Permission> {
        RoleBuilder::from(&self.default_privilege()).build()
    }
}

/// Request body for creating a new API key. When `keyType` is omitted,
/// defaults to `KeyType::Ingestion` (least-privileged option).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateApiKeyRequest {
    pub key_name: String,
    #[serde(default)]
    pub key_type: KeyType,
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
    Storage(#[from] crate::storage::ObjectStorageError),

    #[error("{0}")]
    Rbac(#[from] crate::handlers::http::rbac::RBACError),

    #[error("{0}")]
    Anyhow(#[from] anyhow::Error),
}

impl actix_web::ResponseError for ApiKeyError {
    fn status_code(&self) -> actix_web::http::StatusCode {
        match self {
            ApiKeyError::KeyNotFound(_) => actix_web::http::StatusCode::NOT_FOUND,
            ApiKeyError::DuplicateKeyName(_) => actix_web::http::StatusCode::CONFLICT,
            ApiKeyError::Unauthorized(_) => actix_web::http::StatusCode::FORBIDDEN,
            ApiKeyError::Storage(_) | ApiKeyError::Rbac(_) | ApiKeyError::Anyhow(_) => {
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}
