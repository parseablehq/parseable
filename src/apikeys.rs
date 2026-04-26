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
//! inside `parseable.json`, alongside native and OAuth users. The backing
//! user's permissions are resolved from the `roles` assigned to it (same
//! mechanism as native and OAuth users).

use std::collections::HashSet;

use serde::Deserialize;

/// Request body for creating a new API key. `roles` is a set of role names
/// that must already exist in the tenant; permissions for the backing user
/// are derived from these roles (same flow as native/OAuth users).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateApiKeyRequest {
    pub key_name: String,
    #[serde(default)]
    pub roles: HashSet<String>,
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
