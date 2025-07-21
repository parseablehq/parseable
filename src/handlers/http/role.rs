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

use actix_web::{
    HttpResponse, Responder,
    http::header::ContentType,
    web::{self, Json},
};
use http::StatusCode;

use crate::{
    parseable::PARSEABLE,
    rbac::{
        map::{DEFAULT_ROLE, mut_roles, mut_sessions, read_user_groups, users},
        role::model::DefaultPrivilege,
    },
    storage::{self, ObjectStorageError, StorageMetadata},
};

// Handler for PUT /api/v1/role/{name}
// Creates a new role or update existing one
pub async fn put(
    name: web::Path<String>,
    Json(privileges): Json<Vec<DefaultPrivilege>>,
) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let mut metadata = get_metadata().await?;
    metadata.roles.insert(name.clone(), privileges.clone());

    put_metadata(&metadata).await?;
    mut_roles().insert(name.clone(), privileges.clone());

    // refresh the sessions of all users using this role
    // for this, iterate over all user_groups and users and create a hashset of users
    let mut session_refresh_users: HashSet<String> = HashSet::new();
    for user_group in read_user_groups().values().cloned() {
        if user_group.roles.contains(&name) {
            session_refresh_users.extend(user_group.users);
        }
    }

    // iterate over all users to see if they have this role
    for user in users().values().cloned() {
        if user.roles.contains(&name) {
            session_refresh_users.insert(user.username().to_string());
        }
    }

    for username in session_refresh_users {
        mut_sessions().remove_user(&username);
    }

    Ok(HttpResponse::Ok().finish())
}

// Handler for GET /api/v1/role/{name}
// Fetch role by name
pub async fn get(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let metadata = get_metadata().await?;
    let privileges = metadata.roles.get(&name).cloned().unwrap_or_default();
    Ok(web::Json(privileges))
}

// Handler for GET /api/v1/role
// Fetch all roles in the system
pub async fn list() -> Result<impl Responder, RoleError> {
    let metadata = get_metadata().await?;
    let roles: Vec<String> = metadata.roles.keys().cloned().collect();
    Ok(web::Json(roles))
}

// Handler for GET /api/v1/roles
// Fetch all roles in the system
pub async fn list_roles() -> Result<impl Responder, RoleError> {
    let metadata = get_metadata().await?;
    let roles = metadata.roles.clone();
    Ok(web::Json(roles))
}

// Handler for DELETE /api/v1/role/{name}
// Delete existing role
pub async fn delete(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    // check if the role is being used by any user or group
    let mut metadata = get_metadata().await?;
    if metadata.users.iter().any(|user| user.roles.contains(&name)) {
        return Err(RoleError::RoleInUse);
    }
    if metadata
        .user_groups
        .iter()
        .any(|user_group| user_group.roles.contains(&name))
    {
        return Err(RoleError::RoleInUse);
    }
    metadata.roles.remove(&name);
    put_metadata(&metadata).await?;
    mut_roles().remove(&name);

    Ok(HttpResponse::Ok().finish())
}

// Handler for PUT /api/v1/role/default
// Delete existing role
pub async fn put_default(name: web::Json<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let mut metadata = get_metadata().await?;
    metadata.default_role = Some(name.clone());
    *DEFAULT_ROLE.lock().unwrap() = Some(name);
    put_metadata(&metadata).await?;
    Ok(HttpResponse::Ok().finish())
}

// Handler for GET /api/v1/role/default
// Delete existing role
pub async fn get_default() -> Result<impl Responder, RoleError> {
    let res = match DEFAULT_ROLE.lock().unwrap().clone() {
        Some(role) => serde_json::Value::String(role),
        None => serde_json::Value::Null,
    };

    Ok(web::Json(res))
}

async fn get_metadata() -> Result<crate::storage::StorageMetadata, ObjectStorageError> {
    let metadata = PARSEABLE
        .storage
        .get_object_store()
        .get_metadata()
        .await?
        .expect("metadata is initialized");
    Ok(metadata)
}

async fn put_metadata(metadata: &StorageMetadata) -> Result<(), ObjectStorageError> {
    storage::put_remote_metadata(metadata).await?;
    storage::put_staging_metadata(metadata)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum RoleError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("Cannot perform this operation as role is assigned to an existing user.")]
    RoleInUse,
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("{0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Network Error: {0}")]
    Network(#[from] reqwest::Error),
}

impl actix_web::ResponseError for RoleError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RoleInUse => StatusCode::BAD_REQUEST,
            Self::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::SerdeError(_) => StatusCode::BAD_REQUEST,
            Self::Network(_) => StatusCode::BAD_GATEWAY,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
