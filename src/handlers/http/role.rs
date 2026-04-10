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
use actix_web::{
    HttpRequest, HttpResponse, Responder,
    http::header::ContentType,
    web::{self, Json},
};

use tracing::instrument;

use crate::rbac::map::roles;
use crate::rbac::role::model::{Role, RoleType, RoleUI};
use crate::{
    parseable::{DEFAULT_TENANT, PARSEABLE},
    rbac::map::{DEFAULT_ROLE, mut_roles, mut_sessions, read_user_groups, users},
    storage::{self, ObjectStorageError, StorageMetadata},
    utils::get_tenant_id_from_request,
    validator::{self, error::UsernameValidationError},
};

// Handler for PUT /api/v1/role/{name}
// Creates a new role or update existing one
pub async fn put(
    req: HttpRequest,
    name: web::Path<String>,
    Json(role): Json<Role>,
) -> Result<impl Responder, RoleError> {
    // internal role manipulation not allowed
    if role.role_type().eq(&RoleType::Internal) {
        return Err(RoleError::ProtectedRole);
    }
    if role.deny_super_admin() {
        return Err(RoleError::SuperAdminPrivilege);
    }
    let name = name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);

    // validate the role name
    validator::user_role_name(&name).map_err(RoleError::ValidationError)?;

    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.roles.insert(name.clone(), role.clone());

    put_metadata(&metadata, &tenant_id).await?;

    let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    mut_roles()
        .entry(tenant_id.to_owned())
        .or_default()
        .insert(name.clone(), role.clone());

    // refresh the sessions of all users using this role
    // for this, iterate over all user_groups and users and create a hashset of users
    let mut session_refresh_users: HashSet<String> = HashSet::new();
    if let Some(groups) = read_user_groups().get(tenant_id) {
        for user_group in groups.values() {
            if user_group.roles.contains(&name) {
                session_refresh_users
                    .extend(user_group.users.iter().map(|u| u.userid().to_string()));
            }
        }
    }

    // iterate over all users to see if they have this role
    if let Some(users) = users().get(tenant_id) {
        for user in users.values() {
            if user.roles.contains(&name) {
                session_refresh_users.insert(user.userid().to_string());
            }
        }
    }

    for userid in session_refresh_users {
        mut_sessions().remove_user(&userid, tenant_id);
    }

    Ok(HttpResponse::Ok().finish())
}

// Handler for GET /api/v1/role/{name}
// Fetch role by name
pub async fn get(req: HttpRequest, name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let metadata = get_metadata(&tenant_id).await?;
    let role = metadata.roles.get(&name).cloned().unwrap_or_default();
    if role.role_type().eq(&RoleType::Internal) {
        return Err(RoleError::ProtectedRole);
    }
    Ok(web::Json(role))
}

// Handler for GET /api/v1/role
// Fetch all roles in the system
#[instrument(name = "GET /role", skip(req), fields(http.route = "/role"))]
pub async fn list(req: HttpRequest) -> Result<impl Responder, RoleError> {
    let tenant_id = get_tenant_id_from_request(&req);
    let metadata = get_metadata(&tenant_id).await?;
    let mut roles = HashMap::new();
    for (k, r) in metadata.roles.into_iter() {
        if !r.role_type().eq(&RoleType::Internal) {
            roles.insert(k, RoleUI(r));
        }
    }

    Ok(web::Json(roles))
}

// Handler for DELETE /api/v1/role/{name}
// Delete existing role
pub async fn delete(
    req: HttpRequest,
    name: web::Path<String>,
) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    if let Some(tenant_roles) = roles().get(tenant)
        && let Some(role) = tenant_roles.get(&name)
        && role.role_type().eq(&RoleType::Internal)
    {
        return Err(RoleError::ProtectedRole);
    }

    // check if the role is being used by any user or group
    let mut metadata = get_metadata(&tenant_id).await?;
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
    put_metadata(&metadata, &tenant_id).await?;

    mut_roles()
        .entry(tenant.to_owned())
        .or_default()
        .remove(&name);
    // mut_roles().remove(&name);

    Ok(HttpResponse::Ok().finish())
}

// Handler for PUT /api/v1/role/default
// Delete existing role
#[instrument(name = "PUT /role/default", skip(req, name), fields(http.route = "/role/default"))]
pub async fn put_default(
    req: HttpRequest,
    name: web::Json<String>,
) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.default_role = Some(name.clone());
    DEFAULT_ROLE
        .write()
        // .unwrap()
        .insert(
            tenant_id.as_deref().unwrap_or(DEFAULT_TENANT).to_owned(),
            Some(name),
        );
    // *DEFAULT_ROLE.lock().unwrap() = Some(name);
    put_metadata(&metadata, &tenant_id).await?;
    Ok(HttpResponse::Ok().finish())
}

// Handler for GET /api/v1/role/default
// Delete existing role
pub async fn get_default(req: HttpRequest) -> Result<impl Responder, RoleError> {
    let tenant_id = get_tenant_id_from_request(&req);
    let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let res = if let Some(role) = DEFAULT_ROLE
        .read()
        // .unwrap()
        .get(tenant_id)
        && let Some(role) = role
    {
        serde_json::Value::String(role.to_string())
    } else {
        serde_json::Value::Null
    };
    // let res = match DEFAULT_ROLE
    //     .read()
    //     .unwrap()
    //     .get()
    // {
    //     Some(role) => serde_json::Value::String(role),
    //     None => serde_json::Value::Null,
    // };

    Ok(web::Json(res))
}

#[instrument(name = "get_metadata", skip_all)]
async fn get_metadata(
    tenant_id: &Option<String>,
) -> Result<crate::storage::StorageMetadata, ObjectStorageError> {
    let metadata = PARSEABLE
        .metastore
        .get_parseable_metadata(tenant_id)
        .await
        .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?
        .ok_or_else(|| ObjectStorageError::Custom("parseable metadata not initialized".into()))?;
    Ok(serde_json::from_slice::<StorageMetadata>(&metadata)?)
}

#[instrument(name = "put_metadata", skip_all)]
async fn put_metadata(
    metadata: &StorageMetadata,
    tenant_id: &Option<String>,
) -> Result<(), ObjectStorageError> {
    storage::put_remote_metadata(metadata, tenant_id).await?;
    storage::put_staging_metadata(metadata, tenant_id)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum RoleError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("Cannot perform this operation as role is assigned to an existing user.")]
    RoleInUse,
    #[error("Cannot perform this operation as role is assigned to a protected user.")]
    ProtectedRole,
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("{0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Network Error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Validation Error: {0}")]
    ValidationError(#[from] UsernameValidationError),
    #[error("Cannot create a role with superadmin privilege.")]
    SuperAdminPrivilege,
}

impl actix_web::ResponseError for RoleError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RoleInUse => StatusCode::BAD_REQUEST,
            Self::SuperAdminPrivilege => StatusCode::BAD_REQUEST,
            Self::ProtectedRole => StatusCode::BAD_REQUEST,
            Self::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::SerdeError(_) => StatusCode::BAD_REQUEST,
            Self::Network(_) => StatusCode::BAD_GATEWAY,
            Self::ValidationError(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
