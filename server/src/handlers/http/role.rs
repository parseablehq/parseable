/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use actix_web::{http::header::ContentType, web, HttpResponse, Responder};
use http::StatusCode;

use crate::{
    option::CONFIG,
    rbac::{map::mut_roles, role::model::DefaultPrivilege},
    storage::{self, ObjectStorageError, StorageMetadata},
};

// Handler for POST /api/v1/role/{name}
// Creates a new user by username if it does not exists
pub async fn put(
    name: web::Path<String>,
    body: web::Json<Vec<DefaultPrivilege>>,
) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let privileges = body.into_inner();
    let mut metadata = get_metadata().await?;
    metadata.roles.insert(name.clone(), privileges.clone());
    put_metadata(&metadata).await?;
    mut_roles().insert(name, privileges);
    Ok(HttpResponse::Ok().finish())
}

// Handler for GET /api/v1/role/{name}
// Creates a new user by username if it does not exists
pub async fn get(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let metadata = get_metadata().await?;
    let privileges = metadata.roles.get(&name).cloned().unwrap_or_default();
    Ok(web::Json(privileges))
}

// Handler for POST /api/v1/role
// Creates a new user by username if it does not exists
pub async fn list() -> Result<impl Responder, RoleError> {
    let metadata = get_metadata().await?;
    let roles: Vec<String> = metadata.roles.keys().cloned().collect();
    Ok(web::Json(roles))
}

// Handler for POST /api/v1/user/{username}
// Creates a new user by username if it does not exists
pub async fn delete(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let mut metadata = get_metadata().await?;
    metadata.roles.remove(&name);
    put_metadata(&metadata).await?;
    mut_roles().remove(&name);
    Ok(HttpResponse::Ok().finish())
}

async fn get_metadata() -> Result<crate::storage::StorageMetadata, ObjectStorageError> {
    let metadata = CONFIG
        .storage()
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
}

impl actix_web::ResponseError for RoleError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
