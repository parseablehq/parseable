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

use std::collections::HashSet;

use crate::{
    option::CONFIG,
    rbac::{
        role::model::DefaultPrivilege,
        user::{PassCode, User},
        Users,
    },
    storage::{self, ObjectStorageError, StorageMetadata},
    validator::{self, error::UsernameValidationError},
};
use actix_web::{http::header::ContentType, web, Responder};
use http::StatusCode;
use tokio::sync::Mutex;

// async aware lock for updating storage metadata and user map atomicically
static UPDATE_LOCK: Mutex<()> = Mutex::const_new(());

// Handler for GET /api/v1/user
// returns list of all registerd users
pub async fn list_users() -> impl Responder {
    web::Json(Users.list_users())
}

// Handler for PUT /api/v1/user/{username}
// Creates a new user by username if it does not exists
// Otherwise make a call to reset password
// returns password generated for this user
pub async fn put_user(
    username: web::Path<String>,
    body: Option<web::Json<serde_json::Value>>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    validator::user_name(&username)?;
    if username == CONFIG.parseable.username {
        return Err(RBACError::BadUser);
    }
    let _ = UPDATE_LOCK.lock().await;
    if Users.contains(&username) {
        reset_password(username).await
    } else {
        let mut metadata = get_metadata().await?;
        if metadata.users.iter().any(|user| user.username == username) {
            // should be unreachable given state is always consistent
            return Err(RBACError::UserExists);
        }
        let (user, password) = User::create_new(username.clone());
        metadata.users.push(user.clone());
        put_metadata(&metadata).await?;
        // set this user to user map
        Users.put_user(user);

        if let Some(body) = body {
            put_role(web::Path::<String>::from(username), body).await?;
        }

        Ok(password)
    }
}

// Handler for GET /api/v1/user/{username}/role
// returns role for a user if that user exists
pub async fn get_role(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };

    Ok(web::Json(Users.get_role(&username)))
}

// Handler for DELETE /api/v1/user/delete/{username}
pub async fn delete_user(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    if username == CONFIG.parseable.username {
        return Err(RBACError::BadUser);
    }
    let _ = UPDATE_LOCK.lock().await;
    // fail this request if the user does not exists
    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    // delete from parseable.json first
    let mut metadata = get_metadata().await?;
    metadata.users.retain(|user| user.username != username);
    put_metadata(&metadata).await?;
    // update in mem table
    Users.delete_user(&username);
    Ok(format!("deleted user: {username}"))
}

// Reset password for given username
// returns new password generated for this user
pub async fn reset_password(username: String) -> Result<String, RBACError> {
    // generate new password for this user
    let PassCode { password, hash } = User::gen_new_password();
    // update parseable.json first
    let mut metadata = get_metadata().await?;
    if let Some(user) = metadata
        .users
        .iter_mut()
        .find(|user| user.username == username)
    {
        user.password_hash.clone_from(&hash);
    } else {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserDoesNotExist);
    }
    put_metadata(&metadata).await?;

    // update in mem table
    Users.change_password_hash(&username, &hash);
    Ok(password)
}

// Put roles for given user
pub async fn put_role(
    username: web::Path<String>,
    role: web::Json<serde_json::Value>,
) -> Result<String, RBACError> {
    let username = username.into_inner();
    if username == CONFIG.parseable.username {
        return Err(RBACError::BadUser);
    }
    let role = role.into_inner();
    let role: HashSet<DefaultPrivilege> = serde_json::from_value(role)?;
    let role = role.into_iter().collect();

    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    // update parseable.json first
    let mut metadata = get_metadata().await?;
    if let Some(user) = metadata
        .users
        .iter_mut()
        .find(|user| user.username == username)
    {
        user.role.clone_from(&role);
    } else {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserDoesNotExist);
    }

    put_metadata(&metadata).await?;
    // update in mem table
    Users.put_role(&username, role);
    Ok(format!("Roles updated successfully for {username}"))
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
pub enum RBACError {
    #[error("Request cannot be allowed for this user")]
    BadUser,
    #[error("User exists already")]
    UserExists,
    #[error("User does not exist")]
    UserDoesNotExist,
    #[error("{0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Failed to connect to storage: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("invalid Username: {0}")]
    ValidationError(#[from] UsernameValidationError),
}

impl actix_web::ResponseError for RBACError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::BadUser => StatusCode::BAD_REQUEST,
            Self::UserExists => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist => StatusCode::NOT_FOUND,
            Self::SerdeError(_) => StatusCode::BAD_REQUEST,
            Self::ValidationError(_) => StatusCode::BAD_REQUEST,
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
