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

use crate::{
    handlers::http::cluster::sync_users_with_roles_with_ingestors,
    option::{Mode, CONFIG},
    rbac::{
        map::roles,
        role::model::DefaultPrivilege,
        user::{self, User as ParseableUser},
        Users,
    },
    storage::{self, ObjectStorageError, StorageMetadata},
    validator::{self, error::UsernameValidationError},
};
use actix_web::{http::header::ContentType, web, Responder};
use http::StatusCode;
use tokio::sync::Mutex;

use super::cluster::{
    sync_password_reset_with_ingestors, sync_user_creation_with_ingestors,
    sync_user_deletion_with_ingestors,
};

// async aware lock for updating storage metadata and user map atomicically
static UPDATE_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(serde::Serialize)]
struct User {
    id: String,
    method: String,
}

impl From<&user::User> for User {
    fn from(user: &user::User) -> Self {
        let method = match user.ty {
            user::UserType::Native(_) => "native".to_string(),
            user::UserType::OAuth(_) => "oauth".to_string(),
        };

        User {
            id: user.username().to_owned(),
            method,
        }
    }
}

// Handler for GET /api/v1/user
// returns list of all registerd users
pub async fn list_users() -> impl Responder {
    web::Json(Users.collect_user::<User>())
}

// Handler for POST /api/v1/user/{username}
// Creates a new user by username if it does not exists
pub async fn post_user(
    username: web::Path<String>,
    body: Option<web::Json<serde_json::Value>>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();

    let mut generated_password = String::default();
    let mut created_role: HashSet<String> = HashSet::new();
    let mut metadata = get_metadata().await?;
    if CONFIG.parseable.mode == Mode::Ingest {
        let user: Option<ParseableUser> = body
            .map(|body| serde_json::from_value(body.into_inner()))
            .transpose()?;
        let _ = storage::put_staging_metadata(&metadata);
        created_role.clone_from(&user.unwrap().roles);
    } else {
        validator::user_name(&username)?;
        let roles: Option<HashSet<String>> = body
            .map(|body| serde_json::from_value(body.into_inner()))
            .transpose()?;
        if roles.is_none() || roles.as_ref().unwrap().is_empty() {
            return Err(RBACError::RoleValidationError);
        }
        let _ = UPDATE_LOCK.lock().await;
        if Users.contains(&username)
            || metadata
                .users
                .iter()
                .any(|user| user.username() == username)
        {
            return Err(RBACError::UserExists);
        }

        let (user, password) = user::User::new_basic(username.clone());

        generated_password = password;
        metadata.users.push(user.clone());

        put_metadata(&metadata).await?;
        if CONFIG.parseable.mode == Mode::Query {
            sync_user_creation_with_ingestors(user, &roles).await?;
        }
        if let Some(roles) = roles {
            created_role.clone_from(&roles);
        }
    }
    let created_user = metadata
        .users
        .iter()
        .find(|user| user.username() == username)
        .unwrap();
    Users.put_user(created_user.clone());
    put_role(web::Path::<String>::from(username), web::Json(created_role)).await?;
    Ok(generated_password)
}

// Handler for POST /api/v1/user/{username}/generate-new-password
// Resets password for the user to a newly generated one and returns it
pub async fn post_gen_password(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let mut new_password = String::default();
    let mut new_hash = String::default();
    let mut metadata = get_metadata().await?;
    if CONFIG.parseable.mode == Mode::Ingest {
        let _ = storage::put_staging_metadata(&metadata);
        if let Some(user) = metadata
            .users
            .iter_mut()
            .filter_map(|user| match user.ty {
                user::UserType::Native(ref mut user) => Some(user),
                _ => None,
            })
            .find(|user| user.username == username)
        {
            new_hash.clone_from(&user.password_hash);
        } else {
            return Err(RBACError::UserDoesNotExist);
        }
    } else {
        let _ = UPDATE_LOCK.lock().await;
        let user::PassCode { password, hash } = user::Basic::gen_new_password();
        new_password.clone_from(&password);
        new_hash.clone_from(&hash);
        if let Some(user) = metadata
            .users
            .iter_mut()
            .filter_map(|user| match user.ty {
                user::UserType::Native(ref mut user) => Some(user),
                _ => None,
            })
            .find(|user| user.username == username)
        {
            user.password_hash.clone_from(&hash);
        } else {
            return Err(RBACError::UserDoesNotExist);
        }
        put_metadata(&metadata).await?;
        if CONFIG.parseable.mode == Mode::Query {
            sync_password_reset_with_ingestors(&username).await?;
        }
    }
    Users.change_password_hash(&username, &new_hash);
    Ok(new_password)
}

// Handler for GET /api/v1/user/{username}/role
// returns role for a user if that user exists
pub async fn get_role(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    let res: HashMap<String, Vec<DefaultPrivilege>> = Users
        .get_role(&username)
        .iter()
        .filter_map(|role_name| {
            roles()
                .get(role_name)
                .map(|role| (role_name.to_owned(), role.clone()))
        })
        .collect();

    Ok(web::Json(res))
}

// Handler for DELETE /api/v1/user/delete/{username}
pub async fn delete_user(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let _ = UPDATE_LOCK.lock().await;
    // fail this request if the user does not exists
    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    // delete from parseable.json first
    let mut metadata = get_metadata().await?;
    metadata.users.retain(|user| user.username() != username);

    if CONFIG.parseable.mode == Mode::Ingest {
        let _ = storage::put_staging_metadata(&metadata);
    } else {
        put_metadata(&metadata).await?;
        if CONFIG.parseable.mode == Mode::Query {
            sync_user_deletion_with_ingestors(&username).await?;
        }
    }

    // update in mem table
    Users.delete_user(&username);
    Ok(format!("deleted user: {username}"))
}

// Handler PUT /user/{username}/roles => Put roles for user
// Put roles for given user
pub async fn put_role(
    username: web::Path<String>,
    role: web::Json<HashSet<String>>,
) -> Result<String, RBACError> {
    let username = username.into_inner();
    let role = role.into_inner();

    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    // update parseable.json first
    let mut metadata = get_metadata().await?;
    if let Some(user) = metadata
        .users
        .iter_mut()
        .find(|user| user.username() == username)
    {
        user.roles.clone_from(&role);
    } else {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserDoesNotExist);
    }

    if CONFIG.parseable.mode == Mode::Ingest {
        let _ = storage::put_staging_metadata(&metadata);
    } else {
        put_metadata(&metadata).await?;
        if CONFIG.parseable.mode == Mode::Query {
            sync_users_with_roles_with_ingestors(&username, &role).await?;
        }
    }

    // update in mem table
    Users.put_role(&username.clone(), role.clone());
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
    #[error("Network Error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("User cannot be created without a role")]
    RoleValidationError,
}

impl actix_web::ResponseError for RBACError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::UserExists => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist => StatusCode::NOT_FOUND,
            Self::SerdeError(_) => StatusCode::BAD_REQUEST,
            Self::ValidationError(_) => StatusCode::BAD_REQUEST,
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Network(_) => StatusCode::BAD_GATEWAY,
            Self::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RoleValidationError => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
