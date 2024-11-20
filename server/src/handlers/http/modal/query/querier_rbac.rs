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

use actix_web::{http::header::HeaderMap, web, Responder};
use bytes::Bytes;
use http::StatusCode;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::{
    handlers::http::{
        cluster::{
            sync_password_reset_with_ingestors, sync_user_creation_with_ingestors,
            sync_user_deletion_with_ingestors, sync_users_with_roles_with_ingestors,
            sync_with_queriers,
        },
        modal::{
            utils::rbac_utils::{get_metadata, put_metadata},
            LEADER,
        },
        rbac::RBACError,
    },
    rbac::{
        user::{self, User},
        Users,
    },
    storage, validator,
};

use super::{LeaderRequest, Method};

// async aware lock for updating storage metadata and user map atomicically
static UPDATE_LOCK: Mutex<()> = Mutex::const_new(());

// Handler for POST /api/v1/user/{username}
// Creates a new user by username if it does not exists
pub async fn post_user(
    username: web::Path<String>,
    body: Option<Bytes>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let role_body: Value = if let Some(role_body) = body.as_ref() {
        serde_json::from_slice(role_body)?
    } else {
        return Err(RBACError::RoleValidationError);
    };

    if LEADER.lock().await.is_leader() {
        let mut metadata = get_metadata().await?;

        put_metadata(&metadata).await?;

        validator::user_name(&username)?;
        let roles: HashSet<String> = serde_json::from_value(role_body)?;

        if roles.is_empty() {
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

        metadata.users.push(user.clone());

        let created_role = roles.clone();
        Users.put_user(user.clone());

        put_role(
            web::Path::<String>::from(username.clone()),
            web::Json(created_role),
        )
        .await?;

        sync_user_creation_with_ingestors(user.clone(), &Some(roles)).await?;
        let u = serde_json::to_string(&user).unwrap().as_bytes().to_vec();
        sync_with_queriers(
            HeaderMap::new(),
            Some(u.into()),
            &format!("user/{username}/sync"),
            Method::Post,
        )
        .await?;
        Ok(password)
    } else {
        let resource = username.to_string();
        let request = LeaderRequest {
            body,
            api: "user",
            resource: Some(&resource),
            method: Method::Post,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(res.text().await?),
            _ => {
                let err_msg = res.text().await?;
                Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

/// This is a special case API endpoint
/// which should only be called by Query Nodes leader
/// to sync user creation with follower nodes
pub async fn post_user_sync(
    username: web::Path<String>,
    body: Option<web::Json<serde_json::Value>>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();

    let metadata = get_metadata().await?;
    if let Some(body) = body {
        let user: User = serde_json::from_value(body.into_inner())?;
        let _ = storage::put_staging_metadata(&metadata);
        let created_role = user.roles.clone();
        Users.put_user(user.clone());
        Users.put_role(&username, created_role.clone());
    }

    Ok("Synced user")
}

// Handler for DELETE /api/v1/user/{username}
pub async fn delete_user(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();

    if LEADER.lock().await.is_leader() {
        let _ = UPDATE_LOCK.lock().await;
        // fail this request if the user does not exists
        if !Users.contains(&username) {
            return Err(RBACError::UserDoesNotExist);
        };
        // delete from parseable.json first
        let mut metadata = get_metadata().await?;
        metadata.users.retain(|user| user.username() != username);

        // update in mem table
        Users.delete_user(&username);

        put_metadata(&metadata).await?;
        sync_user_deletion_with_ingestors(&username).await?;
        sync_with_queriers(
            HeaderMap::new(),
            None,
            &format!("user/{username}/sync"),
            Method::Delete,
        )
        .await?;
        Ok(format!("deleted user: {username}"))
    } else {
        let resource = username.to_string();
        let request = LeaderRequest {
            body: None,
            api: "user",
            resource: Some(&resource),
            method: Method::Delete,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(format!("deleted user: {username}")),
            _ => {
                let err_msg = res.text().await?;
                Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

// Handler for DELETE /api/v1/user/{username}
pub async fn delete_user_sync(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let _ = UPDATE_LOCK.lock().await;
    // fail this request if the user does not exists
    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    // delete from parseable.json first
    let mut metadata = get_metadata().await?;
    metadata.users.retain(|user| user.username() != username);

    // update in mem table
    Users.delete_user(&username);

    storage::put_staging_metadata(&metadata).map_err(|err| {
        log::error!("error while deleting user: {err:?}");
        RBACError::Anyhow(anyhow::Error::msg(err))
    })?;

    Ok(format!("deleted user: {username}"))
}

// Handler PUT /user/{username}/role => Put roles for user
// Put roles for given user
pub async fn put_role(
    username: web::Path<String>,
    role: web::Json<HashSet<String>>,
) -> Result<String, RBACError> {
    let username = username.into_inner();
    let role = role.into_inner();

    if LEADER.lock().await.is_leader() {
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

        // update in mem table
        Users.put_role(&username.clone(), role.clone());

        put_metadata(&metadata).await?;
        sync_users_with_roles_with_ingestors(&username, &role).await?;

        let body = role.into_iter().fold(Vec::new(), |mut acc, v| {
            acc.extend_from_slice(v.as_bytes());
            acc
        });
        sync_with_queriers(
            HeaderMap::new(),
            Some(body.into()),
            &format!("user/{username}/role/sync"),
            Method::Put,
        )
        .await?;
        Ok(format!("Roles updated successfully for {username}"))
    } else {
        let resource = format!("{username}/role");
        let request = LeaderRequest {
            body: None,
            api: "user",
            resource: Some(&resource),
            method: Method::Put,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(format!("Roles updated successfully for {username}")),
            _ => {
                let err_msg = res.text().await?;
                Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

// Handler PUT /user/{username}/role => Put roles for user
// Put roles for given user
pub async fn put_role_sync(
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

    // update in mem table
    Users.put_role(&username.clone(), role.clone());

    storage::put_staging_metadata(&metadata).map_err(|err| {
        log::error!("error while put role- {err:?}");
        RBACError::Anyhow(anyhow::Error::msg(err.to_string()))
    })?;

    Ok(format!("Roles updated successfully for {username}"))
}

// Handler for POST /api/v1/user/{username}/generate-new-password
// Resets password for the user to a newly generated one and returns it
pub async fn post_gen_password(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();

    if LEADER.lock().await.is_leader() {
        let mut new_password = String::default();
        let mut new_hash = String::default();
        let mut metadata = get_metadata().await?;

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

        Users.change_password_hash(&username, &new_hash);

        put_metadata(&metadata).await?;
        sync_password_reset_with_ingestors(&username).await?;
        sync_with_queriers(
            HeaderMap::new(),
            None,
            &format!("{username}/generate-new-password/sync"),
            Method::Post,
        )
        .await?;
        Ok(new_password)
    } else {
        let resource = format!("{username}/generate-new-password");
        let request = LeaderRequest {
            body: None,
            api: "user",
            resource: Some(&resource),
            method: Method::Post,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(res.text().await?),
            _ => {
                let err_msg = res.text().await?;
                Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

/// This is a special case API endpoint
/// which should only be called by Query Nodes leader
/// to sync user creation with follower nodes
pub async fn post_gen_password_sync(
    username: web::Path<String>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let mut new_hash = String::default();
    let mut metadata = get_metadata().await?;

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
    Users.change_password_hash(&username, &new_hash);

    Ok("Updated")
}
