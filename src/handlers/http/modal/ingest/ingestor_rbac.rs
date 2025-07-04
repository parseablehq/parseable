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

use actix_web::{web, Responder};
use tokio::sync::Mutex;

use crate::{
    handlers::http::{modal::utils::rbac_utils::get_metadata, rbac::RBACError},
    rbac::{
        user::{self, User as ParseableUser},
        Users,
    },
    storage,
};

// async aware lock for updating storage metadata and user map atomicically
static UPDATE_LOCK: Mutex<()> = Mutex::const_new(());

// Handler for POST /api/v1/user/{username}
// Creates a new user by username if it does not exists
pub async fn post_user(
    username: web::Path<String>,
    body: Option<web::Json<serde_json::Value>>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();

    let generated_password = String::default();
    let metadata = get_metadata().await?;
    if let Some(body) = body {
        let user: ParseableUser = serde_json::from_value(body.into_inner())?;
        let _ = storage::put_staging_metadata(&metadata);
        let created_role = user.roles.clone();
        Users.put_user(user.clone());
        Users.add_roles(&username, created_role.clone());
    }

    Ok(generated_password)
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

    let _ = storage::put_staging_metadata(&metadata);

    // update in mem table
    Users.delete_user(&username);
    Ok(format!("deleted user: {username}"))
}

// // Handler PUT /user/{username}/roles => Put roles for user
// // Put roles for given user
// pub async fn put_role(
//     username: web::Path<String>,
//     role: web::Json<HashSet<String>>,
// ) -> Result<String, RBACError> {
//     let username = username.into_inner();
//     let role = role.into_inner();

//     if !Users.contains(&username) {
//         return Err(RBACError::UserDoesNotExist);
//     };
//     // update parseable.json first
//     let mut metadata = get_metadata().await?;
//     if let Some(user) = metadata
//         .users
//         .iter_mut()
//         .find(|user| user.username() == username)
//     {
//         user.roles.clone_from(&role);
//     } else {
//         // should be unreachable given state is always consistent
//         return Err(RBACError::UserDoesNotExist);
//     }

//     let _ = storage::put_staging_metadata(&metadata);
//     // update in mem table
//     Users.add_roles(&username.clone(), role.clone());

//     Ok(format!("Roles updated successfully for {username}"))
// }

// Handler PATCH /user/{username}/role/sync/add => Add roles to a user
pub async fn add_roles_to_user(
    username: web::Path<String>,
    roles_to_add: web::Json<HashSet<String>>,
) -> Result<String, RBACError> {
    let username = username.into_inner();
    let roles_to_add = roles_to_add.into_inner();

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
        user.roles.extend(roles_to_add.clone());
    } else {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserDoesNotExist);
    }

    let _ = storage::put_staging_metadata(&metadata);
    // update in mem table
    Users.add_roles(&username.clone(), roles_to_add.clone());

    Ok(format!("Roles updated successfully for {username}"))
}

// Handler PATCH /user/{username}/role/sync/add => Add roles to a user
pub async fn remove_roles_from_user(
    username: web::Path<String>,
    roles_to_remove: web::Json<HashSet<String>>,
) -> Result<String, RBACError> {
    let username = username.into_inner();
    let roles_to_remove = roles_to_remove.into_inner();

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
        let diff: HashSet<String> =
            HashSet::from_iter(user.roles.difference(&roles_to_remove).cloned());
        user.roles = diff;
    } else {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserDoesNotExist);
    }

    let _ = storage::put_staging_metadata(&metadata);
    // update in mem table
    Users.remove_roles(&username.clone(), roles_to_remove.clone());

    Ok(format!("Roles updated successfully for {username}"))
}

// Handler for POST /api/v1/user/{username}/generate-new-password
// Resets password for the user to a newly generated one and returns it
pub async fn post_gen_password(username: web::Path<String>) -> Result<impl Responder, RBACError> {
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
