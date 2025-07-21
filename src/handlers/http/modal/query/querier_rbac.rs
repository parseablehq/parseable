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

use actix_web::{Responder, web};
use tokio::sync::Mutex;

use crate::{
    handlers::http::{
        cluster::{
            sync_password_reset_with_ingestors, sync_user_creation_with_ingestors,
            sync_user_deletion_with_ingestors, sync_users_with_roles_with_ingestors,
        },
        modal::utils::rbac_utils::{get_metadata, put_metadata},
        rbac::RBACError,
    },
    rbac::{
        Users,
        map::{roles, write_user_groups},
        user,
    },
    validator,
};

// async aware lock for updating storage metadata and user map atomically
static UPDATE_LOCK: Mutex<()> = Mutex::const_new(());

// Handler for POST /api/v1/user/{username}
// Creates a new user by username if it does not exists
pub async fn post_user(
    username: web::Path<String>,
    body: Option<web::Json<serde_json::Value>>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();

    let mut metadata = get_metadata().await?;

    validator::user_name(&username)?;
    let user_roles: HashSet<String> = if let Some(body) = body {
        serde_json::from_value(body.into_inner())?
    } else {
        return Err(RBACError::RoleValidationError);
    };

    if user_roles.is_empty() {
        return Err(RBACError::RoleValidationError);
    } else {
        let mut non_existant_roles = Vec::new();
        user_roles
            .iter()
            .map(|r| {
                if !roles().contains_key(r) {
                    non_existant_roles.push(r.clone());
                }
            })
            .for_each(drop);
        if !non_existant_roles.is_empty() {
            return Err(RBACError::RolesDoNotExist(non_existant_roles));
        }
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

    put_metadata(&metadata).await?;
    let created_role = user_roles.clone();
    Users.put_user(user.clone());

    sync_user_creation_with_ingestors(user, &Some(user_roles)).await?;

    add_roles_to_user(
        web::Path::<String>::from(username.clone()),
        web::Json(created_role),
    )
    .await?;

    Ok(password)
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

    // also delete from user groups
    let user_groups = Users.get_user_groups(&username);
    let mut groups_to_update = Vec::new();
    for user_group in user_groups {
        if let Some(ug) = write_user_groups().get_mut(&user_group) {
            ug.remove_users(HashSet::from_iter([username.clone()]))?;
            groups_to_update.push(ug.clone());
            // ug.update_in_metadata().await?;
        } else {
            continue;
        };
    }

    // update in metadata user group
    metadata
        .user_groups
        .retain(|x| !groups_to_update.contains(x));
    metadata.user_groups.extend(groups_to_update);
    put_metadata(&metadata).await?;

    sync_user_deletion_with_ingestors(&username).await?;

    // update in mem table
    Users.delete_user(&username);
    Ok(format!("deleted user: {username}"))
}

// Handler PATCH /user/{username}/role/add => Add roles to a user
pub async fn add_roles_to_user(
    username: web::Path<String>,
    roles_to_add: web::Json<HashSet<String>>,
) -> Result<String, RBACError> {
    let username = username.into_inner();
    let roles_to_add = roles_to_add.into_inner();

    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };

    let mut non_existant_roles = Vec::new();

    // check if the role exists
    roles_to_add.iter().for_each(|r| {
        if roles().get(r).is_none() {
            non_existant_roles.push(r.clone());
        }
    });

    if !non_existant_roles.is_empty() {
        return Err(RBACError::RolesDoNotExist(non_existant_roles));
    }

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

    put_metadata(&metadata).await?;
    // update in mem table
    Users.add_roles(&username.clone(), roles_to_add.clone());

    sync_users_with_roles_with_ingestors(&username, &roles_to_add, "add").await?;

    Ok(format!("Roles updated successfully for {username}"))
}

// Handler PATCH /user/{username}/role/remove => Remove roles from a user
pub async fn remove_roles_from_user(
    username: web::Path<String>,
    roles_to_remove: web::Json<HashSet<String>>,
) -> Result<String, RBACError> {
    let username = username.into_inner();
    let roles_to_remove = roles_to_remove.into_inner();

    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };

    let mut non_existant_roles = Vec::new();

    // check if the role exists
    roles_to_remove.iter().for_each(|r| {
        if roles().get(r).is_none() {
            non_existant_roles.push(r.clone());
        }
    });

    if !non_existant_roles.is_empty() {
        return Err(RBACError::RolesDoNotExist(non_existant_roles));
    }

    // check for role not present with user
    let user_roles: HashSet<String> = HashSet::from_iter(Users.get_role(&username));
    let roles_not_with_user: HashSet<String> =
        HashSet::from_iter(roles_to_remove.difference(&user_roles).cloned());
    if !roles_not_with_user.is_empty() {
        return Err(RBACError::RolesNotAssigned(Vec::from_iter(
            roles_not_with_user,
        )));
    }

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

    put_metadata(&metadata).await?;
    // update in mem table
    Users.remove_roles(&username.clone(), roles_to_remove.clone());

    sync_users_with_roles_with_ingestors(&username, &roles_to_remove, "remove").await?;

    Ok(format!("Roles updated successfully for {username}"))
}

// Handler for POST /api/v1/user/{username}/generate-new-password
// Resets password for the user to a newly generated one and returns it
pub async fn post_gen_password(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
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
    put_metadata(&metadata).await?;
    Users.change_password_hash(&username, &new_hash);

    sync_password_reset_with_ingestors(&username).await?;

    Ok(new_password)
}
