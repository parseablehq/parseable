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

use std::collections::HashSet;

use actix_web::{HttpRequest, HttpResponse, Responder, web};

use crate::{
    handlers::http::{
        cluster::{
            sync_password_reset_with_ingestors, sync_user_creation,
            sync_user_deletion_with_ingestors, sync_users_with_roles_with_ingestors,
        },
        modal::utils::rbac_utils::{get_metadata, put_metadata},
        rbac::{RBACError, UPDATE_LOCK},
    },
    parseable::DEFAULT_TENANT,
    rbac::{
        Users,
        map::{roles, users, write_user_groups},
        user::{self, UserType},
    },
    utils::get_tenant_id_from_request,
    validator,
};

// Handler for POST /api/v1/user/{username}
// Creates a new user by username if it does not exists
pub async fn post_user(
    req: HttpRequest,
    username: web::Path<String>,
    body: Option<web::Json<serde_json::Value>>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    validator::user_role_name(&username)?;
    let mut metadata = get_metadata(&tenant_id).await?;

    let user_roles: HashSet<String> = if let Some(body) = body {
        serde_json::from_value(body.into_inner())?
    } else {
        HashSet::new()
    };

    let mut non_existent_roles = Vec::new();
    for role in &user_roles {
        if !roles().contains_key(role) {
            non_existent_roles.push(role.clone());
        }
    }
    if !non_existent_roles.is_empty() {
        return Err(RBACError::RolesDoNotExist(non_existent_roles));
    }
    let _guard = UPDATE_LOCK.lock().await;
    if Users.contains(&username, &tenant_id)
        || metadata
            .users
            .iter()
            .any(|user| matches!(&user.ty, UserType::Native(basic) if basic.username == username))
    {
        return Err(RBACError::UserExists(username));
    }

    let (user, password) = user::User::new_basic(username.clone(), None);

    metadata.users.push(user.clone());

    put_metadata(&metadata, &tenant_id).await?;
    let created_role = user_roles.clone();
    Users.put_user(user.clone());

    sync_user_creation(user, &Some(user_roles)).await?;
    if !created_role.is_empty() {
        add_roles_to_user(
            req,
            web::Path::<String>::from(username.clone()),
            web::Json(created_role),
        )
        .await?;
    }

    Ok(password)
}

// Handler for DELETE /api/v1/user/{userid}
pub async fn delete_user(
    req: HttpRequest,
    userid: web::Path<String>,
) -> Result<impl Responder, RBACError> {
    let userid = userid.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let tenant = tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v);
    let _guard = UPDATE_LOCK.lock().await;
    // fail this request if the user does not exist
    if !Users.contains(&userid, &tenant_id) {
        return Err(RBACError::UserDoesNotExist);
    };

    // find username by userid, for native users, username is userid, for oauth users, we need to look up
    let username = if let Some(users) = users().get(tenant)
        && let Some(user) = users.get(&userid)
    {
        user.username_by_userid()
    } else {
        return Err(RBACError::UserDoesNotExist);
    };

    // delete from parseable.json first
    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.users.retain(|user| user.userid() != userid);

    // also delete from user groups
    let user_groups = Users.get_user_groups(&userid, &tenant_id);
    let mut groups_to_update = Vec::new();

    for user_group in user_groups {
        if let Some(groups) = write_user_groups().get_mut(tenant)
            && let Some(ug) = groups.get_mut(&user_group)
            && let Some(users) = users().get(tenant)
            && let Some(user) = users.get(&userid)
        {
            let userid = match &user.ty {
                UserType::Native(basic) => basic.username.clone(),
                UserType::OAuth(oauth) => oauth.userid.clone(),
            };
            ug.remove_users_by_user_ids(HashSet::from_iter([userid]))?;
            groups_to_update.push(ug.clone());
        } else {
            // User not found, skip or log as needed
            continue;
        }
    }

    // For each updated group, replace in place if found; otherwise push
    for updated_group in &groups_to_update {
        if let Some(existing) = metadata
            .user_groups
            .iter_mut()
            .find(|ug| ug.name == updated_group.name)
        {
            existing.clone_from(updated_group);
        } else {
            metadata.user_groups.push(updated_group.clone());
        }
    }
    put_metadata(&metadata, &tenant_id).await?;

    sync_user_deletion_with_ingestors(&userid).await?;

    // update in mem table
    Users.delete_user(&userid, &tenant_id);
    Ok(HttpResponse::Ok().json(format!("deleted user: {username}")))
}

// Handler PATCH /user/{userid}/role/add => Add roles to a user
pub async fn add_roles_to_user(
    req: HttpRequest,
    userid: web::Path<String>,
    roles_to_add: web::Json<HashSet<String>>,
) -> Result<String, RBACError> {
    let userid = userid.into_inner();
    let roles_to_add = roles_to_add.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    if !Users.contains(&userid, &tenant_id) {
        return Err(RBACError::UserDoesNotExist);
    };

    let tenant = tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v);
    // find username by userid, for native users, username is userid, for oauth users, we need to look up
    let username = if let Some(users) = users().get(tenant)
        && let Some(user) = users.get(&userid)
    {
        user.username_by_userid()
    } else {
        return Err(RBACError::UserDoesNotExist);
    };

    let mut non_existent_roles = Vec::new();

    // check if the role exists
    roles_to_add.iter().for_each(|r| {
        if roles().get(r).is_none() {
            non_existent_roles.push(r.clone());
        }
    });

    if !non_existent_roles.is_empty() {
        return Err(RBACError::RolesDoNotExist(non_existent_roles));
    }

    // update parseable.json first
    let mut metadata = get_metadata(&tenant_id).await?;
    if let Some(user) = metadata
        .users
        .iter_mut()
        .find(|user| user.userid() == userid)
    {
        user.roles.extend(roles_to_add.clone());
    } else {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserDoesNotExist);
    }

    put_metadata(&metadata, &tenant_id).await?;
    // update in mem table
    Users.add_roles(&userid.clone(), roles_to_add.clone(), &tenant_id);

    sync_users_with_roles_with_ingestors(&userid, &roles_to_add, "add").await?;

    Ok(format!("Roles updated successfully for {username}"))
}

// Handler PATCH /user/{userid}/role/remove => Remove roles from a user
pub async fn remove_roles_from_user(
    req: HttpRequest,
    userid: web::Path<String>,
    roles_to_remove: web::Json<HashSet<String>>,
) -> Result<impl Responder, RBACError> {
    let userid = userid.into_inner();
    let roles_to_remove = roles_to_remove.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let tenant = tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v);
    let _guard = UPDATE_LOCK.lock().await;

    if !Users.contains(&userid, &tenant_id) {
        return Err(RBACError::UserDoesNotExist);
    };

    // find username by userid, for native users, username is userid, for oauth users, we need to look up
    let username = if let Some(users) = users().get(tenant)
        && let Some(user) = users.get(&userid)
    {
        user.username_by_userid()
    } else {
        return Err(RBACError::UserDoesNotExist);
    };

    let mut non_existent_roles = Vec::new();

    // check if the role exists
    roles_to_remove.iter().for_each(|r| {
        if roles().get(r).is_none() {
            non_existent_roles.push(r.clone());
        }
    });

    if !non_existent_roles.is_empty() {
        return Err(RBACError::RolesDoNotExist(non_existent_roles));
    }

    // check for role not present with user
    let user_roles: HashSet<String> = HashSet::from_iter(Users.get_role(&userid, &tenant_id));
    let roles_not_with_user: HashSet<String> =
        HashSet::from_iter(roles_to_remove.difference(&user_roles).cloned());
    if !roles_not_with_user.is_empty() {
        return Err(RBACError::RolesNotAssigned(Vec::from_iter(
            roles_not_with_user,
        )));
    }

    // update parseable.json first
    let mut metadata = get_metadata(&tenant_id).await?;
    if let Some(user) = metadata
        .users
        .iter_mut()
        .find(|user| user.userid() == userid)
    {
        let diff: HashSet<String> =
            HashSet::from_iter(user.roles.difference(&roles_to_remove).cloned());
        user.roles = diff;
    } else {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserDoesNotExist);
    }

    put_metadata(&metadata, &tenant_id).await?;
    // update in mem table
    Users.remove_roles(&userid.clone(), roles_to_remove.clone(), &tenant_id);

    sync_users_with_roles_with_ingestors(&userid, &roles_to_remove, "remove").await?;

    Ok(HttpResponse::Ok().json(format!("Roles updated successfully for {username}")))
}

// Handler for POST /api/v1/user/{username}/generate-new-password
// Resets password for the user to a newly generated one and returns it
pub async fn post_gen_password(
    req: HttpRequest,
    username: web::Path<String>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let mut new_password = String::default();
    let mut new_hash = String::default();
    let tenant_id = get_tenant_id_from_request(&req);
    let mut metadata = get_metadata(&tenant_id).await?;

    let _guard = UPDATE_LOCK.lock().await;
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
    put_metadata(&metadata, &tenant_id).await?;
    Users.change_password_hash(&username, &new_hash, &tenant_id);

    sync_password_reset_with_ingestors(req, &username).await?;

    Ok(new_password)
}
