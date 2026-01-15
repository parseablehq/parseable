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

use crate::{
    parseable::DEFAULT_TENANT,
    rbac::{
        self, Users,
        map::{read_user_groups, roles, users},
        role::model::DefaultPrivilege,
        user::{self, UserType},
        utils::to_prism_user,
    },
    storage::ObjectStorageError,
    utils::get_tenant_id_from_request,
    validator::{self, error::UsernameValidationError},
};
use actix_web::http::StatusCode;
use actix_web::{
    HttpRequest, HttpResponse, Responder,
    http::header::ContentType,
    web::{self, Path},
};
use itertools::Itertools;
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;

use super::modal::utils::rbac_utils::{get_metadata, put_metadata};

// async aware lock for updating storage metadata and user map atomically
pub(crate) static UPDATE_LOCK: Mutex<()> = Mutex::const_new(());

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
            id: user.userid().to_owned(),
            method,
        }
    }
}

// Handler for GET /api/v1/user
// returns list of all registered users
pub async fn list_users(req: HttpRequest) -> impl Responder {
    let tenant_id = get_tenant_id_from_request(&req);
    web::Json(Users.collect_user::<User>(&tenant_id))
}

/// Handler for GET /api/v1/users
/// returns list of all registered users along with their roles and other info
pub async fn list_users_prism(req: HttpRequest) -> impl Responder {
    let tenant_id = get_tenant_id_from_request(&req);
    let tenant_id = tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v);
    // get all users
    let prism_users = match rbac::map::users().get(tenant_id) {
        Some(users) => users.values().map(to_prism_user).collect_vec(),
        None => vec![],
    };
    web::Json(prism_users)
}

/// Function for GET /users/{username}
pub async fn get_prism_user(
    req: HttpRequest,
    username: Path<String>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    // First check if the user exists
    let users = rbac::map::users();
    if let Some(users) = users.get(tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v))
        && let Some(user) = users.get(&username)
    {
        // Create UsersPrism for the found user only
        let prism_user = to_prism_user(user);
        Ok(web::Json(prism_user))
    } else {
        Err(RBACError::UserDoesNotExist)
    }
}

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
        if let Some(tenant_roles) = roles().get(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
            && !tenant_roles.contains_key(role)
        {
            non_existent_roles.push(role.clone());
        }
    }
    if !non_existent_roles.is_empty() {
        return Err(RBACError::RolesDoNotExist(non_existent_roles));
    }
    let _guard = UPDATE_LOCK.lock().await;
    if Users.contains(&username, &tenant_id)
        || metadata.users.iter().any(|user| match &user.ty {
            UserType::Native(basic) => basic.username == username,
            UserType::OAuth(_) => false, // OAuth users should be created differently
        })
    {
        return Err(RBACError::UserExists(username));
    }

    let (user, password) = user::User::new_basic(username.clone(), tenant_id.clone());

    metadata.users.push(user.clone());

    put_metadata(&metadata, &tenant_id).await?;
    let created_role = user_roles.clone();
    Users.put_user(user.clone());
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

// Handler for POST /api/v1/user/{username}/generate-new-password
// Resets password for the user to a newly generated one and returns it
pub async fn post_gen_password(
    req: HttpRequest,
    username: web::Path<String>,
) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let mut new_password = String::default();
    let mut new_hash = String::default();
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

    Ok(new_password)
}

// Handler for GET /api/v1/user/{userid}/role
// returns role for a user if that user exists
pub async fn get_role(
    req: HttpRequest,
    userid: web::Path<String>,
) -> Result<impl Responder, RBACError> {
    let userid = userid.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let tenant = tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v);
    if !Users.contains(&userid, &tenant_id) {
        return Err(RBACError::UserDoesNotExist);
    };
    let direct_roles: HashMap<String, Vec<DefaultPrivilege>> = Users
        .get_role(&userid, &tenant_id)
        .iter()
        .filter_map(|role_name| {
            if let Some(roles) = roles().get(tenant)
                && let Some(role) = roles.get(role_name)
            {
                Some((role_name.to_owned(), role.clone()))
            } else {
                None
            }
            // roles()
            //     .get(role_name)
            //     .map(|role| (role_name.to_owned(), role.clone()))
        })
        .collect();

    let mut group_roles: HashMap<String, HashMap<String, Vec<DefaultPrivilege>>> = HashMap::new();
    // user might be part of some user groups, fetch the roles from there as well
    for user_group in Users.get_user_groups(&userid, &tenant_id) {
        if let Some(groups) = read_user_groups().get(tenant)
            && let Some(group) = groups.get(&user_group)
        {
            let ug_roles: HashMap<String, Vec<DefaultPrivilege>> = group
                .roles
                .iter()
                .filter_map(|role_name| {
                    if let Some(roles) = roles().get(tenant)
                        && let Some(role) = roles.get(role_name)
                    {
                        Some((role_name.to_owned(), role.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            group_roles.insert(group.name.clone(), ug_roles);
        }
    }
    let res = RolesResponse {
        direct_roles,
        group_roles,
    };
    Ok(web::Json(res))
}

// Handler for DELETE /api/v1/user/delete/{userid}
pub async fn delete_user(
    req: HttpRequest,
    userid: web::Path<String>,
) -> Result<impl Responder, RBACError> {
    let userid = userid.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let _guard = UPDATE_LOCK.lock().await;
    // if user is a part of any groups then don't allow deletion
    if !Users.get_user_groups(&userid, &tenant_id).is_empty() {
        return Err(RBACError::InvalidDeletionRequest(format!(
            "User: {userid} should not be a part of any groups"
        )));
    }
    // fail this request if the user does not exists
    if !Users.contains(&userid, &tenant_id) {
        return Err(RBACError::UserDoesNotExist);
    };

    // find username by userid, for native users, username is userid, for oauth users, we need to look up
    let username = if let Some(users) =
        users().get(tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v))
        && let Some(user) = users.get(&userid)
    {
        user.username_by_userid()
    } else {
        return Err(RBACError::UserDoesNotExist);
    };

    // delete from parseable.json first
    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.users.retain(|user| user.userid() != userid);
    put_metadata(&metadata, &tenant_id).await?;

    // update in mem table
    Users.delete_user(&userid, &tenant_id);

    Ok(HttpResponse::Ok().json(format!("deleted user: {username}")))
}

// Handler PATCH /user/{userid}/role/add => Add roles to a user
pub async fn add_roles_to_user(
    req: HttpRequest,
    userid: web::Path<String>,
    roles_to_add: web::Json<HashSet<String>>,
) -> Result<impl Responder, RBACError> {
    let userid = userid.into_inner();
    let roles_to_add = roles_to_add.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    if !Users.contains(&userid, &tenant_id) {
        return Err(RBACError::UserDoesNotExist);
    };

    // find username by userid, for native users, username is userid, for oauth users, we need to look up
    let username = if let Some(users) =
        users().get(tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v))
        && let Some(user) = users.get(&userid)
    {
        user.username_by_userid()
    } else {
        return Err(RBACError::UserDoesNotExist);
    };

    let mut non_existent_roles = Vec::new();

    // check if the role exists
    for role in &roles_to_add {
        if let Some(tenant_roles) = roles().get(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
            && !tenant_roles.contains_key(role)
        {
            non_existent_roles.push(role.clone());
        }
    }

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
    Users.add_roles(&userid.clone(), roles_to_add, &tenant_id);

    Ok(HttpResponse::Ok().json(format!("Roles updated successfully for {username}")))
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
    if !Users.contains(&userid, &tenant_id) {
        return Err(RBACError::UserDoesNotExist);
    };

    // find username by userid, for native users, username is userid, for oauth users, we need to look up
    let username = if let Some(users) =
        users().get(tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v))
        && let Some(user) = users.get(&userid)
    {
        user.username_by_userid()
    } else {
        return Err(RBACError::UserDoesNotExist);
    };

    let mut non_existent_roles = Vec::new();

    // check if the role exists
    for role in &roles_to_remove {
        if let Some(tenant_roles) = roles().get(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
            && !tenant_roles.contains_key(role)
        {
            non_existent_roles.push(role.clone());
        }
    }

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
    Users.remove_roles(&userid.clone(), roles_to_remove, &tenant_id);

    Ok(HttpResponse::Ok().json(format!("Roles updated successfully for {username}")))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvalidUserGroupError {
    pub valid_name: bool,
    pub non_existent_roles: Vec<String>,
    pub non_existent_users: Vec<String>,
    pub roles_not_in_group: Vec<String>,
    pub users_not_in_group: Vec<String>,
    pub comments: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RBACError {
    #[error("User {0} already exists")]
    UserExists(String),
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
    #[error("User group `{0}` already exists")]
    UserGroupExists(String),
    #[error("UserGroup `{0}` does not exist")]
    UserGroupDoesNotExist(String),
    #[error("Invalid Roles: {0:?}")]
    RolesDoNotExist(Vec<String>),
    #[error("Roles have not been assigned: {0:?}")]
    RolesNotAssigned(Vec<String>),
    #[error("{0:?}")]
    InvalidUserGroupRequest(Box<InvalidUserGroupError>),
    #[error("{0}")]
    InvalidSyncOperation(String),
    #[error("UserGroup `{0}` is still in use")]
    UserGroupNotEmpty(String),
    #[error("Resource `{0}` is still in use")]
    ResourceInUse(String),
    #[error("{0}")]
    InvalidDeletionRequest(String),
}

impl actix_web::ResponseError for RBACError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::UserExists(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist => StatusCode::NOT_FOUND,
            Self::SerdeError(_) => StatusCode::BAD_REQUEST,
            Self::ValidationError(_) => StatusCode::BAD_REQUEST,
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Network(_) => StatusCode::BAD_GATEWAY,
            Self::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RoleValidationError => StatusCode::BAD_REQUEST,
            Self::UserGroupExists(_) => StatusCode::BAD_REQUEST,
            Self::UserGroupDoesNotExist(_) => StatusCode::BAD_REQUEST,
            Self::RolesDoNotExist(_) => StatusCode::BAD_REQUEST,
            Self::RolesNotAssigned(_) => StatusCode::BAD_REQUEST,
            Self::InvalidUserGroupRequest(_) => StatusCode::BAD_REQUEST,
            Self::InvalidSyncOperation(_) => StatusCode::BAD_REQUEST,
            Self::UserGroupNotEmpty(_) => StatusCode::BAD_REQUEST,
            Self::ResourceInUse(_) => StatusCode::BAD_REQUEST,
            Self::InvalidDeletionRequest(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        match self {
            RBACError::RolesNotAssigned(obj) => actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .json(json!({
                    "roles_not_assigned": obj
                })),
            RBACError::RolesDoNotExist(obj) => actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .json(json!({
                    "non_existent_roles": obj
                })),
            RBACError::InvalidUserGroupRequest(obj) => {
                actix_web::HttpResponse::build(self.status_code())
                    .insert_header(ContentType::plaintext())
                    .json(obj)
            }
            _ => actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string()),
        }
    }
}

#[derive(Serialize)]
#[serde(rename = "camelCase")]
pub struct RolesResponse {
    #[serde(rename = "roles")]
    pub direct_roles: HashMap<String, Vec<DefaultPrivilege>>,
    pub group_roles: HashMap<String, HashMap<String, Vec<DefaultPrivilege>>>,
}
