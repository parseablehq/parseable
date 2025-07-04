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
    rbac::{
        self,
        map::{mut_users, read_user_groups, roles, write_user_groups},
        role::model::DefaultPrivilege,
        user,
        utils::to_prism_user,
        Users,
    },
    storage::ObjectStorageError,
    validator::{self, error::UsernameValidationError},
};
use actix_web::{
    http::header::ContentType,
    web::{self, Path},
    Responder,
};
use http::StatusCode;
use itertools::Itertools;
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;

use super::modal::utils::rbac_utils::{get_metadata, put_metadata};

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

/// Handler for GET /api/v1/users
/// returns list of all registerd users along with their roles and other info
pub async fn list_users_prism() -> impl Responder {
    // get all users
    let prism_users = rbac::map::users().values().map(to_prism_user).collect_vec();

    web::Json(prism_users)
}

/// Function for GET /users/{username}
pub async fn get_prism_user(username: Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    // First check if the user exists
    let users = rbac::map::users();
    if let Some(user) = users.get(&username) {
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

    add_roles_to_user(
        web::Path::<String>::from(username.clone()),
        web::Json(created_role),
    )
    .await?;

    Ok(password)
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

    Ok(new_password)
}

// Handler for GET /api/v1/user/{username}/role
// returns role for a user if that user exists
pub async fn get_role(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    if !Users.contains(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    let direct_roles: HashMap<String, Vec<DefaultPrivilege>> = Users
        .get_role(&username)
        .iter()
        .filter_map(|role_name| {
            roles()
                .get(role_name)
                .map(|role| (role_name.to_owned(), role.clone()))
        })
        .collect();

    let mut group_roles: HashMap<String, HashMap<String, Vec<DefaultPrivilege>>> = HashMap::new();
    // user might be part of some user groups, fetch the roles from there as well
    for user_group in Users.get_user_groups(&username) {
        if let Some(group) = read_user_groups().get(&user_group) {
            let ug_roles: HashMap<String, Vec<DefaultPrivilege>> = group
                .roles
                .iter()
                .filter_map(|role_name| {
                    roles()
                        .get(role_name)
                        .map(|role| (role_name.to_owned(), role.clone()))
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

    // ensure that the users remove the user group from their map
    [&username]
        .iter()
        .map(|user| {
            if let Some(user) = mut_users().get_mut(*user) {
                for group in groups_to_update.iter() {
                    user.user_groups.remove(&group.name);
                }

                metadata.users.retain(|u| u.username() != user.username());
                metadata.users.push(user.clone());
            }
        })
        .for_each(drop);
    put_metadata(&metadata).await?;

    // update in metadata user groups
    metadata
        .user_groups
        .retain(|x| !groups_to_update.contains(x));
    metadata.user_groups.extend(groups_to_update);
    put_metadata(&metadata).await?;

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

//     put_metadata(&metadata).await?;
//     // update in mem table
//     Users.add_roles(&username.clone(), role.clone());

//     Ok(format!("Roles updated successfully for {username}"))
// }

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
    Users.add_roles(&username.clone(), roles_to_add);

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
    Users.remove_roles(&username.clone(), roles_to_remove);

    Ok(format!("Roles updated successfully for {username}"))
}

#[derive(Debug, Serialize)]
#[serde(rename = "camelCase")]
pub struct InvalidUserGroupError {
    pub valid_name: bool,
    pub non_existant_roles: Vec<String>,
    pub non_existant_users: Vec<String>,
    pub roles_not_in_group: Vec<String>,
    pub users_not_in_group: Vec<String>,
    pub comments: String,
}

// impl Display for InvalidUserGroupRequestStruct {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         if !self.invalid_name {
//             write!(
//                 f,
//                 "Invalid user group request- {{invalidName: {}\nnonExistantRoles: {:?}\nnonExistantUsers: {:?}\nThe name should follow this regex- ^[A-Za-z0-9_-]+$}}",
//                 self.invalid_name, self.non_existant_roles, self.non_existant_users
//             )
//         } else {
//             write!(
//                 f,
//                 "Invalid user group request- {{nonExistantRoles: {:?}\nnonExistantUsers: {:?}}}",
//                 self.non_existant_roles, self.non_existant_users
//             )
//         }
//     }
// }

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
    #[error("User group already exists: '{0}'")]
    UserGroupExists(String),
    #[error("UserGroup does not exist: {0}")]
    UserGroupDoesNotExist(String),
    #[error("Invalid Roles: {0:?}")]
    RolesDoNotExist(Vec<String>),
    #[error("Roles have not been assigned: {0:?}")]
    RolesNotAssigned(Vec<String>),
    #[error("{0:?}")]
    InvalidUserGroupRequest(Box<InvalidUserGroupError>),
    #[error("{0}")]
    InvalidSyncOperation(String),
    #[error("User group still being used by users: {0}")]
    UserGroupNotEmpty(String),
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
            Self::UserGroupExists(_) => StatusCode::BAD_REQUEST,
            Self::UserGroupDoesNotExist(_) => StatusCode::BAD_REQUEST,
            Self::RolesDoNotExist(_) => StatusCode::BAD_REQUEST,
            Self::RolesNotAssigned(_) => StatusCode::BAD_REQUEST,
            Self::InvalidUserGroupRequest(_) => StatusCode::BAD_REQUEST,
            Self::InvalidSyncOperation(_) => StatusCode::BAD_REQUEST,
            Self::UserGroupNotEmpty(_) => StatusCode::BAD_REQUEST,
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
                    "non_existant_roles": obj
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
