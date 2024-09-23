use std::collections::HashSet;

use actix_web::{web, Responder};
use tokio::sync::Mutex;

use crate::{handlers::http::{cluster::{sync_password_reset_with_ingestors, sync_user_creation_with_ingestors, sync_user_deletion_with_ingestors, sync_users_with_roles_with_ingestors}, modal::utils::rbac_utils::{get_metadata, put_metadata}, rbac::RBACError}, rbac::{user, Users}, validator};

// async aware lock for updating storage metadata and user map atomicically
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
    let roles: HashSet<String> = if let Some(body) = body {
        serde_json::from_value(body.into_inner())?
    } else {
        return Err(RBACError::RoleValidationError);
    };

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

    put_metadata(&metadata).await?;
    let created_role = roles.clone();
    Users.put_user(user.clone());

    sync_user_creation_with_ingestors(user, &Some(roles)).await?;
    
    put_role(
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

    put_metadata(&metadata).await?;
    
    sync_user_deletion_with_ingestors(&username).await?;

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

    put_metadata(&metadata).await?;
    // update in mem table
    Users.put_role(&username.clone(), role.clone());
    
    sync_users_with_roles_with_ingestors(&username, &role).await?;
    
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