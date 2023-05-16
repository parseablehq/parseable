use crate::{
    option::CONFIG,
    rbac::{
        get_user_map,
        user::{PassCode, User},
    },
    storage::{self, ObjectStorageError, StorageMetadata},
    validator::{self, error::UsernameValidationError},
};
use actix_web::{http::header::ContentType, web, Responder};
use http::StatusCode;
use tokio::sync::Mutex;

// async aware lock for updating storage metadata and user map atomicically
static UPDATE_LOCK: Mutex<()> = Mutex::const_new(());

pub async fn put_user(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    // verify username
    validator::verify_username(&username)?;
    // get exclusive lock
    let _ = UPDATE_LOCK.lock().await;
    // fail this request if the user is already in the map
    // there is an exisiting config for this user
    if get_user_map().read().unwrap().contains_key(&username) {
        return Err(RBACError::UserExists);
    }

    let mut metadata = get_metadata().await?;
    if metadata.users.iter().any(|user| user.username == username) {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserExists);
    }

    let (user, password) = User::create_new(username);
    metadata.users.push(user.clone());
    put_metadata(&metadata).await?;
    // set this user to user map
    get_user_map().write().unwrap().insert(user);

    Ok(password)
}

pub async fn reset_password(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    // get exclusive lock
    let _ = UPDATE_LOCK.lock().await;
    // fail this request if the user does not exists
    if !get_user_map().read().unwrap().contains_key(&username) {
        return Err(RBACError::UserDoesNotExist);
    }
    // get new password for this user
    let PassCode { password, hashcode } = User::gen_new_password();
    // update parseable.json first
    let mut metadata = get_metadata().await?;
    if let Some(user) = metadata
        .users
        .iter_mut()
        .find(|user| user.username == username)
    {
        user.password = hashcode.clone();
    } else {
        // should be unreachable given state is always consistent
        return Err(RBACError::UserDoesNotExist);
    }
    put_metadata(&metadata).await?;

    // update in mem table
    get_user_map()
        .write()
        .unwrap()
        .get_mut(&username)
        .expect("checked that user exists in map")
        .password = hashcode;

    Ok(password)
}

pub async fn delete_user(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let _ = UPDATE_LOCK.lock().await;
    // fail this request if the user does not exists
    if !get_user_map().read().unwrap().contains_key(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    // delete from parseable.json first
    let mut metadata = get_metadata().await?;
    metadata.users.retain(|user| user.username != username);
    put_metadata(&metadata).await?;
    // update in mem table
    get_user_map().write().unwrap().remove(&username);
    Ok(format!("deleted user: {}", username))
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
    // put to remote
    storage::put_remote_metadata(metadata).await?;
    // put to staging
    storage::put_staging_metadata(metadata)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum RBACError {
    #[error("User exists already")]
    UserExists,
    #[error("User does not exist")]
    UserDoesNotExist,
    #[error("Failed to connect to storage: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("invalid Username: {0}")]
    ValidationError(#[from] UsernameValidationError),
}

impl actix_web::ResponseError for RBACError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::UserExists => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist => StatusCode::NOT_FOUND,
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
