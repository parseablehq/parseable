use crate::rbac::{
    get_user_map,
    user::{PassCode, User},
};
use actix_web::{http::header::ContentType, web, Responder};
use http::StatusCode;

pub async fn put_user(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let mut user_map = get_user_map().write().unwrap();
    // fail this request if the user is already in the map
    // there is an exisiting config for this user
    if user_map.contains_key(&username) {
        return Err(RBACError::UserExists);
    }
    // todo: update parseable.json first
    let (user, password) = User::create_new(username);
    // set this user to user map
    user_map.insert(user);
    Ok(password)
}

pub async fn reset_password(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let mut user_map = get_user_map().write().unwrap();
    // fail this request if the user does not exists
    let Some(user) = user_map.get_mut(&username) else {
        return Err(RBACError::UserDoesNotExist);
    };
    // get new password for this user
    let PassCode { password, hashcode } = User::gen_new_password();
    // todo: update parseable.json first
    // update in mem table
    user.password = hashcode;

    Ok(password)
}

pub async fn delete_user(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let mut user_map = get_user_map().write().unwrap();
    // fail this request if the user does not exists
    if !user_map.contains_key(&username) {
        return Err(RBACError::UserDoesNotExist);
    };
    // todo: delete from parseable.json first
    // update in mem table
    user_map.remove(&username);
    Ok("deleted user")
}

#[derive(Debug, thiserror::Error)]
pub enum RBACError {
    #[error("User exists already")]
    UserExists,
    #[error("User does not exist")]
    UserDoesNotExist,
}

impl actix_web::ResponseError for RBACError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::UserExists => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist => StatusCode::NOT_FOUND,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
