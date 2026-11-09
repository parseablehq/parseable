/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

use actix_web::{
    HttpRequest, HttpResponse, Responder,
    http::{StatusCode, header::ContentType},
    web::Json,
};

use crate::{
    handlers::http::rbac::RBACError,
    metastore::MetastoreError,
    users::preferences::{USER_PREFERENCES, UserPreferences},
    utils::{get_hash, get_user_and_tenant_from_request},
};

pub async fn get(req: HttpRequest) -> Result<impl Responder, UserPreferencesError> {
    let (user_id, tenant_id) = get_user_and_tenant_from_request(&req)?;
    let user_id = get_hash(&user_id);

    let preferences = USER_PREFERENCES.get(&user_id, &tenant_id).await;

    Ok(Json(preferences))
}

pub async fn put(
    req: HttpRequest,
    Json(mut preferences): Json<UserPreferences>,
) -> Result<impl Responder, UserPreferencesError> {
    let (user_id, tenant_id) = get_user_and_tenant_from_request(&req)?;
    preferences.set_metadata(get_hash(&user_id), tenant_id.clone());
    USER_PREFERENCES.put(&preferences, &tenant_id).await?;

    Ok(Json(preferences))
}

#[derive(Debug, thiserror::Error)]
pub enum UserPreferencesError {
    #[error("User does not exist")]
    UserDoesNotExist(#[from] RBACError),
    #[error(transparent)]
    Metastore(#[from] MetastoreError),
}

impl actix_web::ResponseError for UserPreferencesError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::UserDoesNotExist(error) => error.status_code(),
            Self::Metastore(error) => error.status_code(),
        }
    }

    fn error_response(&self) -> HttpResponse {
        match self {
            Self::Metastore(error) => HttpResponse::build(self.status_code())
                .insert_header(ContentType::json())
                .json(error.to_detail()),
            _ => HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string()),
        }
    }
}
