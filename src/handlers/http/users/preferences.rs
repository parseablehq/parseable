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
    web::{Json, Path},
};

use crate::{
    metastore::MetastoreError,
    rbac::Users,
    users::preferences::{USER_PREFERENCES, UserPreferences},
    utils::{get_hash, get_tenant_id_from_request},
};

pub async fn get(
    req: HttpRequest,
    user_id: Path<String>,
) -> Result<impl Responder, UserPreferencesError> {
    let tenant_id = get_tenant_id_from_request(&req);
    let user_id = user_id.into_inner();
    if !Users.contains(&user_id, &tenant_id) {
        return Err(UserPreferencesError::UserDoesNotExist);
    }
    let user_id = get_hash(&user_id);

    let preferences = USER_PREFERENCES.get(&user_id, &tenant_id).await;

    Ok(Json(preferences))
}

pub async fn put(
    req: HttpRequest,
    user_id: Path<String>,
    Json(mut preferences): Json<UserPreferences>,
) -> Result<impl Responder, UserPreferencesError> {
    let tenant_id = get_tenant_id_from_request(&req);
    let user_id = user_id.into_inner();
    if !Users.contains(&user_id, &tenant_id) {
        return Err(UserPreferencesError::UserDoesNotExist);
    }
    preferences.set_metadata(get_hash(&user_id), tenant_id.clone());
    USER_PREFERENCES.put(&preferences, &tenant_id).await?;

    Ok(Json(preferences))
}

#[derive(Debug, thiserror::Error)]
pub enum UserPreferencesError {
    #[error("User does not exist")]
    UserDoesNotExist,
    #[error(transparent)]
    Metastore(#[from] MetastoreError),
}

impl actix_web::ResponseError for UserPreferencesError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::UserDoesNotExist => StatusCode::NOT_FOUND,
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
