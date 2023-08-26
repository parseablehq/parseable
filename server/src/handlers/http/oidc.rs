/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::sync::Arc;

use actix_web::{
    cookie::{time, Cookie, SameSite},
    error::ErrorBadRequest,
    http::header::{self, ContentType},
    web::{self, Data},
    HttpRequest, HttpResponse,
};
use chrono::{Days, Utc};
use http::StatusCode;
use log::info;
use openid::{Options, Token, Userinfo};
use serde::Deserialize;
use ulid::Ulid;
use url::Url;

use crate::{
    option::CONFIG,
    rbac::{
        map::{mut_sessions, sessions, users, SessionKey},
        role::Permission,
        user::{User, UserType},
        Users,
    },
    storage::{self, ObjectStorageError, StorageMetadata},
    utils::actix::extract_session_key_from_req,
};

// fetch common personalization scope to determine username.
const SCOPE: &str = "openid profile email";
const COOKIE_AGE_DAYS: usize = 7;

/// Struct representing query params returned from oidc provider
#[derive(Deserialize, Debug)]
pub struct Login {
    pub code: String,
    pub state: Option<String>,
}

/// Struct representing query param when visiting /login
/// Caller can set the state for code auth flow and this is
/// at the end used as target for redirect
#[derive(Deserialize, Debug)]
pub struct RedirectAfterLogin {
    pub redirect: Url,
}

pub async fn login(
    req: HttpRequest,
    query: web::Query<RedirectAfterLogin>,
    oidc_client: Data<openid::Client>,
) -> HttpResponse {
    let session_key = extract_session_key_from_req(&req).ok();
    // if no authentication method is used then the client is requesting for a oauth session to be established
    let Some(session_key) = session_key else {
        return redirect_to_oidc(query, oidc_client);
    };

    match session_key {
        // We can exchange basic auth for session cookie
        SessionKey::BasicAuth { username, password } => match users().get(&username) {
            Some(
                user @ User {
                    ty: UserType::Native(basic),
                    ..
                },
            ) if basic.verify_password(&password) => {
                let cookie = exchange_basic_for_cookie(
                    username.clone(),
                    SessionKey::BasicAuth { username, password },
                    user.permissions(),
                );

                HttpResponse::MovedPermanently()
                    .insert_header((header::LOCATION, query.redirect.to_string()))
                    .cookie(cookie)
                    .finish()
            }
            _ => ErrorBadRequest("Request contains basic auth that does not match").into(),
        },
        // if it's a valid active session, just redirect back
        key @ SessionKey::SessionId(_) => {
            if sessions().get(&key).is_some() {
                HttpResponse::MovedPermanently()
                    .insert_header((header::LOCATION, query.redirect.to_string()))
                    .finish()
            } else {
                mut_sessions().remove_session(&key);
                redirect_to_oidc(query, oidc_client)
            }
        }
    }
}

fn exchange_basic_for_cookie(
    username: String,
    key: SessionKey,
    permissions: Vec<Permission>,
) -> Cookie<'static> {
    let id = Ulid::new();
    let mut sessions = mut_sessions();
    sessions.remove_session(&key);
    sessions.track_new(
        username,
        SessionKey::SessionId(id),
        Utc::now() + Days::new(COOKIE_AGE_DAYS as u64),
        permissions,
    );
    cookie(id)
}

/// Handler for code callback
/// User should be redirected to page they were trying to access with cookie
pub async fn reply_login(
    oidc_client: Data<openid::Client>,
    login_query: web::Query<Login>,
) -> Result<HttpResponse, OIDCError> {
    let oidc_client = Data::into_inner(oidc_client);
    let Ok((_, user_info)) = request_token(oidc_client, &login_query).await else {
        return Ok(HttpResponse::Unauthorized().finish());
    };
    let username = user_info.preferred_username.unwrap();

    // User may not exist
    // create a new one depending on state of metadata
    if !Users.contains(&username) {
        let mut metadata = get_metadata().await?;
        let user = match metadata
            .users
            .iter()
            .find(|user| user.username() == username)
        {
            Some(user) => user.clone(),
            None => {
                let user = User::new_oauth(username.clone());
                metadata.users.push(user.clone());
                put_metadata(&metadata).await?;
                user
            }
        };
        Users.put_user(user);
    };

    let id = Ulid::new();
    mut_sessions().track_new(
        username,
        crate::rbac::map::SessionKey::SessionId(id),
        Utc::now() + Days::new(7),
        vec![Permission::SelfRole],
    );
    let authorization_cookie = cookie(id);

    let redirect_url = login_query
        .state
        .clone()
        .unwrap_or_else(|| CONFIG.parseable.address.to_string());

    Ok(HttpResponse::MovedPermanently()
        .insert_header((header::LOCATION, redirect_url))
        .cookie(authorization_cookie)
        .body(""))
}

fn redirect_to_oidc(
    query: web::Query<RedirectAfterLogin>,
    oidc_client: Data<openid::Client>,
) -> HttpResponse {
    let redirect = query.into_inner().redirect.to_string();
    let auth_url = oidc_client.auth_url(&Options {
        scope: Some(SCOPE.into()),
        state: Some(redirect),
        ..Default::default()
    });
    let url: String = auth_url.into();
    HttpResponse::TemporaryRedirect()
        .insert_header((header::LOCATION, url))
        .finish()
}

fn cookie(id: Ulid) -> Cookie<'static> {
    let authorization_cookie = Cookie::build("session", id.to_string())
        .http_only(true)
        .max_age(time::Duration::days(COOKIE_AGE_DAYS as i64))
        .same_site(SameSite::Strict)
        .path("/")
        .finish();
    authorization_cookie
}

async fn request_token(
    oidc_client: Arc<openid::Client>,
    login_query: &Login,
) -> anyhow::Result<(Token, Userinfo)> {
    let mut token: Token = oidc_client.request_token(&login_query.code).await?.into();
    if let Some(id_token) = token.id_token.as_mut() {
        oidc_client.decode_token(id_token)?;
        oidc_client.validate_token(id_token, None, None)?;
        info!("token: {:?}", id_token);
    } else {
        return Err(anyhow::anyhow!("No id_token provided"));
    }
    let userinfo = oidc_client.request_userinfo(&token).await?;
    info!("user info: {:?}", userinfo);
    Ok((token, userinfo))
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
    storage::put_remote_metadata(metadata).await?;
    storage::put_staging_metadata(metadata)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum OIDCError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
}

impl actix_web::ResponseError for OIDCError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
