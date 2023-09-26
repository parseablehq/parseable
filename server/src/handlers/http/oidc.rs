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

use std::{collections::HashSet, sync::Arc};

use actix_web::{
    cookie::{time, Cookie, SameSite},
    http::header::{self, ContentType},
    web::{self, Data},
    HttpRequest, HttpResponse,
};
use http::StatusCode;
use openid::{Options, Token, Userinfo};
use serde::Deserialize;
use ulid::Ulid;
use url::Url;

use crate::{
    oidc::{Claims, DiscoveredClient},
    option::CONFIG,
    rbac::{
        map::{SessionKey, DEFAULT_ROLE},
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
) -> Result<HttpResponse, OIDCError> {
    let oidc_client = req.app_data::<Data<DiscoveredClient>>();
    let session_key = extract_session_key_from_req(&req).ok();

    let (session_key, oidc_client) = match (session_key, oidc_client) {
        (None, None) => return Ok(redirect_no_oauth_setup(query.redirect.clone())),
        (None, Some(client)) => return Ok(redirect_to_oidc(query, client)),
        (Some(session_key), client) => (session_key, client),
    };

    match session_key {
        // We can exchange basic auth for session cookie
        SessionKey::BasicAuth { username, password } => match Users.get_user(&username) {
            Some(
                ref user @ User {
                    ty: UserType::Native(ref basic),
                    ..
                },
            ) if basic.verify_password(&password) => {
                let user_cookie = cookie_username(&username);
                let session_cookie =
                    exchange_basic_for_cookie(user, SessionKey::BasicAuth { username, password });
                Ok(redirect_to_client(
                    query.redirect.as_str(),
                    [user_cookie, session_cookie],
                ))
            }
            _ => Err(OIDCError::BadRequest),
        },
        // if it's a valid active session, just redirect back
        key @ SessionKey::SessionId(_) => {
            let resp = if Users.session_exists(&key) {
                redirect_to_client(query.redirect.as_str(), None)
            } else {
                Users.remove_session(&key);
                if let Some(oidc_client) = oidc_client {
                    redirect_to_oidc(query, oidc_client)
                } else {
                    redirect_to_client(query.redirect.as_str(), None)
                }
            };
            Ok(resp)
        }
    }
}

pub async fn logout(req: HttpRequest, query: web::Query<RedirectAfterLogin>) -> HttpResponse {
    let oidc_client = req.app_data::<Data<DiscoveredClient>>();
    let Some(session) = extract_session_key_from_req(&req).ok() else {
        return redirect_to_client(query.redirect.as_str(), None);
    };
    let user = Users.remove_session(&session);
    let logout_endpoint =
        oidc_client.and_then(|client| client.config().end_session_endpoint.clone());

    match (user, logout_endpoint) {
        (Some(username), Some(logout_endpoint))
            if Users.is_oauth(&username).unwrap_or_default() =>
        {
            redirect_to_oidc_logout(logout_endpoint, &query.redirect)
        }
        _ => redirect_to_client(query.redirect.as_str(), None),
    }
}

/// Handler for code callback
/// User should be redirected to page they were trying to access with cookie
pub async fn reply_login(
    oidc_client: Data<DiscoveredClient>,
    login_query: web::Query<Login>,
) -> Result<HttpResponse, OIDCError> {
    let oidc_client = Data::into_inner(oidc_client);
    let Ok((mut claims, user_info)): Result<(Claims, Userinfo), anyhow::Error> =
        request_token(oidc_client, &login_query).await
    else {
        return Ok(HttpResponse::Unauthorized().finish());
    };
    let username = user_info.sub.unwrap();
    let group: Option<HashSet<String>> = claims
        .other
        .remove("groups")
        .map(serde_json::from_value)
        .transpose()?;

    // User may not exist
    // create a new one depending on state of metadata
    let user = match (Users.get_user(&username), group) {
        (Some(user), Some(group)) => update_user_if_changed(user, group).await?,
        (Some(user), None) => user,
        (None, group) => put_user(&username, group).await?,
    };
    let id = Ulid::new();
    Users.new_session(&user, SessionKey::SessionId(id));

    let redirect_url = login_query
        .state
        .clone()
        .unwrap_or_else(|| CONFIG.parseable.address.to_string());

    Ok(redirect_to_client(
        &redirect_url,
        [cookie_session(id), cookie_username(&username)],
    ))
}

fn exchange_basic_for_cookie(user: &User, key: SessionKey) -> Cookie<'static> {
    let id = Ulid::new();
    Users.remove_session(&key);
    Users.new_session(user, SessionKey::SessionId(id));
    cookie_session(id)
}

fn redirect_to_oidc(
    query: web::Query<RedirectAfterLogin>,
    oidc_client: &DiscoveredClient,
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

fn redirect_to_oidc_logout(mut logout_endpoint: Url, redirect: &Url) -> HttpResponse {
    logout_endpoint.set_query(Some(&format!("post_logout_redirect_uri={}", redirect)));
    HttpResponse::TemporaryRedirect()
        .insert_header((header::CACHE_CONTROL, "no-store"))
        .insert_header((header::LOCATION, logout_endpoint.to_string()))
        .finish()
}

fn redirect_to_client(
    url: &str,
    cookies: impl IntoIterator<Item = Cookie<'static>>,
) -> HttpResponse {
    let mut response = HttpResponse::MovedPermanently();
    response.insert_header((header::LOCATION, url));
    for cookie in cookies {
        response.cookie(cookie);
    }
    response.insert_header((header::CACHE_CONTROL, "no-store"));
    response.finish()
}

fn redirect_no_oauth_setup(mut url: Url) -> HttpResponse {
    url.set_path("oidc-not-configured");
    let mut response = HttpResponse::MovedPermanently();
    response.insert_header((header::LOCATION, url.as_str()));
    response.insert_header((header::CACHE_CONTROL, "no-store"));
    response.finish()
}

fn cookie_session(id: Ulid) -> Cookie<'static> {
    let authorization_cookie = Cookie::build("session", id.to_string())
        .max_age(time::Duration::days(COOKIE_AGE_DAYS as i64))
        .same_site(SameSite::Strict)
        .path("/")
        .finish();
    authorization_cookie
}

fn cookie_username(username: &str) -> Cookie<'static> {
    let authorization_cookie = Cookie::build("username", username.to_string())
        .max_age(time::Duration::days(COOKIE_AGE_DAYS as i64))
        .same_site(SameSite::Strict)
        .path("/")
        .finish();
    authorization_cookie
}

async fn request_token(
    oidc_client: Arc<DiscoveredClient>,
    login_query: &Login,
) -> anyhow::Result<(Claims, Userinfo)> {
    let mut token: Token<Claims> = oidc_client.request_token(&login_query.code).await?.into();
    let Some(id_token) = token.id_token.as_mut() else {
        return Err(anyhow::anyhow!("No id_token provided"));
    };

    oidc_client.decode_token(id_token)?;
    oidc_client.validate_token(id_token, None, None)?;
    let claims = id_token.payload().expect("payload is decoded").clone();

    let userinfo = oidc_client.request_userinfo(&token).await?;
    Ok((claims, userinfo))
}

// put new user in metadata if does not exits
// update local cache
async fn put_user(
    username: &str,
    group: Option<HashSet<String>>,
) -> Result<User, ObjectStorageError> {
    let mut metadata = get_metadata().await?;
    let group = group.unwrap_or_else(|| {
        DEFAULT_ROLE
            .lock()
            .unwrap()
            .clone()
            .map(|role| HashSet::from([role]))
            .unwrap_or_default()
    });

    let user = metadata
        .users
        .iter()
        .find(|user| user.username() == username)
        .cloned()
        .unwrap_or_else(|| {
            let user = User::new_oauth(username.to_owned(), group);
            metadata.users.push(user.clone());
            user
        });

    put_metadata(&metadata).await?;
    Users.put_user(user.clone());
    Ok(user)
}

async fn update_user_if_changed(
    mut user: User,
    group: HashSet<String>,
) -> Result<User, ObjectStorageError> {
    // update user if roles have changed
    if user.roles == group {
        return Ok(user);
    }
    let metadata = get_metadata().await?;
    user.roles = group;
    put_metadata(&metadata).await?;
    Users.put_user(user.clone());
    Ok(user)
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
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    #[error("Bad Request")]
    BadRequest,
}

impl actix_web::ResponseError for OIDCError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRequest => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
