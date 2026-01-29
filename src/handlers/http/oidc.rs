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

use std::{collections::HashSet, sync::Arc};

use actix_web::http::StatusCode;
use actix_web::{
    HttpRequest, HttpResponse,
    cookie::{Cookie, SameSite, time},
    http::header::ContentType,
    web,
};
use chrono::{Duration, TimeDelta};
use openid::{Bearer, Options, Token, Userinfo};
use regex::Regex;
use serde::Deserialize;
use tokio::sync::RwLock;
use ulid::Ulid;
use url::Url;

use crate::utils::login_sync;
use crate::{
    handlers::{
        COOKIE_AGE_DAYS, SESSION_COOKIE_NAME, USER_COOKIE_NAME, USER_ID_COOKIE_NAME,
        http::{
            API_BASE_PATH, API_VERSION,
            modal::{GlobalClient, OIDC_CLIENT},
        },
    },
    oidc::{Claims, DiscoveredClient},
    parseable::{DEFAULT_TENANT, PARSEABLE},
    rbac::{
        self, EXPIRY_DURATION, Users,
        map::{DEFAULT_ROLE, SessionKey},
        user::{self, GroupUser, User, UserType},
    },
    storage::{self, ObjectStorageError, StorageMetadata},
    utils::{
        actix::extract_session_key_from_req, get_tenant_id_from_key, get_tenant_id_from_request,
    },
};

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
    let conn = req.connection_info().clone();
    let base_url_without_scheme = format!("{}/", conn.host());
    if !is_valid_redirect_url(&base_url_without_scheme, query.redirect.as_str()) {
        return Err(OIDCError::BadRequest(
            "Bad Request, Invalid Redirect URL!".to_string(),
        ));
    }

    let oidc_client = OIDC_CLIENT.get();

    let session_key = extract_session_key_from_req(&req).ok();
    let (session_key, oidc_client) = match (session_key, oidc_client) {
        (None, None) => return Ok(redirect_no_oauth_setup(query.redirect.clone())),
        (None, Some(client)) => {
            return Ok(redirect_to_oidc(
                query,
                client.read().await.client(),
                PARSEABLE.options.scope.to_string().as_str(),
            ));
        }
        (Some(session_key), client) => (session_key, client),
    };
    // if control flow is here then it is most likely basic auth
    // try authorize
    match Users.authorize(session_key.clone(), rbac::role::Action::Login, None, None) {
        rbac::Response::Authorized => (),
        rbac::Response::UnAuthorized
        | rbac::Response::ReloadRequired
        | rbac::Response::Suspended(_) => {
            return Err(OIDCError::Unauthorized);
        }
    }
    let tenant_id = get_tenant_id_from_key(&session_key);
    match session_key {
        // We can exchange basic auth for session cookie
        SessionKey::BasicAuth { username, password } => match Users.get_user(&username, &tenant_id)
        {
            Some(
                ref user @ User {
                    ty: UserType::Native(ref basic),
                    ..
                },
            ) if basic.verify_password(&password) => {
                let user_cookie = cookie_username(&username);
                let user_id_cookie = cookie_userid(&username);
                let session_cookie = exchange_basic_for_cookie(
                    user,
                    SessionKey::BasicAuth { username, password },
                    EXPIRY_DURATION,
                );
                let _session = session_cookie.value().to_owned();
                let _user = user.clone();
                let r = login_sync(_session, _user, EXPIRY_DURATION, &tenant_id).await;
                tracing::warn!(login_sync=?r);
                Ok(redirect_to_client(
                    query.redirect.as_str(),
                    [user_cookie, user_id_cookie, session_cookie],
                ))
            }
            _ => Err(OIDCError::BadRequest("Bad Request".to_string())),
        },
        // if it's a valid active session, just redirect back
        key @ SessionKey::SessionId(_) => {
            let resp = if Users.session_exists(&key) {
                redirect_to_client(query.redirect.as_str(), None)
            } else {
                Users.remove_session(&key);
                if let Some(oidc_client) = oidc_client {
                    redirect_to_oidc(
                        query,
                        oidc_client.read().await.client(),
                        PARSEABLE.options.scope.to_string().as_str(),
                    )
                } else {
                    redirect_to_client(query.redirect.as_str(), None)
                }
            };
            Ok(resp)
        }
    }
}

pub async fn logout(req: HttpRequest, query: web::Query<RedirectAfterLogin>) -> HttpResponse {
    let oidc_client = OIDC_CLIENT.get();

    let Some(session) = extract_session_key_from_req(&req).ok() else {
        return redirect_to_client(query.redirect.as_str(), None);
    };
    let tenant_id = get_tenant_id_from_key(&session);
    let user = Users.remove_session(&session);
    let logout_endpoint = if let Some(client) = oidc_client {
        client
            .read()
            .await
            .client()
            .config()
            .end_session_endpoint
            .clone()
    } else {
        None
    };

    match (user, logout_endpoint) {
        (Some(username), Some(logout_endpoint))
            if Users.is_oauth(&username, &tenant_id).unwrap_or_default() =>
        {
            redirect_to_oidc_logout(logout_endpoint, &query.redirect)
        }
        _ => redirect_to_client(query.redirect.as_str(), None),
    }
}

/// Handler for code callback
/// User should be redirected to page they were trying to access with cookie
pub async fn reply_login(
    req: HttpRequest,
    login_query: web::Query<Login>,
) -> Result<HttpResponse, OIDCError> {
    let oidc_client = if let Some(oidc_client) = OIDC_CLIENT.get() {
        oidc_client
    } else {
        return Err(OIDCError::Unauthorized);
    };
    let tenant_id = get_tenant_id_from_request(&req);

    let (mut claims, user_info, bearer) = match request_token(oidc_client, &login_query).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("reply_login call failed- {e}");
            return Ok(HttpResponse::Unauthorized().finish());
        }
    };
    let username = user_info
        .name
        .clone()
        .or_else(|| user_info.email.clone())
        .or_else(|| user_info.sub.clone())
        .expect("OIDC provider did not return a usable identifier (name, email or sub)");
    let user_id = match user_info.sub.clone() {
        Some(id) => id,
        None => {
            tracing::error!("OIDC provider did not return a sub");
            return Err(OIDCError::Unauthorized);
        }
    };
    let user_info: user::UserInfo = user_info.into();

    // if provider has group A, and parseable as has role A
    // then user will automatically get assigned role A
    // else, the default oidc role (inside parseable) will get assigned
    let group: HashSet<String> = claims
        .other
        .remove("groups")
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let metadata = get_metadata(&tenant_id).await?;

    // Find which OIDC groups match existing roles in Parseable
    let mut valid_oidc_roles = HashSet::new();
    for role in metadata.roles.iter() {
        let role_name = role.0;
        if group.contains(role_name) {
            valid_oidc_roles.insert(role_name.clone());
        }
    }

    let default_role = if let Some(role) = DEFAULT_ROLE
        .read()
        // .unwrap()
        .get(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
        && let Some(role) = role
    {
        HashSet::from([role.to_owned()])
    } else {
        HashSet::new()
    };
    // let default_role = if let Some(default_role) = DEFAULT_ROLE.lock().unwrap().clone() {
    //     HashSet::from([default_role])
    // } else {
    //     HashSet::new()
    // };

    let existing_user = find_existing_user(&user_info, tenant_id);
    let mut final_roles = match existing_user {
        Some(ref user) => {
            // For existing users: keep existing roles + add new valid OIDC roles
            let mut roles = user.roles.clone();
            roles.extend(valid_oidc_roles); // Add new matching roles
            roles
        }
        None => {
            // For new users: use valid OIDC roles, fallback to default if none
            if valid_oidc_roles.is_empty() {
                default_role.clone()
            } else {
                valid_oidc_roles
            }
        }
    };
    if final_roles.is_empty() {
        // If no roles were found, use the default role
        final_roles.clone_from(&default_role);
    }

    let expires_in = if let Some(expires_in) = bearer.expires_in.as_ref() {
        // need an i64 somehow
        if *expires_in > u32::MAX.into() {
            EXPIRY_DURATION
        } else {
            let v = i64::from(*expires_in as u32);
            Duration::seconds(v)
        }
    } else {
        EXPIRY_DURATION
    };

    let user = match (existing_user, final_roles) {
        (Some(user), roles) => update_user_if_changed(user, roles, user_info, bearer).await?,
        // LET TENANT BE NONE FOR NOW!!!
        (None, roles) => put_user(&user_id, roles, user_info, bearer, None).await?,
    };
    let id = Ulid::new();

    Users.new_session(&user, SessionKey::SessionId(id), expires_in);

    let redirect_url = login_query
        .state
        .clone()
        .unwrap_or_else(|| PARSEABLE.options.address.to_string());

    Ok(redirect_to_client(
        &redirect_url,
        [
            cookie_session(id),
            cookie_username(&username),
            cookie_userid(&user_id),
        ],
    ))
}

fn find_existing_user(user_info: &user::UserInfo, tenant_id: Option<String>) -> Option<User> {
    if let Some(sub) = &user_info.sub
        && let Some(user) = Users.get_user(sub, &tenant_id)
        && matches!(user.ty, UserType::OAuth(_))
    {
        return Some(user);
    }

    if let Some(name) = &user_info.name
        && let Some(user) = Users.get_user(name, &tenant_id)
        && matches!(user.ty, UserType::OAuth(_))
    {
        return Some(user);
    }

    if let Some(email) = &user_info.email
        && let Some(user) = Users.get_user(email, &tenant_id)
        && matches!(user.ty, UserType::OAuth(_))
    {
        return Some(user);
    }

    None
}

fn exchange_basic_for_cookie(
    user: &User,
    key: SessionKey,
    expires_in: TimeDelta,
) -> Cookie<'static> {
    let id = Ulid::new();
    Users.remove_session(&key);
    Users.new_session(user, SessionKey::SessionId(id), expires_in);
    cookie_session(id)
}

fn redirect_to_oidc(
    query: web::Query<RedirectAfterLogin>,
    oidc_client: &DiscoveredClient,
    scope: &str,
) -> HttpResponse {
    let redirect = query.into_inner().redirect.to_string();
    let auth_url = oidc_client.auth_url(&Options {
        scope: Some(scope.to_string()),
        state: Some(redirect),
        ..Default::default()
    });
    let mut url: String = auth_url.into();
    url.push_str("&access_type=offline&prompt=consent");
    HttpResponse::TemporaryRedirect()
        .insert_header((actix_web::http::header::LOCATION, url))
        .finish()
}

fn redirect_to_oidc_logout(mut logout_endpoint: Url, redirect: &Url) -> HttpResponse {
    logout_endpoint.set_query(Some(&format!("post_logout_redirect_uri={redirect}")));
    HttpResponse::TemporaryRedirect()
        .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
        .insert_header((
            actix_web::http::header::LOCATION,
            logout_endpoint.to_string(),
        ))
        .finish()
}

pub fn redirect_to_client(
    url: &str,
    cookies: impl IntoIterator<Item = Cookie<'static>>,
) -> HttpResponse {
    let mut response = HttpResponse::MovedPermanently();
    response.insert_header((actix_web::http::header::LOCATION, url));
    for cookie in cookies {
        response.cookie(cookie);
    }
    response.insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"));

    response.finish()
}

fn redirect_no_oauth_setup(mut url: Url) -> HttpResponse {
    url.set_path("oidc-not-configured");
    let mut response = HttpResponse::MovedPermanently();
    response.insert_header((actix_web::http::header::LOCATION, url.as_str()));
    response.insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"));
    response.finish()
}

pub fn cookie_session(id: Ulid) -> Cookie<'static> {
    Cookie::build(SESSION_COOKIE_NAME, id.to_string())
        .max_age(time::Duration::days(COOKIE_AGE_DAYS as i64))
        .same_site(SameSite::Strict)
        .path("/")
        .finish()
}

pub fn cookie_username(username: &str) -> Cookie<'static> {
    Cookie::build(USER_COOKIE_NAME, username.to_string())
        .max_age(time::Duration::days(COOKIE_AGE_DAYS as i64))
        .same_site(SameSite::Strict)
        .path("/")
        .finish()
}

pub fn cookie_userid(user_id: &str) -> Cookie<'static> {
    Cookie::build(USER_ID_COOKIE_NAME, user_id.to_string())
        .max_age(time::Duration::days(COOKIE_AGE_DAYS as i64))
        .same_site(SameSite::Strict)
        .path("/")
        .finish()
}

pub async fn request_token(
    oidc_client: &Arc<RwLock<GlobalClient>>,
    login_query: &Login,
) -> anyhow::Result<(Claims, Userinfo, Bearer)> {
    let old_client = oidc_client.read().await.client().clone();
    let mut token: Token<Claims> = old_client.request_token(&login_query.code).await?.into();

    let id_token = if let Some(token) = token.id_token.as_mut() {
        token
    } else {
        return Err(anyhow::anyhow!("No id_token provided"));
    };

    if let Err(e) = old_client.decode_token(id_token) {
        tracing::error!("error while decoding the id_token- {e}");
        let new_client = PARSEABLE
            .options
            .openid()
            .unwrap()
            .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
            .await?;

        // Reuse the already-obtained token, just decode with new client's JWKS
        new_client.decode_token(id_token)?;
        new_client.validate_token(id_token, None, None)?;
        let claims = id_token.payload().expect("payload is decoded").clone();

        let userinfo = new_client.request_userinfo(&token).await?;
        let bearer = token.bearer;

        // replace old client with new one
        drop(old_client);

        oidc_client.write().await.set(new_client);
        return Ok((claims, userinfo, bearer));
    }

    old_client.validate_token(id_token, None, None)?;
    let claims = id_token.payload().expect("payload is decoded").clone();

    let userinfo = old_client.request_userinfo(&token).await?;
    let bearer = token.bearer;
    Ok((claims, userinfo, bearer))
}

// put new user in metadata if does not exit
// update local cache
pub async fn put_user(
    userid: &str,
    group: HashSet<String>,
    user_info: user::UserInfo,
    bearer: Bearer,
    tenant: Option<String>,
) -> Result<User, ObjectStorageError> {
    let mut metadata = get_metadata(&tenant).await?;

    let mut user = metadata
        .users
        .iter()
        .find(|user| user.userid() == userid)
        .cloned()
        .unwrap_or_else(|| {
            let user = User::new_oauth(
                userid.to_owned(),
                group,
                user_info,
                None,
                tenant.clone(),
                false,
            );
            metadata.users.push(user.clone());
            user
        });

    put_metadata(&metadata, &tenant).await?;

    // modify before storing
    if let user::UserType::OAuth(oauth) = &mut user.ty {
        oauth.bearer = Some(bearer);
    }
    Users.put_user(user.clone());
    Ok(user)
}

pub async fn update_user_if_changed(
    mut user: User,
    group: HashSet<String>,
    user_info: user::UserInfo,
    bearer: Bearer,
) -> Result<User, ObjectStorageError> {
    // Store the old username before modifying the user object
    let old_username = user.userid().to_string();
    let User { ty, roles, .. } = &mut user;
    let UserType::OAuth(oauth_user) = ty else {
        unreachable!()
    };

    // Check if userid needs migration to sub (even if nothing else changed)
    let needs_userid_migration = if let Some(ref sub) = user_info.sub {
        oauth_user.userid != *sub
    } else {
        false
    };

    // update user only if roles, userinfo has changed, or userid needs migration, or bearer is updated
    if roles == &group
        && oauth_user.user_info == user_info
        && !needs_userid_migration
        && oauth_user.bearer.as_ref() == Some(&bearer)
    {
        return Ok(user);
    }

    oauth_user.user_info.clone_from(&user_info);
    *roles = group;

    // Update userid to use sub if available (migration from name-based to sub-based identification)
    if let Some(ref sub) = user_info.sub {
        oauth_user.userid.clone_from(sub);
    }

    let mut metadata = get_metadata(&user.tenant).await?;

    // Find the user entry using the old username (before migration)
    if let Some(entry) = metadata
        .users
        .iter_mut()
        .find(|x| x.userid() == old_username)
    {
        entry.clone_from(&user);
        // migrate user references inside user groups
        for group in metadata.user_groups.iter_mut() {
            group.users.retain(|u| u.userid() != old_username);
            group.users.insert(GroupUser::from_user(&user));
        }
    }
    put_metadata(&metadata, &user.tenant).await?;
    Users.delete_user(&old_username, &user.tenant);
    // update oauth bearer
    if let user::UserType::OAuth(oauth) = &mut user.ty {
        oauth.bearer = Some(bearer);
    }
    Users.put_user(user.clone());
    Ok(user)
}

async fn get_metadata(
    tenant_id: &Option<String>,
) -> Result<crate::storage::StorageMetadata, ObjectStorageError> {
    let metadata = PARSEABLE
        .metastore
        .get_parseable_metadata(tenant_id)
        .await
        .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?
        .ok_or_else(|| ObjectStorageError::Custom("parseable metadata not initialized".into()))?;
    Ok(serde_json::from_slice::<StorageMetadata>(&metadata)?)
}

async fn put_metadata(
    metadata: &StorageMetadata,
    tenant_id: &Option<String>,
) -> Result<(), ObjectStorageError> {
    storage::put_remote_metadata(metadata, tenant_id).await?;
    storage::put_staging_metadata(metadata, tenant_id)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum OIDCError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    #[error("{0}")]
    BadRequest(String),
    #[error("Unauthorized")]
    Unauthorized,
}

impl actix_web::ResponseError for OIDCError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

fn is_valid_redirect_url(base_url_without_scheme: &str, redirect_url: &str) -> bool {
    let http_scheme_match_regex = Regex::new(r"^(https?://)").unwrap();
    let redirect_url_without_scheme = http_scheme_match_regex.replace(redirect_url, "");

    base_url_without_scheme == redirect_url_without_scheme
}
