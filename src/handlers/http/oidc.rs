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

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};

use actix_web::http::StatusCode;
use actix_web::{
    HttpRequest, HttpResponse,
    cookie::{Cookie, SameSite, time},
    http::header::ContentType,
    web,
};

/// When set to true, cookies use SameSite::None + Secure (required for Clerk OAuth).
/// Enterprise sets this when P_CLERK_SECRET is configured.
static COOKIE_REQUIRE_CROSS_SITE: AtomicBool = AtomicBool::new(false);

pub fn set_cookie_cross_site(enabled: bool) {
    COOKIE_REQUIRE_CROSS_SITE.store(enabled, Ordering::Relaxed);
}
use chrono::{Duration, TimeDelta};
use openid::Bearer;
use regex::Regex;
use serde::Deserialize;
use ulid::Ulid;
use url::Url;

use crate::{
    handlers::{
        COOKIE_AGE_DAYS, SESSION_COOKIE_NAME, USER_COOKIE_NAME, USER_ID_COOKIE_NAME,
        http::modal::OIDC_CLIENT,
    },
    oauth::OAuthSession,
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
            let redirect = query.into_inner().redirect.to_string();

            let scope = PARSEABLE.options.scope.to_string();
            let mut auth_url: String = client.read().await.auth_url(&scope, Some(redirect)).into();

            auth_url.push_str("&access_type=offline&prompt=consent");
            return Ok(HttpResponse::TemporaryRedirect()
                .insert_header((actix_web::http::header::LOCATION, auth_url))
                .finish());
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
                    let redirect = query.into_inner().redirect.to_string();
                    let scope = PARSEABLE.options.scope.to_string();
                    let mut auth_url: String = oidc_client
                        .read()
                        .await
                        .auth_url(&scope, Some(redirect))
                        .into();
                    auth_url.push_str("&access_type=offline&prompt=consent");
                    HttpResponse::TemporaryRedirect()
                        .insert_header((actix_web::http::header::LOCATION, auth_url))
                        .finish()
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
        client.read().await.logout_url()
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
    let oidc_client = OIDC_CLIENT.get().ok_or(OIDCError::Unauthorized)?;
    let tenant_id = get_tenant_id_from_request(&req);

    let session = oidc_client
        .write()
        .await
        .exchange_code(&login_query.code)
        .await
        .map_err(|e| {
            tracing::error!("reply_login exchange_code failed: {e}");
            OIDCError::Unauthorized
        })?;

    let (username, user_id, user_info) = extract_identity(&session)?;
    let metadata = get_metadata(&tenant_id).await?;
    let existing_user = find_existing_user(&user_info, tenant_id.clone());
    let final_roles = resolve_roles(
        &session.claims.groups,
        &metadata,
        &user_info,
        &tenant_id,
        existing_user.as_ref(),
    );

    let expires_in = bearer_expiry(&session.bearer);
    let user = match (existing_user, final_roles) {
        (Some(user), roles) => {
            update_user_if_changed(user, roles, user_info, session.bearer).await?
        }
        (None, roles) => {
            put_user(
                &user_id,
                roles,
                user_info,
                session.bearer,
                tenant_id.clone(),
            )
            .await?
        }
    };

    let id = Ulid::new();
    Users.new_session(&user, SessionKey::SessionId(id), expires_in);

    let cookies = [
        cookie_session(id),
        cookie_username(&username),
        cookie_userid(&user_id),
    ];

    Ok(build_login_response(
        &req,
        &login_query,
        cookies,
        id,
        &username,
        &user_id,
    ))
}

/// Extract username, user_id, and UserInfo from the OAuth session.
fn extract_identity(session: &OAuthSession) -> Result<(String, String, user::UserInfo), OIDCError> {
    let user_info = &session.userinfo;
    let username = user_info
        .name
        .clone()
        .or_else(|| user_info.email.clone())
        .or_else(|| user_info.sub.clone())
        .ok_or_else(|| {
            tracing::error!(
                "OAuth provider did not return a usable identifier (name, email or sub)"
            );
            OIDCError::Unauthorized
        })?;
    let user_id = user_info.sub.clone().ok_or_else(|| {
        tracing::error!("OAuth provider did not return a sub");
        OIDCError::Unauthorized
    })?;
    Ok((username, user_id, user_info.clone().into()))
}

/// Determine the final set of roles for the user.
fn resolve_roles(
    groups: &HashSet<String>,
    metadata: &StorageMetadata,
    user_info: &user::UserInfo,
    tenant_id: &Option<String>,
    existing_user: Option<&User>,
) -> HashSet<String> {
    let valid_oidc_roles: HashSet<String> = metadata
        .roles
        .keys()
        .filter(|role_name| groups.contains(*role_name))
        .cloned()
        .collect();

    let default_role = DEFAULT_ROLE
        .read()
        .get(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
        .and_then(|r| r.clone())
        .map(|r| HashSet::from([r]))
        .unwrap_or_default();

    let mut roles = match existing_user {
        Some(user) => {
            let mut roles = user.roles.clone();
            roles.extend(valid_oidc_roles);
            roles
        }
        None if !valid_oidc_roles.is_empty() => valid_oidc_roles,
        None => default_role.clone(),
    };

    if roles.is_empty() {
        roles.clone_from(&default_role);
    }

    // Inherit roles from a native user with the same email (e.g. tenant owner via OAuth)
    if roles.is_empty()
        && let Some(email) = &user_info.email
        && let Some(native) = metadata.users.iter().find(|u| {
            matches!(u.ty, UserType::Native(_))
                && u.userid() == email.as_str()
                && !u.roles.is_empty()
        })
    {
        roles.clone_from(&native.roles);
    }

    roles
}

/// Compute session expiry from the bearer token.
fn bearer_expiry(bearer: &Bearer) -> TimeDelta {
    match bearer.expires_in.as_ref() {
        Some(&exp) if exp <= u32::MAX.into() => Duration::seconds(i64::from(exp as u32)),
        _ => EXPIRY_DURATION,
    }
}

/// Build the HTTP response for the login callback (XHR JSON or redirect).
fn build_login_response(
    req: &HttpRequest,
    login_query: &web::Query<Login>,
    cookies: [Cookie<'static>; 3],
    session_id: Ulid,
    username: &str,
    user_id: &str,
) -> HttpResponse {
    let is_xhr = req.headers().contains_key("x-p-tenant")
        || req
            .headers()
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("application/json"));

    if is_xhr {
        let mut response = HttpResponse::Ok();
        for cookie in cookies {
            response.cookie(cookie);
        }
        response.json(serde_json::json!({
            "session": session_id.to_string(),
            "username": username,
            "user_id": user_id,
        }))
    } else {
        let redirect_url = login_query
            .state
            .clone()
            .unwrap_or_else(|| PARSEABLE.options.address.to_string());

        redirect_to_client(&redirect_url, cookies)
    }
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

fn build_cookie(name: &str, value: String) -> Cookie<'static> {
    let mut cookie = Cookie::build(name.to_string(), value)
        .max_age(time::Duration::days(COOKIE_AGE_DAYS as i64))
        .path("/".to_string());

    if COOKIE_REQUIRE_CROSS_SITE.load(Ordering::Relaxed) {
        cookie = cookie.same_site(SameSite::None).secure(true);
    } else {
        cookie = cookie.same_site(SameSite::Lax);
    }

    cookie.finish()
}

pub fn cookie_session(id: Ulid) -> Cookie<'static> {
    build_cookie(SESSION_COOKIE_NAME, id.to_string())
}

pub fn cookie_username(username: &str) -> Cookie<'static> {
    build_cookie(USER_COOKIE_NAME, username.to_string())
}

pub fn cookie_userid(user_id: &str) -> Cookie<'static> {
    build_cookie(USER_ID_COOKIE_NAME, user_id.to_string())
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
    // If the userid matches the super admin (P_USERNAME), return the existing
    // Native user as-is. This prevents overwriting the super admin with an
    // OAuth user while still allowing OAuth login to create a session.
    if userid == PARSEABLE.options.username
        && let Some(user) = Users.get_user(userid, &tenant)
    {
        return Ok(user);
    }

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
