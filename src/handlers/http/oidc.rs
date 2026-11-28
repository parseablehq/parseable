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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

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
use chrono::{TimeDelta, Utc};
use openid::Bearer;
use serde::Deserialize;
use tokio::sync::RwLock;
use ulid::Ulid;
use url::Url;

use crate::{
    handlers::{
        COOKIE_AGE_DAYS, SESSION_COOKIE_NAME, USER_COOKIE_NAME, USER_ID_COOKIE_NAME,
        http::{
            API_BASE_PATH, API_VERSION,
            cluster::sync_user_creation,
            modal::{OIDC_CLIENT, utils::rbac_utils::put_metadata},
            rbac::{RBACError, UPDATE_LOCK},
        },
    },
    oauth::{OAuthProvider, OAuthSession},
    parseable::{DEFAULT_TENANT, PARSEABLE},
    rbac::{
        self, EXPIRY_DURATION, Users,
        map::{DEFAULT_ROLE, SessionKey, mut_sessions, mut_users, roles, write_user_groups},
        user::{self, GroupUser, User, UserType},
    },
    storage::{ObjectStorageError, StorageMetadata},
    utils::{
        actix::extract_session_key_from_req, get_tenant_id_from_key, get_tenant_id_from_request,
        header_parsing::HeaderBuildError,
    },
};

/// Holds the token that ties an IdP round trip to the browser that started it.
const LOGIN_STATE_COOKIE_NAME: &str = "oidc_state";
/// How long a browser has to finish signing in at the IdP.
const LOGIN_STATE_MINUTES: i64 = 10;

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

/// Query params for /login: the redirect target, and whether `login` already
/// moved the browser to the `P_ORIGIN_URI` host.
#[derive(Deserialize, Debug)]
pub struct LoginQuery {
    pub redirect: Url,
    #[serde(default)]
    pub at_origin: bool,
}

pub async fn login(
    req: HttpRequest,
    query: web::Query<LoginQuery>,
) -> Result<HttpResponse, OIDCError> {
    if !is_valid_redirect_url(&request_base_url(&req), query.redirect.as_str()) {
        return Err(OIDCError::BadRequest(
            "Bad Request, Invalid Redirect URL!".to_string(),
        ));
    }

    let oidc_client = OIDC_CLIENT.get();

    let session_key = extract_session_key_from_req(&req).ok();
    let (session_key, oidc_client) = match (session_key, oidc_client) {
        (None, None) => return Ok(redirect_no_oauth_setup(query.redirect.clone())),
        (None, Some(client)) => return Ok(redirect_to_idp(&req, client, &query).await),
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
        SessionKey::BasicAuth { username, password } => {
            match exchange_basic_for_cookie(&username, &password, &tenant_id, EXPIRY_DURATION) {
                Some(session_cookie) => {
                    let user_cookie = cookie_username(&username);
                    let user_id_cookie = cookie_userid(&username);

                    Ok(redirect_to_client(
                        query.redirect.as_str(),
                        [user_cookie, user_id_cookie, session_cookie],
                    ))
                }
                None => Err(OIDCError::BadRequest("Bad Request".to_string())),
            }
        }
        // if it's a valid active session, just redirect back
        key @ (SessionKey::SessionId(_) | SessionKey::Ephemeral(_)) => {
            let resp = if Users.session_exists(&key) {
                redirect_to_client(query.redirect.as_str(), None)
            } else {
                Users.remove_session(&key);
                if let Some(oidc_client) = oidc_client {
                    redirect_to_idp(&req, oidc_client, &query).await
                } else {
                    redirect_to_client(query.redirect.as_str(), None)
                }
            };
            Ok(resp)
        }
    }
}

pub async fn logout(
    req: HttpRequest,
    query: web::Query<RedirectAfterLogin>,
) -> Result<HttpResponse, OIDCError> {
    let oidc_client = OIDC_CLIENT.get();

    if !is_valid_redirect_url(&request_base_url(&req), query.redirect.as_str()) {
        return Err(OIDCError::BadRequest(
            "Bad Request, Invalid Redirect URL!".to_string(),
        ));
    }

    let Some(session) = extract_session_key_from_req(&req).ok() else {
        return Ok(redirect_to_client(
            query.redirect.as_str(),
            removal_cookies(),
        ));
    };
    let tenant_id = get_tenant_id_from_key(&session);
    let user = {
        let _guard = UPDATE_LOCK.lock().await;
        Users.remove_session(&session)
    };
    let logout_endpoint = if let Some(client) = oidc_client {
        client.read().await.logout_url()
    } else {
        None
    };

    match (user, logout_endpoint) {
        (Some(username), Some(logout_endpoint))
            if Users.is_oauth(&username, &tenant_id).unwrap_or_default() =>
        {
            Ok(redirect_to_oidc_logout(logout_endpoint, &query.redirect))
        }
        _ => Ok(redirect_to_client(
            query.redirect.as_str(),
            removal_cookies(),
        )),
    }
}

/// Handler for code callback
/// User should be redirected to page they were trying to access with cookie
pub async fn reply_login(
    req: HttpRequest,
    login_query: web::Query<Login>,
) -> Result<HttpResponse, OIDCError> {
    let oidc_client = OIDC_CLIENT.get().ok_or(OIDCError::Unauthorized)?;
    let tenant_id = get_tenant_id_from_request(&req)
        .map_err(|error| OIDCError::BadRequest(error.to_string()))?;
    // An XHR caller forwards the code itself and receives the session as JSON;
    // a browser arrives from the IdP and is redirected on. Check either caller
    // before the code is redeemed: a forged callback must not create a session.
    let redirect = if is_xhr(&req) {
        verify_xhr_origin(&req)?;
        None
    } else {
        Some(login_redirect(&req, login_query.state.as_deref())?)
    };

    let session = oidc_client
        .read()
        .await
        .exchange_code(&login_query.code)
        .await
        .map_err(|_| {
            tracing::warn!("OIDC authorization code exchange or identity verification failed");
            OIDCError::Unauthorized
        })?;

    let (username, user_id, _) = extract_identity(&session)?;
    warn_if_refresh_unavailable(&session.bearer);
    let id = Ulid::new();
    let user = reconcile_oauth_session(session, tenant_id.clone(), None, SessionKey::SessionId(id))
        .await?;

    if !PARSEABLE.options.is_multi_tenant() {
        let roles = Some(user.roles.clone());
        let mut cluster_user = user.clone();
        if let UserType::OAuth(oauth) = &mut cluster_user.ty {
            oauth.bearer = None;
        }
        best_effort_user_sync(
            sync_user_creation(
                &req,
                cluster_user,
                &roles,
                &tenant_id,
                &PARSEABLE.options.username,
            )
            .await,
        )?;
    }

    let cookies = [
        cookie_session(id),
        cookie_username(&username),
        cookie_userid(&user_id),
    ];

    Ok(build_login_response(
        redirect, cookies, id, &username, &user_id,
    ))
}

/// Syncing the OAuth user to other nodes is best effort: a node or network
/// failure is logged and login continues. Forwarding headers that can't be
/// built on any node fail the login rather than being ignored, retaining
/// its own status (400 client header, generic 500 stored or generated value).
fn best_effort_user_sync(result: Result<(), RBACError>) -> Result<(), OIDCError> {
    match result {
        Ok(()) => Ok(()),
        Err(RBACError::HeaderBuild(e)) => Err(e.into()),
        Err(e) => {
            tracing::error!("Failed to sync OAuth user with roles to cluster nodes: {e}");
            Ok(())
        }
    }
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
    let subject = session
        .claims
        .sub
        .as_deref()
        .ok_or(OIDCError::Unauthorized)?;
    crate::oauth::authorization::validate_subjects(subject, user_info.sub.as_deref())
        .map_err(|_| OIDCError::Unauthorized)?;
    if session.claims.issuer.is_empty() {
        return Err(OIDCError::Unauthorized);
    }
    let user_id = crate::oauth::authorization::oidc_user_id(&session.claims.issuer, subject);
    Ok((username, user_id, user_info.clone().into()))
}

/// Authorization is revalidated at least every `P_OIDC_REVALIDATION_INTERVAL`
/// seconds of active usage, regardless of a provider's longer token lifetime.
pub(crate) fn bearer_expiry(bearer: &Bearer) -> TimeDelta {
    let interval = PARSEABLE.options.oidc_revalidation_interval;
    TimeDelta::seconds(bearer.expires_in.unwrap_or(interval).min(interval) as i64)
}

/// Provider groups map to roles only in standalone mode, the only mode that
/// advertises the capability. Other modes do not sync refreshed grants to every
/// node, so they keep manual and default grants only.
pub(crate) fn group_mapping_enabled() -> bool {
    matches!(PARSEABLE.options.mode, crate::option::Mode::All)
}

/// Without a refresh token every revalidation is a full sign-in, so make the
/// usual cause (a scope missing `offline_access`) visible to operators once.
fn warn_if_refresh_unavailable(bearer: &Bearer) {
    static WARNED: std::sync::Once = std::sync::Once::new();
    if bearer.refresh_token.is_none() {
        WARNED.call_once(|| {
            tracing::warn!(
                "OIDC provider issued no refresh token; users must sign in again every {}s. \
                 Add `offline_access` (or the provider's equivalent) to P_OIDC_SCOPE, \
                 or raise P_OIDC_REVALIDATION_INTERVAL. See docs/oidc.md.",
                PARSEABLE.options.oidc_revalidation_interval
            );
        });
    }
}

fn is_xhr(req: &HttpRequest) -> bool {
    req.headers().contains_key("x-p-tenant")
        || req
            .headers()
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("application/json"))
}

/// The page to return to after a browser login. Only the browser holding the
/// state cookie set by `login` may finish the login, and only toward an
/// allowed origin.
fn login_redirect(req: &HttpRequest, state: Option<&str>) -> Result<String, OIDCError> {
    // `login` sets the cookie on the host it was reached by, which can differ
    // from the host the IdP returns to; say so rather than invite a retry.
    let Some(cookie) = req.cookie(LOGIN_STATE_COOKIE_NAME) else {
        return Err(OIDCError::BadRequest(
            "Bad Request, no sign-in is pending in this browser on this host. Sign in again; \
             if this repeats, set P_ORIGIN_URI to the address browsers use for Parseable."
                .to_string(),
        ));
    };
    let redirect = state
        .and_then(|state| verify_login_state(state, cookie.value()))
        .ok_or_else(|| {
            OIDCError::BadRequest(
                "Bad Request, login state does not match this browser. Sign in again.".to_string(),
            )
        })?;
    // Redirect to the URL as parsed for validation, not the raw string.
    Url::parse(redirect)
        .ok()
        .filter(|url| is_valid_redirect_url(&callback_base_url(req), url.as_str()))
        .map(String::from)
        .ok_or_else(|| OIDCError::BadRequest("Bad Request, Invalid Redirect URL!".to_string()))
}

/// Accept is CORS-safelisted, so a page on another site can make a browser
/// send a JSON callback and keep the session cookies in the response. Callers
/// must show they run on this or an allowed origin. Browsers send no Fetch
/// Metadata over plain HTTP, and a same-origin GET carries no Origin, so a
/// same-origin Referer is the remaining proof there.
fn verify_xhr_origin(req: &HttpRequest) -> Result<(), OIDCError> {
    let header = |name| req.headers().get(name).and_then(|v| v.to_str().ok());
    let allowed = |url: &str| is_valid_redirect_url(&callback_base_url(req), url);
    let trusted = match header("origin") {
        Some(origin) => allowed(origin),
        None => match header("sec-fetch-site") {
            Some(site) => site == "same-origin",
            None => header("referer").is_some_and(allowed),
        },
    };
    if trusted {
        Ok(())
    } else {
        Err(OIDCError::BadRequest(
            "Bad Request, login callback from a disallowed origin".to_string(),
        ))
    }
}

/// The IdP echoes this back as `state`. A ULID never contains the separator.
fn login_state(token: &str, redirect: &Url) -> String {
    format!("{token}.{redirect}")
}

/// Returns the redirect carried in `state` if its token matches the cookie.
fn verify_login_state<'a>(state: &'a str, token: &str) -> Option<&'a str> {
    let (state_token, redirect) = state.split_once('.')?;
    (!token.is_empty() && state_token == token).then_some(redirect)
}

/// Build the HTTP response for the login callback (XHR JSON or redirect).
fn build_login_response(
    redirect: Option<String>,
    cookies: [Cookie<'static>; 3],
    session_id: Ulid,
    username: &str,
    user_id: &str,
) -> HttpResponse {
    match redirect {
        Some(redirect) => redirect_to_client(
            &redirect,
            cookies.into_iter().chain([login_state_removal()]),
        ),
        None => {
            let mut response = HttpResponse::Ok();
            for cookie in cookies {
                response.cookie(cookie);
            }
            response.json(serde_json::json!({
                "session": session_id.to_string(),
                "username": username,
                "user_id": user_id,
            }))
        }
    }
}

/// Send the browser to the IdP with a fresh state token that only this
/// browser holds, so the callback can reject codes obtained elsewhere.
async fn redirect_to_idp(
    req: &HttpRequest,
    client: &Arc<RwLock<Box<dyn OAuthProvider>>>,
    query: &LoginQuery,
) -> HttpResponse {
    if let Some(url) = login_at_origin(req, query) {
        return HttpResponse::TemporaryRedirect()
            .insert_header((actix_web::http::header::LOCATION, url.as_str()))
            .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
            .finish();
    }
    let redirect = &query.redirect;
    let token = Ulid::new().to_string();
    let scope = PARSEABLE.options.scope.to_string();
    let mut auth_url: String = client
        .read()
        .await
        .auth_url(&scope, Some(login_state(&token, redirect)))
        .into();
    if let Some(query_params) = PARSEABLE.options.oidc_query_params.as_ref() {
        if !query_params.starts_with('&') {
            auth_url = format!("{auth_url}&{query_params}");
        } else {
            auth_url.push_str(query_params.as_str());
        }
    }
    HttpResponse::TemporaryRedirect()
        .cookie(cookie_login_state(&token))
        .insert_header((actix_web::http::header::LOCATION, auth_url))
        .finish()
}

/// The IdP returns browsers to `P_ORIGIN_URI`, so the state cookie must be set
/// on that host. A login reached under another name (`localhost` for
/// `127.0.0.1`, an internal DNS name) moves there first, but only once: a
/// proxy that rewrites `Host` would otherwise bounce the browser forever.
fn login_at_origin(req: &HttpRequest, query: &LoginQuery) -> Option<Url> {
    let origin = PARSEABLE.options.domain_address.as_ref()?;
    if query.at_origin {
        return None;
    }
    origin_login_url(origin, req.connection_info().host(), &query.redirect)
}

/// `/o/login` on `origin`, if `request_host` names another host. Cookies are
/// not scoped by scheme or port, so only the host name counts.
fn origin_login_url(origin: &Url, request_host: &str, redirect: &Url) -> Option<Url> {
    let request = Url::parse(&format!("http://{request_host}/")).ok()?;
    if request.host_str() == origin.host_str() {
        return None;
    }
    // Joined like the callback's `redirect_uri`, so a path prefix carries over.
    let mut url = origin
        .join(&format!("{API_BASE_PATH}/{API_VERSION}/o/login"))
        .ok()?;
    url.query_pairs_mut()
        .append_pair("redirect", redirect.as_str())
        .append_pair("at_origin", "true");
    Some(url)
}

fn request_base_url(req: &HttpRequest) -> String {
    let conn = req.connection_info();
    format!("{}://{}/", conn.scheme(), conn.host())
}

/// The origin the IdP returns browsers to. A client can set forwarded headers,
/// so trust `P_ORIGIN_URI`, or else the scheme Parseable serves and the Host.
fn callback_base_url(req: &HttpRequest) -> String {
    if let Some(url) = &PARSEABLE.options.domain_address {
        return url.origin().ascii_serialization();
    }
    let host = req
        .headers()
        .get(actix_web::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.uri().authority().map(|authority| authority.as_str()))
        .unwrap_or_default();
    format!("{}://{host}/", PARSEABLE.options.get_scheme())
}

/// Re-verifies the Basic credentials and issues a cookie session for them.
/// `new_basic_session` re-reads the user under SESSIONS, so an admin change
/// racing the login cannot leave a cookie session with stale credentials.
///
/// The Basic session `/login` authorized through is kept: other requests with
/// the same credentials may be between their middleware check and a later
/// `get_permissions`, and would see no permissions if it vanished. Dropping it
/// revoked nothing, as the next Basic request re-verifies and re-tracks it;
/// password changes, deletions and role changes still reach it through the
/// user's tracked sessions.
pub(crate) fn exchange_basic_for_cookie(
    username: &str,
    password: &str,
    tenant_id: &Option<String>,
    expires_in: TimeDelta,
) -> Option<Cookie<'static>> {
    let id = Ulid::new();
    Users
        .new_basic_session(
            username,
            password,
            tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
            SessionKey::SessionId(id),
            Utc::now() + expires_in,
        )
        .map(|_| cookie_session(id))
}

fn redirect_to_oidc_logout(mut logout_endpoint: Url, redirect: &Url) -> HttpResponse {
    logout_endpoint.set_query(Some(&format!("post_logout_redirect_uri={redirect}")));
    let mut response = HttpResponse::TemporaryRedirect();
    for cookie in removal_cookies() {
        response.cookie(cookie);
    }
    response
        .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
        .insert_header((
            actix_web::http::header::LOCATION,
            logout_endpoint.to_string(),
        ))
        .finish()
}

/// The session is already gone server-side; also drop the browser's copies so
/// a top-level logout navigation leaves no stale identity behind.
fn removal_cookies() -> [Cookie<'static>; 3] {
    [SESSION_COOKIE_NAME, USER_COOKIE_NAME, USER_ID_COOKIE_NAME].map(|name| {
        let mut cookie = build_cookie(name, String::new());
        cookie.make_removal();
        cookie
    })
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
    // The side-by-side frontend under `/next` has its own explanation page.
    let next_ui = url.path() == "/next" || url.path().starts_with("/next/");
    url.set_path(if next_ui {
        "next/oidc-not-configured"
    } else {
        "oidc-not-configured"
    });
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

fn cookie_login_state(token: &str) -> Cookie<'static> {
    let mut cookie = build_cookie(LOGIN_STATE_COOKIE_NAME, token.to_string());
    cookie.set_max_age(time::Duration::minutes(LOGIN_STATE_MINUTES));
    cookie.set_http_only(true);
    cookie
}

fn login_state_removal() -> Cookie<'static> {
    let mut cookie = cookie_login_state("");
    cookie.make_removal();
    cookie
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

/// Persist authorization from freshly verified claims, then update sessions and
/// the local user map. Admin writes share this lock to avoid lost manual grants.
pub(crate) async fn reconcile_oauth_session(
    mut session: OAuthSession,
    tenant: Option<String>,
    expected_userid: Option<&str>,
    session_key: SessionKey,
) -> Result<User, OIDCError> {
    use crate::oauth::authorization::{legacy_identity, matching_identity, reconcile_roles};

    if !group_mapping_enabled() {
        session.claims.groups.clear();
    }
    let expires_in = bearer_expiry(&session.bearer);
    let (_, user_id, user_info) = extract_identity(&session)?;
    let issuer = &session.claims.issuer;
    let subject = session
        .claims
        .sub
        .as_deref()
        .ok_or(OIDCError::Unauthorized)?;
    let _guard = UPDATE_LOCK.lock().await;
    if let Some(expected) = expected_userid {
        // Logout or admin invalidation while contacting the provider must win.
        let current_owner = Users.get_userid_from_session(&session_key);
        if !current_owner.is_some_and(|(user, session_tenant)| {
            user == expected && session_tenant == tenant.as_deref().unwrap_or(DEFAULT_TENANT)
        }) {
            return Err(OIDCError::Unauthorized);
        }
        // Most refreshes change nothing durable; skip the metastore round trip.
        if let Some(user) = unchanged_refresh(expected, &tenant, &session, &user_id, &user_info) {
            mut_users().insert(user.clone());
            Users.new_session(&user, session_key, expires_in);
            return Ok(user);
        }
    }
    let mut metadata = get_metadata(&tenant).await?;

    let existing = if let Some(expected) = expected_userid {
        // Refresh may not recreate a user deleted while contacting the provider.
        metadata
            .users
            .iter()
            .find(|user| user.userid() == expected && matching_identity(user, issuer, subject))
            .cloned()
            .ok_or(OIDCError::Unauthorized)
            .map(Some)?
    } else {
        // A bound identity can only match its own issuer and subject. An
        // issuer-less legacy identity is migrated with all ambiguous grants reset.
        let mut matches = metadata.users.iter().filter(|user| {
            matching_identity(user, issuer, subject) || legacy_identity(user, subject)
        });
        let candidate = matches.next().cloned();
        if matches.next().is_some() {
            return Err(OIDCError::Unauthorized);
        }
        candidate
    };
    if metadata.users.iter().any(|user| {
        user.userid() == user_id
            && existing
                .as_ref()
                .is_none_or(|existing| existing.userid() != user.userid())
    }) {
        return Err(OIDCError::Unauthorized);
    }
    if existing.as_ref().is_some_and(|user| user.protected) {
        return Err(OIDCError::Unauthorized);
    }
    let legacy = existing
        .as_ref()
        .is_some_and(|user| legacy_identity(user, subject));
    let grants = reconcile_roles(
        existing.as_ref(),
        &session.claims.groups,
        &metadata.roles,
        metadata.default_role.as_deref(),
    );
    let mut user = existing.clone().unwrap_or_else(|| {
        User::new_oauth(
            user_id.clone(),
            Default::default(),
            user_info.clone(),
            None,
            tenant.clone(),
            false,
        )
    });
    user.roles = grants.effective_roles();
    if legacy {
        // Unbound administrative group grants cannot be attributed to this issuer.
        user.user_groups.clear();
    }
    user.ty = UserType::OAuth(Box::new(user::OAuth {
        userid: user_id,
        user_info,
        bearer: None,
        issuer: Some(issuer.clone()),
        role_grants: Some(grants),
    }));
    let old_id = existing.as_ref().map(|user| user.userid().to_owned());
    let changed = existing.as_ref() != Some(&user);
    let authorization_changed = existing.as_ref().is_none_or(|old| {
        old.userid() != user.userid()
            || old.roles != user.roles
            || old.user_groups != user.user_groups
    });
    if changed {
        if let Some(old_id) = &old_id {
            metadata.users.retain(|entry| entry.userid() != old_id);
            for group in &mut metadata.user_groups {
                // Only migrate groups that actually contained this user.
                let belonged = group.users.iter().any(|entry| entry.userid() == old_id);
                if belonged {
                    group.users.retain(|entry| entry.userid() != old_id);
                    if !legacy {
                        group.users.insert(GroupUser::from_user(&user));
                    }
                }
            }
        }
        metadata.users.push(user.clone());
        put_metadata(&metadata, &tenant).await?;
        let tenant_name = tenant.as_deref().unwrap_or(DEFAULT_TENANT);
        for group in metadata.user_groups {
            write_user_groups().insert(group, tenant_name);
        }
    }
    if let Some(old_id) = &old_id {
        if authorization_changed {
            mut_sessions().remove_user(old_id, tenant.as_deref().unwrap_or(DEFAULT_TENANT));
        }
        if old_id != user.userid() {
            Users.delete_user(old_id, &tenant);
        }
    }
    if let UserType::OAuth(oauth) = &mut user.ty {
        // Access/refresh tokens remain in memory and are never persisted here.
        oauth.bearer = Some(session.bearer);
    }
    mut_users().insert(user.clone());
    // The session and current grants become visible under the same admin lock.
    Users.new_session(&user, session_key, expires_in);
    Ok(user)
}

/// Returns the in-memory user with the refreshed bearer when the verified claims
/// reproduce exactly the stored identity and grants. Must hold `UPDATE_LOCK`,
/// which serializes this with administrative role and grant writes.
fn unchanged_refresh(
    userid: &str,
    tenant: &Option<String>,
    session: &OAuthSession,
    user_id: &str,
    user_info: &user::UserInfo,
) -> Option<User> {
    use crate::oauth::authorization::{matching_identity, reconcile_roles};

    let mut user = Users.get_user(userid, tenant)?;
    let subject = session.claims.sub.as_deref()?;
    if user.protected || !matching_identity(&user, &session.claims.issuer, subject) {
        return None;
    }
    let tenant_name = tenant.as_deref().unwrap_or(DEFAULT_TENANT);
    let default_role = DEFAULT_ROLE.read().get(tenant_name).cloned().flatten();
    let grants = {
        let roles = roles();
        let empty = Default::default();
        reconcile_roles(
            Some(&user),
            &session.claims.groups,
            roles.get(tenant_name).unwrap_or(&empty),
            default_role.as_deref(),
        )
    };
    let UserType::OAuth(oauth) = &mut user.ty else {
        return None;
    };
    let unchanged = oauth.userid == user_id
        && &oauth.user_info == user_info
        && oauth.role_grants.as_ref() == Some(&grants)
        && user.roles == grants.effective_roles();
    if !unchanged {
        return None;
    }
    // Access/refresh tokens remain in memory and are never persisted here.
    oauth.bearer = Some(session.bearer.clone());
    Some(user)
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
    #[error("{0}")]
    HeaderBuild(#[from] HeaderBuildError),
}

impl actix_web::ResponseError for OIDCError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::ObjectStorageError(e) => e.http_status(),
            Self::Serde(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::HeaderBuild(e) => actix_web::ResponseError::status_code(e),
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

fn is_valid_redirect_url(base_url: &str, redirect_url: &str) -> bool {
    let Ok(redirect_url) = Url::parse(redirect_url) else {
        return false;
    };

    let redirect_origin = redirect_url.origin();

    if PARSEABLE
        .options
        .allow_origins
        .iter()
        .any(|url| url.origin() == redirect_origin)
    {
        return true;
    }

    Url::parse(base_url)
        .map(|url| url.origin() == redirect_origin)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(from: &str) -> String {
        let response = redirect_no_oauth_setup(Url::parse(from).unwrap());
        response
            .headers()
            .get(actix_web::http::header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn no_oauth_redirect_keeps_next_ui_requests_on_next() {
        assert_eq!(
            location("http://host/next"),
            "http://host/next/oidc-not-configured"
        );
        assert_eq!(
            location("http://host/next/logs?x=1"),
            "http://host/next/oidc-not-configured?x=1"
        );
    }

    fn callback(cookie: Option<&str>) -> HttpRequest {
        let mut req = actix_web::test::TestRequest::get().uri("/api/v1/o/code");
        if let Some(token) = cookie {
            req = req.cookie(Cookie::new(LOGIN_STATE_COOKIE_NAME, token.to_owned()));
        }
        req.to_http_request()
    }

    fn rejected(result: Result<String, OIDCError>) -> bool {
        matches!(result, Err(OIDCError::BadRequest(_)))
    }

    #[test]
    fn login_state_round_trips_the_redirect_for_the_matching_token() {
        let redirect = Url::parse("https://host.example/next/logs?q=a.b&x=1#frag").unwrap();
        let state = login_state("01JTOKEN", &redirect);
        assert_eq!(
            verify_login_state(&state, "01JTOKEN"),
            Some(redirect.as_str())
        );
    }

    #[test]
    fn login_state_rejects_other_tokens_and_unbound_states() {
        let redirect = Url::parse("https://host.example/").unwrap();
        let state = login_state("01JTOKEN", &redirect);
        assert_eq!(verify_login_state(&state, "01JOTHER"), None);
        assert_eq!(verify_login_state(&state, ""), None);
        // States issued before the binding, or crafted without a token.
        assert_eq!(verify_login_state("https://evil.example/", ""), None);
        assert_eq!(verify_login_state(".https://evil.example/", ""), None);
    }

    #[test]
    fn browser_callback_without_the_state_cookie_is_rejected() {
        let state = login_state("01JTOKEN", &Url::parse("https://evil.example/").unwrap());
        assert!(rejected(login_redirect(&callback(None), Some(&state))));
        assert!(rejected(login_redirect(&callback(None), None)));
        assert!(rejected(login_redirect(
            &callback(Some("01JOTHER")),
            Some(&state)
        )));
        assert!(rejected(login_redirect(&callback(Some("01JTOKEN")), None)));
    }

    #[test]
    fn login_moves_to_the_origin_host_only_when_the_host_differs() {
        let origin = Url::parse("http://127.0.0.1:8000/").unwrap();
        let redirect = Url::parse("http://localhost:8000/next/logs?q=a&b=1").unwrap();
        let url = origin_login_url(&origin, "localhost:8000", &redirect).unwrap();
        assert_eq!(url.origin(), origin.origin());
        assert_eq!(url.path(), "/api/v1/o/login");
        let query: Vec<_> = url.query_pairs().into_owned().collect();
        assert_eq!(
            query,
            [
                ("redirect".to_owned(), redirect.to_string()),
                ("at_origin".to_owned(), "true".to_owned()),
            ]
        );
        // Ports and case don't scope cookies, so these already share them.
        for host in ["127.0.0.1:8000", "127.0.0.1", "127.0.0.1:3000"] {
            assert_eq!(origin_login_url(&origin, host, &redirect), None, "{host}");
        }
        let origin = Url::parse("https://Parseable.Example/").unwrap();
        assert_eq!(
            origin_login_url(&origin, "parseable.example:443", &redirect),
            None
        );
        assert!(origin_login_url(&origin, "10.0.0.5:8000", &redirect).is_some());
        assert!(origin_login_url(&origin, "[::1]:8000", &redirect).is_some());
    }

    #[test]
    fn login_keeps_the_origin_path_prefix() {
        let origin = Url::parse("https://host.example/parseable/").unwrap();
        let redirect = Url::parse("https://host.example/parseable/").unwrap();
        let url = origin_login_url(&origin, "internal:8000", &redirect).unwrap();
        assert_eq!(url.path(), "/parseable/api/v1/o/login");
    }

    #[test]
    fn xhr_detection_matches_tenant_header_or_json_accept() {
        let req = |headers: &[(&str, &str)]| {
            let mut req = actix_web::test::TestRequest::get();
            for &header in headers {
                req = req.insert_header(header);
            }
            req.to_http_request()
        };
        assert!(is_xhr(&req(&[("x-p-tenant", "acme")])));
        assert!(is_xhr(&req(&[("accept", "application/json")])));
        assert!(!is_xhr(&req(&[(
            "accept",
            "text/html,application/xhtml+xml,*/*;q=0.8"
        )])));
        assert!(!is_xhr(&req(&[])));
    }

    #[test]
    fn json_callback_without_proof_of_origin_is_rejected() {
        let req = |headers: &[(&str, &str)]| {
            let mut req =
                actix_web::test::TestRequest::get().insert_header(("accept", "application/json"));
            for &header in headers {
                req = req.insert_header(header);
            }
            req.to_http_request()
        };
        // A `no-cors` fetch from another site sends no Origin, and over plain
        // HTTP no Fetch Metadata either; it can also suppress the Referer.
        for headers in [
            &[("sec-fetch-site", "cross-site")][..],
            &[("sec-fetch-site", "same-site")],
            &[("sec-fetch-site", "none")],
            // Fetch Metadata outranks a Referer, which a redirect chain carries along.
            &[
                ("sec-fetch-site", "cross-site"),
                ("referer", "https://parseable.example/"),
            ],
            &[],
        ] {
            assert!(matches!(
                verify_xhr_origin(&req(headers)),
                Err(OIDCError::BadRequest(_))
            ));
        }
        assert!(verify_xhr_origin(&req(&[("sec-fetch-site", "same-origin")])).is_ok());
    }

    #[test]
    fn state_cookie_is_short_lived_and_hidden_from_scripts() {
        let cookie = cookie_login_state("01JTOKEN");
        assert_eq!(cookie.http_only(), Some(true));
        assert_eq!(cookie.path(), Some("/"));
        assert_eq!(
            cookie.max_age(),
            Some(time::Duration::minutes(LOGIN_STATE_MINUTES))
        );
    }

    #[test]
    fn browser_login_response_clears_the_state_cookie() {
        let cookies = [
            cookie_session(Ulid::new()),
            cookie_username("user"),
            cookie_userid("id"),
        ];
        let response = build_login_response(
            Some("https://host.example/next".to_owned()),
            cookies,
            Ulid::new(),
            "user",
            "id",
        );
        assert_eq!(
            response
                .headers()
                .get(actix_web::http::header::LOCATION)
                .unwrap(),
            "https://host.example/next"
        );
        let state = response
            .cookies()
            .find(|cookie| cookie.name() == LOGIN_STATE_COOKIE_NAME)
            .expect("state cookie removal");
        assert_eq!(state.value(), "");
        assert_eq!(state.max_age(), Some(time::Duration::ZERO));
    }

    #[test]
    fn no_oauth_redirect_sends_everything_else_to_the_classic_page() {
        for from in [
            "http://host/",
            "http://host/nextfoo",
            "http://host/logs/next",
        ] {
            assert_eq!(
                location(from),
                "http://host/oidc-not-configured",
                "redirect from {from}"
            );
        }
    }

    // a request builder error, so no network is touched
    fn network_error() -> reqwest::Error {
        reqwest::Client::new().get("not a url").build().unwrap_err()
    }

    #[actix_web::test]
    async fn user_sync_header_build_input_fails_login_with_400() {
        let err = best_effort_user_sync(Err(HeaderBuildError::Input(
            crate::utils::header_parsing::ParseHeaderError::InvalidTenantId,
        )
        .into()))
        .unwrap_err();
        assert!(matches!(
            err,
            OIDCError::HeaderBuild(HeaderBuildError::Input(_))
        ));
        assert_eq!(
            actix_web::ResponseError::status_code(&err),
            StatusCode::BAD_REQUEST
        );
    }

    #[actix_web::test]
    async fn user_sync_header_build_internal_fails_login_with_generic_500() {
        let err =
            best_effort_user_sync(Err(HeaderBuildError::Internal("configured username").into()))
                .unwrap_err();
        assert!(matches!(
            err,
            OIDCError::HeaderBuild(HeaderBuildError::Internal("configured username"))
        ));
        let response = actix_web::ResponseError::error_response(&err);
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        assert_eq!(body, "Failed to build internal request headers");
    }

    #[actix_web::test]
    async fn user_sync_node_and_network_failures_stay_best_effort() {
        assert!(best_effort_user_sync(Ok(())).is_ok());
        assert!(best_effort_user_sync(Err(RBACError::Network(network_error()))).is_ok());
        assert!(best_effort_user_sync(Err(anyhow::anyhow!("node info").into())).is_ok());
    }
}
