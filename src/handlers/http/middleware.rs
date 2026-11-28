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
*
*/

use std::{
    future::{Ready, ready},
    rc::Rc,
    sync::Arc,
};

use actix_web::{
    Error, HttpMessage, HttpRequest, Route,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
    error::{ErrorBadRequest, ErrorForbidden, ErrorServiceUnavailable, ErrorUnauthorized},
    http::header::{self, HeaderMap, HeaderName, HeaderValue},
};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use chrono::{TimeDelta, Utc};
use dashmap::DashMap;
use futures_util::future::LocalBoxFuture;
use once_cell::sync::{Lazy, OnceCell};
use ulid::Ulid;

use crate::{
    handlers::{
        AUTHORIZATION_KEY, KINESIS_COMMON_ATTRIBUTES_KEY, LOG_SOURCE_KEY, LOG_SOURCE_KINESIS,
        STREAM_NAME_HEADER_KEY, TENANT_ID,
        http::{
            base_path, ingest::PostError, modal::OIDC_CLIENT,
            otel_generator::OtelGeneratorHeaderError, prism_base_path, promql, rbac::RBACError,
            traces::TraceError,
        },
    },
    option::Mode,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    rbac::{
        EXPIRY_DURATION,
        map::{EphemeralSession, SessionKey, mut_sessions, sessions, users},
        roles_to_permission, user,
        user::UserType,
    },
    tenants::TENANT_METADATA,
    tool_executor::ToolCallHeaderError,
    users::dashboard_runtime,
    utils::{
        get_user_and_tenant_from_request, get_user_from_request,
        header_parsing::{HeaderBuildError, ParseHeaderError, optional_header_str},
    },
};
use crate::{
    rbac::Users,
    rbac::{self, role::Action},
    utils::actix::extract_session_key,
};

use serde::{Deserialize, Serialize};

pub const CLUSTER_SECRET_HEADER: &str = "x-p-cluster-secret";
const INTRA_CLUSTER_TENANT_HEADER: &str = "intra-cluster-tenant";
const INTRA_CLUSTER_USERID_HEADER: &str = "intra-cluster-userid";
pub static CLUSTER_SECRET: OnceCell<(String, String)> = OnceCell::new();

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub common_attributes: CommonAttributes,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommonAttributes {
    #[serde(rename = "Authorization")]
    authorization: String,
    #[serde(rename = "X-P-Stream")]
    pub x_p_stream: String,
}

pub trait RouteExt {
    /// Authenticate a multiplexed endpoint whose handler authorizes each operation.
    fn authenticate(self) -> Self;
    fn authorize(self, action: Action) -> Self;
    fn authorize_for_resource(self, action: Action) -> Self;
    fn authorize_for_user(self, action: Action) -> Self;
}

impl RouteExt for Route {
    fn authenticate(self) -> Self {
        self.wrap(Auth {
            action: Action::Query,
            method: auth_session_only,
        })
    }
    fn authorize(self, action: Action) -> Self {
        self.wrap(Auth {
            action,
            method: auth_no_context,
        })
    }

    fn authorize_for_resource(self, action: Action) -> Self {
        self.wrap(Auth {
            action,
            method: auth_resource_context,
        })
    }

    fn authorize_for_user(self, action: Action) -> Self {
        self.wrap(Auth {
            action,
            method: auth_user_context,
        })
    }
}

// Authentication Layer with no context
pub struct Auth {
    pub action: Action,
    pub method: fn(&mut ServiceRequest, Action) -> Result<rbac::Response, Error>,
}

impl<S, B> Transform<S, ServiceRequest> for Auth
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = AuthMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(AuthMiddleware {
            action: self.action,
            service: Rc::new(service),
            auth_method: self.method,
        }))
    }
}

pub struct AuthMiddleware<S> {
    action: Action,
    auth_method: fn(&mut ServiceRequest, Action) -> Result<rbac::Response, Error>,
    service: Rc<S>,
}

impl<S, B> Service<ServiceRequest> for AuthMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, mut req: ServiceRequest) -> Self::Future {
        // Extract Kinesis Firehose headers if applicable
        if self.action.eq(&Action::Ingest) {
            extract_kinesis_headers(&mut req);
        }

        // Capture the incoming tenant header value before `get_user_and_tenant`
        // potentially mutates it, so the API-key branch below sees the original
        // client-supplied value. A malformed value is rejected here, before it
        // can be replaced or any identity or session is resolved.
        let tenant_id = match api_key_tenant_hint(req.headers()) {
            Ok(tenant_id) => tenant_id,
            Err(error) => {
                let error = tenant_header_error(&req, error);
                return Box::pin(async move { Err(error) });
            }
        };

        // an optional error to track the presence of CORRECT tenant header in case of ingestion
        let mut header_error = None;
        let user_and_tenant_id =
            match get_user_and_tenant(&self.action, &mut req, &mut header_error) {
                Ok(user_and_tenant_id) => user_and_tenant_id,
                Err(error) => return Box::pin(async move { Err(error) }),
            };

        // If an API key is present in X-API-KEY or as a Bearer token, resolve it to the backing
        // `UserType::ApiKey` user and attach a request-local session whose
        // permissions are derived from the user's assigned roles (same flow
        // as native/OAuth users). The request then falls through to the
        // normal auth check, which consults the session permissions. The
        // session lives in the request's extensions only, so it is never
        // entered into SESSIONS and ends with the request.
        let api_key_session = if let Some(api_key) = extract_api_key(&req) {
            let tenant_id: Option<String> = tenant_id.clone();
            match find_api_key_user(&api_key, &tenant_id) {
                Some(user) => {
                    let Some(api_key) = user.as_api_key() else {
                        return Box::pin(async { Err(ErrorUnauthorized("Invalid API key")) });
                    };
                    if let Err(error) =
                        insert_api_key_tenant_header(&mut req, &user, is_multi_tenant())
                    {
                        return Box::pin(async move { Err(Error::from(error)) });
                    }
                    let tenant = user.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
                    let session = Arc::new(EphemeralSession {
                        id: Ulid::new(),
                        userid: user.userid().to_owned(),
                        tenant: tenant.to_owned(),
                        expires_at: api_key.session_expires_at(Utc::now()),
                        permissions: roles_to_permission(user.roles(), tenant),
                    });
                    req.extensions_mut()
                        .insert(SessionKey::Ephemeral(Arc::clone(&session)));
                    Some(session)
                }
                None => {
                    let source_ip = req
                        .peer_addr()
                        .map(|address| address.ip().to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    let endpoint = req.path();
                    let user_agent = req
                        .headers()
                        .get(header::USER_AGENT)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    let dataset = req
                        .match_info()
                        .get("logstream")
                        .or_else(|| {
                            req.headers()
                                .get(STREAM_NAME_HEADER_KEY)
                                .and_then(|value| value.to_str().ok())
                        })
                        .unwrap_or("unknown");

                    tracing::warn!(
                        source_ip,
                        endpoint,
                        user_agent,
                        dataset,
                        requested_tenant = tenant_id.as_deref().unwrap_or("unknown"),
                        "Invalid API key"
                    );
                    return Box::pin(async { Err(ErrorUnauthorized("Invalid API key")) });
                }
            }
        } else {
            None
        };
        let key: Result<SessionKey, Error> = extract_session_key(&mut req);

        // if action is ingestion, the client-supplied tenant header must match
        // the tenant the credential itself belongs to
        if self.action.eq(&Action::Ingest)
            && let Ok(key) = &key
            && let (_, Some(tenant)) = &user_and_tenant_id
        {
            // Basic credentials are verified against the users of that tenant.
            // Every other credential is a session (a tracked cookie session,
            // or the request-local one of a forwarded or API-key request)
            // already bound to the tenant it was issued in, so the claimed
            // tenant must be the session's own.
            let tenant_matches = match &key {
                SessionKey::BasicAuth { username, password } => {
                    Users.validate_basic_user_tenant_id(username, password, tenant)
                }
                session @ (SessionKey::SessionId(_) | SessionKey::Ephemeral(_)) => Users
                    .get_userid_from_session(session)
                    .is_some_and(|(_, session_tenant)| session_tenant.eq(tenant)),
            };
            if !tenant_matches {
                header_error = Some(actix_web::Error::from(PostError::Header(
                    crate::utils::header_parsing::ParseHeaderError::InvalidTenantId,
                )));
            }
        }

        let auth_result: Result<_, Error> = (self.auth_method)(&mut req, self.action);
        let service = Rc::clone(&self.service);
        let auth_method = self.auth_method;
        let action = self.action;
        Box::pin(async move {
            let Ok(key) = key else {
                return Err(ErrorUnauthorized(
                    "Your session has expired or is no longer valid. Please re-authenticate to access this resource.",
                ));
            };

            if let Some(err) = header_error {
                return Err(err);
            }

            let first_response = auth_result?;
            // Basic auth retains its existing precedence when both mechanisms
            // are supplied. Only the selected tracked session may be refreshed;
            // a request-local one (API key or forwarded) is never renewed.
            let mut refreshed = false;
            match &key {
                SessionKey::Ephemeral(session) => {
                    let is_api_key_session = api_key_session
                        .as_ref()
                        .is_some_and(|api_key_session| api_key_session.id == session.id);
                    // The API-key session's expiry is reported below.
                    if !is_api_key_session && session.is_expired() {
                        return Err(ErrorUnauthorized(SESSION_EXPIRED));
                    }
                }
                key => {
                    if sessions().is_session_expired(key) {
                        // The refresh is the only reader of the headers, so
                        // they are copied only when a refresh is actually
                        // attempted. Nothing changes them between `call` and
                        // this point.
                        let headers = req.headers().clone();
                        refresh_token(Ok(user_and_tenant_id), key, headers).await?;
                        refreshed = true;
                    }
                }
            }
            if let Some(api_key_session) = &api_key_session
                && api_key_session.is_expired()
            {
                // This per-request session is capped by the credential's expiry.
                // Check it after any Basic refresh without scanning users again.
                return Err(ErrorUnauthorized("Invalid API key"));
            }
            // Recheck after an async refresh, which may have replaced the
            // session's permissions. Without one, an `Authorized` decision
            // made in `call` stands: the session is the same one it was made
            // on (an expired one was refreshed above). Every other decision
            // is re-evaluated, so a rejection reflects the current grants.
            let response = if !refreshed && first_response == rbac::Response::Authorized {
                first_response
            } else {
                auth_method(&mut req, action)?
            };
            match response {
                rbac::Response::UnAuthorized => {
                    return Err(ErrorForbidden(
                        "You don't have permission to access this resource. Please contact your administrator for assistance.",
                    ));
                }
                rbac::Response::ReloadRequired => {
                    return Err(ErrorUnauthorized(
                        "Your session has expired or is no longer valid. Please re-authenticate to access this resource.",
                    ));
                }
                rbac::Response::Suspended(msg) => {
                    return Err(ErrorBadRequest(msg));
                }
                _ => {}
            }

            service.call(req).await
        })
    }
}

/// Extract Kinesis Firehose headers (Authorization, X-P-Stream) from
/// the x-amz-firehose-common-attributes custom header when present.
fn extract_kinesis_headers(req: &mut ServiceRequest) {
    if let Some(kinesis_common_attributes) =
        req.request().headers().get(KINESIS_COMMON_ATTRIBUTES_KEY)
        && let Ok(attribute_value) = kinesis_common_attributes.to_str()
        && let Ok(message) = serde_json::from_str::<Message>(attribute_value)
        && let Ok(auth_value) =
            header::HeaderValue::from_str(&message.common_attributes.authorization)
        && let Ok(stream_name_key) =
            header::HeaderValue::from_str(&message.common_attributes.x_p_stream)
    {
        req.headers_mut()
            .insert(HeaderName::from_static(AUTHORIZATION_KEY), auth_value);
        req.headers_mut().insert(
            HeaderName::from_static(STREAM_NAME_HEADER_KEY),
            stream_name_key,
        );
        req.headers_mut().insert(
            HeaderName::from_static(LOG_SOURCE_KEY),
            header::HeaderValue::from_static(LOG_SOURCE_KINESIS),
        );
    }
}

/// Extract an API key from X-API-KEY or an Authorization Bearer token.
/// X-API-KEY takes precedence when both headers are present.
fn extract_api_key(req: &ServiceRequest) -> Option<String> {
    if let Some(api_key) = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
    {
        return Some(api_key);
    }

    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .and_then(|(scheme, api_key)| {
            scheme
                .eq_ignore_ascii_case("Bearer")
                .then_some(api_key.trim_start_matches(' '))
        })
        .filter(|api_key| !api_key.is_empty())
        .map(String::from)
}

/// The client-supplied tenant used to scope an API-key lookup: `x-p-tenant`,
/// or `intra-cluster-tenant` when `x-p-tenant` is absent. A missing header is
/// `Ok(None)`; a present but non visible-ASCII value is an error, never `None`.
fn api_key_tenant_hint(headers: &HeaderMap) -> Result<Option<String>, ParseHeaderError> {
    let tenant = match optional_header_str(headers, TENANT_ID) {
        Ok(Some(tenant)) => Some(tenant),
        Ok(None) => optional_header_str(headers, INTRA_CLUSTER_TENANT_HEADER)
            .map_err(|_| ParseHeaderError::InvalidTenantId)?,
        Err(_) => return Err(ParseHeaderError::InvalidTenantId),
    };
    Ok(tenant
        .filter(|tenant| !tenant.eq(&DEFAULT_TENANT))
        .map(String::from))
}

/// The rejection for a malformed tenant header. This happens before the
/// handler runs, so endpoints that answer errors in a JSON envelope get theirs
/// from the handler's own constructor; every other endpoint keeps the plain
/// text error. Endpoints are recognized by the registered route pattern, found
/// from the path as the router decoded it, so path parameters and
/// percent-encoded spellings of a route resolve to the same endpoint.
fn tenant_header_error(req: &ServiceRequest, error: ParseHeaderError) -> Error {
    let Some(pattern) = req.resource_map().match_pattern(req.match_info().as_str()) else {
        return PostError::Header(error).into();
    };
    // Registered by `promql::scope` and by the server's dashboards, llm,
    // traces and OTel generator scopes (the generator under both prefixes).
    if pattern.starts_with(&format!("{}/", promql::SCOPE)) {
        promql::ApiError::invalid_header(error).into()
    } else if pattern.starts_with(&format!("{}/services/traces/", prism_base_path())) {
        TraceError::invalid_header(error).into()
    } else if pattern == format!("{}/dashboards/preview", base_path()) {
        dashboard_runtime::invalid_header(error).into()
    } else if pattern == format!("{}/llm/tools/call", prism_base_path()) {
        ToolCallHeaderError(error).into()
    } else if pattern == format!("{}/otel_generator", prism_base_path())
        || pattern == format!("{}/otel_generator", base_path())
    {
        OtelGeneratorHeaderError(error).into()
    } else {
        PostError::Header(error).into()
    }
}

/// Encode a tenant id the server resolved from its own user records.
/// The value must read back as text, since handlers re-read the header;
/// anything else is corrupt stored data and a server error.
fn stored_tenant_header_value(tenant: &str) -> Result<HeaderValue, HeaderBuildError> {
    HeaderValue::from_str(tenant)
        .ok()
        .filter(|value| value.to_str().is_ok())
        .ok_or(HeaderBuildError::Internal("stored tenant id"))
}

/// Add the tenant resolved from an API-key user to a multi-tenant request.
/// The request is left unchanged on error.
fn insert_api_key_tenant_header(
    req: &mut ServiceRequest,
    user: &user::User,
    is_multi_tenant: bool,
) -> Result<(), HeaderBuildError> {
    if is_multi_tenant {
        let tenant = user.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
        let value = stored_tenant_header_value(tenant)?;
        req.headers_mut()
            .insert(HeaderName::from_static(TENANT_ID), value);
    }
    Ok(())
}

/// Unit tests cannot initialize `PARSEABLE`, which parses the process arguments,
/// so they exercise the middleware in single-tenant mode. Tests that need the
/// multi-tenant branches flip the test-only `TEST_MULTI_TENANT` flag while
/// holding the RBAC serial lock, so no other middleware test observes the change.
#[cfg(not(test))]
fn is_multi_tenant() -> bool {
    PARSEABLE.options.is_multi_tenant()
}

#[cfg(test)]
static TEST_MULTI_TENANT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
fn is_multi_tenant() -> bool {
    TEST_MULTI_TENANT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Resolve an incoming API key value to its backing `UserType::ApiKey` user.
/// In multi-tenant deployments the `tenant_hint` is required and the lookup
/// is scoped to that tenant; in single-tenant deployments we look in the
/// default tenant. This prevents an API key from one tenant authenticating a
/// request that is claiming another.
fn find_api_key_user(api_key_value: &str, tenant_id: &Option<String>) -> Option<user::User> {
    let tenant = if is_multi_tenant() {
        tenant_id.as_deref()?
    } else {
        DEFAULT_TENANT
    };

    let now = Utc::now();
    // Stored keys are hashes: hash the presented one once for the whole scan
    let hint_key = (tenant.to_owned(), user::hash_api_key(api_key_value));
    let hash = hint_key.1.as_str();
    let matches = |user: &user::User| matches!(&user.ty, UserType::ApiKey(api_key) if api_key.matches_active_hash(hash, now));
    // Copied out so no hint shard lock is held while USERS is read.
    let hinted = API_KEY_USER_HINTS
        .get(&hint_key)
        .map(|userid| userid.value().clone());

    let had_hint = hinted.is_some();
    let found = {
        let users_guard = users();
        users_guard.get(tenant).and_then(|tenant_users| {
            let hinted_user = hinted
                .as_deref()
                .and_then(|userid| tenant_users.get(userid))
                .filter(|user| matches(user));
            hinted_user
                .or_else(|| tenant_users.values().find(|user| matches(user)))
                .cloned()
        })
    };

    match &found {
        Some(user) => remember_api_key_user(hint_key, user.userid()),
        None if had_hint => {
            API_KEY_USER_HINTS.remove(&hint_key);
        }
        None => {}
    }
    found
}

/// Which user a presented API key last resolved to, keyed by the tenant the
/// lookup was scoped to and the key's SHA-256 (`hash_api_key`), so a repeat
/// request reads one user instead of scanning the tenant.
///
/// An entry is only a hint: the user it names is re-read from USERS and must
/// still pass `matches_active_hash` with the presented key's hash, otherwise
/// the tenant is scanned as before. Rotation, revocation, deletion, expiry and
/// role changes therefore apply on the very next request with no
/// invalidation. Only keys that resolved are recorded, so presenting invalid
/// keys cannot grow the map; it is cleared if it ever reaches its cap.
static API_KEY_USER_HINTS: Lazy<DashMap<(String, String), String>> = Lazy::new(DashMap::new);
const API_KEY_USER_HINTS_CAPACITY: usize = 10_000;

fn remember_api_key_user(hint_key: (String, String), userid: &str) {
    if let Some(hinted) = API_KEY_USER_HINTS.get(&hint_key)
        && hinted.value() == userid
    {
        return;
    }
    if API_KEY_USER_HINTS.len() >= API_KEY_USER_HINTS_CAPACITY {
        API_KEY_USER_HINTS.clear();
    }
    API_KEY_USER_HINTS.insert(hint_key, userid.to_owned());
}

/// Resolve the requesting user and tenant. Client header problems are
/// recorded in `header_error`; an `Err` is a server-side failure that must
/// fail the request immediately.
#[inline]
fn get_user_and_tenant(
    action: &Action,
    request: &mut ServiceRequest,
    header_error: &mut Option<Error>,
) -> Result<(Result<String, RBACError>, Option<String>), Error> {
    if is_multi_tenant() {
        // if ingestion then tenant MUST be present and should not be DEFAULT_TENANT
        let tenant = if action.eq(&Action::Ingest) {
            let tenant = optional_header_str(request.headers(), TENANT_ID)
                .map_err(|_| PostError::Header(ParseHeaderError::InvalidTenantId))?;
            if let Some(tenant) = tenant {
                if tenant.eq(DEFAULT_TENANT) {
                    *header_error = Some(actix_web::Error::from(PostError::Header(
                        crate::utils::header_parsing::ParseHeaderError::InvalidTenantId,
                    )));
                }
                Some(tenant.to_owned())
            } else {
                // tenant header is not present, error out
                *header_error = Some(actix_web::Error::from(PostError::Header(
                    crate::utils::header_parsing::ParseHeaderError::MissingTenantId,
                )));
                None
            }
        } else if action.eq(&Action::SuperAdmin) || action.eq(&Action::Login) {
            None
        } else {
            // tenant header should not be present, modify request to add
            let mut t = None;
            if let Ok((_, tenant)) = get_user_and_tenant_from_request(request.request())
                && let Some(tid) = tenant.as_ref()
            {
                let value = stored_tenant_header_value(tid)?;
                request
                    .headers_mut()
                    .insert(HeaderName::from_static(TENANT_ID), value);
                t = tenant;
            } else {
                // remove the header if already present
                request.headers_mut().remove(TENANT_ID);
            }
            t
        };
        let userid = get_user_from_request(request.request());
        Ok((userid, tenant))
    } else {
        // not multi-tenant, tenant header should NOT be present
        if request.headers().get(TENANT_ID).is_some() {
            *header_error = Some(actix_web::Error::from(PostError::Header(
                crate::utils::header_parsing::ParseHeaderError::UnexpectedHeader(TENANT_ID.into()),
            )));
        }
        let userid = get_user_from_request(request.request());
        Ok((userid, None))
    }
}

// Serialize refreshes per (tenant, userid) and reload bearer state after waiting.
// Concurrent browser requests must not reuse a refresh token that the previous
// request rotated; other users' refreshes proceed independently.
type RefreshLocks = DashMap<(String, String), Arc<tokio::sync::Mutex<()>>>;
static OAUTH_REFRESH_LOCKS: Lazy<RefreshLocks> = Lazy::new(DashMap::new);

const SESSION_EXPIRED: &str = "Your session has expired. Please re-authenticate.";

#[inline]
pub async fn refresh_token(
    user_and_tenant_id: Result<(Result<String, RBACError>, Option<String>), RBACError>,
    key: &SessionKey,
    headers: HeaderMap,
) -> Result<(), Error> {
    // A request-local session is never renewed into SESSIONS.
    if let SessionKey::Ephemeral(session) = key {
        return if session.is_expired() {
            Err(ErrorUnauthorized(SESSION_EXPIRED))
        } else {
            Ok(())
        };
    }
    if !sessions().is_session_expired(key) {
        return Ok(());
    }
    let (userid, tenant_id) = user_and_tenant_id
        .and_then(|(userid, tenant)| userid.map(|userid| (userid, tenant)))
        .map_err(|_| ErrorUnauthorized(SESSION_EXPIRED))?;
    let user = Users
        .get_user(&userid, &tenant_id)
        .ok_or_else(|| ErrorUnauthorized(SESSION_EXPIRED))?;
    if user.is_oauth() {
        let lock_key = (
            tenant_id.as_deref().unwrap_or(DEFAULT_TENANT).to_owned(),
            userid.clone(),
        );
        let lock = OAUTH_REFRESH_LOCKS
            .entry(lock_key.clone())
            .or_default()
            .clone();
        let result = {
            let _refresh_guard = lock.lock().await;
            refresh_oauth_session(&userid, &tenant_id, key, headers).await
        };
        drop(lock);
        // Waiters hold their own clone, so an entry is only dropped when idle.
        OAUTH_REFRESH_LOCKS.remove_if(&lock_key, |_, lock| Arc::strong_count(lock) == 1);
        return result;
    }
    // Serialize native renewal with administrative writes.
    let _guard = crate::handlers::http::rbac::UPDATE_LOCK.lock().await;
    if !Users
        .get_userid_from_session(key)
        .is_some_and(|(user, tenant)| {
            user == userid && tenant == tenant_id.as_deref().unwrap_or(DEFAULT_TENANT)
        })
    {
        return Err(ErrorUnauthorized(SESSION_EXPIRED));
    }
    let current = Users
        .get_user(&userid, &tenant_id)
        .ok_or_else(|| ErrorUnauthorized(SESSION_EXPIRED))?;
    Users.new_session(&current, key.clone(), EXPIRY_DURATION);
    Ok(())
}

/// Must run under the user's refresh lock. Only an affirmative refusal from the
/// provider (or a deleted/rebound user) ends all of the user's sessions;
/// transient provider or storage failures fail just this request and leave the
/// expired session in place so the next request retries.
async fn refresh_oauth_session(
    userid: &str,
    tenant_id: &Option<String>,
    key: &SessionKey,
    headers: HeaderMap,
) -> Result<(), Error> {
    use crate::{handlers::http::oidc::OIDCError, oauth::AuthorizationRevoked};

    // Another request may have refreshed this session while we waited.
    if !sessions().is_session_expired(key) {
        return Ok(());
    }
    // Reload after waiting: the previous holder may have rotated the bearer.
    let user = Users
        .get_user(userid, tenant_id)
        .ok_or_else(|| ErrorUnauthorized(SESSION_EXPIRED))?;
    let UserType::OAuth(oauth) = &user.ty else {
        return Err(ErrorUnauthorized(SESSION_EXPIRED));
    };
    let revoke = || {
        mut_sessions().remove_user(userid, tenant_id.as_deref().unwrap_or(DEFAULT_TENANT));
        ErrorUnauthorized(SESSION_EXPIRED)
    };
    let unavailable = || {
        ErrorServiceUnavailable(
            "Unable to revalidate your session with the identity provider. Please retry.",
        )
    };
    let session = match OIDC_CLIENT.get() {
        Some(client) => client
            .read()
            .await
            .refresh_session(oauth, Some(PARSEABLE.options.scope.as_str()), headers)
            .await
            .map_err(|error| {
                if AuthorizationRevoked::is_revoked(&error) {
                    revoke()
                } else {
                    tracing::warn!("OIDC session refresh failed transiently: {error}");
                    unavailable()
                }
            })?,
        None => return Err(revoke()),
    };
    match crate::handlers::http::oidc::reconcile_oauth_session(
        session,
        tenant_id.clone(),
        Some(userid),
        key.clone(),
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(OIDCError::Unauthorized) => Err(revoke()),
        Err(error) => {
            tracing::warn!("Failed to persist refreshed OIDC authorization: {error}");
            Err(unavailable())
        }
    }
}

#[inline(always)]
pub fn check_suspension(req: &HttpRequest, action: Action) -> rbac::Response {
    let tenant = req.headers().get(TENANT_ID).and_then(|v| v.to_str().ok());
    check_suspension_for_tenant(tenant, action)
}

/// Variant of check_suspension that takes the tenant id directly.
/// Useful when the caller has already captured the tenant from the request
/// (e.g., before another function may have mutated the TENANT_ID header).
pub fn check_suspension_for_tenant(tenant: Option<&str>, action: Action) -> rbac::Response {
    if let Some(tenant) = tenant
        && let Ok(Some(suspension)) = TENANT_METADATA.is_action_suspended(tenant, &action)
    {
        return rbac::Response::Suspended(suspension);
    }
    rbac::Response::Authorized
}

pub fn auth_no_context(req: &mut ServiceRequest, action: Action) -> Result<rbac::Response, Error> {
    // check if tenant is suspended
    if let rbac::Response::Suspended(msg) = check_suspension(req.request(), action) {
        return Ok(rbac::Response::Suspended(msg));
    }
    extract_session_key(req).map(|key| Users.authorize(key, action, None, None))
}

fn auth_session_only(req: &mut ServiceRequest, _action: Action) -> Result<rbac::Response, Error> {
    let key = extract_session_key(req)?;
    if !Users.session_exists(&key) {
        // Basic credentials are verified and lazily registered by authorize.
        // Its resource decision is intentionally left to the selected tool.
        let _ = Users.authorize(key.clone(), Action::GetAbout, None, None);
    }
    Ok(if Users.session_exists(&key) {
        rbac::Response::Authorized
    } else {
        rbac::Response::ReloadRequired
    })
}

pub fn auth_resource_context(
    req: &mut ServiceRequest,
    action: Action,
) -> Result<rbac::Response, Error> {
    // check if tenant is suspended
    if let rbac::Response::Suspended(msg) = check_suspension(req.request(), action) {
        return Ok(rbac::Response::Suspended(msg));
    }
    let creds = extract_session_key(req);
    let usergroup = req.match_info().get("usergroup");
    let llmid = req.match_info().get("llmid");
    let stream = req.match_info().get("logstream");
    if let Some(usergroup) = usergroup {
        creds.map(|key| Users.authorize(key, action, Some(usergroup), None))
    } else if let Some(llmid) = llmid {
        creds.map(|key| Users.authorize(key, action, Some(llmid), None))
    } else if let Some(stream) = stream {
        creds.map(|key| Users.authorize(key, action, Some(stream), None))
    } else {
        // Path resources take precedence, so the header is only read here.
        let stream = optional_header_str(req.headers(), STREAM_NAME_HEADER_KEY)
            .map_err(PostError::Header)?;
        creds.map(|key| Users.authorize(key, action, stream, None))
    }
}

pub fn auth_user_context(
    req: &mut ServiceRequest,
    action: Action,
) -> Result<rbac::Response, Error> {
    // check if tenant is suspended
    if let rbac::Response::Suspended(msg) = check_suspension(req.request(), action) {
        return Ok(rbac::Response::Suspended(msg));
    }
    let creds = extract_session_key(req);
    let user = req.match_info().get("userid");
    creds.map(|key| Users.authorize(key, action, None, user))
}

// The credentials set in the env vars (P_USERNAME & P_PASSWORD) are treated
// as root credentials. Any other user is not allowed to modify or delete
// the root user. Deny request if username is same as username
// from env variable P_USERNAME.
pub struct DisAllowRootUser;

impl<S, B> Transform<S, ServiceRequest> for DisAllowRootUser
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = DisallowRootUserMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(DisallowRootUserMiddleware { service }))
    }
}

pub struct DisallowRootUserMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for DisallowRootUserMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let username = req.match_info().get("userid").unwrap_or("");
        let is_root = username == PARSEABLE.options.username;
        let fut = self.service.call(req);

        Box::pin(async move {
            if is_root {
                return Err(ErrorBadRequest("Cannot call this API for root admin user"));
            }
            fut.await
        })
    }
}

// The credentials set in the env vars (P_USERNAME & P_PASSWORD) are treated
// as root credentials. Any other user is not allowed to modify or delete
// the root user. Deny request if username is same as username
// from env variable P_USERNAME.
pub struct IntraClusterRequest;

impl<S, B> Transform<S, ServiceRequest> for IntraClusterRequest
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = IntraClusterRequestMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(IntraClusterRequestMiddleware { service }))
    }
}

pub struct IntraClusterRequestMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for IntraClusterRequestMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, mut req: ServiceRequest) -> Self::Future {
        // Without a configured secret the request passes through unchanged.
        if let Some((secret, _)) = CLUSTER_SECRET.get()
            && let Err(error) = authenticate_intra_cluster_request(&mut req, secret)
        {
            // Rejected before the inner service sees the request.
            return Box::pin(async move { Err(error) });
        }

        // The forwarded user's session lives in the request and ends with it.
        Box::pin(self.service.call(req))
    }
}

/// Verify the forwarded cluster credentials and give the forwarded user a
/// short-lived request-local session (never entered into SESSIONS). Credentials present but not visible ASCII, or a
/// secret that is not a PHC password hash, are 400; missing or wrong
/// credentials are 401. Submitted values never appear in errors.
fn authenticate_intra_cluster_request(req: &mut ServiceRequest, secret: &str) -> Result<(), Error> {
    let headers = req.headers();
    let incoming_secret = optional_header_str(headers, CLUSTER_SECRET_HEADER)?;
    let tenant = optional_header_str(headers, INTRA_CLUSTER_TENANT_HEADER)?;
    let userid = optional_header_str(headers, INTRA_CLUSTER_USERID_HEADER)?;
    let (Some(incoming_secret), Some(tenant), Some(userid)) = (incoming_secret, tenant, userid)
    else {
        return Err(ErrorUnauthorized(
            "Incoming intra-cluster request doesn't contain the proper header or the server was started without P_CLUSTER_SECRET",
        ));
    };

    verify_cluster_secret_hash(secret, incoming_secret)?;

    let tenant_id = if tenant.eq(DEFAULT_TENANT) {
        None
    } else {
        Some(tenant.to_owned())
    };
    let Some(user) = Users.get_user(userid, &tenant_id) else {
        return Ok(());
    };
    let now = Utc::now();
    // Forwarded work must not outlive the API key that authorized it.
    if user
        .as_api_key()
        .is_some_and(|api_key| api_key.is_expired_at(now))
    {
        return Err(ErrorUnauthorized("Invalid API key"));
    }

    // Build the cookie before attaching the session so a failure here leaves
    // the request untouched.
    let id = Ulid::new();
    let cookie = HeaderValue::from_str(&format!("session={id}"))
        .map_err(|_| HeaderBuildError::Internal("intra-cluster session cookie"))?;
    req.headers_mut().insert(header::COOKIE, cookie);

    // remove basic auth header
    req.headers_mut().remove(header::AUTHORIZATION);

    let tenant = user.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
    let expires_at = now + TimeDelta::seconds(20);
    let expires_at = user
        .as_api_key()
        .map_or(expires_at, |api_key| api_key.cap_session_expiry(expires_at));
    let session = EphemeralSession {
        id,
        userid: user.userid().to_owned(),
        tenant: tenant.to_owned(),
        expires_at,
        permissions: roles_to_permission(user.roles(), tenant),
    };
    req.extensions_mut()
        .insert(SessionKey::Ephemeral(Arc::new(session)));
    Ok(())
}

/// PHC hashes of the cluster secret that already passed Argon2 verification,
/// as `(secret, hash)`. A node sends the same hash with every forwarded
/// request, so after the first one a peer's requests are checked with a
/// constant-time comparison instead of a fresh Argon2id verification on the
/// worker thread. Only hashes that verified against the paired secret are
/// recorded, so learning one requires the secret, and any other value is
/// parsed and verified exactly as before. Each peer contributes the hash it
/// was started with; the cap only bounds memory, evicting the oldest first.
static VERIFIED_CLUSTER_HASHES: Lazy<parking_lot::RwLock<Vec<(String, String)>>> =
    Lazy::new(|| parking_lot::RwLock::new(Vec::new()));
const VERIFIED_CLUSTER_HASHES_CAPACITY: usize = 64;

/// Byte equality whose time depends only on the lengths, so a caller cannot
/// learn a remembered hash byte by byte from response timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).fold(0u8, |differing, (a, b)| {
            std::hint::black_box(differing | (a ^ b))
        }) == 0
}

fn cluster_hash_already_verified(secret: &str, incoming_hash: &str) -> bool {
    // Every entry is compared, so the time does not reveal which one matched.
    VERIFIED_CLUSTER_HASHES
        .read()
        .iter()
        .fold(false, |found, (known_secret, known_hash)| {
            let secret_matches = constant_time_eq(known_secret.as_bytes(), secret.as_bytes());
            let hash_matches = constant_time_eq(known_hash.as_bytes(), incoming_hash.as_bytes());
            found | (secret_matches & hash_matches)
        })
}

/// Checks that `incoming_hash` is a PHC hash of `secret`: 400 if it is not a
/// PHC string, 401 if it does not verify.
fn verify_cluster_secret_hash(secret: &str, incoming_hash: &str) -> Result<(), Error> {
    if cluster_hash_already_verified(secret, incoming_hash) {
        return Ok(());
    }
    let parsed_hash = PasswordHash::new(incoming_hash)
        .map_err(|_| ErrorBadRequest("Incoming intra-cluster secret is malformed"))?;
    if Argon2::default()
        .verify_password(secret.as_bytes(), &parsed_hash)
        .is_err()
    {
        return Err(ErrorUnauthorized(
            "Incoming intra-cluster request validation failed",
        ));
    }

    let mut verified = VERIFIED_CLUSTER_HASHES.write();
    // Another request may have recorded it while this one verified.
    if !verified
        .iter()
        .any(|(known_secret, known_hash)| known_secret == secret && known_hash == incoming_hash)
    {
        if verified.len() >= VERIFIED_CLUSTER_HASHES_CAPACITY {
            verified.remove(0);
        }
        verified.push((secret.to_owned(), incoming_hash.to_owned()));
    }
    Ok(())
}

/// ModeFilterMiddleware factory
pub struct ModeFilter;

/// PathFilterMiddleware needs to implement Service trait
impl<S, B> Transform<S, ServiceRequest> for ModeFilter
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = ModeFilterMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ModeFilterMiddleware { service }))
    }
}

/// Actual middleware service
pub struct ModeFilterMiddleware<S> {
    service: S,
}

/// Impl the service trait for the middleware service
impl<S, B> Service<ServiceRequest> for ModeFilterMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    // impl poll_ready
    actix_web::dev::forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let path = req.path();
        let mode = &PARSEABLE.options.mode;
        // change error messages based on mode
        match mode {
            Mode::Query => {
                // In Query mode, only allows /ingest endpoint, and /logstream endpoint with GET method
                let base_cond = path.split('/').any(|x| x == "ingest");
                let logstream_cond =
                    !(path.split('/').any(|x| x == "logstream") && req.method() == "GET");
                if base_cond {
                    Box::pin(async {
                        Err(actix_web::error::ErrorUnauthorized(
                            "Ingestion API cannot be accessed in Query Mode",
                        ))
                    })
                } else if logstream_cond {
                    Box::pin(async {
                        Err(actix_web::error::ErrorUnauthorized(
                            "Logstream cannot be changed in Query Mode",
                        ))
                    })
                } else {
                    let fut = self.service.call(req);

                    Box::pin(async move {
                        let res = fut.await?;
                        Ok(res)
                    })
                }
            }

            Mode::Ingest => {
                let accessable_endpoints = ["ingest", "logstream", "liveness", "readiness"];
                let cond = path.split('/').any(|x| accessable_endpoints.contains(&x));
                if !cond {
                    Box::pin(async {
                        Err(actix_web::error::ErrorUnauthorized(
                            "Only Ingestion API can be accessed in Ingest Mode",
                        ))
                    })
                } else {
                    let fut = self.service.call(req);

                    Box::pin(async move {
                        let res = fut.await?;
                        Ok(res)
                    })
                }
            }

            Mode::All => {
                let fut = self.service.call(req);

                Box::pin(async move {
                    let res = fut.await?;
                    Ok(res)
                })
            }

            Mode::Index | Mode::Prism => {
                let fut = self.service.call(req);

                Box::pin(async move {
                    let res = fut.await?;
                    Ok(res)
                })
            }
        }
    }
}

#[cfg(test)]
mod api_key_header_tests {
    use std::collections::HashSet;

    use actix_web::test as actix_test;

    use super::*;

    #[test]
    fn extracts_x_api_key() {
        let req = actix_test::TestRequest::default()
            .insert_header(("x-api-key", "x-api-key-value"))
            .to_srv_request();

        assert_eq!(extract_api_key(&req).as_deref(), Some("x-api-key-value"));
    }

    #[test]
    fn extracts_bearer_api_key() {
        let req = actix_test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer bearer-api-key-value"))
            .to_srv_request();

        assert_eq!(
            extract_api_key(&req).as_deref(),
            Some("bearer-api-key-value")
        );
    }

    #[test]
    fn bearer_scheme_is_case_insensitive() {
        let req = actix_test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "bearer api-key-value"))
            .to_srv_request();

        assert_eq!(extract_api_key(&req).as_deref(), Some("api-key-value"));
    }

    #[test]
    fn bearer_allows_repeated_spaces_before_api_key() {
        let req = actix_test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer   api-key-value"))
            .to_srv_request();

        assert_eq!(extract_api_key(&req).as_deref(), Some("api-key-value"));
    }

    #[test]
    fn bearer_requires_api_key() {
        let req = actix_test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer   "))
            .to_srv_request();

        assert_eq!(extract_api_key(&req), None);
    }

    #[test]
    fn ignores_non_bearer_authorization() {
        let req = actix_test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Basic dXNlcjpwYXNzd29yZA=="))
            .to_srv_request();

        assert_eq!(extract_api_key(&req), None);
    }

    #[test]
    fn x_api_key_takes_precedence_over_bearer() {
        let req = actix_test::TestRequest::default()
            .insert_header(("x-api-key", "x-api-key-value"))
            .insert_header((header::AUTHORIZATION, "Bearer bearer-api-key-value"))
            .to_srv_request();

        assert_eq!(extract_api_key(&req).as_deref(), Some("x-api-key-value"));
    }

    #[test]
    fn extracted_header_values_follow_key_expiry() {
        let mut user = user::User::new_api_key(
            Ulid::new(),
            "test-secret".into(),
            "test".into(),
            HashSet::new(),
            "admin".into(),
            None,
        );
        let UserType::ApiKey(key) = &mut user.ty else {
            unreachable!()
        };
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        key.expires_at = Some(now);
        for (header_name, header_value) in [
            ("x-api-key", "test-secret"),
            ("authorization", "Bearer test-secret"),
        ] {
            let request = actix_test::TestRequest::default()
                .insert_header((header_name, header_value))
                .to_srv_request();
            let value = extract_api_key(&request).unwrap();
            assert!(key.matches_active_key(&value, now - TimeDelta::nanoseconds(1)));
            assert!(!key.matches_active_key(&value, now));
            assert!(!key.matches_active_key(&value, now + TimeDelta::nanoseconds(1)));
        }
    }

    #[test]
    fn resolved_api_key_user_sets_multi_tenant_header() {
        let user = user::User::new_api_key(
            Ulid::new(),
            "valid-api-key".to_owned(),
            "test-key".to_owned(),
            HashSet::new(),
            "admin".to_owned(),
            Some("acme".to_owned()),
        );
        let mut req = actix_test::TestRequest::default().to_srv_request();

        insert_api_key_tenant_header(&mut req, &user, true).unwrap();

        assert_eq!(req.headers().get(TENANT_ID).unwrap(), "acme");
    }

    #[test]
    fn unencodable_stored_api_key_tenant_is_a_server_error() {
        // Stored data that cannot be sent as a header, or would not read
        // back as text, is the server's fault and must not panic.
        for tenant in ["bad\ntenant", "tenänt"] {
            let user = user::User::new_api_key(
                Ulid::new(),
                "valid-api-key".to_owned(),
                "test-key".to_owned(),
                HashSet::new(),
                "admin".to_owned(),
                Some(tenant.to_owned()),
            );
            let mut req = actix_test::TestRequest::default().to_srv_request();

            let error = insert_api_key_tenant_header(&mut req, &user, true).unwrap_err();

            assert!(matches!(error, HeaderBuildError::Internal(_)));
            assert_eq!(
                Error::from(error).as_response_error().status_code(),
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
            );
            assert!(req.headers().get(TENANT_ID).is_none());
            // Single-tenant requests never carry the header.
            insert_api_key_tenant_header(&mut req, &user, false).unwrap();
            assert!(req.headers().get(TENANT_ID).is_none());
        }
    }

    fn header_map(pairs: &[(&'static str, &[u8])]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for &(name, value) in pairs {
            headers.insert(
                HeaderName::from_static(name),
                HeaderValue::from_bytes(value).unwrap(),
            );
        }
        headers
    }

    #[test]
    fn api_key_tenant_hint_rejects_malformed_headers() {
        let invalid = b"\xff".as_slice();
        for (pairs, expected) in [
            (vec![], Ok(None)),
            (vec![(TENANT_ID, b"acme".as_slice())], Ok(Some("acme"))),
            (vec![(TENANT_ID, DEFAULT_TENANT.as_bytes())], Ok(None)),
            (vec![(TENANT_ID, invalid)], Err(())),
            (vec![(TENANT_ID, b"caf\xc3\xa9".as_slice())], Err(())),
            // A malformed client tenant is not replaced by the fallback.
            (
                vec![
                    (TENANT_ID, invalid),
                    (INTRA_CLUSTER_TENANT_HEADER, b"acme".as_slice()),
                ],
                Err(()),
            ),
            (
                vec![(INTRA_CLUSTER_TENANT_HEADER, b"acme".as_slice())],
                Ok(Some("acme")),
            ),
            (vec![(INTRA_CLUSTER_TENANT_HEADER, invalid)], Err(())),
            // The fallback is only read when `x-p-tenant` is absent.
            (
                vec![
                    (TENANT_ID, b"acme".as_slice()),
                    (INTRA_CLUSTER_TENANT_HEADER, invalid),
                ],
                Ok(Some("acme")),
            ),
        ] {
            let result = api_key_tenant_hint(&header_map(&pairs));
            match expected {
                Ok(expected) => assert_eq!(result.unwrap().as_deref(), expected),
                Err(()) => assert!(matches!(result, Err(ParseHeaderError::InvalidTenantId))),
            }
        }
    }
}

#[cfg(test)]
mod api_key_auth_tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use actix_web::{
        App, HttpResponse,
        body::{MessageBody, to_bytes},
        dev::fn_service,
        http::{Method, StatusCode},
        test as actix_test, web,
    };
    use argon2::{PasswordHasher, password_hash::SaltString};
    use base64::{Engine, prelude::BASE64_STANDARD};
    use bytes::Bytes;
    use chrono::DateTime;
    use parking_lot::RwLock;
    use proptest::{
        collection::vec,
        prelude::*,
        test_runner::{Config, TestRunner},
    };
    use serde_json::{Value, json};
    use tokio::sync::MutexGuard;

    use super::*;
    use crate::rbac::{
        map::{
            ROLES, SESSIONS, Sessions, TEST_SERIAL, USER_GROUPS, USERS, UserGroups,
            Users as UserMap, mut_roles, mut_users,
        },
        role::model::{DefaultPrivilege, Role},
    };

    const ROLE: &str = "api-key-expiry-admin";
    const CLUSTER_TEST_SECRET: &str = "api-key-expiry-cluster-secret";

    type AuthMethod = fn(&mut ServiceRequest, Action) -> Result<rbac::Response, Error>;

    // `rbac::map::init` needs `PARSEABLE`, so tests create the maps directly.
    // Tests use their own users but run one at a time: concurrent first Basic
    // logins and session checks take the user and session locks in opposite order.
    async fn auth_state() -> MutexGuard<'static, ()> {
        let serial = TEST_SERIAL.lock().await;
        init_auth_maps();
        serial
    }

    fn blocking_auth_state() -> MutexGuard<'static, ()> {
        let serial = TEST_SERIAL.blocking_lock();
        init_auth_maps();
        serial
    }

    fn init_auth_maps() {
        USERS.get_or_init(|| RwLock::new(UserMap::default()));
        SESSIONS.get_or_init(|| RwLock::new(Sessions::default()));
        USER_GROUPS.get_or_init(|| RwLock::new(UserGroups::default()));
        // Other tests may create ROLES first, so add the role either way.
        ROLES.get_or_init(|| RwLock::new(HashMap::new()));
        mut_roles()
            .entry(DEFAULT_TENANT.to_owned())
            .or_default()
            .entry(ROLE.to_owned())
            .or_insert_with(|| Role::create_internal_role(vec![DefaultPrivilege::Admin]));
        CLUSTER_SECRET.get_or_init(|| {
            let salt = SaltString::encode_b64(b"api-key-expiry-salt").unwrap();
            let hash = Argon2::default()
                .hash_password(CLUSTER_TEST_SECRET.as_bytes(), &salt)
                .unwrap()
                .to_string();
            (CLUSTER_TEST_SECRET.to_owned(), hash)
        });
    }

    fn api_key_user(expires_at: Option<DateTime<Utc>>) -> (user::User, String) {
        let secret = format!("api-key-expiry-{}", Ulid::new());
        let mut user = user::User::new_api_key(
            Ulid::new(),
            secret.clone(),
            "expiry-test".into(),
            HashSet::from([ROLE.to_owned()]),
            "admin".into(),
            None,
        );
        let UserType::ApiKey(key) = &mut user.ty else {
            unreachable!()
        };
        key.expires_at = expires_at;
        mut_users().insert(user.clone());
        (user, secret)
    }

    fn api_key_headers(secret: &str) -> [(&'static str, String); 2] {
        [
            ("x-api-key", secret.to_owned()),
            ("authorization", format!("Bearer {secret}")),
        ]
    }

    fn cluster_headers(user: &user::User) -> [(&'static str, String); 3] {
        [
            (
                CLUSTER_SECRET_HEADER,
                CLUSTER_SECRET.get().unwrap().1.clone(),
            ),
            ("intra-cluster-tenant", DEFAULT_TENANT.to_owned()),
            ("intra-cluster-userid", user.userid().to_owned()),
        ]
    }

    fn has_sessions(user: &user::User) -> bool {
        sessions()
            .get_active_sessions()
            .iter()
            .any(|(_, userid, _)| userid == user.userid())
    }

    async fn request(
        method: AuthMethod,
        intra_cluster: bool,
        headers: &[(&'static str, String)],
    ) -> StatusCode {
        let headers: Vec<_> = headers
            .iter()
            .map(|(name, value)| (*name, HeaderValue::from_str(value).unwrap()))
            .collect();
        call_app(method, intra_cluster, "/", &headers).await.status
    }

    /// Calls to `counting_auth`. `Auth` always runs its method synchronously
    /// in `call` unless it rejects the request first, so an unchanged count
    /// shows an outer middleware never called into it. Tests only compare
    /// counts while holding `TEST_SERIAL`.
    static AUTH_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn counting_auth(req: &mut ServiceRequest, action: Action) -> Result<rbac::Response, Error> {
        AUTH_CALLS.fetch_add(1, Ordering::SeqCst);
        auth_no_context(req, action)
    }

    struct Outcome {
        status: StatusCode,
        error: Option<String>,
        handled: usize,
        auth_calls: usize,
    }

    /// Serves `/` and `/logstream/{logstream}` behind `Auth` (and optionally
    /// `IntraClusterRequest`), accepting raw header values.
    async fn call_app(
        method: AuthMethod,
        intra_cluster: bool,
        uri: &str,
        headers: &[(&'static str, HeaderValue)],
    ) -> Outcome {
        let handled = Arc::new(AtomicUsize::new(0));
        let route = |handled: Arc<AtomicUsize>| {
            let route = web::get()
                .to(move || {
                    handled.fetch_add(1, Ordering::SeqCst);
                    async { HttpResponse::Ok().finish() }
                })
                .wrap(Auth {
                    action: Action::Query,
                    method,
                });
            if intra_cluster {
                route.wrap(IntraClusterRequest)
            } else {
                route
            }
        };
        let app = actix_test::init_service(
            App::new()
                .route("/", route(Arc::clone(&handled)))
                .route("/logstream/{logstream}", route(Arc::clone(&handled))),
        )
        .await;
        let mut req = actix_test::TestRequest::get().uri(uri);
        for header in headers {
            req = req.insert_header(header.clone());
        }
        let auth_calls = AUTH_CALLS.load(Ordering::SeqCst);
        let (status, error) = match actix_test::try_call_service(&app, req.to_request()).await {
            Ok(response) => (response.status(), None),
            Err(error) => (
                error.as_response_error().status_code(),
                Some(error.to_string()),
            ),
        };
        Outcome {
            status,
            error,
            handled: handled.load(Ordering::SeqCst),
            auth_calls: AUTH_CALLS.load(Ordering::SeqCst) - auth_calls,
        }
    }

    fn basic_admin() -> (&'static str, HeaderValue) {
        let username = format!("header-test-{}", Ulid::new()).to_lowercase();
        let (mut basic, password) = user::User::new_basic(username.clone(), None, false);
        basic.roles.insert(ROLE.to_owned());
        mut_users().insert(basic);
        let credentials = BASE64_STANDARD.encode(format!("{username}:{password}"));
        (
            "authorization",
            HeaderValue::from_str(&format!("Basic {credentials}")).unwrap(),
        )
    }

    fn raw(value: &[u8]) -> HeaderValue {
        HeaderValue::from_bytes(value).unwrap()
    }

    fn text(value: &str) -> HeaderValue {
        HeaderValue::from_str(value).unwrap()
    }

    fn raw_cluster_headers(user: &user::User) -> Vec<(&'static str, HeaderValue)> {
        cluster_headers(user)
            .into_iter()
            .map(|(name, value)| (name, text(&value)))
            .collect()
    }

    fn with_header(
        mut headers: Vec<(&'static str, HeaderValue)>,
        name: &'static str,
        value: Option<HeaderValue>,
    ) -> Vec<(&'static str, HeaderValue)> {
        headers.retain(|(header, _)| *header != name);
        headers.extend(value.map(|value| (name, value)));
        headers
    }

    /// Lets the key's deadline pass after its request-local session was
    /// attached, by waiting out the (short) expiry the session carries.
    fn expire_session_then_authorize(
        req: &mut ServiceRequest,
        action: Action,
    ) -> Result<rbac::Response, Error> {
        let key = req.extensions().get::<SessionKey>().cloned().unwrap();
        let SessionKey::Ephemeral(session) = &key else {
            unreachable!("an API-key request carries a request-local session");
        };
        let wait = (session.expires_at - Utc::now())
            .to_std()
            .unwrap_or_default();
        assert!(wait < std::time::Duration::from_secs(5), "{wait:?}");
        std::thread::sleep(wait + std::time::Duration::from_millis(5));
        assert!(session.is_expired());
        auth_no_context(req, action)
    }

    /// The request-local session an inner service sees, and whether the
    /// request's user had a tracked session while it was handled.
    fn seen_session(req: &ServiceRequest) -> (Option<SessionKey>, bool) {
        let key = req.extensions().get::<SessionKey>().cloned();
        let tracked = key
            .as_ref()
            .and_then(|key| Users.get_userid_from_session(key))
            .is_some_and(|(userid, _)| {
                sessions()
                    .get_active_sessions()
                    .iter()
                    .any(|(_, tracked, _)| tracked == &userid)
            });
        (key, tracked)
    }

    #[actix_web::test]
    async fn active_api_key_authenticates_through_both_headers() {
        let _state = auth_state().await;
        let (user, secret) = api_key_user(Some(Utc::now() + TimeDelta::hours(1)));
        for header in api_key_headers(&secret) {
            assert_eq!(
                request(auth_no_context, false, &[header]).await,
                StatusCode::OK
            );
        }
        assert!(!has_sessions(&user));
    }

    #[actix_web::test]
    async fn nonexpiring_api_key_authenticates_through_both_headers() {
        let _state = auth_state().await;
        let (_, secret) = api_key_user(None);
        for header in api_key_headers(&secret) {
            assert_eq!(
                request(auth_no_context, false, &[header]).await,
                StatusCode::OK
            );
        }
    }

    #[actix_web::test]
    async fn expired_api_key_is_rejected_through_both_headers() {
        let _state = auth_state().await;
        let (user, secret) = api_key_user(Some(Utc::now() - TimeDelta::seconds(1)));
        for header in api_key_headers(&secret) {
            assert_eq!(
                request(auth_no_context, false, &[header]).await,
                StatusCode::UNAUTHORIZED
            );
        }
        assert!(!has_sessions(&user));
    }

    #[actix_web::test]
    async fn api_key_expiring_after_session_registration_is_rejected() {
        let _state = auth_state().await;
        for index in 0..2 {
            // Long enough for `call` to resolve the still-active key.
            let (user, secret) = api_key_user(Some(Utc::now() + TimeDelta::milliseconds(500)));
            let header = api_key_headers(&secret)[index].clone();
            assert_eq!(
                request(expire_session_then_authorize, false, &[header]).await,
                StatusCode::UNAUTHORIZED
            );
            assert!(!has_sessions(&user));
        }
    }

    #[actix_web::test]
    async fn intra_cluster_request_accepts_active_api_key_user() {
        let _state = auth_state().await;
        for expires_at in [None, Some(Utc::now() + TimeDelta::hours(1))] {
            let (user, _) = api_key_user(expires_at);
            assert_eq!(
                request(auth_no_context, true, &cluster_headers(&user)).await,
                StatusCode::OK
            );
            assert!(!has_sessions(&user));
        }
    }

    #[actix_web::test]
    async fn intra_cluster_request_rejects_expired_api_key_user() {
        let _state = auth_state().await;
        let (user, _) = api_key_user(Some(Utc::now() - TimeDelta::seconds(1)));
        assert_eq!(
            request(auth_no_context, true, &cluster_headers(&user)).await,
            StatusCode::UNAUTHORIZED
        );
        assert!(!has_sessions(&user));
    }

    #[actix_web::test]
    async fn intra_cluster_request_rejects_expired_api_key_despite_forwarded_basic_auth() {
        let _state = auth_state().await;
        // Distributed trace queries also forward the tenant's stored query credentials.
        let username = format!("query-{}", Ulid::new()).to_lowercase();
        let (mut basic, password) = user::User::new_basic(username.clone(), None, false);
        basic.roles.insert(ROLE.to_owned());
        mut_users().insert(basic);
        let forwarded = (
            "authorization",
            format!(
                "Basic {}",
                BASE64_STANDARD.encode(format!("{username}:{password}"))
            ),
        );
        assert_eq!(
            request(auth_no_context, false, std::slice::from_ref(&forwarded)).await,
            StatusCode::OK
        );

        let (user, _) = api_key_user(Some(Utc::now() - TimeDelta::seconds(1)));
        let mut headers = cluster_headers(&user).to_vec();
        headers.push(forwarded);
        assert_eq!(
            request(auth_no_context, true, &headers).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn new_sessions_never_outlive_the_api_key() {
        let _state = blocking_auth_state();
        let now = Utc::now();
        for (expires_at, expired) in [
            (None, false),
            (Some(now + TimeDelta::hours(1)), false),
            (Some(now - TimeDelta::seconds(1)), true),
        ] {
            let (user, _) = api_key_user(expires_at);
            let session = SessionKey::SessionId(Ulid::new());
            Users.new_session(&user, session.clone(), TimeDelta::seconds(20));
            assert_eq!(sessions().is_session_expired(&session), expired);
            mut_sessions().remove_session(&session);
        }
    }

    // A forwarded request gets a request-local session for the forwarded
    // user, capped by an API key's expiry, and nothing is tracked.
    #[test]
    fn forwarded_requests_get_a_request_local_session() {
        let _state = blocking_auth_state();
        let tracked = sessions().get_active_sessions().len();
        let now = Utc::now();
        let soon = now + TimeDelta::seconds(5);
        for (expires_at, cap) in [
            (None, None),
            (Some(now + TimeDelta::hours(1)), None),
            (Some(soon), Some(soon)),
        ] {
            let (user, _) = api_key_user(expires_at);
            let mut req = actix_test::TestRequest::default();
            for header in cluster_headers(&user) {
                req = req.insert_header(header);
            }
            let mut req = req.to_srv_request();
            authenticate_intra_cluster_request(&mut req, CLUSTER_TEST_SECRET).unwrap();

            let Some(SessionKey::Ephemeral(session)) =
                req.extensions().get::<SessionKey>().cloned()
            else {
                panic!("a forwarded request carries a request-local session");
            };
            assert_eq!(session.userid, user.userid());
            assert_eq!(session.tenant, DEFAULT_TENANT);
            // Role order is a set's, so compare the grants as sets.
            assert_eq!(
                session.permissions.iter().collect::<HashSet<_>>(),
                roles_to_permission(user.roles(), DEFAULT_TENANT)
                    .iter()
                    .collect::<HashSet<_>>()
            );
            match cap {
                Some(cap) => assert_eq!(session.expires_at, cap),
                None => assert!(
                    session.expires_at > now
                        && session.expires_at <= Utc::now() + TimeDelta::seconds(20)
                ),
            }
            // The cookie names the same session, as before.
            let cookie = req.headers().get(header::COOKIE).unwrap().to_str().unwrap();
            assert_eq!(cookie, format!("session={}", session.id));
            assert!(!has_sessions(&user));
        }
        assert_eq!(sessions().get_active_sessions().len(), tracked);
    }

    #[actix_web::test]
    async fn malformed_stream_header_is_rejected_before_authorization() {
        let _state = auth_state().await;
        let auth = basic_admin();
        let stream = |value: HeaderValue| vec![auth.clone(), (STREAM_NAME_HEADER_KEY, value)];

        for value in [raw(b"\xff"), raw(b"caf\xc3\xa9")] {
            let outcome = call_app(auth_resource_context, false, "/", &stream(value)).await;
            assert_eq!(outcome.status, StatusCode::BAD_REQUEST);
            assert!(outcome.error.unwrap().contains("visible ASCII"));
            assert_eq!(outcome.handled, 0);
        }

        let outcome = call_app(auth_resource_context, false, "/", &stream(text("app-logs"))).await;
        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!(outcome.handled, 1);
    }

    #[actix_web::test]
    async fn path_stream_takes_precedence_over_malformed_stream_header() {
        let _state = auth_state().await;
        let headers = [basic_admin(), (STREAM_NAME_HEADER_KEY, raw(b"\xff"))];

        let outcome = call_app(
            auth_resource_context,
            false,
            "/logstream/app-logs",
            &headers,
        )
        .await;

        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!(outcome.handled, 1);
    }

    #[actix_web::test]
    async fn malformed_tenant_header_is_rejected_before_api_key_session() {
        let _state = auth_state().await;
        let (user, secret) = api_key_user(None);
        let api_key = ("x-api-key", text(&secret));

        for tenant_header in [TENANT_ID, INTRA_CLUSTER_TENANT_HEADER] {
            let headers = [api_key.clone(), (tenant_header, raw(b"\xff"))];
            let outcome = call_app(counting_auth, false, "/", &headers).await;
            assert_eq!(outcome.status, StatusCode::BAD_REQUEST);
            // Rejected as malformed up front, not as an unexpected header
            // after the API key resolved an identity.
            assert!(outcome.error.unwrap().contains("Invalid tenant id"));
            assert_eq!((outcome.handled, outcome.auth_calls), (0, 0));
            assert!(!has_sessions(&user));
        }

        let outcome = call_app(counting_auth, false, "/", &[api_key]).await;
        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!(outcome.handled, 1);
        assert!(!has_sessions(&user));
    }

    /// Status, content type and body the client receives, whether the app
    /// answered or failed with an error.
    async fn read_reply<S, R, B>(app: &S, req: R) -> (StatusCode, Option<String>, Bytes)
    where
        S: Service<R, Response = ServiceResponse<B>, Error = Error>,
        B: MessageBody + 'static,
    {
        let response = match actix_test::try_call_service(app, req).await {
            Ok(response) => response.map_into_boxed_body().into_parts().1,
            Err(error) => error.error_response(),
        };
        reply_parts(response).await
    }

    /// Status, content type and body of `response`.
    async fn reply_parts(response: HttpResponse) -> (StatusCode, Option<String>, Bytes) {
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let status = response.status();
        let body = to_bytes(response.into_body()).await.expect("buffered body");
        (status, content_type, body)
    }

    /// The tenant error for each endpoint family, in its handler's envelope.
    fn expected_tenant_error(family: &str) -> Option<Value> {
        let message = ParseHeaderError::InvalidTenantId.to_string();
        match family {
            "promql" => Some(json!({"status":"error","errorType":"bad_data","error":message})),
            "traces" | "otel" => Some(json!({"error":message})),
            "preview" => Some(json!({
                "error": {"code":"INVALID_HEADER","message":message,"details":{}},
                "message": message,
            })),
            "tools" => Some(json!({
                "content": [{"type":"text","text":message}],
                "structuredContent": {"error":{"message":message}},
                "isError": true,
            })),
            _ => None,
        }
    }

    /// Asserts a malformed-tenant 400 in `family`'s format that does not echo
    /// the submitted value.
    fn assert_tenant_rejection(
        family: &str,
        context: &str,
        (status, content_type, body): (StatusCode, Option<String>, Bytes),
    ) {
        assert_eq!(status, StatusCode::BAD_REQUEST, "{context}");
        assert!(
            !body
                .windows(SUBMITTED_TENANT.len())
                .any(|w| w == SUBMITTED_TENANT),
            "{context}: the submitted tenant is echoed"
        );
        match expected_tenant_error(family) {
            Some(expected) => {
                assert_eq!(
                    content_type.as_deref(),
                    Some("application/json"),
                    "{context}"
                );
                let body: Value = serde_json::from_slice(&body).expect("JSON error body");
                assert_eq!(body, expected, "{context}");
            }
            None => {
                assert_eq!(
                    content_type.as_deref(),
                    Some("text/plain; charset=utf-8"),
                    "{context}"
                );
                assert_eq!(
                    body,
                    format!("Header Error: {}", ParseHeaderError::InvalidTenantId),
                    "{context}"
                );
            }
        }
    }

    const SUBMITTED_TENANT: &[u8] = b"acme-tenant";

    /// Present but not visible ASCII, carrying a recognizable submitted value.
    fn malformed_tenants() -> [HeaderValue; 2] {
        [raw(b"acme-tenant\xff"), raw(b"acme-tenant\xc3\xa9")]
    }

    #[actix_web::test]
    async fn malformed_tenant_is_rejected_in_each_endpoint_error_format() {
        let _state = auth_state().await;
        let (api_user, secret) = api_key_user(None);
        let credentials = [
            ("basic", basic_admin()),
            ("api key", ("x-api-key", text(&secret))),
        ];

        // Counting stubs behind `Auth` at the paths the servers register,
        // so only the middleware and the production error constructors run.
        let handled = Arc::new(AtomicUsize::new(0));
        let stub = |route: Route| {
            let handled = Arc::clone(&handled);
            route
                .to(move || {
                    handled.fetch_add(1, Ordering::SeqCst);
                    async { HttpResponse::Ok().finish() }
                })
                .wrap(Auth {
                    action: Action::Query,
                    method: counting_auth,
                })
        };
        let otel_stub = || {
            web::resource("")
                .route(stub(web::get()))
                .route(stub(web::post()))
                .route(stub(web::delete()))
        };
        let app = actix_test::init_service(
            App::new()
                .service(
                    web::scope(promql::SCOPE)
                        .route("/query", stub(web::get()))
                        .route("/query_range", stub(web::post()))
                        .route("/label/{name}/values", stub(web::get())),
                )
                .service(
                    web::scope(&base_path())
                        .route("/query", stub(web::post()))
                        .service(
                            web::scope("/dashboards")
                                .route("/preview", stub(web::post()))
                                .route("/{dashboard_id}", stub(web::post())),
                        )
                        .service(
                            web::scope("/otel_generator")
                                .service(otel_stub())
                                .route("/status", stub(web::get())),
                        ),
                )
                .service(
                    web::scope(&prism_base_path())
                        .service(
                            web::scope("/llm")
                                .route("/tools/list", stub(web::post()))
                                .route("/tools/call", stub(web::post())),
                        )
                        .service(
                            web::scope("/services/traces")
                                .route("/list", stub(web::post()))
                                .route("/detail", stub(web::post())),
                        )
                        .service(web::scope("/otel_generator").service(otel_stub())),
                ),
        )
        .await;

        let cases = [
            (Method::GET, "/prometheus/api/v1/query", "promql"),
            (Method::POST, "/prometheus/api/v1/query_range", "promql"),
            (Method::GET, "/prometheus/api/v1/label/job/values", "promql"),
            (Method::POST, "/api/prism/v1/services/traces/list", "traces"),
            (
                Method::POST,
                "/api/prism/v1/services/traces/detail",
                "traces",
            ),
            (
                Method::POST,
                "/api/prism/v1/services/traces/%6cist",
                "traces",
            ),
            (Method::POST, "/api/v1/dashboards/preview", "preview"),
            // Routed to `/preview`, though the raw path would match `/{dashboard_id}`.
            (Method::POST, "/api/v1/dashboards/%70review", "preview"),
            (Method::POST, "/api/prism/v1/llm/tools/call", "tools"),
            (Method::POST, "/api/prism/v1/llm/tools/%63all", "tools"),
            // The generator is served under the prism prefix by query
            // servers and under the base prefix by ingestors.
            (Method::GET, "/api/prism/v1/otel_generator", "otel"),
            (Method::POST, "/api/prism/v1/otel_generator", "otel"),
            (Method::DELETE, "/api/prism/v1/otel_generator", "otel"),
            (Method::GET, "/api/prism/v1/%6ftel_generator", "otel"),
            (Method::GET, "/api/v1/otel_generator", "otel"),
            (Method::POST, "/api/v1/otel_generator", "otel"),
            (Method::DELETE, "/api/v1/otel_generator", "otel"),
            (Method::DELETE, "/api/v1/otel_%67enerator", "otel"),
            // Ordinary endpoints keep the plain-text error, including the
            // SQL endpoint whose path is a suffix of a PromQL one, and a
            // route nested under the generator's.
            (Method::POST, "/api/v1/query", "plain"),
            (Method::POST, "/api/v1/dashboards/abc", "plain"),
            (Method::POST, "/api/prism/v1/llm/tools/list", "plain"),
            (Method::GET, "/api/v1/otel_generator/status", "plain"),
        ];
        let active_sessions = sessions().get_active_sessions().len();
        for (method, uri, family) in &cases {
            for (credential, auth) in &credentials {
                for tenant_header in [TENANT_ID, INTRA_CLUSTER_TENANT_HEADER] {
                    for tenant in malformed_tenants() {
                        let context =
                            format!("{method} {uri} with {credential} and {tenant_header}");
                        let handled_before = handled.load(Ordering::SeqCst);
                        let auth_calls = AUTH_CALLS.load(Ordering::SeqCst);
                        let req = actix_test::TestRequest::default()
                            .method(method.clone())
                            .uri(uri)
                            .insert_header(auth.clone())
                            .insert_header((tenant_header, tenant))
                            .to_request();
                        assert_tenant_rejection(family, &context, read_reply(&app, req).await);
                        assert_eq!(handled.load(Ordering::SeqCst), handled_before, "{context}");
                        assert_eq!(AUTH_CALLS.load(Ordering::SeqCst), auth_calls, "{context}");
                        assert_eq!(
                            sessions().get_active_sessions().len(),
                            active_sessions,
                            "{context}: a session was created"
                        );
                    }
                }
            }
        }
        assert!(!has_sessions(&api_user));

        // The same requests without a tenant header reach the handler, so
        // every path above is a registered route.
        for (method, uri, _) in &cases {
            let handled_before = handled.load(Ordering::SeqCst);
            let req = actix_test::TestRequest::default()
                .method(method.clone())
                .uri(uri)
                .insert_header(credentials[1].1.clone())
                .to_request();
            let (status, _, _) = read_reply(&app, req).await;
            assert_eq!(status, StatusCode::OK, "{method} {uri}");
            assert_eq!(
                handled.load(Ordering::SeqCst),
                handled_before + 1,
                "{method} {uri}"
            );
        }
        assert!(!has_sessions(&api_user));
    }

    #[actix_web::test]
    async fn production_promql_and_traces_routes_reject_malformed_tenant_as_json() {
        let _state = auth_state().await;
        let (api_user, secret) = api_key_user(None);
        // Mounted exactly as the servers' `configure_routes` mount them.
        let app = actix_test::init_service(
            App::new().service(promql::scope()).service(
                web::scope(&prism_base_path())
                    .service(crate::handlers::http::modal::server::Server::get_traces_webscope()),
            ),
        )
        .await;

        let cases = [
            (Method::GET, "/prometheus/api/v1/query", "promql"),
            (Method::POST, "/prometheus/api/v1/query", "promql"),
            (Method::GET, "/prometheus/api/v1/query_range", "promql"),
            (Method::GET, "/prometheus/api/v1/labels", "promql"),
            (
                Method::GET,
                "/prometheus/api/v1/label/__name__/values",
                "promql",
            ),
            (Method::POST, "/api/prism/v1/services/traces/list", "traces"),
            (
                Method::POST,
                "/api/prism/v1/services/traces/detail",
                "traces",
            ),
        ];
        let active_sessions = sessions().get_active_sessions().len();
        for (method, uri, family) in cases {
            for tenant_header in [TENANT_ID, INTRA_CLUSTER_TENANT_HEADER] {
                for tenant in malformed_tenants() {
                    let req = actix_test::TestRequest::default()
                        .method(method.clone())
                        .uri(uri)
                        .insert_header(("x-api-key", text(&secret)))
                        .insert_header((tenant_header, tenant))
                        .to_request();
                    let context = format!("{method} {uri} with {tenant_header}");
                    assert_tenant_rejection(family, &context, read_reply(&app, req).await);
                    assert_eq!(sessions().get_active_sessions().len(), active_sessions);
                }
            }
        }
        assert!(!has_sessions(&api_user));
    }

    #[actix_web::test]
    async fn production_otel_generator_routes_reject_malformed_tenant_as_json() {
        use crate::handlers::http::modal::server::Server;

        let _state = auth_state().await;
        let (api_user, secret) = api_key_user(None);
        let credentials = [
            ("basic", basic_admin()),
            ("api key", ("x-api-key", text(&secret))),
        ];
        // Mounted exactly as the query and ingest servers mount them. The
        // handlers need `PARSEABLE`, so every request must stop in `Auth`.
        let app = actix_test::init_service(
            App::new()
                .service(
                    web::scope(&prism_base_path()).service(Server::get_otel_generator_webscope()),
                )
                .service(
                    web::scope(&base_path()).service(Server::get_otel_generator_ingest_webscope()),
                ),
        )
        .await;

        let cluster = raw_cluster_headers(&api_user);
        let mut cases = Vec::new();
        for method in [Method::GET, Method::POST, Method::DELETE] {
            for uri in [
                "/api/prism/v1/otel_generator",
                "/api/prism/v1/%6ftel_generator",
            ] {
                for tenant_header in [TENANT_ID, INTRA_CLUSTER_TENANT_HEADER] {
                    cases.push((method.clone(), uri, tenant_header, Vec::new()));
                }
            }
            // The ingest scope first verifies the forwarded cluster
            // credentials, which carry a well-formed `intra-cluster-tenant`.
            for uri in ["/api/v1/otel_generator", "/api/v1/otel_%67enerator"] {
                cases.push((method.clone(), uri, TENANT_ID, cluster.clone()));
            }
        }
        let active_sessions = sessions().get_active_sessions().len();
        for (method, uri, tenant_header, forwarded) in &cases {
            for (credential, auth) in &credentials {
                for tenant in malformed_tenants() {
                    let context = format!("{method} {uri} with {credential} and {tenant_header}");
                    let mut req = actix_test::TestRequest::default()
                        .method(method.clone())
                        .uri(uri)
                        .insert_header(auth.clone());
                    for header in forwarded {
                        req = req.insert_header(header.clone());
                    }
                    let req = req.insert_header((*tenant_header, tenant)).to_request();
                    let error = actix_test::try_call_service(&app, req)
                        .await
                        .err()
                        .unwrap_or_else(|| panic!("{context}: reached the handler"));
                    assert_tenant_rejection(
                        "otel",
                        &context,
                        reply_parts(error.error_response()).await,
                    );
                    assert_eq!(
                        sessions().get_active_sessions().len(),
                        active_sessions,
                        "{context}: a session was left behind"
                    );
                }
            }
        }
        assert!(!has_sessions(&api_user));
    }

    #[actix_web::test]
    async fn intra_cluster_rejects_malformed_credentials_before_inner_service() {
        let _state = auth_state().await;
        let (user, _) = api_key_user(None);
        let valid = raw_cluster_headers(&user);
        let malformed_hash = "not-a-phc-hash";

        // The messages show `IntraClusterRequest` rejected the request, not
        // the inner `Auth`, which reports a malformed tenant differently.
        for (name, value, message) in [
            (CLUSTER_SECRET_HEADER, raw(b"\xff"), "visible ASCII"),
            (INTRA_CLUSTER_TENANT_HEADER, raw(b"\xff"), "visible ASCII"),
            (
                INTRA_CLUSTER_USERID_HEADER,
                raw(b"caf\xc3\xa9"),
                "visible ASCII",
            ),
            (CLUSTER_SECRET_HEADER, text(malformed_hash), "malformed"),
        ] {
            let headers = with_header(valid.clone(), name, Some(value));
            let outcome = call_app(counting_auth, true, "/", &headers).await;
            assert_eq!(outcome.status, StatusCode::BAD_REQUEST, "{name}");
            let error = outcome.error.unwrap();
            assert!(error.contains(message), "{name}: {error}");
            assert!(!error.contains(malformed_hash));
            assert_eq!((outcome.handled, outcome.auth_calls), (0, 0), "{name}");
            assert!(!has_sessions(&user));
        }

        let outcome = call_app(counting_auth, true, "/", &valid).await;
        assert_eq!(outcome.status, StatusCode::OK);
        // Authorized in `call` with no refresh, so the decision is not re-run.
        assert_eq!((outcome.handled, outcome.auth_calls), (1, 1));
        assert!(!has_sessions(&user));
    }

    /// Default Argon2 parameters take seconds per hash in debug builds;
    /// verification uses the parameters recorded in the hash.
    fn cheap_hash(secret: &str) -> String {
        use argon2::{Algorithm, Params, Version};
        let salt = SaltString::encode_b64(&Ulid::new().to_bytes()).unwrap();
        let params = Params::new(Params::MIN_M_COST, 1, 1, None).unwrap();
        Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
            .hash_password(secret.as_bytes(), &salt)
            .unwrap()
            .to_string()
    }

    fn remembered_cluster_hash(secret: &str, hash: &str) -> bool {
        VERIFIED_CLUSTER_HASHES
            .read()
            .iter()
            .any(|(known_secret, known_hash)| known_secret == secret && known_hash == hash)
    }

    #[test]
    fn verified_cluster_hashes_are_remembered_per_secret() {
        let secret = format!("cluster-cache-{}", Ulid::new());
        let other_secret = format!("cluster-cache-other-{}", Ulid::new());
        let hash = cheap_hash(&secret);
        let status = |result: Result<(), Error>| {
            result
                .err()
                .map(|error| error.as_response_error().status_code())
        };

        // A hash of another secret, or a malformed one, never verifies and
        // is never remembered.
        let wrong = cheap_hash(&other_secret);
        assert_eq!(
            status(verify_cluster_secret_hash(&secret, &wrong)),
            Some(StatusCode::UNAUTHORIZED)
        );
        assert!(!remembered_cluster_hash(&secret, &wrong));
        assert_eq!(
            status(verify_cluster_secret_hash(&secret, "not-a-phc-hash")),
            Some(StatusCode::BAD_REQUEST)
        );
        assert!(!remembered_cluster_hash(&secret, "not-a-phc-hash"));

        // The first request runs Argon2 and remembers the hash; later ones
        // are answered from the record.
        assert!(!remembered_cluster_hash(&secret, &hash));
        assert_eq!(status(verify_cluster_secret_hash(&secret, &hash)), None);
        assert!(remembered_cluster_hash(&secret, &hash));
        assert!(cluster_hash_already_verified(&secret, &hash));
        assert_eq!(status(verify_cluster_secret_hash(&secret, &hash)), None);
        assert_eq!(
            VERIFIED_CLUSTER_HASHES
                .read()
                .iter()
                .filter(|(known_secret, _)| known_secret == &secret)
                .count(),
            1
        );

        // The record is bound to the secret it verified against: the same
        // hash presented to a node with another secret is verified afresh.
        assert!(!cluster_hash_already_verified(&other_secret, &hash));
        assert_eq!(
            status(verify_cluster_secret_hash(&other_secret, &hash)),
            Some(StatusCode::UNAUTHORIZED)
        );
        assert!(!remembered_cluster_hash(&other_secret, &hash));
        // A one-character change to a remembered hash is not a hit.
        let mut altered = hash.clone();
        let last = altered.pop().unwrap();
        altered.push(if last == 'A' { 'B' } else { 'A' });
        assert!(!cluster_hash_already_verified(&secret, &altered));
    }

    #[test]
    fn constant_time_eq_matches_plain_equality() {
        for (a, b) in [
            ("", ""),
            ("a", "a"),
            ("a", "b"),
            ("abc", "abd"),
            ("abc", "ab"),
            ("ab", "abc"),
        ] {
            assert_eq!(constant_time_eq(a.as_bytes(), b.as_bytes()), a == b);
        }
    }

    fn hinted_user(secret: &str) -> Option<String> {
        API_KEY_USER_HINTS
            .get(&(DEFAULT_TENANT.to_owned(), user::hash_api_key(secret)))
            .map(|userid| userid.value().clone())
    }

    #[test]
    fn api_key_hints_never_outlive_the_key() {
        let _state = blocking_auth_state();
        let (user, secret) = api_key_user(None);
        let userid = user.userid().to_owned();
        let lookup =
            |secret: &str| find_api_key_user(secret, &None).map(|user| user.userid().to_owned());

        assert_eq!(lookup(&secret).as_deref(), Some(userid.as_str()));
        assert_eq!(hinted_user(&secret).as_deref(), Some(userid.as_str()));
        // Answered through the hint.
        assert_eq!(lookup(&secret).as_deref(), Some(userid.as_str()));

        // Invalid keys resolve nothing and are not recorded.
        let invalid = format!("invalid-{}", Ulid::new());
        assert_eq!(lookup(&invalid), None);
        assert_eq!(hinted_user(&invalid), None);

        // Expiry applies to a hinted key on the next lookup.
        let set_expiry = |expires_at: Option<DateTime<Utc>>| {
            let mut users = mut_users();
            let Some(UserType::ApiKey(key)) = users
                .get_mut(DEFAULT_TENANT)
                .and_then(|users| users.get_mut(&userid))
                .map(|user| &mut user.ty)
            else {
                unreachable!()
            };
            key.expires_at = expires_at;
        };
        set_expiry(Some(Utc::now() - TimeDelta::seconds(1)));
        assert_eq!(lookup(&secret), None);
        assert_eq!(hinted_user(&secret), None);
        set_expiry(None);
        assert_eq!(lookup(&secret).as_deref(), Some(userid.as_str()));

        // Rotation: the old key stops working, the new one resolves.
        let rotated = format!("rotated-{}", Ulid::new());
        {
            let mut users = mut_users();
            let Some(UserType::ApiKey(key)) = users
                .get_mut(DEFAULT_TENANT)
                .and_then(|users| users.get_mut(&userid))
                .map(|user| &mut user.ty)
            else {
                unreachable!()
            };
            key.key_hash = user::hash_api_key(&rotated);
        }
        assert_eq!(lookup(&secret), None);
        assert_eq!(hinted_user(&secret), None);
        assert_eq!(lookup(&rotated).as_deref(), Some(userid.as_str()));

        // Deletion: a hinted key of a deleted user resolves nothing.
        Users.delete_user(&userid, &None);
        assert_eq!(lookup(&rotated), None);
        assert_eq!(hinted_user(&rotated), None);
    }

    #[test]
    fn stale_api_key_hint_falls_back_to_the_matching_user() {
        let _state = blocking_auth_state();
        let (owner, secret) = api_key_user(None);
        let (other, _) = api_key_user(None);
        // A hint naming a user that does not hold the key is ignored.
        API_KEY_USER_HINTS.insert(
            (DEFAULT_TENANT.to_owned(), user::hash_api_key(&secret)),
            other.userid().to_owned(),
        );
        let found = find_api_key_user(&secret, &None).unwrap();
        assert_eq!(found.userid(), owner.userid());
        assert_eq!(hinted_user(&secret).as_deref(), Some(owner.userid()));
        // A hint naming a user that no longer exists is ignored too.
        API_KEY_USER_HINTS.insert(
            (DEFAULT_TENANT.to_owned(), user::hash_api_key(&secret)),
            format!("missing-{}", Ulid::new()),
        );
        let found = find_api_key_user(&secret, &None).unwrap();
        assert_eq!(found.userid(), owner.userid());
    }

    #[actix_web::test]
    async fn api_key_requests_still_end_their_sessions_with_cached_lookup() {
        let _state = auth_state().await;
        let (user, secret) = api_key_user(Some(Utc::now() + TimeDelta::hours(1)));
        for _ in 0..3 {
            let outcome =
                call_app(counting_auth, false, "/", &[("x-api-key", text(&secret))]).await;
            assert_eq!(outcome.status, StatusCode::OK);
            assert_eq!((outcome.handled, outcome.auth_calls), (1, 1));
            assert!(!has_sessions(&user));
        }
        // Revocation between requests is seen on the next one.
        Users.delete_user(user.userid(), &None);
        let outcome = call_app(counting_auth, false, "/", &[("x-api-key", text(&secret))]).await;
        assert_eq!(outcome.status, StatusCode::UNAUTHORIZED);
        assert_eq!(outcome.handled, 0);
    }

    #[actix_web::test]
    async fn authorization_is_rechecked_only_when_a_session_was_refreshed() {
        let _state = auth_state().await;
        let username = format!("refresh-test-{}", Ulid::new()).to_lowercase();
        let (mut native, _) = user::User::new_basic(username, None, false);
        native.roles.insert(ROLE.to_owned());
        mut_users().insert(native.clone());
        let cookie = |key: &SessionKey| {
            let SessionKey::SessionId(id) = key else {
                unreachable!()
            };
            vec![("cookie", text(&format!("session={id}")))]
        };

        // A live session: decided once, in `call`.
        let live = SessionKey::SessionId(Ulid::new());
        Users.new_session(&native, live.clone(), TimeDelta::minutes(5));
        let outcome = call_app(counting_auth, false, "/", &cookie(&live)).await;
        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!((outcome.handled, outcome.auth_calls), (1, 1));

        // An expired native session is renewed, then authorized again.
        let expired = SessionKey::SessionId(Ulid::new());
        Users.new_session(&native, expired.clone(), TimeDelta::seconds(-1));
        assert!(sessions().is_session_expired(&expired));
        let outcome = call_app(counting_auth, false, "/", &cookie(&expired)).await;
        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!((outcome.handled, outcome.auth_calls), (1, 2));
        assert!(!sessions().is_session_expired(&expired));

        // A rejection is always re-evaluated, and still rejects.
        Users.remove_roles(native.userid(), HashSet::from([ROLE.to_owned()]), &None);
        let outcome = call_app(counting_auth, false, "/", &cookie(&live)).await;
        assert_eq!(outcome.status, StatusCode::FORBIDDEN);
        assert_eq!((outcome.handled, outcome.auth_calls), (0, 2));
        Users.delete_user(native.userid(), &None);
    }

    // Sending X-API-KEY alongside an Authorization: Basic header must resolve
    // to the same authenticated request-local SessionKey in both middleware and
    // downstream handlers, preventing username spoofing or privilege mismatch.
    #[actix_web::test]
    async fn x_api_key_with_basic_header_resolves_consistently_to_ephemeral_session() {
        use base64::Engine;
        let _state = auth_state().await;
        let (user, secret) = api_key_user(Some(Utc::now() + TimeDelta::hours(1)));
        let bogus_basic = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("admin:wrong-password")
        );
        let seen_user: Arc<std::sync::Mutex<Option<String>>> = Arc::default();
        let seen_user_handler = Arc::clone(&seen_user);
        let app = actix_test::init_service(
            App::new().route(
                "/",
                web::get()
                    .to(move |req: actix_web::HttpRequest| {
                        let seen_user_handler = Arc::clone(&seen_user_handler);
                        async move {
                            let (userid, _) = get_user_and_tenant_from_request(&req).unwrap();
                            *seen_user_handler.lock().unwrap() = Some(userid);
                            HttpResponse::Ok().finish()
                        }
                    })
                    .authorize(Action::Query),
            ),
        )
        .await;
        let req = actix_test::TestRequest::get()
            .uri("/")
            .insert_header(("x-api-key", secret))
            .insert_header((header::AUTHORIZATION, bogus_basic))
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            seen_user.lock().unwrap().as_deref(),
            Some(user.userid()),
            "handler must resolve the API key's ephemeral identity, not the unverified BasicAuth header"
        );
    }

    #[actix_web::test]
    async fn intra_cluster_rejects_missing_or_wrong_credentials() {
        let _state = auth_state().await;
        let (user, _) = api_key_user(None);
        let valid = raw_cluster_headers(&user);
        let salt = SaltString::encode_b64(b"wrong-cluster-salt").unwrap();
        let wrong_hash = Argon2::default()
            .hash_password(b"wrong-cluster-secret", &salt)
            .unwrap()
            .to_string();

        let mut cases: Vec<_> = [
            CLUSTER_SECRET_HEADER,
            INTRA_CLUSTER_TENANT_HEADER,
            INTRA_CLUSTER_USERID_HEADER,
        ]
        .into_iter()
        .map(|name| with_header(valid.clone(), name, None))
        .collect();
        cases.push(with_header(
            valid.clone(),
            CLUSTER_SECRET_HEADER,
            Some(text(&wrong_hash)),
        ));

        for headers in cases {
            let outcome = call_app(counting_auth, true, "/", &headers).await;
            assert_eq!(outcome.status, StatusCode::UNAUTHORIZED);
            assert_eq!((outcome.handled, outcome.auth_calls), (0, 0));
            assert!(!has_sessions(&user));
        }

        let outcome = call_app(counting_auth, true, "/", &valid).await;
        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!(outcome.handled, 1);
        assert!(!has_sessions(&user));
    }

    #[actix_web::test]
    async fn api_key_session_lives_only_in_the_request() {
        let _state = auth_state().await;
        let (user, secret) = api_key_user(Some(Utc::now() + TimeDelta::hours(1)));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner = {
            let seen = Arc::clone(&seen);
            fn_service(move |req: ServiceRequest| {
                seen.lock().unwrap().push(seen_session(&req));
                async move { Ok::<_, Error>(req.into_response(HttpResponse::Ok().finish())) }
            })
        };
        let middleware = AuthMiddleware {
            action: Action::Query,
            auth_method: counting_auth,
            service: Rc::new(inner),
        };
        let request = || {
            actix_test::TestRequest::default()
                .insert_header(("x-api-key", secret.clone()))
                .to_srv_request()
        };
        let tracked = sessions().get_active_sessions().len();

        // `call` attaches the session to the request and tracks nothing,
        // so dropping the future unpolled (a client disconnect) leaves
        // nothing behind either.
        let future = middleware.call(request());
        assert_eq!(sessions().get_active_sessions().len(), tracked);
        assert!(!has_sessions(&user));
        drop(future);
        assert!(seen.lock().unwrap().is_empty());

        let response = middleware.call(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let seen = std::mem::take(&mut *seen.lock().unwrap());
        let [(Some(SessionKey::Ephemeral(session)), tracked_while_handled)] = seen.as_slice()
        else {
            panic!("the handler sees one request-local session: {seen:?}");
        };
        assert!(!tracked_while_handled);
        assert_eq!(session.userid, user.userid());
        assert_eq!(session.tenant, DEFAULT_TENANT);
        let UserType::ApiKey(api_key) = &user.ty else {
            unreachable!()
        };
        assert!(session.expires_at <= api_key.session_expires_at(Utc::now()));
        assert_eq!(sessions().get_active_sessions().len(), tracked);
        assert!(!has_sessions(&user));
    }

    // API-key and forwarded requests are authenticated, authorized and
    // handled while another thread holds SESSIONS for writing: neither path
    // takes the SESSIONS lock at all.
    #[test]
    fn api_key_and_forwarded_requests_never_take_the_sessions_lock() {
        let _state = blocking_auth_state();
        let (user, secret) = api_key_user(None);
        // Verify the cluster hash once up front so the timed part below does
        // not include a debug-build Argon2 run.
        let hash = CLUSTER_SECRET.get().unwrap().1.clone();
        verify_cluster_secret_hash(CLUSTER_TEST_SECRET, &hash).unwrap();

        let mut cases = api_key_headers(&secret)
            .map(|header| (false, vec![(header.0, text(&header.1))]))
            .to_vec();
        cases.push((true, raw_cluster_headers(&user)));

        let writer = mut_sessions();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let outcomes = actix_web::rt::System::new().block_on(async move {
                let mut outcomes = Vec::new();
                for (intra_cluster, headers) in cases {
                    let outcome = call_app(auth_no_context, intra_cluster, "/", &headers).await;
                    outcomes.push((outcome.status, outcome.handled));
                }
                outcomes
            });
            let _ = tx.send(outcomes);
        });
        let outcomes = rx.recv_timeout(std::time::Duration::from_secs(30));
        drop(writer);
        let outcomes = outcomes.expect("requests finished while SESSIONS was write-locked");
        assert_eq!(outcomes, vec![(StatusCode::OK, 1); 3]);
        assert!(!has_sessions(&user));
    }

    // Revocation, deletion and role changes reach the very next API-key
    // request, which builds its session from the user as stored then.
    #[actix_web::test]
    async fn api_key_grant_changes_apply_to_the_next_request() {
        let _state = auth_state().await;
        let (user, secret) = api_key_user(None);
        let headers = [("x-api-key", text(&secret))];
        let status = |outcome: Outcome| (outcome.status, outcome.handled);

        assert_eq!(
            status(call_app(auth_no_context, false, "/", &headers).await),
            (StatusCode::OK, 1)
        );
        Users.remove_roles(user.userid(), HashSet::from([ROLE.to_owned()]), &None);
        assert_eq!(
            status(call_app(auth_no_context, false, "/", &headers).await),
            (StatusCode::FORBIDDEN, 0)
        );
        Users.add_roles(user.userid(), HashSet::from([ROLE.to_owned()]), &None);
        assert_eq!(
            status(call_app(auth_no_context, false, "/", &headers).await),
            (StatusCode::OK, 1)
        );
        Users.delete_user(user.userid(), &None);
        assert_eq!(
            status(call_app(auth_no_context, false, "/", &headers).await),
            (StatusCode::UNAUTHORIZED, 0)
        );
        assert!(!has_sessions(&user));
    }

    /// Bytes `HeaderValue` accepts, so every case can reach the server:
    /// printable ASCII (possibly empty), or visible ASCII mixed with at least
    /// one obs-text byte (0x80..=0xff) that `to_str` rejects. At most 256 bytes.
    fn accepted_header_bytes() -> impl Strategy<Value = Vec<u8>> {
        let printable = vec(0x20u8..=0x7e, 0..=256);
        let obs_text = (
            vec(prop_oneof![0x20u8..=0x7e, 0x80u8..=0xff], 0..=255),
            0x80u8..=0xff,
            any::<prop::sample::Index>(),
        )
            .prop_map(|(mut bytes, obs, at)| {
                let at = at.index(bytes.len() + 1);
                bytes.insert(at, obs);
                bytes
            });
        prop_oneof![1 => printable, 1 => obs_text]
    }

    #[test]
    fn header_extractors_never_panic_on_accepted_bytes() {
        let _state = blocking_auth_state();
        let auth = basic_admin();
        let bad_request = |error: Error| error.as_response_error().status_code();
        let (text_cases, obs_text_cases) = (AtomicUsize::new(0), AtomicUsize::new(0));

        let mut runner = TestRunner::new(Config {
            cases: 256,
            failure_persistence: None,
            ..Config::default()
        });
        runner
            .run(&accepted_header_bytes(), |bytes| {
                prop_assert!(bytes.len() <= 256);
                let value = HeaderValue::from_bytes(&bytes)
                    .expect("generator only yields bytes `HeaderValue` accepts");
                let accepted = value.to_str().ok();
                let printable = bytes.iter().all(|byte| (0x20..=0x7e).contains(byte));
                prop_assert_eq!(accepted.is_some(), printable);
                let category = if printable {
                    &text_cases
                } else {
                    &obs_text_cases
                };
                category.fetch_add(1, Ordering::SeqCst);

                let mut headers = HeaderMap::new();
                headers.insert(HeaderName::from_static(TENANT_ID), value.clone());
                match (api_key_tenant_hint(&headers), accepted) {
                    (Ok(hint), Some(text)) => {
                        prop_assert_eq!(hint.as_deref(), (text != DEFAULT_TENANT).then_some(text))
                    }
                    (Err(error), None) => {
                        prop_assert_eq!(bad_request(error.into()), StatusCode::BAD_REQUEST)
                    }
                    (result, _) => prop_assert!(false, "unexpected tenant hint {result:?}"),
                }

                let mut req = actix_test::TestRequest::default()
                    .insert_header(auth.clone())
                    .insert_header((STREAM_NAME_HEADER_KEY, value.clone()))
                    .to_srv_request();
                match (auth_resource_context(&mut req, Action::Query), accepted) {
                    (Ok(rbac::Response::Authorized), Some(_)) => {}
                    (Err(error), None) => {
                        prop_assert_eq!(bad_request(error), StatusCode::BAD_REQUEST)
                    }
                    (result, _) => prop_assert!(false, "unexpected auth result {result:?}"),
                }

                let mut req = actix_test::TestRequest::default()
                    .insert_header((CLUSTER_SECRET_HEADER, value.clone()))
                    .insert_header((INTRA_CLUSTER_TENANT_HEADER, DEFAULT_TENANT))
                    .insert_header((INTRA_CLUSTER_USERID_HEADER, "header-proptest"))
                    .to_srv_request();
                let status = authenticate_intra_cluster_request(&mut req, CLUSTER_TEST_SECRET)
                    .err()
                    .map(bad_request);
                match accepted {
                    None => prop_assert_eq!(status, Some(StatusCode::BAD_REQUEST)),
                    Some(_) => prop_assert!(matches!(
                        status,
                        Some(StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED)
                    )),
                }
                Ok(())
            })
            .unwrap();
        // Both categories ran, so neither branch above went untested.
        assert!(text_cases.load(Ordering::SeqCst) > 0);
        assert!(obs_text_cases.load(Ordering::SeqCst) > 0);
    }
}

/// The ingest routes resolve the target tenant from the client-supplied
/// `x-p-tenant` header. Basic credentials are checked against that tenant, but
/// a session cookie is not: a user whose session belongs to tenant A must not
/// be able to ingest as tenant B just by claiming B's header.
#[cfg(test)]
mod ingest_tenant_binding_tests {
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;

    use actix_web::{
        App, HttpResponse, dev::ServiceResponse, http::StatusCode, test as actix_test, web,
    };
    use chrono::TimeDelta;
    use parking_lot::RwLock;
    use ulid::Ulid;

    use super::*;
    use crate::rbac::map::{
        ROLES, Roles, SESSIONS, Sessions, USER_GROUPS, USERS, UserGroups, mut_roles, mut_users,
    };
    use crate::rbac::role::ParseableResourceType;
    use crate::rbac::role::model::{DefaultPrivilege, Role};
    use crate::rbac::user::{Basic, User, UserType};

    const STREAM: &str = "shared-stream";

    /// Runs with the middleware's multi-tenant answer enabled, restoring it
    /// on scope exit (including on panic).
    struct MultiTenant;
    impl MultiTenant {
        fn enable() -> Self {
            TEST_MULTI_TENANT.store(true, Ordering::SeqCst);
            MultiTenant
        }
    }
    impl Drop for MultiTenant {
        fn drop(&mut self) {
            TEST_MULTI_TENANT.store(false, Ordering::SeqCst);
        }
    }

    fn unique_tenant(prefix: &str) -> String {
        format!("{prefix}-{}", Ulid::new()).to_lowercase()
    }

    async fn auth_state() -> tokio::sync::MutexGuard<'static, ()> {
        let serial = crate::rbac::map::TEST_SERIAL.lock().await;
        USERS.get_or_init(|| RwLock::new(crate::rbac::map::Users::default()));
        SESSIONS.get_or_init(|| RwLock::new(Sessions::default()));
        USER_GROUPS.get_or_init(|| RwLock::new(UserGroups::default()));
        ROLES.get_or_init(|| RwLock::new(Roles::default()));
        serial
    }

    /// A role in `tenant` that only grants `Ingest` on `STREAM`.
    fn scoped_ingest_role(tenant: &str) -> String {
        let role_name = format!("scoped-ingester-{}", Ulid::new()).to_lowercase();
        mut_roles().entry(tenant.to_owned()).or_default().insert(
            role_name.clone(),
            Role::create_user_role(vec![DefaultPrivilege::Ingestor {
                resource: Some(ParseableResourceType::Stream(STREAM.to_owned())),
            }]),
        );
        role_name
    }

    /// An API key in `tenant` holding only `scoped_ingest_role`; its secret.
    fn scoped_api_key(tenant: &str) -> String {
        let secret = format!("scoped-api-key-{}", Ulid::new());
        let user = User::new_api_key(
            Ulid::new(),
            secret.clone(),
            "tenant-binding".into(),
            HashSet::from([scoped_ingest_role(tenant)]),
            "admin".into(),
            Some(tenant.to_owned()),
        );
        mut_users().insert(user);
        secret
    }

    /// A native user in `tenant` whose only role grants `Ingest` on `STREAM`,
    /// plus a tracked cookie session for them.
    fn scoped_session_user(tenant: &str) -> SessionKey {
        let role_name = scoped_ingest_role(tenant);
        let user = User {
            ty: UserType::Native(Basic {
                username: format!("scoped-{}", Ulid::new()).to_lowercase(),
                password_hash: "unused-in-this-test".to_owned(),
                email: None,
            }),
            roles: HashSet::from([role_name]),
            user_groups: HashSet::new(),
            tenant: Some(tenant.to_owned()),
            protected: false,
        };
        mut_users().insert(user.clone());
        let key = SessionKey::SessionId(Ulid::new());
        crate::rbac::Users.new_session(&user, key.clone(), TimeDelta::minutes(5));
        key
    }

    /// A POST route wrapped exactly like the ingest routes: `Auth` with
    /// `Action::Ingest` and the resource-context decision. The stub handler
    /// records the tenant header it was reached with.
    async fn call_ingest_route(
        session: &SessionKey,
        claimed_tenant: &str,
    ) -> (StatusCode, Option<String>) {
        let SessionKey::SessionId(id) = session else {
            unreachable!("the fixture tracks a cookie session");
        };
        call_ingest_route_with(("cookie", format!("session={id}")), claimed_tenant).await
    }

    async fn call_ingest_route_with(
        credential: (&'static str, String),
        claimed_tenant: &str,
    ) -> (StatusCode, Option<String>) {
        let seen_tenant = std::sync::Arc::new(std::sync::Mutex::new(None));
        let handler_tenant = std::sync::Arc::clone(&seen_tenant);
        let route = web::post()
            .to(move |req: actix_web::HttpRequest| {
                let tenant = req
                    .headers()
                    .get(TENANT_ID)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                *handler_tenant.lock().unwrap() = tenant;
                async { HttpResponse::Ok().finish() }
            })
            .wrap(Auth {
                action: Action::Ingest,
                method: auth_resource_context,
            });
        let app = actix_test::init_service(App::new().route("/", route)).await;

        let request = actix_test::TestRequest::post()
            .insert_header(credential)
            .insert_header((TENANT_ID, claimed_tenant))
            .insert_header((STREAM_NAME_HEADER_KEY, STREAM))
            .to_request();
        let response: ServiceResponse = match actix_test::try_call_service(&app, request).await {
            Ok(response) => response,
            Err(error) => {
                let status = error.as_response_error().status_code();
                return (status, None);
            }
        };
        let status = response.status();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("buffered body");
        assert!(body.is_empty(), "the stub returns an empty body");
        (status, seen_tenant.lock().unwrap().clone())
    }

    #[actix_web::test]
    async fn session_ingest_is_bound_to_the_sessions_tenant() {
        // `check_auth` resolves the context stream through `PARSEABLE`.
        crate::parseable::install_default_test_parseable();
        let _state = auth_state().await;
        let _multi = MultiTenant::enable();

        let tenant_a = unique_tenant("tenant-a");
        let tenant_b = unique_tenant("tenant-b");
        let session = scoped_session_user(&tenant_a);

        // The session belongs to tenant A; claiming tenant B's header must be
        // rejected before the handler, like a Basic credential of another
        // tenant already is.
        let (status, seen) = call_ingest_route(&session, &tenant_b).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a session of tenant {tenant_a} must not ingest as {tenant_b}"
        );
        assert!(
            seen.is_none(),
            "the request must be rejected before the handler sees the tenant header"
        );

        // Control: claiming the session's own tenant still ingests.
        let (status, seen) = call_ingest_route(&session, &tenant_a).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(seen.as_deref(), Some(tenant_a.as_str()));
    }

    // An API key's request-local session is bound to the key's tenant the
    // same way: claiming another tenant never reaches the handler.
    #[actix_web::test]
    async fn api_key_ingest_is_bound_to_the_keys_tenant() {
        crate::parseable::install_default_test_parseable();
        let _state = auth_state().await;
        let _multi = MultiTenant::enable();

        let tenant_a = unique_tenant("tenant-a");
        let tenant_b = unique_tenant("tenant-b");
        let secret = scoped_api_key(&tenant_a);
        let tracked = SESSIONS.get().unwrap().read().get_active_sessions().len();

        let (status, seen) = call_ingest_route_with(("x-api-key", secret.clone()), &tenant_b).await;
        assert!(
            status.is_client_error(),
            "an API key of tenant {tenant_a} must not ingest as {tenant_b}: {status}"
        );
        assert!(seen.is_none());

        let (status, seen) = call_ingest_route_with(("x-api-key", secret), &tenant_a).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(seen.as_deref(), Some(tenant_a.as_str()));
        assert_eq!(
            SESSIONS.get().unwrap().read().get_active_sessions().len(),
            tracked
        );
    }
}
