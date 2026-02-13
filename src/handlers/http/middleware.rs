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

use std::future::{Ready, ready};

use actix_web::{
    Error, HttpMessage, HttpRequest, Route,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
    error::{ErrorBadRequest, ErrorForbidden, ErrorUnauthorized},
    http::header::{self, HeaderName, HeaderValue},
};
use chrono::{Duration, Utc};
use futures_util::future::LocalBoxFuture;

use crate::{
    handlers::{
        AUTHORIZATION_KEY, KINESIS_COMMON_ATTRIBUTES_KEY, LOG_SOURCE_KEY, LOG_SOURCE_KINESIS,
        STREAM_NAME_HEADER_KEY,
        http::{ingest::PostError, modal::OIDC_CLIENT, rbac::RBACError},
    },
    option::Mode,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    rbac::{
        EXPIRY_DURATION,
        map::{SessionKey, mut_sessions, mut_users, sessions, users},
        roles_to_permission, user,
    },
    tenants::TENANT_METADATA,
    utils::{get_user_and_tenant_from_request, get_user_from_request},
};
use crate::{
    rbac::Users,
    rbac::{self, role::Action},
    utils::actix::extract_session_key,
};

use serde::{Deserialize, Serialize};

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
    fn authorize(self, action: Action) -> Self;
    fn authorize_for_resource(self, action: Action) -> Self;
    fn authorize_for_user(self, action: Action) -> Self;
}

impl RouteExt for Route {
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
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
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
            service,
            auth_method: self.method,
        }))
    }
}

pub struct AuthMiddleware<S> {
    action: Action,
    auth_method: fn(&mut ServiceRequest, Action) -> Result<rbac::Response, Error>,
    service: S,
}

impl<S, B> Service<ServiceRequest> for AuthMiddleware<S>
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
        /*Below section is added to extract the Authorization and X-P-Stream headers from x-amz-firehose-common-attributes custom header
        when request is made from Kinesis Firehose.
        For requests made from other clients, no change.

        ## Section start */
        if let Some(kinesis_common_attributes) =
            req.request().headers().get(KINESIS_COMMON_ATTRIBUTES_KEY)
        {
            let attribute_value: &str = kinesis_common_attributes.to_str().unwrap();
            let message: Message = serde_json::from_str(attribute_value).unwrap();
            req.headers_mut().insert(
                HeaderName::from_static(AUTHORIZATION_KEY),
                header::HeaderValue::from_str(&message.common_attributes.authorization).unwrap(),
            );
            req.headers_mut().insert(
                HeaderName::from_static(STREAM_NAME_HEADER_KEY),
                header::HeaderValue::from_str(&message.common_attributes.x_p_stream).unwrap(),
            );
            req.headers_mut().insert(
                HeaderName::from_static(LOG_SOURCE_KEY),
                header::HeaderValue::from_static(LOG_SOURCE_KINESIS),
            );
        }
        /* ## Section end */
        // if action is Ingest and multi-tenancy is on, then request MUST have tenant id
        // else check for the presence of tenant id using other details

        // an optional error to track the presence of CORRECT tenant header in case of ingestion
        let mut header_error = None;
        let user_and_tenant_id: Result<(Result<String, RBACError>, Option<String>), RBACError> =
            if PARSEABLE.options.is_multi_tenant() {
                // if ingestion then tenant MUST be present and should not be DEFAULT_TENANT
                let tenant = if self.action.eq(&Action::Ingest) {
                    if let Some(tenant) = req.headers().get("tenant")
                        && let Ok(tenant) = tenant.to_str()
                    {
                        if tenant.eq(DEFAULT_TENANT) {
                            header_error = Some(actix_web::Error::from(PostError::Header(
                                crate::utils::header_parsing::ParseHeaderError::InvalidTenantId,
                            )));
                        }
                        Some(tenant.to_owned())
                    } else {
                        // tenant header is not present, error out
                        header_error = Some(actix_web::Error::from(PostError::Header(
                            crate::utils::header_parsing::ParseHeaderError::MissingTenantId,
                        )));
                        None
                    }
                } else if self.action.eq(&Action::SuperAdmin) || self.action.eq(&Action::Login) {
                    None
                } else {
                    // tenant header should not be present, modify request to add
                    let mut t = None;
                    if let Ok((_, tenant)) = get_user_and_tenant_from_request(req.request())
                        && let Some(tid) = tenant.as_ref()
                    {
                        req.headers_mut().insert(
                            HeaderName::from_static("tenant"),
                            HeaderValue::from_str(tid).unwrap(),
                        );
                        t = tenant;
                    } else {
                        // remove the header if already present
                        req.headers_mut().remove("tenant");
                    }
                    t
                };
                let userid = get_user_from_request(req.request());
                Ok((userid, tenant))
            } else {
                // not multi-tenant, tenant header should NOT be present
                if req.headers().get("tenant").is_some() {
                    header_error = Some(actix_web::Error::from(PostError::Header(
                        crate::utils::header_parsing::ParseHeaderError::UnexpectedHeader(
                            "tenant".into(),
                        ),
                    )));
                }
                let userid = get_user_from_request(req.request());
                Ok((userid, None))
            };

        let key: Result<SessionKey, Error> = extract_session_key(&mut req);

        // if action is ingestion, check if tenant is correct for basic auth user
        if self.action.eq(&Action::Ingest)
            && let Ok(key) = &key
            && let SessionKey::BasicAuth { username, password } = &key
            && let Ok((_, tenant)) = &user_and_tenant_id
            && let Some(tenant) = tenant.as_ref()
            && !Users.validate_basic_user_tenant_id(username, password, tenant)
        {
            header_error = Some(actix_web::Error::from(PostError::Header(
                crate::utils::header_parsing::ParseHeaderError::InvalidTenantId,
            )));
        }

        let auth_result: Result<_, Error> = (self.auth_method)(&mut req, self.action);

        let fut = self.service.call(req);
        Box::pin(async move {
            let Ok(key) = key else {
                return Err(ErrorUnauthorized(
                    "Your session has expired or is no longer valid. Please re-authenticate to access this resource.",
                ));
            };

            if let Some(err) = header_error {
                return Err(err);
            }

            // if session is expired, refresh token
            if sessions().is_session_expired(&key) {
                refresh_token(user_and_tenant_id, &key).await?;
            }

            match auth_result? {
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

            fut.await
        })
    }
}

#[inline]
pub async fn refresh_token(
    user_and_tenant_id: Result<(Result<String, RBACError>, Option<String>), RBACError>,
    key: &SessionKey,
) -> Result<(), Error> {
    let oidc_client = OIDC_CLIENT.get();

    if let Some(client) = oidc_client
        && let Ok((userid, tenant_id)) = user_and_tenant_id
        && let Ok(userid) = userid
    {
        let bearer_to_refresh = {
            if let Some(users) = users().get(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
                && let Some(user) = users.get(&userid)
            {
                match &user.ty {
                    user::UserType::OAuth(oauth) if oauth.bearer.is_some() => Some(oauth.clone()),
                    _ => None,
                }
            } else {
                None
            }
        };

        if let Some(oauth_data) = bearer_to_refresh {
            let refreshed_token = match client
                .read()
                .await
                .client()
                .refresh_token(&oauth_data, Some(PARSEABLE.options.scope.as_str()))
                .await
            {
                Ok(bearer) => bearer,
                Err(e) => {
                    tracing::error!("client refresh_token call failed- {e}");
                    // remove user session
                    Users.remove_session(key);
                    return Err(ErrorUnauthorized(
                        "Your session has expired or is no longer valid. Please re-authenticate to access this resource.",
                    ));
                }
            };

            let expires_in = if let Some(expires_in) = refreshed_token.expires_in.as_ref() {
                if *expires_in > u32::MAX.into() {
                    EXPIRY_DURATION
                } else {
                    let v = i64::from(*expires_in as u32);
                    Duration::seconds(v)
                }
            } else {
                EXPIRY_DURATION
            };

            let user_roles = {
                let mut users_guard = mut_users();
                if let Some(users) =
                    users_guard.get_mut(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
                    && let Some(user) = users.get_mut(&userid)
                {
                    if let user::UserType::OAuth(oauth) = &mut user.ty {
                        oauth.bearer = Some(refreshed_token);
                    }
                    user.roles().to_vec()
                } else {
                    return Err(ErrorUnauthorized(
                        "Your session has expired or is no longer valid. Please re-authenticate to access this resource.",
                    ));
                }
            };

            mut_sessions().track_new(
                userid.clone(),
                key.clone(),
                Utc::now() + expires_in,
                roles_to_permission(user_roles, tenant_id.as_deref().unwrap_or(DEFAULT_TENANT)),
                &tenant_id,
            );
        } else if let Some(users) = users().get(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
            && let Some(user) = users.get(&userid)
        {
            mut_sessions().track_new(
                userid.clone(),
                key.clone(),
                Utc::now() + EXPIRY_DURATION,
                roles_to_permission(user.roles(), tenant_id.as_deref().unwrap_or(DEFAULT_TENANT)),
                &tenant_id,
            );
        }
    }
    Ok(())
}

#[inline(always)]
pub fn check_suspension(req: &HttpRequest, action: Action) -> rbac::Response {
    if let Some(tenant) = req.headers().get("tenant")
        && let Ok(tenant) = tenant.to_str()
    {
        if let Ok(Some(suspension)) = TENANT_METADATA.is_action_suspended(tenant, &action) {
            return rbac::Response::Suspended(suspension);
        } else {
            // tenant does not exist
        }
    }
    rbac::Response::Authorized
}

pub fn auth_no_context(req: &mut ServiceRequest, action: Action) -> Result<rbac::Response, Error> {
    // check if tenant is suspended
    if let rbac::Response::Suspended(msg) = check_suspension(req.request(), action) {
        return Ok(rbac::Response::Suspended(msg));
    }
    let creds = extract_session_key(req);
    creds.map(|key| Users.authorize(key, action, None, None))
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
    let mut stream = req.match_info().get("logstream");
    if let Some(usergroup) = usergroup {
        creds.map(|key| Users.authorize(key, action, Some(usergroup), None))
    } else if let Some(llmid) = llmid {
        creds.map(|key| Users.authorize(key, action, Some(llmid), None))
    } else if let Some(stream) = stream {
        creds.map(|key| Users.authorize(key, action, Some(stream), None))
    } else {
        if let Some(stream_name) = req.headers().get(STREAM_NAME_HEADER_KEY) {
            stream = Some(stream_name.to_str().unwrap());
        }
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
    let user = req.match_info().get("username");
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
        let username = req.match_info().get("username").unwrap_or("");
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
