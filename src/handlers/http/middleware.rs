/*
* Parseable Server (C) 2022 - 2024 Parseable, Inc.
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
    Error, HttpMessage, Route,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
    error::{ErrorBadRequest, ErrorForbidden, ErrorUnauthorized},
    http::header::{self, HeaderName},
    web::Data,
};
use chrono::{Duration, Utc};
use futures_util::future::LocalBoxFuture;

use crate::{
    handlers::{
        AUTHORIZATION_KEY, KINESIS_COMMON_ATTRIBUTES_KEY, LOG_SOURCE_KEY, LOG_SOURCE_KINESIS,
        STREAM_NAME_HEADER_KEY, http::rbac::RBACError,
    },
    oidc::DiscoveredClient,
    option::Mode,
    parseable::PARSEABLE,
    rbac::{
        EXPIRY_DURATION,
        map::{SessionKey, mut_sessions, mut_users, sessions, users},
        roles_to_permission, user,
    },
    utils::get_user_from_request,
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

        let auth_result: Result<_, Error> = (self.auth_method)(&mut req, self.action);

        let http_req = req.request().clone();
        let key: Result<SessionKey, Error> = extract_session_key(&mut req);
        let userid: Result<String, RBACError> = get_user_from_request(&http_req);

        let fut = self.service.call(req);
        Box::pin(async move {
            let Ok(key) = key else {
                return Err(ErrorUnauthorized(
                    "Your session has expired or is no longer valid. Please re-authenticate to access this resource.",
                ));
            };

            // if session is expired, refresh token
            if sessions().is_session_expired(&key) {
                // request using oidc client
                let oidc_client = match http_req.app_data::<Data<Option<DiscoveredClient>>>() {
                    Some(client) => {
                        let c = client.clone().into_inner();
                        c.as_ref().clone()
                    }
                    None => None,
                };

                if let Some(client) = oidc_client
                    && let Ok(userid) = userid
                    && users().get(&userid).is_some()
                {
                    // get the bearer token
                    let user = users().get(&userid).unwrap().clone();
                    match &user.ty {
                        user::UserType::OAuth(oauth) => {
                            if oauth.bearer.as_ref().is_some() {
                                let Ok(refreshed_token) = client
                                    .refresh_token(oauth, Some(PARSEABLE.options.scope.as_str()))
                                    .await
                                else {
                                    return Err(ErrorUnauthorized(
                                        "Your session has expired or is no longer valid. Please re-authenticate to access this resource.",
                                    ));
                                };
                                let expires_in =
                                    if let Some(expires_in) = refreshed_token.expires_in.as_ref() {
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

                                // set the new oauth bearer value
                                if let Some(user) = mut_users().get_mut(&userid)
                                    && let user::UserType::OAuth(oauth) = &mut user.ty
                                {
                                    oauth.bearer = Some(refreshed_token)
                                }

                                mut_sessions().track_new(
                                    userid.clone(),
                                    key.clone(),
                                    Utc::now() + expires_in,
                                    roles_to_permission(user.roles()),
                                );
                            }
                        }
                        _ => {
                            mut_sessions().track_new(
                                userid.clone(),
                                key.clone(),
                                Utc::now() + EXPIRY_DURATION,
                                roles_to_permission(user.roles()),
                            );
                        }
                    }
                }
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
                _ => {}
            }

            fut.await
        })
    }
}

pub fn auth_no_context(req: &mut ServiceRequest, action: Action) -> Result<rbac::Response, Error> {
    let creds = extract_session_key(req);
    creds.map(|key| Users.authorize(key, action, None, None))
}

pub fn auth_resource_context(
    req: &mut ServiceRequest,
    action: Action,
) -> Result<rbac::Response, Error> {
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
