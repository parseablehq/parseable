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

use std::future::{ready, Ready};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    error::{ErrorBadRequest, ErrorForbidden, ErrorUnauthorized},
    http::header::{self, HeaderName},
    Error, HttpMessage, Route,
};
use futures_util::future::LocalBoxFuture;

use crate::{
    audit::AuditLogBuilder,
    handlers::{
        AUTHORIZATION_KEY, KINESIS_COMMON_ATTRIBUTES_KEY, LOG_SOURCE_KEY, LOG_SOURCE_KINESIS,
        STREAM_NAME_HEADER_KEY,
    },
    option::Mode,
};
use crate::{
    option::CONFIG,
    rbac::Users,
    rbac::{self, role::Action},
    utils::actix::extract_session_key,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Message {
    common_attributes: CommonAttributes,
}

#[derive(Serialize, Deserialize, Debug)]
struct CommonAttributes {
    #[serde(rename = "Authorization")]
    authorization: String,
    #[serde(rename = "X-P-Stream")]
    x_p_stream: String,
}

pub trait RouteExt {
    fn authorize(self, action: Action) -> Self;
    fn authorize_for_stream(self, action: Action) -> Self;
    fn authorize_for_user(self, action: Action) -> Self;
}

impl RouteExt for Route {
    fn authorize(self, action: Action) -> Self {
        self.wrap(Auth {
            action,
            method: auth_no_context,
        })
    }

    fn authorize_for_stream(self, action: Action) -> Self {
        self.wrap(Auth {
            action,
            method: auth_stream_context,
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
        let mut stream_name = None;
        if let Some((_, kinesis_common_attributes)) = req
            .request()
            .headers()
            .iter()
            .find(|&(key, _)| key == KINESIS_COMMON_ATTRIBUTES_KEY)
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
            stream_name = Some(message.common_attributes.x_p_stream);
        }

        if let Some(stream) = req.match_info().get("logstream") {
            stream_name = Some(stream.to_owned());
        } else if let Some(value) = req.headers().get(STREAM_NAME_HEADER_KEY) {
            if let Ok(stream) = value.to_str() {
                stream_name = Some(stream.to_owned())
            }
        }

        /* ## Section end */

        let auth_result: Result<_, Error> = (self.auth_method)(&mut req, self.action);

        // Ensures that log will be pushed to subscriber on drop
        let mut log_builder = AuditLogBuilder::default();
        log_builder.set_stream_name(stream_name.unwrap_or_default());
        log_builder.update_from_http(&mut req);
        let fut = self.service.call(req);
        Box::pin(async move {
            log_builder.set_deployment_id().await;

            match auth_result? {
                rbac::Response::UnAuthorized => return Err(
                    ErrorForbidden("You don't have permission to access this resource. Please contact your administrator for assistance.")
                ),
                rbac::Response::ReloadRequired => return Err(
                    ErrorUnauthorized("Your session has expired or is no longer valid. Please re-authenticate to access this resource.")
                ),
                _ => {}
            }
            let res = fut.await;

            // Capture status_code and error information from response
            match &res {
                Ok(res) => {
                    let status = res.status();
                    log_builder.response.status_code = status.as_u16();
                    // Use error information from reponse object if an error
                    if let Some(err) = res.response().error() {
                        log_builder.set_response_error(err.to_string());
                    }
                }
                Err(err) => log_builder.set_response_error(err.to_string()),
            }

            res
        })
    }
}

pub fn auth_no_context(req: &mut ServiceRequest, action: Action) -> Result<rbac::Response, Error> {
    let creds = extract_session_key(req);
    creds.map(|key| Users.authorize(key, action, None, None))
}

pub fn auth_stream_context(
    req: &mut ServiceRequest,
    action: Action,
) -> Result<rbac::Response, Error> {
    let creds = extract_session_key(req);
    let mut stream = req.match_info().get("logstream");
    if stream.is_none() {
        if let Some((_, stream_name)) = req
            .headers()
            .iter()
            .find(|&(key, _)| key == STREAM_NAME_HEADER_KEY)
        {
            stream = Some(stream_name.to_str().unwrap());
        }
    }
    creds.map(|key| Users.authorize(key, action, stream, None))
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
        let is_root = username == CONFIG.parseable.username;
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
        let mode = &CONFIG.parseable.mode;
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
        }
    }
}
