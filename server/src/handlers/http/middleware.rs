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
*
*/

use std::future::{ready, Ready};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    error::{ErrorBadRequest, ErrorForbidden, ErrorUnauthorized},
    http::header::{self, HeaderName},
    Error, Route,
};
use futures_util::future::LocalBoxFuture;

use crate::handlers::{AUTHORIZATION_KEY, KINESIS_COMMON_ATTRIBUTES_KEY, STREAM_NAME_HEADER_KEY};
use crate::{
    option::CONFIG,
    rbac::Users,
    rbac::{self, role::Action},
    utils::actix::extract_session_key,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    #[serde(rename = "commonAttributes")]
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
                header::HeaderValue::from_str(&message.common_attributes.authorization.clone())
                    .unwrap(),
            );
            req.headers_mut().insert(
                HeaderName::from_static(STREAM_NAME_HEADER_KEY),
                header::HeaderValue::from_str(&message.common_attributes.x_p_stream.clone())
                    .unwrap(),
            );
        }

        /* ## Section end */

        let auth_result: Result<_, Error> = (self.auth_method)(&mut req, self.action);
        let fut = self.service.call(req);
        Box::pin(async move {
            match auth_result? {
                rbac::Response::UnAuthorized => return Err(
                    ErrorForbidden("You don't have permission to access this resource. Please contact your administrator for assistance.")
                ),
                rbac::Response::ReloadRequired => return Err(
                    ErrorUnauthorized("Your session has expired or is no longer valid. Please re-authenticate to access this resource.")
                ),
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

pub fn auth_stream_context(
    req: &mut ServiceRequest,
    action: Action,
) -> Result<rbac::Response, Error> {
    let creds = extract_session_key(req);
    let stream = req.match_info().get("logstream");
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
