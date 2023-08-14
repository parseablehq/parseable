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
    error::{ErrorBadRequest, ErrorUnauthorized},
    Error, Route,
};
use actix_web_httpauth::extractors::basic::BasicAuth;
use futures_util::future::LocalBoxFuture;

use crate::{option::CONFIG, rbac::role::Action, rbac::Users};

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
    pub method: fn(&mut ServiceRequest, Action) -> Result<bool, Error>,
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
    auth_method: fn(&mut ServiceRequest, Action) -> Result<bool, Error>,
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
        let auth_result: Result<bool, Error> = (self.auth_method)(&mut req, self.action);
        let fut = self.service.call(req);
        Box::pin(async move {
            if !auth_result? {
                return Err(ErrorUnauthorized("Not authorized"));
            }
            fut.await
        })
    }
}

pub fn auth_no_context(req: &mut ServiceRequest, action: Action) -> Result<bool, Error> {
    let creds = extract_basic_auth(req);
    creds.map(|(username, password)| Users.authenticate(username, password, action, None, None))
}

pub fn auth_stream_context(req: &mut ServiceRequest, action: Action) -> Result<bool, Error> {
    let creds = extract_basic_auth(req);
    let stream = req.match_info().get("logstream");
    creds.map(|(username, password)| Users.authenticate(username, password, action, stream, None))
}

pub fn auth_user_context(req: &mut ServiceRequest, action: Action) -> Result<bool, Error> {
    let creds = extract_basic_auth(req);
    let user = req.match_info().get("username");
    creds.map(|(username, password)| Users.authenticate(username, password, action, None, user))
}

fn extract_basic_auth(req: &mut ServiceRequest) -> Result<(String, String), Error> {
    // Extract username and password from the request using basic auth extractor.
    let creds = req.extract::<BasicAuth>().into_inner();
    creds.map_err(Into::into).map(|creds| {
        let username = creds.user_id().trim().to_owned();
        // password is not mandatory by basic auth standard.
        // If not provided then treat as empty string
        let password = creds.password().unwrap_or("").trim().to_owned();
        (username, password)
    })
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
