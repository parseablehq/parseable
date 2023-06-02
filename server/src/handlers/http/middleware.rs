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

use std::{
    collections::{HashMap, HashSet},
    future::{ready, Ready},
};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    error::{ErrorBadRequest, ErrorUnauthorized},
    Error, HttpMessage,
};
use actix_web_httpauth::extractors::basic::BasicAuth;
use futures_util::future::LocalBoxFuture;

use crate::{
    option::CONFIG,
    rbac::role::Action,
    rbac::{role::Permission, Users},
};

pub struct Auth {
    pub action: Action,
    pub stream: bool,
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
            match_stream: self.stream,
            service,
        }))
    }
}

pub struct AuthMiddleware<S> {
    action: Action,
    match_stream: bool,
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
        // Extract username and password from the request using basic auth extractor.
        let creds = req.extract::<BasicAuth>().into_inner();
        let creds = creds.map_err(Into::into).map(|creds| {
            let username = creds.user_id().trim().to_owned();
            // password is not mandatory by basic auth standard.
            // If not provided then treat as empty string
            let password = creds.password().unwrap_or("").trim().to_owned();
            (username, password)
        });

        let stream = if self.match_stream {
            req.match_info().get("logstream")
        } else {
            None
        };

        let auth_result: Result<bool, Error> = creds.map(|(username, password)| {
            Users.authenticate(username, password, self.action, stream)
        });

        let fut = self.service.call(req);

        Box::pin(async move {
            if !auth_result? {
                return Err(ErrorUnauthorized("Not authorized"));
            }

            fut.await
        })
    }
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

// extract username from auth information and attach to request.
// For use in query handler
pub struct ExtractQueryPermission;

impl<S, B> Transform<S, ServiceRequest> for ExtractQueryPermission
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = ExtractQueryPermissionMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ExtractQueryPermissionMiddleware { service }))
    }
}

pub struct ExtractQueryPermissionMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for ExtractQueryPermissionMiddleware<S>
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
        if let Ok(creds) = req.extract::<BasicAuth>().into_inner() {
            // Extract username and password from the request using basic auth extractor.
            let username = creds.user_id().trim().to_owned();
            let password = creds.password().unwrap_or("").trim().to_owned();

            // maintain a map for all tags allowed for a stream.
            let mut map = StreamTag::default();

            for permission in Users.get_permissions(username, password) {
                match permission {
                    Permission::Stream(Action::All, stream) => map.update(stream, None),
                    Permission::StreamWithTag(Action::Query, stream, tag) => {
                        map.update(stream, tag)
                    }
                    _ => (),
                }
            }

            req.extensions_mut().insert(map.finalize());
        }

        let fut = self.service.call(req);

        Box::pin(async move { fut.await })
    }
}

// A map is maintained where key is stream and value is tag.
// for each stream and tag in permission list we do following
// if any permission for a stream contains no tag then that stream tag is set to none
// if permission contains tag then it is added to that stream's entry
#[derive(Debug, Default)]
struct StreamTag {
    inner: HashMap<String, Option<HashSet<String>>>,
}

impl StreamTag {
    fn update(&mut self, stream: String, tag: Option<String>) {
        let entry = self.inner.entry(stream).or_insert(Some(HashSet::default()));
        if let Some(tags) = entry {
            if let Some(tag) = tag {
                tags.insert(tag);
            } else {
                *entry = None;
            }
        }
    }

    fn finalize(self) -> HashMap<String, Option<HashSet<String>>> {
        self.inner
    }
}
