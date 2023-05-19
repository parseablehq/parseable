use std::future::{ready, Ready};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    error::ErrorUnauthorized,
    Error, HttpMessage,
};
use futures_util::future::LocalBoxFuture;

use crate::rbac::{role::Action, Users};

pub struct Authorization {
    pub action: Action,
    pub stream: bool,
}

impl<S, B> Transform<S, ServiceRequest> for Authorization
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = AuthorizationMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(AuthorizationMiddleware {
            action: self.action,
            match_stream: self.stream,
            service,
        }))
    }
}

pub struct AuthorizationMiddleware<S> {
    action: Action,
    match_stream: bool,
    service: S,
}

impl<S, B> Service<ServiceRequest> for AuthorizationMiddleware<S>
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
        let stream = if self.match_stream {
            req.match_info().get("logstream")
        } else {
            None
        };
        let extensions = req.extensions();
        let username = extensions
            .get::<String>()
            .expect("authentication layer verified username");
        let is_auth = Users.check_permission(username, self.action, stream);
        drop(extensions);

        let fut = self.service.call(req);

        Box::pin(async move {
            if !is_auth {
                return Err(ErrorUnauthorized("Not authorized"));
            }
            fut.await
        })
    }
}
