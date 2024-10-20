use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use crate::handlers::http::query::{Query, QueryError};
use actix_web::{web, FromRequest, HttpRequest};
use actix_web::web::Json;

/// Query Request through http endpoint.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct DynamicQuery {
    pub query: String,
    pub cache_duration: String,
}

impl FromRequest for DynamicQuery {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let query = Json::<DynamicQuery>::from_request(req, payload);
        let params = web::Query::<HashMap<String, bool>>::from_request(req, payload)
            .into_inner()
            .map(|x| x.0)
            .unwrap_or_default();

        let fut = async move {
            let mut query = query.await?.into_inner();

            Ok(query)
        };

        Box::pin(fut)
    }
}


pub async fn dynamic_query(req: HttpRequest, query_request: DynamicQuery) -> Result<String, QueryError> {
    Ok(query_request.query)
}
