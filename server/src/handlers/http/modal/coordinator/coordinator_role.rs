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
 */

use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder, Result};
use bytes::Bytes;
use http::{header, StatusCode};
use itertools::Itertools;
use reqwest::{self, Client, RequestBuilder, Response};
use serde_json::{json, Value};
use crate::{handlers::http::{base_path, modal::coordinator_server::QUERY_COORDINATION, rbac::RBACError, role::RoleError}, metrics::{build_metrics_handler, error::MetricsError}};

use super::{CoordinatorRequest, Method};

pub async fn list() -> Result<impl Responder, RoleError> {
    let request = CoordinatorRequest{
        body: None,
        api: "role",
        resource: None,
        method: Method::GET
    };

    let res = request.request().await?;

    Ok(web::Json(res.json::<Value>().await.unwrap()))
}

pub async fn put_default(body: Option<Bytes>) -> Result<impl Responder, RoleError> {
    let request = CoordinatorRequest{
        body: body,
        api: "role",
        resource: Some("default"),
        method: Method::PUT
    };

    request.request().await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn get_default() -> Result<impl Responder, RoleError> {
    let request = CoordinatorRequest{
        body: None,
        api: "role",
        resource: Some("default"),
        method: Method::GET
    };

    let res = request.request().await?;

    Ok(web::Json(res.json::<Value>().await.unwrap()))
}

pub async fn put(name: web::Path<String>, body: Bytes) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let request = CoordinatorRequest{
        body: Some(body),
        api: "role",
        resource: Some(&name),
        method: Method::PUT
    };

    request.request().await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn delete(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let request = CoordinatorRequest{
        body: None,
        api: "role",
        resource: Some(&name),
        method: Method::DELETE
    };

    request.request().await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn get(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let request = CoordinatorRequest{
        body: None,
        api: "role",
        resource: Some(&name),
        method: Method::GET
    };

    let res = request.request().await?;

    Ok(web::Json(res.json::<Value>().await.unwrap()))
}