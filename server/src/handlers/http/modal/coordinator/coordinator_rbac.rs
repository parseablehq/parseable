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
use crate::{handlers::http::{base_path, modal::coordinator_server::QUERY_COORDINATION, rbac::RBACError}, metrics::{build_metrics_handler, error::MetricsError}};

use super::{CoordinatorRequest, Method};

pub async fn list_users(req: HttpRequest) -> Result<impl Responder, RBACError> {
    let request = CoordinatorRequest{
        body: None,
        api: "user",
        resource: None,
        method: Method::GET
    };

    let res = request.request().await?;

    Ok(web::Json(res.json::<Value>().await.unwrap()))
}

pub async fn post_user(
    username: web::Path<String>,
    body: Option<Bytes>,
) -> Result<impl Responder, RBACError> {

    let username = username.into_inner();

    let request = CoordinatorRequest{
        body: body,
        api: "user",
        resource: Some(&username),
        method: Method::POST
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(res.text().await.unwrap())
        },
        _ => {
            let err_msg = res.text().await?;
            Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn put_role(
    username: web::Path<String>,
    body: Option<Bytes>,
) -> Result<String, RBACError> {
    let username = username.into_inner();
    let resource = format!("{username}/role");
    let request = CoordinatorRequest{
        body: body,
        api: "user",
        resource: Some(&resource),
        method: Method::PUT
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(format!("Roles updated successfully for {}",res.text().await.unwrap()))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn get_role(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    
    let username = username.into_inner();
    let resource = format!("{username}/role");
    let request = CoordinatorRequest{
        body: None,
        api: "user",
        resource: Some(&resource),
        method: Method::GET
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((web::Json(res.json::<Value>().await.unwrap()),StatusCode::OK))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn delete_user(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();
    let request = CoordinatorRequest{
        body: None,
        api: "user",
        resource: Some(&username),
        method: Method::DELETE
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(format!("deleted user: {}",res.text().await.unwrap()))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn post_gen_password(username: web::Path<String>) -> Result<impl Responder, RBACError> {
    let username = username.into_inner();

    let request = CoordinatorRequest{
        body: None,
        api: "user",
        resource: Some(&username),
        method: Method::POST
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(res.text().await.unwrap())
        },
        _ => {
            let err_msg = res.text().await?;
            Err(RBACError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}