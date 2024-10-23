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

use actix_web::{http::header, web::{self, Redirect}, HttpMessage, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use http::StatusCode;
use reqwest::{Client, RequestBuilder, Response};
use serde_json::Value;

use crate::{handlers::http::{base_path, cluster::utils::check_liveness, logstream::error::StreamError, modal::{coordinator_server::QUERY_COORDINATION, utils::logstream_utils::create_update_stream}, users::dashboards::DashboardError}, storage::LogStream};

use super::{CoordinatorRequest, Method};

pub async fn post(req: HttpRequest, body: Bytes) -> Result<impl Responder, DashboardError> {
    let request = CoordinatorRequest{
        body: Some(body),
        api: "dashboards",
        resource: None,
        method: Method::POST
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((web::Json(res.json::<Value>().await.unwrap()),StatusCode::OK))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(DashboardError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn list(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let request = CoordinatorRequest{
        body: None,
        api: "dashboards",
        resource: None,
        method: Method::GET
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((web::Json(res.json::<Value>().await.unwrap()),StatusCode::OK))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(DashboardError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn get(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?
        .to_owned();

    let request = CoordinatorRequest{
        body: None,
        api: "dashboards",
        resource: Some(&dashboard_id),
        method: Method::GET
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((web::Json(res.json::<Value>().await.unwrap()),StatusCode::OK))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(DashboardError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, DashboardError> {
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?
        .to_owned();

    let request = CoordinatorRequest{
        body: None,
        api: "dashboards",
        resource: Some(&dashboard_id),
        method: Method::DELETE
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(HttpResponse::Ok().finish())
        },
        _ => {
            let err_msg = res.text().await?;
            Err(DashboardError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn update(req: HttpRequest, body: Bytes) -> Result<impl Responder, DashboardError> {
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?
        .to_owned();

    let request = CoordinatorRequest{
        body: Some(body),
        api: "dashboards",
        resource: Some(&dashboard_id),
        method: Method::PUT
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((web::Json(res.json::<Value>().await.unwrap()),StatusCode::OK))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(DashboardError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}