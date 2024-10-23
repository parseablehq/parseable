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

use actix_web::{http::header, web::{self, Redirect}, HttpMessage, HttpRequest, Responder};
use bytes::Bytes;
use http::StatusCode;
use reqwest::{Client, RequestBuilder, Response};
use serde_json::Value;

use crate::{handlers::http::{base_path, ingest::PostError, logstream::error::StreamError, modal::{coordinator_server::QUERY_COORDINATION, utils::logstream_utils::create_update_stream}}, storage::LogStream};

use super::{CoordinatorRequest, Method};

pub async fn get_cluster_info() -> Result<impl Responder, StreamError> {

    let request = CoordinatorRequest {
        body: None,
        api: "cluster",
        resource: Some("info"),
        method: Method::GET
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(actix_web::HttpResponse::Ok().json(res.json::<Value>().await.unwrap()))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn get_cluster_metrics() -> Result<impl Responder, PostError> {
    let request = CoordinatorRequest {
        body: None,
        api: "cluster",
        resource: Some("metrics"),
        method: Method::GET
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(actix_web::HttpResponse::Ok().json(res.json::<Value>().await.unwrap()))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(PostError::CustomError(err_msg))
        }
    }
}

pub async fn remove_ingestor(req: HttpRequest) -> Result<impl Responder, PostError> {
    let domain_name: String = req.match_info().get("ingestor").unwrap().parse().unwrap();
    let request = CoordinatorRequest {
        body: None,
        api: "cluster",
        resource: Some(&domain_name),
        method: Method::DELETE
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((res.text().await.unwrap(), StatusCode::OK))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(PostError::CustomError(err_msg))
        }
    }
}