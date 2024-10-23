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

use crate::{handlers::http::{base_path, logstream::error::StreamError, modal::{coordinator_server::QUERY_COORDINATION, utils::logstream_utils::create_update_stream}}, storage::LogStream};

use super::{CoordinatorRequest, Method};

pub async fn put_stream(req: HttpRequest, body: Bytes) -> Result<impl Responder, StreamError> {
    
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let request = CoordinatorRequest {
        body: Some(body),
        api: "logstream",
        resource: Some(&stream_name),
        method: Method::PUT
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(("Log stream created", StatusCode::OK))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn delete(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let request = CoordinatorRequest {
        body: None,
        api: "logstream",
        resource: Some(&stream_name),
        method: Method::DELETE
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}


pub async fn list(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let request = CoordinatorRequest {
        body: None,
        api: "logstream",
        resource: None,
        method: Method::GET
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(web::Json(res.json::<Value>().await.unwrap()))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn get_stream_info(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let resource = format!("{stream_name}/info");
    let request = CoordinatorRequest {
        body: None,
        api: "logstream",
        resource: Some(&resource),
        method: Method::GET
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(web::Json(res.json::<Value>().await.unwrap()))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn put_alert(
    req: HttpRequest,
    body: Bytes,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let resource = format!("{stream_name}/alert");
    let request = CoordinatorRequest {
        body: Some(body),
        api: "logstream",
        resource: Some(&resource),
        method: Method::PUT
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((
                format!("set alert configuration for log stream {stream_name}"),
                StatusCode::OK,
            ))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn get_alert(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let resource = format!("{stream_name}/alert");
    let request = CoordinatorRequest {
        body: None,
        api: "logstream",
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
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn schema(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let resource = format!("{stream_name}/schema");
    let request = CoordinatorRequest {
        body: None,
        api: "logstream",
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
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn get_stats(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let resource = format!("{stream_name}/stats");
    let request = CoordinatorRequest {
        body: None,
        api: "logstream",
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
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn put_retention(
    req: HttpRequest,
    body: Bytes,
) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let resource = format!("{stream_name}/retention");
    let request = CoordinatorRequest {
        body: Some(body),
        api: "logstream",
        resource: Some(&resource),
        method: Method::PUT
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok((
                format!("set retention configuration for log stream {stream_name}"),
                StatusCode::OK,
            ))
        },
        _ => {
            let err_msg = res.text().await?;
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}

pub async fn get_retention(req: HttpRequest) -> Result<impl Responder, StreamError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let resource = format!("{stream_name}/retention");
    let request = CoordinatorRequest {
        body: None,
        api: "logstream",
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
            Err(StreamError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}