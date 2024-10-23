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

use actix_web::{http::header::ContentType, web, HttpResponse, Responder, Result};
use http::{header, StatusCode};
use itertools::Itertools;
use reqwest::{self, Client};
use serde_json::{json, Value};
use crate::{handlers::http::{base_path, modal::coordinator_server::QUERY_COORDINATION}, metrics::{build_metrics_handler, error::MetricsError}};

use super::{CoordinatorRequest, Method};



pub async fn get() -> Result<impl Responder, MetricsError> {

    let request = CoordinatorRequest{
        body: None,
        api: "metrics",
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
            Err(MetricsError::Anyhow(anyhow::Error::msg(err_msg)))
        }
    }
}
    