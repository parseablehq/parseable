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

use actix_cors::Cors;
use serde_json::Value;

use self::{modal::query_server::QueryServer, query::Query};

pub(crate) mod about;
pub(crate) mod health_check;
pub(crate) mod ingest;
mod kinesis;
pub(crate) mod llm;
pub(crate) mod logstream;
pub(crate) mod middleware;
pub(crate) mod modal;
pub(crate) mod oidc;
mod otel;
pub(crate) mod query;
pub(crate) mod rbac;
pub(crate) mod role;

pub const MAX_EVENT_PAYLOAD_SIZE: usize = 10485760;
pub const API_BASE_PATH: &str = "/api";
pub const API_VERSION: &str = "v1";

pub(crate) fn base_path() -> String {
    format!("{API_BASE_PATH}/{API_VERSION}")
}

pub fn metrics_path() -> String {
    format!("{}/metrics", base_path())
}

pub(crate) fn cross_origin_config() -> Cors {
    if cfg!(feature = "debug") {
        Cors::permissive().block_on_origin_mismatch(false)
    } else {
        Cors::default().block_on_origin_mismatch(false)
    }
}

pub fn base_path_without_preceding_slash() -> String {
    base_path().trim_start_matches('/').to_string()
}

pub async fn send_schema_request(stream_name: &str) -> anyhow::Result<Vec<arrow_schema::Schema>> {
    let mut res = vec![];
    let ima = QueryServer::get_ingester_info().await.unwrap();

    for im in ima {
        // TODO: when you rebase the code from the Cluster Info PR update this uri generation
        let uri = format!("{}api/v1/logstream/{}/schema", im.domain_name, stream_name);
        let reqw = reqwest::Client::new()
            .get(uri)
            .header(http::header::AUTHORIZATION, im.token.clone())
            .header(http::header::CONTENT_TYPE, "application/json")
            .send()
            .await?;

        if reqw.status().is_success() {
            let v = serde_json::from_slice(&reqw.bytes().await?)?;
            res.push(v);
        }
    }

    Ok(res)
}

pub async fn send_query_request_to_ingester(query: &Query) -> anyhow::Result<Vec<Value>> {
    // send the query request to the ingester
    let mut res = vec![];
    let ima = QueryServer::get_ingester_info().await.unwrap();

    for im in ima.iter() {
        // TODO: when you rebase the code from the Cluster Info PR update this uri generation
        let uri = format!("{}api/v1/{}", im.domain_name, "query");
        let reqw = reqwest::Client::new()
            .post(uri)
            .json(query)
            .header(http::header::AUTHORIZATION, im.token.clone())
            .header(http::header::CONTENT_TYPE, "application/json")
            .send()
            .await?;

        if reqw.status().is_success() {
            let v: Value = serde_json::from_slice(&reqw.bytes().await?)?;
            // the value returned is an array of json objects
            // so it needs to be flattened
            if let Some(arr) = v.as_array() {
                for val in arr {
                    res.push(val.to_owned())
                }
            }
        }
    }

    Ok(res)
}
