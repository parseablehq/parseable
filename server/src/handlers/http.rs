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
use arrow_schema::Schema;
use itertools::Itertools;
use serde_json::Value;

use crate::option::CONFIG;

use self::{cluster::get_ingestor_info, query::Query};

pub(crate) mod about;
mod cache;
pub mod cluster;
pub(crate) mod health_check;
pub(crate) mod ingest;
mod kinesis;
pub(crate) mod llm;
pub(crate) mod logstream;
pub(crate) mod middleware;
pub mod modal;
pub(crate) mod oidc;
mod otel;
pub(crate) mod query;
pub(crate) mod rbac;
pub(crate) mod role;
pub mod users;
pub const MAX_EVENT_PAYLOAD_SIZE: usize = 10485760;
pub const API_BASE_PATH: &str = "api";
pub const API_VERSION: &str = "v1";

pub(crate) fn base_path() -> String {
    format!("/{API_BASE_PATH}/{API_VERSION}")
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
    format!("{API_BASE_PATH}/{API_VERSION}")
}

/// Fetches the schema for the specified stream.
///
/// # Arguments
///
/// * `stream_name` - The name of the stream to fetch the schema for.
///
/// # Returns
///
/// An `anyhow::Result` containing the `arrow_schema::Schema` for the specified stream.
pub async fn fetch_schema(stream_name: &str) -> anyhow::Result<arrow_schema::Schema> {
    let path_prefix =
        relative_path::RelativePathBuf::from(format!("{}/{}", stream_name, ".stream"));
    let store = CONFIG.storage().get_object_store();
    let res: Vec<Schema> = store
        .get_objects(
            Some(&path_prefix),
            Box::new(|file_name: String| file_name.contains(".schema")),
        )
        .await?
        .iter()
        // we should be able to unwrap as we know the data is valid schema
        .map(|byte_obj| serde_json::from_slice(byte_obj).expect("data is valid json"))
        .collect_vec();

    let new_schema = Schema::try_merge(res)?;
    Ok(new_schema)
}

/// unused for now, might need it later
#[allow(unused)]
pub async fn send_query_request_to_ingestor(query: &Query) -> anyhow::Result<Value> {
    // send the query request to the ingestor
    let mut res = vec![];
    let ima = get_ingestor_info().await?;

    for im in ima.iter() {
        let uri = format!(
            "{}{}/{}",
            im.domain_name,
            base_path_without_preceding_slash(),
            "query"
        );
        let reqw = reqwest::Client::new()
            .post(uri)
            .json(query)
            .header(http::header::AUTHORIZATION, im.token.clone())
            .header(http::header::CONTENT_TYPE, "application/json")
            .send()
            .await;

        if let Ok(reqw) = reqw {
            // do i need to do a success check??
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

    Ok(Value::Array(res))
}
