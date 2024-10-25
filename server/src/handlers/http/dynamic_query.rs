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

use crate::dynamic_query::DynamicQuery;
use crate::handlers::http::query::QueryError;
use crate::query::QUERY_SESSION;
use actix_web::web::Json;
use actix_web::{FromRequest, HttpRequest, Responder};
use anyhow::anyhow;
use lazy_static::lazy_static;
use regex::Regex;
use std::{future::Future, pin::Pin, time::Duration};
use ulid::Ulid;

const MAX_CACHE_DURATION: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct RawDynamicQuery {
    pub query: String,
    pub cache_duration: String,
}
lazy_static! {
    static ref DURATION_REGEX: Regex = Regex::new(r"^([0-9]+)([dhms])$").unwrap();
}
fn parse_duration(s: &str) -> Option<Duration> {
    DURATION_REGEX.captures(s).and_then(|cap| {
        let value = cap[1].parse::<u64>().unwrap();
        let unit = &cap[2];
        match unit {
            "s" => Duration::from_secs(value).into(),
            "m" => Duration::from_secs(value * 60).into(),
            "h" => Duration::from_secs(value * 60 * 60).into(),
            "d" => Duration::from_secs(value * 60 * 60 * 24).into(),
            _ => None,
        }
    })
}
async fn from_raw_query(raw: &RawDynamicQuery) -> DynamicQuery {
    let session_state = QUERY_SESSION.state();

    // get the logical plan and extract the table name
    let raw_logical_plan = session_state.create_logical_plan(&raw.query).await.unwrap();
    DynamicQuery {
        cache_duration: parse_duration(&raw.cache_duration).expect("Invalid duration"),
        plan: raw_logical_plan,
    }
}
impl FromRequest for DynamicQuery {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let query = Json::<RawDynamicQuery>::from_request(req, payload);
        let fut = async move {
            let query_res = query.await?.into_inner();
            Ok(from_raw_query(&query_res).await)
        };
        Box::pin(fut)
    }
}
pub async fn dynamic_query(req: HttpRequest, query: DynamicQuery) -> Result<String, QueryError> {
    if query.cache_duration > MAX_CACHE_DURATION {
        return Err(QueryError::Anyhow(anyhow!(
            "Cache duration is over limit: {} mins",
            MAX_CACHE_DURATION.as_secs() / 60u64
        )));
    }
    let uuid = Ulid::new();
    crate::dynamic_query::register_query(uuid, query).await?;
    Ok(format!("{}/{}", req.uri(), uuid))
}
pub async fn dynamic_lookup(req: HttpRequest) -> Result<impl Responder, QueryError> {
    let uuid_txt = req
        .match_info()
        .get("uuid")
        .ok_or_else(|| QueryError::Anyhow(anyhow!("Missing UUID")))?;
    let uuid =
        Ulid::from_string(uuid_txt).map_err(|_| QueryError::Anyhow(anyhow!("Invalid UUID")))?;
    let res = crate::dynamic_query::load(uuid).await?;
    res.to_http()
}
