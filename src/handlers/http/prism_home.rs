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

use std::collections::HashMap;

use actix_web::{HttpRequest, Responder, web};

use crate::{
    prism::home::{PrismHomeError, generate_home_response, generate_home_search_response},
    utils::actix::extract_session_key_from_req,
};

const HOME_SEARCH_QUERY_PARAM: &str = "key";
pub const HOME_QUERY_PARAM: &str = "includeInternal";
/// Fetches the data to populate Prism's home
///
///
/// # Returns
///
/// A JSONified version of the `HomeResponse` struct.
pub async fn home_api(req: HttpRequest) -> Result<impl Responder, PrismHomeError> {
    let key = extract_session_key_from_req(&req)
        .map_err(|err| PrismHomeError::Anyhow(anyhow::Error::msg(err.to_string())))?;
    let query_map = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|_| PrismHomeError::InvalidQueryParameter(HOME_QUERY_PARAM.to_string()))?;

    let include_internal = query_map.get(HOME_QUERY_PARAM).is_some_and(|v| v == "true");

    let res = generate_home_response(&key, include_internal).await?;

    Ok(web::Json(res))
}

pub async fn home_search(req: HttpRequest) -> Result<impl Responder, PrismHomeError> {
    let key = extract_session_key_from_req(&req)
        .map_err(|err| PrismHomeError::Anyhow(anyhow::Error::msg(err.to_string())))?;
    let query_map = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|_| PrismHomeError::InvalidQueryParameter(HOME_SEARCH_QUERY_PARAM.to_string()))?;

    if query_map.is_empty() {
        return Ok(web::Json(serde_json::json!({})));
    }

    let query_key = query_map
        .get(HOME_SEARCH_QUERY_PARAM)
        .ok_or_else(|| PrismHomeError::InvalidQueryParameter(HOME_SEARCH_QUERY_PARAM.to_string()))?
        .to_lowercase();

    let res = generate_home_search_response(&key, &query_key).await?;
    let json_res = serde_json::to_value(res)
        .map_err(|err| PrismHomeError::Anyhow(anyhow::Error::msg(err.to_string())))?;

    Ok(web::Json(json_res))
}
