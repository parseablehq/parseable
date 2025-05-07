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

use actix_web::{web, HttpRequest, Responder};

use crate::{
    prism::home::{generate_home_response, generate_home_search_response, PrismHomeError},
    utils::actix::extract_session_key_from_req,
};

const HOME_SEARCH_QUERY_PARAM: &str = "key";

/// Fetches the data to populate Prism's home
///
///
/// # Returns
///
/// A JSONified version of the `HomeResponse` struct.
pub async fn home_api(req: HttpRequest) -> Result<impl Responder, PrismHomeError> {
    let key = extract_session_key_from_req(&req)
        .map_err(|err| PrismHomeError::Anyhow(anyhow::Error::msg(err.to_string())))?;

    let res = generate_home_response(&key).await?;

    Ok(web::Json(res))
}

pub async fn home_search(req: HttpRequest) -> Result<impl Responder, PrismHomeError> {
    let key = extract_session_key_from_req(&req)
        .map_err(|err| PrismHomeError::Anyhow(anyhow::Error::msg(err.to_string())))?;
    let query_string = req.query_string();
    if query_string.is_empty() {
        return Ok(web::Json(serde_json::json!({})));
    }
    // Validate query string format
    let query_parts: Vec<&str> = query_string.split('=').collect();
    if query_parts.len() != 2 || query_parts[0] != HOME_SEARCH_QUERY_PARAM {
        return Err(PrismHomeError::InvalidQueryParameter(
            HOME_SEARCH_QUERY_PARAM.to_string(),
        ));
    }

    let query_value = query_parts[1].to_lowercase();
    let res = generate_home_search_response(&key, &query_value).await?;
    let json_res = serde_json::to_value(res)
        .map_err(|err| PrismHomeError::Anyhow(anyhow::Error::msg(err.to_string())))?;

    Ok(web::Json(json_res))
}
