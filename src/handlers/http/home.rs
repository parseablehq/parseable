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
    home::{generate_home_response, HomeError},
    utils::actix::extract_session_key_from_req,
};

/// Fetches the data to populate Prism's home
///
///
/// # Returns
///
/// A JSONified version of the `HomeResponse` struct.
pub async fn home_api(req: HttpRequest) -> Result<impl Responder, HomeError> {
    let key = extract_session_key_from_req(&req)
        .map_err(|err| HomeError::Anyhow(anyhow::Error::msg(err.to_string())))?;

    let res = generate_home_response(&key).await?;

    Ok(web::Json(res))
}
