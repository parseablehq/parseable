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

use actix_web::{
    HttpRequest, Responder,
    web::{self, Json, Path},
};

use crate::{
    prism::logstream::{PrismDatasetRequest, PrismLogstreamError, get_prism_logstream_info},
    utils::actix::extract_session_key_from_req,
};

/// This API is essentially just combining the responses of /info and /schema together
pub async fn get_info(stream_name: Path<String>) -> Result<impl Responder, PrismLogstreamError> {
    let prism_logstream_info = get_prism_logstream_info(&stream_name).await?;

    Ok(web::Json(prism_logstream_info))
}

/// A combination of /stats, /retention, /hottier, /info, /counts and /query
pub async fn post_datasets(
    dataset_req: Option<Json<PrismDatasetRequest>>,
    req: HttpRequest,
) -> Result<impl Responder, PrismLogstreamError> {
    let session_key = extract_session_key_from_req(&req)?;
    let dataset = dataset_req
        .map(|Json(r)| r)
        .unwrap_or_default()
        .get_datasets(session_key)
        .await?;

    Ok(web::Json(dataset))
}
