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
    web::{self, Path},
    Responder,
};

use crate::prism::logstream::{get_prism_logstream_info, PrismLogstreamError};

/// This API is essentially just combining the responses of /info and /schema together
pub async fn get_info(stream_name: Path<String>) -> Result<impl Responder, PrismLogstreamError> {
    let prism_logstream_info = get_prism_logstream_info(&stream_name).await?;

    Ok(web::Json(prism_logstream_info))
}
