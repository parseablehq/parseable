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
use serde_json::json;

use crate::{
    handlers::http::logstream::error::StreamError, hottier::get_disk_usage, option::CONFIG,
};

/// This endpoint will fetch hottier info for the query node
pub async fn hottier_info(_req: HttpRequest) -> Result<impl Responder, StreamError> {
    if CONFIG.parseable.hot_tier_storage_path.is_none() {
        return Err(StreamError::Anyhow(anyhow::Error::msg(
            "HotTier is not enabled for this server",
        )));
    }

    // if hottier is enabled, send back disk usage info
    let (total, available, used) = get_disk_usage();

    Ok(web::Json(json!({
        "total": total,
        "available": available,
        "used": used
    })))
}
