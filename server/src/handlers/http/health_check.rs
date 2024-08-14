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

use actix_web::http::StatusCode;
use actix_web::HttpResponse;

use crate::option::CONFIG;

#[utoipa::path(
    get,
    tag = "Health Status",
    context_path = "/api/v1",
    path = "/liveness",
    responses(
        (status = 200, description = "The server is live.")
    )
)]
pub async fn liveness() -> HttpResponse {
    HttpResponse::new(StatusCode::OK)
}

#[utoipa::path(
    get,
    tag = "Health Status",
    context_path = "/api/v1",
    path = "/readiness",
    responses(
        (status = 200, description = "The object store is live."),
        (status = 503, description = "Service Unavailable.")
    ),
    security(
        ("basic_auth" = [])
    )
)]
pub async fn readiness() -> HttpResponse {
    if CONFIG.storage().get_object_store().check().await.is_ok() {
        return HttpResponse::new(StatusCode::OK);
    }

    HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE)
}
