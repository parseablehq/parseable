/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

pub mod event;
pub mod logstream;

use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use sysinfo::{System, SystemExt};

use crate::s3::S3;
use crate::storage::ObjectStorage;

pub async fn liveness() -> HttpResponse {
    // If the available memory is less than 100MiB, return a 503 error.
    // As liveness check fails, Kubelet will restart the server.
    if System::new_all().available_memory() < 100 * 1024 * 1024 {
        return HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE);
    }

    HttpResponse::new(StatusCode::OK)
}

pub async fn readiness() -> HttpResponse {
    if S3::new().is_available().await {
        return HttpResponse::new(StatusCode::OK);
    }

    HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE)
}
