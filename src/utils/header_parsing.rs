/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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
use actix_web::{HttpResponse, ResponseError};

#[derive(Debug, thiserror::Error)]
pub enum ParseHeaderError {
    #[error("Too many headers received. Limit is of 5 headers")]
    MaxHeadersLimitExceeded,
    #[error("A value passed in header can't be formatted to plain visible ASCII")]
    InvalidValue,
    #[error("Invalid Key was passed which terminated just after the end of prefix")]
    Emptykey,
    #[error("A key passed in header contains reserved char {0}")]
    SeperatorInKey(char),
    #[error("A value passed in header contains reserved char {0}")]
    SeperatorInValue(char),
    #[error("Stream name not found in header [x-p-stream]")]
    MissingStreamName,
    #[error("Log source not found in header [x-p-log-source]")]
    MissingLogSource,
    #[error("Tenant id not found in header [tenant]")]
    MissingTenantId,
    #[error("Invalid tenant id found in header [tenant]")]
    InvalidTenantId,
    #[error("Unexpected header found- {0}")]
    UnexpectedHeader(String),
}

impl ResponseError for ParseHeaderError {
    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).body(self.to_string())
    }
}
