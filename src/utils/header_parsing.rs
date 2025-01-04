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

const MAX_HEADERS_ALLOWED: usize = 10;
use actix_web::{HttpRequest, HttpResponse, ResponseError};

pub fn collect_labelled_headers(
    req: &HttpRequest,
    prefix: &str,
    kv_separator: char,
) -> Result<String, ParseHeaderError> {
    // filter out headers which has right prefix label and convert them into str;
    let headers = req.headers().iter().filter_map(|(key, value)| {
        let key = key.as_str().strip_prefix(prefix)?;
        Some((key, value))
    });

    let mut labels: Vec<String> = Vec::new();

    for (key, value) in headers {
        let value = value.to_str().map_err(|_| ParseHeaderError::InvalidValue)?;
        if key.is_empty() {
            return Err(ParseHeaderError::Emptykey);
        }
        if key.contains(kv_separator) {
            return Err(ParseHeaderError::SeperatorInKey(kv_separator));
        }
        if value.contains(kv_separator) {
            return Err(ParseHeaderError::SeperatorInValue(kv_separator));
        }

        labels.push(format!("{key}={value}"));
    }

    if labels.len() > MAX_HEADERS_ALLOWED {
        return Err(ParseHeaderError::MaxHeadersLimitExceeded);
    }

    Ok(labels.join(&kv_separator.to_string()))
}

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
}

impl ResponseError for ParseHeaderError {
    fn status_code(&self) -> http::StatusCode {
        http::StatusCode::BAD_REQUEST
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).body(self.to_string())
    }
}
