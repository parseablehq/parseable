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
*
*/

use std::collections::HashMap;

use actix_web::{
    dev::ServiceRequest,
    error::{ErrorUnauthorized, ErrorUnprocessableEntity},
    Error, FromRequest, HttpRequest,
};
use actix_web_httpauth::extractors::basic::BasicAuth;
use http::header::USER_AGENT;
use tracing::warn;

use crate::{
    event::{FORMAT_KEY, SOURCE_IP_KEY, USER_AGENT_KEY},
    handlers::{LOG_SOURCE_KEY, STREAM_NAME_HEADER_KEY},
    rbac::map::SessionKey,
};

const IGNORE_HEADERS: [&str; 2] = [STREAM_NAME_HEADER_KEY, LOG_SOURCE_KEY];
const MAX_CUSTOM_FIELDS: usize = 10;
const MAX_FIELD_VALUE_LENGTH: usize = 100;

pub fn extract_session_key(req: &mut ServiceRequest) -> Result<SessionKey, Error> {
    // Extract username and password from the request using basic auth extractor.
    let creds = req.extract::<BasicAuth>().into_inner();
    let basic = creds.map(|creds| {
        let username = creds.user_id().trim().to_owned();
        // password is not mandatory by basic auth standard.
        // If not provided then treat as empty string
        let password = creds.password().unwrap_or("").trim().to_owned();
        SessionKey::BasicAuth { username, password }
    });

    if let Ok(basic) = basic {
        Ok(basic)
    } else if let Some(cookie) = req.cookie("session") {
        let ulid = ulid::Ulid::from_string(cookie.value())
            .map_err(|_| ErrorUnprocessableEntity("Cookie is tampered with or invalid"))?;
        Ok(SessionKey::SessionId(ulid))
    } else {
        Err(ErrorUnauthorized("No authentication method supplied"))
    }
}

pub fn extract_session_key_from_req(req: &HttpRequest) -> Result<SessionKey, Error> {
    // Extract username and password from the request using basic auth extractor.
    let creds = BasicAuth::extract(req).into_inner();
    let basic = creds.map(|creds| {
        let username = creds.user_id().trim().to_owned();
        // password is not mandatory by basic auth standard.
        // If not provided then treat as empty string
        let password = creds.password().unwrap_or("").trim().to_owned();
        SessionKey::BasicAuth { username, password }
    });

    if let Ok(basic) = basic {
        Ok(basic)
    } else if let Some(cookie) = req.cookie("session") {
        let ulid = ulid::Ulid::from_string(cookie.value())
            .map_err(|_| ErrorUnprocessableEntity("Cookie is tampered with or invalid"))?;
        Ok(SessionKey::SessionId(ulid))
    } else {
        Err(ErrorUnauthorized("No authentication method supplied"))
    }
}

pub fn get_custom_fields_from_header(req: HttpRequest) -> HashMap<String, String> {
    let user_agent = req
        .headers()
        .get(USER_AGENT)
        .and_then(|a| a.to_str().ok())
        .unwrap_or_default();

    let conn = req.connection_info().clone();

    let source_ip = conn.realip_remote_addr().unwrap_or_default();
    let mut p_custom_fields = HashMap::new();
    p_custom_fields.insert(USER_AGENT_KEY.to_string(), user_agent.to_string());
    p_custom_fields.insert(SOURCE_IP_KEY.to_string(), source_ip.to_string());

    // Iterate through headers and add custom fields
    for (header_name, header_value) in req.headers().iter() {
        // Check if we've reached the maximum number of custom fields
        if p_custom_fields.len() >= MAX_CUSTOM_FIELDS {
            warn!(
                "Maximum number of custom fields ({}) reached, ignoring remaining headers",
                MAX_CUSTOM_FIELDS
            );
            break;
        }

        let header_name = header_name.as_str();
        if header_name.starts_with("x-p-") && !IGNORE_HEADERS.contains(&header_name) {
            if let Ok(value) = header_value.to_str() {
                let key = header_name.trim_start_matches("x-p-");
                if !key.is_empty() {
                    // Truncate value if it exceeds the maximum length
                    let truncated_value = if value.len() > MAX_FIELD_VALUE_LENGTH {
                        warn!(
                            "Header value for '{}' exceeds maximum length, truncating",
                            header_name
                        );
                        &value[..MAX_FIELD_VALUE_LENGTH]
                    } else {
                        value
                    };
                    p_custom_fields.insert(key.to_string(), truncated_value.to_string());
                } else {
                    warn!(
                        "Ignoring header with empty key after prefix: {}",
                        header_name
                    );
                }
            }
        }

        if header_name == LOG_SOURCE_KEY {
            if let Ok(value) = header_value.to_str() {
                p_custom_fields.insert(FORMAT_KEY.to_string(), value.to_string());
            }
        }
    }

    p_custom_fields
}
