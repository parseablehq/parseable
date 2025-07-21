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

use actix_web::{
    Error, FromRequest, HttpRequest,
    dev::ServiceRequest,
    error::{ErrorUnauthorized, ErrorUnprocessableEntity},
};
use actix_web_httpauth::extractors::basic::BasicAuth;

use crate::rbac::map::SessionKey;

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
