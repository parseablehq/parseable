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

use super::middleware::Message;
use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
};
use actix_web_httpauth::extractors::basic::BasicAuth;
use ulid::Ulid;

use crate::{
    audit::AuditLogBuilder,
    handlers::{KINESIS_COMMON_ATTRIBUTES_KEY, STREAM_NAME_HEADER_KEY},
    rbac::{map::SessionKey, Users},
};

const DROP_HEADERS: [&str; 4] = ["authorization", "cookie", "user-agent", "x-p-stream"];

pub async fn audit_log_middleware(
    mut req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, actix_web::Error> {
    let mut log_builder = AuditLogBuilder::default();

    if let Some(kinesis_common_attributes) =
        req.request().headers().get(KINESIS_COMMON_ATTRIBUTES_KEY)
    {
        let attribute_value: &str = kinesis_common_attributes.to_str().unwrap();
        let message: Message = serde_json::from_str(attribute_value).unwrap();
        log_builder.set_stream_name(message.common_attributes.x_p_stream);
    } else if let Some(stream) = req.match_info().get("logstream") {
        log_builder.set_stream_name(stream.to_owned());
    } else if let Some(value) = req.headers().get(STREAM_NAME_HEADER_KEY) {
        if let Ok(stream) = value.to_str() {
            log_builder.set_stream_name(stream.to_owned());
        }
    }
    let mut username = "Unknown".to_owned();
    let mut authorization_method = "None".to_owned();

    // Extract authorization details from request, either from basic auth
    // header or cookie, else use default value.
    if let Ok(creds) = req.extract::<BasicAuth>().into_inner() {
        username = creds.user_id().trim().to_owned();
        authorization_method = "Basic Auth".to_owned();
    } else if let Some(cookie) = req.cookie("session") {
        authorization_method = "Session Cookie".to_owned();
        if let Some(user_id) = Ulid::from_string(cookie.value())
            .ok()
            .and_then(|ulid| Users.get_username_from_session(&SessionKey::SessionId(ulid)))
        {
            username = user_id;
        }
    }

    let conn = req.connection_info();
    let headers = req
        .headers()
        .iter()
        .filter_map(|(name, value)| match name.as_str() {
            // NOTE: drop headers that are not required
            name if DROP_HEADERS.contains(&name.to_lowercase().as_str()) => None,
            name => {
                // NOTE: Drop headers that can't be parsed as string
                value
                    .to_str()
                    .map(|value| (name.to_owned(), value.to_string()))
                    .ok()
            }
        });
    log_builder.set_request(req.method().as_str(), req.path(), conn.scheme(), headers);

    log_builder.set_actor(
        conn.realip_remote_addr().unwrap_or_default(),
        req.headers()
            .get("User-Agent")
            .and_then(|a| a.to_str().ok())
            .unwrap_or_default(),
        username,
        authorization_method,
    );
    drop(conn);

    let res = next.call(req).await;

    // Capture status_code and error information from response
    match &res {
        Ok(res) => {
            let status = res.status();
            // Use error information from reponse object if an error
            if let Some(err) = res.response().error() {
                log_builder.set_response(status.as_u16(), err);
            }
        }
        Err(err) => log_builder.set_response(500, err),
    }

    log_builder.send().await;

    res
}
