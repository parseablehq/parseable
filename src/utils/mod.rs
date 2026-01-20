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

pub mod actix;
pub mod arrow;
pub mod error;
pub mod header_parsing;
pub mod human_size;
pub mod json;
pub mod time;
pub mod uid;
pub mod update;

use crate::handlers::http::rbac::RBACError;
use crate::parseable::{DEFAULT_TENANT, PARSEABLE};
use crate::query::resolve_stream_names;
use crate::rbac::Users;
use crate::rbac::map::{SessionKey, sessions};
use crate::rbac::role::{Action, ParseableResourceType, Permission};
use actix::extract_session_key_from_req;
use actix_web::dev::ServiceRequest;
use actix_web::{FromRequest, HttpRequest};
use actix_web_httpauth::extractors::basic::BasicAuth;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use regex::Regex;
use sha2::{Digest, Sha256};

pub fn get_node_id() -> String {
    let now = Utc::now().to_rfc3339();
    get_hash(&now).to_string().split_at(15).0.to_string()
}

pub fn extract_datetime(path: &str) -> Option<NaiveDateTime> {
    let re = Regex::new(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/minute=(\d{2})").unwrap();
    if let Some(caps) = re.captures(path) {
        let date_str = caps.get(1)?.as_str();
        let hour_str = caps.get(2)?.as_str();
        let minute_str = caps.get(3)?.as_str();

        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
        let time = NaiveTime::parse_from_str(&format!("{hour_str}:{minute_str}"), "%H:%M").ok()?;
        Some(NaiveDateTime::new(date, time))
    } else {
        None
    }
}

pub fn mutate_request_with_tenant(req: &mut ServiceRequest) {
    let creds = BasicAuth::extract(req.request()).into_inner();

    if let Ok(basic) = &creds {
        Users.mutate_request_with_basic_user(basic.user_id(), basic.password().unwrap(), req);
    } else if let Some(cookie) = req.cookie("session")
        && let Ok(ulid) = ulid::Ulid::from_string(cookie.value())
    {
        let key = SessionKey::SessionId(ulid);
        sessions().mutate_request_with_tenant(&key, req);
    };
}

pub fn get_user_from_request(req: &HttpRequest) -> Result<String, RBACError> {
    let session_key = extract_session_key_from_req(req).map_err(|_| RBACError::UserDoesNotExist)?;
    let user_id = Users.get_userid_from_session(&session_key);
    if user_id.is_none() {
        return Err(RBACError::UserDoesNotExist);
    }
    let (user_id, _) = user_id.unwrap();
    Ok(user_id)
}

pub fn get_user_and_tenant_from_request(
    req: &HttpRequest,
) -> Result<(String, Option<String>), RBACError> {
    let session_key = extract_session_key_from_req(req).map_err(|_| RBACError::UserDoesNotExist)?;
    match session_key {
        SessionKey::BasicAuth { username, password } => {
            if let Some(tenant) = Users.get_user_tenant_from_basic(&username, &password) {
                tracing::warn!(tenant=?tenant);
                tracing::warn!(user=?username);
                Ok((username.clone(), Some(tenant)))
            } else {
                // tracing::warn!(req=?req);
                // tracing::warn!(user=?username);
                Ok((username.clone(), None))
            }
        }
        session @ SessionKey::SessionId(_) => {
            let Some((user_id, tenant_id)) = Users.get_userid_from_session(&session) else {
                return Err(RBACError::UserDoesNotExist);
            };
            let tenant_id = if tenant_id.eq(DEFAULT_TENANT) {
                None
            } else {
                Some(tenant_id)
            };
            Ok((user_id, tenant_id))
        }
    }
    // let Some((user_id, tenant_id)) = Users.get_userid_from_session(&session_key) else {
    //     return Err(RBACError::UserDoesNotExist);
    // };
    // let tenant_id = if tenant_id.eq(DEFAULT_TENANT) {
    //     None
    // } else {
    //     // Some(std::borrow::Cow::Borrowed(tenant_id.as_str()))
    //     Some(tenant_id)
    // };
    // Ok((user_id, tenant_id))
}

pub fn get_tenant_id_from_request(req: &HttpRequest) -> Option<String> {
    req.headers()
        .get("tenant")
        .map(|tenant_value| tenant_value.to_str().unwrap().to_owned())
}

pub fn get_tenant_id_from_key(key: &SessionKey) -> Option<String> {
    if let Some((_, tenant_id)) = Users.get_userid_from_session(key)
        && tenant_id.ne(DEFAULT_TENANT)
    {
        Some(tenant_id.clone())
    } else {
        None
    }
}

pub fn get_hash(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key);
    let result = format!("{:x}", hasher.finalize());
    result
}

pub async fn user_auth_for_query(
    session_key: &SessionKey,
    query: &str,
) -> Result<(), actix_web::error::Error> {
    let tables = resolve_stream_names(query).map_err(|e| {
        actix_web::error::ErrorBadRequest(format!("Failed to extract table names: {e}"))
    })?;
    let tenant_id = get_tenant_id_from_key(session_key);
    let permissions = Users.get_permissions(session_key);
    user_auth_for_datasets(&permissions, &tables, &tenant_id).await
}

pub async fn user_auth_for_datasets(
    permissions: &[Permission],
    tables: &[String],
    tenant_id: &Option<String>,
) -> Result<(), actix_web::error::Error> {
    for table_name in tables {
        let mut authorized = false;

        // in permission check if user can run query on the stream.
        // also while iterating add any filter tags for this stream
        for permission in permissions.iter() {
            match permission {
                Permission::Resource(Action::All, _) => {
                    authorized = true;
                    break;
                }
                Permission::Resource(
                    Action::Query,
                    Some(ParseableResourceType::Stream(stream)),
                ) => {
                    if !PARSEABLE.check_or_load_stream(stream, tenant_id).await {
                        return Err(actix_web::error::ErrorUnauthorized(format!(
                            "Stream not found: {table_name}"
                        )));
                    }
                    let is_internal =
                        PARSEABLE
                            .get_stream(table_name, tenant_id)
                            .is_ok_and(|stream| {
                                stream
                                    .get_stream_type()
                                    .eq(&crate::storage::StreamType::Internal)
                            });

                    if stream == table_name || stream == "*" || is_internal {
                        authorized = true;
                    }
                }
                Permission::Resource(action, Some(ParseableResourceType::All))
                    if ![
                        Action::All,
                        Action::PutUser,
                        Action::PutRole,
                        Action::DeleteUser,
                        Action::DeleteRole,
                        Action::ModifyUserGroup,
                        Action::CreateUserGroup,
                        Action::DeleteUserGroup,
                        Action::DeleteNode,
                    ]
                    .contains(action) =>
                {
                    authorized = true;
                }
                _ => (),
            }
        }

        if !authorized {
            return Err(actix_web::error::ErrorUnauthorized(format!(
                "User does not have access to stream- {table_name}"
            )));
        }
    }

    Ok(())
}

pub fn is_admin(req: &HttpRequest) -> Result<bool, anyhow::Error> {
    let session_key =
        extract_session_key_from_req(req).map_err(|e| anyhow::Error::msg(e.to_string()))?;

    let permissions = Users.get_permissions(&session_key);

    // Check if user has admin permissions (Action::All on All resources)
    for permission in permissions.iter() {
        match permission {
            Permission::Resource(Action::All, Some(ParseableResourceType::All)) => {
                return Ok(true);
            }
            _ => continue,
        }
    }

    Ok(false)
}
