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
pub mod disk;
pub mod error;
pub mod header_parsing;
pub mod human_size;
pub mod json;
pub mod time;
pub mod uid;
pub mod update;

use crate::handlers::TENANT_ID;
use crate::handlers::http::middleware::{CLUSTER_SECRET, CLUSTER_SECRET_HEADER};
use crate::handlers::http::rbac::RBACError;
use crate::parseable::{DEFAULT_TENANT, PARSEABLE};
use crate::query::resolve_stream_names;
use crate::rbac::Users;
use crate::rbac::map::{SessionKey, sessions};
use crate::rbac::role::{Action, ParseableResourceType, Permission};
use crate::utils::header_parsing::{
    HeaderBuildError, ParseHeaderError, generated_text_reqwest_header_value, optional_header_str,
};
use actix::extract_session_key_from_req;
use actix_web::dev::ServiceRequest;
use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};
use actix_web::{FromRequest, HttpMessage, HttpRequest};
use actix_web_httpauth::extractors::basic::BasicAuth;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use once_cell::sync::Lazy;
use regex::Regex;

/// Compiled once at startup; reused for every `extract_datetime` call.
/// Recompiling per call was the hot-path bottleneck for hot-tier work-list
/// construction (~50μs per call * 100k+ files per tick).
static PARTITION_DATETIME_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/minute=(\d{2})")
        .expect("partition datetime regex is valid")
});
use sha2::{Digest, Sha256};

pub fn get_node_id() -> String {
    let now = Utc::now().to_rfc3339();
    get_hash(&now).to_string().split_at(15).0.to_string()
}

pub fn extract_datetime(path: &str) -> Option<NaiveDateTime> {
    if let Some(caps) = PARTITION_DATETIME_RE.captures(path) {
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
    let ext_session = req.extensions().get::<SessionKey>().cloned();
    if let Some(session) = ext_session {
        match &session {
            SessionKey::Ephemeral(ephemeral) => {
                if let Ok(header_val) = HeaderValue::from_str(&ephemeral.tenant) {
                    req.headers_mut()
                        .insert(HeaderName::from_static(TENANT_ID), header_val);
                }
            }
            _ => sessions().mutate_request_with_tenant(&session, req),
        }
        return;
    }

    let creds = BasicAuth::extract(req.request()).into_inner();

    if let Ok(basic) = &creds {
        Users.mutate_request_with_basic_user(basic.user_id(), basic.password().unwrap_or(""), req);
    } else if let Some(cookie) = req.cookie("session")
        && let Ok(ulid) = ulid::Ulid::from_string(cookie.value())
    {
        let key = SessionKey::SessionId(ulid);
        sessions().mutate_request_with_tenant(&key, req);
    };
}

pub fn get_user_from_request(req: &HttpRequest) -> Result<String, RBACError> {
    let session_key = extract_session_key_from_req(req).map_err(|_| RBACError::UserDoesNotExist)?;
    let (user_id, _) = Users
        .get_userid_from_session(&session_key)
        .ok_or(RBACError::UserDoesNotExist)?;
    Ok(user_id)
}

pub fn get_user_and_tenant_from_request(
    req: &HttpRequest,
) -> Result<(String, Option<String>), RBACError> {
    let session_key = extract_session_key_from_req(req).map_err(|_| RBACError::UserDoesNotExist)?;
    match session_key {
        SessionKey::BasicAuth { username, password } => {
            if let Some(tenant) = Users.get_user_tenant_from_basic(&username, &password) {
                Ok((username.clone(), Some(tenant)))
            } else {
                Ok((username.clone(), None))
            }
        }
        session @ (SessionKey::SessionId(_) | SessionKey::Ephemeral(_)) => {
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
}

/// Returns the tenant id from the `x-p-tenant` request header.
///
/// A missing header is `Ok(None)`. A header that is present but not visible
/// ASCII is rejected with `ParseHeaderError::InvalidTenantId` (400) instead of
/// silently falling back to the default tenant.
pub fn get_tenant_id_from_request(req: &HttpRequest) -> Result<Option<String>, ParseHeaderError> {
    tenant_id_from_headers(req.headers())
}

fn tenant_id_from_headers(headers: &HeaderMap) -> Result<Option<String>, ParseHeaderError> {
    optional_header_str(headers, TENANT_ID)
        .map(|tenant| tenant.map(str::to_owned))
        .map_err(|_| ParseHeaderError::InvalidTenantId)
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
                    // `stream` is the role's resource selector and may be the
                    // wildcard `*`; load the dataset being authorized instead.
                    if !PARSEABLE.check_or_load_stream(table_name, tenant_id).await {
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
                // global query role
                Permission::Resource(Action::Query, None) => {
                    authorized = true;
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

    Ok(is_admin_for_session(&session_key))
}

pub fn is_admin_for_session(session_key: &SessionKey) -> bool {
    let permissions = Users.get_permissions(session_key);

    // Check if user has admin permissions (Action::All on All resources)
    for permission in permissions.iter() {
        match permission {
            Permission::Resource(Action::All, Some(ParseableResourceType::All)) => {
                return true;
            }
            _ => continue,
        }
    }

    false
}

/// Builds the auth headers used to forward a request to another node.
///
/// With a cluster secret the peer authenticates the caller by `userid`.
/// Without one, the caller's `Authorization` header is forwarded if present;
/// otherwise the request authenticates as this node with `node_token`. A
/// session cookie is never forwarded, since sessions live only on the node
/// that issued them and the peer would reject it.
///
/// A malformed tenant header on the incoming request is a client error
/// (`HeaderBuildError::Input`, 400). Values the server stored or generated
/// itself (cluster secret hash, user id, node token) failing to encode is a
/// server error (`HeaderBuildError::Internal`, 500). Header values are never
/// included in the error.
pub fn create_intracluster_auth_headermap(
    req: &HeaderMap,
    node_token: &str,
    userid: &str,
) -> Result<reqwest::header::HeaderMap, HeaderBuildError> {
    build_intracluster_auth_headermap(
        req,
        node_token,
        userid,
        CLUSTER_SECRET.get().map(|(_, hash)| hash.as_str()),
    )
}

fn copied_header_value(
    value: &actix_web::http::header::HeaderValue,
) -> Result<reqwest::header::HeaderValue, HeaderBuildError> {
    reqwest::header::HeaderValue::from_bytes(value.as_bytes())
        .map_err(|_| HeaderBuildError::Input(ParseHeaderError::InvalidValue))
}

fn build_intracluster_auth_headermap(
    req: &HeaderMap,
    node_token: &str,
    userid: &str,
    cluster_secret_hash: Option<&str>,
) -> Result<reqwest::header::HeaderMap, HeaderBuildError> {
    let mut map = reqwest::header::HeaderMap::new();
    // reject a malformed client-supplied tenant regardless of the auth branch
    let tenant_id = tenant_id_from_headers(req)?;
    if let Some(hash) = cluster_secret_hash {
        let tenant_value =
            reqwest::header::HeaderValue::from_str(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
                .map_err(|_| HeaderBuildError::Input(ParseHeaderError::InvalidTenantId))?;
        // also insert server secret along with other required headers for query forwarding
        map.insert(
            reqwest::header::HeaderName::from_static(CLUSTER_SECRET_HEADER),
            generated_text_reqwest_header_value(hash, "cluster secret hash")?,
        );
        map.insert(
            reqwest::header::HeaderName::from_static("intra-cluster-tenant"),
            tenant_value,
        );
        map.insert(
            reqwest::header::HeaderName::from_static("intra-cluster-userid"),
            generated_text_reqwest_header_value(userid, "user id")?,
        );
    } else if let Some(auth) = req.get(actix_web::http::header::AUTHORIZATION) {
        map.insert(reqwest::header::AUTHORIZATION, copied_header_value(auth)?);
    } else {
        map.insert(
            reqwest::header::AUTHORIZATION,
            generated_text_reqwest_header_value(node_token, "node token")?,
        );
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::header::{HeaderName, HeaderValue};
    use actix_web::test::TestRequest;

    fn invalid_tenant_value() -> HeaderValue {
        HeaderValue::from_bytes(b"tenant\xff").expect("obs-text is a valid header value")
    }

    fn headers(pairs: &[(&'static str, HeaderValue)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(HeaderName::from_static(name), value.clone());
        }
        map
    }

    #[test]
    fn tenant_id_missing_is_none() {
        let req = TestRequest::default().to_http_request();
        assert!(matches!(get_tenant_id_from_request(&req), Ok(None)));
    }

    #[test]
    fn tenant_id_valid_is_some() {
        let req = TestRequest::default()
            .insert_header((TENANT_ID, "acme"))
            .to_http_request();
        assert_eq!(
            get_tenant_id_from_request(&req).unwrap().as_deref(),
            Some("acme")
        );
    }

    #[test]
    fn tenant_id_invalid_bytes_is_rejected() {
        let req = TestRequest::default()
            .insert_header((TENANT_ID, invalid_tenant_value()))
            .to_http_request();
        assert!(matches!(
            get_tenant_id_from_request(&req),
            Err(ParseHeaderError::InvalidTenantId)
        ));
    }

    #[test]
    fn forwarding_with_cluster_secret_sets_intracluster_headers() {
        let req = headers(&[(TENANT_ID, HeaderValue::from_static("acme"))]);
        let map = build_intracluster_auth_headermap(&req, "Basic token", "alice", Some("hash"))
            .expect("valid inputs build headers");
        assert_eq!(map.get(CLUSTER_SECRET_HEADER).unwrap(), "hash");
        assert_eq!(map.get("intra-cluster-tenant").unwrap(), "acme");
        assert_eq!(map.get("intra-cluster-userid").unwrap(), "alice");
        assert!(map.get(reqwest::header::AUTHORIZATION).is_none());
    }

    #[test]
    fn forwarding_with_cluster_secret_defaults_missing_tenant() {
        let map = build_intracluster_auth_headermap(&HeaderMap::new(), "t", "alice", Some("hash"))
            .expect("valid inputs build headers");
        assert_eq!(map.get("intra-cluster-tenant").unwrap(), DEFAULT_TENANT);
    }

    #[test]
    fn forwarding_rejects_malformed_tenant_as_input_error() {
        let req = headers(&[(TENANT_ID, invalid_tenant_value())]);
        for secret in [Some("hash"), None] {
            let err = build_intracluster_auth_headermap(&req, "t", "alice", secret).unwrap_err();
            assert!(matches!(
                err,
                HeaderBuildError::Input(ParseHeaderError::InvalidTenantId)
            ));
            assert_eq!(
                actix_web::ResponseError::status_code(&err),
                actix_web::http::StatusCode::BAD_REQUEST
            );
        }
    }

    #[test]
    fn forwarding_rejects_invalid_secret_hash_as_internal_without_leaking() {
        let err = build_intracluster_auth_headermap(
            &HeaderMap::new(),
            "t",
            "alice",
            Some("secret\nvalue"),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            HeaderBuildError::Internal("cluster secret hash")
        ));
        assert_eq!(
            actix_web::ResponseError::status_code(&err),
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(!err.to_string().contains("secret\nvalue"));
        assert!(!format!("{err:?}").contains("secret\nvalue"));
    }

    #[test]
    fn forwarding_rejects_invalid_userid_as_internal() {
        let err =
            build_intracluster_auth_headermap(&HeaderMap::new(), "t", "al\u{7f}ice", Some("hash"))
                .unwrap_err();
        assert!(matches!(err, HeaderBuildError::Internal("user id")));
    }

    #[test]
    fn forwarding_copies_authorization_header() {
        let req = headers(&[(
            "authorization",
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        )]);
        let map = build_intracluster_auth_headermap(&req, "node", "alice", None).unwrap();
        assert_eq!(
            map.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Basic dXNlcjpwYXNz"
        );
        assert!(map.get(reqwest::header::COOKIE).is_none());
    }

    #[test]
    fn forwarding_replaces_session_cookie_with_node_token() {
        let req = headers(&[(
            "cookie",
            HeaderValue::from_static("session=01M6ARA6TCK200TXKYC6CWRN5D; username=admin"),
        )]);
        let map = build_intracluster_auth_headermap(&req, "Basic node", "alice", None).unwrap();
        assert!(map.get(reqwest::header::COOKIE).is_none());
        assert_eq!(
            map.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Basic node"
        );
    }

    #[test]
    fn forwarding_never_copies_cookie() {
        let req = headers(&[("cookie", HeaderValue::from_static("session=abc"))]);
        for (req, secret) in [
            (req.clone(), Some("hash")),
            (
                headers(&[
                    ("cookie", HeaderValue::from_static("session=abc")),
                    (
                        "authorization",
                        HeaderValue::from_static("Basic dXNlcjpwYXNz"),
                    ),
                ]),
                None,
            ),
        ] {
            let map = build_intracluster_auth_headermap(&req, "node", "alice", secret).unwrap();
            assert!(map.get(reqwest::header::COOKIE).is_none());
        }
    }

    #[test]
    fn forwarding_copies_non_ascii_authorization_bytes() {
        // obs-text is valid in both http versions and must not be rejected
        let req = headers(&[(
            "authorization",
            HeaderValue::from_bytes(b"Basic \xe9").unwrap(),
        )]);
        let map = build_intracluster_auth_headermap(&req, "node", "alice", None).unwrap();
        assert_eq!(
            map.get(reqwest::header::AUTHORIZATION).unwrap().as_bytes(),
            b"Basic \xe9"
        );
    }

    #[test]
    fn forwarding_falls_back_to_node_token() {
        let map = build_intracluster_auth_headermap(&HeaderMap::new(), "Basic node", "alice", None)
            .unwrap();
        assert_eq!(
            map.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Basic node"
        );
    }

    #[test]
    fn forwarding_rejects_invalid_node_token_as_internal_without_leaking() {
        let err = build_intracluster_auth_headermap(&HeaderMap::new(), "tok\r\nen", "alice", None)
            .unwrap_err();
        assert!(matches!(err, HeaderBuildError::Internal("node token")));
        assert!(!err.to_string().contains("tok"));
    }

    // text the receiving node can't read with `to_str`; `from_str` accepts the non-ASCII ones
    const UNREADABLE_TEXT: [&str; 3] = ["josé", "用户", "ctl\u{1}"];

    fn assert_internal_without_leaking(err: HeaderBuildError, field: &str, value: &str) {
        assert!(matches!(err, HeaderBuildError::Internal(f) if f == field));
        assert_eq!(
            actix_web::ResponseError::status_code(&err),
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(!err.to_string().contains(value));
        assert!(!format!("{err:?}").contains(value));
    }

    #[test]
    fn forwarding_rejects_unreadable_generated_text_as_internal() {
        for value in UNREADABLE_TEXT {
            let err =
                build_intracluster_auth_headermap(&HeaderMap::new(), "t", "alice", Some(value))
                    .unwrap_err();
            assert_internal_without_leaking(err, "cluster secret hash", value);

            let err =
                build_intracluster_auth_headermap(&HeaderMap::new(), "t", value, Some("hash"))
                    .unwrap_err();
            assert_internal_without_leaking(err, "user id", value);

            let token = format!("Basic {value}");
            let err = build_intracluster_auth_headermap(&HeaderMap::new(), &token, "alice", None)
                .unwrap_err();
            assert_internal_without_leaking(err, "node token", value);
        }
    }

    #[test]
    fn forwarding_ignores_unused_generated_text() {
        // a copied credential wins over the node token, so a bad token is never encoded
        let req = headers(&[(
            "authorization",
            HeaderValue::from_bytes(b"Basic caf\xc3\xa9").unwrap(),
        )]);
        let map = build_intracluster_auth_headermap(&req, "Basic josé", "josé", None).unwrap();
        assert_eq!(
            map.get(reqwest::header::AUTHORIZATION).unwrap().as_bytes(),
            b"Basic caf\xc3\xa9"
        );
        assert!(map.get("intra-cluster-userid").is_none());
    }
}
