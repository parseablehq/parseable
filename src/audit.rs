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

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use crate::about::current;
use crate::handlers::http::modal::utils::rbac_utils::get_metadata;

use super::option::CONFIG;
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use tracing::error;

use ulid::Ulid;
use url::Url;

static AUDIT_LOGGER: Lazy<Option<AuditLogger>> = Lazy::new(AuditLogger::new);

pub struct AuditLogger {
    client: Arc<Client>,
    log_endpoint: Url,
    username: Option<String>,
    password: Option<String>,
}

impl AuditLogger {
    /// Create an audit logger that can be used to capture
    /// and push audit logs to the appropriate logging system over HTTP
    pub fn new() -> Option<AuditLogger> {
        let log_endpoint = match CONFIG
            .parseable
            .audit_logger
            .as_ref()?
            .join("/api/v1/ingest")
        {
            Ok(url) => url,
            Err(err) => {
                eprintln!("Couldn't setup audit logger: {err}");
                return None;
            }
        };

        let client = Arc::new(reqwest::Client::new());

        let username = CONFIG.parseable.audit_username.clone();
        let password = CONFIG.parseable.audit_password.clone();

        Some(AuditLogger {
            client,
            log_endpoint,
            username,
            password,
        })
    }

    async fn send_log(&self, json: Value) {
        let mut req = self
            .client
            .post(self.log_endpoint.as_str())
            .json(&json)
            .header("x-p-stream", "audit_log");
        if let Some(username) = self.username.as_ref() {
            req = req.basic_auth(username, self.password.as_ref())
        }

        match req.send().await {
            Ok(r) => {
                if let Err(e) = r.error_for_status() {
                    error!("{e}")
                }
            }
            Err(e) => error!("Failed to send audit event: {}", e),
        }
    }
}

#[non_exhaustive]
#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize)]
pub enum AuditLogVersion {
    V1 = 1,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActorLog {
    pub remote_host: String,
    pub user_agent: String,
    pub username: String,
    pub authorization_method: String,
}

#[derive(Serialize, Default)]
pub struct RequestLog {
    pub method: String,
    pub path: String,
    pub protocol: String,
    pub headers: HashMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseLog {
    pub status_code: u16,
    pub error: Option<String>,
}

impl Default for ResponseLog {
    fn default() -> Self {
        // Server failed to respond
        ResponseLog {
            status_code: 500,
            error: None,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditLog {
    pub version: AuditLogVersion,
    pub parseable_version: String,
    pub deployment_id: Ulid,
    pub audit_id: Ulid,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub stream: String,
    pub actor: ActorLog,
    pub request: RequestLog,
    pub response: ResponseLog,
}

pub struct AuditLogBuilder {
    start_time: DateTime<Utc>,
    stream: String,
    pub actor: Option<ActorLog>,
    pub request: Option<RequestLog>,
    pub response: Option<ResponseLog>,
}

impl Default for AuditLogBuilder {
    fn default() -> Self {
        AuditLogBuilder {
            start_time: Utc::now(),
            stream: String::default(),
            actor: None,
            request: None,
            response: None,
        }
    }
}

impl AuditLogBuilder {
    pub fn set_stream_name(mut self, stream: impl Into<String>) -> Self {
        if AUDIT_LOGGER.is_none() {
            return self;
        }
        self.stream = stream.into();

        self
    }

    pub fn set_actor(
        mut self,
        host: impl Into<String>,
        username: impl Into<String>,
        user_agent: impl Into<String>,
        auth_method: impl Into<String>,
    ) -> Self {
        if AUDIT_LOGGER.is_none() {
            return self;
        }
        self.actor = Some(ActorLog {
            remote_host: host.into(),
            user_agent: user_agent.into(),
            username: username.into(),
            authorization_method: auth_method.into(),
        });

        self
    }

    pub fn set_request(
        mut self,
        method: impl Into<String>,
        path: impl Into<String>,
        protocol: impl Into<String>,
        headers: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        if AUDIT_LOGGER.is_none() {
            return self;
        }
        self.request = Some(RequestLog {
            method: method.into(),
            path: path.into(),
            protocol: protocol.into(),
            headers: headers.into_iter().collect(),
        });

        self
    }

    pub fn set_response(mut self, status_code: u16, err: impl Display) -> Self {
        if AUDIT_LOGGER.is_none() {
            return self;
        }
        let error = err.to_string();
        let error = (!error.is_empty()).then_some(error);
        self.response = Some(ResponseLog { status_code, error });

        self
    }

    // NOTE: Ensure that the logger has been constructed by Default
    pub async fn send(self) {
        let AuditLogBuilder {
            start_time,
            stream,
            actor,
            request,
            response,
        } = self;
        let Some(logger) = AUDIT_LOGGER.as_ref() else {
            return;
        };
        let audit_log = AuditLog {
            version: AuditLogVersion::V1,
            parseable_version: current().released_version.to_string(),
            deployment_id: get_metadata().await.unwrap().deployment_id,
            audit_id: Ulid::new(),
            start_time,
            end_time: Utc::now(),
            stream,
            actor: actor.unwrap_or_default(),
            request: request.unwrap_or_default(),
            response: response.unwrap_or_default(),
        };

        logger.send_log(json!(audit_log)).await
    }
}
