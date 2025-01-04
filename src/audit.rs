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

// AuditLogger handles sending audit logs to a remote logging system
pub struct AuditLogger {
    client: Client,
    log_endpoint: Url,
}

impl AuditLogger {
    /// Create an audit logger that can be used to capture and push 
    /// audit logs to the appropriate logging system over HTTP
    pub fn new() -> Option<AuditLogger> {
        // Try to construct the log endpoint URL by joining the base URL 
        // with the ingest path, This can fail if the URL is not valid, 
        // when the base URL is not set or the ingest path is not valid
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

        Some(AuditLogger {
            client: reqwest::Client::new(),
            log_endpoint,
        })
    }

    // Sends the audit log to the configured endpoint with proper authentication
    async fn send_log(&self, json: Value) {
        let mut req = self
            .client
            .post(self.log_endpoint.as_str())
            .json(&json)
            .header("x-p-stream", "audit_log");
        
        // Use basic auth if credentials are configured
        if let Some(username) = CONFIG.parseable.audit_username.as_ref() {
            req = req.basic_auth(username, CONFIG.parseable.audit_password.as_ref())
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

// Represents the version of the audit log format
#[non_exhaustive]
#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize)]
pub enum AuditLogVersion {
    V1 = 1,
}

// Contains information about the actor (user) who performed the action
#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActorLog {
    pub remote_host: String,
    pub user_agent: String,
    pub username: String,
    pub authorization_method: String,
}

// Contains details about the HTTP request that was made
#[derive(Serialize, Default)]
pub struct RequestLog {
    pub method: String,
    pub path: String,
    pub protocol: String,
    pub headers: HashMap<String, String>,
}

/// Contains information about the response sent back to the client
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseLog {
    pub status_code: u16,
    pub error: Option<String>,
}

/// The main audit log structure that combines all audit information
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

/// Builder pattern implementation for constructing audit logs
pub struct AuditLogBuilder {
    // Used to ensure that log is only constructed if the logger is enabled
    enabled: bool,
    start_time: DateTime<Utc>,
    stream: String,
    pub actor: Option<ActorLog>,
    pub request: Option<RequestLog>,
    pub response: Option<ResponseLog>,
}

impl Default for AuditLogBuilder {
    fn default() -> Self {
        AuditLogBuilder {
            enabled: AUDIT_LOGGER.is_some(),
            start_time: Utc::now(),
            stream: String::default(),
            actor: None,
            request: None,
            response: None,
        }
    }
}

impl AuditLogBuilder {
    /// Sets the stream name for the audit log if logger is set
    pub fn set_stream_name(mut self, stream: impl Into<String>) -> Self {
        if !self.enabled {
            return self;
        }
        self.stream = stream.into();

        self
    }

    /// Sets the actor details for the audit log if logger is set
    pub fn set_actor(
        mut self,
        host: impl Into<String>,
        username: impl Into<String>,
        user_agent: impl Into<String>,
        auth_method: impl Into<String>,
    ) -> Self {
        if !self.enabled {
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

    /// Sets the request details for the audit log if logger is set
    pub fn set_request(
        mut self,
        method: impl Into<String>,
        path: impl Into<String>,
        protocol: impl Into<String>,
        headers: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        if !self.enabled {
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

    /// Sets the response details for the audit log if logger is set
    pub fn set_response(mut self, status_code: u16, err: impl Display) -> Self {
        if !self.enabled {
            return self;
        }
        let error = err.to_string();
        let error = (!error.is_empty()).then_some(error);
        self.response = Some(ResponseLog { status_code, error });

        self
    }

    /// Sends the audit log to the logging server if configured
    pub async fn send(self) {
        let AuditLogBuilder {
            start_time,
            stream,
            actor,
            request,
            response,
            ..
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
