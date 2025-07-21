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

use crate::{HTTP_CLIENT, about::current, parseable::PARSEABLE, storage::StorageMetadata};

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde::Serialize;
use serde_json::{Value, json};
use tracing::error;

use ulid::Ulid;
use url::Url;

static AUDIT_LOGGER: Lazy<Option<AuditLogger>> = Lazy::new(AuditLogger::new);

// AuditLogger handles sending audit logs to a remote logging system
pub struct AuditLogger {
    log_endpoint: Url,
}

impl AuditLogger {
    /// Create an audit logger that can be used to capture and push
    /// audit logs to the appropriate logging system over HTTP
    pub fn new() -> Option<AuditLogger> {
        // Try to construct the log endpoint URL by joining the base URL
        // with the ingest path, This can fail if the URL is not valid,
        // when the base URL is not set or the ingest path is not valid
        let log_endpoint = match PARSEABLE
            .options
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

        Some(AuditLogger { log_endpoint })
    }

    // Sends the audit log to the configured endpoint with proper authentication
    async fn send_log(&self, json: Value) {
        let mut req = HTTP_CLIENT
            .post(self.log_endpoint.as_str())
            .json(&json)
            .header("x-p-stream", "audit_log");

        // Use basic auth if credentials are configured
        if let Some(username) = PARSEABLE.options.audit_username.as_ref() {
            req = req.basic_auth(username, PARSEABLE.options.audit_password.as_ref())
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
#[derive(Debug, Clone, Copy, Serialize, Default)]
pub enum AuditLogVersion {
    // NOTE: default should be latest version
    #[default]
    V1 = 1,
}

#[derive(Serialize, Default)]
pub struct AuditDetails {
    pub version: AuditLogVersion,
    pub id: Ulid,
    pub generated_at: DateTime<Utc>,
}

#[derive(Serialize, Default)]
pub struct ServerDetails {
    pub version: String,
    pub deployment_id: Ulid,
}

// Contains information about the actor (user) who performed the action
#[derive(Serialize, Default)]
pub struct ActorDetails {
    pub remote_host: String,
    pub user_agent: String,
    pub username: String,
    pub authorization_method: String,
}

// Contains details about the HTTP request that was made
#[derive(Serialize, Default)]
pub struct RequestDetails {
    pub stream: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub method: String,
    pub path: String,
    pub protocol: String,
    pub headers: HashMap<String, String>,
}

/// Contains information about the response sent back to the client
#[derive(Default, Serialize)]
pub struct ResponseDetails {
    pub status_code: u16,
    pub error: Option<String>,
}

/// The main audit log structure that combines all audit information
#[derive(Serialize)]
pub struct AuditLog {
    pub audit: AuditDetails,
    pub parseable_server: ServerDetails,
    pub actor: ActorDetails,
    pub request: RequestDetails,
    pub response: ResponseDetails,
}

/// Builder pattern implementation for constructing audit logs
pub struct AuditLogBuilder {
    // Used to ensure that log is only constructed if the logger is enabled
    enabled: bool,
    inner: AuditLog,
}

impl Default for AuditLogBuilder {
    fn default() -> Self {
        AuditLogBuilder {
            enabled: AUDIT_LOGGER.is_some(),
            inner: AuditLog {
                audit: AuditDetails {
                    version: AuditLogVersion::V1,
                    id: Ulid::new(),
                    ..Default::default()
                },
                parseable_server: ServerDetails {
                    version: current().released_version.to_string(),
                    deployment_id: StorageMetadata::global().deployment_id,
                },
                request: RequestDetails {
                    start_time: Utc::now(),
                    ..Default::default()
                },
                actor: ActorDetails::default(),
                response: ResponseDetails::default(),
            },
        }
    }
}

impl AuditLogBuilder {
    /// Sets the remote host for the audit log
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        if self.enabled {
            self.inner.actor.remote_host = host.into();
        }
        self
    }

    /// Sets the username for the audit log
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        if self.enabled {
            self.inner.actor.username = username.into();
        }
        self
    }

    /// Sets the user agent for the audit log
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        if self.enabled {
            self.inner.actor.user_agent = user_agent.into();
        }
        self
    }

    /// Sets the authorization method for the audit log
    pub fn with_auth_method(mut self, auth_method: impl Into<String>) -> Self {
        if self.enabled {
            self.inner.actor.authorization_method = auth_method.into();
        }
        self
    }

    /// Sets the stream for the request details
    pub fn with_stream(mut self, stream: impl Into<String>) -> Self {
        if self.enabled {
            self.inner.request.stream = stream.into();
        }
        self
    }

    /// Sets the request method details
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        if self.enabled {
            self.inner.request.method = method.into();
        }
        self
    }

    /// Sets the request path
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        if self.enabled {
            self.inner.request.path = path.into();
        }
        self
    }

    /// Sets the request protocol
    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        if self.enabled {
            self.inner.request.protocol = protocol.into();
        }
        self
    }

    /// Sets the request headers
    pub fn with_headers(mut self, headers: impl IntoIterator<Item = (String, String)>) -> Self {
        if self.enabled {
            self.inner.request.headers = headers.into_iter().collect();
        }
        self
    }

    /// Sets the response status code
    pub fn with_status(mut self, status_code: u16) -> Self {
        if self.enabled {
            self.inner.response.status_code = status_code;
        }
        self
    }

    /// Sets the response error if any
    pub fn with_error(mut self, err: impl Display) -> Self {
        if self.enabled {
            let error = err.to_string();
            if !error.is_empty() {
                self.inner.response.error = Some(error);
            }
        }
        self
    }

    /// Sends the audit log to the logging server if configured
    pub async fn send(self) {
        // ensures that we don't progress if logger is not enabled
        if !self.enabled {
            return;
        }

        // build the audit log
        let AuditLogBuilder {
            inner: mut audit_log,
            ..
        } = self;

        let now = Utc::now();
        audit_log.audit.generated_at = now;
        audit_log.request.end_time = now;

        AUDIT_LOGGER
            .as_ref()
            .unwrap()
            .send_log(json!(audit_log))
            .await
    }
}
