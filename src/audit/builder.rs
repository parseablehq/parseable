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

use std::fmt::Display;

use crate::{about::current, storage::StorageMetadata};

use chrono::Utc;
use tracing::error;
use ulid::Ulid;

use super::{
    ActorDetails, AuditDetails, AuditLog, AuditLogVersion, RequestDetails, ResponseDetails,
    ServerDetails, AUDIT_LOG_TX,
};

/// Builder pattern implementation for constructing audit logs
pub struct AuditLogBuilder {
    // Used to ensure that log is only constructed if the logger is enabled
    enabled: bool,
    inner: AuditLog,
}

impl Default for AuditLogBuilder {
    fn default() -> Self {
        AuditLogBuilder {
            enabled: AUDIT_LOG_TX.get().is_some(),
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

        // NOTE: we are fine with blocking here as user expects audit logs to be sent at all costs
        if let Err(e) = AUDIT_LOG_TX
            .get()
            .expect("Audit logger not initialized")
            .send(audit_log)
            .await
        {
            error!("Couldn't send to logger: {e}")
        }
    }
}
