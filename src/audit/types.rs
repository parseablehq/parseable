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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use ulid::Ulid;

// Represents the version of the audit log format
#[non_exhaustive]
#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Default, Deserialize)]
pub enum AuditLogVersion {
    // NOTE: default should be latest version
    #[default]
    V1 = 1,
}

#[derive(Serialize, Default, Deserialize)]
pub struct AuditDetails {
    pub version: AuditLogVersion,
    pub id: Ulid,
    pub generated_at: DateTime<Utc>,
}

#[derive(Serialize, Default, Deserialize)]
pub struct ServerDetails {
    pub version: String,
    pub deployment_id: Ulid,
}

// Contains information about the actor (user) who performed the action
#[derive(Serialize, Default, Deserialize)]
pub struct ActorDetails {
    pub remote_host: String,
    pub user_agent: String,
    pub username: String,
    pub authorization_method: String,
}

// Contains details about the HTTP request that was made
#[derive(Serialize, Default, Deserialize)]
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
#[derive(Default, Serialize, Deserialize)]
pub struct ResponseDetails {
    pub status_code: u16,
    pub error: Option<String>,
}

/// The main audit log structure that combines all audit information
#[derive(Serialize, Deserialize)]
pub struct AuditLog {
    pub audit: AuditDetails,
    pub parseable_server: ServerDetails,
    pub actor: ActorDetails,
    pub request: RequestDetails,
    pub response: ResponseDetails,
}
