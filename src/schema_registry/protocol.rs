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

//! Wire types for the ingestor -> querier schema resolution RPC.

use arrow_schema::{DataType, Field};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub const PROTOCOL_VERSION: u32 = 1;
pub const PROTOCOL_HEADER: &str = "x-p-schema-proto";
/// Path (relative to the API base path) of the resolve endpoint on the authority.
pub const RESOLVE_PATH: &str = "internal/schema/resolve";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResolveRequest {
    pub protocol_version: u32,
    /// Idempotency / correlation key. Reused verbatim on every retry of the same field set.
    pub request_id: Ulid,
    pub node_id: String,
    pub tenant_id: Option<String>,
    pub stream: String,
    /// Last `registry_version` this node observed for the stream (0 = never).
    pub known_version: u64,
    /// Fields the node has not resolved yet, with the type it wrote them as.
    pub fields: Vec<Field>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// The proposal was registered (first writer) or matches the registered type.
    Accepted,
    /// The name is registered with another type; write the column as `resolved_name`.
    Renamed,
    /// Like `Renamed`, and the suffixed name was already registered by an earlier request.
    AlreadyRenamed,
    /// No safe name exists (reserved column, or the suffix chain is exhausted).
    Unresolvable,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FieldResolution {
    pub original_name: String,
    /// Type the node proposed (as found in its arrow files).
    pub proposed_type: DataType,
    /// Name the node must write this column under. Equals `original_name` unless renamed.
    pub resolved_name: String,
    /// Authoritative type of `resolved_name`.
    pub data_type: DataType,
    /// Authoritative type of `original_name`, so a loser can flip its in-memory schema.
    pub registry_type: Option<DataType>,
    pub outcome: Outcome,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RegistryEntry {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResolveResponse {
    pub protocol_version: u32,
    pub request_id: Ulid,
    /// Registry version after this request (>= known_version).
    pub version: u64,
    /// One entry per proposed (non-null) field, in request order.
    pub resolutions: Vec<FieldResolution>,
    /// Full registry snapshot when the node is behind; lets it refresh its cache.
    #[serde(default)]
    pub delta: Vec<RegistryEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    BadRequest,
    StreamNotFound,
    StreamDeleting,
    Unavailable,
    UnsupportedProtocol,
    NotAuthority,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResolveErrorBody {
    pub protocol_version: u32,
    pub code: ErrorCode,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    #[test]
    fn request_round_trips_arrow_types() {
        let req = ResolveRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: Ulid::new(),
            node_id: "n1".into(),
            tenant_id: None,
            stream: "s".into(),
            known_version: 3,
            fields: vec![
                Field::new("a", DataType::Int64, true),
                Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            ],
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: ResolveRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.fields, req.fields);
        assert_eq!(back.request_id, req.request_id);
    }

    #[test]
    fn error_codes_are_screaming_snake_case() {
        let body = ResolveErrorBody {
            protocol_version: 1,
            code: ErrorCode::StreamNotFound,
            message: String::new(),
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["code"], "STREAM_NOT_FOUND");
    }
}
