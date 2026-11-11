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

//! The schema authority hosted by the querier (OSS) or Prism (enterprise).
//!
//! Exactly one authority runs per cluster. It keeps one write-once [`StreamRegistry`]
//! per (tenant, stream), serialises resolutions per stream, and durably writes the
//! stream-level `.schema` before replying.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use actix_web::{HttpRequest, HttpResponse, ResponseError, http::StatusCode, web};
use arrow_schema::{Field, Schema};
use once_cell::sync::Lazy;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tracing::{info, warn};

use crate::{
    event::{
        DEFAULT_TIMESTAMP_KEY, FORMAT_KEY, FORMAT_VERIFY_KEY, SOURCE_IP_KEY, USER_AGENT_KEY,
        commit_schema,
    },
    parseable::{DEFAULT_TENANT, PARSEABLE, Stream},
    utils::get_tenant_id_from_request,
};

use super::{
    authoritative_schema_path, merge_schemas_tolerant,
    protocol::{
        ErrorCode, Outcome, PROTOCOL_VERSION, RegistryEntry, ResolveErrorBody, ResolveRequest,
        ResolveResponse,
    },
    registry::StreamRegistry,
};

/// Metadata key holding the registry version inside the persisted `.schema`.
pub const REGISTRY_VERSION_KEY: &str = "parseable.registry_version";

type Slot = Arc<AsyncMutex<Option<StreamRegistry>>>;

#[derive(Default)]
pub struct SchemaAuthority {
    slots: Mutex<HashMap<(String, String), Slot>>,
}

pub static SCHEMA_AUTHORITY: Lazy<SchemaAuthority> = Lazy::new(SchemaAuthority::default);

#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error("{0}")]
    BadRequest(String),
    #[error("stream {0} not found")]
    StreamNotFound(String),
    #[error("stream {0} is being deleted")]
    StreamDeleting(String),
    #[error("unsupported schema resolution protocol version {0}")]
    UnsupportedProtocol(u32),
    #[error("schema authority unavailable: {0}")]
    Unavailable(String),
}

impl AuthorityError {
    fn code(&self) -> ErrorCode {
        match self {
            Self::BadRequest(_) => ErrorCode::BadRequest,
            Self::StreamNotFound(_) => ErrorCode::StreamNotFound,
            Self::StreamDeleting(_) => ErrorCode::StreamDeleting,
            Self::UnsupportedProtocol(_) => ErrorCode::UnsupportedProtocol,
            Self::Unavailable(_) => ErrorCode::Unavailable,
        }
    }
}

impl ResponseError for AuthorityError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::StreamNotFound(_) => StatusCode::NOT_FOUND,
            Self::StreamDeleting(_) => StatusCode::CONFLICT,
            Self::UnsupportedProtocol(_) => StatusCode::UPGRADE_REQUIRED,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ResolveErrorBody {
            protocol_version: PROTOCOL_VERSION,
            code: self.code(),
            message: self.to_string(),
        })
    }
}

fn slot_key(tenant_id: &Option<String>, stream: &str) -> (String, String) {
    (
        tenant_id.as_deref().unwrap_or(DEFAULT_TENANT).to_owned(),
        stream.to_owned(),
    )
}

impl SchemaAuthority {
    fn slot(&self, tenant_id: &Option<String>, stream: &str) -> Slot {
        self.slots
            .lock()
            .expect("schema authority lock poisoned")
            .entry(slot_key(tenant_id, stream))
            .or_default()
            .clone()
    }

    /// Takes the per-stream lock and drops the cached registry, so another writer of the
    /// stream-level `.schema` is serialised with resolutions and the registry is re-read
    /// from storage afterwards.
    pub async fn lock_and_invalidate(
        &self,
        tenant_id: &Option<String>,
        stream: &str,
    ) -> OwnedMutexGuard<Option<StreamRegistry>> {
        let mut guard = self.slot(tenant_id, stream).lock_owned().await;
        *guard = None;
        guard
    }

    /// Drops cached state, e.g. after the stream was deleted.
    pub fn forget_stream(&self, tenant_id: &Option<String>, stream: &str) {
        self.slots
            .lock()
            .expect("schema authority lock poisoned")
            .remove(&slot_key(tenant_id, stream));
    }

    pub async fn resolve(&self, req: ResolveRequest) -> Result<ResolveResponse, AuthorityError> {
        if req.protocol_version != PROTOCOL_VERSION {
            return Err(AuthorityError::UnsupportedProtocol(req.protocol_version));
        }
        if req.stream.is_empty() {
            return Err(AuthorityError::BadRequest("stream name is empty".into()));
        }
        // Load before taking the slot lock: loading may rewrite `.schema` through
        // `create_schema_from_metastore`, which takes the same lock.
        if !PARSEABLE
            .check_or_load_stream(&req.stream, &req.tenant_id)
            .await
        {
            return Err(AuthorityError::StreamNotFound(req.stream));
        }
        let stream = PARSEABLE
            .get_stream(&req.stream, &req.tenant_id)
            .map_err(|_| AuthorityError::StreamNotFound(req.stream.clone()))?;
        if stream.is_deleting() {
            return Err(AuthorityError::StreamDeleting(req.stream));
        }

        let reserved = reserved_fields(&stream);
        let mut guard = self.slot(&req.tenant_id, &req.stream).lock_owned().await;
        if guard.is_none() {
            *guard = Some(load_registry(&req.stream, &req.tenant_id, reserved).await?);
        }
        let version_before = guard.as_ref().expect("registry loaded above").version;
        let resolution = guard
            .as_ref()
            .expect("registry loaded above")
            .resolve(&req.fields);

        let registry = guard.as_mut().expect("registry loaded above");
        if !resolution.registered.is_empty() {
            let mut next = registry.clone();
            next.apply(&resolution.registered);
            let schema = registry_schema(&next);
            // Persist before replying: once an ingestor sees an answer it must never change.
            PARSEABLE
                .metastore
                .put_schema(schema.clone(), &req.stream, &req.tenant_id)
                .await
                .map_err(|e| AuthorityError::Unavailable(e.to_string()))?;
            *registry = next;
            // Best effort: let this node's own queries see the new fields immediately.
            let in_memory = Schema::new(schema.fields().clone());
            if let Err(err) = commit_schema(&req.stream, Arc::new(in_memory), &req.tenant_id) {
                warn!(
                    stream = req.stream,
                    "in-memory schema update skipped: {err}"
                );
            }
        }

        for r in &resolution.resolutions {
            match r.outcome {
                Outcome::Renamed | Outcome::AlreadyRenamed => info!(
                    stream = req.stream,
                    node = req.node_id,
                    "schema conflict: field {} proposed as {} is registered as {:?}; writing it as {}",
                    r.original_name,
                    r.proposed_type,
                    r.registry_type,
                    r.resolved_name
                ),
                Outcome::Unresolvable => warn!(
                    stream = req.stream,
                    node = req.node_id,
                    "schema conflict on field {} ({}) cannot be resolved automatically",
                    r.original_name,
                    r.proposed_type
                ),
                Outcome::Accepted => {}
            }
        }

        let delta = if req.known_version < version_before {
            let mut entries: Vec<RegistryEntry> = registry
                .fields
                .iter()
                .map(|(name, data_type)| RegistryEntry {
                    name: name.clone(),
                    data_type: data_type.clone(),
                })
                .collect();
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            entries
        } else {
            Vec::new()
        };

        Ok(ResolveResponse {
            protocol_version: PROTOCOL_VERSION,
            request_id: req.request_id,
            version: registry.version,
            resolutions: resolution.resolutions,
            delta,
        })
    }
}

/// Columns that are never renamed: the server-set `p_*` fields and the partition columns.
fn reserved_fields(stream: &Stream) -> HashSet<String> {
    let mut reserved: HashSet<String> = [
        DEFAULT_TIMESTAMP_KEY,
        USER_AGENT_KEY,
        SOURCE_IP_KEY,
        FORMAT_KEY,
        FORMAT_VERIFY_KEY,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    reserved.extend(stream.get_time_partition());
    reserved.extend(
        stream
            .get_custom_partition()
            .iter()
            .flat_map(|p| p.split(',').map(|s| s.trim().to_owned())),
    );
    reserved
}

fn registry_schema(registry: &StreamRegistry) -> Schema {
    let mut fields: Vec<Field> = registry
        .fields
        .iter()
        .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
        .collect();
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Schema::new_with_metadata(
        fields,
        HashMap::from([(
            REGISTRY_VERSION_KEY.to_owned(),
            registry.version.to_string(),
        )]),
    )
}

/// Seeds a registry from the authoritative `.schema` first, then from every per-node
/// schema file (first type wins). Persists immediately when seeding added fields, so a
/// restart re-seeds to the same answer regardless of listing order.
async fn load_registry(
    stream: &str,
    tenant_id: &Option<String>,
    reserved: HashSet<String>,
) -> Result<StreamRegistry, AuthorityError> {
    let mut registry = StreamRegistry {
        reserved,
        ..Default::default()
    };
    let storage = PARSEABLE.storage.get_object_store();
    let authoritative = match storage
        .get_object(&authoritative_schema_path(stream, tenant_id), tenant_id)
        .await
    {
        Ok(bytes) => serde_json::from_slice::<Schema>(&bytes).ok(),
        Err(_) => None,
    };
    let mut persisted_fields = 0;
    if let Some(schema) = &authoritative {
        registry.version = schema
            .metadata()
            .get(REGISTRY_VERSION_KEY)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        registry.seed(schema.fields().iter().map(|f| f.as_ref()));
        persisted_fields = registry.fields.len();
    }

    let others = PARSEABLE
        .metastore
        .get_all_schemas(stream, tenant_id)
        .await
        .map_err(|e| AuthorityError::Unavailable(e.to_string()))?;
    let merged = merge_schemas_tolerant(stream, others);
    for (name, kept, skipped) in registry.seed(merged.fields().iter().map(|f| f.as_ref())) {
        warn!(
            stream,
            "legacy schema conflict on field {name}: keeping {kept}, ignoring {skipped}"
        );
    }

    if registry.fields.len() != persisted_fields || authoritative.is_none() {
        registry.version += 1;
        PARSEABLE
            .metastore
            .put_schema(registry_schema(&registry), stream, tenant_id)
            .await
            .map_err(|e| AuthorityError::Unavailable(e.to_string()))?;
    }
    Ok(registry)
}

/// `POST {base}/internal/schema/resolve`
pub async fn resolve_handler(
    req: HttpRequest,
    body: web::Json<ResolveRequest>,
) -> Result<HttpResponse, AuthorityError> {
    let body = body.into_inner();
    let header_tenant = get_tenant_id_from_request(&req).filter(|t| t != DEFAULT_TENANT);
    let body_tenant = body.tenant_id.clone().filter(|t| t != DEFAULT_TENANT);
    if header_tenant.is_some() && header_tenant != body_tenant {
        return Err(AuthorityError::BadRequest(
            "tenant in request body does not match the authenticated tenant".into(),
        ));
    }
    let mut body = body;
    body.tenant_id = body_tenant;
    let response = SCHEMA_AUTHORITY.resolve(body).await?;
    Ok(HttpResponse::Ok().json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn registry_schema_carries_version_and_sorted_fields() {
        let mut r = StreamRegistry::default();
        r.fields.insert("b".into(), DataType::Utf8);
        r.fields.insert("a".into(), DataType::Int64);
        r.version = 7;
        let s = registry_schema(&r);
        assert_eq!(s.field(0).name(), "a");
        assert_eq!(s.metadata()[REGISTRY_VERSION_KEY], "7");
        let round: Schema = serde_json::from_slice(&serde_json::to_vec(&s).unwrap()).unwrap();
        assert_eq!(round.metadata()[REGISTRY_VERSION_KEY], "7");
    }

    #[test]
    fn error_status_mapping() {
        assert_eq!(
            AuthorityError::StreamNotFound("s".into()).status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            AuthorityError::StreamDeleting("s".into()).status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            AuthorityError::UnsupportedProtocol(9).status_code(),
            StatusCode::UPGRADE_REQUIRED
        );
    }
}
