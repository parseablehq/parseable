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

//! Ingestor-side client of the schema authority.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use once_cell::sync::Lazy;
use reqwest::{StatusCode, header};
use tokio::{runtime::Runtime, sync::Semaphore};
use tracing::warn;

use crate::{
    INTRA_CLUSTER_CLIENT,
    handlers::http::{
        base_path_without_preceding_slash, cluster::utils::check_liveness,
        middleware::CLUSTER_SECRET, modal::NodeMetadata,
    },
    parseable::{DEFAULT_TENANT, PARSEABLE},
};

use super::protocol::{
    ErrorCode, PROTOCOL_HEADER, PROTOCOL_VERSION, RESOLVE_PATH, ResolveErrorBody, ResolveRequest,
    ResolveResponse,
};
use super::{authority_node_type, nodes_of_type};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ATTEMPTS: u32 = 3;
const BACKOFF_BASE: Duration = Duration::from_millis(200);
const AUTHORITY_CACHE_TTL: Duration = Duration::from_secs(30);

/// An authority without the resolve endpoint (or protocol) blocks streams with new fields
/// until it is upgraded, so upgrade the querier/Prism before the ingestors.
const UPGRADE_AUTHORITY: &str =
    "schema authority does not support this schema resolution protocol; upgrade the querier/Prism";

#[derive(Debug, thiserror::Error)]
pub enum ResolveFailure {
    /// Network error, timeout, overload or an authority that is not ready. Retry later.
    #[error("schema authority unavailable: {0}")]
    Transient(String),
    #[error("stream not found on the schema authority")]
    StreamNotFound,
    #[error("stream is being deleted")]
    StreamDeleting,
    /// Request rejected; retrying the same request will not help.
    #[error("schema resolution rejected: {0}")]
    Rejected(String),
}

#[derive(Clone, Debug)]
struct Target {
    /// Base URL ending with `/`.
    domain: String,
    authorization: String,
}

static RESOLVER_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("schema-resolver")
        .enable_all()
        .build()
        .expect("schema resolver runtime")
});

static PERMITS: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(PARSEABLE.options.schema_resolve_concurrency as usize));

static AUTHORITY: Lazy<Mutex<Option<(Instant, Target)>>> = Lazy::new(|| Mutex::new(None));

fn invalidate_authority() {
    *AUTHORITY.lock().expect("authority cache poisoned") = None;
}

async fn find_authority(tenant_id: &Option<String>) -> Result<Target, ResolveFailure> {
    if let Some((at, target)) = AUTHORITY.lock().expect("authority cache poisoned").clone()
        && at.elapsed() < AUTHORITY_CACHE_TTL
    {
        return Ok(target);
    }

    // The authority (Prism if the cluster has one, else the querier) publishes its address
    // in its node metadata. Pick deterministically (lowest live node id) so every ingestor
    // talks to the same node.
    let node_type = authority_node_type(tenant_id)
        .await
        .map_err(|e| ResolveFailure::Transient(e.to_string()))?;
    let mut nodes: Vec<NodeMetadata> = nodes_of_type(node_type.clone(), tenant_id)
        .await
        .map_err(|e| ResolveFailure::Transient(e.to_string()))?;
    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let mut chosen = None;
    for node in nodes {
        if check_liveness(&node.domain_name).await {
            chosen = Some(node);
            break;
        }
    }
    let q = chosen.ok_or_else(|| {
        ResolveFailure::Transient(format!(
            "no live {node_type:?} node found for schema resolution"
        ))
    })?;
    let domain = if q.domain_name.ends_with('/') {
        q.domain_name
    } else {
        format!("{}/", q.domain_name)
    };
    let target = Target {
        domain,
        authorization: q.token,
    };

    *AUTHORITY.lock().expect("authority cache poisoned") = Some((Instant::now(), target.clone()));
    Ok(target)
}

fn headers(target: &Target, tenant_id: &Option<String>) -> header::HeaderMap {
    let mut map = header::HeaderMap::new();
    map.insert(
        header::HeaderName::from_static(PROTOCOL_HEADER),
        header::HeaderValue::from(PROTOCOL_VERSION),
    );
    if let Some((_, hash)) = CLUSTER_SECRET.get()
        && let Ok(hash) = header::HeaderValue::from_str(hash)
    {
        map.insert(
            header::HeaderName::from_static(
                crate::handlers::http::middleware::CLUSTER_SECRET_HEADER,
            ),
            hash,
        );
        if let Ok(tenant) =
            header::HeaderValue::from_str(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT))
        {
            map.insert(
                header::HeaderName::from_static("intra-cluster-tenant"),
                tenant,
            );
        }
        if let Ok(user) = header::HeaderValue::from_str(&PARSEABLE.options.username) {
            map.insert(
                header::HeaderName::from_static("intra-cluster-userid"),
                user,
            );
        }
    } else if let Ok(auth) = header::HeaderValue::from_str(&target.authorization) {
        map.insert(header::AUTHORIZATION, auth);
    }
    map
}

enum Attempt {
    Done(Result<ResolveResponse, ResolveFailure>),
    Retry(String),
}

async fn attempt(req: &ResolveRequest) -> Attempt {
    let target = match find_authority(&req.tenant_id).await {
        Ok(t) => t,
        Err(e) => return Attempt::Retry(e.to_string()),
    };
    let url = format!(
        "{}{}/{}",
        target.domain,
        base_path_without_preceding_slash(),
        RESOLVE_PATH
    );
    let response = INTRA_CLUSTER_CLIENT
        .post(url)
        .headers(headers(&target, &req.tenant_id))
        .timeout(REQUEST_TIMEOUT)
        .json(req)
        .send()
        .await;
    let response = match response {
        Ok(r) => r,
        Err(e) => {
            invalidate_authority();
            return Attempt::Retry(e.to_string());
        }
    };
    let status = response.status();
    let bytes = match response.bytes().await {
        Ok(b) => b,
        Err(e) => return Attempt::Retry(e.to_string()),
    };
    if status.is_success() {
        return Attempt::Done(
            serde_json::from_slice::<ResolveResponse>(&bytes)
                .map_err(|e| ResolveFailure::Rejected(format!("invalid response: {e}"))),
        );
    }
    match serde_json::from_slice::<ResolveErrorBody>(&bytes) {
        Ok(body) => match body.code {
            ErrorCode::StreamNotFound => Attempt::Done(Err(ResolveFailure::StreamNotFound)),
            ErrorCode::StreamDeleting => Attempt::Done(Err(ResolveFailure::StreamDeleting)),
            ErrorCode::UnsupportedProtocol => {
                Attempt::Done(Err(ResolveFailure::Rejected(UPGRADE_AUTHORITY.into())))
            }
            ErrorCode::BadRequest => Attempt::Done(Err(ResolveFailure::Rejected(body.message))),
            ErrorCode::Unavailable => Attempt::Retry(body.message),
        },
        // Not one of our error bodies: an authority without the route answers a bare 404.
        Err(_) => match status {
            StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED => {
                // Also a node that never serves resolution (an enterprise querier picked
                // before Prism registered): re-discover on the next attempt.
                invalidate_authority();
                Attempt::Done(Err(ResolveFailure::Rejected(UPGRADE_AUTHORITY.into())))
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Attempt::Done(Err(
                ResolveFailure::Rejected(format!("authority refused credentials ({status})")),
            )),
            _ => Attempt::Retry(format!("authority returned {status}")),
        },
    }
}

/// Resolves with a few short in-process retries. Cross-cycle retries are the caller's job.
pub async fn resolve(req: ResolveRequest) -> Result<ResolveResponse, ResolveFailure> {
    // Only a cluster-secret request carries a tenant other than the caller's own (the
    // authority takes it from `intra-cluster-tenant`); plain credentials resolve the
    // default tenant only.
    if CLUSTER_SECRET.get().is_none()
        && req
            .tenant_id
            .as_deref()
            .is_some_and(|t| t != crate::parseable::DEFAULT_TENANT)
    {
        return Err(ResolveFailure::Rejected(
            "schema resolution for a tenant stream requires P_CLUSTER_SECRET".into(),
        ));
    }
    let _permit = PERMITS
        .acquire()
        .await
        .map_err(|e| ResolveFailure::Transient(e.to_string()))?;
    let mut last = String::default();
    for n in 0..MAX_ATTEMPTS {
        match attempt(&req).await {
            Attempt::Done(result) => return result,
            Attempt::Retry(reason) => {
                warn!(
                    stream = req.stream,
                    "schema resolution attempt {} failed: {reason}",
                    n + 1
                );
                last = reason;
                if n + 1 < MAX_ATTEMPTS {
                    let jitter = rand::random::<u64>() % 100;
                    tokio::time::sleep(BACKOFF_BASE * 3u32.pow(n) + Duration::from_millis(jitter))
                        .await;
                }
            }
        }
    }
    Err(ResolveFailure::Transient(last))
}

/// Blocking wrapper for the conversion threads (which run on `spawn_blocking`).
pub fn resolve_blocking(req: ResolveRequest) -> Result<ResolveResponse, ResolveFailure> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    RESOLVER_RUNTIME.spawn(async move {
        let _ = tx.send(resolve(req).await);
    });
    rx.blocking_recv()
        .map_err(|_| ResolveFailure::Transient("resolver task dropped".into()))?
}
