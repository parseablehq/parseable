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

pub mod utils;

use futures::{StreamExt, future, stream};
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use actix_web::Responder;
use actix_web::http::header::{self, HeaderMap};
use actix_web::web::Path;
use bytes::Bytes;
use chrono::Utc;
use clokwerk::{AsyncScheduler, Interval};
use http::{StatusCode, header as http_header};
use itertools::Itertools;
use relative_path::RelativePathBuf;
use serde::de::{DeserializeOwned, Error};
use serde_json::error::Error as SerdeError;
use serde_json::{Value as JsonValue, to_vec};
use tracing::{error, info, warn};
use url::Url;
use utils::{IngestionStats, QueriedStats, StorageStats, check_liveness, to_url_string};

use crate::INTRA_CLUSTER_CLIENT;
use crate::handlers::http::ingest::ingest_internal_stream;
use crate::metrics::prom_utils::Metrics;
use crate::parseable::PARSEABLE;
use crate::rbac::role::model::DefaultPrivilege;
use crate::rbac::user::User;
use crate::stats::Stats;
use crate::storage::{
    ObjectStorage, ObjectStorageError, ObjectStoreFormat, PARSEABLE_ROOT_DIRECTORY,
    STREAM_ROOT_DIRECTORY,
};

use super::base_path_without_preceding_slash;
use super::ingest::PostError;
use super::logstream::error::StreamError;
use super::modal::{
    IndexerMetadata, IngestorMetadata, Metadata, NodeMetadata, NodeType, QuerierMetadata,
};
use super::rbac::RBACError;
use super::role::RoleError;

pub const INTERNAL_STREAM_NAME: &str = "pmeta";

const CLUSTER_METRICS_INTERVAL_SECONDS: Interval = clokwerk::Interval::Minutes(1);

pub async fn for_each_live_ingestor<F, Fut, E>(api_fn: F) -> Result<(), E>
where
    F: Fn(NodeMetadata) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send,
    E: From<anyhow::Error> + Send + Sync + 'static,
{
    let ingestor_infos: Vec<NodeMetadata> =
        get_node_info(NodeType::Ingestor).await.map_err(|err| {
            error!("Fatal: failed to get ingestor info: {:?}", err);
            E::from(err)
        })?;

    let mut live_ingestors = Vec::new();
    for ingestor in ingestor_infos {
        if utils::check_liveness(&ingestor.domain_name).await {
            live_ingestors.push(ingestor);
        } else {
            warn!("Ingestor {} is not live", ingestor.domain_name);
        }
    }

    // Process all live ingestors in parallel
    let results = futures::future::join_all(live_ingestors.into_iter().map(|ingestor| {
        let api_fn = api_fn.clone();
        async move { api_fn(ingestor).await }
    }))
    .await;

    // collect results
    for result in results {
        result?;
    }

    Ok(())
}

// forward the create/update stream request to all ingestors to keep them in sync
pub async fn sync_streams_with_ingestors(
    headers: HeaderMap,
    body: Bytes,
    stream_name: &str,
) -> Result<(), StreamError> {
    let mut reqwest_headers = http_header::HeaderMap::new();

    for (key, value) in headers.iter() {
        reqwest_headers.insert(key.clone(), value.clone());
    }

    let body_clone = body.clone();
    let stream_name = stream_name.to_string();
    let reqwest_headers_clone = reqwest_headers.clone();

    for_each_live_ingestor(
        move |ingestor| {
            let url = format!(
                "{}{}/logstream/{}/sync",
                ingestor.domain_name,
                base_path_without_preceding_slash(),
                stream_name
            );
            let headers = reqwest_headers_clone.clone();
            let body = body_clone.clone();
            async move {
                let res = INTRA_CLUSTER_CLIENT
                    .put(url)
                    .headers(headers)
                    .header(header::AUTHORIZATION, &ingestor.token)
                    .body(body)
                    .send()
                    .await
                    .map_err(|err| {
                        error!(
                            "Fatal: failed to forward upsert stream request to ingestor: {}\n Error: {:?}",
                            ingestor.domain_name, err
                        );
                        StreamError::Network(err)
                    })?;

                if !res.status().is_success() {
                    error!(
                        "failed to forward upsert stream request to ingestor: {}\nResponse Returned: {:?}",
                        ingestor.domain_name,
                        res.text().await
                    );
                }
                Ok(())
            }
        }
    ).await
}

// forward the demo data request to one of the live ingestor
pub async fn get_demo_data_from_ingestor(action: &str) -> Result<(), PostError> {
    let ingestor_infos: Vec<NodeMetadata> =
        get_node_info(NodeType::Ingestor).await.map_err(|err| {
            error!("Fatal: failed to get ingestor info: {:?}", err);
            PostError::Invalid(err)
        })?;

    let mut live_ingestors: Vec<NodeMetadata> = Vec::new();
    for ingestor in ingestor_infos {
        if utils::check_liveness(&ingestor.domain_name).await {
            live_ingestors.push(ingestor);
            break;
        }
    }

    if live_ingestors.is_empty() {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "No live ingestors found"
        )));
    }

    // Pick the first live ingestor
    let ingestor = &live_ingestors[0];

    let url = format!(
        "{}{}/demodata?action={action}",
        ingestor.domain_name,
        base_path_without_preceding_slash()
    );

    let res = INTRA_CLUSTER_CLIENT
        .get(url)
        .header(header::AUTHORIZATION, &ingestor.token)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await
        .map_err(|err| {
            error!(
                "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                ingestor.domain_name, err
            );
            PostError::Invalid(err.into())
        })?;

    if !res.status().is_success() {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "failed to forward request to ingestor: {}\nResponse status: {}",
            ingestor.domain_name,
            res.status()
        )));
    }

    Ok(())
}

// forward the role update request to all ingestors to keep them in sync
pub async fn sync_users_with_roles_with_ingestors(
    username: &str,
    role: &HashSet<String>,
    operation: &str,
) -> Result<(), RBACError> {
    match operation {
        "add" | "remove" => {}
        _ => return Err(RBACError::InvalidSyncOperation(operation.to_string())),
    }

    let role_data = to_vec(&role.clone()).map_err(|err| {
        error!("Fatal: failed to serialize role: {:?}", err);
        RBACError::SerdeError(err)
    })?;

    let username = username.to_owned();

    let op = operation.to_string();

    for_each_live_ingestor(move |ingestor| {
        let url = format!(
            "{}{}/user/{}/role/sync/{}",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username,
            op
        );

        let role_data = role_data.clone();

        async move {
            let res = INTRA_CLUSTER_CLIENT
                .put(url)
                .header(header::AUTHORIZATION, &ingestor.token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(role_data)
                .send()
                .await
                .map_err(|err| {
                    error!(
                        "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                        ingestor.domain_name, err
                    );
                    RBACError::Network(err)
                })?;

            if !res.status().is_success() {
                error!(
                    "failed to forward request to ingestor: {}\nResponse Returned: {:?}",
                    ingestor.domain_name,
                    res.text().await
                );
            }

            Ok(())
        }
    })
    .await
}

// forward the delete user request to all ingestors to keep them in sync
pub async fn sync_user_deletion_with_ingestors(username: &str) -> Result<(), RBACError> {
    let username = username.to_owned();

    for_each_live_ingestor(move |ingestor| {
        let url = format!(
            "{}{}/user/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        async move {
            let res = INTRA_CLUSTER_CLIENT
                .delete(url)
                .header(header::AUTHORIZATION, &ingestor.token)
                .send()
                .await
                .map_err(|err| {
                    error!(
                        "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                        ingestor.domain_name, err
                    );
                    RBACError::Network(err)
                })?;

            if !res.status().is_success() {
                error!(
                    "failed to forward request to ingestor: {}\nResponse Returned: {:?}",
                    ingestor.domain_name,
                    res.text().await
                );
            }

            Ok(())
        }
    })
    .await
}

// forward the create user request to all ingestors to keep them in sync
pub async fn sync_user_creation_with_ingestors(
    user: User,
    role: &Option<HashSet<String>>,
) -> Result<(), RBACError> {
    let mut user = user.clone();

    if let Some(role) = role {
        user.roles.clone_from(role);
    }
    let username = user.username();

    let user_data = to_vec(&user).map_err(|err| {
        error!("Fatal: failed to serialize user: {:?}", err);
        RBACError::SerdeError(err)
    })?;

    let username = username.to_string();

    for_each_live_ingestor(move |ingestor| {
        let url = format!(
            "{}{}/user/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let user_data = user_data.clone();

        async move {
            let res = INTRA_CLUSTER_CLIENT
                .post(url)
                .header(header::AUTHORIZATION, &ingestor.token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(user_data)
                .send()
                .await
                .map_err(|err| {
                    error!(
                        "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                        ingestor.domain_name, err
                    );
                    RBACError::Network(err)
                })?;

            if !res.status().is_success() {
                error!(
                    "failed to forward request to ingestor: {}\nResponse Returned: {:?}",
                    ingestor.domain_name,
                    res.text().await
                );
            }

            Ok(())
        }
    })
    .await
}

// forward the password reset request to all ingestors to keep them in sync
pub async fn sync_password_reset_with_ingestors(username: &str) -> Result<(), RBACError> {
    let username = username.to_owned();

    for_each_live_ingestor(move |ingestor| {
        let url = format!(
            "{}{}/user/{}/generate-new-password/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        async move {
            let res = INTRA_CLUSTER_CLIENT
                .post(url)
                .header(header::AUTHORIZATION, &ingestor.token)
                .header(header::CONTENT_TYPE, "application/json")
                .send()
                .await
                .map_err(|err| {
                    error!(
                        "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                        ingestor.domain_name, err
                    );
                    RBACError::Network(err)
                })?;

            if !res.status().is_success() {
                error!(
                    "failed to forward request to ingestor: {}\nResponse Returned: {:?}",
                    ingestor.domain_name,
                    res.text().await
                );
            }

            Ok(())
        }
    })
    .await
}

// forward the put role request to all ingestors to keep them in sync
pub async fn sync_role_update_with_ingestors(
    name: String,
    privileges: Vec<DefaultPrivilege>,
) -> Result<(), RoleError> {
    for_each_live_ingestor(move |ingestor| {
        let url = format!(
            "{}{}/role/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            name
        );

        let privileges = privileges.clone();

        async move {
            let res = INTRA_CLUSTER_CLIENT
                .put(url)
                .header(header::AUTHORIZATION, &ingestor.token)
                .header(header::CONTENT_TYPE, "application/json")
                .json(&privileges)
                .send()
                .await
                .map_err(|err| {
                    error!(
                        "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                        ingestor.domain_name, err
                    );
                    RoleError::Network(err)
                })?;

            if !res.status().is_success() {
                error!(
                    "failed to forward request to ingestor: {}\nResponse Returned: {:?}",
                    ingestor.domain_name,
                    res.text().await
                );
            }

            Ok(())
        }
    })
    .await
}

pub fn fetch_daily_stats(
    date: &str,
    stream_meta_list: &[ObjectStoreFormat],
) -> Result<Stats, StreamError> {
    // for the given date, get the stats from the ingestors
    let mut events_ingested = 0;
    let mut ingestion_size = 0;
    let mut storage_size = 0;

    for meta in stream_meta_list.iter() {
        for manifest in meta.snapshot.manifest_list.iter() {
            if manifest.time_lower_bound.date_naive().to_string() == date {
                events_ingested += manifest.events_ingested;
                ingestion_size += manifest.ingestion_size;
                storage_size += manifest.storage_size;
            }
        }
    }

    let stats = Stats {
        events: events_ingested,
        ingestion: ingestion_size,
        storage: storage_size,
    };
    Ok(stats)
}

/// get the cumulative stats from all ingestors
pub async fn fetch_stats_from_ingestors(
    stream_name: &str,
) -> Result<Vec<utils::QueriedStats>, StreamError> {
    let path = RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY]);
    let obs = PARSEABLE
        .storage
        .get_object_store()
        .get_objects(
            Some(&path),
            Box::new(|file_name| {
                file_name.starts_with(".ingestor") && file_name.ends_with("stream.json")
            }),
        )
        .await?;

    let mut ingestion_size = 0u64;
    let mut storage_size = 0u64;
    let mut count = 0u64;
    let mut lifetime_ingestion_size = 0u64;
    let mut lifetime_storage_size = 0u64;
    let mut lifetime_count = 0u64;
    let mut deleted_ingestion_size = 0u64;
    let mut deleted_storage_size = 0u64;
    let mut deleted_count = 0u64;
    for ob in obs {
        let stream_metadata: ObjectStoreFormat =
            serde_json::from_slice(&ob).expect("stream.json is valid json");

        count += stream_metadata.stats.current_stats.events;
        ingestion_size += stream_metadata.stats.current_stats.ingestion;
        storage_size += stream_metadata.stats.current_stats.storage;
        lifetime_count += stream_metadata.stats.lifetime_stats.events;
        lifetime_ingestion_size += stream_metadata.stats.lifetime_stats.ingestion;
        lifetime_storage_size += stream_metadata.stats.lifetime_stats.storage;
        deleted_count += stream_metadata.stats.deleted_stats.events;
        deleted_ingestion_size += stream_metadata.stats.deleted_stats.ingestion;
        deleted_storage_size += stream_metadata.stats.deleted_stats.storage;
    }

    let qs = QueriedStats::new(
        "",
        Utc::now(),
        IngestionStats::new(
            count,
            ingestion_size,
            lifetime_count,
            lifetime_ingestion_size,
            deleted_count,
            deleted_ingestion_size,
            "json",
        ),
        StorageStats::new(
            storage_size,
            lifetime_storage_size,
            deleted_storage_size,
            "parquet",
        ),
    );

    Ok(vec![qs])
}

/// send a delete stream request to all ingestors
pub async fn send_stream_delete_request(
    url: &str,
    ingestor: IngestorMetadata,
) -> Result<(), StreamError> {
    if !utils::check_liveness(&ingestor.domain_name).await {
        return Ok(());
    }
    let resp = INTRA_CLUSTER_CLIENT
        .delete(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, ingestor.token)
        .send()
        .await
        .map_err(|err| {
            // log the error and return a custom error
            error!(
                "Fatal: failed to delete stream: {}\n Error: {:?}",
                ingestor.domain_name, err
            );
            StreamError::Network(err)
        })?;

    // if the response is not successful, log the error and return a custom error
    // this could be a bit too much, but we need to be sure it covers all cases
    if !resp.status().is_success() {
        error!(
            "failed to delete stream: {}\nResponse Returned: {:?}",
            ingestor.domain_name,
            resp.text().await
        );
    }

    Ok(())
}

/// send a retention cleanup request to all ingestors
pub async fn send_retention_cleanup_request(
    url: &str,
    ingestor: IngestorMetadata,
    dates: &Vec<String>,
) -> Result<String, ObjectStorageError> {
    let mut first_event_at: String = String::default();
    if !utils::check_liveness(&ingestor.domain_name).await {
        return Ok(first_event_at);
    }
    let resp = INTRA_CLUSTER_CLIENT
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, ingestor.token)
        .json(dates)
        .send()
        .await
        .map_err(|err| {
            // log the error and return a custom error
            error!(
                "Fatal: failed to perform cleanup on retention: {}\n Error: {:?}",
                ingestor.domain_name, err
            );
            ObjectStorageError::Custom(err.to_string())
        })?;

    // if the response is not successful, log the error and return a custom error
    // this could be a bit too much, but we need to be sure it covers all cases
    if !resp.status().is_success() {
        error!(
            "failed to perform cleanup on retention: {}\nResponse Returned: {:?}",
            ingestor.domain_name,
            resp.status()
        );
    }

    let resp_data = resp.bytes().await.map_err(|err| {
        error!("Fatal: failed to parse response to bytes: {:?}", err);
        ObjectStorageError::Custom(err.to_string())
    })?;

    first_event_at = String::from_utf8_lossy(&resp_data).to_string();
    Ok(first_event_at)
}

/// Fetches cluster information for all nodes (ingestor, indexer, querier and prism)
pub async fn get_cluster_info() -> Result<impl Responder, StreamError> {
    // Get querier, ingestor and indexer metadata concurrently
    let (prism_result, querier_result, ingestor_result, indexer_result) = future::join4(
        get_node_info(NodeType::Prism),
        get_node_info(NodeType::Querier),
        get_node_info(NodeType::Ingestor),
        get_node_info(NodeType::Indexer),
    )
    .await;

    // Handle prism metadata result
    let prism_metadata: Vec<NodeMetadata> = prism_result
        .map_err(|err| {
            error!("Fatal: failed to get prism info: {:?}", err);
            PostError::Invalid(err)
        })
        .map_err(|err| StreamError::Anyhow(err.into()))?;

    // Handle querier metadata result
    let querier_metadata: Vec<NodeMetadata> = querier_result
        .map_err(|err| {
            error!("Fatal: failed to get querier info: {:?}", err);
            PostError::Invalid(err)
        })
        .map_err(|err| StreamError::Anyhow(err.into()))?;

    // Handle ingestor metadata result
    let ingestor_metadata: Vec<NodeMetadata> = ingestor_result
        .map_err(|err| {
            error!("Fatal: failed to get ingestor info: {:?}", err);
            PostError::Invalid(err)
        })
        .map_err(|err| StreamError::Anyhow(err.into()))?;

    // Handle indexer metadata result
    let indexer_metadata: Vec<NodeMetadata> = indexer_result
        .map_err(|err| {
            error!("Fatal: failed to get indexer info: {:?}", err);
            PostError::Invalid(err)
        })
        .map_err(|err| StreamError::Anyhow(err.into()))?;

    // Fetch info for all nodes concurrently
    let (prism_infos, querier_infos, ingestor_infos, indexer_infos) = future::join4(
        fetch_nodes_info(prism_metadata),
        fetch_nodes_info(querier_metadata),
        fetch_nodes_info(ingestor_metadata),
        fetch_nodes_info(indexer_metadata),
    )
    .await;

    // Combine results from all node types
    let mut infos = Vec::new();
    infos.extend(prism_infos?);
    infos.extend(querier_infos?);
    infos.extend(ingestor_infos?);
    infos.extend(indexer_infos?);
    Ok(actix_web::HttpResponse::Ok().json(infos))
}

/// Fetches info for a single node
/// call the about endpoint of the node
/// construct the ClusterInfo struct and return it
async fn fetch_node_info<T: Metadata>(node: &T) -> Result<utils::ClusterInfo, StreamError> {
    let uri = Url::parse(&format!(
        "{}{}/about",
        node.domain_name(),
        base_path_without_preceding_slash()
    ))
    .expect("should always be a valid url");

    let resp = INTRA_CLUSTER_CLIENT
        .get(uri)
        .header(header::AUTHORIZATION, node.token().to_owned())
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await;

    let (reachable, staging_path, error, status) = if let Ok(resp) = resp {
        let status = Some(resp.status().to_string());

        let resp_data = resp.bytes().await.map_err(|err| {
            error!("Fatal: failed to parse node info to bytes: {:?}", err);
            StreamError::Network(err)
        })?;

        let sp = serde_json::from_slice::<JsonValue>(&resp_data)
            .map_err(|err| {
                error!("Fatal: failed to parse node info: {:?}", err);
                StreamError::SerdeError(err)
            })?
            .get("staging")
            .ok_or(StreamError::SerdeError(SerdeError::missing_field(
                "staging",
            )))?
            .as_str()
            .ok_or(StreamError::SerdeError(SerdeError::custom(
                "staging path not a string/ not provided",
            )))?
            .to_string();

        (true, sp, None, status)
    } else {
        (
            false,
            "".to_owned(),
            resp.as_ref().err().map(|e| e.to_string()),
            resp.unwrap_err().status().map(|s| s.to_string()),
        )
    };

    Ok(utils::ClusterInfo::new(
        node.domain_name(),
        reachable,
        staging_path,
        PARSEABLE.storage.get_endpoint(),
        error,
        status,
        node.node_type(),
    ))
}

/// Fetches info for multiple nodes in parallel
async fn fetch_nodes_info<T: Metadata>(
    nodes: Vec<T>,
) -> Result<Vec<utils::ClusterInfo>, StreamError> {
    let nodes_len = nodes.len();
    if nodes_len == 0 {
        return Ok(vec![]);
    }
    let results = stream::iter(nodes)
        .map(|node| async move { fetch_node_info(&node).await })
        .buffer_unordered(nodes_len) // No concurrency limit
        .collect::<Vec<_>>()
        .await;

    // Collect results, propagating any errors
    let mut infos = Vec::with_capacity(results.len());
    for result in results {
        infos.push(result?);
    }

    Ok(infos)
}

pub async fn get_cluster_metrics() -> Result<impl Responder, PostError> {
    let dresses = fetch_cluster_metrics().await.map_err(|err| {
        error!("Fatal: failed to fetch cluster metrics: {:?}", err);
        PostError::Invalid(err.into())
    })?;

    Ok(actix_web::HttpResponse::Ok().json(dresses))
}

/// get node info for a specific node type
/// this is used to get the node info for ingestor, indexer, querier and prism
/// it will return the metadata for all nodes of that type
pub async fn get_node_info<T: Metadata + DeserializeOwned>(
    node_type: NodeType,
) -> anyhow::Result<Vec<T>> {
    let store = PARSEABLE.storage.get_object_store();
    let root_path = RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY);
    let prefix_owned = node_type.to_string();

    let metadata = store
        .get_objects(
            Some(&root_path),
            Box::new(move |file_name| file_name.starts_with(&prefix_owned)), // Use the owned copy
        )
        .await?
        .iter()
        .filter_map(|x| match serde_json::from_slice::<T>(x) {
            Ok(val) => Some(val),
            Err(e) => {
                error!("Failed to parse node metadata: {:?}", e);
                None
            }
        })
        .collect();

    Ok(metadata)
}
/// remove a node from the cluster
/// check liveness of the node
/// if the node is live, return an error
/// if the node is not live, remove the node from the cluster
/// remove the node metadata from the object store
pub async fn remove_node(node_url: Path<String>) -> Result<impl Responder, PostError> {
    let domain_name = to_url_string(node_url.into_inner());

    if check_liveness(&domain_name).await {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "The node is currently live and cannot be removed"
        )));
    }
    let object_store = PARSEABLE.storage.get_object_store();

    // Delete ingestor metadata
    let removed_ingestor =
        remove_node_metadata::<IngestorMetadata>(&object_store, &domain_name, NodeType::Ingestor)
            .await?;

    // Delete indexer metadata
    let removed_indexer =
        remove_node_metadata::<IndexerMetadata>(&object_store, &domain_name, NodeType::Indexer)
            .await?;

    // Delete querier metadata
    let removed_querier =
        remove_node_metadata::<QuerierMetadata>(&object_store, &domain_name, NodeType::Querier)
            .await?;

    // Delete prism metadata
    let removed_prism =
        remove_node_metadata::<NodeMetadata>(&object_store, &domain_name, NodeType::Prism).await?;

    if removed_ingestor || removed_indexer || removed_querier || removed_prism {
        return Ok((
            format!("node {domain_name} removed successfully"),
            StatusCode::OK,
        ));
    }
    Err(PostError::Invalid(anyhow::anyhow!(
        "node {domain_name} not found"
    )))
}

/// Removes node metadata from the object store
/// Returns true if the metadata was removed, false if it was not found
async fn remove_node_metadata<T: Metadata + DeserializeOwned + Default>(
    object_store: &Arc<dyn ObjectStorage>,
    domain_name: &str,
    node_type: NodeType,
) -> Result<bool, PostError> {
    let metadatas = object_store
        .get_objects(
            Some(&RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY)),
            Box::new(move |file_name| file_name.starts_with(&node_type.to_string())),
        )
        .await?;

    let node_metadatas = metadatas
        .iter()
        .filter_map(|elem| match serde_json::from_slice::<T>(elem) {
            Ok(meta) if meta.domain_name() == domain_name => Some(meta),
            _ => None,
        })
        .collect::<Vec<_>>();

    if node_metadatas.is_empty() {
        return Ok(false);
    }

    let node_meta_filename = node_metadatas[0].file_path().to_string();
    match object_store.try_delete_node_meta(node_meta_filename).await {
        Ok(_) => Ok(true),
        Err(err) => {
            if matches!(err, ObjectStorageError::IoError(_)) {
                Ok(false)
            } else {
                Err(PostError::ObjectStorageError(err))
            }
        }
    }
}

/// Fetches metrics for a single node
/// This function is used to fetch metrics from a single node
/// It checks if the node is live and then fetches the metrics
/// If the node is not live, it returns None
async fn fetch_node_metrics<T>(node: &T) -> Result<Option<Metrics>, PostError>
where
    T: Metadata + Send + Sync + 'static,
{
    // Format the metrics URL
    let uri = Url::parse(&format!(
        "{}{}/metrics",
        node.domain_name(),
        base_path_without_preceding_slash()
    ))
    .map_err(|err| PostError::Invalid(anyhow::anyhow!("Invalid URL in node metadata: {}", err)))?;

    // Check if the node is live
    if !check_liveness(node.domain_name()).await {
        warn!("node {} is not live", node.domain_name());
        return Ok(None);
    }

    // Fetch metrics
    let res = INTRA_CLUSTER_CLIENT
        .get(uri)
        .header(header::AUTHORIZATION, node.token())
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await;

    match res {
        Ok(res) => {
            let text = res.text().await.map_err(PostError::NetworkError)?;
            let lines: Vec<Result<String, std::io::Error>> =
                text.lines().map(|line| Ok(line.to_owned())).collect_vec();

            let sample = prometheus_parse::Scrape::parse(lines.into_iter())
                .map_err(|err| PostError::CustomError(err.to_string()))?
                .samples;

            let metrics = Metrics::from_prometheus_samples(sample, node)
                .await
                .map_err(|err| {
                    error!("Fatal: failed to get node metrics: {:?}", err);
                    PostError::Invalid(err.into())
                })?;

            Ok(Some(metrics))
        }
        Err(_) => {
            warn!(
                "Failed to fetch metrics from node: {}\n",
                node.domain_name()
            );
            Ok(None)
        }
    }
}

/// Fetches metrics from multiple nodes in parallel
async fn fetch_nodes_metrics<T>(nodes: Vec<T>) -> Result<Vec<Metrics>, PostError>
where
    T: Metadata + Send + Sync + 'static,
{
    let nodes_len = nodes.len();
    if nodes_len == 0 {
        return Ok(vec![]);
    }
    let results = stream::iter(nodes)
        .map(|node| async move { fetch_node_metrics(&node).await })
        .buffer_unordered(nodes_len) // No concurrency limit
        .collect::<Vec<_>>()
        .await;

    // Process results
    let mut metrics = Vec::new();
    for result in results {
        match result {
            Ok(Some(node_metrics)) => metrics.push(node_metrics),
            Ok(_) => {} // node was not live or metrics couldn't be fetched
            Err(err) => return Err(err),
        }
    }

    Ok(metrics)
}

/// Main function to fetch cluster metrics
/// fetches node info for all nodes
/// fetches metrics for all nodes
/// combines all metrics into a single vector
async fn fetch_cluster_metrics() -> Result<Vec<Metrics>, PostError> {
    // Get ingestor and indexer metadata concurrently
    let (prism_result, querier_result, ingestor_result, indexer_result) = future::join4(
        get_node_info(NodeType::Prism),
        get_node_info(NodeType::Querier),
        get_node_info(NodeType::Ingestor),
        get_node_info(NodeType::Indexer),
    )
    .await;

    // Handle prism metadata result
    let prism_metadata: Vec<NodeMetadata> = prism_result.map_err(|err| {
        error!("Fatal: failed to get prism info: {:?}", err);
        PostError::Invalid(err)
    })?;

    // Handle querier metadata result
    let querier_metadata: Vec<NodeMetadata> = querier_result.map_err(|err| {
        error!("Fatal: failed to get querier info: {:?}", err);
        PostError::Invalid(err)
    })?;
    // Handle ingestor metadata result
    let ingestor_metadata: Vec<NodeMetadata> = ingestor_result.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        PostError::Invalid(err)
    })?;
    // Handle indexer metadata result
    let indexer_metadata: Vec<NodeMetadata> = indexer_result.map_err(|err| {
        error!("Fatal: failed to get indexer info: {:?}", err);
        PostError::Invalid(err)
    })?;
    // Fetch metrics from ingestors and indexers concurrently
    let (prism_metrics, querier_metrics, ingestor_metrics, indexer_metrics) = future::join4(
        fetch_nodes_metrics(prism_metadata),
        fetch_nodes_metrics(querier_metadata),
        fetch_nodes_metrics(ingestor_metadata),
        fetch_nodes_metrics(indexer_metadata),
    )
    .await;

    // Combine all metrics
    let mut all_metrics = Vec::new();

    // Add prism metrics
    match prism_metrics {
        Ok(metrics) => all_metrics.extend(metrics),
        Err(err) => return Err(err),
    }

    // Add querier metrics
    match querier_metrics {
        Ok(metrics) => all_metrics.extend(metrics),
        Err(err) => return Err(err),
    }

    // Add ingestor metrics
    match ingestor_metrics {
        Ok(metrics) => all_metrics.extend(metrics),
        Err(err) => return Err(err),
    }

    // Add indexer metrics
    match indexer_metrics {
        Ok(metrics) => all_metrics.extend(metrics),
        Err(err) => return Err(err),
    }

    Ok(all_metrics)
}

pub fn init_cluster_metrics_schedular() -> Result<(), PostError> {
    info!("Setting up schedular for cluster metrics ingestion");
    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(CLUSTER_METRICS_INTERVAL_SECONDS)
        .run(move || async {
            let result: Result<(), PostError> = async {
                let cluster_metrics = fetch_cluster_metrics().await;
                if let Ok(metrics) = cluster_metrics {
                    if !metrics.is_empty() {
                        info!("Cluster metrics fetched successfully from all ingestors");
                        if let Ok(metrics_bytes) = serde_json::to_vec(&metrics) {
                            if matches!(
                                ingest_internal_stream(
                                    INTERNAL_STREAM_NAME.to_string(),
                                    bytes::Bytes::from(metrics_bytes),
                                )
                                .await,
                                Ok(())
                            ) {
                                info!("Cluster metrics successfully ingested into internal stream");
                            } else {
                                error!("Failed to ingest cluster metrics into internal stream");
                            }
                        } else {
                            error!("Failed to serialize cluster metrics");
                        }
                    }
                }
                Ok(())
            }
            .await;

            if let Err(err) = result {
                error!("Error in cluster metrics scheduler: {:?}", err);
            }
        });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    Ok(())
}
