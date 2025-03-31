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

use futures::{future, stream, StreamExt};
use std::collections::HashSet;
use std::time::Duration;

use actix_web::http::header::{self, HeaderMap};
use actix_web::web::Path;
use actix_web::Responder;
use bytes::Bytes;
use chrono::Utc;
use clokwerk::{AsyncScheduler, Interval};
use http::{header as http_header, StatusCode};
use itertools::Itertools;
use relative_path::RelativePathBuf;
use serde::de::Error;
use serde_json::error::Error as SerdeError;
use serde_json::{to_vec, Value as JsonValue};
use tracing::{error, info, warn};
use url::Url;
use utils::{check_liveness, to_url_string, IngestionStats, QueriedStats, StorageStats};

use crate::handlers::http::ingest::ingest_internal_stream;
use crate::metrics::prom_utils::Metrics;
use crate::parseable::PARSEABLE;
use crate::rbac::role::model::DefaultPrivilege;
use crate::rbac::user::User;
use crate::stats::Stats;
use crate::storage::{
    ObjectStorageError, ObjectStoreFormat, PARSEABLE_ROOT_DIRECTORY, STREAM_ROOT_DIRECTORY,
};
use crate::HTTP_CLIENT;

use super::base_path_without_preceding_slash;
use super::ingest::PostError;
use super::logstream::error::StreamError;
use super::modal::{IndexerMetadata, IngestorMetadata, Metadata};
use super::rbac::RBACError;
use super::role::RoleError;

type IngestorMetadataArr = Vec<IngestorMetadata>;

type IndexerMetadataArr = Vec<IndexerMetadata>;

pub const INTERNAL_STREAM_NAME: &str = "pmeta";

const CLUSTER_METRICS_INTERVAL_SECONDS: Interval = clokwerk::Interval::Minutes(1);

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
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        StreamError::Anyhow(err)
    })?;

    for ingestor in ingestor_infos {
        if !utils::check_liveness(&ingestor.domain_name).await {
            warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/logstream/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            stream_name
        );
        let res = HTTP_CLIENT
            .put(url)
            .headers(reqwest_headers.clone())
            .header(header::AUTHORIZATION, &ingestor.token)
            .body(body.clone())
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
    }

    Ok(())
}

// forward the role update request to all ingestors to keep them in sync
pub async fn sync_users_with_roles_with_ingestors(
    username: &String,
    role: &HashSet<String>,
) -> Result<(), RBACError> {
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        RBACError::Anyhow(err)
    })?;

    let role = to_vec(&role.clone()).map_err(|err| {
        error!("Fatal: failed to serialize role: {:?}", err);
        RBACError::SerdeError(err)
    })?;
    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/user/{}/role/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let res = HTTP_CLIENT
            .put(url)
            .header(header::AUTHORIZATION, &ingestor.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(role.clone())
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
    }

    Ok(())
}

// forward the delete user request to all ingestors to keep them in sync
pub async fn sync_user_deletion_with_ingestors(username: &String) -> Result<(), RBACError> {
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        RBACError::Anyhow(err)
    })?;

    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/user/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let res = HTTP_CLIENT
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
    }

    Ok(())
}

// forward the create user request to all ingestors to keep them in sync
pub async fn sync_user_creation_with_ingestors(
    user: User,
    role: &Option<HashSet<String>>,
) -> Result<(), RBACError> {
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        RBACError::Anyhow(err)
    })?;

    let mut user = user.clone();

    if let Some(role) = role {
        user.roles.clone_from(role);
    }
    let username = user.username();

    let user = to_vec(&user).map_err(|err| {
        error!("Fatal: failed to serialize user: {:?}", err);
        RBACError::SerdeError(err)
    })?;

    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/user/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let res = HTTP_CLIENT
            .post(url)
            .header(header::AUTHORIZATION, &ingestor.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(user.clone())
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
    }

    Ok(())
}

// forward the password reset request to all ingestors to keep them in sync
pub async fn sync_password_reset_with_ingestors(username: &String) -> Result<(), RBACError> {
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        RBACError::Anyhow(err)
    })?;

    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/user/{}/generate-new-password/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let res = HTTP_CLIENT
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
    }

    Ok(())
}

// forward the put role request to all ingestors to keep them in sync
pub async fn sync_role_update_with_ingestors(
    name: String,
    privileges: Vec<DefaultPrivilege>,
) -> Result<(), RoleError> {
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        RoleError::Anyhow(err)
    })?;

    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/role/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            name
        );

        let res = HTTP_CLIENT
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
    }

    Ok(())
}

pub fn fetch_daily_stats_from_ingestors(
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
    let resp = HTTP_CLIENT
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
    let resp = HTTP_CLIENT
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

pub async fn get_cluster_info() -> Result<impl Responder, StreamError> {
    // Get ingestor and indexer metadata concurrently
    let (ingestor_result, indexer_result) =
        future::join(get_ingestor_info(), get_indexer_info()).await;

    // Handle ingestor metadata result
    let ingestor_metadata = ingestor_result
        .map_err(|err| {
            error!("Fatal: failed to get ingestor info: {:?}", err);
            PostError::Invalid(err)
        })
        .map_err(|err| StreamError::Anyhow(err.into()))?;

    // Handle indexer metadata result
    let indexer_metadata = indexer_result
        .map_err(|err| {
            error!("Fatal: failed to get indexer info: {:?}", err);
            PostError::Invalid(err)
        })
        .map_err(|err| StreamError::Anyhow(err.into()))?;

    // Fetch info for both node types concurrently
    let (ingestor_infos, indexer_infos) = future::join(
        fetch_nodes_info(ingestor_metadata),
        fetch_nodes_info(indexer_metadata),
    )
    .await;

    // Combine results from both node types
    let mut infos = Vec::new();
    infos.extend(ingestor_infos?);
    infos.extend(indexer_infos?);

    Ok(actix_web::HttpResponse::Ok().json(infos))
}

/// Fetches info for a single node (ingestor or indexer)
async fn fetch_node_info<T: Metadata>(node: &T) -> Result<utils::ClusterInfo, StreamError> {
    let uri = Url::parse(&format!(
        "{}{}/about",
        node.domain_name(),
        base_path_without_preceding_slash()
    ))
    .expect("should always be a valid url");

    let resp = HTTP_CLIENT
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

pub async fn get_ingestor_info() -> anyhow::Result<IngestorMetadataArr> {
    let store = PARSEABLE.storage.get_object_store();

    let root_path = RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY);
    let arr = store
        .get_objects(
            Some(&root_path),
            Box::new(|file_name| file_name.starts_with("ingestor")),
        )
        .await?
        .iter()
        // this unwrap will most definateley shoot me in the foot later
        .map(|x| serde_json::from_slice::<IngestorMetadata>(x).unwrap_or_default())
        .collect_vec();

    Ok(arr)
}

pub async fn get_indexer_info() -> anyhow::Result<IndexerMetadataArr> {
    let store = PARSEABLE.storage.get_object_store();

    let root_path = RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY);
    let arr = store
        .get_objects(
            Some(&root_path),
            Box::new(|file_name| file_name.starts_with("indexer")),
        )
        .await?
        .iter()
        // this unwrap will most definateley shoot me in the foot later
        .map(|x| serde_json::from_slice::<IndexerMetadata>(x).unwrap_or_default())
        .collect_vec();

    Ok(arr)
}

pub async fn remove_ingestor(ingestor: Path<String>) -> Result<impl Responder, PostError> {
    let domain_name = to_url_string(ingestor.into_inner());

    if check_liveness(&domain_name).await {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "The ingestor is currently live and cannot be removed"
        )));
    }
    let object_store = PARSEABLE.storage.get_object_store();

    let ingestor_metadatas = object_store
        .get_objects(
            Some(&RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY)),
            Box::new(|file_name| file_name.starts_with("ingestor")),
        )
        .await?;

    let ingestor_metadata = ingestor_metadatas
        .iter()
        .map(|elem| serde_json::from_slice::<IngestorMetadata>(elem).unwrap_or_default())
        .collect_vec();

    let ingestor_metadata = ingestor_metadata
        .iter()
        .filter(|elem| elem.domain_name == domain_name)
        .collect_vec();

    let ingestor_meta_filename = ingestor_metadata[0].file_path().to_string();
    let msg = match object_store
        .try_delete_ingestor_meta(ingestor_meta_filename)
        .await
    {
        Ok(_) => {
            format!("Ingestor {} removed successfully", domain_name)
        }
        Err(err) => {
            if matches!(err, ObjectStorageError::IoError(_)) {
                format!("Ingestor {} is not found", domain_name)
            } else {
                format!("Error removing ingestor {}\n Reason: {}", domain_name, err)
            }
        }
    };

    info!("{}", &msg);
    Ok((msg, StatusCode::OK))
}

/// Fetches metrics from a node (ingestor or indexer)
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
    let res = HTTP_CLIENT
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
            Ok(None) => {} // node was not live or metrics couldn't be fetched
            Err(err) => return Err(err),
        }
    }

    Ok(metrics)
}

/// Main function to fetch all cluster metrics, parallelized and refactored
async fn fetch_cluster_metrics() -> Result<Vec<Metrics>, PostError> {
    // Get ingestor and indexer metadata concurrently
    let (ingestor_result, indexer_result) =
        future::join(get_ingestor_info(), get_indexer_info()).await;

    // Handle ingestor metadata result
    let ingestor_metadata = ingestor_result.map_err(|err| {
        error!("Fatal: failed to get ingestor info: {:?}", err);
        PostError::Invalid(err)
    })?;

    // Handle indexer metadata result
    let indexer_metadata = indexer_result.map_err(|err| {
        error!("Fatal: failed to get indexer info: {:?}", err);
        PostError::Invalid(err)
    })?;

    // Fetch metrics from ingestors and indexers concurrently
    let (ingestor_metrics, indexer_metrics) = future::join(
        fetch_nodes_metrics(ingestor_metadata),
        fetch_nodes_metrics(indexer_metadata),
    )
    .await;

    // Combine all metrics
    let mut all_metrics = Vec::new();

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
