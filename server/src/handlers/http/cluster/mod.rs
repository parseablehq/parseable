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

use crate::handlers::http::cluster::utils::{
    check_liveness, to_url_string, IngestionStats, QueriedStats,
};
use crate::handlers::http::ingest::{ingest_internal_stream, PostError};
use crate::handlers::http::logstream::error::StreamError;
use crate::handlers::http::role::RoleError;
use crate::option::CONFIG;

use crate::metrics::prom_utils::Metrics;
use crate::rbac::role::model::DefaultPrivilege;
use crate::rbac::user::User;
use crate::stats::Stats;
use crate::storage::object_storage::ingestor_metadata_path;
use crate::storage::{ObjectStorageError, STREAM_ROOT_DIRECTORY};
use crate::storage::{ObjectStoreFormat, PARSEABLE_ROOT_DIRECTORY};
use actix_web::http::header::{self, HeaderMap};
use actix_web::{HttpRequest, Responder};
use bytes::Bytes;
use chrono::Utc;
use http::{header as http_header, StatusCode};
use itertools::Itertools;
use relative_path::RelativePathBuf;
use serde::de::Error;
use serde_json::error::Error as SerdeError;
use serde_json::{to_vec, Value as JsonValue};
use url::Url;
type IngestorMetadataArr = Vec<IngestorMetadata>;

use self::utils::StorageStats;

use super::base_path_without_preceding_slash;
use super::rbac::RBACError;
use std::collections::HashSet;
use std::time::Duration;

use super::modal::IngestorMetadata;
use clokwerk::{AsyncScheduler, Interval};
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
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        StreamError::Anyhow(err)
    })?;

    let client = reqwest::Client::new();

    for ingestor in ingestor_infos {
        if !utils::check_liveness(&ingestor.domain_name).await {
            log::warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/logstream/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            stream_name
        );
        let res = client
            .put(url)
            .headers(reqwest_headers.clone())
            .header(header::AUTHORIZATION, &ingestor.token)
            .body(body.clone())
            .send()
            .await
            .map_err(|err| {
                log::error!(
                    "Fatal: failed to forward upsert stream request to ingestor: {}\n Error: {:?}",
                    ingestor.domain_name,
                    err
                );
                StreamError::Network(err)
            })?;

        if !res.status().is_success() {
            log::error!(
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
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        RBACError::Anyhow(err)
    })?;

    let client = reqwest::Client::new();
    let role = to_vec(&role.clone()).map_err(|err| {
        log::error!("Fatal: failed to serialize role: {:?}", err);
        RBACError::SerdeError(err)
    })?;
    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            log::warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/user/{}/role/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let res = client
            .put(url)
            .header(header::AUTHORIZATION, &ingestor.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(role.clone())
            .send()
            .await
            .map_err(|err| {
                log::error!(
                    "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                    ingestor.domain_name,
                    err
                );
                RBACError::Network(err)
            })?;

        if !res.status().is_success() {
            log::error!(
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
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        RBACError::Anyhow(err)
    })?;

    let client = reqwest::Client::new();
    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            log::warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/user/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let res = client
            .delete(url)
            .header(header::AUTHORIZATION, &ingestor.token)
            .send()
            .await
            .map_err(|err| {
                log::error!(
                    "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                    ingestor.domain_name,
                    err
                );
                RBACError::Network(err)
            })?;

        if !res.status().is_success() {
            log::error!(
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
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        RBACError::Anyhow(err)
    })?;

    let mut user = user.clone();

    if let Some(role) = role {
        user.roles.clone_from(role);
    }
    let username = user.username();
    let client = reqwest::Client::new();

    let user = to_vec(&user).map_err(|err| {
        log::error!("Fatal: failed to serialize user: {:?}", err);
        RBACError::SerdeError(err)
    })?;

    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            log::warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/user/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let res = client
            .post(url)
            .header(header::AUTHORIZATION, &ingestor.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(user.clone())
            .send()
            .await
            .map_err(|err| {
                log::error!(
                    "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                    ingestor.domain_name,
                    err
                );
                RBACError::Network(err)
            })?;

        if !res.status().is_success() {
            log::error!(
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
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        RBACError::Anyhow(err)
    })?;
    let client = reqwest::Client::new();

    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            log::warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/user/{}/generate-new-password/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            username
        );

        let res = client
            .post(url)
            .header(header::AUTHORIZATION, &ingestor.token)
            .header(header::CONTENT_TYPE, "application/json")
            .send()
            .await
            .map_err(|err| {
                log::error!(
                    "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                    ingestor.domain_name,
                    err
                );
                RBACError::Network(err)
            })?;

        if !res.status().is_success() {
            log::error!(
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
    body: Vec<DefaultPrivilege>,
) -> Result<(), RoleError> {
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        RoleError::Anyhow(err)
    })?;

    let roles = to_vec(&body).map_err(|err| {
        log::error!("Fatal: failed to serialize roles: {:?}", err);
        RoleError::SerdeError(err)
    })?;
    let roles = Bytes::from(roles);
    let client = reqwest::Client::new();

    for ingestor in ingestor_infos.iter() {
        if !utils::check_liveness(&ingestor.domain_name).await {
            log::warn!("Ingestor {} is not live", ingestor.domain_name);
            continue;
        }
        let url = format!(
            "{}{}/role/{}/sync",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            name
        );

        let res = client
            .put(url)
            .header(header::AUTHORIZATION, &ingestor.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(roles.clone())
            .send()
            .await
            .map_err(|err| {
                log::error!(
                    "Fatal: failed to forward request to ingestor: {}\n Error: {:?}",
                    ingestor.domain_name,
                    err
                );
                RoleError::Network(err)
            })?;

        if !res.status().is_success() {
            log::error!(
                "failed to forward request to ingestor: {}\nResponse Returned: {:?}",
                ingestor.domain_name,
                res.text().await
            );
        }
    }

    Ok(())
}

pub async fn fetch_daily_stats_from_ingestors(
    stream_name: &str,
    date: &str,
) -> Result<Stats, StreamError> {
    let mut total_events_ingested: u64 = 0;
    let mut total_ingestion_size: u64 = 0;
    let mut total_storage_size: u64 = 0;

    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        StreamError::Anyhow(err)
    })?;
    for ingestor in ingestor_infos.iter() {
        let uri = Url::parse(&format!(
            "{}{}/metrics",
            &ingestor.domain_name,
            base_path_without_preceding_slash()
        ))
        .map_err(|err| {
            StreamError::Anyhow(anyhow::anyhow!("Invalid URL in Ingestor Metadata: {}", err))
        })?;

        let res = reqwest::Client::new()
            .get(uri)
            .header(header::AUTHORIZATION, &ingestor.token)
            .header(header::CONTENT_TYPE, "application/json")
            .send()
            .await;

        if let Ok(res) = res {
            let text = res
                .text()
                .await
                .map_err(|err| StreamError::Anyhow(anyhow::anyhow!("Request failed: {}", err)))?;
            let lines: Vec<Result<String, std::io::Error>> =
                text.lines().map(|line| Ok(line.to_owned())).collect_vec();

            let sample = prometheus_parse::Scrape::parse(lines.into_iter())
                .map_err(|err| {
                    StreamError::Anyhow(anyhow::anyhow!(
                        "Invalid URL in Ingestor Metadata: {}",
                        err
                    ))
                })?
                .samples;

            let (events_ingested, ingestion_size, storage_size) =
                Metrics::get_daily_stats_from_samples(sample, stream_name, date);
            total_events_ingested += events_ingested;
            total_ingestion_size += ingestion_size;
            total_storage_size += storage_size;
        }
    }

    let stats = Stats {
        events: total_events_ingested,
        ingestion: total_ingestion_size,
        storage: total_storage_size,
    };
    Ok(stats)
}

/// get the cumulative stats from all ingestors
pub async fn fetch_stats_from_ingestors(
    stream_name: &str,
) -> Result<Vec<utils::QueriedStats>, StreamError> {
    let path = RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY]);
    let obs = CONFIG
        .storage()
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
            format!("{} Bytes", ingestion_size),
            lifetime_count,
            format!("{} Bytes", lifetime_ingestion_size),
            deleted_count,
            format!("{} Bytes", deleted_ingestion_size),
            "json",
        ),
        StorageStats::new(
            format!("{} Bytes", storage_size),
            format!("{} Bytes", lifetime_storage_size),
            format!("{} Bytes", deleted_storage_size),
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
    let client = reqwest::Client::new();
    let resp = client
        .delete(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, ingestor.token)
        .send()
        .await
        .map_err(|err| {
            // log the error and return a custom error
            log::error!(
                "Fatal: failed to delete stream: {}\n Error: {:?}",
                ingestor.domain_name,
                err
            );
            StreamError::Network(err)
        })?;

    // if the response is not successful, log the error and return a custom error
    // this could be a bit too much, but we need to be sure it covers all cases
    if !resp.status().is_success() {
        log::error!(
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
    body: Bytes,
) -> Result<String, ObjectStorageError> {
    let mut first_event_at: String = String::default();
    if !utils::check_liveness(&ingestor.domain_name).await {
        return Ok(first_event_at);
    }
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, ingestor.token)
        .body(body)
        .send()
        .await
        .map_err(|err| {
            // log the error and return a custom error
            log::error!(
                "Fatal: failed to perform cleanup on retention: {}\n Error: {:?}",
                ingestor.domain_name,
                err
            );
            ObjectStorageError::Custom(err.to_string())
        })?;

    // if the response is not successful, log the error and return a custom error
    // this could be a bit too much, but we need to be sure it covers all cases
    if !resp.status().is_success() {
        log::error!(
            "failed to perform cleanup on retention: {}\nResponse Returned: {:?}",
            ingestor.domain_name,
            resp.status()
        );
    }

    let resp_data = resp.bytes().await.map_err(|err| {
        log::error!("Fatal: failed to parse response to bytes: {:?}", err);
        ObjectStorageError::Custom(err.to_string())
    })?;

    first_event_at = String::from_utf8_lossy(&resp_data).to_string();
    Ok(first_event_at)
}

pub async fn get_cluster_info() -> Result<impl Responder, StreamError> {
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        StreamError::Anyhow(err)
    })?;

    let mut infos = vec![];

    for ingestor in ingestor_infos {
        let uri = Url::parse(&format!(
            "{}{}/about",
            ingestor.domain_name,
            base_path_without_preceding_slash()
        ))
        .expect("should always be a valid url");

        let resp = reqwest::Client::new()
            .get(uri)
            .header(header::AUTHORIZATION, ingestor.token.clone())
            .header(header::CONTENT_TYPE, "application/json")
            .send()
            .await;

        let (reachable, staging_path, error, status) = if let Ok(resp) = resp {
            let status = Some(resp.status().to_string());

            let resp_data = resp.bytes().await.map_err(|err| {
                log::error!("Fatal: failed to parse ingestor info to bytes: {:?}", err);
                StreamError::Network(err)
            })?;

            let sp = serde_json::from_slice::<JsonValue>(&resp_data)
                .map_err(|err| {
                    log::error!("Fatal: failed to parse ingestor info: {:?}", err);
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

        infos.push(utils::ClusterInfo::new(
            &ingestor.domain_name,
            reachable,
            staging_path,
            CONFIG.storage().get_endpoint(),
            error,
            status,
        ));
    }

    Ok(actix_web::HttpResponse::Ok().json(infos))
}

pub async fn get_cluster_metrics() -> Result<impl Responder, PostError> {
    let dresses = fetch_cluster_metrics().await.map_err(|err| {
        log::error!("Fatal: failed to fetch cluster metrics: {:?}", err);
        PostError::Invalid(err.into())
    })?;

    Ok(actix_web::HttpResponse::Ok().json(dresses))
}

// update the .query.json file and return the new ingestorMetadataArr
pub async fn get_ingestor_info() -> anyhow::Result<IngestorMetadataArr> {
    let store = CONFIG.storage().get_object_store();

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

pub async fn remove_ingestor(req: HttpRequest) -> Result<impl Responder, PostError> {
    let domain_name: String = req.match_info().get("ingestor").unwrap().parse().unwrap();
    let domain_name = to_url_string(domain_name);

    if check_liveness(&domain_name).await {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "The ingestor is currently live and cannot be removed"
        )));
    }
    let object_store = CONFIG.storage().get_object_store();

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

    let ingestor_meta_filename =
        ingestor_metadata_path(Some(&ingestor_metadata[0].ingestor_id)).to_string();
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

    log::info!("{}", &msg);
    Ok((msg, StatusCode::OK))
}

async fn fetch_cluster_metrics() -> Result<Vec<Metrics>, PostError> {
    let ingestor_metadata = get_ingestor_info().await.map_err(|err| {
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        PostError::Invalid(err)
    })?;

    let mut dresses = vec![];

    for ingestor in ingestor_metadata {
        let uri = Url::parse(&format!(
            "{}{}/metrics",
            &ingestor.domain_name,
            base_path_without_preceding_slash()
        ))
        .map_err(|err| {
            PostError::Invalid(anyhow::anyhow!("Invalid URL in Ingestor Metadata: {}", err))
        })?;

        let res = reqwest::Client::new()
            .get(uri)
            .header(header::AUTHORIZATION, &ingestor.token)
            .header(header::CONTENT_TYPE, "application/json")
            .send()
            .await;

        if let Ok(res) = res {
            let text = res.text().await.map_err(PostError::NetworkError)?;
            let lines: Vec<Result<String, std::io::Error>> =
                text.lines().map(|line| Ok(line.to_owned())).collect_vec();

            let sample = prometheus_parse::Scrape::parse(lines.into_iter())
                .map_err(|err| PostError::CustomError(err.to_string()))?
                .samples;
            let ingestor_metrics = Metrics::from_prometheus_samples(sample, &ingestor)
                .await
                .map_err(|err| {
                    log::error!("Fatal: failed to get ingestor metrics: {:?}", err);
                    PostError::Invalid(err.into())
                })?;
            dresses.push(ingestor_metrics);
        } else {
            log::warn!(
                "Failed to fetch metrics from ingestor: {}\n",
                &ingestor.domain_name,
            );
        }
    }
    Ok(dresses)
}

pub fn init_cluster_metrics_schedular() -> Result<(), PostError> {
    log::info!("Setting up schedular for cluster metrics ingestion");
    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(CLUSTER_METRICS_INTERVAL_SECONDS)
        .run(move || async {
            let result: Result<(), PostError> = async {
                let cluster_metrics = fetch_cluster_metrics().await;
                if let Ok(metrics) = cluster_metrics {
                    if !metrics.is_empty() {
                        log::info!("Cluster metrics fetched successfully from all ingestors");
                        if let Ok(metrics_bytes) = serde_json::to_vec(&metrics) {
                            if matches!(
                                ingest_internal_stream(
                                    INTERNAL_STREAM_NAME.to_string(),
                                    bytes::Bytes::from(metrics_bytes),
                                )
                                .await,
                                Ok(())
                            ) {
                                log::info!(
                                    "Cluster metrics successfully ingested into internal stream"
                                );
                            } else {
                                log::error!(
                                    "Failed to ingest cluster metrics into internal stream"
                                );
                            }
                        } else {
                            log::error!("Failed to serialize cluster metrics");
                        }
                    }
                }
                Ok(())
            }
            .await;

            if let Err(err) = result {
                log::error!("Error in cluster metrics scheduler: {:?}", err);
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
