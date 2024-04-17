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
use crate::handlers::http::ingest::PostError;
use crate::handlers::http::logstream::error::StreamError;
use crate::handlers::{STATIC_SCHEMA_FLAG, TIME_PARTITION_KEY};
use crate::option::CONFIG;

use crate::metrics::prom_utils::Metrics;
use crate::storage::object_storage::ingestor_metadata_path;
use crate::storage::{ObjectStorageError, STREAM_ROOT_DIRECTORY};
use crate::storage::{ObjectStoreFormat, PARSEABLE_ROOT_DIRECTORY};
use actix_web::http::header;
use actix_web::{HttpRequest, Responder};
use bytes::Bytes;
use chrono::Utc;
use http::StatusCode;
use itertools::Itertools;
use relative_path::RelativePathBuf;
use serde_json::Value as JsonValue;
use url::Url;

type IngestorMetadataArr = Vec<IngestorMetadata>;

use self::utils::StorageStats;

use super::base_path_without_preceding_slash;

use super::modal::IngestorMetadata;

// forward the request to all ingestors to keep them in sync
#[allow(dead_code)]
pub async fn sync_streams_with_ingestors(
    stream_name: &str,
    time_partition: &str,
    static_schema: &str,
    schema: Bytes,
) -> Result<(), StreamError> {
    let ingestor_infos = get_ingestor_info().await.map_err(|err| {
        log::error!("Fatal: failed to get ingestor info: {:?}", err);
        StreamError::Anyhow(err)
    })?;

    let mut errored = false;
    for ingestor in ingestor_infos.iter() {
        let url = format!(
            "{}{}/logstream/{}",
            ingestor.domain_name,
            base_path_without_preceding_slash(),
            stream_name
        );

        match send_stream_sync_request(
            &url,
            ingestor.clone(),
            time_partition,
            static_schema,
            schema.clone(),
        )
        .await
        {
            Ok(_) => continue,
            Err(_) => {
                errored = true;
                break;
            }
        }
    }

    if errored {
        for ingestor in ingestor_infos {
            let url = format!(
                "{}{}/logstream/{}",
                ingestor.domain_name,
                base_path_without_preceding_slash(),
                stream_name
            );

            // delete the stream
            send_stream_delete_request(&url, ingestor.clone()).await?;
        }

        // this might be a bit too much
        return Err(StreamError::Custom {
            msg: "Failed to sync stream with ingestors".to_string(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        });
    }

    Ok(())
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
            Box::new(|file_name| file_name.starts_with(".ingestor")),
        )
        .await?;
    let mut ingestion_size = 0u64;
    let mut storage_size = 0u64;
    let mut count = 0u64;
    for ob in obs {
        if let Ok(stat) = serde_json::from_slice::<ObjectStoreFormat>(&ob) {
            count += stat.stats.events;
            ingestion_size += stat.stats.ingestion;
            storage_size += stat.stats.storage;
        }
    }

    let qs = QueriedStats::new(
        "",
        Utc::now(),
        IngestionStats::new(count, format!("{} Bytes", ingestion_size), "json"),
        StorageStats::new(format!("{} Bytes", storage_size), "parquet"),
    );

    Ok(vec![qs])
}

#[allow(dead_code)]
async fn send_stream_sync_request(
    url: &str,
    ingestor: IngestorMetadata,
    time_partition: &str,
    static_schema: &str,
    schema: Bytes,
) -> Result<(), StreamError> {
    if !utils::check_liveness(&ingestor.domain_name).await {
        return Ok(());
    }

    let client = reqwest::Client::new();
    let res = client
        .put(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(TIME_PARTITION_KEY, time_partition)
        .header(STATIC_SCHEMA_FLAG, static_schema)
        .header(header::AUTHORIZATION, ingestor.token)
        .body(schema)
        .send()
        .await
        .map_err(|err| {
            log::error!(
                "Fatal: failed to forward create stream request to ingestor: {}\n Error: {:?}",
                ingestor.domain_name,
                err
            );
            StreamError::Network(err)
        })?;

    if !res.status().is_success() {
        log::error!(
            "failed to forward create stream request to ingestor: {}\nResponse Returned: {:?}",
            ingestor.domain_name,
            res
        );
        return Err(StreamError::Network(res.error_for_status().unwrap_err()));
    }

    Ok(())
}

/// send a rollback request to all ingestors
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
            resp
        );
    }

    Ok(())
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
                .unwrap()
                .as_str()
                .unwrap()
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
        .unwrap();

        let res = reqwest::Client::new()
            .get(uri)
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

            dresses.push(Metrics::from_prometheus_samples(
                sample,
                ingestor.domain_name,
            ));
        } else {
            log::warn!(
                "Failed to fetch metrics from ingestor: {}\n",
                ingestor.domain_name,
            );
        }
    }

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
        return Err(PostError::Invalid(anyhow::anyhow!("Node Online")));
    }

    let url = Url::parse(&domain_name).unwrap();
    let ingestor_meta_filename = ingestor_metadata_path(
        url.host_str().unwrap().to_owned(),
        url.port().unwrap().to_string(),
    )
    .to_string();
    let object_store = CONFIG.storage().get_object_store();
    let msg = match object_store
        .try_delete_ingestor_meta(ingestor_meta_filename)
        .await
    {
        Ok(_) => {
            format!("Node {} Removed Successfully", domain_name)
        }
        Err(err) => {
            if matches!(err, ObjectStorageError::IoError(_)) {
                format!("Node {} Not Found", domain_name)
            } else {
                format!("Error Removing Node {}\n Reason: {}", domain_name, err)
            }
        }
    };

    log::info!("{}", &msg);
    Ok((msg, StatusCode::OK))
}
