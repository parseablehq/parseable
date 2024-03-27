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

use crate::handlers::http::{logstream::error::StreamError, modal::IngesterMetadata};
use actix_web::http::header;
use chrono::{DateTime, Utc};
use http::StatusCode;
use itertools::Itertools;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct QueriedStats {
    pub stream: String,
    pub creation_time: String,
    pub first_event_at: Option<String>,
    pub time: DateTime<Utc>,
    pub ingestion: IngestionStats,
    pub storage: StorageStats,
}

impl QueriedStats {
    pub fn new(
        stream: &str,
        creation_time: &str,
        first_event_at: Option<String>,
        time: DateTime<Utc>,
        ingestion: IngestionStats,
        storage: StorageStats,
    ) -> Self {
        Self {
            stream: stream.to_string(),
            creation_time: creation_time.to_string(),
            first_event_at,
            time,
            ingestion,
            storage,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ClusterInfo {
    domain_name: String,
    reachable: bool,
    staging_path: String,
    storage_path: String,
    error: Option<String>,  // error message if the ingester is not reachable
    status: Option<String>, // status message if the ingester is reachable
}

impl ClusterInfo {
    pub fn new(
        domain_name: &str,
        reachable: bool,
        staging_path: String,
        storage_path: String,
        error: Option<String>,
        status: Option<String>,
    ) -> Self {
        Self {
            domain_name: domain_name.to_string(),
            reachable,
            staging_path,
            storage_path,
            error,
            status,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IngestionStats {
    pub count: u64,
    pub size: String,
    pub format: String,
}

impl IngestionStats {
    pub fn new(count: u64, size: String, format: &str) -> Self {
        Self {
            count,
            size,
            format: format.to_string(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StorageStats {
    size: String,
    format: String,
}

impl StorageStats {
    pub fn new(size: String, format: &str) -> Self {
        Self {
            size,
            format: format.to_string(),
        }
    }
}

pub fn merge_quried_stats(stats: Vec<QueriedStats>) -> QueriedStats {
    // get the actual creation time
    let min_creation_time = stats
        .iter()
        .map(|x| x.creation_time.parse::<DateTime<Utc>>().unwrap())
        .min()
        .unwrap();  // should never be None

    // get the stream name
    let stream_name = stats[0].stream.clone();

    // get the first event at
    let min_first_event_at = stats
        .iter()
        .map(|x| match x.first_event_at.as_ref() {
            // we can directly unwrap here because
            // we are sure that the first_event_at is a valid date
            Some(fea) => fea.parse::<DateTime<Utc>>().unwrap(),
            None => Utc::now(), // current time ie the max time
        })
        .min()
        .unwrap();  // should never be None

    let min_time = stats.iter().map(|x| x.time).min().unwrap_or_else(Utc::now);

    let cumulative_ingestion =
        stats
            .iter()
            .map(|x| &x.ingestion)
            .fold(IngestionStats::default(), |acc, x| IngestionStats {
                count: acc.count + x.count,
                size: format!(
                    "{} Bytes",
                    acc.size.split(' ').collect_vec()[0]
                        .parse::<u64>()
                        .unwrap_or_default()
                        + x.size.split(' ').collect_vec()[0]
                            .parse::<u64>()
                            .unwrap_or_default()
                ),
                format: x.format.clone(),
            });

    let cumulative_storage =
        stats
            .iter()
            .map(|x| &x.storage)
            .fold(StorageStats::default(), |acc, x| StorageStats {
                size: format!(
                    "{} Bytes",
                    acc.size.split(' ').collect_vec()[0]
                        .parse::<u64>()
                        .unwrap_or_default()
                        + x.size.split(' ').collect_vec()[0]
                            .parse::<u64>()
                            .unwrap_or_default()
                ),
                format: x.format.clone(),
            });

    QueriedStats::new(
        &stream_name,
        &min_creation_time.to_string(),
        Some(min_first_event_at.to_string()),
        min_time,
        cumulative_ingestion,
        cumulative_storage,
    )
}

pub async fn check_liveness(domain_name: &str) -> bool {
    let uri = Url::parse(&format!("{}liveness", domain_name)).unwrap();

    let reqw = reqwest::Client::new()
        .get(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await;

    reqw.is_ok()
}

/// send a request to the ingester to fetch its stats
pub async fn send_stats_request(
    url: &str,
    ingester: IngesterMetadata,
) -> Result<Option<Response>, StreamError> {
    if !check_liveness(&ingester.domain_name).await {
        return Ok(None);
    }

    let client = reqwest::Client::new();
    let res = client
        .get(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, ingester.token)
        .send()
        .await
        .map_err(|err| {
            log::error!(
                "Fatal: failed to fetch stats from ingester: {}\n Error: {:?}",
                ingester.domain_name,
                err
            );

            StreamError::Custom {
                msg: format!(
                    "failed to fetch stats from ingester: {}\n Error: {:?}",
                    ingester.domain_name, err
                ),
                status: StatusCode::INTERNAL_SERVER_ERROR,
            }
        })?;

    if !res.status().is_success() {
        log::error!(
            "failed to forward create stream request to ingester: {}\nResponse Returned: {:?}",
            ingester.domain_name,
            res
        );
        return Err(StreamError::Custom {
            msg: format!(
                "failed to forward create stream request to ingester: {}\nResponse Returned: {:?}",
                ingester.domain_name,
                res.text().await.unwrap_or_default()
            ),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        });
    }

    Ok(Some(res))
}
