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

use crate::handlers::http::{logstream::error::StreamError, modal::IngestorMetadata};
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
    pub time: DateTime<Utc>,
    pub ingestion: IngestionStats,
    pub storage: StorageStats,
}

impl QueriedStats {
    pub fn new(
        stream: &str,
        time: DateTime<Utc>,
        ingestion: IngestionStats,
        storage: StorageStats,
    ) -> Self {
        Self {
            stream: stream.to_string(),
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
    error: Option<String>,  // error message if the ingestor is not reachable
    status: Option<String>, // status message if the ingestor is reachable
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
    pub size: String,
    pub format: String,
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
    // let min_creation_time = stats
    //     .iter()
    //     .map(|x| x.creation_time.parse::<DateTime<Utc>>().unwrap())
    //     .min()
    //     .unwrap(); // should never be None

    // get the stream name
    let stream_name = stats[1].stream.clone();

    // get the first event at
    // let min_first_event_at = stats
    //     .iter()
    //     .map(|x| match x.first_event_at.as_ref() {
    // we can directly unwrap here because
    // we are sure that the first_event_at is a valid date
    //         Some(fea) => fea.parse::<DateTime<Utc>>().unwrap(),
    //         None => Utc::now(), // current time ie the max time
    //     })
    //     .min()
    //     .unwrap(); // should never be None

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
        min_time,
        cumulative_ingestion,
        cumulative_storage,
    )
}

pub async fn check_liveness(domain_name: &str) -> bool {
    let uri = match Url::parse(&format!("{}liveness", domain_name)) {
        Ok(uri) => uri,
        Err(err) => {
            log::error!("Node Indentifier Failed To Parse: {}", err);
            return false;
        }
    };

    let reqw = reqwest::Client::new()
        .get(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await;

    reqw.is_ok()
}

/// send a request to the ingestor to fetch its stats
/// dead for now
#[allow(dead_code)]
pub async fn send_stats_request(
    url: &str,
    ingestor: IngestorMetadata,
) -> Result<Option<Response>, StreamError> {
    if !check_liveness(&ingestor.domain_name).await {
        return Ok(None);
    }

    let client = reqwest::Client::new();
    let res = client
        .get(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, ingestor.token)
        .send()
        .await
        .map_err(|err| {
            log::error!(
                "Fatal: failed to fetch stats from ingestor: {}\n Error: {:?}",
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
        return Err(StreamError::Custom {
            msg: format!(
                "failed to forward create stream request to ingestor: {}\nResponse Returned: {:?}",
                ingestor.domain_name,
                res.text().await.unwrap_or_default()
            ),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        });
    }

    Ok(Some(res))
}

/// domain_name needs to be http://ip:port
/// dead code for now
#[allow(dead_code)]
pub fn ingestor_meta_filename(domain_name: &str) -> String {
    if domain_name.starts_with("http://") | domain_name.starts_with("https://") {
        let url = Url::parse(domain_name).unwrap();
        return format!(
            "ingestor.{}.{}.json",
            url.host_str().unwrap(),
            url.port().unwrap()
        );
    }
    format!("ingestor.{}.json", domain_name)
}

pub fn to_url_string(str: String) -> String {
    // if the str is already a url i am guessing that it will end in '/'
    if str.starts_with("http://") || str.starts_with("https://") {
        return str;
    }

    format!("http://{}/", str)
}
