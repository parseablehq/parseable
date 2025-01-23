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

use crate::{handlers::http::base_path_without_preceding_slash, HTTP_CLIENT};
use actix_web::http::header;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::error;
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
    pub lifetime_count: u64,
    pub lifetime_size: String,
    pub deleted_count: u64,
    pub deleted_size: String,
}

impl IngestionStats {
    pub fn new(
        count: u64,
        size: String,
        lifetime_count: u64,
        lifetime_size: String,
        deleted_count: u64,
        deleted_size: String,
        format: &str,
    ) -> Self {
        Self {
            count,
            size,
            format: format.to_string(),
            lifetime_count,
            lifetime_size,
            deleted_count,
            deleted_size,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StorageStats {
    pub size: String,
    pub format: String,
    pub lifetime_size: String,
    pub deleted_size: String,
}

impl StorageStats {
    pub fn new(size: String, lifetime_size: String, deleted_size: String, format: &str) -> Self {
        Self {
            size,
            format: format.to_string(),
            lifetime_size,
            deleted_size,
        }
    }
}

pub fn merge_quried_stats(stats: Vec<QueriedStats>) -> QueriedStats {
    // get the stream name
    let stream_name = stats[1].stream.clone();

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
                lifetime_count: acc.lifetime_count + x.lifetime_count,
                lifetime_size: format!(
                    "{} Bytes",
                    acc.lifetime_size.split(' ').collect_vec()[0]
                        .parse::<u64>()
                        .unwrap_or_default()
                        + x.lifetime_size.split(' ').collect_vec()[0]
                            .parse::<u64>()
                            .unwrap_or_default()
                ),
                deleted_count: acc.deleted_count + x.deleted_count,
                deleted_size: format!(
                    "{} Bytes",
                    acc.deleted_size.split(' ').collect_vec()[0]
                        .parse::<u64>()
                        .unwrap_or_default()
                        + x.deleted_size.split(' ').collect_vec()[0]
                            .parse::<u64>()
                            .unwrap_or_default()
                ),
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
                lifetime_size: format!(
                    "{} Bytes",
                    acc.lifetime_size.split(' ').collect_vec()[0]
                        .parse::<u64>()
                        .unwrap_or_default()
                        + x.lifetime_size.split(' ').collect_vec()[0]
                            .parse::<u64>()
                            .unwrap_or_default()
                ),
                deleted_size: format!(
                    "{} Bytes",
                    acc.deleted_size.split(' ').collect_vec()[0]
                        .parse::<u64>()
                        .unwrap_or_default()
                        + x.deleted_size.split(' ').collect_vec()[0]
                            .parse::<u64>()
                            .unwrap_or_default()
                ),
            });

    QueriedStats::new(
        &stream_name,
        min_time,
        cumulative_ingestion,
        cumulative_storage,
    )
}

pub async fn check_liveness(domain_name: &str) -> bool {
    let uri = match Url::parse(&format!(
        "{}{}/liveness",
        domain_name,
        base_path_without_preceding_slash()
    )) {
        Ok(uri) => uri,
        Err(err) => {
            error!("Node Indentifier Failed To Parse: {}", err);
            return false;
        }
    };

    let req = HTTP_CLIENT
        .get(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await;

    req.is_ok()
}

pub fn to_url_string(str: String) -> String {
    // if the str is already a url i am guessing that it will end in '/'
    if str.starts_with("http://") || str.starts_with("https://") {
        return str;
    }

    format!("http://{}/", str)
}
