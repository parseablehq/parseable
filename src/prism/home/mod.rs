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

use std::collections::HashMap;

use actix_web::http::header::ContentType;
use chrono::Utc;
use http::StatusCode;
use itertools::Itertools;
use relative_path::RelativePathBuf;
use serde::Serialize;
use tracing::error;

use crate::{
    alerts::{ALERTS, AlertError, AlertsSummary, get_alerts_summary},
    correlation::{CORRELATIONS, CorrelationError},
    event::format::LogSource,
    handlers::http::{cluster::fetch_daily_stats, logstream::error::StreamError},
    parseable::PARSEABLE,
    rbac::{Users, map::SessionKey, role::Action},
    stats::Stats,
    storage::{ObjectStorageError, ObjectStoreFormat, STREAM_ROOT_DIRECTORY, StreamType},
    users::{dashboards::DASHBOARDS, filters::FILTERS},
};

type StreamMetadataResponse = Result<(String, Vec<ObjectStoreFormat>, DataSetType), PrismHomeError>;

#[derive(Debug, Serialize, Default)]
pub struct DatedStats {
    date: String,
    events: u64,
    ingestion: u64,
    storage: u64,
}

#[derive(Debug, Serialize)]
enum DataSetType {
    Logs,
    Metrics,
    Traces,
}

#[derive(Debug, Serialize)]
pub struct DataSet {
    title: String,
    dataset_type: DataSetType,
}

#[derive(Debug, Serialize)]
pub struct HomeResponse {
    pub alerts_summary: AlertsSummary,
    pub stats_details: Vec<DatedStats>,
    pub datasets: Vec<DataSet>,
    pub top_five_ingestion: HashMap<String, Stats>,
}

#[derive(Debug, Serialize)]
pub enum ResourceType {
    Alert,
    Correlation,
    Dashboard,
    Filter,
    DataSet,
}

#[derive(Debug, Serialize)]
pub struct Resource {
    id: String,
    name: String,
    resource_type: ResourceType,
}

#[derive(Debug, Serialize)]
pub struct HomeSearchResponse {
    resources: Vec<Resource>,
}

pub async fn generate_home_response(
    key: &SessionKey,
    include_internal: bool,
) -> Result<HomeResponse, PrismHomeError> {
    // Execute these operations concurrently
    let (stream_titles_result, alerts_summary_result) =
        tokio::join!(get_stream_titles(key), get_alerts_summary());

    let stream_titles = stream_titles_result?;
    let alerts_summary = alerts_summary_result?;

    // Generate dates for date-wise stats
    let mut dates = (0..7)
        .map(|i| {
            Utc::now()
                .checked_sub_signed(chrono::Duration::days(i))
                .expect("Date conversion failed")
        })
        .map(|date| date.format("%Y-%m-%d").to_string())
        .collect_vec();
    dates.reverse();

    // Process stream metadata concurrently
    let stream_metadata_futures = stream_titles
        .iter()
        .map(|stream| get_stream_metadata(stream.clone()));
    let stream_metadata_results: Vec<StreamMetadataResponse> =
        futures::future::join_all(stream_metadata_futures).await;

    let mut stream_wise_stream_json: HashMap<String, Vec<ObjectStoreFormat>> = HashMap::new();
    let mut datasets = Vec::new();

    for result in stream_metadata_results {
        match result {
            Ok((stream, metadata, dataset_type)) => {
                // Skip internal streams if the flag is false
                if !include_internal
                    && metadata
                        .iter()
                        .all(|m| m.stream_type == StreamType::Internal)
                {
                    continue;
                }
                stream_wise_stream_json.insert(stream.clone(), metadata);
                datasets.push(DataSet {
                    title: stream,
                    dataset_type,
                });
            }
            Err(e) => {
                error!("Failed to process stream metadata: {:?}", e);
                // Continue with other streams instead of failing entirely
            }
        }
    }

    let top_five_ingestion = get_top_5_streams_by_ingestion(&stream_wise_stream_json);

    // Process stats for all dates concurrently
    let stats_futures = dates
        .iter()
        .map(|date| stats_for_date(date.clone(), stream_wise_stream_json.clone()));
    let stats_results: Vec<Result<DatedStats, PrismHomeError>> =
        futures::future::join_all(stats_futures).await;

    let mut stream_details = Vec::new();

    for result in stats_results {
        match result {
            Ok(dated_stats) => {
                stream_details.push(dated_stats);
            }
            Err(e) => {
                error!("Failed to process stats for date: {:?}", e);
                // Continue with other dates instead of failing entirely
            }
        }
    }

    Ok(HomeResponse {
        stats_details: stream_details,
        datasets,
        alerts_summary,
        top_five_ingestion,
    })
}

fn get_top_5_streams_by_ingestion(
    stream_wise_stream_json: &HashMap<String, Vec<ObjectStoreFormat>>,
) -> HashMap<String, Stats> {
    let mut result: Vec<_> = stream_wise_stream_json
        .iter()
        .map(|(stream_name, formats)| {
            let total_stats = formats.iter().fold(
                Stats {
                    events: 0,
                    ingestion: 0,
                    storage: 0,
                },
                |mut acc, osf| {
                    let current = &osf.stats.current_stats;
                    acc.events += current.events;
                    acc.ingestion += current.ingestion;
                    acc.storage += current.storage;
                    acc
                },
            );
            (stream_name.clone(), total_stats)
        })
        .collect();

    result.sort_by_key(|(_, stats)| std::cmp::Reverse(stats.ingestion));
    result.truncate(5);
    result.into_iter().collect()
}

async fn get_stream_metadata(
    stream: String,
) -> Result<(String, Vec<ObjectStoreFormat>, DataSetType), PrismHomeError> {
    let path = RelativePathBuf::from_iter([&stream, STREAM_ROOT_DIRECTORY]);
    let obs = PARSEABLE
        .storage
        .get_object_store()
        .get_objects(
            Some(&path),
            Box::new(|file_name| file_name.ends_with("stream.json")),
        )
        .await?;

    let mut stream_jsons = Vec::new();
    for ob in obs {
        let stream_metadata: ObjectStoreFormat = match serde_json::from_slice(&ob) {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to parse stream metadata: {:?}", e);
                continue;
            }
        };
        stream_jsons.push(stream_metadata);
    }

    if stream_jsons.is_empty() {
        return Err(PrismHomeError::Anyhow(anyhow::Error::msg(
            "No stream metadata found",
        )));
    }

    // let log_source = &stream_jsons[0].clone().log_source;
    let log_source_format = stream_jsons
        .iter()
        .find(|sj| !sj.log_source.is_empty())
        .map(|sj| sj.log_source[0].log_source_format.clone())
        .unwrap_or_default();

    let dataset_type = match log_source_format {
        LogSource::OtelMetrics => DataSetType::Metrics,
        LogSource::OtelTraces => DataSetType::Traces,
        _ => DataSetType::Logs,
    };

    Ok((stream, stream_jsons, dataset_type))
}

async fn stats_for_date(
    date: String,
    stream_wise_meta: HashMap<String, Vec<ObjectStoreFormat>>,
) -> Result<DatedStats, PrismHomeError> {
    // Initialize result structure
    let mut details = DatedStats {
        date: date.clone(),
        ..Default::default()
    };

    // Process each stream concurrently
    let stream_stats_futures = stream_wise_meta
        .values()
        .map(|meta| get_stream_stats_for_date(date.clone(), meta));

    let stream_stats_results = futures::future::join_all(stream_stats_futures).await;

    // Aggregate results
    for result in stream_stats_results {
        match result {
            Ok((events, ingestion, storage)) => {
                details.events += events;
                details.ingestion += ingestion;
                details.storage += storage;
            }
            Err(e) => {
                error!("Failed to get stats for stream: {:?}", e);
                // Continue with other streams
            }
        }
    }

    Ok(details)
}

async fn get_stream_stats_for_date(
    date: String,
    meta: &[ObjectStoreFormat],
) -> Result<(u64, u64, u64), PrismHomeError> {
    let stats = fetch_daily_stats(&date, meta)?;

    Ok((stats.events, stats.ingestion, stats.storage))
}

pub async fn generate_home_search_response(
    key: &SessionKey,
    query_value: &str,
) -> Result<HomeSearchResponse, PrismHomeError> {
    let mut resources = Vec::new();
    let (alert_titles, correlation_titles, dashboard_titles, filter_titles, stream_titles) = tokio::join!(
        get_alert_titles(key, query_value),
        get_correlation_titles(key, query_value),
        get_dashboard_titles(query_value),
        get_filter_titles(key, query_value),
        get_stream_titles(key)
    );

    let alerts = alert_titles?;
    resources.extend(alerts);
    let correlations = correlation_titles?;
    resources.extend(correlations);
    let dashboards = dashboard_titles?;
    resources.extend(dashboards);
    let filters = filter_titles?;
    resources.extend(filters);
    let stream_titles = stream_titles?;

    for title in stream_titles {
        if title.to_lowercase().contains(query_value) {
            resources.push(Resource {
                id: title.clone(),
                name: title,
                resource_type: ResourceType::DataSet,
            });
        }
    }
    Ok(HomeSearchResponse { resources })
}

// Helper functions to split the work
async fn get_stream_titles(key: &SessionKey) -> Result<Vec<String>, PrismHomeError> {
    let stream_titles: Vec<String> = PARSEABLE
        .storage
        .get_object_store()
        .list_streams()
        .await
        .map_err(|e| PrismHomeError::Anyhow(anyhow::Error::new(e)))?
        .into_iter()
        .filter(|logstream| {
            Users.authorize(key.clone(), Action::ListStream, Some(logstream), None)
                == crate::rbac::Response::Authorized
        })
        .sorted()
        .collect_vec();

    Ok(stream_titles)
}

async fn get_alert_titles(
    key: &SessionKey,
    query_value: &str,
) -> Result<Vec<Resource>, PrismHomeError> {
    let alerts = ALERTS
        .list_alerts_for_user(key.clone())
        .await?
        .iter()
        .filter_map(|alert| {
            if alert.title.to_lowercase().contains(query_value)
                || alert.id.to_string().to_lowercase().contains(query_value)
            {
                Some(Resource {
                    id: alert.id.to_string(),
                    name: alert.title.clone(),
                    resource_type: ResourceType::Alert,
                })
            } else {
                None
            }
        })
        .collect_vec();

    Ok(alerts)
}

async fn get_correlation_titles(
    key: &SessionKey,
    query_value: &str,
) -> Result<Vec<Resource>, PrismHomeError> {
    let correlations = CORRELATIONS
        .list_correlations(key)
        .await?
        .iter()
        .filter_map(|correlation| {
            if correlation.title.to_lowercase().contains(query_value)
                || correlation.id.to_lowercase().contains(query_value)
            {
                Some(Resource {
                    id: correlation.id.to_string(),
                    name: correlation.title.clone(),
                    resource_type: ResourceType::Correlation,
                })
            } else {
                None
            }
        })
        .collect_vec();

    Ok(correlations)
}

async fn get_dashboard_titles(query_value: &str) -> Result<Vec<Resource>, PrismHomeError> {
    let dashboard_titles = DASHBOARDS
        .list_dashboards(0)
        .await
        .iter()
        .filter_map(|dashboard| {
            let dashboard_id = *dashboard.dashboard_id.as_ref().unwrap();
            let dashboard_id = dashboard_id.to_string();
            if dashboard.title.to_lowercase().contains(query_value)
                || dashboard_id.to_lowercase().contains(query_value)
            {
                Some(Resource {
                    id: dashboard_id,
                    name: dashboard.title.clone(),
                    resource_type: ResourceType::Dashboard,
                })
            } else {
                None
            }
        })
        .collect_vec();

    Ok(dashboard_titles)
}

async fn get_filter_titles(
    key: &SessionKey,
    query_value: &str,
) -> Result<Vec<Resource>, PrismHomeError> {
    let filter_titles = FILTERS
        .list_filters(key)
        .await
        .iter()
        .filter_map(|filter| {
            let filter_id = filter.filter_id.as_ref().unwrap().clone();
            if filter.filter_name.to_lowercase().contains(query_value)
                || filter_id.to_lowercase().contains(query_value)
            {
                Some(Resource {
                    id: filter_id,
                    name: filter.filter_name.clone(),
                    resource_type: ResourceType::Filter,
                })
            } else {
                None
            }
        })
        .collect_vec();
    Ok(filter_titles)
}

#[derive(Debug, thiserror::Error)]
pub enum PrismHomeError {
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("AlertError: {0}")]
    AlertError(#[from] AlertError),
    #[error("CorrelationError: {0}")]
    CorrelationError(#[from] CorrelationError),
    #[error("StreamError: {0}")]
    StreamError(#[from] StreamError),
    #[error("ObjectStorageError: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("Invalid query parameter: {0}")]
    InvalidQueryParameter(String),
}

impl actix_web::ResponseError for PrismHomeError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PrismHomeError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismHomeError::AlertError(e) => e.status_code(),
            PrismHomeError::CorrelationError(e) => e.status_code(),
            PrismHomeError::StreamError(e) => e.status_code(),
            PrismHomeError::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismHomeError::InvalidQueryParameter(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
