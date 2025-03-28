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
    alerts::{get_alerts_info, AlertError, AlertsInfo, ALERTS},
    correlation::{CorrelationError, CORRELATIONS},
    event::format::LogSource,
    handlers::http::{
        cluster::fetch_daily_stats_from_ingestors,
        logstream::{error::StreamError, get_stats_date},
    },
    parseable::PARSEABLE,
    rbac::{map::SessionKey, role::Action, Users},
    stats::Stats,
    storage::{ObjectStorageError, ObjectStoreFormat, STREAM_ROOT_DIRECTORY},
    users::{dashboards::DASHBOARDS, filters::FILTERS},
};

type StreamMetadataResponse = Result<(String, Vec<ObjectStoreFormat>, DataSetType), PrismHomeError>;

#[derive(Debug, Serialize, Default)]
struct StreamInfo {
    // stream_count: u32,
    // log_source_count: u32,
    stats_summary: Stats,
}

#[derive(Debug, Serialize, Default)]
struct DatedStats {
    date: String,
    events: u64,
    ingestion_size: u64,
    storage_size: u64,
}

#[derive(Debug, Serialize)]
struct TitleAndId {
    title: String,
    id: String,
}

#[derive(Debug, Serialize)]
enum DataSetType {
    Logs,
    Metrics,
    Traces,
}

#[derive(Debug, Serialize)]
struct DataSet {
    title: String,
    dataset_type: DataSetType,
}

#[derive(Debug, Serialize)]
pub struct HomeResponse {
    alert_titles: Vec<TitleAndId>,
    alerts_info: AlertsInfo,
    correlation_titles: Vec<TitleAndId>,
    stream_info: StreamInfo,
    stats_details: Vec<DatedStats>,
    stream_titles: Vec<String>,
    datasets: Vec<DataSet>,
    dashboard_titles: Vec<TitleAndId>,
    filter_titles: Vec<TitleAndId>,
}

pub async fn generate_home_response(key: &SessionKey) -> Result<HomeResponse, PrismHomeError> {
    // Execute these operations concurrently
    let (
        stream_titles_result,
        alert_titles_result,
        correlation_titles_result,
        dashboards_result,
        filters_result,
        alerts_info_result,
    ) = tokio::join!(
        get_stream_titles(key),
        get_alert_titles(key),
        get_correlation_titles(key),
        get_dashboard_titles(key),
        get_filter_titles(key),
        get_alerts_info()
    );

    let stream_titles = stream_titles_result?;
    let alert_titles = alert_titles_result?;
    let correlation_titles = correlation_titles_result?;
    let dashboard_titles = dashboards_result?;
    let filter_titles = filters_result?;
    let alerts_info = alerts_info_result?;

    // Generate dates for date-wise stats
    let mut dates = (0..7)
        .map(|i| {
            Utc::now()
                .checked_sub_signed(chrono::Duration::days(i))
                .ok_or_else(|| anyhow::Error::msg("Date conversion failed"))
                .unwrap()
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

    let mut stream_wise_stream_json = HashMap::new();
    let mut datasets = Vec::new();

    for result in stream_metadata_results {
        match result {
            Ok((stream, metadata, dataset_type)) => {
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

    // Process stats for all dates concurrently
    let stats_futures = dates
        .iter()
        .map(|date| stats_for_date(date.clone(), stream_wise_stream_json.clone()));
    let stats_results: Vec<Result<DatedStats, PrismHomeError>> =
        futures::future::join_all(stats_futures).await;

    let mut stream_details = Vec::new();
    let mut summary = StreamInfo::default();

    for result in stats_results {
        match result {
            Ok(dated_stats) => {
                summary.stats_summary.events += dated_stats.events;
                summary.stats_summary.ingestion += dated_stats.ingestion_size;
                summary.stats_summary.storage += dated_stats.storage_size;
                stream_details.push(dated_stats);
            }
            Err(e) => {
                error!("Failed to process stats for date: {:?}", e);
                // Continue with other dates instead of failing entirely
            }
        }
    }

    Ok(HomeResponse {
        stream_info: summary,
        stats_details: stream_details,
        stream_titles,
        datasets,
        alert_titles,
        correlation_titles,
        dashboard_titles,
        filter_titles,
        alerts_info,
    })
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

async fn get_alert_titles(key: &SessionKey) -> Result<Vec<TitleAndId>, PrismHomeError> {
    let alert_titles = ALERTS
        .list_alerts_for_user(key.clone())
        .await?
        .iter()
        .map(|alert| TitleAndId {
            title: alert.title.clone(),
            id: alert.id.to_string(),
        })
        .collect_vec();

    Ok(alert_titles)
}

async fn get_correlation_titles(key: &SessionKey) -> Result<Vec<TitleAndId>, PrismHomeError> {
    let correlation_titles = CORRELATIONS
        .list_correlations(key)
        .await?
        .iter()
        .map(|corr| TitleAndId {
            title: corr.title.clone(),
            id: corr.id.clone(),
        })
        .collect_vec();

    Ok(correlation_titles)
}

async fn get_dashboard_titles(key: &SessionKey) -> Result<Vec<TitleAndId>, PrismHomeError> {
    let dashboard_titles = DASHBOARDS
        .list_dashboards(key)
        .await
        .iter()
        .map(|dashboard| TitleAndId {
            title: dashboard.name.clone(),
            id: dashboard
                .dashboard_id
                .as_ref()
                .ok_or_else(|| anyhow::Error::msg("Dashboard ID is null"))
                .unwrap()
                .clone(),
        })
        .collect_vec();

    Ok(dashboard_titles)
}

async fn get_filter_titles(key: &SessionKey) -> Result<Vec<TitleAndId>, PrismHomeError> {
    let filter_titles = FILTERS
        .list_filters(key)
        .await
        .iter()
        .map(|filter| TitleAndId {
            title: filter.filter_name.clone(),
            id: filter
                .filter_id
                .as_ref()
                .ok_or_else(|| anyhow::Error::msg("Filter ID is null"))
                .unwrap()
                .clone(),
        })
        .collect_vec();

    Ok(filter_titles)
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
    let stream_stats_futures = stream_wise_meta.iter().map(|(stream, meta)| {
        get_stream_stats_for_date(stream.clone(), date.clone(), meta.clone())
    });

    let stream_stats_results = futures::future::join_all(stream_stats_futures).await;

    // Aggregate results
    for result in stream_stats_results {
        match result {
            Ok((events, ingestion, storage)) => {
                details.events += events;
                details.ingestion_size += ingestion;
                details.storage_size += storage;
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
    stream: String,
    date: String,
    meta: Vec<ObjectStoreFormat>,
) -> Result<(u64, u64, u64), PrismHomeError> {
    let querier_stats = get_stats_date(&stream, &date).await?;
    let ingestor_stats = fetch_daily_stats_from_ingestors(&date, &meta)?;

    Ok((
        querier_stats.events + ingestor_stats.events,
        querier_stats.ingestion + ingestor_stats.ingestion,
        querier_stats.storage + ingestor_stats.storage,
    ))
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
}

impl actix_web::ResponseError for PrismHomeError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PrismHomeError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismHomeError::AlertError(e) => e.status_code(),
            PrismHomeError::CorrelationError(e) => e.status_code(),
            PrismHomeError::StreamError(e) => e.status_code(),
            PrismHomeError::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
