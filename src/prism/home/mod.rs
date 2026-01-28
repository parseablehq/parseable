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
use actix_web::http::StatusCode;
use actix_web::http::header::ContentType;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::{
    alerts::{ALERTS, AlertError, AlertState},
    correlation::{CORRELATIONS, CorrelationError},
    event::format::{LogSource, LogSourceEntry},
    handlers::{TelemetryType, http::logstream::error::StreamError},
    metastore::MetastoreError,
    parseable::PARSEABLE,
    rbac::{
        Users,
        map::{SessionKey, users},
        role::Action,
    },
    storage::{ObjectStorageError, ObjectStoreFormat, StreamType},
    users::{dashboards::DASHBOARDS, filters::FILTERS},
};

type StreamMetadataResponse = Result<
    (
        String,
        Vec<ObjectStoreFormat>,
        TelemetryType,
        Option<String>,
        LogSource,
        bool,
    ),
    PrismHomeError,
>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSet {
    title: String,
    dataset_type: TelemetryType,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_partition: Option<String>,
    dataset_format: LogSource,
    ingestion: bool,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Checklist {
    pub data_ingested: bool,
    pub keystone_created: bool,
    pub alert_created: bool,
    pub user_added: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HomeResponse {
    pub datasets: Vec<DataSet>,
    pub checklist: Checklist,
    pub triggered_alerts_count: u64,
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
    let all_streams = PARSEABLE.metastore.list_streams().await?;

    let stream_titles: Vec<String> = all_streams
        .iter()
        .filter(|logstream| {
            Users.authorize(key.clone(), Action::ListStream, Some(logstream), None)
                == crate::rbac::Response::Authorized
        })
        .cloned()
        .sorted()
        .collect_vec();

    // Process stream metadata concurrently
    let stream_metadata_futures = stream_titles
        .iter()
        .map(|stream| get_stream_metadata(stream.clone()));
    let stream_metadata_results: Vec<StreamMetadataResponse> =
        futures::future::join_all(stream_metadata_futures).await;

    let mut datasets = Vec::new();

    for result in stream_metadata_results {
        match result {
            Ok((stream, metadata, dataset_type, time_partition, dataset_format, ingestion)) => {
                // Skip internal streams if the flag is false
                if !include_internal
                    && metadata
                        .iter()
                        .all(|m| m.stream_type == StreamType::Internal)
                {
                    continue;
                }

                datasets.push(DataSet {
                    title: stream,
                    dataset_type,
                    time_partition,
                    dataset_format,
                    ingestion,
                });
            }
            Err(e) => {
                error!("Failed to process stream metadata: {:?}", e);
                // Continue with other streams instead of failing entirely
            }
        }
    }

    // Generate checklist and count triggered alerts
    let data_ingested = datasets.iter().any(|d| d.ingestion);
    let user_count = users().len();
    let user_added = user_count > 1; // more than just the default admin user

    // Calculate triggered alerts count
    let (alert_created, triggered_alerts_count) = {
        let guard = ALERTS.read().await;
        if let Some(alerts) = guard.as_ref() {
            let user_alerts = alerts.list_alerts_for_user(key.clone(), vec![]).await?;
            let total_alerts = !user_alerts.is_empty();

            // Count alerts currently in triggered state
            let triggered_count = user_alerts
                .iter()
                .filter(|alert| alert.state == AlertState::Triggered)
                .count() as u64;

            (total_alerts, triggered_count)
        } else {
            (false, 0)
        }
    };

    let checklist = Checklist {
        data_ingested,
        keystone_created: false, // Enterprise will override
        alert_created,
        user_added,
    };

    Ok(HomeResponse {
        datasets,
        checklist,
        triggered_alerts_count,
    })
}

async fn get_stream_metadata(
    stream: String,
) -> Result<
    (
        String,
        Vec<ObjectStoreFormat>,
        TelemetryType,
        Option<String>,
        LogSource,
        bool,
    ),
    PrismHomeError,
> {
    let obs = PARSEABLE
        .metastore
        .get_all_stream_jsons(&stream, None)
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

    let dataset_type = stream_jsons[0].telemetry_type;
    let time_partition = stream_jsons[0].time_partition.clone();
    let dataset_format = stream_jsons[0]
        .log_source
        .first()
        .cloned()
        .unwrap_or_else(LogSourceEntry::default)
        .log_source_format
        .clone();
    let ingested = stream_jsons
        .iter()
        .any(|s| s.stats.current_stats.events > 0);

    Ok((
        stream,
        stream_jsons,
        dataset_type,
        time_partition,
        dataset_format,
        ingested,
    ))
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
        .metastore
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
    let guard = ALERTS.read().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(PrismHomeError::AlertError(AlertError::CustomError(
            "No AlertManager set".into(),
        )));
    };
    let alerts = alerts
        .list_alerts_for_user(key.clone(), vec![])
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
    #[error(transparent)]
    MetastoreError(#[from] MetastoreError),
}

impl actix_web::ResponseError for PrismHomeError {
    fn status_code(&self) -> StatusCode {
        match self {
            PrismHomeError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismHomeError::AlertError(e) => e.status_code(),
            PrismHomeError::CorrelationError(e) => e.status_code(),
            PrismHomeError::StreamError(e) => e.status_code(),
            PrismHomeError::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismHomeError::InvalidQueryParameter(_) => StatusCode::BAD_REQUEST,
            PrismHomeError::MetastoreError(e) => e.status_code(),
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        match self {
            PrismHomeError::MetastoreError(e) => actix_web::HttpResponse::build(e.status_code())
                .insert_header(ContentType::json())
                .json(e.to_detail()),
            _ => actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string()),
        }
    }
}
