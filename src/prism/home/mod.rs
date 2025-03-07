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
pub struct HomeResponse {
    alert_titles: Vec<TitleAndId>,
    alerts_info: AlertsInfo,
    correlation_titles: Vec<TitleAndId>,
    stream_info: StreamInfo,
    stats_details: Vec<DatedStats>,
    stream_titles: Vec<String>,

    dashboard_titles: Vec<TitleAndId>,
    filter_titles: Vec<TitleAndId>,
}

pub async fn generate_home_response(key: &SessionKey) -> Result<HomeResponse, PrismHomeError> {
    // get all stream titles
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

    // get all alert IDs (TODO: RBAC)
    // do we need to move alerts into the PARSEABLE struct?
    let alert_titles = ALERTS
        .list_alerts_for_user(key.clone())
        .await?
        .iter()
        .map(|alert| TitleAndId {
            title: alert.title.clone(),
            id: alert.id.to_string(),
        })
        .collect_vec();

    // get correlation IDs
    let correlation_titles = CORRELATIONS
        .list_correlations(key)
        .await?
        .iter()
        .map(|corr| TitleAndId {
            title: corr.title.clone(),
            id: corr.id.clone(),
        })
        .collect_vec();

    // get dashboard IDs
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

    // get filter IDs
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

    // get alerts info (distribution of alerts based on severity and state)
    let alerts_info = get_alerts_info().await?;

    // generate dates for date-wise stats
    let dates = (0..7)
        .map(|i| {
            Utc::now()
                .checked_sub_signed(chrono::Duration::days(i))
                .ok_or_else(|| anyhow::Error::msg("Date conversion faield"))
                .unwrap()
        })
        .map(|date| date.format("%Y-%m-%d").to_string())
        .collect_vec();

    let mut stream_details = Vec::new();

    // this will hold the summary of all streams for the last 7 days
    let mut summary = StreamInfo::default();

    let mut stream_wise_ingestor_stream_json = HashMap::new();
    for stream in stream_titles.clone() {
        let path = RelativePathBuf::from_iter([&stream, STREAM_ROOT_DIRECTORY]);
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

        let mut ingestor_stream_jsons = Vec::new();
        for ob in obs {
            let stream_metadata: ObjectStoreFormat = match serde_json::from_slice(&ob) {
                Ok(d) => d,
                Err(e) => {
                    error!("Failed to parse stream metadata: {:?}", e);
                    continue;
                }
            };
            ingestor_stream_jsons.push(stream_metadata);
        }
        stream_wise_ingestor_stream_json.insert(stream, ingestor_stream_jsons);
    }

    for date in dates.into_iter() {
        let dated_stats = stats_for_date(date, stream_wise_ingestor_stream_json.clone()).await?;
        summary.stats_summary.events += dated_stats.events;
        summary.stats_summary.ingestion += dated_stats.ingestion_size;
        summary.stats_summary.storage += dated_stats.storage_size;

        stream_details.push(dated_stats);
    }

    Ok(HomeResponse {
        stream_info: summary,
        stats_details: stream_details,
        stream_titles: stream_titles.clone(),
        alert_titles,
        correlation_titles,
        dashboard_titles,
        filter_titles,
        alerts_info,
    })
}

async fn stats_for_date(
    date: String,
    stream_wise_meta: HashMap<String, Vec<ObjectStoreFormat>>,
) -> Result<DatedStats, PrismHomeError> {
    // collect stats for all the streams for the given date
    let mut details = DatedStats {
        date: date.clone(),
        ..Default::default()
    };

    for (stream, meta) in stream_wise_meta {
        let querier_stats = get_stats_date(&stream, &date).await?;
        let ingestor_stats = fetch_daily_stats_from_ingestors(&date, &meta)?;
        // collect date-wise stats for all streams
        details.events += querier_stats.events + ingestor_stats.events;
        details.ingestion_size += querier_stats.ingestion + ingestor_stats.ingestion;
        details.storage_size += querier_stats.storage + ingestor_stats.storage;
    }

    Ok(details)
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
