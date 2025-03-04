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

use actix_web::http::header::ContentType;
use chrono::Utc;
use http::StatusCode;
use itertools::Itertools;
use serde::Serialize;

use crate::{
    alerts::{get_alerts_info, AlertError, AlertsInfo, ALERTS},
    correlation::{CorrelationError, CORRELATIONS},
    handlers::http::logstream::{error::StreamError, get_stats_date},
    parseable::PARSEABLE,
    rbac::{map::SessionKey, role::Action, Users},
    stats::Stats,
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
    let user_id = if let Some(user_id) = Users.get_username_from_session(key) {
        user_id
    } else {
        return Err(PrismHomeError::Anyhow(anyhow::Error::msg(
            "User does not exist",
        )));
    };

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
        .list_dashboards_by_user(&user_id)
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
        .list_filters_by_user(&user_id)
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

    let mut summary = StreamInfo::default();

    for date in dates.iter() {
        let mut details = DatedStats {
            date: date.clone(),
            ..Default::default()
        };

        for stream in stream_titles.iter() {
            let stats = get_stats_date(stream, date).await?;

            // collect date-wise stats for all streams
            details.events += stats.events;
            details.ingestion_size += stats.ingestion;
            details.storage_size += stats.storage;

            // collect all 7-day stats for all streams
            summary.stats_summary.events += stats.events;
            summary.stats_summary.ingestion += stats.ingestion;
            summary.stats_summary.storage += stats.storage;
        }

        stream_details.push(details);
    }

    Ok(HomeResponse {
        stream_info: summary,
        stats_details: stream_details,
        stream_titles,
        alert_titles,
        correlation_titles,
        dashboard_titles,
        filter_titles,
        alerts_info,
    })
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
}

impl actix_web::ResponseError for PrismHomeError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PrismHomeError::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PrismHomeError::AlertError(e) => e.status_code(),
            PrismHomeError::CorrelationError(e) => e.status_code(),
            PrismHomeError::StreamError(e) => e.status_code(),
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
