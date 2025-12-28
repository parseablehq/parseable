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

use actix_web::http::header::HeaderMap;
use datafusion::common::HashSet;
use ulid::Ulid;

use crate::{
    event::format::LogSource, handlers::{
        CUSTOM_PARTITION_KEY, LOG_SOURCE_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY,
        TELEMETRY_TYPE_KEY, TIME_PARTITION_KEY, TIME_PARTITION_LIMIT_KEY, TelemetryType,
        UPDATE_STREAM_KEY,
    }, 
    metastore::MetastoreError, 
    parseable::{
        PARSEABLE, StreamNotFound
    }, 
    storage::StreamType, 
    users::{
        dashboards::{Dashboard}, 
        filters::Filter
    }
};

/// Field in a dashboard's tile that should contain the logstream name
const TILE_FIELD_REFERRING_TO_STREAM: &str = "chartQuery";

#[derive(Debug, Default)]
pub struct PutStreamHeaders {
    pub time_partition: String,
    pub time_partition_limit: String,
    pub custom_partition: Option<String>,
    pub static_schema_flag: bool,
    pub update_stream_flag: bool,
    pub stream_type: StreamType,
    pub log_source: LogSource,
    pub telemetry_type: TelemetryType,
}

impl From<&HeaderMap> for PutStreamHeaders {
    fn from(headers: &HeaderMap) -> Self {
        PutStreamHeaders {
            time_partition: headers
                .get(TIME_PARTITION_KEY)
                .map_or("", |v| v.to_str().unwrap())
                .to_string(),
            time_partition_limit: headers
                .get(TIME_PARTITION_LIMIT_KEY)
                .map_or("", |v| v.to_str().unwrap())
                .to_string(),
            custom_partition: headers
                .get(CUSTOM_PARTITION_KEY)
                .map(|v| v.to_str().unwrap().to_string()),
            static_schema_flag: headers
                .get(STATIC_SCHEMA_FLAG)
                .is_some_and(|v| v.to_str().unwrap() == "true"),
            update_stream_flag: headers
                .get(UPDATE_STREAM_KEY)
                .is_some_and(|v| v.to_str().unwrap() == "true"),
            stream_type: headers
                .get(STREAM_TYPE_KEY)
                .map(|v| StreamType::from(v.to_str().unwrap()))
                .unwrap_or_default(),
            log_source: headers
                .get(LOG_SOURCE_KEY)
                .map_or(LogSource::default(), |v| v.to_str().unwrap().into()),
            telemetry_type: headers
                .get(TELEMETRY_TYPE_KEY)
                .and_then(|v| v.to_str().ok())
                .map_or(TelemetryType::Logs, TelemetryType::from),
        }
    }
}

/// Resources that rely on a specific logstream and will be affected if it gets deleted
#[derive(Debug, Default, serde::Serialize)]
pub struct LogstreamAffectedResources {
    pub filters: Vec<Filter>,
    pub dashboards: Vec<LogstreamAffectedDashboard>
}

#[derive(Debug, Default, serde::Serialize)]
pub struct LogstreamAffectedDashboard {
    pub dashboard: Dashboard,
    pub affected_tile_ids: Vec<Ulid>
}

#[derive(thiserror::Error, Debug)]
pub enum LogstreamAffectedResourcesError {
    #[error("Stream not found: {0}")]
    NoSuchStream(#[from] StreamNotFound),
    
    #[error("Metastore error: {0}")]
    FromMetastoreError(#[from] MetastoreError),
}

impl LogstreamAffectedResources {
    pub async fn load(stream_name: &str) -> Self {
        Self {
            filters: LogstreamAffectedResources::fetch_affected_filters(stream_name)
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!("failed to fetch filters: {}", e);
                    Vec::new()
                }),

            dashboards: LogstreamAffectedResources::fetch_affected_dashboards(stream_name)
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!("failed to fetch dashboards: {}", e);
                    Vec::new()
                }),
        }
    }

    pub async fn fetch_affected_filters(
        stream_name: &str
    ) -> Result<Vec<Filter>, LogstreamAffectedResourcesError> {
        if !PARSEABLE.streams.contains(stream_name) {
            return Err(LogstreamAffectedResourcesError::NoSuchStream(
                StreamNotFound(stream_name.to_string())
            ));
        }

        Ok(PARSEABLE.metastore.get_filters().await?
            .into_iter()
            .filter(|filter| filter.stream_name == stream_name)
            .collect())
    }

    pub async fn fetch_affected_dashboards(
        stream_name: &str
    ) -> Result<Vec<LogstreamAffectedDashboard>, LogstreamAffectedResourcesError> {
        if !PARSEABLE.streams.contains(stream_name) {
            return Err(LogstreamAffectedResourcesError::NoSuchStream(
                StreamNotFound(stream_name.to_string())
            ));
        }

        let all_dashboards = PARSEABLE.metastore.get_dashboards().await?;
        let mut parsed_dashboards = Vec::<Dashboard>::new();

        for dashboard in all_dashboards {
            if dashboard.is_empty() {
                continue;
            }

            let dashboard_value = match serde_json::from_slice::<serde_json::Value>(&dashboard) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!("Failed to parse dashboard JSON: {}", err);
                    continue;
                }
            };

            if let Ok(dashboard) = serde_json::from_value::<Dashboard>(dashboard_value.clone()) {
                parsed_dashboards.retain(|d: &Dashboard| {
                    d.dashboard_id != dashboard.dashboard_id
                });

                parsed_dashboards.push(dashboard);
            } else {
                tracing::warn!("Failed to deserialize dashboard: {:?}", dashboard_value);
            }
        }

        let mut affected_dashboards: Vec<LogstreamAffectedDashboard> = vec![];
        
        for dashboard in parsed_dashboards {
            let Some(tiles) = dashboard.tiles.as_ref() else { 
                continue; 
            };

            println!("here");

            let mut affected_tile_ids = HashSet::<Ulid>::new();

            for tile in tiles {
                let Some(tile_fields) = tile.other_fields.as_ref() else { 
                    continue; 
                };
                
                let Some(tile_value) = tile_fields.get(TILE_FIELD_REFERRING_TO_STREAM) else {
                    continue;
                };

                

                if let Some(chart_query) = tile_value.as_str() {
                    println!("{}", chart_query);
                    if chart_query.contains(stream_name) && !affected_tile_ids.contains(&tile.tile_id) {
                        affected_tile_ids.insert(tile.tile_id);
                    }
                }
            }

            if !affected_tile_ids.is_empty() {
                affected_dashboards.push(LogstreamAffectedDashboard { 
                    dashboard, 
                    affected_tile_ids: affected_tile_ids.into_iter().collect()
                });
            }
        }

        println!("h2: {}", affected_dashboards.len());
        Ok(affected_dashboards)
    }
}