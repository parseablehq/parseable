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

use std::collections::HashSet;
use actix_web::http::header::HeaderMap;
use bytes::Bytes;
use ulid::Ulid;

use crate::{
    alerts::AlertConfig, event::format::LogSource, handlers::{
        CUSTOM_PARTITION_KEY, LOG_SOURCE_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY,
        TELEMETRY_TYPE_KEY, TIME_PARTITION_KEY, TIME_PARTITION_LIMIT_KEY, TelemetryType,
        UPDATE_STREAM_KEY,
    }, 
    metastore::MetastoreError, 
    parseable::{
        PARSEABLE, StreamNotFound
    }, 
    storage::StreamType, 
    users::dashboards::Dashboard
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
    pub filters: Vec<String>,
    pub dashboards: Vec<LogstreamAffectedDashboard>
}

#[derive(Debug, Default, serde::Serialize)]
pub struct LogstreamAffectedDashboard {
    pub dashboard_id: Ulid,
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
    ) -> Result<Vec<String>, LogstreamAffectedResourcesError> {
        if !PARSEABLE.streams.contains(stream_name) {
            return Err(LogstreamAffectedResourcesError::NoSuchStream(
                StreamNotFound(stream_name.to_string())
            ));
        }

        Ok(PARSEABLE.metastore.get_filters().await?
            .into_iter()
            .filter_map(|filter| {
                if filter.stream_name == stream_name && 
                    let Some(f_id) = filter.filter_id { 
                        Some(f_id) 
                    } else { None }
            }).collect())
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

        for dashboard_bytes in all_dashboards {
            let dashboard = match self::bytes_to_json::<Dashboard>(dashboard_bytes) {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("{}", e.to_string());
                    continue;
                }
            };

            if !parsed_dashboards.iter().any(|d| d.dashboard_id == dashboard.dashboard_id) {
                parsed_dashboards.push(dashboard);
            }
        }

        let mut affected_dashboards: Vec<LogstreamAffectedDashboard> = vec![];
        
        for (dash_i, dashboard) in parsed_dashboards.iter().enumerate() {
            let Some(tiles) = dashboard.tiles.as_ref() else { 
                continue; 
            };

            let mut affected_tile_ids = HashSet::<Ulid>::new();

            for tile in tiles {
                let Some(tile_fields) = tile.other_fields.as_ref() else { 
                    continue; 
                };
                
                let Some(tile_value) = tile_fields.get(TILE_FIELD_REFERRING_TO_STREAM) else {
                    continue;
                };

                if let Some(chart_query) = tile_value.as_str() {
                    if chart_query.contains(stream_name) && !affected_tile_ids.contains(&tile.tile_id) {
                        affected_tile_ids.insert(tile.tile_id);
                    }
                }
            }

            if !affected_tile_ids.is_empty() {
                affected_dashboards.push(LogstreamAffectedDashboard { 
                    dashboard_id: dashboard.dashboard_id.unwrap_or_else(|| {
                        tracing::warn!("dashboard {}: [id] is missing -- for logstream {}", dash_i, stream_name);
                        Ulid::new() // default to a new ULID if missing -- what else?
                    }), 
                    affected_tile_ids: affected_tile_ids.into_iter().collect()
                });
            }
        }

        Ok(affected_dashboards)
    }

    pub async fn fetch_affected_alerts(
        stream_name: &str
    ) -> Result<Vec<Ulid>, LogstreamAffectedResourcesError> {
        if !PARSEABLE.streams.contains(stream_name) {
            return Err(LogstreamAffectedResourcesError::NoSuchStream(
                StreamNotFound(stream_name.to_string())
            ));
        }

        let all_alerts = PARSEABLE.metastore.get_alerts().await?;
        
        let mut stream_alerts = HashSet::<Ulid>::new();
        for alert_bytes in all_alerts {
            let alert = match self::bytes_to_json::<AlertConfig>(alert_bytes) {
                Ok(alert_val) => alert_val,
                Err(e) => {
                    tracing::warn!("{}", e.to_string());
                    continue;
                }
            };

            if !alert.datasets.contains(&stream_name.to_string()) { continue };
            stream_alerts.insert(alert.id);
        }
        
        Ok(stream_alerts.into_iter().collect())
    }
}


// utility funcs:

#[derive(Debug, thiserror::Error)]
pub enum Bytes2JSONError {
    #[error("zero sized Bytes")]
    ZeroSizedBytes,

    #[error("failed to parse bytes to JSON: {0}")]
    FailedToParse(String)
}

fn bytes_to_json<T: serde::de::DeserializeOwned>(json_bytes: Bytes) -> Result<T, Bytes2JSONError> {
    if json_bytes.is_empty() {
        return Err(Bytes2JSONError::ZeroSizedBytes);
    }

    let json_bytes_value = match serde_json::from_slice::<serde_json::Value>(&json_bytes) {
        Ok(value) => value,
        Err(err) => {
            return Err(Bytes2JSONError::FailedToParse(format!("{:#?}", err)))
        }
    };

    return match serde_json::from_value::<T>(json_bytes_value.clone()) {
        Ok(parsed_object) => Ok(parsed_object),
        Err(e) => Err(Bytes2JSONError::FailedToParse(format!("deserialization failed: {:#?}", e)))
    };
}