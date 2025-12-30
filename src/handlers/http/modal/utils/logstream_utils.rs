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

use actix_web::http::header::HeaderMap;
use bytes::Bytes;
use ulid::Ulid;

use crate::{
    alerts::AlertConfig, 
    event::format::LogSource, 
    handlers::{
        CUSTOM_PARTITION_KEY, LOG_SOURCE_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY,
        TELEMETRY_TYPE_KEY, TIME_PARTITION_KEY, TIME_PARTITION_LIMIT_KEY, TelemetryType,
        UPDATE_STREAM_KEY, http::logstream::error::StreamError,
    }, 
    metastore::MetastoreError, 
    parseable::{PARSEABLE, StreamNotFound}, 
    rbac::role::{
        ParseableResourceType, 
        model::DefaultPrivilege
    }, 
    storage::{ObjectStorageError, StorageMetadata, StreamType}, 
    users::dashboards::Dashboard
};

/// Field in a dashboard's tile that should contain the logstream name
const TILE_FIELD_REFERRING_TO_STREAM: &str = "dbName";

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
    pub dashboards: Vec<LogstreamAffectedDashboard>,
    pub alerts: Vec<Ulid>,
    pub roles: Vec<String>,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct LogstreamAffectedDashboard {
    pub dashboard_id: Ulid,
    pub affected_tile_ids: Vec<Ulid>
}

#[derive(thiserror::Error, Debug)]
pub enum LogstreamAffectedResourcesError {
    #[error("(to fetch affected resources) logstream not found: {0}")]
    StreamNotFound(#[from] StreamNotFound),
    
    #[error("(get affected resources) metastore error: {0}")]
    MetastoreError(#[from] MetastoreError),

    #[error("(get affected resources) objectstore error: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),

    #[error("(get affected resources) could not parse JSON: {0}")]
    Bytes2JSONError(#[from] Bytes2JSONError)
}

impl LogstreamAffectedResources {
    pub async fn load(stream_name: &str) -> Result<Self, LogstreamAffectedResourcesError> {
        Ok(Self {
            filters: Self::fetch_affected_filters(stream_name).await?,
            dashboards: Self::fetch_affected_dashboards(stream_name).await?,
            alerts: Self::fetch_affected_alerts(stream_name).await?,
            roles: Self::fetch_affected_roles(stream_name).await?,
        })
    }

    pub async fn fetch_affected_filters(
        stream_name: &str
    ) -> Result<Vec<String>, LogstreamAffectedResourcesError> {
        if !PARSEABLE.streams.contains(stream_name) {
            return Err(StreamNotFound(stream_name.to_string()).into());
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
            return Err(StreamNotFound(stream_name.to_string()).into());
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

            let mut affected_tile_ids = Vec::<Ulid>::new();
            for tile in tiles {
                let Some(tile_fields) = tile.other_fields.as_ref() else { 
                    continue; 
                };
                
                let Some(tile_value) = tile_fields.get(TILE_FIELD_REFERRING_TO_STREAM) else {
                    continue;
                };

                if let Some(db_names) = tile_value.as_array() {
                    let dbs_have_stream = db_names
                        .iter()
                        .any(|db| {
                            if let Some(db_str) = db.as_str() {
                                return db_str == stream_name
                            } else { false }
                        });

                    if dbs_have_stream && !affected_tile_ids.contains(&tile.tile_id) {
                        affected_tile_ids.push(tile.tile_id);
                    }
                }
            }

            if !affected_tile_ids.is_empty() && dashboard.dashboard_id.is_some()  {
                affected_dashboards.push(LogstreamAffectedDashboard { 
                    dashboard_id: dashboard.dashboard_id.unwrap(),
                    affected_tile_ids
                });
            } else if !affected_tile_ids.is_empty() { 
                tracing::warn!("dashboard {}: [id] is missing, skipping -- for logstream {}", dash_i, stream_name); 
            }
        }

        Ok(affected_dashboards)
    }

    pub async fn fetch_affected_alerts(
        stream_name: &str
    ) -> Result<Vec<Ulid>, LogstreamAffectedResourcesError> {
        if !PARSEABLE.streams.contains(stream_name) {
            return Err(StreamNotFound(stream_name.to_string()).into());
        }

        let all_alerts = PARSEABLE.metastore.get_alerts().await?;
        
        let mut stream_alerts = Vec::<Ulid>::new();
        for alert_bytes in all_alerts {
            let alert = match self::bytes_to_json::<AlertConfig>(alert_bytes) {
                Ok(alert_val) => alert_val,
                Err(e) => {
                    tracing::warn!("{}", e.to_string());
                    continue;
                }
            };

            if !alert.datasets.iter().any(|s| s == stream_name) { 
                continue 
            };
            
            if !stream_alerts.contains(&alert.id) {
                stream_alerts.push(alert.id);
            }
        }
        
        Ok(stream_alerts)
    }


    pub async fn fetch_affected_roles(
        stream_name: &str
    ) -> Result<Vec<String>, LogstreamAffectedResourcesError> {
        if !PARSEABLE.streams.contains(stream_name) {
            return Err(StreamNotFound(stream_name.to_string()).into());
        }

        let metadata_bytes = PARSEABLE
            .metastore
            .get_parseable_metadata()
            .await
            .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?
            .ok_or_else(|| ObjectStorageError::Custom("parseable metadata not initialized".into()))?;

        let metadata = self::bytes_to_json::<StorageMetadata>(metadata_bytes)?;

        let mut stream_associated_roles = Vec::<String>::new();
        for (role_name, privileges) in &metadata.roles {
            for privilege in privileges {

                let associated_stream = match privilege {
                    DefaultPrivilege::Ingestor { resource } => {
                        match resource {
                            ParseableResourceType::Stream(stream) => stream,
                            _ => continue
                        }
                    },

                    DefaultPrivilege::Reader { resource } => {
                        match resource {
                            ParseableResourceType::Stream(stream) => stream,
                            _ => continue
                        }
                    },

                    DefaultPrivilege::Writer { resource } => {
                        match resource {
                            ParseableResourceType::Stream(stream) => stream,
                            _ => continue
                        }
                    },

                    _ => continue
                };

                if associated_stream == stream_name && !stream_associated_roles.contains(role_name) {
                    stream_associated_roles.push(role_name.to_string());
                    
                    // if any role privilege matches the input stream, 
                    // add the role to the set and break
                    break; 
                }

            }
        }

        Ok(stream_associated_roles)
    }
}


impl From<LogstreamAffectedResourcesError> for StreamError {
    fn from(err: LogstreamAffectedResourcesError) -> Self {
        match err {
            LogstreamAffectedResourcesError::StreamNotFound(e) => {
                StreamError::StreamNotFound(e)
            }
            LogstreamAffectedResourcesError::MetastoreError(e) => {
                StreamError::MetastoreError(e)
            }
            other => {
                StreamError::Anyhow(anyhow::anyhow!(other.to_string()))
            }
        }
    }
}


// utility:

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