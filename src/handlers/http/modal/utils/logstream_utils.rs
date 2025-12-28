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

use crate::{
    event::format::LogSource, 
    handlers::{
        CUSTOM_PARTITION_KEY, LOG_SOURCE_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY,
        TELEMETRY_TYPE_KEY, TIME_PARTITION_KEY, TIME_PARTITION_LIMIT_KEY, TelemetryType,
        UPDATE_STREAM_KEY,
    }, 
    metastore::MetastoreError, 
    parseable::PARSEABLE, 
    storage::StreamType, 
    users::filters::{FILTERS, Filter}
};

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


#[derive(thiserror::Error, Debug)]
pub enum ZombieFiltersDeletionError {
    #[error("{0}")]
    StreamStillExists(#[from] StreamFoundForZombieFilters),

    #[error("Metastore error: {0}")]
    GetFromMetastore(#[from] MetastoreError)
}

#[derive(Debug, thiserror::Error)]
#[error("Stream still exists for zombie filters: {0}")]
pub struct StreamFoundForZombieFilters(String);


pub async fn delete_zombie_filters(stream_name: &str) -> Result<(), ZombieFiltersDeletionError> {
    // stream should not exist in order to have zombie filters
    if PARSEABLE.streams.contains(stream_name) {
        return Err(ZombieFiltersDeletionError::StreamStillExists(
            StreamFoundForZombieFilters(stream_name.to_string())
        ));
    }

    let all_filters = PARSEABLE.metastore.get_filters().await?;

    // collect filters associated with the logstream being deleted
    let filters_for_stream: Vec<Filter> = all_filters
        .into_iter()
        .filter(|filter| filter.stream_name == stream_name)
        .collect();

    for filter in filters_for_stream.iter() {
        if let Err(err) = PARSEABLE.metastore.delete_filter(filter).await {
            tracing::warn!(
                "failed to delete the zombie filter: {} \nfrom storage. For logstream: {}\nError: {:#?}", 
                filter.filter_name,
                stream_name,
                err 
            );
        } else { // ok: have the filter removed from memory only when the storage deletion succeeds
            if let Some(filter_id) = filter.filter_id.as_ref() {
                FILTERS.delete_filter(filter_id).await;
            }
        }
    }

    Ok(())
}