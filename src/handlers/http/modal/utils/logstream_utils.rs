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

use crate::{
    event::format::LogSource,
    handlers::{
        CUSTOM_PARTITION_KEY, DATASET_LABELS_KEY, DATASET_TAG_KEY, DATASET_TAGS_KEY, DatasetTag,
        LOG_SOURCE_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY, TELEMETRY_TYPE_KEY,
        TIME_PARTITION_KEY, TIME_PARTITION_LIMIT_KEY, TelemetryType, UPDATE_STREAM_KEY,
        parse_dataset_labels, parse_dataset_tags,
    },
    storage::StreamType,
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
    pub dataset_tags: Vec<DatasetTag>,
    pub dataset_labels: Vec<String>,
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
            dataset_tags: headers
                .get(DATASET_TAGS_KEY)
                .or_else(|| headers.get(DATASET_TAG_KEY))
                .and_then(|v| v.to_str().ok())
                .map(parse_dataset_tags)
                .unwrap_or_default(),
            dataset_labels: headers
                .get(DATASET_LABELS_KEY)
                .and_then(|v| v.to_str().ok())
                .map(parse_dataset_labels)
                .unwrap_or_default(),
        }
    }
}
