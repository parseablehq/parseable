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

use std::num::NonZeroU32;

use actix_web::http::header::HeaderMap;

use crate::{
    event::format::LogSource,
    handlers::{
        CUSTOM_PARTITION_KEY, LOG_SOURCE_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY,
        TIME_PARTITION_KEY, TIME_PARTITION_LIMIT_KEY, UPDATE_STREAM_KEY,
    },
    storage::StreamType,
};

/// Name of a field that appears within a data stream
pub type FieldName = String;

/// Name of the field used as a custom partition
pub type CustomPartition = String;

#[derive(Debug, thiserror::Error)]
pub enum HeaderParseError {
    #[error("Maximum 3 custom partition keys are supported")]
    TooManyPartitions,
    #[error("Missing 'd' suffix for duration value")]
    UnsupportedUnit,
    #[error("Could not convert duration to an unsigned number")]
    ZeroOrNegative,
}

#[derive(Debug, Default)]
pub struct PutStreamHeaders {
    pub time_partition: Option<FieldName>,
    pub time_partition_limit: Option<NonZeroU32>,
    pub custom_partitions: Vec<String>,
    pub static_schema_flag: bool,
    pub update_stream_flag: bool,
    pub stream_type: StreamType,
    pub log_source: LogSource,
}

impl TryFrom<&HeaderMap> for PutStreamHeaders {
    type Error = HeaderParseError;

    fn try_from(headers: &HeaderMap) -> Result<Self, Self::Error> {
        let time_partition = headers
            .get(TIME_PARTITION_KEY)
            .map(|v| v.to_str().unwrap().to_owned());
        let time_partition_limit = match headers
            .get(TIME_PARTITION_LIMIT_KEY)
            .map(|v| v.to_str().unwrap())
        {
            Some(limit) => Some(parse_time_partition_limit(limit)?),
            None => None,
        };
        let custom_partition = headers
            .get(CUSTOM_PARTITION_KEY)
            .map(|v| v.to_str().unwrap())
            .unwrap_or_default();
        let custom_partitions = parse_custom_partition(custom_partition)?;

        let headers = PutStreamHeaders {
            time_partition,
            time_partition_limit,
            custom_partitions,
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
        };

        Ok(headers)
    }
}

pub fn parse_custom_partition(
    custom_partition: &str,
) -> Result<Vec<CustomPartition>, HeaderParseError> {
    let custom_partition_list = custom_partition
        .split(',')
        .map(String::from)
        .collect::<Vec<String>>();
    if custom_partition_list.len() > 3 {
        return Err(HeaderParseError::TooManyPartitions);
    }

    Ok(custom_partition_list)
}

pub fn parse_time_partition_limit(
    time_partition_limit: &str,
) -> Result<NonZeroU32, HeaderParseError> {
    if !time_partition_limit.ends_with('d') {
        return Err(HeaderParseError::UnsupportedUnit);
    }
    let days = &time_partition_limit[0..time_partition_limit.len() - 1];
    let Ok(days) = days.parse::<NonZeroU32>() else {
        return Err(HeaderParseError::ZeroOrNegative);
    };

    Ok(days)
}
