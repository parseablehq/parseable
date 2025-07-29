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

use std::fmt::Display;

use serde::{Deserialize, Serialize};

pub mod airplane;
pub mod http;
pub mod livetail;

pub const STREAM_NAME_HEADER_KEY: &str = "x-p-stream";
const LOG_SOURCE_KEY: &str = "x-p-log-source";
const EXTRACT_LOG_KEY: &str = "x-p-extract-log";
const TIME_PARTITION_KEY: &str = "x-p-time-partition";
const TIME_PARTITION_LIMIT_KEY: &str = "x-p-time-partition-limit";
const CUSTOM_PARTITION_KEY: &str = "x-p-custom-partition";
const STATIC_SCHEMA_FLAG: &str = "x-p-static-schema-flag";
const AUTHORIZATION_KEY: &str = "authorization";
const UPDATE_STREAM_KEY: &str = "x-p-update-stream";
pub const STREAM_TYPE_KEY: &str = "x-p-stream-type";
pub const TELEMETRY_TYPE_KEY: &str = "x-p-telemetry-type";
const COOKIE_AGE_DAYS: usize = 7;
const SESSION_COOKIE_NAME: &str = "session";
const USER_COOKIE_NAME: &str = "username";

// constants for log Source values for known sources and formats
const LOG_SOURCE_KINESIS: &str = "kinesis";

// AWS Kinesis constants
const KINESIS_COMMON_ATTRIBUTES_KEY: &str = "x-amz-firehose-common-attributes";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TelemetryType {
    #[default]
    Logs,
    Metrics,
    Traces,
    Events,
}

impl From<&str> for TelemetryType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "logs" => TelemetryType::Logs,
            "metrics" => TelemetryType::Metrics,
            "traces" => TelemetryType::Traces,
            "events" => TelemetryType::Events,
            _ => TelemetryType::Logs,
        }
    }
}

impl Display for TelemetryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TelemetryType::Logs => "logs",
            TelemetryType::Metrics => "metrics",
            TelemetryType::Traces => "traces",
            TelemetryType::Events => "events",
        })
    }
}
