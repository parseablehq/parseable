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

pub mod airplane;
pub mod http;
pub mod livetail;

const PREFIX_TAGS: &str = "x-p-tag-";
const PREFIX_META: &str = "x-p-meta-";
pub const STREAM_NAME_HEADER_KEY: &str = "x-p-stream";
const LOG_SOURCE_KEY: &str = "x-p-log-source";
const TIME_PARTITION_KEY: &str = "x-p-time-partition";
const TIME_PARTITION_LIMIT_KEY: &str = "x-p-time-partition-limit";
const CUSTOM_PARTITION_KEY: &str = "x-p-custom-partition";
const STATIC_SCHEMA_FLAG: &str = "x-p-static-schema-flag";
const AUTHORIZATION_KEY: &str = "authorization";
const SEPARATOR: char = '^';
const UPDATE_STREAM_KEY: &str = "x-p-update-stream";
const STREAM_TYPE_KEY: &str = "x-p-stream-type";
const OIDC_SCOPE: &str = "openid profile email";
const COOKIE_AGE_DAYS: usize = 7;
const SESSION_COOKIE_NAME: &str = "session";
const USER_COOKIE_NAME: &str = "username";

//constants for trino
const TRINO_SCHEMA: &str = "x-trino-schema";
const TRINO_CATALOG: &str = "x-trino-catalog";
const TRINO_USER: &str = "x-trino-user";

// constants for log Source values for known sources and formats
const LOG_SOURCE_KINESIS: &str = "kinesis";

// OpenTelemetry sends data in JSON format with
// specification as explained here https://opentelemetry.io/docs/specs/otel/logs/data-model/
const LOG_SOURCE_OTEL: &str = "otel";

// AWS Kinesis constants
const KINESIS_COMMON_ATTRIBUTES_KEY: &str = "x-amz-firehose-common-attributes";
