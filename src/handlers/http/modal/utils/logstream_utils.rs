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
        INFER_TIMESTAMP_KEY, LOG_SOURCE_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY,
        TELEMETRY_TYPE_KEY, TIME_PARTITION_KEY, TIME_PARTITION_LIMIT_KEY, TelemetryType,
        UPDATE_STREAM_KEY, parse_dataset_labels, parse_dataset_tags,
    },
    storage::StreamType,
};

#[derive(Debug)]
pub struct PutStreamHeaders {
    pub time_partition: String,
    pub time_partition_limit: String,
    pub custom_partition: Option<String>,
    pub static_schema_flag: bool,
    pub update_stream_flag: bool,
    pub stream_type: StreamType,
    pub log_source: LogSource,
    pub telemetry_type: TelemetryType,
    pub telemetry_type_set: bool,
    pub dataset_tags: Vec<DatasetTag>,
    pub dataset_labels: Vec<String>,
    pub infer_timestamp: bool,
    pub infer_timestamp_set: bool,
}

impl Default for PutStreamHeaders {
    fn default() -> Self {
        Self {
            time_partition: String::default(),
            time_partition_limit: String::default(),
            custom_partition: None,
            static_schema_flag: false,
            update_stream_flag: false,
            stream_type: StreamType::default(),
            log_source: LogSource::default(),
            telemetry_type: TelemetryType::default(),
            telemetry_type_set: false,
            dataset_tags: Vec::new(),
            dataset_labels: Vec::new(),
            infer_timestamp: true,
            infer_timestamp_set: false,
        }
    }
}

impl From<&HeaderMap> for PutStreamHeaders {
    fn from(headers: &HeaderMap) -> Self {
        // Track raw header presence first so a present-but-unparseable header
        // (non-UTF-8 bytes, etc.) still trips downstream validation gates
        // instead of silently being treated as absent.
        let infer_timestamp_set = headers.contains_key(INFER_TIMESTAMP_KEY);
        let infer_timestamp_value = headers
            .get(INFER_TIMESTAMP_KEY)
            .and_then(|v| v.to_str().ok());
        let telemetry_type_set = headers.contains_key(TELEMETRY_TYPE_KEY);
        let telemetry_type_value = headers
            .get(TELEMETRY_TYPE_KEY)
            .and_then(|v| v.to_str().ok());
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
            telemetry_type: telemetry_type_value.map_or(TelemetryType::Logs, TelemetryType::from),
            telemetry_type_set,
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
            infer_timestamp: infer_timestamp_value.is_none_or(|v| !v.eq_ignore_ascii_case("false")),
            infer_timestamp_set,
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};

    use super::PutStreamHeaders;
    use crate::handlers::{INFER_TIMESTAMP_KEY, TELEMETRY_TYPE_KEY, TelemetryType};

    fn headers_with_infer(value: &str) -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(
            HeaderName::from_static(INFER_TIMESTAMP_KEY),
            HeaderValue::from_str(value).unwrap(),
        );
        map
    }

    #[test]
    fn defaults_to_true_when_header_absent() {
        let headers = HeaderMap::new();
        let parsed = PutStreamHeaders::from(&headers);
        assert!(parsed.infer_timestamp);
        assert!(!parsed.infer_timestamp_set);
    }

    #[test]
    fn parses_false_value() {
        let parsed = PutStreamHeaders::from(&headers_with_infer("false"));
        assert!(!parsed.infer_timestamp);
        assert!(parsed.infer_timestamp_set);
    }

    #[test]
    fn parses_true_value() {
        let parsed = PutStreamHeaders::from(&headers_with_infer("true"));
        assert!(parsed.infer_timestamp);
        assert!(parsed.infer_timestamp_set);
    }

    #[test]
    fn case_insensitive_false() {
        let parsed = PutStreamHeaders::from(&headers_with_infer("FALSE"));
        assert!(!parsed.infer_timestamp);
        assert!(parsed.infer_timestamp_set);
    }

    #[test]
    fn unknown_value_treated_as_true() {
        // Anything not matching "false" (case-insensitive) keeps the default behavior
        // of inferring timestamps; flag is still considered explicitly set.
        let parsed = PutStreamHeaders::from(&headers_with_infer("yes"));
        assert!(parsed.infer_timestamp);
        assert!(parsed.infer_timestamp_set);
    }

    #[test]
    fn telemetry_type_set_flag_tracks_header_presence() {
        // Header absent -> flag is false, value defaults to Logs.
        let parsed = PutStreamHeaders::from(&HeaderMap::new());
        assert!(!parsed.telemetry_type_set);
        assert_eq!(parsed.telemetry_type, TelemetryType::Logs);

        // Header present -> flag is true, value parsed from header.
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static(TELEMETRY_TYPE_KEY),
            HeaderValue::from_static("metrics"),
        );
        let parsed = PutStreamHeaders::from(&h);
        assert!(parsed.telemetry_type_set);
        assert_eq!(parsed.telemetry_type, TelemetryType::Metrics);
    }

    #[test]
    fn infer_timestamp_set_when_header_present_but_non_utf8() {
        // A header with non-UTF-8 bytes must still be treated as "set" so
        // downstream validation (otel-metrics-only, create-only) sees it and
        // can reject it. Previously the parsed Option<&str> was used as the
        // presence signal, letting non-UTF-8 silently bypass validation.
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static(INFER_TIMESTAMP_KEY),
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );
        let parsed = PutStreamHeaders::from(&h);
        assert!(parsed.infer_timestamp_set);
        // Value couldn't be parsed -> falls back to default (true), but the
        // _set flag still ensures downstream gates fire.
        assert!(parsed.infer_timestamp);
    }

    #[test]
    fn telemetry_type_set_when_header_present_but_non_utf8() {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static(TELEMETRY_TYPE_KEY),
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );
        let parsed = PutStreamHeaders::from(&h);
        assert!(parsed.telemetry_type_set);
        assert_eq!(parsed.telemetry_type, TelemetryType::Logs);
    }
}
