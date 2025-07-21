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

use actix_web::HttpRequest;
use chrono::Utc;
use http::header::USER_AGENT;
use opentelemetry_proto::tonic::{
    logs::v1::LogsData, metrics::v1::MetricsData, trace::v1::TracesData,
};
use serde_json::Value;
use std::collections::HashMap;
use tracing::warn;

use crate::{
    event::{
        FORMAT_KEY, SOURCE_IP_KEY, USER_AGENT_KEY,
        format::{EventFormat, LogSource, json},
    },
    handlers::{
        EXTRACT_LOG_KEY, LOG_SOURCE_KEY, STREAM_NAME_HEADER_KEY,
        http::{
            ingest::PostError,
            kinesis::{Message, flatten_kinesis_logs},
        },
    },
    otel::{logs::flatten_otel_logs, metrics::flatten_otel_metrics, traces::flatten_otel_traces},
    parseable::PARSEABLE,
    storage::StreamType,
    utils::json::{convert_array_to_object, flatten::convert_to_array},
};

const IGNORE_HEADERS: [&str; 3] = [STREAM_NAME_HEADER_KEY, LOG_SOURCE_KEY, EXTRACT_LOG_KEY];
const MAX_CUSTOM_FIELDS: usize = 10;
const MAX_FIELD_VALUE_LENGTH: usize = 100;

pub async fn flatten_and_push_logs(
    json: Value,
    stream_name: &str,
    log_source: &LogSource,
    p_custom_fields: &HashMap<String, String>,
) -> Result<(), PostError> {
    // Verify the dataset fields count
    verify_dataset_fields_count(stream_name)?;

    match log_source {
        LogSource::Kinesis => {
            //custom flattening required for Amazon Kinesis
            let message: Message = serde_json::from_value(json)?;
            let flattened_kinesis_data = flatten_kinesis_logs(message).await?;
            let record = convert_to_array(flattened_kinesis_data)?;
            push_logs(stream_name, record, log_source, p_custom_fields).await?;
        }
        LogSource::OtelLogs => {
            //custom flattening required for otel logs
            let logs: LogsData = serde_json::from_value(json)?;
            for record in flatten_otel_logs(&logs) {
                push_logs(stream_name, record, log_source, p_custom_fields).await?;
            }
        }
        LogSource::OtelTraces => {
            //custom flattening required for otel traces
            let traces: TracesData = serde_json::from_value(json)?;
            for record in flatten_otel_traces(&traces) {
                push_logs(stream_name, record, log_source, p_custom_fields).await?;
            }
        }
        LogSource::OtelMetrics => {
            //custom flattening required for otel metrics
            let metrics: MetricsData = serde_json::from_value(json)?;
            for record in flatten_otel_metrics(metrics) {
                push_logs(stream_name, record, log_source, p_custom_fields).await?;
            }
        }
        _ => push_logs(stream_name, json, log_source, p_custom_fields).await?,
    }

    Ok(())
}

async fn push_logs(
    stream_name: &str,
    json: Value,
    log_source: &LogSource,
    p_custom_fields: &HashMap<String, String>,
) -> Result<(), PostError> {
    let stream = PARSEABLE.get_stream(stream_name)?;
    let time_partition = stream.get_time_partition();
    let time_partition_limit = PARSEABLE
        .get_stream(stream_name)?
        .get_time_partition_limit();
    let static_schema_flag = stream.get_static_schema_flag();
    let custom_partition = stream.get_custom_partition();
    let schema_version = stream.get_schema_version();
    let p_timestamp = Utc::now();

    let data = if time_partition.is_some() || custom_partition.is_some() {
        convert_array_to_object(
            json,
            time_partition.as_ref(),
            time_partition_limit,
            custom_partition.as_ref(),
            schema_version,
            log_source,
        )?
    } else {
        vec![convert_to_array(convert_array_to_object(
            json,
            None,
            None,
            None,
            schema_version,
            log_source,
        )?)?]
    };

    for json in data {
        let origin_size = serde_json::to_vec(&json).unwrap().len() as u64; // string length need not be the same as byte length
        let schema = PARSEABLE.get_stream(stream_name)?.get_schema_raw();
        json::Event { json, p_timestamp }
            .into_event(
                stream_name.to_owned(),
                origin_size,
                &schema,
                static_schema_flag,
                custom_partition.as_ref(),
                time_partition.as_ref(),
                schema_version,
                StreamType::UserDefined,
                p_custom_fields,
            )?
            .process()?;
    }
    Ok(())
}

pub fn get_custom_fields_from_header(req: &HttpRequest) -> HashMap<String, String> {
    let user_agent = req
        .headers()
        .get(USER_AGENT)
        .and_then(|a| a.to_str().ok())
        .unwrap_or_default();

    let conn = req.connection_info().clone();

    let source_ip = conn.realip_remote_addr().unwrap_or_default();
    let mut p_custom_fields = HashMap::new();
    p_custom_fields.insert(USER_AGENT_KEY.to_string(), user_agent.to_string());
    p_custom_fields.insert(SOURCE_IP_KEY.to_string(), source_ip.to_string());

    // Iterate through headers and add custom fields
    for (header_name, header_value) in req.headers().iter() {
        // Check if we've reached the maximum number of custom fields
        if p_custom_fields.len() >= MAX_CUSTOM_FIELDS {
            warn!(
                "Maximum number of custom fields ({}) reached, ignoring remaining headers",
                MAX_CUSTOM_FIELDS
            );
            break;
        }

        let header_name = header_name.as_str();
        if header_name.starts_with("x-p-") && !IGNORE_HEADERS.contains(&header_name) {
            if let Ok(value) = header_value.to_str() {
                let key = header_name.trim_start_matches("x-p-");
                if !key.is_empty() {
                    // Truncate value if it exceeds the maximum length
                    let truncated_value = if value.len() > MAX_FIELD_VALUE_LENGTH {
                        warn!(
                            "Header value for '{}' exceeds maximum length, truncating",
                            header_name
                        );
                        &value[..MAX_FIELD_VALUE_LENGTH]
                    } else {
                        value
                    };
                    p_custom_fields.insert(key.to_string(), truncated_value.to_string());
                } else {
                    warn!(
                        "Ignoring header with empty key after prefix: {}",
                        header_name
                    );
                }
            }
        }

        if header_name == LOG_SOURCE_KEY {
            if let Ok(value) = header_value.to_str() {
                p_custom_fields.insert(FORMAT_KEY.to_string(), value.to_string());
            }
        }
    }

    p_custom_fields
}

fn verify_dataset_fields_count(stream_name: &str) -> Result<(), PostError> {
    let fields_count = PARSEABLE
        .get_stream(stream_name)?
        .get_schema()
        .fields()
        .len();
    let dataset_fields_warn_threshold = 0.8 * PARSEABLE.options.dataset_fields_allowed_limit as f64;
    // Check if the fields count exceeds the warn threshold
    if fields_count > dataset_fields_warn_threshold as usize {
        tracing::warn!(
            "Dataset {0} has {1} fields, which exceeds the warning threshold of {2}. Ingestion will not be possible after reaching {3} fields. We recommend creating a new dataset.",
            stream_name,
            fields_count,
            dataset_fields_warn_threshold as usize,
            PARSEABLE.options.dataset_fields_allowed_limit
        );
    }
    // Check if the fields count exceeds the limit
    // Return an error if the fields count exceeds the limit
    if fields_count > PARSEABLE.options.dataset_fields_allowed_limit {
        let error = PostError::FieldsCountLimitExceeded(
            stream_name.to_string(),
            fields_count,
            PARSEABLE.options.dataset_fields_allowed_limit,
        );
        tracing::error!("{}", error);
        // Return an error if the fields count exceeds the limit
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;

    #[test]
    fn test_get_custom_fields_from_header_with_custom_fields() {
        let req = TestRequest::default()
            .insert_header((USER_AGENT, "TestUserAgent"))
            .insert_header(("x-p-environment", "dev"))
            .to_http_request();

        let custom_fields = get_custom_fields_from_header(&req);

        assert_eq!(custom_fields.get(USER_AGENT_KEY).unwrap(), "TestUserAgent");
        assert_eq!(custom_fields.get("environment").unwrap(), "dev");
    }

    #[test]
    fn test_get_custom_fields_from_header_with_ignored_headers() {
        let req = TestRequest::default()
            .insert_header((USER_AGENT, "TestUserAgent"))
            .insert_header((STREAM_NAME_HEADER_KEY, "teststream"))
            .to_http_request();

        let custom_fields = get_custom_fields_from_header(&req);

        assert_eq!(custom_fields.get(USER_AGENT_KEY).unwrap(), "TestUserAgent");
        assert!(!custom_fields.contains_key(STREAM_NAME_HEADER_KEY));
    }

    #[test]
    fn test_get_custom_fields_from_header_with_format_key() {
        let req = TestRequest::default()
            .insert_header((USER_AGENT, "TestUserAgent"))
            .insert_header((LOG_SOURCE_KEY, "otel-logs"))
            .to_http_request();

        let custom_fields = get_custom_fields_from_header(&req);

        assert_eq!(custom_fields.get(USER_AGENT_KEY).unwrap(), "TestUserAgent");
        assert_eq!(custom_fields.get(FORMAT_KEY).unwrap(), "otel-logs");
    }

    #[test]
    fn test_get_custom_fields_empty_header_after_prefix() {
        let req = TestRequest::default()
            .insert_header(("x-p-", "empty"))
            .to_http_request();

        let custom_fields = get_custom_fields_from_header(&req);

        assert_eq!(custom_fields.len(), 2);
        assert_eq!(custom_fields.get(USER_AGENT_KEY).unwrap(), "");
        assert_eq!(custom_fields.get(SOURCE_IP_KEY).unwrap(), "");
    }
}
