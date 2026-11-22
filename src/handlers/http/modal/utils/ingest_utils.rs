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

use actix_web::http::header::USER_AGENT;
use actix_web::{HttpRequest, web};
use chrono::Utc;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};
use opentelemetry_proto::tonic::{
    logs::v1::LogsData, metrics::v1::MetricsData, trace::v1::TracesData,
};
use prost::Message as OtelProstMessage;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::oneshot;
use tracing::{error, info_span, instrument, warn};

use crate::event::error::EventError;
use crate::{
    LOCK_EXPECT,
    event::{
        FORMAT_KEY, SOURCE_IP_KEY, USER_AGENT_KEY,
        format::{EventFormat, LogSource, json},
    },
    handlers::{
        CONTENT_TYPE_JSON, CONTENT_TYPE_PROTOBUF, EXTRACT_LOG_KEY, LOG_SOURCE_KEY,
        STREAM_NAME_HEADER_KEY, TelemetryType,
        http::{
            ingest::PostError,
            kinesis::{Message, flatten_kinesis_logs},
            max_event_payload_size,
        },
    },
    otel::{
        logs::{flatten_otel_logs, flatten_otel_protobuf as flatten_otel_logs_protobuf},
        metrics::{flatten_otel_metrics, flatten_otel_metrics_protobuf},
        otel_utils::deserialize_otel_json,
        traces::{flatten_otel_traces, flatten_otel_traces_protobuf},
    },
    parseable::{DEFAULT_TENANT, PARSEABLE},
    storage::StreamType,
    utils::{
        get_tenant_id_from_request,
        json::{convert_array_to_object, flatten::convert_to_array},
    },
};

const IGNORE_HEADERS: [&str; 3] = [STREAM_NAME_HEADER_KEY, LOG_SOURCE_KEY, EXTRACT_LOG_KEY];
const MAX_CUSTOM_FIELDS: usize = 10;
const MAX_FIELD_VALUE_LENGTH: usize = 100;

pub fn ingest_helper(
    stream_name: String,
    tenant_id: Option<String>,
    log_source: LogSource,
    telemetry_type: TelemetryType,
    p_custom_fields: HashMap<String, String>,
    json: Value,
    time_partition: Option<String>,
) -> Result<(), PostError> {
    let result = match &log_source {
        LogSource::Kinesis => decode_flatten_and_push_kinesis(
            json,
            &stream_name,
            &p_custom_fields,
            time_partition,
            telemetry_type,
            &tenant_id,
        ),
        LogSource::OtelLogs | LogSource::OtelMetrics | LogSource::OtelTraces => {
            Err(PostError::OtelNotSupported(stream_name.clone()))
        }
        _ => push_logs(
            &stream_name,
            json,
            &log_source,
            &p_custom_fields,
            time_partition,
            telemetry_type,
            &tenant_id,
        ),
    };

    if let Err(e) = result {
        error!("Ingestion failed for stream {stream_name}: {e}");
        return Err(e);
    }

    Ok(())
}

fn decode_flatten_and_push_kinesis(
    json: Value,
    stream_name: &str,
    p_custom_fields: &HashMap<String, String>,
    time_partition: Option<String>,
    telemetry_type: TelemetryType,
    tenant_id: &Option<String>,
) -> Result<(), PostError> {
    let message: Message = serde_json::from_value(json)?;
    let records = convert_to_array(flatten_kinesis_logs(message)?)?;
    push_logs(
        stream_name,
        records,
        &LogSource::Kinesis,
        p_custom_fields,
        time_partition,
        telemetry_type,
        tenant_id,
    )
}

// Common content processing for OTEL ingestion
pub async fn process_otel_content(
    req: &HttpRequest,
    body: web::Bytes,
    stream_name: &str,
    log_source: &LogSource,
    telemetry_type: TelemetryType,
) -> Result<(), PostError> {
    let p_custom_fields = get_otel_custom_fields_from_header(req, log_source);
    let content_type = req
        .headers()
        .get("Content-Type")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);
    let tenant_id = get_tenant_id_from_request(req);
    let (s, r) = oneshot::channel();
    let stream_name = stream_name.to_string();
    let log_source = log_source.clone();

    rayon::spawn(move || {
        let r = handle_otel_ingestion(
            body,
            p_custom_fields,
            stream_name,
            log_source,
            telemetry_type,
            content_type,
            tenant_id,
        );
        let _ = s.send(r);
    });

    if let Err(e) = r.await.map_err(|e| PostError::CustomError(e.to_string()))? {
        Err(e)
    } else {
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_otel_ingestion(
    body: web::Bytes,
    p_custom_fields: HashMap<String, String>,
    stream_name: String,
    log_source: LogSource,
    telemetry_type: TelemetryType,
    content_type: Option<String>,
    tenant_id: Option<String>,
) -> Result<(), PostError> {
    let Some(content_type) = content_type else {
        let message = format!(
            "Ingestion failed for stream {stream_name}: Missing Content-Type header. Expected application/json or application/x-protobuf"
        );
        error!("{message}");
        return Err(PostError::Invalid(anyhow::anyhow!(message)));
    };
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let records = {
        let span = info_span!(
            "flatten_otel_payload",
            stream_name,
            content_type,
            record_count = tracing::field::Empty,
        );
        let _guard = span.enter();
        let records = match content_type.as_str() {
            CONTENT_TYPE_JSON => {
                let json = serde_json::from_slice(&body).map_err(|error| {
                    error!(
                        "Ingestion failed for stream {stream_name}: malformed JSON in request body"
                    );
                    PostError::SerdeError(error)
                })?;
                decode_and_flatten_otel_json(
                    json,
                    &log_source,
                    tenant,
                    PARSEABLE.options.normalise_otel_payload,
                )
                .map_err(|error| {
                    error!("Ingestion failed for stream {stream_name}: {error}");
                    error
                })?
            }
            CONTENT_TYPE_PROTOBUF => {
                if body.len() > max_event_payload_size() {
                    let message = format!(
                        "Ingestion failed for stream {stream_name}: Protobuf message size {} exceeds maximum allowed size of {} bytes",
                        body.len(),
                        max_event_payload_size(),
                    );
                    error!("{message}");
                    return Err(PostError::Invalid(anyhow::anyhow!(message)));
                }
                decode_and_flatten_otel_protobuf(body.as_ref(), &log_source, tenant).map_err(
                    |error| {
                        let message = format!(
                            "Ingestion failed for stream {stream_name}: Failed to decode protobuf message: {error}"
                        );
                        error!("{message}");
                        PostError::Invalid(anyhow::anyhow!(message))
                    },
                )?
            }
            _ => {
                let message = format!(
                    "Ingestion failed for stream {stream_name}: Unsupported Content-Type: {content_type}. Expected application/json or application/x-protobuf"
                );
                error!("{message}");
                return Err(PostError::Invalid(anyhow::anyhow!(message)));
            }
        };
        span.record("record_count", records.len());
        records
    };

    if !records.is_empty() {
        push_logs(
            &stream_name,
            Value::Array(records),
            &log_source,
            &p_custom_fields,
            None,
            telemetry_type,
            &tenant_id,
        )?;
    }

    Ok(())
}

fn decode_and_flatten_otel_json(
    json: Value,
    log_source: &LogSource,
    tenant_id: &str,
    normalise: bool,
) -> Result<Vec<Value>, PostError> {
    match log_source {
        LogSource::OtelLogs => {
            let message: LogsData = deserialize_otel_json(json, normalise)?;
            Ok(flatten_otel_logs(&message, tenant_id))
        }
        LogSource::OtelMetrics => {
            let message: MetricsData = deserialize_otel_json(json, normalise)?;
            Ok(flatten_otel_metrics(message, tenant_id))
        }
        LogSource::OtelTraces => {
            let message: TracesData = deserialize_otel_json(json, normalise)?;
            Ok(flatten_otel_traces(&message, tenant_id))
        }
        _ => Err(PostError::Invalid(anyhow::anyhow!(
            "JSON OTLP ingestion requires an OTLP log source"
        ))),
    }
}

fn decode_and_flatten_otel_protobuf(
    body: &[u8],
    log_source: &LogSource,
    tenant_id: &str,
) -> anyhow::Result<Vec<Value>> {
    match log_source {
        LogSource::OtelLogs => {
            let message = ExportLogsServiceRequest::decode(body)?;
            Ok(flatten_otel_logs_protobuf(&message, tenant_id))
        }
        LogSource::OtelMetrics => {
            let message = ExportMetricsServiceRequest::decode(body)?;
            Ok(flatten_otel_metrics_protobuf(&message, tenant_id))
        }
        LogSource::OtelTraces => {
            let message = ExportTraceServiceRequest::decode(body)?;
            Ok(flatten_otel_traces_protobuf(&message, tenant_id))
        }
        _ => anyhow::bail!("protobuf OTLP ingestion requires an OTLP log source"),
    }
}

#[instrument(
    name = "push_logs",
    level = "info",
    skip(json, log_source, p_custom_fields, time_partition, telemetry_type, tenant_id),
    fields(stream_name, record_count = tracing::field::Empty)
)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn push_logs(
    stream_name: &str,
    json: Value,
    log_source: &LogSource,
    p_custom_fields: &HashMap<String, String>,
    time_partition: Option<String>,
    telemetry_type: TelemetryType,
    tenant_id: &Option<String>,
) -> Result<(), PostError> {
    let stream = PARSEABLE.get_stream(stream_name, tenant_id)?;
    // Hold one shared metadata snapshot while converting the request. This
    // avoids six lock acquisitions and an O(fields) HashMap/Arc clone on every
    // batch. The guard is dropped before Event::process can update the schema.
    let metadata = stream.metadata.read().expect(LOCK_EXPECT);
    let time_partition_limit = metadata.time_partition_limit;
    let static_schema_flag = metadata.static_schema_flag;
    let custom_partition = metadata.custom_partition.as_ref();
    let schema_version = metadata.schema_version;
    let schema = &metadata.schema;
    let infer_timestamp = metadata.infer_timestamp;
    let p_timestamp = Utc::now();

    // Keep this in the common write path so pre-flattened inputs cannot bypass the limit.
    verify_dataset_fields_count(
        stream_name,
        metadata.schema.len(),
        PARSEABLE.options.dataset_fields_allowed_limit,
    )?;

    let data = convert_array_to_object(
        json,
        time_partition.as_ref(),
        time_partition_limit,
        custom_partition,
        schema_version,
        log_source,
    )?;

    if data.is_empty() {
        return Err(PostError::Invalid(anyhow::Error::msg(
            "Empty data object received",
        )));
    }

    // Batch path: one schema inference + one decoder for the entire batch.
    // When custom partitions are set, different records may need different
    // partition keys, so fall back to per-record processing.
    if custom_partition.is_none() && time_partition.is_none() {
        // process_non_partitioned returns vec![Value::Array([...])], a single
        // element wrapping the whole batch. Unwrap it to avoid double-nesting.
        let json_batch = data.into_iter().next().unwrap();
        let origin_size = json_byte_size(&json_batch);

        let (rb, is_first_event) = (json::Event {
            json: json_batch,
            p_timestamp,
        })
        .into_recordbatch(
            schema,
            static_schema_flag,
            time_partition.as_ref(),
            schema_version,
            p_custom_fields,
            infer_timestamp,
        )?;

        let event = crate::event::Event {
            rb,
            stream_name: stream_name.to_owned(),
            origin_format: "json",
            origin_size,
            is_first_event,
            parsed_timestamp: p_timestamp.naive_utc(),
            time_partition: None,
            custom_partition_values: HashMap::new(),
            stream_type: StreamType::UserDefined,
            telemetry_type,
            tenant_id: tenant_id.to_owned(),
        };
        drop(metadata);
        event.process()?;
    } else {
        // Per-record path for custom-partitioned streams
        // This path enters Rayon's parallel iterator. Release the metadata
        // guard first so a worker that processes another request can acquire
        // the write lock in commit_schema instead of deadlocking the pool.
        let schema = schema.clone();
        let custom_partition = custom_partition.cloned();
        drop(metadata);
        let events: Result<Vec<_>, _> = data
            .into_par_iter()
            .map(|json| {
                let origin_size = json_byte_size(&json);
                (json::Event { json, p_timestamp }).into_event(
                    stream_name.to_owned(),
                    origin_size,
                    &schema,
                    static_schema_flag,
                    custom_partition.as_ref(),
                    time_partition.as_ref(),
                    schema_version,
                    StreamType::UserDefined,
                    p_custom_fields,
                    telemetry_type,
                    tenant_id,
                    infer_timestamp,
                )
            })
            .collect();
        for event in events.map_err(PostError::Invalid)? {
            event.process()?;
        }
    }
    Ok(())
}

/// Zero-allocation JSON byte size counter.
/// Uses serde_json::to_writer with a writer that only counts bytes.
pub fn json_byte_size(value: &Value) -> u64 {
    struct CountingWriter(u64);
    impl std::io::Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len() as u64;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = CountingWriter(0);
    // serde_json::to_writer won't fail on Value types
    let _ = serde_json::to_writer(&mut counter, value);
    counter.0
}

pub fn get_custom_fields_from_header(req: &HttpRequest) -> HashMap<String, String> {
    let user_agent = req
        .headers()
        .get(USER_AGENT)
        .and_then(|a| a.to_str().ok())
        .unwrap_or_default();

    let source_ip = req
        .peer_addr()
        .map(|address| address.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let mut p_custom_fields = HashMap::new();
    p_custom_fields.insert(USER_AGENT_KEY.to_string(), user_agent.to_string());
    p_custom_fields.insert(SOURCE_IP_KEY.to_string(), source_ip);

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
        if header_name.starts_with("x-p-")
            && !IGNORE_HEADERS.contains(&header_name)
            && let Ok(value) = header_value.to_str()
        {
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

        if header_name == LOG_SOURCE_KEY
            && let Ok(value) = header_value.to_str()
        {
            p_custom_fields.insert(FORMAT_KEY.to_string(), value.to_string());
        }
    }

    p_custom_fields
}

pub fn get_otel_custom_fields_from_header(
    req: &HttpRequest,
    log_source: &LogSource,
) -> HashMap<String, String> {
    let mut custom_fields = get_custom_fields_from_header(req);
    custom_fields.insert(FORMAT_KEY.to_string(), log_source.to_string());
    custom_fields
}

fn verify_dataset_fields_count(
    stream_name: &str,
    fields_count: usize,
    allowed_limit: usize,
) -> Result<(), PostError> {
    let dataset_fields_warn_threshold = 0.8 * allowed_limit as f64;
    // Check if the fields count exceeds the warn threshold
    if fields_count > dataset_fields_warn_threshold as usize {
        tracing::trace!(
            "Dataset {0} has {1} fields, which exceeds the warning threshold of {2}. Ingestion will not be possible after reaching {3} fields. We recommend creating a new dataset.",
            stream_name,
            fields_count,
            dataset_fields_warn_threshold as usize,
            allowed_limit
        );
    }
    // Check if the fields count exceeds the limit
    // Return an error if the fields count exceeds the limit
    if fields_count > allowed_limit {
        let error = PostError::FieldsCountLimitExceeded(
            stream_name.to_string(),
            fields_count,
            allowed_limit,
        );
        tracing::error!("{}", error);
        // Return an error if the fields count exceeds the limit
        return Err(error);
    }
    Ok(())
}

pub fn validate_stream_for_ingestion(
    stream_name: &str,
    log_source: &LogSource,
    tenant_id: &Option<String>,
) -> Result<(), PostError> {
    let stream = PARSEABLE.get_stream(stream_name, tenant_id)?;

    if stream.is_deleting() {
        return Err(PostError::StreamBeingDeleted(stream_name.to_string()));
    }

    // Reject early while the stream's schema cannot be reconciled with the cluster.
    stream.check_schema_resolution().map_err(|reason| {
        PostError::Event(EventError::SchemaResolutionBlocked(
            stream_name.to_string(),
            reason,
        ))
    })?;

    // Validate that the stream's log source is compatible
    stream
        .get_log_source()
        .iter()
        .find(|&stream_log_source_entry| {
            stream_log_source_entry.log_source_format != LogSource::OtelTraces
                && stream_log_source_entry.log_source_format != LogSource::OtelMetrics
        })
        .ok_or(PostError::IncorrectLogFormat(stream_name.to_string()))?;

    // If stream has OtelLogs as log source, only allow OtelLogs ingestion
    let has_otel_logs = stream
        .get_log_source()
        .iter()
        .any(|entry| entry.log_source_format == LogSource::OtelLogs);

    if has_otel_logs && *log_source != LogSource::OtelLogs {
        return Err(PostError::IncorrectLogFormat(stream_name.to_string()));
    }

    // Check for time partition
    if stream.get_time_partition().is_some() {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "Ingestion with time partition is not supported in Parseable OSS"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;

    #[test]
    fn dataset_field_limit_accepts_configured_limit() {
        assert!(verify_dataset_fields_count("test-stream", 100, 100).is_ok());
    }

    #[test]
    fn dataset_field_limit_rejects_count_above_configured_limit() {
        let error = verify_dataset_fields_count("test-stream", 101, 100).unwrap_err();

        assert!(matches!(
            error,
            PostError::FieldsCountLimitExceeded(stream, fields, allowed)
                if stream == "test-stream" && fields == 101 && allowed == 100
        ));
    }

    #[test]
    fn json_logs_decode_to_flattened_records() {
        let payload = serde_json::json!({
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{}]
                }]
            }]
        });

        let records =
            decode_and_flatten_otel_json(payload, &LogSource::OtelLogs, "test-tenant", false)
                .unwrap();

        assert_eq!(records.len(), 1);
    }

    #[test]
    fn protobuf_logs_decode_to_flattened_records() {
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let records = decode_and_flatten_otel_protobuf(
            &request.encode_to_vec(),
            &LogSource::OtelLogs,
            "test-tenant",
        )
        .unwrap();

        assert_eq!(records.len(), 1);
    }

    #[test]
    fn protobuf_decode_rejects_non_otel_log_source() {
        let error =
            decode_and_flatten_otel_protobuf(&[], &LogSource::Json, "test-tenant").unwrap_err();
        assert!(error.to_string().contains("requires an OTLP log source"));
    }

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
    fn test_otel_custom_fields_use_endpoint_log_source() {
        let req = TestRequest::default().to_http_request();
        let custom_fields = get_otel_custom_fields_from_header(&req, &LogSource::OtelMetrics);
        assert_eq!(custom_fields.get(FORMAT_KEY).unwrap(), "otel-metrics");

        let req = TestRequest::default()
            .insert_header((LOG_SOURCE_KEY, "otel-traces"))
            .to_http_request();
        let custom_fields = get_otel_custom_fields_from_header(&req, &LogSource::OtelMetrics);
        assert_eq!(custom_fields.get(FORMAT_KEY).unwrap(), "otel-metrics");
    }

    #[test]
    fn test_get_custom_fields_empty_header_after_prefix() {
        let req = TestRequest::default()
            .insert_header(("x-p-", "empty"))
            .to_http_request();

        let custom_fields = get_custom_fields_from_header(&req);

        assert_eq!(custom_fields.len(), 2);
        assert_eq!(custom_fields.get(USER_AGENT_KEY).unwrap(), "");
        assert_eq!(custom_fields.get(SOURCE_IP_KEY).unwrap(), "unknown");
    }

    #[test]
    fn test_get_custom_fields_from_header_with_peer_address() {
        let req = TestRequest::default()
            .peer_addr("192.0.2.1:8080".parse().unwrap())
            .to_http_request();

        let custom_fields = get_custom_fields_from_header(&req);

        assert_eq!(custom_fields.get(SOURCE_IP_KEY).unwrap(), "192.0.2.1");
    }
}
