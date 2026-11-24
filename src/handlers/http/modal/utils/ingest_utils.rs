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

use actix_web::http::header::{CONTENT_TYPE, USER_AGENT};
use actix_web::mime::Mime;
use actix_web::{HttpRequest, HttpResponse, web};
use chrono::Utc;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};
use opentelemetry_proto::tonic::{
    logs::v1::LogsData, metrics::v1::MetricsData, trace::v1::TracesData,
};
use prost::Message as _;
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::oneshot;
use tracing::{error, instrument, warn};

use crate::{
    event::{
        FORMAT_KEY, SOURCE_IP_KEY, USER_AGENT_KEY, commit_ingested_schemas,
        error::EventError,
        format::{EventFormat, LogSource, json},
    },
    handlers::{
        CONTENT_TYPE_JSON, CONTENT_TYPE_PROTOBUF, EXTRACT_LOG_KEY, LOG_SOURCE_KEY,
        STREAM_NAME_HEADER_KEY, TelemetryType,
        http::{
            ingest::PostError,
            kinesis::{Message, flatten_kinesis_logs},
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
        json::{
            convert_array_to_object,
            flatten::{FlattenBudget, FlattenLimits, JsonFlattenError, convert_to_array},
        },
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
    if let Err(e) = flatten_and_push_logs(
        json,
        &stream_name,
        &log_source,
        &p_custom_fields,
        time_partition,
        telemetry_type,
        &tenant_id,
    ) {
        error!("Ingestion failed for stream {stream_name}: {e}");
        return Err(e);
    }

    Ok(())
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
    let tenant_id = get_tenant_id_from_request(req)?;
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

/// Response for an OTEL export request that was ingested in full. OTLP/HTTP
/// answers in the encoding of the request: for protobuf that is an
/// `Export*ServiceResponse`, whose full-success form encodes to zero bytes.
/// JSON requests keep the empty response they always had.
pub fn otel_success_response(req: &HttpRequest) -> HttpResponse {
    let protobuf = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .and_then(classify_otel_content_type)
        == Some(OtelContentType::Protobuf);
    let mut response = HttpResponse::Ok();
    if protobuf {
        response.content_type(CONTENT_TYPE_PROTOBUF);
    }
    response.finish()
}

/// Payload encodings accepted by the OTEL HTTP endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OtelContentType {
    Json,
    Protobuf,
}

/// Classifies a raw `Content-Type` header value by its media-type essence
/// (`type/subtype`) only. Everything from the first `;` onwards is ignored,
/// including malformed parameter text, so `application/json; charset=utf-8`
/// is treated as JSON. Only HTTP optional whitespace (SP/HTAB) around the
/// essence is trimmed; type and subtype are compared case-insensitively.
/// Structured-syntax suffixes such as `application/otlp+json` are not aliases.
fn classify_otel_content_type(content_type: &str) -> Option<OtelContentType> {
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim_matches([' ', '\t']);
    let mime: Mime = essence.parse().ok()?;
    match mime.essence_str() {
        CONTENT_TYPE_JSON => Some(OtelContentType::Json),
        CONTENT_TYPE_PROTOBUF => Some(OtelContentType::Protobuf),
        _ => None,
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
    match content_type {
        Some(content_type) => match classify_otel_content_type(&content_type) {
            Some(OtelContentType::Json) => {
                let json: serde_json::Value = match serde_json::from_slice(&body) {
                    Ok(v) => v,
                    Err(e) => {
                        error!(
                            "Ingestion failed for stream {stream_name}: malformed JSON in request body"
                        );
                        return Err(PostError::SerdeError(e));
                    }
                };
                if let Err(e) = flatten_and_push_logs(
                    json,
                    &stream_name,
                    &log_source,
                    &p_custom_fields,
                    None,
                    telemetry_type,
                    &tenant_id,
                ) {
                    error!("Ingestion failed for stream {stream_name}: {e}");
                    return Err(e);
                }
            }
            Some(OtelContentType::Protobuf) => {
                if let Err(e) = flatten_and_push_otel_protobuf(
                    &body,
                    &stream_name,
                    &log_source,
                    &p_custom_fields,
                    telemetry_type,
                    &tenant_id,
                ) {
                    error!("Ingestion failed for stream {stream_name}: {e}");
                    return Err(e);
                }
            }
            None => {
                error!(
                    "Ingestion failed for stream {stream_name}: Unsupported Content-Type: {content_type}. Expected application/json or application/x-protobuf"
                );
                return Err(PostError::Invalid(anyhow::anyhow!(
                    "Ingestion failed for stream {stream_name}: Unsupported Content-Type: {content_type}. Expected application/json or application/x-protobuf"
                )));
            }
        },
        None => {
            error!(
                "Ingestion failed for stream {stream_name}: Missing Content-Type header. Expected application/json or application/x-protobuf"
            );
            return Err(PostError::Invalid(anyhow::anyhow!(
                "Ingestion failed for stream {stream_name}: Missing Content-Type header. Expected application/json or application/x-protobuf"
            )));
        }
    }
    Ok(())
}

/// An OTLP/HTTP export request decoded from its protobuf encoding.
#[derive(Debug)]
enum OtelProtobufRequest {
    Logs(ExportLogsServiceRequest),
    Metrics(ExportMetricsServiceRequest),
    Traces(ExportTraceServiceRequest),
}

/// Why an OTLP protobuf body could not be decoded.
#[derive(Debug, thiserror::Error)]
enum OtelProtobufError {
    // prost's message already starts with "failed to decode Protobuf message"
    #[error(transparent)]
    Decode(#[from] prost::DecodeError),
    #[error("Protobuf OTLP ingestion requires an OTLP log source, got {0}")]
    NotOtel(LogSource),
}

impl OtelProtobufRequest {
    /// Decodes the export request that `log_source` selects. The whole body is
    /// decoded before any row is built, so a body that fails to decode yields
    /// no rows. A body cut between two top-level entries is still a valid, shorter
    /// request: the HTTP layer, not this decoder, detects a truncated transfer.
    fn decode(body: &[u8], log_source: &LogSource) -> Result<Self, OtelProtobufError> {
        match log_source {
            LogSource::OtelLogs => Ok(Self::Logs(ExportLogsServiceRequest::decode(body)?)),
            LogSource::OtelMetrics => Ok(Self::Metrics(ExportMetricsServiceRequest::decode(body)?)),
            LogSource::OtelTraces => Ok(Self::Traces(ExportTraceServiceRequest::decode(body)?)),
            other => Err(OtelProtobufError::NotOtel(other.clone())),
        }
    }

    /// Flattens into the same rows the JSON encoding of this request produces.
    fn flatten(
        &self,
        tenant_id: &str,
        budget: &mut FlattenBudget,
    ) -> Result<Vec<Value>, JsonFlattenError> {
        match self {
            Self::Logs(message) => flatten_otel_logs_protobuf(message, tenant_id, budget),
            Self::Metrics(message) => Ok(flatten_otel_metrics_protobuf(message, tenant_id)),
            Self::Traces(message) => Ok(flatten_otel_traces_protobuf(message, tenant_id)),
        }
    }
}

/// Protobuf counterpart of the OTEL arms of `flatten_and_push_logs`: the typed
/// request is decoded straight from the body instead of going through a JSON
/// value tree, then shares flattening limits and the staging path with JSON.
/// The body size is capped by the `PayloadConfig` of the OTEL routes.
fn flatten_and_push_otel_protobuf(
    body: &[u8],
    stream_name: &str,
    log_source: &LogSource,
    p_custom_fields: &HashMap<String, String>,
    telemetry_type: TelemetryType,
    tenant_id: &Option<String>,
) -> Result<(), PostError> {
    let request = OtelProtobufRequest::decode(body, log_source).map_err(|e| {
        PostError::Invalid(anyhow::anyhow!(
            "Ingestion failed for stream {stream_name}: {e}"
        ))
    })?;
    let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    // Verify the dataset fields count
    verify_dataset_fields_count(stream_name, tenant_id)?;
    // Shared by every record of this request
    let mut budget = FlattenBudget::new(FlattenLimits::from_config());
    let records = request.flatten(tenant_str, &mut budget)?;
    if !records.is_empty() {
        push_logs(
            stream_name,
            Value::Array(records),
            log_source,
            p_custom_fields,
            None,
            telemetry_type,
            tenant_id,
            &mut budget,
        )?;
    }
    Ok(())
}

#[instrument(
    name = "flatten_and_push_logs",
    level = "info",
    skip(
        json,
        log_source,
        p_custom_fields,
        time_partition,
        telemetry_type,
        tenant_id
    ),
    fields(stream_name)
)]
pub fn flatten_and_push_logs(
    json: Value,
    stream_name: &str,
    log_source: &LogSource,
    p_custom_fields: &HashMap<String, String>,
    time_partition: Option<String>,
    telemetry_type: TelemetryType,
    tenant_id: &Option<String>,
) -> Result<(), PostError> {
    let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    // Verify the dataset fields count
    verify_dataset_fields_count(stream_name, tenant_id)?;
    // Shared by every event of this request
    let mut budget = FlattenBudget::new(FlattenLimits::from_config());

    match log_source {
        LogSource::Kinesis => {
            //custom flattening required for Amazon Kinesis
            let message: Message = serde_json::from_value(json)?;
            let flattened_kinesis_data = flatten_kinesis_logs(message, &mut budget)?;
            let record = convert_to_array(flattened_kinesis_data)?;
            push_logs(
                stream_name,
                record,
                log_source,
                p_custom_fields,
                time_partition,
                telemetry_type,
                tenant_id,
                &mut budget,
            )?;
        }
        LogSource::OtelLogs => {
            let logs: LogsData =
                deserialize_otel_json(json, PARSEABLE.options.normalise_otel_payload)?;
            let records = flatten_otel_logs(&logs, tenant_str, &mut budget)?;
            if !records.is_empty() {
                push_logs(
                    stream_name,
                    Value::Array(records),
                    log_source,
                    p_custom_fields,
                    None,
                    telemetry_type,
                    tenant_id,
                    &mut budget,
                )?;
            }
        }
        LogSource::OtelTraces => {
            let traces: TracesData =
                deserialize_otel_json(json, PARSEABLE.options.normalise_otel_payload)?;
            let records = flatten_otel_traces(&traces, tenant_str);
            if !records.is_empty() {
                push_logs(
                    stream_name,
                    Value::Array(records),
                    log_source,
                    p_custom_fields,
                    None,
                    telemetry_type,
                    tenant_id,
                    &mut budget,
                )?;
            }
        }
        LogSource::OtelMetrics => {
            let metrics: MetricsData =
                deserialize_otel_json(json, PARSEABLE.options.normalise_otel_payload)?;
            let records = flatten_otel_metrics(metrics, tenant_str);
            if !records.is_empty() {
                push_logs(
                    stream_name,
                    Value::Array(records),
                    log_source,
                    p_custom_fields,
                    None,
                    telemetry_type,
                    tenant_id,
                    &mut budget,
                )?;
            }
        }
        _ => push_logs(
            stream_name,
            json,
            log_source,
            p_custom_fields,
            time_partition,
            telemetry_type,
            tenant_id,
            &mut budget,
        )?,
    }

    Ok(())
}

#[instrument(
    name = "push_logs",
    level = "info",
    skip(
        json,
        log_source,
        p_custom_fields,
        time_partition,
        telemetry_type,
        tenant_id,
        budget
    ),
    fields(stream_name, record_count = tracing::field::Empty)
)]
// `budget` is shared with the other parts of the request, so it can not be folded into them
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn push_logs(
    stream_name: &str,
    json: Value,
    log_source: &LogSource,
    p_custom_fields: &HashMap<String, String>,
    time_partition: Option<String>,
    telemetry_type: TelemetryType,
    tenant_id: &Option<String>,
    budget: &mut FlattenBudget,
) -> Result<(), PostError> {
    // Snapshot at request entry. A subscriber attaching later receives ordered
    // single-row slices from the grouped path.
    let grouped = use_partitioned_ingest_batching(
        PARSEABLE.options.partitioned_ingest_batching,
        PARSEABLE.options.mode,
        &crate::livetail::LIVETAIL,
        stream_name,
    );
    let stream = PARSEABLE.get_stream(stream_name, tenant_id)?;
    let time_partition_limit = stream.get_time_partition_limit();
    let static_schema_flag = stream.get_static_schema_flag();
    let custom_partition = stream.get_custom_partition();
    let schema_version = stream.get_schema_version();
    let schema = stream.get_schema_raw();
    let infer_timestamp = stream.get_infer_timestamp();
    let p_timestamp = Utc::now();

    let data = convert_array_to_object(
        json,
        time_partition.as_ref(),
        time_partition_limit,
        custom_partition.as_ref(),
        schema_version,
        log_source,
        budget,
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
            &schema,
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
        event.process()?;
    } else if !grouped {
        // Per-record path for custom-partitioned streams
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
        let events = events.map_err(PostError::Invalid)?;
        // Check the union of all events' fields against the limit and commit
        // it under one write lock before processing any event. The fields are
        // then reserved, so processing cannot fail on the limit partway through
        // (leaving earlier events staged) even if concurrent requests add
        // fields meanwhile.
        let schemas: Vec<_> = events
            .iter()
            .map(|event| (event.rb.schema(), event.is_first_event))
            .collect();
        let committed = commit_ingested_schemas(
            stream_name,
            &stream.metadata,
            &schemas,
            PARSEABLE.options.dataset_fields_allowed_limit,
        )?;
        if committed && !stream.get_static_schema_flag() {
            stream
                .stage_schema_file(stream.get_schema().as_ref().clone())
                .map_err(EventError::from)?;
        }
        for event in events {
            event.process_without_field_limit()?;
        }
    } else {
        let records = data
            .into_iter()
            .map(|json| {
                let size = json_byte_size(&json);
                (json, size)
            })
            .collect();
        let events = json::Event::partitioned_events(
            records,
            p_timestamp,
            stream_name,
            &schema,
            static_schema_flag,
            custom_partition.as_ref(),
            time_partition.as_ref(),
            schema_version,
            p_custom_fields,
            telemetry_type,
            tenant_id,
            infer_timestamp,
            // Query-mode user ingestion has a single, unsegmented memory
            // buffer, whose global input order must also remain unchanged.
            PARSEABLE.options.mode != crate::option::Mode::Query,
        )
        .map_err(PostError::Invalid)?;
        // Check the union of all events' fields against the limit and commit
        // it under one write lock before processing any event. The fields are
        // then reserved, so processing cannot fail on the limit partway through
        // (leaving earlier events staged) even if concurrent requests add
        // fields meanwhile.
        let schemas: Vec<_> = events
            .iter()
            .map(|event| (event.rb.schema(), event.is_first_event))
            .collect();
        let committed = commit_ingested_schemas(
            stream_name,
            &stream.metadata,
            &schemas,
            PARSEABLE.options.dataset_fields_allowed_limit,
        )?;
        if committed && !stream.get_static_schema_flag() {
            stream
                .stage_schema_file(stream.get_schema().as_ref().clone())
                .map_err(EventError::from)?;
        }
        events.process()?;
    }
    Ok(())
}

fn use_partitioned_ingest_batching(
    enabled: bool,
    mode: crate::option::Mode,
    tail: &crate::livetail::LiveTail,
    stream_name: &str,
) -> bool {
    enabled && mode != crate::option::Mode::Query && !tail.has_subscribers(stream_name)
}

/// Zero-allocation JSON byte size counter.
/// Uses serde_json::to_writer with a writer that only counts bytes.
/// Exact for values that always serialize, such as a `Value` or a struct of strings and
/// numbers, which is what it is used for.
pub fn json_byte_size(value: &(impl serde::Serialize + ?Sized)) -> u64 {
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
    // serde_json::to_writer won't fail on those
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
    tenant_id: &Option<String>,
) -> Result<(), PostError> {
    let fields_count = PARSEABLE
        .get_stream(stream_name, tenant_id)?
        .get_schema()
        .fields()
        .len();
    let dataset_fields_warn_threshold = 0.8 * PARSEABLE.options.dataset_fields_allowed_limit as f64;
    // Check if the fields count exceeds the warn threshold
    if fields_count > dataset_fields_warn_threshold as usize {
        tracing::trace!(
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

pub fn validate_stream_for_ingestion(
    stream_name: &str,
    log_source: &LogSource,
    tenant_id: &Option<String>,
) -> Result<(), PostError> {
    let stream = PARSEABLE.get_stream(stream_name, tenant_id)?;

    if stream.is_deleting() {
        return Err(PostError::StreamBeingDeleted(stream_name.to_string()));
    }

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
    fn partitioned_batching_defaults_to_per_record_and_falls_back_for_subscribers() {
        use crate::{
            cli::{Cli, StorageOptions},
            option::Mode,
        };
        use clap::Parser;
        let StorageOptions::Local(args) = Cli::try_parse_from(["parseable", "local-store", "/tmp"])
            .unwrap()
            .storage
        else {
            panic!("expected local store")
        };
        let tail = crate::livetail::LiveTail::default();
        assert!(!args.options.partitioned_ingest_batching);
        assert!(!use_partitioned_ingest_batching(
            args.options.partitioned_ingest_batching,
            Mode::All,
            &tail,
            "test"
        ));
        assert!(use_partitioned_ingest_batching(
            true,
            Mode::All,
            &tail,
            "test"
        ));
        assert!(!use_partitioned_ingest_batching(
            true,
            Mode::Query,
            &tail,
            "test"
        ));
        let receiver = tail.new_pipe("subscriber".into(), "test".into());
        assert!(!use_partitioned_ingest_batching(
            true,
            Mode::All,
            &tail,
            "test"
        ));
        // Registry keys are stream names, not tenant-prefixed names.
        assert!(use_partitioned_ingest_batching(
            true,
            Mode::All,
            &tail,
            "other"
        ));
        drop(receiver);
        assert!(use_partitioned_ingest_batching(
            true,
            Mode::All,
            &tail,
            "test"
        ));
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
    fn test_classify_otel_content_type_by_essence_only() {
        use OtelContentType::{Json, Protobuf};

        let cases: &[(&str, Option<OtelContentType>)] = &[
            // bare media types
            ("application/json", Some(Json)),
            ("application/x-protobuf", Some(Protobuf)),
            // charset parameter, plain and quoted
            ("application/json; charset=utf-8", Some(Json)),
            ("application/json;charset=utf-8", Some(Json)),
            ("application/json; charset=\"utf-8\"", Some(Json)),
            // unknown and multiple parameters
            ("application/json; foo=bar", Some(Json)),
            ("application/json; charset=utf-8; foo=bar", Some(Json)),
            // type/subtype are case-insensitive
            ("APPLICATION/JSON", Some(Json)),
            ("Application/Json; Charset=UTF-8", Some(Json)),
            // outer SP/HTAB around the whole value
            ("  application/json  ", Some(Json)),
            ("\tapplication/json\t", Some(Json)),
            // SP/HTAB around the parameter separator
            ("application/json ; charset=utf-8", Some(Json)),
            ("application/json\t;\tcharset=utf-8", Some(Json)),
            (" \tapplication/json \t; charset=utf-8", Some(Json)),
            // malformed parameter text is ignored (essence-only validation)
            ("application/json;", Some(Json)),
            ("application/json;;", Some(Json)),
            ("application/json; charset", Some(Json)),
            ("application/json; charset=", Some(Json)),
            ("application/json; charset=\"utf-8", Some(Json)),
            ("application/json; =utf-8", Some(Json)),
            ("application/json; a b c", Some(Json)),
            // parameterised and case-varied protobuf still classifies as protobuf
            ("application/x-protobuf; charset=utf-8", Some(Protobuf)),
            ("Application/X-Protobuf", Some(Protobuf)),
            ("APPLICATION/X-PROTOBUF ; proto=foo", Some(Protobuf)),
            // whitespace inside the essence is rejected
            ("application /json", None),
            ("application/ json", None),
            ("application\t/json", None),
            ("application/js on", None),
            // malformed essences
            ("", None),
            ("   ", None),
            (";charset=utf-8", None),
            ("application", None),
            ("application/", None),
            ("/json", None),
            ("application/json/extra", None),
            ("\"application/json\"", None),
            // unsupported media types and suffix aliases
            ("*/*", None),
            ("text/plain", None),
            ("application/xml", None),
            ("application/protobuf", None),
            ("application/jsonl", None),
            ("application/otlp+json", None),
            ("application/vnd.api+json", None),
        ];

        for (input, expected) in cases {
            assert_eq!(
                classify_otel_content_type(input),
                *expected,
                "classification of {input:?}"
            );
        }
    }

    const OTEL_TEST_STREAM: &str = "otel-content-type-test";
    const OTEL_SIGNALS: [(TelemetryType, LogSource); 3] = [
        (TelemetryType::Logs, LogSource::OtelLogs),
        (TelemetryType::Metrics, LogSource::OtelMetrics),
        (TelemetryType::Traces, LogSource::OtelTraces),
    ];

    // Malformed JSON body: an accepted JSON content type fails in serde
    // before any PARSEABLE/storage access, so no global state is needed.
    async fn run_otel_content(
        content_type: Option<actix_web::http::header::HeaderValue>,
        telemetry_type: TelemetryType,
        log_source: &LogSource,
    ) -> PostError {
        let mut req = TestRequest::default();
        if let Some(value) = content_type {
            req = req.insert_header((actix_web::http::header::CONTENT_TYPE, value));
        }
        let req = req.to_http_request();
        process_otel_content(
            &req,
            web::Bytes::from_static(b"{not valid json"),
            OTEL_TEST_STREAM,
            log_source,
            telemetry_type,
        )
        .await
        .expect_err("malformed body or rejected content type must fail")
    }

    async fn assert_bad_request_body(err: &PostError, expected_body: &str) {
        use actix_web::ResponseError;

        assert_eq!(err.status_code(), actix_web::http::StatusCode::BAD_REQUEST);
        let resp = err.error_response();
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.headers()
                .get(actix_web::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/plain; charset=utf-8"
        );
        let body = actix_web::body::to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), expected_body);
    }

    fn header(value: &'static str) -> Option<actix_web::http::header::HeaderValue> {
        Some(actix_web::http::header::HeaderValue::from_static(value))
    }

    #[tokio::test]
    async fn test_process_otel_content_accepts_parameterised_json() {
        let accepted = [
            "application/json",
            "application/json; charset=utf-8",
            "application/json; charset=\"utf-8\"",
            "Application/JSON ; foo=bar; charset=utf-8",
            "\tapplication/json\t;\tcharset=utf-8",
            "application/json; charset=\"unterminated",
        ];
        for (telemetry_type, log_source) in OTEL_SIGNALS {
            for value in accepted {
                let err = run_otel_content(header(value), telemetry_type, &log_source).await;
                assert!(
                    matches!(err, PostError::SerdeError(_)),
                    "{log_source} with {value:?} should reach JSON decoding, got {err:?}"
                );
                let expected = err.to_string();
                assert!(expected.starts_with("Could not deserialize into JSON object, "));
                assert_bad_request_body(&err, &expected).await;
            }
        }
    }

    #[tokio::test]
    async fn test_process_otel_content_missing_or_non_text_content_type() {
        let non_text =
            actix_web::http::header::HeaderValue::from_bytes(b"application/json\xff").unwrap();
        for (telemetry_type, log_source) in OTEL_SIGNALS {
            for content_type in [None, Some(non_text.clone())] {
                let err = run_otel_content(content_type, telemetry_type, &log_source).await;
                assert!(matches!(err, PostError::Invalid(_)), "{err:?}");
                assert_bad_request_body(
                    &err,
                    &format!(
                        "Invalid Request: Ingestion failed for stream {OTEL_TEST_STREAM}: Missing Content-Type header. Expected application/json or application/x-protobuf"
                    ),
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn test_process_otel_content_unsupported_reports_original_value() {
        let unsupported = [
            "text/plain; charset=utf-8",
            "application/otlp+json",
            "application /json; charset=utf-8",
            "  Application/XML ; q=1",
            "",
        ];
        for (telemetry_type, log_source) in OTEL_SIGNALS {
            for value in unsupported {
                let err = run_otel_content(header(value), telemetry_type, &log_source).await;
                assert!(matches!(err, PostError::Invalid(_)), "{err:?}");
                assert_bad_request_body(
                    &err,
                    &format!(
                        "Invalid Request: Ingestion failed for stream {OTEL_TEST_STREAM}: Unsupported Content-Type: {value}. Expected application/json or application/x-protobuf"
                    ),
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn test_process_otel_content_protobuf_variants_reach_protobuf_decoding() {
        let protobuf = [
            "application/x-protobuf",
            "application/x-protobuf; charset=utf-8",
            "Application/X-Protobuf",
            " APPLICATION/X-PROTOBUF\t;proto=foo",
        ];
        for (telemetry_type, log_source) in OTEL_SIGNALS {
            for value in protobuf {
                // The shared malformed body is not a protobuf message either, so this
                // fails in the decoder before any PARSEABLE/storage access
                let err = run_otel_content(header(value), telemetry_type, &log_source).await;
                assert!(matches!(err, PostError::Invalid(_)), "{err:?}");
                let expected = err.to_string();
                assert!(
                    expected.starts_with(&format!(
                        "Invalid Request: Ingestion failed for stream {OTEL_TEST_STREAM}: failed to decode Protobuf message: "
                    )),
                    "{log_source} with {value:?}: {expected}"
                );
                assert_bad_request_body(&err, &expected).await;
            }
        }
    }

    #[test]
    fn otel_success_response_answers_in_the_request_encoding() {
        use actix_web::http::header::CONTENT_TYPE;

        for (request_type, response_type) in [
            (
                Some("application/x-protobuf"),
                Some("application/x-protobuf"),
            ),
            (
                Some("Application/X-Protobuf; charset=utf-8"),
                Some("application/x-protobuf"),
            ),
            (Some("application/json"), None),
            (None, None),
        ] {
            let mut req = TestRequest::default();
            if let Some(value) = request_type {
                req = req.insert_header((CONTENT_TYPE, value));
            }
            let resp = otel_success_response(&req.to_http_request());
            assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
            assert_eq!(
                resp.headers()
                    .get(CONTENT_TYPE)
                    .map(|v| v.to_str().unwrap()),
                response_type,
                "request content type {request_type:?}"
            );
        }
    }

    #[test]
    fn otel_protobuf_decode_rejects_non_otel_log_source() {
        let err = OtelProtobufRequest::decode(&[], &LogSource::Json).unwrap_err();
        assert!(matches!(err, OtelProtobufError::NotOtel(LogSource::Json)));
    }

    #[test]
    fn otel_protobuf_body_cut_inside_a_record_fails_to_decode() {
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

        let encoded = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        severity_text: "INFO".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec();

        assert!(matches!(
            OtelProtobufRequest::decode(&encoded, &LogSource::OtelLogs),
            Ok(OtelProtobufRequest::Logs(_))
        ));
        // One top-level entry, so every proper prefix ends inside it
        for len in 1..encoded.len() {
            assert!(
                matches!(
                    OtelProtobufRequest::decode(&encoded[..len], &LogSource::OtelLogs),
                    Err(OtelProtobufError::Decode(_))
                ),
                "a body cut to {len} of {} bytes must not decode",
                encoded.len()
            );
        }
    }

    /// The protobuf and JSON encodings of one request must flatten to the same rows.
    #[test]
    fn otel_protobuf_and_json_encodings_flatten_to_the_same_rows() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        fn string_attribute(key: &str, value: &str) -> KeyValue {
            KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(value.to_string())),
                }),
                ..Default::default()
            }
        }
        let limits = || FlattenBudget::new(FlattenLimits::DEFAULT);

        let resource = Some(Resource {
            attributes: vec![string_attribute("service.name", "checkout")],
            ..Default::default()
        });
        let logs = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: resource.clone(),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        LogRecord {
                            time_unix_nano: 1_700_000_000_000_000_000,
                            observed_time_unix_nano: 1_700_000_000_000_000_001,
                            severity_number: 9,
                            severity_text: "INFO".to_string(),
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("paid".to_string())),
                            }),
                            attributes: vec![string_attribute("order", "42")],
                            trace_id: vec![0xab; 16],
                            span_id: vec![0xcd; 8],
                            ..Default::default()
                        },
                        // Unset timestamps are filled from the clock, so every one is set
                        LogRecord {
                            time_unix_nano: 1_700_000_001_000_000_000,
                            observed_time_unix_nano: 1_700_000_001_000_000_001,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let json: LogsData = serde_json::from_value(serde_json::to_value(&logs).unwrap()).unwrap();
        let protobuf = OtelProtobufRequest::decode(
            &ExportLogsServiceRequest {
                resource_logs: logs.resource_logs.clone(),
            }
            .encode_to_vec(),
            &LogSource::OtelLogs,
        )
        .unwrap();
        let from_json = flatten_otel_logs(&json, "tenant", &mut limits()).unwrap();
        assert_eq!(from_json.len(), 2);
        assert_eq!(
            protobuf.flatten("tenant", &mut limits()).unwrap(),
            from_json
        );

        let traces = TracesData {
            resource_spans: vec![ResourceSpans {
                resource,
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![0x11; 16],
                        span_id: vec![0x22; 8],
                        name: "charge".to_string(),
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_500_000_000,
                        attributes: vec![string_attribute("order", "42")],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let json: TracesData =
            serde_json::from_value(serde_json::to_value(&traces).unwrap()).unwrap();
        let protobuf = OtelProtobufRequest::decode(
            &ExportTraceServiceRequest {
                resource_spans: traces.resource_spans.clone(),
            }
            .encode_to_vec(),
            &LogSource::OtelTraces,
        )
        .unwrap();
        let from_json = flatten_otel_traces(&json, "tenant");
        assert_eq!(from_json.len(), 1);
        assert_eq!(
            protobuf.flatten("tenant", &mut limits()).unwrap(),
            from_json
        );
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

#[cfg(test)]
mod otel_protobuf_props;
