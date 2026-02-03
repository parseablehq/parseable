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

use std::collections::{HashMap, HashSet};

use actix_web::http::StatusCode;
use actix_web::web::{self, Json, Path};
use actix_web::{HttpRequest, HttpResponse, http::header::ContentType};
use arrow_array::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use tracing::error;

use crate::event::error::EventError;
use crate::event::format::known_schema::{self, KNOWN_SCHEMA_LIST};
use crate::event::format::{self, EventFormat, LogSource, LogSourceEntry};
use crate::event::{self, FORMAT_KEY, USER_AGENT_KEY};
use crate::handlers::http::modal::utils::ingest_utils::validate_stream_for_ingestion;
use crate::handlers::{
    CONTENT_TYPE_JSON, CONTENT_TYPE_PROTOBUF, EXTRACT_LOG_KEY, LOG_SOURCE_KEY,
    STREAM_NAME_HEADER_KEY, TELEMETRY_TYPE_KEY, TelemetryType,
};
use crate::metadata::SchemaVersion;
use crate::metastore::MetastoreError;
use crate::option::Mode;
use crate::otel::logs::OTEL_LOG_KNOWN_FIELD_LIST;
use crate::otel::metrics::OTEL_METRICS_KNOWN_FIELD_LIST;
use crate::otel::traces::OTEL_TRACES_KNOWN_FIELD_LIST;
use crate::parseable::{PARSEABLE, StreamNotFound};
use crate::storage::{ObjectStorageError, StreamType};
use crate::utils::header_parsing::ParseHeaderError;
use crate::utils::json::{flatten::JsonFlattenError, strict::StrictValue};

use super::logstream::error::{CreateStreamError, StreamError};
use super::modal::utils::ingest_utils::{flatten_and_push_logs, get_custom_fields_from_header};
use super::users::dashboards::DashboardError;
use super::users::filters::FiltersError;

// Handler for POST /api/v1/ingest
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn ingest(
    req: HttpRequest,
    Json(json): Json<StrictValue>,
) -> Result<HttpResponse, PostError> {
    let Some(stream_name) = req.headers().get(STREAM_NAME_HEADER_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingStreamName));
    };

    let stream_name = stream_name.to_str().unwrap().to_owned();
    let internal_stream_names = PARSEABLE.streams.list_internal_streams();
    if internal_stream_names.contains(&stream_name) {
        return Err(PostError::InternalStream(stream_name));
    }

    let log_source = req
        .headers()
        .get(LOG_SOURCE_KEY)
        .and_then(|h| h.to_str().ok())
        .map_or(LogSource::default(), LogSource::from);

    let telemetry_type = req
        .headers()
        .get(TELEMETRY_TYPE_KEY)
        .and_then(|h| h.to_str().ok())
        .map_or(TelemetryType::default(), TelemetryType::from);

    let extract_log = req
        .headers()
        .get(EXTRACT_LOG_KEY)
        .and_then(|h| h.to_str().ok());

    if matches!(
        log_source,
        LogSource::OtelLogs | LogSource::OtelMetrics | LogSource::OtelTraces
    ) {
        return Err(PostError::OtelNotSupported);
    }

    let mut p_custom_fields = get_custom_fields_from_header(&req);

    let mut json = json.into_inner();

    let fields = match &log_source {
        LogSource::Custom(src) => KNOWN_SCHEMA_LIST.extract_from_inline_log(
            &mut json,
            &mut p_custom_fields,
            src,
            extract_log,
        )?,
        _ => HashSet::new(),
    };

    let log_source_entry = LogSourceEntry::new(log_source.clone(), fields);

    PARSEABLE
        .create_stream_if_not_exists(
            &stream_name,
            StreamType::UserDefined,
            None,
            vec![log_source_entry.clone()],
            telemetry_type,
        )
        .await?;

    //if stream exists, fetch the stream log source
    //return error if the stream log source is otel traces or otel metrics or otel logs
    validate_stream_for_ingestion(&stream_name, &log_source)?;

    PARSEABLE
        .add_update_log_source(&stream_name, log_source_entry)
        .await?;

    flatten_and_push_logs(
        json,
        &stream_name,
        &log_source,
        &p_custom_fields,
        None,
        telemetry_type,
    )
    .await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn ingest_internal_stream(stream_name: String, body: Bytes) -> Result<(), PostError> {
    let size: usize = body.len();
    let json: StrictValue = serde_json::from_slice(&body)?;
    let schema = PARSEABLE.get_stream(&stream_name)?.get_schema_raw();
    let mut p_custom_fields = HashMap::new();
    p_custom_fields.insert(USER_AGENT_KEY.to_string(), "parseable".to_string());
    p_custom_fields.insert(FORMAT_KEY.to_string(), LogSource::Json.to_string());
    // For internal streams, use old schema
    format::json::Event::new(json.into_inner(), Utc::now())
        .into_event(
            stream_name,
            size as u64,
            &schema,
            false,
            None,
            None,
            SchemaVersion::V0,
            StreamType::Internal,
            &p_custom_fields,
            TelemetryType::Logs,
        )?
        .process()?;

    Ok(())
}

// Common validation and setup for OTEL ingestion
pub async fn setup_otel_stream(
    req: &HttpRequest,
    expected_log_source: LogSource,
    known_fields: &[&str],
    telemetry_type: TelemetryType,
) -> Result<(String, LogSource, LogSourceEntry, Option<String>), PostError> {
    let Some(stream_name) = req.headers().get(STREAM_NAME_HEADER_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingStreamName));
    };

    let Some(log_source) = req.headers().get(LOG_SOURCE_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingLogSource));
    };

    let log_source = LogSource::from(log_source.to_str().unwrap());
    if log_source != expected_log_source {
        return Err(PostError::IncorrectLogSource(
            expected_log_source,
            telemetry_type.to_string(),
        ));
    }

    let stream_name = stream_name.to_str().unwrap().to_owned();

    let log_source_entry = LogSourceEntry::new(
        log_source.clone(),
        known_fields.iter().map(|&s| s.to_string()).collect(),
    );

    PARSEABLE
        .create_stream_if_not_exists(
            &stream_name,
            StreamType::UserDefined,
            None,
            vec![log_source_entry.clone()],
            telemetry_type,
        )
        .await?;
    let mut time_partition = None;
    // Validate stream compatibility
    if let Ok(stream) = PARSEABLE.get_stream(&stream_name) {
        match log_source {
            LogSource::OtelLogs => {
                // For logs, reject if stream is metrics or traces
                stream
                    .get_log_source()
                    .iter()
                    .find(|&stream_log_source_entry| {
                        stream_log_source_entry.log_source_format != LogSource::OtelTraces
                            && stream_log_source_entry.log_source_format != LogSource::OtelMetrics
                    })
                    .ok_or(PostError::IncorrectLogFormat(stream_name.clone()))?;
            }
            LogSource::OtelMetrics | LogSource::OtelTraces => {
                // For metrics/traces, only allow same type
                stream
                    .get_log_source()
                    .iter()
                    .find(|&stream_log_source_entry| {
                        stream_log_source_entry.log_source_format == log_source
                    })
                    .ok_or(PostError::IncorrectLogFormat(stream_name.clone()))?;
            }
            _ => {}
        }

        time_partition = stream.get_time_partition();
    }

    PARSEABLE
        .add_update_log_source(&stream_name, log_source_entry.clone())
        .await?;

    Ok((stream_name, log_source, log_source_entry, time_partition))
}

// Common content processing for OTEL ingestion
async fn process_otel_content(
    req: &HttpRequest,
    body: web::Bytes,
    stream_name: &str,
    log_source: &LogSource,
    telemetry_type: TelemetryType,
) -> Result<(), PostError> {
    let p_custom_fields = get_custom_fields_from_header(req);

    match req
        .headers()
        .get("Content-Type")
        .and_then(|h| h.to_str().ok())
    {
        Some(content_type) => {
            if content_type == CONTENT_TYPE_JSON {
                flatten_and_push_logs(
                    serde_json::from_slice(&body)?,
                    stream_name,
                    log_source,
                    &p_custom_fields,
                    None,
                    telemetry_type,
                )
                .await?;
            } else if content_type == CONTENT_TYPE_PROTOBUF {
                return Err(PostError::Invalid(anyhow::anyhow!(
                    "Protobuf ingestion is not supported in Parseable OSS"
                )));
            } else {
                return Err(PostError::Invalid(anyhow::anyhow!(
                    "Unsupported Content-Type: {}. Expected application/json or application/x-protobuf",
                    content_type
                )));
            }
        }
        None => {
            return Err(PostError::Invalid(anyhow::anyhow!(
                "Missing Content-Type header. Expected application/json or application/x-protobuf"
            )));
        }
    }

    Ok(())
}

// Handler for POST /v1/logs to ingest OTEL logs
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn handle_otel_logs_ingestion(
    req: HttpRequest,
    body: web::Bytes,
) -> Result<HttpResponse, PostError> {
    let (stream_name, log_source, ..) = setup_otel_stream(
        &req,
        LogSource::OtelLogs,
        &OTEL_LOG_KNOWN_FIELD_LIST,
        TelemetryType::Logs,
    )
    .await?;

    process_otel_content(&req, body, &stream_name, &log_source, TelemetryType::Logs).await?;

    Ok(HttpResponse::Ok().finish())
}

// Handler for POST /v1/metrics to ingest OTEL metrics
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn handle_otel_metrics_ingestion(
    req: HttpRequest,
    body: web::Bytes,
) -> Result<HttpResponse, PostError> {
    let (stream_name, log_source, ..) = setup_otel_stream(
        &req,
        LogSource::OtelMetrics,
        &OTEL_METRICS_KNOWN_FIELD_LIST,
        TelemetryType::Metrics,
    )
    .await?;

    process_otel_content(
        &req,
        body,
        &stream_name,
        &log_source,
        TelemetryType::Metrics,
    )
    .await?;

    Ok(HttpResponse::Ok().finish())
}

// Handler for POST /v1/traces to ingest OTEL traces
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn handle_otel_traces_ingestion(
    req: HttpRequest,
    body: web::Bytes,
) -> Result<HttpResponse, PostError> {
    let (stream_name, log_source, ..) = setup_otel_stream(
        &req,
        LogSource::OtelTraces,
        &OTEL_TRACES_KNOWN_FIELD_LIST,
        TelemetryType::Traces,
    )
    .await?;

    process_otel_content(&req, body, &stream_name, &log_source, TelemetryType::Traces).await?;

    Ok(HttpResponse::Ok().finish())
}

// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
pub async fn post_event(
    req: HttpRequest,
    stream_name: Path<String>,
    Json(json): Json<StrictValue>,
) -> Result<HttpResponse, PostError> {
    let stream_name = stream_name.into_inner();

    let internal_stream_names = PARSEABLE.streams.list_internal_streams();
    if internal_stream_names.contains(&stream_name) {
        return Err(PostError::InternalStream(stream_name));
    }
    if !PARSEABLE.streams.contains(&stream_name) {
        // For distributed deployments, if the stream not found in memory map,
        //check if it exists in the storage
        //create stream and schema from storage
        if PARSEABLE.options.mode != Mode::All {
            match PARSEABLE
                .create_stream_and_schema_from_storage(&stream_name)
                .await
            {
                Ok(true) => {}
                Ok(false) | Err(_) => return Err(StreamNotFound(stream_name.clone()).into()),
            }
        } else {
            return Err(StreamNotFound(stream_name.clone()).into());
        }
    }

    let log_source = req
        .headers()
        .get(LOG_SOURCE_KEY)
        .and_then(|h| h.to_str().ok())
        .map_or(LogSource::default(), LogSource::from);

    let extract_log = req
        .headers()
        .get(EXTRACT_LOG_KEY)
        .and_then(|h| h.to_str().ok());
    let mut p_custom_fields = get_custom_fields_from_header(&req);
    let mut json = json.into_inner();
    match &log_source {
        LogSource::OtelLogs | LogSource::OtelMetrics | LogSource::OtelTraces => {
            return Err(PostError::OtelNotSupported);
        }
        LogSource::Custom(src) => {
            KNOWN_SCHEMA_LIST.extract_from_inline_log(
                &mut json,
                &mut p_custom_fields,
                src,
                extract_log,
            )?;
        }
        _ => {}
    }

    //if stream exists, fetch the stream log source
    //return error if the stream log source is otel traces or otel metrics or otel logs
    validate_stream_for_ingestion(&stream_name, &log_source)?;

    flatten_and_push_logs(
        json,
        &stream_name,
        &log_source,
        &p_custom_fields,
        None,
        TelemetryType::Logs,
    )
    .await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn push_logs_unchecked(
    batches: RecordBatch,
    stream_name: &str,
) -> Result<event::Event, PostError> {
    let unchecked_event = event::Event {
        rb: batches,
        stream_name: stream_name.to_string(),
        origin_format: "json",
        origin_size: 0,
        parsed_timestamp: Utc::now().naive_utc(),
        time_partition: None,
        is_first_event: true,                    // NOTE: Maybe should be false
        custom_partition_values: HashMap::new(), // should be an empty map for unchecked push
        stream_type: StreamType::UserDefined,
        telemetry_type: TelemetryType::Logs,
    };
    unchecked_event.process_unchecked()?;

    Ok(unchecked_event)
}

#[derive(Debug, thiserror::Error)]
pub enum PostError {
    #[error("{0}")]
    StreamNotFound(#[from] StreamNotFound),
    #[error("Could not deserialize into JSON object, {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Header Error: {0}")]
    Header(#[from] ParseHeaderError),
    #[error("Event Error: {0}")]
    Event(#[from] EventError),
    #[error("Invalid Request: {0}")]
    Invalid(#[from] anyhow::Error),
    #[error("{0}")]
    CreateStream(#[from] CreateStreamError),
    #[allow(unused)]
    #[error("Error: {0}")]
    CustomError(String),
    #[error("Error: {0}")]
    NetworkError(#[from] reqwest::Error),
    #[error("ObjectStorageError: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("Error: {0}")]
    FiltersError(#[from] FiltersError),
    #[error("Error: {0}")]
    DashboardError(#[from] DashboardError),
    #[error("Error: {0}")]
    StreamError(#[from] StreamError),
    #[error("Error: {0}")]
    JsonFlattenError(#[from] JsonFlattenError),
    #[error(
        "Use the endpoints `/v1/logs` for otel logs, `/v1/metrics` for otel metrics and `/v1/traces` for otel traces"
    )]
    OtelNotSupported,
    #[error("The stream {0} is reserved for internal use and cannot be ingested into")]
    InternalStream(String),
    #[error(r#"Please use "x-p-log-source: {0}" for ingesting otel {1} data"#)]
    IncorrectLogSource(LogSource, String),
    #[error("Ingestion is not allowed in Query mode")]
    IngestionNotAllowed,
    #[error("Missing field for time partition in json: {0}")]
    MissingTimePartition(String),
    #[error("{0}")]
    KnownFormat(#[from] known_schema::Error),
    #[error(
        "Ingestion is not allowed to stream {0} as it is already associated with a different OTEL format"
    )]
    IncorrectLogFormat(String),
    #[error(
        "Failed to ingest events in dataset {0}. Total number of fields {1} exceeds the permissible limit of {2}. We recommend creating a new dataset beyond {2} for better query performance."
    )]
    FieldsCountLimitExceeded(String, usize, usize),
    #[error("Invalid query parameter")]
    InvalidQueryParameter,
    #[error("Missing query parameter")]
    MissingQueryParameter,
    #[error(transparent)]
    MetastoreError(#[from] MetastoreError),
}

impl actix_web::ResponseError for PostError {
    fn status_code(&self) -> StatusCode {
        use PostError::*;
        match self {
            SerdeError(_)
            | Header(_)
            | Invalid(_)
            | InternalStream(_)
            | IncorrectLogSource(_, _)
            | IngestionNotAllowed
            | MissingTimePartition(_)
            | KnownFormat(_)
            | IncorrectLogFormat(_)
            | FieldsCountLimitExceeded(_, _, _)
            | InvalidQueryParameter
            | MissingQueryParameter
            | CreateStream(CreateStreamError::StreamNameValidation(_))
            | OtelNotSupported => StatusCode::BAD_REQUEST,

            Event(_)
            | CreateStream(_)
            | CustomError(_)
            | NetworkError(_)
            | ObjectStorageError(_)
            | DashboardError(_)
            | FiltersError(_)
            | StreamError(_)
            | JsonFlattenError(_) => StatusCode::INTERNAL_SERVER_ERROR,

            StreamNotFound(_) => StatusCode::NOT_FOUND,

            MetastoreError(e) => e.status_code(),
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        error!("{self}");
        match self {
            PostError::MetastoreError(metastore_error) => {
                actix_web::HttpResponse::build(metastore_error.status_code())
                    .insert_header(ContentType::json())
                    .json(metastore_error.to_detail())
            }
            _ => actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::Int64Type;
    use arrow_array::{ArrayRef, Float64Array, Int64Array, ListArray, StringArray};
    use arrow_schema::{DataType, Field};
    use chrono::Utc;
    use serde_json::json;
    use std::{collections::HashMap, sync::Arc};

    use crate::{
        event::format::{EventFormat, json},
        metadata::SchemaVersion,
    };

    trait TestExt {
        fn as_int64_arr(&self) -> Option<&Int64Array>;
        fn as_float64_arr(&self) -> Option<&Float64Array>;
        fn as_utf8_arr(&self) -> Option<&StringArray>;
    }

    impl TestExt for ArrayRef {
        fn as_int64_arr(&self) -> Option<&Int64Array> {
            self.as_any().downcast_ref()
        }

        fn as_float64_arr(&self) -> Option<&Float64Array> {
            self.as_any().downcast_ref()
        }

        fn as_utf8_arr(&self) -> Option<&StringArray> {
            self.as_any().downcast_ref()
        }
    }

    fn fields_to_map(iter: impl Iterator<Item = Field>) -> HashMap<String, Arc<Field>> {
        iter.map(|x| (x.name().clone(), Arc::new(x))).collect()
    }

    #[test]
    fn basic_object_into_rb() {
        let json = json!({
            "c": 4.23,
            "a": 1,
            "b": "hello",
        });

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(
                &HashMap::default(),
                false,
                None,
                SchemaVersion::V0,
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 4);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from_iter_values(["hello"])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from_iter([4.23])
        );
    }

    #[test]
    fn basic_object_with_null_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
            "c": null
        });

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(
                &HashMap::default(),
                false,
                None,
                SchemaVersion::V0,
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 3);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from_iter_values(["hello"])
        );
    }

    #[test]
    fn basic_object_derive_schema_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
        });

        let schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(&schema, false, None, SchemaVersion::V0, &HashMap::new())
            .unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 3);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from_iter_values(["hello"])
        );
    }

    #[test]
    fn basic_object_schema_mismatch() {
        let json = json!({
            "a": 1,
            "b": 1, // type mismatch
        });

        let schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        assert!(
            json::Event::new(json, Utc::now())
                .into_recordbatch(&schema, false, None, SchemaVersion::V0, &HashMap::new())
                .is_err()
        );
    }

    #[test]
    fn empty_object() {
        let json = json!({});

        let schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(&schema, false, None, SchemaVersion::V0, &HashMap::new())
            .unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 1);
    }

    #[test]
    fn array_into_recordbatch_inffered_schema() {
        let json = json!([
            {
                "b": "hello",
            },
            {
                "b": "hello",
                "a": 1,
                "c": 1
            },
            {
                "a": 1,
                "b": "hello",
                "c": null
            },
        ]);

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(
                &HashMap::default(),
                false,
                None,
                SchemaVersion::V0,
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 4);

        let schema = rb.schema();
        let fields = &schema.fields;

        assert_eq!(&*fields[1], &Field::new("a", DataType::Int64, true));
        assert_eq!(&*fields[2], &Field::new("b", DataType::Utf8, true));
        assert_eq!(&*fields[3], &Field::new("c", DataType::Int64, true));

        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![None, Some(1), None])
        );
    }

    #[test]
    fn arr_with_null_into_rb() {
        let json = json!([
            {
                "c": null,
                "b": "hello",
                "a": null
            },
            {
                "a": 1,
                "c": 1.22,
                "b": "hello"
            },
            {
                "b": "hello",
                "a": 1,
                "c": null
            },
        ]);

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(
                &HashMap::default(),
                false,
                None,
                SchemaVersion::V0,
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 4);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![None, Some(1.22), None,])
        );
    }

    #[test]
    fn arr_with_null_derive_schema_into_rb() {
        let json = json!([
            {
                "c": null,
                "b": "hello",
                "a": null
            },
            {
                "a": 1,
                "c": 1.22,
                "b": "hello"
            },
            {
                "b": "hello",
                "a": 1,
                "c": null
            },
        ]);

        let schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(&schema, false, None, SchemaVersion::V0, &HashMap::new())
            .unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 4);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![None, Some(1.22), None,])
        );
    }

    #[test]
    fn arr_schema_mismatch() {
        let json = json!([
            {
                "a": null,
                "b": "hello",
                "c": 1.24
            },
            {
                "a": 1,
                "b": "hello",
                "c": 1
            },
            {
                "a": 1,
                "b": "hello",
                "c": null
            },
        ]);

        let schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        assert!(
            json::Event::new(json, Utc::now())
                .into_recordbatch(&schema, false, None, SchemaVersion::V0, &HashMap::new())
                .is_err()
        );
    }

    #[test]
    fn arr_obj_with_nested_type() {
        let json = json!([
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
                "c_a": [1],
            },
            {
                "a": 1,
                "b": "hello",
                "c_a": [1],
                "c_b": [2],
            },
        ]);

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(
                &HashMap::default(),
                false,
                None,
                SchemaVersion::V0,
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(rb.num_rows(), 4);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr().unwrap(),
            &Int64Array::from(vec![Some(1), Some(1), Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello")
            ])
        );

        assert_eq!(
            rb.column_by_name("c_a")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap(),
            &ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                None,
                None,
                Some(vec![Some(1i64)]),
                Some(vec![Some(1)])
            ])
        );

        assert_eq!(
            rb.column_by_name("c_b")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap(),
            &ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                None,
                None,
                None,
                Some(vec![Some(2i64)])
            ])
        );
    }

    #[test]
    fn arr_obj_with_nested_type_v1() {
        let json = json!([
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
                "c_a": 1,
            },
            {
                "a": 1,
                "b": "hello",
                "c_a": 1,
                "c_b": 2,
            },
        ]);

        let (rb, _) = json::Event::new(json, Utc::now())
            .into_recordbatch(
                &HashMap::default(),
                false,
                None,
                SchemaVersion::V1,
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(rb.num_rows(), 4);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![Some(1.0), Some(1.0), Some(1.0), Some(1.0)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr().unwrap(),
            &StringArray::from(vec![
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello")
            ])
        );

        assert_eq!(
            rb.column_by_name("c_a").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![None, None, Some(1.0), Some(1.0)])
        );

        assert_eq!(
            rb.column_by_name("c_b").unwrap().as_float64_arr().unwrap(),
            &Float64Array::from(vec![None, None, None, Some(2.0)])
        );
    }
}
