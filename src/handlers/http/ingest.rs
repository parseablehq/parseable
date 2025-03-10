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

use actix_web::web::Path;
use actix_web::{http::header::ContentType, HttpRequest, HttpResponse};
use arrow_array::RecordBatch;
use chrono::Utc;
use http::StatusCode;
use serde_json::Value;

use crate::event::error::EventError;
use crate::event::format::{json, EventFormat, LogSource};
use crate::event::{self, get_schema_key, PartitionEvent};
use crate::handlers::{LOG_SOURCE_KEY, STREAM_NAME_HEADER_KEY};
use crate::option::Mode;
use crate::parseable::{Stream, StreamNotFound, PARSEABLE};
use crate::storage::{ObjectStorageError, StreamType};
use crate::utils::header_parsing::ParseHeaderError;
use crate::utils::json::flatten::JsonFlattenError;

use super::cluster::utils::JsonWithSize;
use super::logstream::error::{CreateStreamError, StreamError};
use super::users::dashboards::DashboardError;
use super::users::filters::FiltersError;

// Handler for POST /api/v1/ingest
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn ingest(
    req: HttpRequest,
    JsonWithSize { json, byte_size }: JsonWithSize<Value>,
) -> Result<HttpResponse, PostError> {
    let Some(stream_name) = req.headers().get(STREAM_NAME_HEADER_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingStreamName));
    };

    let stream_name = stream_name.to_str().unwrap().to_owned();
    let internal_stream_names = PARSEABLE.streams.list_internal_streams();
    if internal_stream_names.contains(&stream_name) {
        return Err(PostError::InternalStream(stream_name));
    }
    PARSEABLE
        .create_stream_if_not_exists(&stream_name, StreamType::UserDefined, LogSource::default())
        .await?;

    let log_source = req
        .headers()
        .get(LOG_SOURCE_KEY)
        .and_then(|h| h.to_str().ok())
        .map_or(LogSource::default(), LogSource::from);

    if matches!(
        log_source,
        LogSource::OtelLogs | LogSource::OtelMetrics | LogSource::OtelTraces
    ) {
        return Err(PostError::OtelNotSupported);
    }

    let stream = PARSEABLE.get_or_create_stream(&stream_name);

    json::Event::new(json, byte_size, log_source)
        .into_event(&stream)?
        .process(&stream)?;

    Ok(HttpResponse::Ok().finish())
}

// Handler for POST /v1/logs to ingest OTEL logs
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn handle_otel_logs_ingestion(
    req: HttpRequest,
    JsonWithSize { json, byte_size }: JsonWithSize<Value>,
) -> Result<HttpResponse, PostError> {
    let Some(stream_name) = req.headers().get(STREAM_NAME_HEADER_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingStreamName));
    };

    let Some(log_source) = req.headers().get(LOG_SOURCE_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingLogSource));
    };
    let log_source = LogSource::from(log_source.to_str().unwrap());
    if log_source != LogSource::OtelLogs {
        return Err(PostError::IncorrectLogSource(LogSource::OtelLogs));
    }

    let stream_name = stream_name.to_str().unwrap().to_owned();
    PARSEABLE
        .create_stream_if_not_exists(&stream_name, StreamType::UserDefined, LogSource::OtelLogs)
        .await?;

    let stream = PARSEABLE.get_or_create_stream(&stream_name);

    json::Event::new(json, byte_size, log_source)
        .into_event(&stream)?
        .process(&stream)?;

    Ok(HttpResponse::Ok().finish())
}

// Handler for POST /v1/metrics to ingest OTEL metrics
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn handle_otel_metrics_ingestion(
    req: HttpRequest,
    JsonWithSize { json, byte_size }: JsonWithSize<Value>,
) -> Result<HttpResponse, PostError> {
    let Some(stream_name) = req.headers().get(STREAM_NAME_HEADER_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingStreamName));
    };
    let Some(log_source) = req.headers().get(LOG_SOURCE_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingLogSource));
    };
    let log_source = LogSource::from(log_source.to_str().unwrap());
    if log_source != LogSource::OtelMetrics {
        return Err(PostError::IncorrectLogSource(LogSource::OtelMetrics));
    }
    let stream_name = stream_name.to_str().unwrap().to_owned();
    PARSEABLE
        .create_stream_if_not_exists(
            &stream_name,
            StreamType::UserDefined,
            LogSource::OtelMetrics,
        )
        .await?;

    let stream = PARSEABLE.get_or_create_stream(&stream_name);

    json::Event::new(json, byte_size, log_source)
        .into_event(&stream)?
        .process(&stream)?;

    Ok(HttpResponse::Ok().finish())
}

// Handler for POST /v1/traces to ingest OTEL traces
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn handle_otel_traces_ingestion(
    req: HttpRequest,
    JsonWithSize { json, byte_size }: JsonWithSize<Value>,
) -> Result<HttpResponse, PostError> {
    let Some(stream_name) = req.headers().get(STREAM_NAME_HEADER_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingStreamName));
    };

    let Some(log_source) = req.headers().get(LOG_SOURCE_KEY) else {
        return Err(PostError::Header(ParseHeaderError::MissingLogSource));
    };
    let log_source = LogSource::from(log_source.to_str().unwrap());
    if log_source != LogSource::OtelTraces {
        return Err(PostError::IncorrectLogSource(LogSource::OtelTraces));
    }
    let stream_name = stream_name.to_str().unwrap().to_owned();
    PARSEABLE
        .create_stream_if_not_exists(&stream_name, StreamType::UserDefined, LogSource::OtelTraces)
        .await?;

    let stream = PARSEABLE.get_or_create_stream(&stream_name);

    json::Event::new(json, byte_size, log_source)
        .into_event(&stream)?
        .process(&stream)?;

    Ok(HttpResponse::Ok().finish())
}

// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
pub async fn post_event(
    req: HttpRequest,
    stream_name: Path<String>,
    JsonWithSize { json, byte_size }: JsonWithSize<Value>,
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

    if matches!(
        log_source,
        LogSource::OtelLogs | LogSource::OtelMetrics | LogSource::OtelTraces
    ) {
        return Err(PostError::OtelNotSupported);
    }

    let stream = PARSEABLE.get_or_create_stream(&stream_name);

    json::Event::new(json, byte_size, log_source)
        .into_event(&stream)?
        .process(&stream)?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn push_logs_unchecked(
    rb: RecordBatch,
    stream: &Stream,
) -> Result<event::Event, PostError> {
    let unchecked_event = event::Event {
        origin_format: "json",
        origin_size: 0,
        time_partition: None,
        is_first_event: true, // NOTE: Maybe should be false
        partitions: [(
            get_schema_key(&rb.schema().fields),
            PartitionEvent {
                rb,
                date: Utc::now().date_naive(),
            },
        )]
        .into_iter()
        .collect(),
        stream_type: StreamType::UserDefined,
    };

    unchecked_event.process_unchecked(stream)?;

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
    #[error(r#"Please use "x-p-log-source: {0}" for ingesting otel logs"#)]
    IncorrectLogSource(LogSource),
    #[error("Ingestion is not allowed in Query mode")]
    IngestionNotAllowed,
    #[error("Missing field for time partition in json: {0}")]
    MissingTimePartition(String),
}

impl actix_web::ResponseError for PostError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PostError::SerdeError(_) => StatusCode::BAD_REQUEST,
            PostError::Header(_) => StatusCode::BAD_REQUEST,
            PostError::Event(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::Invalid(_) => StatusCode::BAD_REQUEST,
            PostError::CreateStream(CreateStreamError::StreamNameValidation(_)) => {
                StatusCode::BAD_REQUEST
            }
            PostError::CreateStream(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::StreamNotFound(_) => StatusCode::NOT_FOUND,
            PostError::CustomError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::NetworkError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::DashboardError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::FiltersError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::StreamError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::JsonFlattenError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::OtelNotSupported => StatusCode::BAD_REQUEST,
            PostError::InternalStream(_) => StatusCode::BAD_REQUEST,
            PostError::IncorrectLogSource(_) => StatusCode::BAD_REQUEST,
            PostError::IngestionNotAllowed => StatusCode::BAD_REQUEST,
            PostError::MissingTimePartition(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
