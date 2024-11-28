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

use super::logstream::error::{CreateStreamError, StreamError};
use super::modal::utils::ingest_utils::{flatten_and_push_logs, push_logs};
use super::otel;
use super::users::dashboards::DashboardError;
use super::users::filters::FiltersError;
use crate::event::{
    self,
    error::EventError,
    format::{self, EventFormat},
};
use crate::handlers::http::modal::utils::logstream_utils::create_stream_and_schema_from_storage;
use crate::handlers::{LOG_SOURCE_KEY, LOG_SOURCE_OTEL, STREAM_NAME_HEADER_KEY};
use crate::localcache::CacheError;
use crate::metadata::error::stream_info::MetadataError;
use crate::metadata::STREAM_INFO;
use crate::option::{Mode, CONFIG};
use crate::storage::{ObjectStorageError, StreamType};
use crate::utils::header_parsing::ParseHeaderError;
use actix_web::{http::header::ContentType, HttpRequest, HttpResponse};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use bytes::Bytes;
use chrono::Utc;
use http::StatusCode;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// Handler for POST /api/v1/ingest
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn ingest(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    if let Some((_, stream_name)) = req
        .headers()
        .iter()
        .find(|&(key, _)| key == STREAM_NAME_HEADER_KEY)
    {
        let stream_name = stream_name.to_str().unwrap().to_owned();
        let internal_stream_names = STREAM_INFO.list_internal_streams();
        if internal_stream_names.contains(&stream_name) {
            return Err(PostError::Invalid(anyhow::anyhow!(
                "The stream {} is reserved for internal use and cannot be ingested into",
                stream_name
            )));
        }
        create_stream_if_not_exists(&stream_name, &StreamType::UserDefined.to_string()).await?;

        flatten_and_push_logs(req, body, stream_name).await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        Err(PostError::Header(ParseHeaderError::MissingStreamName))
    }
}

pub async fn ingest_internal_stream(stream_name: String, body: Bytes) -> Result<(), PostError> {
    let size: usize = body.len();
    let parsed_timestamp = Utc::now().naive_utc();
    let (rb, is_first) = {
        let body_val: Value = serde_json::from_slice(&body)?;
        let hash_map = STREAM_INFO.read().unwrap();
        let schema = hash_map
            .get(&stream_name)
            .ok_or(PostError::StreamNotFound(stream_name.clone()))?
            .schema
            .clone();
        let event = format::json::Event {
            data: body_val,
            tags: String::default(),
            metadata: String::default(),
        };
        event.into_recordbatch(schema, None, None)?
    };
    event::Event {
        rb,
        stream_name,
        origin_format: "json",
        origin_size: size as u64,
        is_first_event: is_first,
        parsed_timestamp,
        time_partition: None,
        custom_partition_values: HashMap::new(),
        stream_type: StreamType::Internal,
    }
    .process()
    .await?;
    Ok(())
}

// Handler for POST /v1/logs to ingest OTEL logs
// ingests events by extracting stream name from header
// creates if stream does not exist
pub async fn ingest_otel_logs(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    if let Some((_, stream_name)) = req
        .headers()
        .iter()
        .find(|&(key, _)| key == STREAM_NAME_HEADER_KEY)
    {
        let stream_name = stream_name.to_str().unwrap().to_owned();
        create_stream_if_not_exists(&stream_name, &StreamType::UserDefined.to_string()).await?;

        //flatten logs
        if let Some((_, log_source)) = req.headers().iter().find(|&(key, _)| key == LOG_SOURCE_KEY)
        {
            let log_source: String = log_source.to_str().unwrap().to_owned();
            if log_source == LOG_SOURCE_OTEL {
                let mut json = otel::flatten_otel_logs(&body);
                for record in json.iter_mut() {
                    let body: Bytes = serde_json::to_vec(record).unwrap().into();
                    push_logs(stream_name.to_string(), req.clone(), body).await?;
                }
            } else {
                return Err(PostError::CustomError("Unknown log source".to_string()));
            }
        } else {
            return Err(PostError::CustomError(
                "log source key header is missing".to_string(),
            ));
        }
    } else {
        return Err(PostError::Header(ParseHeaderError::MissingStreamName));
    }
    Ok(HttpResponse::Ok().finish())
}

// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
pub async fn post_event(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let internal_stream_names = STREAM_INFO.list_internal_streams();
    if internal_stream_names.contains(&stream_name) {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "Stream {} is an internal stream and cannot be ingested into",
            stream_name
        )));
    }
    if !STREAM_INFO.stream_exists(&stream_name) {
        // For distributed deployments, if the stream not found in memory map,
        //check if it exists in the storage
        //create stream and schema from storage
        if CONFIG.parseable.mode != Mode::All {
            match create_stream_and_schema_from_storage(&stream_name).await {
                Ok(true) => {}
                Ok(false) | Err(_) => return Err(PostError::StreamNotFound(stream_name.clone())),
            }
        } else {
            return Err(PostError::StreamNotFound(stream_name.clone()));
        }
    }

    flatten_and_push_logs(req, body, stream_name).await?;
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
    };
    unchecked_event.process_unchecked()?;

    Ok(unchecked_event)
}

// Check if the stream exists and create a new stream if doesn't exist
pub async fn create_stream_if_not_exists(
    stream_name: &str,
    stream_type: &str,
) -> Result<bool, PostError> {
    let mut stream_exists = false;
    if STREAM_INFO.stream_exists(stream_name) {
        stream_exists = true;
        return Ok(stream_exists);
    }

    // For distributed deployments, if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if CONFIG.parseable.mode != Mode::All {
        return Ok(create_stream_and_schema_from_storage(stream_name).await?);
    }

    super::logstream::create_stream(
        stream_name.to_string(),
        "",
        "",
        "",
        "",
        Arc::new(Schema::empty()),
        stream_type,
    )
    .await?;

    Ok(stream_exists)
}

#[derive(Debug, thiserror::Error)]
pub enum PostError {
    #[error("Stream {0} not found")]
    StreamNotFound(String),
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
    #[error("Error: {0}")]
    MetadataStreamError(#[from] MetadataError),
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
    CacheError(#[from] CacheError),
    #[error("Error: {0}")]
    StreamError(#[from] StreamError),
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
            PostError::MetadataStreamError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::StreamNotFound(_) => StatusCode::NOT_FOUND,
            PostError::CustomError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::NetworkError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::DashboardError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::FiltersError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::CacheError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::StreamError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use actix_web::test::TestRequest;
    use arrow_array::{
        types::Int64Type, ArrayRef, Float64Array, Int64Array, ListArray, StringArray,
    };
    use arrow_schema::{DataType, Field};
    use serde_json::json;

    use crate::{
        event,
        handlers::{http::modal::utils::ingest_utils::into_event_batch, PREFIX_META, PREFIX_TAGS},
    };

    trait TestExt {
        fn as_int64_arr(&self) -> &Int64Array;
        fn as_float64_arr(&self) -> &Float64Array;
        fn as_utf8_arr(&self) -> &StringArray;
    }

    impl TestExt for ArrayRef {
        fn as_int64_arr(&self) -> &Int64Array {
            self.as_any().downcast_ref().unwrap()
        }

        fn as_float64_arr(&self) -> &Float64Array {
            self.as_any().downcast_ref().unwrap()
        }

        fn as_utf8_arr(&self) -> &StringArray {
            self.as_any().downcast_ref().unwrap()
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

        let req = TestRequest::default()
            .append_header((PREFIX_TAGS.to_string() + "A", "tag1"))
            .append_header((PREFIX_META.to_string() + "C", "meta1"))
            .to_http_request();

        let (rb, _) = into_event_batch(req, json, HashMap::default(), None, None).unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 6);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr(),
            &StringArray::from_iter_values(["hello"])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr(),
            &Float64Array::from_iter([4.23])
        );
        assert_eq!(
            rb.column_by_name(event::DEFAULT_TAGS_KEY)
                .unwrap()
                .as_utf8_arr(),
            &StringArray::from_iter_values(["a=tag1"])
        );
        assert_eq!(
            rb.column_by_name(event::DEFAULT_METADATA_KEY)
                .unwrap()
                .as_utf8_arr(),
            &StringArray::from_iter_values(["c=meta1"])
        );
    }

    #[test]
    fn basic_object_with_null_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
            "c": null
        });

        let req = TestRequest::default().to_http_request();

        let (rb, _) = into_event_batch(req, json, HashMap::default(), None, None).unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 6);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr(),
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

        let req = TestRequest::default().to_http_request();

        let (rb, _) = into_event_batch(req, json, schema, None, None).unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr(),
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

        let req = TestRequest::default().to_http_request();

        assert!(into_event_batch(req, json, schema, None, None).is_err());
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

        let req = TestRequest::default().to_http_request();

        let (rb, _) = into_event_batch(req, json, schema, None, None).unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 3);
    }

    #[test]
    fn non_object_arr_is_err() {
        let json = json!([1]);

        let req = TestRequest::default().to_http_request();

        assert!(into_event_batch(req, json, HashMap::default(), None, None).is_err())
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

        let req = TestRequest::default().to_http_request();

        let (rb, _) = into_event_batch(req, json, HashMap::default(), None, None).unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 6);

        let schema = rb.schema();
        let fields = &schema.fields;

        assert_eq!(&*fields[1], &Field::new("a", DataType::Int64, true));
        assert_eq!(&*fields[2], &Field::new("b", DataType::Utf8, true));
        assert_eq!(&*fields[3], &Field::new("c", DataType::Int64, true));

        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_int64_arr(),
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

        let req = TestRequest::default().to_http_request();

        let (rb, _) = into_event_batch(req, json, HashMap::default(), None, None).unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 6);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr(),
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
        let req = TestRequest::default().to_http_request();

        let (rb, _) = into_event_batch(req, json, schema, None, None).unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 6);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr(),
            &Int64Array::from(vec![None, Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
        );
        assert_eq!(
            rb.column_by_name("c").unwrap().as_float64_arr(),
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
                "c": 1          // All numbers can be cast into float: #989
            },
            {
                "a": 1.24,      // But not the other way round
                "b": "hello",
                "c": null
            },
        ]);

        let req = TestRequest::default().to_http_request();

        let schema = fields_to_map(
            [
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]
            .into_iter(),
        );

        assert!(into_event_batch(req, json, schema, None, None).is_err());
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
                "c": [{"a": 1}]
            },
            {
                "a": 1,
                "b": "hello",
                "c": [{"a": 1, "b": 2}]
            },
        ]);

        let req = TestRequest::default().to_http_request();

        let (rb, _) = into_event_batch(req, json, HashMap::default(), None, None).unwrap();

        assert_eq!(rb.num_rows(), 4);
        assert_eq!(rb.num_columns(), 7);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr(),
            &Int64Array::from(vec![Some(1), Some(1), Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr(),
            &StringArray::from(vec![
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello")
            ])
        );

        let c_a = vec![None, None, Some(vec![Some(1i64)]), Some(vec![Some(1)])];
        let c_b = vec![None, None, None, Some(vec![Some(2i64)])];

        assert_eq!(
            rb.column_by_name("c_a")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap(),
            &ListArray::from_iter_primitive::<Int64Type, _, _>(c_a)
        );

        assert_eq!(
            rb.column_by_name("c_b")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap(),
            &ListArray::from_iter_primitive::<Int64Type, _, _>(c_b)
        );
    }
}
