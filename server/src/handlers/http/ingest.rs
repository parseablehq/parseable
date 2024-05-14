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

use super::cluster::INTERNAL_STREAM_NAME;
use super::logstream::error::CreateStreamError;
use super::users::dashboards::DashboardError;
use super::users::filters::FiltersError;
use super::{kinesis, otel};
use crate::event::{
    self,
    error::EventError,
    format::{self, EventFormat},
};
use crate::handlers::{
    LOG_SOURCE_KEY, LOG_SOURCE_KINESIS, LOG_SOURCE_OTEL, PREFIX_META, PREFIX_TAGS, SEPARATOR,
    STREAM_NAME_HEADER_KEY,
};
use crate::metadata::{self, STREAM_INFO};
use crate::option::{Mode, CONFIG};
use crate::storage::{LogStream, ObjectStorageError};
use crate::utils::header_parsing::{collect_labelled_headers, ParseHeaderError};
use crate::utils::json::convert_array_to_object;
use actix_web::{http::header::ContentType, HttpRequest, HttpResponse};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use http::StatusCode;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
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
        if stream_name.eq(INTERNAL_STREAM_NAME) {
            return Err(PostError::Invalid(anyhow::anyhow!(
                "Stream {} is an internal stream and cannot be ingested into",
                stream_name
            )));
        }
        create_stream_if_not_exists(&stream_name).await?;

        flatten_and_push_logs(req, body, stream_name).await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        Err(PostError::Header(ParseHeaderError::MissingStreamName))
    }
}

pub async fn ingest_internal_stream(stream_name: String, body: Bytes) -> Result<(), PostError> {
    create_stream_if_not_exists(&stream_name).await?;
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
    }
    .process()
    .await?;
    Ok(())
}

async fn flatten_and_push_logs(
    req: HttpRequest,
    body: Bytes,
    stream_name: String,
) -> Result<(), PostError> {
    //flatten logs
    if let Some((_, log_source)) = req.headers().iter().find(|&(key, _)| key == LOG_SOURCE_KEY) {
        let mut json: Vec<BTreeMap<String, Value>> = Vec::new();
        let log_source: String = log_source.to_str().unwrap().to_owned();
        match log_source.as_str() {
            LOG_SOURCE_KINESIS => json = kinesis::flatten_kinesis_logs(&body),
            LOG_SOURCE_OTEL => json = otel::flatten_otel_logs(&body),
            _ => {
                log::warn!("Unknown log source: {}", log_source);
                push_logs(stream_name.to_string(), req.clone(), body).await?;
            }
        }
        for record in json.iter_mut() {
            let body: Bytes = serde_json::to_vec(record).unwrap().into();
            push_logs(stream_name.to_string(), req.clone(), body).await?;
        }
    } else {
        push_logs(stream_name.to_string(), req, body).await?;
    }
    Ok(())
}

// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
pub async fn post_event(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    if stream_name.eq(INTERNAL_STREAM_NAME) {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "Stream {} is an internal stream and cannot be ingested into",
            stream_name
        )));
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
    };
    unchecked_event.process_unchecked()?;

    Ok(unchecked_event)
}

async fn push_logs(stream_name: String, req: HttpRequest, body: Bytes) -> Result<(), PostError> {
    let glob_storage = CONFIG.storage().get_object_store();
    let object_store_format = glob_storage
        .get_object_store_format(&stream_name)
        .await
        .map_err(|_| PostError::StreamNotFound(stream_name.clone()))?;

    let time_partition = object_store_format.time_partition;
    let time_partition_limit = object_store_format.time_partition_limit;
    let static_schema_flag = object_store_format.static_schema_flag;
    let custom_partition = object_store_format.custom_partition;
    let body_val: Value = serde_json::from_slice(&body)?;
    let size: usize = body.len();
    let mut parsed_timestamp = Utc::now().naive_utc();
    if time_partition.is_none() {
        if custom_partition.is_none() {
            let size = size as u64;
            create_process_record_batch(
                stream_name.clone(),
                req.clone(),
                body_val.clone(),
                static_schema_flag.clone(),
                None,
                parsed_timestamp,
                HashMap::new(),
                size,
            )
            .await?;
        } else {
            let data =
                convert_array_to_object(body_val.clone(), None, None, custom_partition.clone())?;
            let custom_partition = custom_partition.unwrap();
            let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();

            for value in data {
                let custom_partition_values =
                    get_custom_partition_values(&value, &custom_partition_list);

                let size = value.to_string().into_bytes().len() as u64;
                create_process_record_batch(
                    stream_name.clone(),
                    req.clone(),
                    value.clone(),
                    static_schema_flag.clone(),
                    None,
                    parsed_timestamp,
                    custom_partition_values.clone(),
                    size,
                )
                .await?;
            }
        }
    } else if custom_partition.is_none() {
        let data = convert_array_to_object(
            body_val.clone(),
            time_partition.clone(),
            time_partition_limit,
            None,
        )?;
        for value in data {
            parsed_timestamp = get_parsed_timestamp(&value, &time_partition);
            let size = value.to_string().into_bytes().len() as u64;
            create_process_record_batch(
                stream_name.clone(),
                req.clone(),
                value.clone(),
                static_schema_flag.clone(),
                time_partition.clone(),
                parsed_timestamp,
                HashMap::new(),
                size,
            )
            .await?;
        }
    } else {
        let data = convert_array_to_object(
            body_val.clone(),
            time_partition.clone(),
            time_partition_limit,
            custom_partition.clone(),
        )?;
        let custom_partition = custom_partition.unwrap();
        let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();

        for value in data {
            let custom_partition_values =
                get_custom_partition_values(&value, &custom_partition_list);

            parsed_timestamp = get_parsed_timestamp(&value, &time_partition);
            let size = value.to_string().into_bytes().len() as u64;
            create_process_record_batch(
                stream_name.clone(),
                req.clone(),
                value.clone(),
                static_schema_flag.clone(),
                time_partition.clone(),
                parsed_timestamp,
                custom_partition_values.clone(),
                size,
            )
            .await?;
        }
    }

    Ok(())
}

fn get_parsed_timestamp(body: &Value, time_partition: &Option<String>) -> NaiveDateTime {
    let body_timestamp = body.get(&time_partition.clone().unwrap().to_string());
    let parsed_timestamp = body_timestamp
        .unwrap()
        .to_owned()
        .as_str()
        .unwrap()
        .parse::<DateTime<Utc>>()
        .unwrap()
        .naive_utc();
    parsed_timestamp
}

fn get_custom_partition_values(
    body: &Value,
    custom_partition_list: &[&str],
) -> HashMap<String, String> {
    let mut custom_partition_values: HashMap<String, String> = HashMap::new();
    for custom_partition_field in custom_partition_list {
        let custom_partition_value = body.get(custom_partition_field.trim()).unwrap().to_owned();
        let custom_partition_value = match custom_partition_value.clone() {
            e @ Value::Number(_) | e @ Value::Bool(_) => e.to_string(),
            Value::String(s) => s,
            _ => "".to_string(),
        };
        custom_partition_values.insert(
            custom_partition_field.trim().to_string(),
            custom_partition_value,
        );
    }
    custom_partition_values
}

#[allow(clippy::too_many_arguments)]
async fn create_process_record_batch(
    stream_name: String,
    req: HttpRequest,
    value: Value,
    static_schema_flag: Option<String>,
    time_partition: Option<String>,
    parsed_timestamp: NaiveDateTime,
    custom_partition_values: HashMap<String, String>,
    origin_size: u64,
) -> Result<(), PostError> {
    let (rb, is_first_event) = get_stream_schema(
        stream_name.clone(),
        req.clone(),
        value.clone(),
        static_schema_flag.clone(),
        time_partition.clone(),
    )?;
    event::Event {
        rb,
        stream_name: stream_name.clone(),
        origin_format: "json",
        origin_size,
        is_first_event,
        parsed_timestamp,
        time_partition: time_partition.clone(),
        custom_partition_values: custom_partition_values.clone(),
    }
    .process()
    .await?;

    Ok(())
}

fn get_stream_schema(
    stream_name: String,
    req: HttpRequest,
    body: Value,
    static_schema_flag: Option<String>,
    time_partition: Option<String>,
) -> Result<(arrow_array::RecordBatch, bool), PostError> {
    let hash_map = STREAM_INFO.read().unwrap();
    let schema = hash_map
        .get(&stream_name)
        .ok_or(PostError::StreamNotFound(stream_name))?
        .schema
        .clone();
    into_event_batch(req, body, schema, static_schema_flag, time_partition)
}

fn into_event_batch(
    req: HttpRequest,
    body: Value,
    schema: HashMap<String, Arc<Field>>,
    static_schema_flag: Option<String>,
    time_partition: Option<String>,
) -> Result<(arrow_array::RecordBatch, bool), PostError> {
    let tags = collect_labelled_headers(&req, PREFIX_TAGS, SEPARATOR)?;
    let metadata = collect_labelled_headers(&req, PREFIX_META, SEPARATOR)?;
    let event = format::json::Event {
        data: body,
        tags,
        metadata,
    };
    let (rb, is_first) = event.into_recordbatch(schema, static_schema_flag, time_partition)?;
    Ok((rb, is_first))
}

// Check if the stream exists and create a new stream if doesn't exist
pub async fn create_stream_if_not_exists(stream_name: &str) -> Result<(), PostError> {
    if STREAM_INFO.stream_exists(stream_name) {
        return Ok(());
    }
    match &CONFIG.parseable.mode {
        Mode::All | Mode::Query => {
            super::logstream::create_stream(
                stream_name.to_string(),
                "",
                "",
                "",
                "",
                Arc::new(Schema::empty()),
            )
            .await?;
        }
        Mode::Ingest => {
            // here the ingest server has not found the stream
            // so it should check if the stream exists in storage
            let store = CONFIG.storage().get_object_store();
            let streams = store.list_streams().await?;
            if !streams.contains(&LogStream {
                name: stream_name.to_owned(),
            }) {
                log::error!("Stream {} not found", stream_name);
                return Err(PostError::Invalid(anyhow::anyhow!(
                    "Stream {} not found. Has it been created?",
                    stream_name
                )));
            }
            metadata::STREAM_INFO
                .upsert_stream_info(
                    &*store,
                    LogStream {
                        name: stream_name.to_owned(),
                    },
                )
                .await
                .map_err(|_| PostError::StreamNotFound(stream_name.to_owned()))?;
        }
    }
    Ok(())
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
    Error(std::io::Error),
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
            PostError::Error(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
        handlers::{PREFIX_META, PREFIX_TAGS},
    };

    use super::into_event_batch;

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
                "c": 1
            },
            {
                "a": 1,
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
