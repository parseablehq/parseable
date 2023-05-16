/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::collections::HashMap;

use actix_web::http::header::ContentType;
use actix_web::{HttpRequest, HttpResponse};
use arrow_schema::Field;
use bytes::Bytes;
use http::StatusCode;
use serde_json::Value;

use crate::event::error::EventError;
use crate::event::format::EventFormat;
use crate::event::{self, format};
use crate::handlers::{PREFIX_META, PREFIX_TAGS, SEPARATOR, STREAM_NAME_HEADER_KEY};
use crate::metadata::STREAM_INFO;
use crate::utils::header_parsing::{collect_labelled_headers, ParseHeaderError};

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
        if let Err(e) = super::logstream::create_stream_if_not_exists(&stream_name).await {
            return Err(PostError::CreateStream(e.into()));
        }
        push_logs(stream_name, req, body).await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        Err(PostError::Header(ParseHeaderError::MissingStreamName))
    }
}

// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
pub async fn post_event(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    push_logs(stream_name, req, body).await?;
    Ok(HttpResponse::Ok().finish())
}

async fn push_logs(stream_name: String, req: HttpRequest, body: Bytes) -> Result<(), PostError> {
    let (size, rb, is_first_event) = {
        let hash_map = STREAM_INFO.read().unwrap();
        let schema = &hash_map
            .get(&stream_name)
            .ok_or(PostError::StreamNotFound(stream_name.clone()))?
            .schema;
        into_event_batch(req, body, schema)?
    };

    event::Event {
        rb,
        stream_name,
        origin_format: "json",
        origin_size: size as u64,
        is_first_event,
    }
    .process()
    .await?;

    Ok(())
}

fn into_event_batch(
    req: HttpRequest,
    body: Bytes,
    schema: &HashMap<String, Field>,
) -> Result<(usize, arrow_array::RecordBatch, bool), PostError> {
    let tags = collect_labelled_headers(&req, PREFIX_TAGS, SEPARATOR)?;
    let metadata = collect_labelled_headers(&req, PREFIX_META, SEPARATOR)?;
    let size = body.len();
    let body: Value = serde_json::from_slice(&body)?;
    let event = format::json::Event {
        data: body,
        tags,
        metadata,
    };
    let (rb, is_first) = event.into_recordbatch(schema)?;
    Ok((size, rb, is_first))
}

#[derive(Debug, thiserror::Error)]
pub enum PostError {
    #[error("{0}")]
    StreamNotFound(String),
    #[error("Could not deserialize into JSON object, {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Header Error: {0}")]
    Header(#[from] ParseHeaderError),
    #[error("Event Error: {0}")]
    Event(#[from] EventError),
    #[error("Invalid Request: {0}")]
    Invalid(#[from] anyhow::Error),
    #[error("Failed to create stream due to {0}")]
    CreateStream(Box<dyn std::error::Error + Send + Sync>),
}

impl actix_web::ResponseError for PostError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PostError::SerdeError(_) => StatusCode::BAD_REQUEST,
            PostError::Header(_) => StatusCode::BAD_REQUEST,
            PostError::Event(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::Invalid(_) => StatusCode::BAD_REQUEST,
            PostError::CreateStream(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PostError::StreamNotFound(_) => StatusCode::NOT_FOUND,
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

    use std::collections::HashMap;

    use actix_web::test::TestRequest;
    use arrow_array::{
        types::Int64Type, ArrayRef, Float64Array, Int64Array, ListArray, StringArray,
    };
    use arrow_schema::{DataType, Field};
    use bytes::Bytes;
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

        let (size, rb, _) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &HashMap::default(),
        )
        .unwrap();

        assert_eq!(size, 28);
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

        let (_, rb, _) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &HashMap::default(),
        )
        .unwrap();

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
    fn basic_object_derive_schema_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
        });

        let schema = HashMap::from([
            ("a".to_string(), Field::new("a", DataType::Int64, true)),
            ("b".to_string(), Field::new("b", DataType::Utf8, true)),
            ("c".to_string(), Field::new("c", DataType::Float64, true)),
        ]);

        let req = TestRequest::default().to_http_request();

        let (_, rb, _) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &schema,
        )
        .unwrap();

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

        let schema = HashMap::from([
            ("a".to_string(), Field::new("a", DataType::Int64, true)),
            ("b".to_string(), Field::new("b", DataType::Utf8, true)),
            ("c".to_string(), Field::new("c", DataType::Float64, true)),
        ]);

        let req = TestRequest::default().to_http_request();

        assert!(into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &schema,
        )
        .is_err());
    }

    #[test]
    fn empty_object() {
        let json = json!({});

        let schema = HashMap::from([
            ("a".to_string(), Field::new("a", DataType::Int64, true)),
            ("b".to_string(), Field::new("b", DataType::Utf8, true)),
            ("c".to_string(), Field::new("c", DataType::Float64, true)),
        ]);

        let req = TestRequest::default().to_http_request();

        let (_, rb, _) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &schema,
        )
        .unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 3);
    }

    #[test]
    fn non_object_arr_is_err() {
        let json = json!([1]);

        let req = TestRequest::default().to_http_request();

        assert!(into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &HashMap::default(),
        )
        .is_err())
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

        let (_, rb, _) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &HashMap::default(),
        )
        .unwrap();

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

        let schema = HashMap::from([
            ("a".to_string(), Field::new("a", DataType::Int64, true)),
            ("b".to_string(), Field::new("b", DataType::Utf8, true)),
            ("c".to_string(), Field::new("c", DataType::Float64, true)),
        ]);
        let req = TestRequest::default().to_http_request();

        let (_, rb, _) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &schema,
        )
        .unwrap();

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
    fn arr_obj_ignore_all_null_field() {
        let json = json!([
            {
                "a": 1,
                "b": "hello",
                "c": null
            },
            {
                "a": 1,
                "b": "hello",
                "c": null
            },
            {
                "a": 1,
                "b": "hello",
                "c": null
            },
        ]);

        let req = TestRequest::default().to_http_request();

        let (_, rb, _) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &HashMap::default(),
        )
        .unwrap();

        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(
            rb.column_by_name("a").unwrap().as_int64_arr(),
            &Int64Array::from(vec![Some(1), Some(1), Some(1)])
        );
        assert_eq!(
            rb.column_by_name("b").unwrap().as_utf8_arr(),
            &StringArray::from(vec![Some("hello"), Some("hello"), Some("hello"),])
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

        let schema = HashMap::from([
            ("a".to_string(), Field::new("a", DataType::Int64, true)),
            ("b".to_string(), Field::new("b", DataType::Utf8, true)),
            ("c".to_string(), Field::new("c", DataType::Float64, true)),
        ]);

        assert!(into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &schema,
        )
        .is_err());
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

        let (_, rb, _) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &HashMap::default(),
        )
        .unwrap();

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
