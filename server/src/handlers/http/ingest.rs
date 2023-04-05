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

use std::sync::Arc;

use actix_web::http::header::ContentType;
use actix_web::{HttpRequest, HttpResponse};
use arrow_schema::Schema;
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
// ingests events by extacting stream name from header
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
    let (size, rb) = into_event_batch(req, body, &get_stream_schema(&stream_name))?;

    event::Event {
        rb,
        stream_name,
        origin_format: "json",
        origin_size: size as u64,
    }
    .process()
    .await?;

    Ok(())
}

// This function is decoupled from handler itself for testing purpose
fn into_event_batch(
    req: HttpRequest,
    body: Bytes,
    schema: &Schema,
) -> Result<(usize, arrow_array::RecordBatch), PostError> {
    let tags = collect_labelled_headers(&req, PREFIX_TAGS, SEPARATOR)?;
    let metadata = collect_labelled_headers(&req, PREFIX_META, SEPARATOR)?;
    let size = body.len();
    let body: Value = serde_json::from_slice(&body)?;
    let event = format::json::Event {
        data: body,
        tags,
        metadata,
    };
    let rb = event.into_recordbatch(schema)?;
    Ok((size, rb))
}

fn get_stream_schema(stream_name: &str) -> Arc<Schema> {
    STREAM_INFO.schema(stream_name).unwrap()
}

#[derive(Debug, thiserror::Error)]
pub enum PostError {
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

    use actix_web::test::TestRequest;
    use arrow_array::{cast::as_string_array, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use datafusion::common::cast::{as_float64_array, as_int64_array};
    use serde_json::json;

    use crate::{
        event,
        handlers::{PREFIX_META, PREFIX_TAGS},
    };

    use super::into_event_batch;

    #[test]
    fn test_basic_object_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
            "c": 4.23
        });

        let req = TestRequest::default()
            .append_header((PREFIX_TAGS.to_string() + "A", "tag1"))
            .append_header((PREFIX_TAGS.to_string() + "B", "tag2"))
            .append_header((PREFIX_META.to_string() + "C", "meta1"))
            .to_http_request();

        let (size, rb) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &Schema::empty(),
        )
        .unwrap();

        assert_eq!(size, 28);
        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 6);
        assert_eq!(
            as_int64_array(rb.column(0)).unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            as_string_array(rb.column(1)),
            &StringArray::from_iter_values(["hello"])
        );
        assert_eq!(
            as_float64_array(rb.column(2)).unwrap(),
            &Float64Array::from_iter([4.23])
        );
        assert_eq!(
            as_string_array(rb.column_by_name(event::DEFAULT_TAGS_KEY).unwrap()),
            &StringArray::from_iter_values(["a=tag1^b=tag2"])
        );
        assert_eq!(
            as_string_array(rb.column_by_name(event::DEFAULT_METADATA_KEY).unwrap()),
            &StringArray::from_iter_values(["c=meta1"])
        );
    }

    #[test]
    fn test_basic_object_with_null_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
            "c": null
        });

        let req = TestRequest::default().to_http_request();

        let (_, rb) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &Schema::empty(),
        )
        .unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(
            as_int64_array(rb.column(0)).unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            as_string_array(rb.column(1)),
            &StringArray::from_iter_values(["hello"])
        );
    }

    #[test]
    fn test_basic_object_derive_schema_into_rb() {
        let json = json!({
            "a": 1,
            "b": "hello",
        });

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]);

        let req = TestRequest::default().to_http_request();

        let (_, rb) = into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &schema,
        )
        .unwrap();

        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(
            as_int64_array(rb.column(0)).unwrap(),
            &Int64Array::from_iter([1])
        );
        assert_eq!(
            as_string_array(rb.column(1)),
            &StringArray::from_iter_values(["hello"])
        );
    }

    #[test]
    fn test_basic_object_schema_mismatch() {
        let json = json!({
            "a": 1,
            "b": 1, // type mismatch
        });

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]);

        let req = TestRequest::default().to_http_request();

        dbg!(into_event_batch(
            req,
            Bytes::from(serde_json::to_vec(&json).unwrap()),
            &schema,
        ));
    }
}
