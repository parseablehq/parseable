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

use crate::handlers::http::query::{get_records_and_fields, Query};
use crate::metadata::STREAM_INFO;
use crate::panorama::{PanoramaRecord, PANORAMA_SESSION};
use actix_web::http::header::ContentType;
use actix_web::web::{self, Json};
use actix_web::{FromRequest, HttpRequest, Responder};
use futures_util::Future;
use http::StatusCode;
use pyo3::{pyclass, Python};
use serde_json::json;
use std::fmt::Display;
use std::pin::Pin;

/// Panorama function
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum PanoramaFunction {
    Train,
    Forecast,
    DetectAnomaly,
}
impl Display for PanoramaFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PanoramaFunction::Train => write!(f, "train"),
            PanoramaFunction::Forecast => write!(f, "forecast"),
            PanoramaFunction::DetectAnomaly => write!(f, "detectAnomaly"),
        }
    }
}

/// Aggregation period
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum AggregationPeriod {
    Second,
    Minute,
}

impl Display for AggregationPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationPeriod::Second => write!(f, "1s"),
            AggregationPeriod::Minute => write!(f, "1m"),
        }
    }
}

/// model type
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ModelType {
    Prophet,
}

impl Display for ModelType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelType::Prophet => write!(f, "prophet"),
        }
    }
}

/// Panorama request body.
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
#[pyclass]
pub struct PanoramaRequestBody {
    pub function: PanoramaFunction,
    pub stream: String,
    pub model_type: ModelType,
    pub aggregation_period: AggregationPeriod,
    pub train_start: String,
    pub train_end: String,
    pub pred_start: Option<String>,
    pub pred_end: Option<String>,
    pub actual_start: Option<String>,
    pub actual_end: Option<String>,
}

/// Panorama request body.
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PanoramaResponseBody {
    // pub stream: String,
    pub record: Vec<PanoramaRecord>,
}

impl PanoramaResponseBody {
    pub fn to_http(&self) -> Result<impl Responder, PanoramaError> {
        let response = json!({
            "records": self.record,
        });

        Ok(web::Json(response))
    }
}

/// POST /panorama
/// This function accepts a body of type `PanoramaRequestBody`
/// All the other logic is implemented in python code
pub async fn panorama_function(
    _req: HttpRequest,
    body: PanoramaRequestBody,
) -> Result<impl Responder, PanoramaError> {
    let mut records = None;
    let mut fields = None;

    match &body.function {
        PanoramaFunction::Train => {
            let time_partition = STREAM_INFO
                .get_time_partition(&body.stream)
                .unwrap()
                .unwrap();
            let query_object = body.into_query(&time_partition, &body.train_start, &body.train_end);
            (records, fields) = get_records_and_fields(&query_object).await.unwrap();
        }
        PanoramaFunction::Forecast => {
            if !(body.pred_start.is_some() & body.pred_end.is_some()) {
                return Err(PanoramaError::InvalidPredictionTime);
            }
        }
        PanoramaFunction::DetectAnomaly => {
            if !(body.actual_start.is_some() & body.actual_end.is_some()) {
                return Err(PanoramaError::InvalidActualTime);
            } else {
                let time_partition = STREAM_INFO
                    .get_time_partition(&body.stream)
                    .unwrap()
                    .unwrap();
                let query_object = body.into_query(
                    &time_partition,
                    &body.actual_start.clone().unwrap(),
                    &body.actual_end.clone().unwrap(),
                );
                (records, fields) = get_records_and_fields(&query_object).await.unwrap();
            }
        }
    }

    let record =
        Python::with_gil(|py| PANORAMA_SESSION.python_function(py, body, records, fields)).unwrap();
    Ok(PanoramaResponseBody { record }.to_http())
}

impl FromRequest for PanoramaRequestBody {
    type Error = actix_web::Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let request = Json::<PanoramaRequestBody>::from_request(req, payload);

        let fut = async move {
            let request = request.await?.into_inner();

            Ok(request)
        };

        Box::pin(fut)
    }
}

#[allow(clippy::wrong_self_convention)]
impl PanoramaRequestBody {
    fn into_query(&self, time_partition_field: &str, start_time: &str, end_time: &str) -> Query {
        // get agg
        let agg = match self.aggregation_period {
            AggregationPeriod::Second => "1 second",
            AggregationPeriod::Minute => "1 minute",
        };

        // get query
        let query = format!("SELECT DATE_BIN('{}', {}, '{}') AS date_bin_timestamp, COUNT(*) AS log_count FROM {} WHERE {} BETWEEN '{}' AND '{}' AND (1 = 1) GROUP BY date_bin_timestamp ORDER BY date_bin_timestamp",agg, time_partition_field, start_time, self.stream, time_partition_field, start_time, end_time);

        Query {
            query,
            start_time: self.train_start.clone(),
            end_time: self.train_end.clone(),
            send_null: false,
            fields: false,
            filter_tags: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PanoramaError {
    #[error("The provided prediction time range is invalid")]
    InvalidPredictionTime,
    #[error("The provided actual time range is invalid")]
    InvalidActualTime,
    #[allow(unused)]
    #[error(
        r#"Error: Failed to Parse Record Batch into Json
Description: {0}"#
    )]
    JsonParse(String),
    #[error("Error: {0}")]
    ActixError(#[from] actix_web::Error),
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

impl actix_web::ResponseError for PanoramaError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            PanoramaError::JsonParse(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
