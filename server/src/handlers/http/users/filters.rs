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

use crate::{
    handlers::{http::ingest::PostError, STREAM_NAME_HEADER_KEY},
    option::CONFIG,
    storage::{object_storage::filter_path, ObjectStorageError},
    users::filters::{Filter, FILTERS},
};
use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use http::StatusCode;
use serde_json::{Error as SerdeError, Value as JsonValue};

pub async fn list(req: HttpRequest) -> Result<impl Responder, FiltersError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(FiltersError::Metadata("No User Id Provided"))?;

    let stream_name = req
        .headers()
        .iter()
        .find(|&(key, _)| key == STREAM_NAME_HEADER_KEY)
        .ok_or_else(|| FiltersError::Metadata("Stream Name Not Provided"))?
        .1
        .to_str()
        .map_err(|_| FiltersError::Metadata("Non ASCII Stream Name Provided"))?;

    // .users/user_id/filters/stream_name/filter_id
    let path = filter_path(user_id, stream_name, "");

    let store = CONFIG.storage().get_object_store();
    let filters = store
        .get_objects(
            Some(&path),
            Box::new(|file_name: String| file_name.ends_with("json")),
        )
        .await?;

    let mut filt = vec![];
    for filter in filters {
        filt.push(serde_json::from_slice::<JsonValue>(&filter)?)
    }

    Ok((web::Json(filt), StatusCode::OK))
}

pub async fn get(req: HttpRequest) -> Result<impl Responder, FiltersError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(FiltersError::Metadata("No User Id Provided"))?;

    let filt_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;

    let stream_name = req
        .headers()
        .iter()
        .find(|&(key, _)| key == STREAM_NAME_HEADER_KEY)
        .ok_or_else(|| FiltersError::Metadata("Stream Name Not Provided"))?
        .1
        .to_str()
        .map_err(|_| FiltersError::Metadata("Non ASCII Stream Name Provided"))?;

    if let Some(filter) = FILTERS.find(filt_id) {
        return Ok((web::Json(filter), StatusCode::OK));
    }

    // if it is not in memory go to s3
    let path = filter_path(user_id, stream_name, &format!("{}.json", filt_id));
    let resource = CONFIG
        .storage()
        .get_object_store()
        .get_object(&path)
        .await?;

    let resource = serde_json::from_slice::<Filter>(&resource)?;

    Ok((web::Json(resource), StatusCode::OK))
}

pub async fn post(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(FiltersError::Metadata("No User Id Provided"))?;

    let filt_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;

    let stream_name = req
        .headers()
        .iter()
        .find(|&(key, _)| key == STREAM_NAME_HEADER_KEY)
        .ok_or_else(|| FiltersError::Metadata("Stream Name Not Provided"))?
        .1
        .to_str()
        .map_err(|_| FiltersError::Metadata("Non ASCII Stream Name Provided"))?;

    let path = filter_path(user_id, stream_name, &format!("{}.json", filt_id));
    let filter: Filter = serde_json::from_slice(&body)?;
    FILTERS.update(filter);

    let store = CONFIG.storage().get_object_store();
    store.put_object(&path, body).await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, PostError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(FiltersError::Metadata("No User Id Provided"))?;

    let filt_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;

    let stream_name = req
        .headers()
        .iter()
        .find(|&(key, _)| key == STREAM_NAME_HEADER_KEY)
        .ok_or_else(|| FiltersError::Metadata("Stream Name Not Provided"))?
        .1
        .to_str()
        .map_err(|_| FiltersError::Metadata("Non ASCII Stream Name Provided"))?;

    let path = filter_path(user_id, stream_name, &format!("{}.json", filt_id));
    let store = CONFIG.storage().get_object_store();
    store.delete_object(&path).await?;

    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, thiserror::Error)]
pub enum FiltersError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Serde Error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Cannot perform this operation: {0}")]
    Metadata(&'static str),
}

impl actix_web::ResponseError for FiltersError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
