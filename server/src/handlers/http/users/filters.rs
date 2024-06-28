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
    users::filters::{Filter, CURRENT_FILTER_VERSION, FILTERS},
};
use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use http::StatusCode;
use rand::distributions::DistString;
use serde_json::Error as SerdeError;

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
    let filters = FILTERS.list_filters_by_user_and_stream(user_id, stream_name);

    Ok((web::Json(filters), StatusCode::OK))
}

pub async fn get(req: HttpRequest) -> Result<impl Responder, FiltersError> {
    let filter_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;

    if let Some(filter) = FILTERS.get_filter(filter_id) {
        return Ok((web::Json(filter), StatusCode::OK));
    }

    Err(FiltersError::Metadata("Filter Not Found"))
}

pub async fn post(body: Bytes) -> Result<impl Responder, PostError> {
    let filter: Filter = serde_json::from_slice(&body)?;
    let filter_id = rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
    let user_id = &filter.user_id;
    let stream_name = &filter.stream_name;
    let mut cloned_filter = filter.clone();
    cloned_filter.filter_id = Some(filter_id.clone());
    cloned_filter.version = Some(CURRENT_FILTER_VERSION.to_string());
    FILTERS.update(&cloned_filter);

    let path = filter_path(user_id, stream_name, &format!("{}.json", filter_id));

    let store = CONFIG.storage().get_object_store();
    let filter_bytes = serde_json::to_vec(&cloned_filter)?;
    store.put_object(&path, Bytes::from(filter_bytes)).await?;

    Ok((web::Json(cloned_filter), StatusCode::OK))
}

pub async fn update(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let filter_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;
    let filter = FILTERS
        .get_filter(filter_id)
        .ok_or(FiltersError::Metadata("Filter Not Found"))?;
    let user_id = &filter.user_id;
    let stream_name = &filter.stream_name;

    let mut cloned_filter: Filter = serde_json::from_slice(&body)?;
    cloned_filter.filter_id = Some(filter_id.to_string());
    cloned_filter.version = Some(CURRENT_FILTER_VERSION.to_string());
    FILTERS.update(&cloned_filter);

    let path = filter_path(user_id, stream_name, &format!("{}.json", filter_id));

    let store = CONFIG.storage().get_object_store();
    let filter_bytes = serde_json::to_vec(&cloned_filter)?;
    store.put_object(&path, Bytes::from(filter_bytes)).await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, PostError> {
    let filter_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;
    let filter = FILTERS
        .get_filter(filter_id)
        .ok_or(FiltersError::Metadata("Filter Not Found"))?;
    let stream_name = &filter.stream_name;
    let user_id = &filter.user_id;

    let path = filter_path(user_id, stream_name, &format!("{}.json", filter_id));
    let store = CONFIG.storage().get_object_store();
    store.delete_object(&path).await?;

    FILTERS.delete_filter(filter_id);

    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, thiserror::Error)]
pub enum FiltersError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Serde Error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Operation cannot be performed: {0}")]
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
