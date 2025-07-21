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
    handlers::http::rbac::RBACError,
    parseable::PARSEABLE,
    storage::{ObjectStorageError, object_storage::filter_path},
    users::filters::{CURRENT_FILTER_VERSION, FILTERS, Filter},
    utils::{actix::extract_session_key_from_req, get_hash, get_user_from_request},
};
use actix_web::{
    HttpRequest, HttpResponse, Responder,
    http::header::ContentType,
    web::{self, Json, Path},
};
use bytes::Bytes;
use chrono::Utc;
use http::StatusCode;
use serde_json::Error as SerdeError;

pub async fn list(req: HttpRequest) -> Result<impl Responder, FiltersError> {
    let key =
        extract_session_key_from_req(&req).map_err(|e| FiltersError::Custom(e.to_string()))?;
    let filters = FILTERS.list_filters(&key).await;
    Ok((web::Json(filters), StatusCode::OK))
}

pub async fn get(
    req: HttpRequest,
    filter_id: Path<String>,
) -> Result<impl Responder, FiltersError> {
    let user_id = get_user_from_request(&req)?;
    let filter_id = filter_id.into_inner();

    if let Some(filter) = FILTERS.get_filter(&filter_id, &get_hash(&user_id)).await {
        return Ok((web::Json(filter), StatusCode::OK));
    }

    Err(FiltersError::Metadata("Filter does not exist"))
}

pub async fn post(
    req: HttpRequest,
    Json(mut filter): Json<Filter>,
) -> Result<impl Responder, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = get_hash(Utc::now().timestamp_micros().to_string().as_str());
    filter.filter_id = Some(filter_id.clone());
    filter.user_id = Some(user_id.clone());
    filter.version = Some(CURRENT_FILTER_VERSION.to_string());
    FILTERS.update(&filter).await;

    let path = filter_path(&user_id, &filter.stream_name, &format!("{filter_id}.json"));

    let store = PARSEABLE.storage.get_object_store();
    let filter_bytes = serde_json::to_vec(&filter)?;
    store.put_object(&path, Bytes::from(filter_bytes)).await?;

    Ok((web::Json(filter), StatusCode::OK))
}

pub async fn update(
    req: HttpRequest,
    filter_id: Path<String>,
    Json(mut filter): Json<Filter>,
) -> Result<impl Responder, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = filter_id.into_inner();
    if FILTERS.get_filter(&filter_id, &user_id).await.is_none() {
        return Err(FiltersError::Metadata("Filter does not exist"));
    }
    filter.filter_id = Some(filter_id.clone());
    filter.user_id = Some(user_id.clone());
    filter.version = Some(CURRENT_FILTER_VERSION.to_string());
    FILTERS.update(&filter).await;

    let path = filter_path(&user_id, &filter.stream_name, &format!("{filter_id}.json"));

    let store = PARSEABLE.storage.get_object_store();
    let filter_bytes = serde_json::to_vec(&filter)?;
    store.put_object(&path, Bytes::from(filter_bytes)).await?;

    Ok((web::Json(filter), StatusCode::OK))
}

pub async fn delete(
    req: HttpRequest,
    filter_id: Path<String>,
) -> Result<HttpResponse, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = filter_id.into_inner();
    let filter = FILTERS
        .get_filter(&filter_id, &user_id)
        .await
        .ok_or(FiltersError::Metadata("Filter does not exist"))?;

    let path = filter_path(&user_id, &filter.stream_name, &format!("{filter_id}.json"));
    let store = PARSEABLE.storage.get_object_store();
    store.delete_object(&path).await?;

    FILTERS.delete_filter(&filter_id).await;

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
    #[error("User does not exist")]
    UserDoesNotExist(#[from] RBACError),
    #[error("Error: {0}")]
    Custom(String),
}

impl actix_web::ResponseError for FiltersError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
            Self::Custom(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
