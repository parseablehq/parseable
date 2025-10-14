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
    metastore::MetastoreError,
    parseable::PARSEABLE,
    storage::ObjectStorageError,
    users::filters::{CURRENT_FILTER_VERSION, FILTERS, Filter},
    utils::{actix::extract_session_key_from_req, get_hash, get_user_from_request},
};
use actix_web::{
    HttpRequest, HttpResponse, Responder,
    http::header::ContentType,
    web::{self, Json, Path},
};
use http::StatusCode;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Error as SerdeError;
use ulid::Ulid;

#[derive(Debug, Default)]
struct FilterQueryParams {
    stream: Option<String>,
    user_id: Option<String>,
    type_param: Option<String>,
}

static FILTER_QUERY_PARAMS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:^|&)(stream|user_id|type)=([^&]*)").unwrap());

pub async fn list(req: HttpRequest) -> Result<impl Responder, FiltersError> {
    let key =
        extract_session_key_from_req(&req).map_err(|e| FiltersError::Custom(e.to_string()))?;
    let mut filters = FILTERS.list_filters(&key).await;

    let query_string = req.query_string();
    if query_string.is_empty() {
        Ok((web::Json(filters), StatusCode::OK))
    } else {
        let mut params = FilterQueryParams::default();

        let re = FILTER_QUERY_PARAMS_RE.clone();

        for cap in re.captures_iter(query_string) {
            let key = cap.get(1).unwrap().as_str();
            let value = cap.get(2).unwrap().as_str().to_string();

            match key {
                "stream" => params.stream = Some(value),
                "user_id" => params.user_id = Some(value),
                "type" => params.type_param = Some(value),
                _ => {} // This shouldn't happen with our regex
            }
        }

        if params.stream.is_some() {
            filters.retain(|f| f.stream_name.eq(params.stream.as_ref().unwrap()));
        }

        if params.user_id.is_some() {
            filters.retain(|f| f.user_id == Some(params.user_id.clone().unwrap()));
        }

        if params.type_param.is_some() {
            filters.retain(|f| f.query.filter_type == params.type_param.clone().unwrap());
        }

        Ok((web::Json(filters), StatusCode::OK))
    }
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

    Err(FiltersError::Metadata(
        "Filter does not exist or user is not authorized",
    ))
}

pub async fn post(
    req: HttpRequest,
    Json(mut filter): Json<Filter>,
) -> Result<impl Responder, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = Ulid::new().to_string();
    filter.filter_id = Some(filter_id.clone());
    filter.user_id = Some(user_id.clone());
    filter.version = Some(CURRENT_FILTER_VERSION.to_string());

    PARSEABLE.metastore.put_filter(&filter).await?;
    FILTERS.update(&filter).await;

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
        return Err(FiltersError::Metadata(
            "Filter does not exist or user is not authorized",
        ));
    }
    filter.filter_id = Some(filter_id.clone());
    filter.user_id = Some(user_id.clone());
    filter.version = Some(CURRENT_FILTER_VERSION.to_string());

    PARSEABLE.metastore.put_filter(&filter).await?;
    FILTERS.update(&filter).await;

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
        .ok_or(FiltersError::Metadata(
            "Filter does not exist or user is not authorized",
        ))?;

    PARSEABLE.metastore.delete_filter(&filter).await?;
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
    #[error(transparent)]
    MetastoreError(#[from] MetastoreError),
}

impl actix_web::ResponseError for FiltersError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
            Self::Custom(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::MetastoreError(e) => e.status_code(),
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        match self {
            FiltersError::MetastoreError(metastore_error) => {
                actix_web::HttpResponse::build(self.status_code())
                    .insert_header(ContentType::json())
                    .json(metastore_error.to_detail())
            }
            _ => actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string()),
        }
    }
}
