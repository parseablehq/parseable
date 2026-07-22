/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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
    storage::{ObjectStorageError, StreamType},
    users::filters::{CURRENT_FILTER_VERSION, FILTERS, Filter},
    utils::{
        actix::extract_session_key_from_req, get_hash, get_user_and_tenant_from_request, is_admin,
    },
    validator,
};
use actix_web::http::StatusCode;
use actix_web::{
    HttpRequest, HttpResponse, Responder,
    http::header::ContentType,
    web::{self, Json, Path},
};
use serde_json::Error as SerdeError;
use ulid::Ulid;

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
    let (user_id, tenant_id) = get_user_and_tenant_from_request(&req)?;
    let filter_id = filter_id.into_inner();
    let is_admin = is_admin(&req).map_err(|e| FiltersError::Custom(e.to_string()))?;
    if let Some(filter) = FILTERS
        .get_filter(&filter_id, &get_hash(&user_id), is_admin, &tenant_id)
        .await
    {
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
    validate_stream_name(&filter.stream_name)?;

    let (mut user_id, tenant_id) = get_user_and_tenant_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = Ulid::new().to_string();
    filter.filter_id = Some(filter_id.clone());
    filter.user_id = Some(user_id.clone());
    filter.version = Some(CURRENT_FILTER_VERSION.to_string());
    filter.tenant_id.clone_from(&tenant_id);
    PARSEABLE.metastore.put_filter(&filter, &tenant_id).await?;
    FILTERS.update(&filter, &tenant_id).await;

    Ok((web::Json(filter), StatusCode::OK))
}

pub async fn update(
    req: HttpRequest,
    filter_id: Path<String>,
    Json(mut filter): Json<Filter>,
) -> Result<impl Responder, FiltersError> {
    validate_stream_name(&filter.stream_name)?;

    let (mut user_id, tenant_id) = get_user_and_tenant_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = filter_id.into_inner();
    let is_admin = is_admin(&req).map_err(|e| FiltersError::Custom(e.to_string()))?;

    if FILTERS
        .get_filter(&filter_id, &user_id, is_admin, &tenant_id)
        .await
        .is_none()
    {
        return Err(FiltersError::Metadata(
            "Filter does not exist or user is not authorized",
        ));
    }
    filter.filter_id = Some(filter_id.clone());
    filter.user_id = Some(user_id.clone());
    filter.version = Some(CURRENT_FILTER_VERSION.to_string());
    filter.tenant_id.clone_from(&tenant_id);

    PARSEABLE.metastore.put_filter(&filter, &tenant_id).await?;
    FILTERS.update(&filter, &tenant_id).await;

    Ok((web::Json(filter), StatusCode::OK))
}

pub async fn delete(
    req: HttpRequest,
    filter_id: Path<String>,
) -> Result<HttpResponse, FiltersError> {
    let (mut user_id, tenant_id) = get_user_and_tenant_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = filter_id.into_inner();
    let is_admin = is_admin(&req).map_err(|e| FiltersError::Custom(e.to_string()))?;
    let filter = FILTERS
        .get_filter(&filter_id, &user_id, is_admin, &tenant_id)
        .await
        .ok_or(FiltersError::Metadata(
            "Filter does not exist or user is not authorized",
        ))?;

    PARSEABLE
        .metastore
        .delete_filter(&filter, &tenant_id)
        .await?;
    FILTERS.delete_filter(&filter_id, &tenant_id).await;

    Ok(HttpResponse::Ok().finish())
}

fn validate_stream_name(stream_name: &str) -> Result<(), FiltersError> {
    validator::stream_name(stream_name, StreamType::UserDefined)
        .map_err(FiltersError::InvalidStreamName)
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
    #[error("Invalid stream name: {0}")]
    InvalidStreamName(#[from] validator::error::StreamNameValidationError),
    #[error(transparent)]
    MetastoreError(#[from] MetastoreError),
}

impl actix_web::ResponseError for FiltersError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
            Self::Custom(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidStreamName(_) => StatusCode::BAD_REQUEST,
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

#[cfg(test)]
mod tests {
    use super::validate_stream_name;
    use crate::validator::error::StreamNameValidationError;

    #[test]
    fn accepts_flat_stream_names() {
        assert!(validate_stream_name("team1").is_ok());
        assert!(validate_stream_name("orders-prod").is_ok());
        assert!(validate_stream_name("user_events_v2").is_ok());
    }

    #[test]
    fn rejects_path_like_stream_names() {
        let err =
            validate_stream_name("team1/serviceA").expect_err("path-like names must be rejected");
        assert!(matches!(
            err,
            super::FiltersError::InvalidStreamName(StreamNameValidationError::NameSpecialChar {
                c: '/'
            })
        ));
    }
}
