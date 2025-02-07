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

use actix_web::{
    http::header::ContentType,
    web::{self, Json, Path},
    HttpRequest, HttpResponse, Responder,
};
use bytes::Bytes;
use http::StatusCode;
use serde_json::Error as SerdeError;

use crate::{
    handlers::http::rbac::RBACError,
    option::CONFIG,
    storage::{object_storage::dashboard_path, ObjectStorageError},
    users::dashboards::{Dashboard, DASHBOARDS},
    utils::{get_hash, get_user_from_request},
};

pub async fn list(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let user_id = get_user_from_request(&req)?;
    let dashboards = DASHBOARDS.list_dashboards_by_user(&get_hash(&user_id));

    Ok((web::Json(dashboards), StatusCode::OK))
}

pub async fn get(
    req: HttpRequest,
    dashboard_id: Path<String>,
) -> Result<impl Responder, DashboardError> {
    let user_id = get_user_from_request(&req)?;
    let dashboard_id = dashboard_id.into_inner();

    if let Some(dashboard) = DASHBOARDS.get(&dashboard_id, &get_hash(&user_id)) {
        return Ok((web::Json(dashboard), StatusCode::OK));
    }

    Err(DashboardError::DashboardDoesNotExist)
}

pub async fn post(
    req: HttpRequest,
    Json(mut dashboard): Json<Dashboard>,
) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    dashboard.user_id = Some(user_id.clone());
    DASHBOARDS.update(&dashboard);

    let path = dashboard_path(&user_id, &format!("{}.json", dashboard.dashboard_id));

    let store = CONFIG.storage().get_object_store();
    let dashboard_bytes = serde_json::to_vec(&dashboard)?;
    store
        .put_object(&path, Bytes::from(dashboard_bytes))
        .await?;

    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn update(
    req: HttpRequest,
    dashboard_id: Path<String>,
    Json(mut dashboard): Json<Dashboard>,
) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = dashboard_id.into_inner();

    if DASHBOARDS.get(&dashboard_id, &user_id).is_none() {
        return Err(DashboardError::DashboardDoesNotExist);
    }
    dashboard.dashboard_id = dashboard_id.to_string();
    dashboard.user_id = Some(user_id.clone());
    DASHBOARDS.update(&dashboard);

    let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));

    let store = CONFIG.storage().get_object_store();
    let dashboard_bytes = serde_json::to_vec(&dashboard)?;
    store
        .put_object(&path, Bytes::from(dashboard_bytes))
        .await?;

    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn delete(
    req: HttpRequest,
    dashboard_id: Path<String>,
) -> Result<HttpResponse, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = dashboard_id.into_inner();
    if DASHBOARDS.get(&dashboard_id, &user_id).is_none() {
        return Err(DashboardError::DashboardDoesNotExist);
    }
    let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));
    let store = CONFIG.storage().get_object_store();
    store.delete_object(&path).await?;

    DASHBOARDS.delete(&dashboard_id);

    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, thiserror::Error)]
pub enum DashboardError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Serde Error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Cannot perform this operation: {0}")]
    Metadata(&'static str),
    #[error("Dashboard does not exist")]
    DashboardDoesNotExist,
    #[error("User does not exist")]
    UserDoesNotExist(#[from] RBACError),
}

impl actix_web::ResponseError for DashboardError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::DashboardDoesNotExist => StatusCode::NOT_FOUND,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
