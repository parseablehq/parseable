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
    handlers::http::ingest::PostError,
    option::CONFIG,
    storage::{object_storage::dashboard_path, ObjectStorageError},
    users::dashboards::{Dashboard, CURRENT_DASHBOARD_VERSION, DASHBOARDS},
};
use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;

use chrono::Utc;
use http::StatusCode;
use serde_json::Error as SerdeError;

pub async fn list(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(DashboardError::Metadata("No User Id Provided"))?;
    let dashboards = DASHBOARDS.list_dashboards_by_user(user_id);

    Ok((web::Json(dashboards), StatusCode::OK))
}

pub async fn get(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    if let Some(dashboard) = DASHBOARDS.get_dashboard(dashboard_id) {
        return Ok((web::Json(dashboard), StatusCode::OK));
    }

    Err(DashboardError::Metadata("Dashboard does not exist"))
}

pub async fn post(body: Bytes) -> Result<impl Responder, PostError> {
    let mut dashboard: Dashboard = serde_json::from_slice(&body)?;
    let dashboard_id = format!("{}.{}", &dashboard.user_id, Utc::now().timestamp_millis());
    dashboard.dashboard_id = Some(dashboard_id.clone());
    dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
    DASHBOARDS.update(&dashboard);

    let path = dashboard_path(&dashboard.user_id, &format!("{}.json", dashboard_id));

    let store = CONFIG.storage().get_object_store();
    let dashboard_bytes = serde_json::to_vec(&dashboard)?;
    store
        .put_object(&path, Bytes::from(dashboard_bytes))
        .await?;

    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn update(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;
    if DASHBOARDS.get_dashboard(dashboard_id).is_none() {
        return Err(PostError::DashboardError(DashboardError::Metadata(
            "Dashboard does not exist",
        )));
    }
    let mut dashboard: Dashboard = serde_json::from_slice(&body)?;
    dashboard.dashboard_id = Some(dashboard_id.to_string());
    dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
    DASHBOARDS.update(&dashboard);

    let path = dashboard_path(&dashboard.user_id, &format!("{}.json", dashboard_id));

    let store = CONFIG.storage().get_object_store();
    let dashboard_bytes = serde_json::to_vec(&dashboard)?;
    store
        .put_object(&path, Bytes::from(dashboard_bytes))
        .await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, PostError> {
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;
    let dashboard = DASHBOARDS
        .get_dashboard(dashboard_id)
        .ok_or(DashboardError::Metadata("Dashboard does not exist"))?;

    let path = dashboard_path(&dashboard.user_id, &format!("{}.json", dashboard_id));
    let store = CONFIG.storage().get_object_store();
    store.delete_object(&path).await?;

    DASHBOARDS.delete_dashboard(dashboard_id);

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
}

impl actix_web::ResponseError for DashboardError {
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
