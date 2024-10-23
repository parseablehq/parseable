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
    handlers::http::{cluster::sync_with_queriers, modal::{coordinator::Method, LEADER}, rbac::RBACError},
    option::CONFIG,
    storage::{object_storage::dashboard_path, ObjectStorageError},
    users::dashboards::{Dashboard, CURRENT_DASHBOARD_VERSION, DASHBOARDS},
    utils::{get_hash, get_user_from_request},
};
use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use rand::distributions::DistString;

use chrono::Utc;
use http::StatusCode;
use serde_json::Error as SerdeError;

pub async fn list(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let user_id = get_user_from_request(&req)?;
    let dashboards = DASHBOARDS.list_dashboards_by_user(&get_hash(&user_id));

    Ok((web::Json(dashboards), StatusCode::OK))
}

pub async fn get(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let user_id = get_user_from_request(&req)?;
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    if let Some(dashboard) = DASHBOARDS.get_dashboard(dashboard_id, &get_hash(&user_id)) {
        return Ok((web::Json(dashboard), StatusCode::OK));
    }

    Err(DashboardError::Metadata("Dashboard does not exist"))
}

pub async fn post(req: HttpRequest, body: Bytes) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let mut dashboard: Dashboard = serde_json::from_slice(&body)?;
    let dashboard_id = get_hash(Utc::now().timestamp_micros().to_string().as_str());
    dashboard.dashboard_id = Some(dashboard_id.clone());
    dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());

    dashboard.user_id = Some(user_id.clone());
    for tile in dashboard.tiles.iter_mut() {
        tile.tile_id = Some(get_hash(
            format!(
                "{}{}",
                rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 8),
                Utc::now().timestamp_micros()
            )
            .as_str(),
        ));
    }
    DASHBOARDS.update(&dashboard);

    let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));

    if LEADER.lock().is_leader() {
        let store = CONFIG.storage().get_object_store();
        let dashboard_bytes = serde_json::to_vec(&dashboard)?;
        store
            .put_object(&path, Bytes::from(dashboard_bytes))
            .await?;

        sync_with_queriers(req.headers().clone(), Some(body), &format!("dashboards"), Method::POST).await?;
    }


    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn update(req: HttpRequest, body: Bytes) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    if DASHBOARDS.get_dashboard(dashboard_id, &user_id).is_none() {
        return Err(DashboardError::Metadata("Dashboard does not exist"));
    }
    let mut dashboard: Dashboard = serde_json::from_slice(&body)?;
    dashboard.dashboard_id = Some(dashboard_id.to_string());
    dashboard.user_id = Some(user_id.clone());
    dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
    for tile in dashboard.tiles.iter_mut() {
        if tile.tile_id.is_none() {
            tile.tile_id = Some(get_hash(Utc::now().timestamp_micros().to_string().as_str()));
        }
    }
    DASHBOARDS.update(&dashboard);

    if LEADER.lock().is_leader() {
        let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));
        let store = CONFIG.storage().get_object_store();
        let dashboard_bytes = serde_json::to_vec(&dashboard)?;
        store
            .put_object(&path, Bytes::from(dashboard_bytes))
            .await?;

            sync_with_queriers(req.headers().clone(), Some(body), &format!("{dashboard_id}"), Method::PUT).await?;
    }


    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;
    if DASHBOARDS.get_dashboard(dashboard_id, &user_id).is_none() {
        return Err(DashboardError::Metadata("Dashboard does not exist"));
    }

    if LEADER.lock().is_leader() {
        let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));
        let store = CONFIG.storage().get_object_store();
        store.delete_object(&path).await?;

        sync_with_queriers(req.headers().clone(), None, &format!("{dashboard_id}"), Method::DELETE).await?;
    }

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
    #[error("User does not exist")]
    UserDoesNotExist(#[from] RBACError),
    #[error("Network Error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

impl actix_web::ResponseError for DashboardError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
            Self::Network(err) => err.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Self::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
