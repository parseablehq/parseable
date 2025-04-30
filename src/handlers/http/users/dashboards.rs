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
    storage::{object_storage::dashboard_path, ObjectStorageError},
    users::dashboards::{Dashboard, Tile, CURRENT_DASHBOARD_VERSION, DASHBOARDS},
    utils::{get_hash, get_user_from_request},
};
use actix_web::{
    http::header::ContentType,
    web::{self, Json, Path},
    HttpRequest, HttpResponse, Responder,
};
use bytes::Bytes;

use chrono::Utc;
use http::StatusCode;
use serde_json::{Error as SerdeError, Map};
use ulid::Ulid;

pub async fn list() -> Result<impl Responder, DashboardError> {
    let dashboards = DASHBOARDS.list_dashboards().await;
    //dashboards list should contain the title, author and modified date
    let dashboards: Vec<Map<String, serde_json::Value>> = dashboards
        .iter()
        .map(|dashboard| {
            let mut map = Map::new();
            map.insert(
                "title".to_string(),
                serde_json::Value::String(dashboard.title.clone()),
            );
            map.insert(
                "author".to_string(),
                serde_json::Value::String(dashboard.author.as_ref().unwrap().clone()),
            );
            map.insert(
                "modified".to_string(),
                serde_json::Value::String(dashboard.modified.unwrap().to_string()),
            );
            map
        })
        .collect();
    Ok((web::Json(dashboards), StatusCode::OK))
}

pub async fn get(dashboard_id: Path<String>) -> Result<impl Responder, DashboardError> {
    let dashboard_id = if let Ok(dashboard_id) = Ulid::from_string(&dashboard_id.into_inner()) {
        dashboard_id
    } else {
        return Err(DashboardError::Metadata("Invalid dashboard ID"));
    };

    if let Some(dashboard) = DASHBOARDS.get_dashboard(dashboard_id).await {
        return Ok((web::Json(dashboard), StatusCode::OK));
    }

    Err(DashboardError::Metadata("Dashboard does not exist"))
}

pub async fn post(
    req: HttpRequest,
    Json(mut dashboard): Json<Dashboard>,
) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = Ulid::new();
    dashboard.dashboard_id = Some(dashboard_id);
    dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
    dashboard.modified = Some(Utc::now());
    dashboard.author = Some(user_id.clone());
    for tile in dashboard.tiles.iter_mut() {
        tile.tile_id = Ulid::new();
    }
    DASHBOARDS.update(&dashboard).await;

    let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));

    let store = PARSEABLE.storage.get_object_store();
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
    let dashboard_id = if let Ok(dashboard_id) = Ulid::from_string(&dashboard_id.into_inner()) {
        dashboard_id
    } else {
        return Err(DashboardError::Metadata("Invalid dashboard ID"));
    };

    if DASHBOARDS.get_dashboard(dashboard_id).await.is_none() {
        return Err(DashboardError::Metadata("Dashboard does not exist"));
    }
    dashboard.dashboard_id = Some(dashboard_id);
    dashboard.author = Some(user_id.clone());
    dashboard.modified = Some(Utc::now());
    dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
    for tile in dashboard.tiles.iter_mut() {
        if tile.tile_id.is_nil() {
            tile.tile_id = Ulid::new();
        }
    }
    DASHBOARDS.update(&dashboard).await;

    let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));

    let store = PARSEABLE.storage.get_object_store();
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
    let dashboard_id = if let Ok(dashboard_id) = Ulid::from_string(&dashboard_id.into_inner()) {
        dashboard_id
    } else {
        return Err(DashboardError::Metadata("Invalid dashboard ID"));
    };
    if DASHBOARDS.get_dashboard(dashboard_id).await.is_none() {
        return Err(DashboardError::Metadata("Dashboard does not exist"));
    }
    let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));
    let store = PARSEABLE.storage.get_object_store();
    store.delete_object(&path).await?;

    DASHBOARDS.delete_dashboard(dashboard_id).await;

    Ok(HttpResponse::Ok().finish())
}

pub async fn add_tile(
    req: HttpRequest,
    dashboard_id: Path<String>,
    Json(mut tile): Json<Tile>,
) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = if let Ok(dashboard_id) = Ulid::from_string(&dashboard_id.into_inner()) {
        dashboard_id
    } else {
        return Err(DashboardError::Metadata("Invalid dashboard ID"));
    };

    let mut dashboard = DASHBOARDS
        .get_dashboard(dashboard_id)
        .await
        .ok_or(DashboardError::Metadata("Dashboard does not exist"))?;
    if tile.tile_id.is_nil() {
        tile.tile_id = Ulid::new();
    }
    dashboard.tiles.push(tile.clone());
    dashboard.modified = Some(Utc::now());
    dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
    DASHBOARDS.update(&dashboard).await;

    let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));

    let store = PARSEABLE.storage.get_object_store();
    let dashboard_bytes = serde_json::to_vec(&dashboard)?;
    store
        .put_object(&path, Bytes::from(dashboard_bytes))
        .await?;

    Ok((web::Json(dashboard), StatusCode::OK))
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
    #[error("Error: {0}")]
    Custom(String),
}

impl actix_web::ResponseError for DashboardError {
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
