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
    storage::ObjectStorageError,
    users::dashboards::{validate_dashboard_id, Dashboard, Tile, DASHBOARDS},
    utils::{get_hash, get_user_from_request},
};
use actix_web::{
    http::header::ContentType,
    web::{self, Json, Path},
    HttpRequest, HttpResponse, Responder,
};
use http::StatusCode;
use serde_json::{Error as SerdeError, Map};

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
            if let Some(author) = &dashboard.author {
                map.insert(
                    "author".to_string(),
                    serde_json::Value::String(author.to_string()),
                );
            }
            if let Some(modified) = &dashboard.modified {
                map.insert(
                    "modified".to_string(),
                    serde_json::Value::String(modified.to_string()),
                );
            }
            if let Some(dashboard_id) = &dashboard.dashboard_id {
                map.insert(
                    "dashboard_id".to_string(),
                    serde_json::Value::String(dashboard_id.to_string()),
                );
            }
            map
        })
        .collect();
    Ok((web::Json(dashboards), StatusCode::OK))
}

pub async fn get(dashboard_id: Path<String>) -> Result<impl Responder, DashboardError> {
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;

    if let Some(dashboard) = DASHBOARDS.get_dashboard(dashboard_id).await {
        return Ok((web::Json(dashboard), StatusCode::OK));
    }

    Err(DashboardError::Metadata("Dashboard does not exist"))
}

pub async fn post(
    req: HttpRequest,
    Json(mut dashboard): Json<Dashboard>,
) -> Result<impl Responder, DashboardError> {
    if dashboard.title.is_empty() {
        return Err(DashboardError::Metadata("Title must be provided"));
    }
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    dashboard.author = Some(user_id.clone());

    DASHBOARDS.create(&user_id, &mut dashboard).await?;
    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn update(
    req: HttpRequest,
    dashboard_id: Path<String>,
    Json(mut dashboard): Json<Dashboard>,
) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;

    for tile in dashboard.tiles.as_ref().unwrap_or(&Vec::new()) {
        if tile.tile_id.is_nil() {
            return Err(DashboardError::Metadata(
                "Tile ID must be provided by the client",
            ));
        }
    }
    dashboard.author = Some(user_id.clone());

    DASHBOARDS
        .update(&user_id, dashboard_id, &mut dashboard)
        .await?;
    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn delete(
    req: HttpRequest,
    dashboard_id: Path<String>,
) -> Result<HttpResponse, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;
    DASHBOARDS.delete_dashboard(&user_id, dashboard_id).await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn add_tile(
    req: HttpRequest,
    dashboard_id: Path<String>,
    Json(tile): Json<Tile>,
) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;

    if tile.tile_id.is_nil() {
        return Err(DashboardError::Metadata(
            "Tile ID must be provided by the client",
        ));
    }

    let mut dashboard = DASHBOARDS
        .get_dashboard_by_user(dashboard_id, &user_id)
        .await
        .ok_or(DashboardError::Unauthorized)?;
    let tiles = dashboard.tiles.get_or_insert_with(Vec::new);
    tiles.push(tile.clone());
    DASHBOARDS
        .update(&user_id, dashboard_id, &mut dashboard)
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
    #[error("Unauthorized to access resource")]
    Unauthorized,
}

impl actix_web::ResponseError for DashboardError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
            Self::UserDoesNotExist(_) => StatusCode::NOT_FOUND,
            Self::Custom(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
