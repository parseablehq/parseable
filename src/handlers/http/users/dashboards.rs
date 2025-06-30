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
use serde_json::Error as SerdeError;

pub async fn list_dashboards() -> Result<impl Responder, DashboardError> {
    let dashboards = DASHBOARDS.list_dashboards().await;
    let dashboard_summaries = dashboards
        .iter()
        .map(|dashboard| dashboard.to_summary())
        .collect::<Vec<_>>();

    Ok((web::Json(dashboard_summaries), StatusCode::OK))
}

pub async fn get_dashboard(dashboard_id: Path<String>) -> Result<impl Responder, DashboardError> {
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;

    let dashboard = DASHBOARDS
        .get_dashboard(dashboard_id)
        .await
        .ok_or_else(|| DashboardError::Metadata("Dashboard does not exist"))?;

    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn create_dashboard(
    req: HttpRequest,
    Json(mut dashboard): Json<Dashboard>,
) -> Result<impl Responder, DashboardError> {
    if dashboard.title.is_empty() {
        return Err(DashboardError::Metadata("Title must be provided"));
    }

    let user_id = get_hash(&get_user_from_request(&req)?);

    DASHBOARDS.create(&user_id, &mut dashboard).await?;
    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn update_dashboard(
    req: HttpRequest,
    dashboard_id: Path<String>,
    Json(mut dashboard): Json<Dashboard>,
) -> Result<impl Responder, DashboardError> {
    let user_id = get_hash(&get_user_from_request(&req)?);
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;

    // Validate all tiles have valid IDs
    if let Some(tiles) = &dashboard.tiles {
        if tiles.iter().any(|tile| tile.tile_id.is_nil()) {
            return Err(DashboardError::Metadata("Tile ID must be provided"));
        }
    }

    // Check if tile_id are unique
    if let Some(tiles) = &dashboard.tiles {
        let unique_tiles: Vec<_> = tiles
            .iter()
            .map(|tile| tile.tile_id)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        if unique_tiles.len() != tiles.len() {
            return Err(DashboardError::Metadata("Tile IDs must be unique"));
        }
    }

    DASHBOARDS
        .update(&user_id, dashboard_id, &mut dashboard)
        .await?;

    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn delete_dashboard(
    req: HttpRequest,
    dashboard_id: Path<String>,
) -> Result<HttpResponse, DashboardError> {
    let user_id = get_hash(&get_user_from_request(&req)?);
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;

    DASHBOARDS.delete_dashboard(&user_id, dashboard_id).await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn add_tile(
    req: HttpRequest,
    dashboard_id: Path<String>,
    Json(tile): Json<Tile>,
) -> Result<impl Responder, DashboardError> {
    if tile.tile_id.is_nil() {
        return Err(DashboardError::Metadata("Tile ID must be provided"));
    }

    let user_id = get_hash(&get_user_from_request(&req)?);
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;

    let mut dashboard = DASHBOARDS
        .get_dashboard_by_user(dashboard_id, &user_id)
        .await
        .ok_or(DashboardError::Unauthorized)?;

    let tiles = dashboard.tiles.get_or_insert_with(Vec::new);

    // check if the tile already exists
    if tiles.iter().any(|t| t.tile_id == tile.tile_id) {
        return Err(DashboardError::Metadata("Tile already exists"));
    }
    tiles.push(tile);

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
    #[error("Dashboard does not exist or is not accessible")]
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
