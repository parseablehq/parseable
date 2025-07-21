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

use std::collections::HashMap;

use crate::{
    handlers::http::rbac::RBACError,
    storage::ObjectStorageError,
    users::dashboards::{DASHBOARDS, Dashboard, Tile, validate_dashboard_id},
    utils::{get_hash, get_user_from_request},
};
use actix_web::{
    HttpRequest, HttpResponse, Responder,
    http::header::ContentType,
    web::{self, Json, Path},
};
use http::StatusCode;
use serde_json::Error as SerdeError;

pub async fn list_dashboards(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let query_map = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|_| DashboardError::InvalidQueryParameter)?;
    let mut dashboard_limit = 0;
    if !query_map.is_empty() {
        if let Some(limit) = query_map.get("limit") {
            if let Ok(parsed_limit) = limit.parse::<usize>() {
                dashboard_limit = parsed_limit;
            } else {
                return Err(DashboardError::Metadata("Invalid limit value"));
            }
        }

        if let Some(tags) = query_map.get("tags") {
            let tags: Vec<String> = tags
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if tags.is_empty() {
                return Err(DashboardError::Metadata("Tags cannot be empty"));
            }
            let dashboards = DASHBOARDS.list_dashboards_by_tags(tags).await;
            let dashboard_summaries = dashboards
                .iter()
                .map(|dashboard| dashboard.to_summary())
                .collect::<Vec<_>>();
            return Ok((web::Json(dashboard_summaries), StatusCode::OK));
        }
    }
    let dashboards = DASHBOARDS.list_dashboards(dashboard_limit).await;
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
    dashboard: Option<Json<Dashboard>>,
) -> Result<impl Responder, DashboardError> {
    let user_id = get_hash(&get_user_from_request(&req)?);
    let dashboard_id = validate_dashboard_id(dashboard_id.into_inner())?;
    let mut existing_dashboard = DASHBOARDS
        .get_dashboard_by_user(dashboard_id, &user_id)
        .await
        .ok_or(DashboardError::Metadata(
            "Dashboard does not exist or user is not authorized",
        ))?;

    let query_map = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|_| DashboardError::InvalidQueryParameter)?;

    // Validate: either query params OR body, not both
    let has_query_params = !query_map.is_empty();
    let has_body_update = dashboard
        .as_ref()
        .is_some_and(|d| d.title != existing_dashboard.title || d.tiles.is_some());

    if has_query_params && has_body_update {
        return Err(DashboardError::Metadata(
            "Cannot use both query parameters and request body for updates",
        ));
    }

    let mut final_dashboard = if has_query_params {
        // Apply partial updates from query parameters
        if let Some(is_favorite) = query_map.get("isFavorite") {
            existing_dashboard.is_favorite = Some(is_favorite == "true");
        }
        if let Some(tags) = query_map.get("tags") {
            let parsed_tags: Vec<String> = tags
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            existing_dashboard.tags = if parsed_tags.is_empty() {
                None
            } else {
                Some(parsed_tags)
            };
        }
        if let Some(rename_to) = query_map.get("renameTo") {
            let trimmed = rename_to.trim();
            if trimmed.is_empty() {
                return Err(DashboardError::Metadata("Rename to cannot be empty"));
            }
            existing_dashboard.title = trimmed.to_string();
        }
        existing_dashboard
    } else {
        let dashboard = dashboard
            .ok_or(DashboardError::Metadata("Request body is required"))?
            .into_inner();
        if let Some(tiles) = &dashboard.tiles {
            if tiles.iter().any(|tile| tile.tile_id.is_nil()) {
                return Err(DashboardError::Metadata("Tile ID must be provided"));
            }

            // Check if tile_id are unique
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

        dashboard
    };

    DASHBOARDS
        .update(&user_id, dashboard_id, &mut final_dashboard)
        .await?;

    Ok((web::Json(final_dashboard), StatusCode::OK))
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

pub async fn list_tags() -> Result<impl Responder, DashboardError> {
    let tags = DASHBOARDS.list_tags().await;
    Ok((web::Json(tags), StatusCode::OK))
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
    #[error("Invalid query parameter")]
    InvalidQueryParameter,
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
            Self::InvalidQueryParameter => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
