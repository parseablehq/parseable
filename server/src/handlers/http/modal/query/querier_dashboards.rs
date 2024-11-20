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

use actix_web::{web, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use chrono::Utc;
use http::StatusCode;
use rand::distributions::DistString;

use crate::{
    handlers::http::{
        cluster::sync_with_queriers, modal::LEADER, users::dashboards::DashboardError,
    },
    option::CONFIG,
    storage::object_storage::dashboard_path,
    users::dashboards::{Dashboard, CURRENT_DASHBOARD_VERSION, DASHBOARDS},
    utils::{get_hash, get_user_from_request},
};

use super::{LeaderRequest, Method};

pub async fn post(req: HttpRequest, body: Bytes) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;

    if LEADER.lock().await.is_leader() {
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

        let store = CONFIG.storage().get_object_store();
        let dashboard_bytes = serde_json::to_vec(&dashboard)?;
        store
            .put_object(&path, Bytes::from(dashboard_bytes))
            .await?;

        sync_with_queriers(
            req.headers().clone(),
            Some(body),
            "dashboards/sync",
            Method::Post,
        )
        .await?;

        Ok((web::Json(dashboard), StatusCode::OK))
    } else {
        let request = LeaderRequest {
            body: Some(body),
            api: "dashboards",
            resource: None,
            method: Method::Post,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok((web::Json(res.json().await?), StatusCode::OK)),
            _ => {
                let err_msg = res.text().await?;
                Err(DashboardError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn post_sync(req: HttpRequest, body: Bytes) -> Result<impl Responder, DashboardError> {
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

    Ok((web::Json(dashboard), StatusCode::OK))
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    if LEADER.lock().await.is_leader() {
        user_id = get_hash(&user_id);

        if DASHBOARDS.get_dashboard(dashboard_id, &user_id).is_none() {
            return Err(DashboardError::Metadata("Dashboard does not exist"));
        }

        DASHBOARDS.delete_dashboard(dashboard_id);

        let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));
        let store = CONFIG.storage().get_object_store();
        store.delete_object(&path).await?;

        sync_with_queriers(
            req.headers().clone(),
            None,
            &format!("dashboards/{dashboard_id}/sync"),
            Method::Delete,
        )
        .await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        let resource = dashboard_id.to_string();
        let request = LeaderRequest {
            body: None,
            api: "dashboards",
            resource: Some(&resource),
            method: Method::Delete,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(HttpResponse::Ok().finish()),
            _ => {
                let err_msg = res.text().await?;
                Err(DashboardError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn delete_sync(req: HttpRequest) -> Result<HttpResponse, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    user_id = get_hash(&user_id);

    if DASHBOARDS.get_dashboard(dashboard_id, &user_id).is_none() {
        return Err(DashboardError::Metadata("Dashboard does not exist"));
    }

    DASHBOARDS.delete_dashboard(dashboard_id);

    Ok(HttpResponse::Ok().finish())
}

pub async fn update(req: HttpRequest, body: Bytes) -> Result<impl Responder, DashboardError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let dashboard_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    if LEADER.lock().await.is_leader() {
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

        let path = dashboard_path(&user_id, &format!("{}.json", dashboard_id));
        let store = CONFIG.storage().get_object_store();
        let dashboard_bytes = serde_json::to_vec(&dashboard)?;
        store
            .put_object(&path, Bytes::from(dashboard_bytes))
            .await?;

        sync_with_queriers(
            req.headers().clone(),
            Some(body),
            &format!("dashboards/{dashboard_id}/sync"),
            Method::Put,
        )
        .await?;
        Ok((web::Json(dashboard), StatusCode::OK))
    } else {
        let resource = dashboard_id.to_string();
        let request = LeaderRequest {
            body: None,
            api: "dashboards",
            resource: Some(&resource),
            method: Method::Put,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok((web::Json(res.json().await?), StatusCode::OK)),
            _ => {
                let err_msg = res.text().await?;
                Err(DashboardError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn update_sync(req: HttpRequest, body: Bytes) -> Result<impl Responder, DashboardError> {
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

    Ok((web::Json(dashboard), StatusCode::OK))
}
