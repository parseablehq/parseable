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

use actix_web::{http::header::HeaderMap, web, HttpResponse, Responder};
use bytes::Bytes;
use http::StatusCode;

use crate::{
    handlers::http::{
        cluster::{sync_role_update_with_ingestors, sync_with_queriers},
        modal::{
            utils::rbac_utils::{get_metadata, put_metadata},
            LEADER,
        },
        role::RoleError,
    },
    rbac::{
        map::{mut_roles, DEFAULT_ROLE},
        role::model::DefaultPrivilege,
    },
    storage,
};

use super::{LeaderRequest, Method};

// Handler for PUT /api/v1/role/{name}
// Creates a new role or update existing one
pub async fn put(name: web::Path<String>, body: Bytes) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let privileges = serde_json::from_slice::<Vec<DefaultPrivilege>>(&body)?;

    if LEADER.lock().await.is_leader() {
        let mut metadata = get_metadata().await?;
        metadata.roles.insert(name.clone(), privileges.clone());

        put_metadata(&metadata).await?;
        mut_roles().insert(name.clone(), privileges.clone());

        sync_role_update_with_ingestors(name.clone(), privileges.clone()).await?;
        sync_with_queriers(
            HeaderMap::new(),
            Some(body),
            &format!("role/{name}/sync"),
            Method::Put,
        )
        .await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        let resource = name.to_string();
        let request = LeaderRequest {
            body: None,
            api: "role",
            resource: Some(&resource),
            method: Method::Put,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(HttpResponse::Ok().finish()),
            _ => {
                let err_msg = res.text().await?;
                Err(RoleError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn put_sync(name: web::Path<String>, body: Bytes) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let privileges = serde_json::from_slice::<Vec<DefaultPrivilege>>(&body)?;
    let mut metadata = get_metadata().await?;
    metadata.roles.insert(name.clone(), privileges.clone());

    storage::put_staging_metadata(&metadata).map_err(|err| {
        log::error!("error while putting role: {err:?}");
        RoleError::Anyhow(anyhow::Error::msg(err))
    })?;

    mut_roles().insert(name.clone(), privileges.clone());

    Ok(HttpResponse::Ok().finish())
}

// Handler for PUT /api/v1/role/default
// Delete existing role
pub async fn put_default(name: web::Json<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    if LEADER.lock().await.is_leader() {
        let mut metadata = get_metadata().await?;
        metadata.default_role = Some(name.clone());
        *DEFAULT_ROLE.lock().unwrap() = Some(name);

        put_metadata(&metadata).await?;
        sync_with_queriers(HeaderMap::new(), None, "role/default/sync", Method::Put).await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        let request = LeaderRequest {
            body: None,
            api: "role",
            resource: Some("default/sync"),
            method: Method::Put,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(HttpResponse::Ok().finish()),
            _ => {
                let err_msg = res.text().await?;
                Err(RoleError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn put_default_sync(name: web::Json<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let mut metadata = get_metadata().await?;
    metadata.default_role = Some(name.clone());
    *DEFAULT_ROLE.lock().unwrap() = Some(name);

    storage::put_staging_metadata(&metadata).map_err(|err| {
        log::error!("error while putting default role: {err:?}");
        RoleError::Anyhow(anyhow::Error::msg(err))
    })?;

    Ok(HttpResponse::Ok().finish())
}

// Handler for DELETE /api/v1/role/{username}
// Delete existing role
pub async fn delete(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    if LEADER.lock().await.is_leader() {
        let mut metadata = get_metadata().await?;
        if metadata.users.iter().any(|user| user.roles.contains(&name)) {
            return Err(RoleError::RoleInUse);
        }
        metadata.roles.remove(&name);
        mut_roles().remove(&name);

        put_metadata(&metadata).await?;
        sync_with_queriers(
            HeaderMap::new(),
            None,
            &format!("role/{name}/sync"),
            Method::Delete,
        )
        .await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        let resource = name.to_string();
        let request = LeaderRequest {
            body: None,
            api: "role",
            resource: Some(&resource),
            method: Method::Delete,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(HttpResponse::Ok().finish()),
            _ => {
                let err_msg = res.text().await?;
                Err(RoleError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn delete_sync(name: web::Path<String>) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let mut metadata = get_metadata().await?;
    if metadata.users.iter().any(|user| user.roles.contains(&name)) {
        return Err(RoleError::RoleInUse);
    }
    metadata.roles.remove(&name);
    mut_roles().remove(&name);

    storage::put_staging_metadata(&metadata).map_err(|err| {
        log::error!("error while deleting role: {err:?}");
        RoleError::Anyhow(anyhow::Error::msg(err))
    })?;

    Ok(HttpResponse::Ok().finish())
}
