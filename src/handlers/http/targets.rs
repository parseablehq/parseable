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

use actix_web::{
    HttpRequest, Responder,
    web::{self, Json, Path},
};
use futures::stream::FuturesUnordered;
use serde_json::json;
use ulid::Ulid;

use crate::{
    alerts::{
        AlertError,
        target::{TARGETS, Target},
    },
    utils::get_tenant_id_from_request,
};

// POST /targets
pub async fn post(
    req: HttpRequest,
    Json(mut target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    let tenant_id = get_tenant_id_from_request(&req);
    target.tenant = tenant_id;
    target.validate_outbound_policy().await?;
    // should check for duplicacy and liveness (??)
    // add to the map
    TARGETS.update(target.clone()).await?;

    Ok(web::Json(target.mask()))
}

// GET /targets
pub async fn list(req: HttpRequest) -> Result<impl Responder, AlertError> {
    let tenant_id = get_tenant_id_from_request(&req);
    let handles = FuturesUnordered::new();
    // add to the map
    let mut list = vec![];
    for target in TARGETS.list(&tenant_id).await? {
        handles.push(tokio::spawn(async move {
            if let Err(e) = target.validate_outbound_policy().await {
                json!({
                    "target": &target.mask(),
                    "enabled": false,
                    "error": &e.to_string()
                })
            } else {
                json!({
                    "target": &target.mask(),
                    "enabled": true
                })
            }
        }));
    }
    for res in handles {
        list.push(
            res.await
                .map_err(|e| AlertError::CustomError(e.to_string()))?,
        );
    }

    Ok(web::Json(list))
}

// GET /targets/{target_id}
pub async fn get(req: HttpRequest, target_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let target = TARGETS.get_target_by_id(&target_id, &tenant_id).await?;
    let res = if let Err(e) = target.validate_outbound_policy().await {
        json!({
            "target": &target.mask(),
            "enabled": false,
            "error": &e.to_string()
        })
    } else {
        json!({
            "target": &target.mask(),
            "enabled": true
        })
    };
    Ok(web::Json(res))
}

// PUT /targets/{target_id}
pub async fn update(
    req: HttpRequest,
    target_id: Path<Ulid>,
    Json(mut target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    // if target_id does not exist, error
    let old_target = TARGETS.get_target_by_id(&target_id, &tenant_id).await?;

    // do not allow modifying name
    if old_target.name != target.name {
        return Err(AlertError::InvalidTargetModification(
            "Can't modify target name".to_string(),
        ));
    }

    // esnure that the supplied target id is assigned to the target config
    target.id = target_id;
    target.tenant = tenant_id;
    target.validate_outbound_policy().await?;
    // should check for duplicacy and liveness (??)
    // add to the map
    TARGETS.update(target.clone()).await?;

    Ok(web::Json(target.mask()))
}

// DELETE /targets/{target_id}
pub async fn delete(req: HttpRequest, target_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let target = TARGETS.delete(&target_id, &tenant_id).await?;

    Ok(web::Json(target.mask()))
}
