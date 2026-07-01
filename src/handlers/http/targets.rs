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
use itertools::Itertools;
use ulid::Ulid;

use crate::{
    alerts::{
        AlertError,
        target::{TARGETS, Target},
    },
    utils::get_user_and_tenant_from_request,
};

fn tenant_from_request(req: &HttpRequest) -> Result<Option<String>, AlertError> {
    get_user_and_tenant_from_request(req)
        .map(|(_, tenant)| tenant)
        .map_err(|err| AlertError::CustomError(err.to_string()))
}

// POST /targets
pub async fn post(
    req: HttpRequest,
    Json(mut target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    let tenant_id = tenant_from_request(&req)?;
    target.tenant = tenant_id;
    target.validate_outbound_policy().await?;
    // should check for duplicacy and liveness (??)
    // add to the map
    TARGETS.update(target.clone()).await?;

    Ok(web::Json(target.mask()))
}

// GET /targets
pub async fn list(req: HttpRequest) -> Result<impl Responder, AlertError> {
    let tenant_id = tenant_from_request(&req)?;
    // add to the map
    let list = TARGETS
        .list(&tenant_id)
        .await?
        .into_iter()
        .map(|t| t.mask())
        .collect_vec();

    Ok(web::Json(list))
}

// GET /targets/{target_id}
pub async fn get(req: HttpRequest, target_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();
    let tenant_id = tenant_from_request(&req)?;
    let target = TARGETS.get_target_by_id(&target_id, &tenant_id).await?;

    Ok(web::Json(target.mask()))
}

// PUT /targets/{target_id}
pub async fn update(
    req: HttpRequest,
    target_id: Path<Ulid>,
    Json(mut target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();
    let tenant_id = tenant_from_request(&req)?;
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
    let tenant_id = tenant_from_request(&req)?;
    let target = TARGETS.delete(&target_id, &tenant_id).await?;

    Ok(web::Json(target.mask()))
}
