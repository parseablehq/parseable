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

use crate::alerts::{
    AlertError,
    target::{TARGETS, Target},
};

// POST /targets
pub async fn post(
    _req: HttpRequest,
    Json(target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    // should check for duplicacy and liveness (??)
    // add to the map
    TARGETS.update(target.clone()).await?;

    // Ok(web::Json(target.mask()))
    Ok(web::Json(target))
}

// GET /targets
pub async fn list(_req: HttpRequest) -> Result<impl Responder, AlertError> {
    // add to the map
    let list = TARGETS
        .list()
        .await?
        .into_iter()
        // .map(|t| t.mask())
        .collect_vec();

    Ok(web::Json(list))
}

// GET /targets/{target_id}
pub async fn get(_req: HttpRequest, target_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();

    let target = TARGETS.get_target_by_id(&target_id).await?;

    // Ok(web::Json(target.mask()))
    Ok(web::Json(target))
}

// PUT /targets/{target_id}
pub async fn update(
    _req: HttpRequest,
    target_id: Path<Ulid>,
    Json(mut target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();

    // if target_id does not exist, error
    let old_target = TARGETS.get_target_by_id(&target_id).await?;

    // do not allow modifying name
    if old_target.name != target.name {
        return Err(AlertError::InvalidTargetModification(
            "Can't modify target name".to_string(),
        ));
    }

    // esnure that the supplied target id is assigned to the target config
    target.id = target_id;

    // should check for duplicacy and liveness (??)
    // add to the map
    TARGETS.update(target.clone()).await?;

    // Ok(web::Json(target.mask()))
    Ok(web::Json(target))
}

// DELETE /targets/{target_id}
pub async fn delete(
    _req: HttpRequest,
    target_id: Path<Ulid>,
) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();

    let target = TARGETS.delete(&target_id).await?;

    // Ok(web::Json(target.mask()))
    Ok(web::Json(target))
}
