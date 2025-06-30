use actix_web::{
    web::{self, Json, Path},
    HttpRequest, Responder,
};
use ulid::Ulid;

use crate::alerts::{
    target::{Target, TARGETS},
    AlertError,
};

// POST /targets
pub async fn post(
    _req: HttpRequest,
    Json(target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    // should check for duplicacy and liveness (??)
    target.validate().await?;

    // add to the map
    TARGETS.update(target.clone()).await?;

    Ok(web::Json(target))
}

// GET /targets
pub async fn list(_req: HttpRequest) -> Result<impl Responder, AlertError> {
    // add to the map
    let list = TARGETS.list().await?;

    Ok(web::Json(list))
}

// GET /targets/{target_id}
pub async fn get(_req: HttpRequest, target_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();

    let target = TARGETS.get_target_by_id(&target_id).await?;

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
    TARGETS.get_target_by_id(&target_id).await?;

    // esnure that the supplied target id is assigned to the target config
    target.id = target_id;

    // should check for duplicacy and liveness (??)
    target.validate().await?;

    // add to the map
    TARGETS.update(target.clone()).await?;

    Ok(web::Json(target))
}

// DELETE /targets/{target_id}
pub async fn delete(
    _req: HttpRequest,
    target_id: Path<Ulid>,
) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();

    let target = TARGETS.delete(&target_id).await?;

    Ok(web::Json(target))
}
