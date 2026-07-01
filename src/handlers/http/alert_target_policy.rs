use actix_web::{
    Responder,
    web::{self, Json},
};

use crate::alerts::{
    AlertError,
    outbound_http_policy::{
        AlertTargetPolicyConfig, active_policy, replace_policy, validate_policy,
    },
};
use crate::utils::get_tenant_id_from_request;
use actix_web::HttpRequest;

pub async fn get(req: HttpRequest) -> Result<impl Responder, AlertError> {
    let tenant_id = get_tenant_id_from_request(&req);
    let policy = active_policy(&tenant_id).await?;
    Ok(web::Json(policy))
}

pub async fn put(
    req: HttpRequest,
    Json(policy): Json<AlertTargetPolicyConfig>,
) -> Result<impl Responder, AlertError> {
    // validate before replacing so a bad policy never becomes active
    let tenant_id = get_tenant_id_from_request(&req);
    validate_policy(&policy)?;
    replace_policy(&tenant_id, policy.clone()).await?;
    Ok(web::Json(policy))
}

pub async fn validate(
    Json(policy): Json<AlertTargetPolicyConfig>,
) -> Result<impl Responder, AlertError> {
    validate_policy(&policy)?;
    Ok(web::Json(policy))
}
