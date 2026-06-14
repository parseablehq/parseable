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

pub async fn get() -> Result<impl Responder, AlertError> {
    Ok(web::Json(active_policy().await))
}

pub async fn put(
    Json(policy): Json<AlertTargetPolicyConfig>,
) -> Result<impl Responder, AlertError> {
    // validate before replacing so a bad policy never becomes active
    replace_policy(policy.clone()).await?;
    Ok(web::Json(policy))
}

pub async fn validate(
    Json(policy): Json<AlertTargetPolicyConfig>,
) -> Result<impl Responder, AlertError> {
    validate_policy(&policy)?;
    Ok(web::Json(policy))
}
