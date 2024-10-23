use actix_web::{web, HttpResponse, Responder};
use bytes::Bytes;

use crate::{
    handlers::http::{modal::utils::rbac_utils::get_metadata, role::RoleError},
    rbac::{map::mut_roles, role::model::DefaultPrivilege},
    storage,
};

// Handler for PUT /api/v1/role/{name}
// Creates a new role or update existing one
pub async fn put(name: web::Path<String>, body: Bytes) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let privileges = serde_json::from_slice::<Vec<DefaultPrivilege>>(&body)?;
    let mut metadata = get_metadata().await?;
    metadata.roles.insert(name.clone(), privileges.clone());

    let _ = storage::put_staging_metadata(&metadata);
    mut_roles().insert(name.clone(), privileges.clone());

    Ok(HttpResponse::Ok().finish())
}
