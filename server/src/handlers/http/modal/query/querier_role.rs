use actix_web::{web, HttpResponse, Responder};
use bytes::Bytes;

use crate::{
    handlers::http::{
        cluster::sync_role_update_with_ingestors,
        modal::utils::rbac_utils::{get_metadata, put_metadata},
        role::RoleError,
    },
    rbac::{map::mut_roles, role::model::DefaultPrivilege},
};

// Handler for PUT /api/v1/role/{name}
// Creates a new role or update existing one
pub async fn put(name: web::Path<String>, body: Bytes) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let privileges = serde_json::from_slice::<Vec<DefaultPrivilege>>(&body)?;
    let mut metadata = get_metadata().await?;
    metadata.roles.insert(name.clone(), privileges.clone());

    put_metadata(&metadata).await?;
    mut_roles().insert(name.clone(), privileges.clone());

    sync_role_update_with_ingestors(name.clone(), privileges.clone()).await?;

    Ok(HttpResponse::Ok().finish())
}
