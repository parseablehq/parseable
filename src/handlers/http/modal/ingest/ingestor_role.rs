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
