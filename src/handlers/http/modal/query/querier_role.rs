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

use std::collections::HashSet;

use actix_web::{
    HttpRequest, HttpResponse, Responder,
    web::{self, Json},
};

use crate::{
    handlers::http::{
        cluster::sync_role_update_with_ingestors,
        modal::utils::rbac_utils::{get_metadata, put_metadata},
        role::RoleError,
    },
    parseable::DEFAULT_TENANT,
    rbac::{
        map::{mut_roles, mut_sessions, read_user_groups, users},
        role::model::DefaultPrivilege,
    },
    utils::get_tenant_id_from_request,
    validator,
};

// Handler for PUT /api/v1/role/{name}
// Creates a new role or update existing one
pub async fn put(
    req: HttpRequest,
    name: web::Path<String>,
    Json(privileges): Json<Vec<DefaultPrivilege>>,
) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let tenant = tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v);
    // validate the role name
    validator::user_role_name(&name).map_err(RoleError::ValidationError)?;
    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.roles.insert(name.clone(), privileges.clone());

    put_metadata(&metadata, &tenant_id).await?;
    mut_roles("querier_role_put")
        .entry(tenant.to_owned())
        .or_default()
        .insert(name.clone(), privileges.clone());
    // mut_roles().insert(name.clone(), privileges.clone());

    // refresh the sessions of all users using this role
    // for this, iterate over all user_groups and users and create a hashset of users
    let mut session_refresh_users: HashSet<String> = HashSet::new();
    if let Some(groups) = read_user_groups().get(tenant) {
        for user_group in groups.values() {
            if user_group.roles.contains(&name) {
                session_refresh_users
                    .extend(user_group.users.iter().map(|u| u.userid().to_string()));
            }
        }
    }

    // iterate over all users to see if they have this role
    if let Some(users) = users().get(tenant) {
        for user in users.values() {
            if user.roles.contains(&name) {
                session_refresh_users.insert(user.userid().to_string());
            }
        }
    }

    for userid in session_refresh_users {
        mut_sessions().remove_user(&userid, tenant);
    }

    sync_role_update_with_ingestors(req, name.clone(), privileges.clone(), tenant).await?;

    Ok(HttpResponse::Ok().finish())
}
