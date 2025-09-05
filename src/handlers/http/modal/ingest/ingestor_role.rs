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

use std::collections::HashSet;

use actix_web::{
    HttpResponse, Responder,
    web::{self, Json},
};

use crate::{
    handlers::http::{modal::utils::rbac_utils::get_metadata, role::RoleError},
    rbac::{
        map::{mut_roles, mut_sessions, read_user_groups, users},
        role::model::DefaultPrivilege,
    },
    storage,
};

// Handler for PUT /api/v1/role/{name}
// Creates a new role or update existing one
pub async fn put(
    name: web::Path<String>,
    Json(privileges): Json<Vec<DefaultPrivilege>>,
) -> Result<impl Responder, RoleError> {
    let name = name.into_inner();
    let mut metadata = get_metadata().await?;
    metadata.roles.insert(name.clone(), privileges.clone());

    let _ = storage::put_staging_metadata(&metadata);
    mut_roles().insert(name.clone(), privileges);

    // refresh the sessions of all users using this role
    // for this, iterate over all user_groups and users and create a hashset of users
    let mut session_refresh_users: HashSet<String> = HashSet::new();
    for user_group in read_user_groups().values() {
        if user_group.roles.contains(&name) {
            session_refresh_users.extend(user_group.get_usernames());
        }
    }

    // iterate over all users to see if they have this role
    for user in users().values() {
        if user.roles.contains(&name) {
            session_refresh_users.insert(user.username().to_string());
        }
    }

    for username in session_refresh_users {
        mut_sessions().remove_user(&username);
    }

    Ok(HttpResponse::Ok().finish())
}
