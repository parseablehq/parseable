/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

pub mod map;
pub mod role;
pub mod user;

use chrono::{DateTime, Utc};

use crate::rbac::map::{mut_sessions, mut_users, sessions, users};
use crate::rbac::role::{model::DefaultPrivilege, Action};
use crate::rbac::user::User;

use self::map::SessionKey;
use self::role::Permission;
use self::user::UserType;

pub enum Response {
    Authorized,
    UnAuthorized,
    ReloadRequired,
}

// This type encapsulates both the user_map and auth_map
// so other entities deal with only this type
pub struct Users;

impl Users {
    pub fn put_user(&self, user: User) {
        mut_sessions().remove_user(user.username());
        mut_users().insert(user);
    }

    pub fn list_users(&self) -> Vec<String> {
        users().keys().cloned().collect()
    }

    pub fn get_role(&self, username: &str) -> Vec<DefaultPrivilege> {
        users()
            .get(username)
            .map(|user| user.role.clone())
            .unwrap_or_default()
    }

    pub fn delete_user(&self, username: &str) {
        mut_users().remove(username);
        mut_sessions().remove_user(username);
    }

    // caller ensures that this operation is valid for the user
    pub fn change_password_hash(&self, username: &str, hash: &String) {
        if let Some(User {
            ty: UserType::Native(user),
            ..
        }) = mut_users().get_mut(username)
        {
            user.password_hash.clone_from(hash);
            mut_sessions().remove_user(username);
        };
    }

    pub fn put_role(&self, username: &str, roles: Vec<DefaultPrivilege>) {
        if let Some(user) = mut_users().get_mut(username) {
            user.role = roles;
            mut_sessions().remove_user(username)
        };
    }

    pub fn contains(&self, username: &str) -> bool {
        users().contains_key(username)
    }

    pub fn get_permissions(&self, session: &SessionKey) -> Vec<Permission> {
        sessions().get(session).cloned().unwrap_or_default()
    }

    pub fn authorize(
        &self,
        key: SessionKey,
        action: Action,
        context_stream: Option<&str>,
        context_user: Option<&str>,
    ) -> Response {
        // try fetch from auth map for faster auth flow
        if let Some(res) = sessions().check_auth(&key, action, context_stream, context_user) {
            return if res {
                Response::Authorized
            } else {
                Response::UnAuthorized
            };
        }

        // attempt reloading permissions into new session for basic auth user
        // id user will be reloaded only through login endpoint
        let SessionKey::BasicAuth { username, password } = &key else { return Response::ReloadRequired ;};
        if let Some(
            user @ User {
                ty: UserType::Native(basic_user),
                ..
            },
        ) = users().get(username)
        {
            // if user exists and password matches
            // add this user to auth map
            if basic_user.verify_password(password) {
                let mut sessions = mut_sessions();
                sessions.track_new(
                    username.clone(),
                    key.clone(),
                    DateTime::<Utc>::MAX_UTC,
                    user.permissions(),
                );
                return if sessions
                    .check_auth(&key, action, context_stream, context_user)
                    .expect("entry for this key just added")
                {
                    Response::Authorized
                } else {
                    Response::UnAuthorized
                };
            }
        }

        Response::UnAuthorized
    }
}
