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

use crate::rbac::map::{auth_map, mut_auth_map, mut_user_map, user_map};
use crate::rbac::role::{model::DefaultPrivilege, Action};
use crate::rbac::user::User;

// This type encapsulates both the user_map and auth_map
// so other entities deal with only this type
pub struct Users;

impl Users {
    pub fn put_user(&self, user: User) {
        mut_auth_map().remove(&user.username);
        mut_user_map().insert(user);
    }

    pub fn list_users(&self) -> Vec<String> {
        user_map().keys().cloned().collect()
    }

    pub fn get_role(&self, username: &str) -> Vec<DefaultPrivilege> {
        user_map()
            .get(username)
            .map(|user| user.role.clone())
            .unwrap_or_default()
    }

    pub fn delete_user(&self, username: &str) {
        mut_user_map().remove(username);
        mut_auth_map().remove(username);
    }

    pub fn change_password_hash(&self, username: &str, hash: &String) {
        if let Some(user) = mut_user_map().get_mut(username) {
            user.password_hash.clone_from(hash)
        };
        mut_auth_map().remove(username);
    }

    pub fn put_role(&self, username: &str, roles: Vec<DefaultPrivilege>) {
        if let Some(user) = mut_user_map().get_mut(username) {
            user.role = roles;
            mut_auth_map().remove(username)
        };
    }

    pub fn contains(&self, username: &str) -> bool {
        user_map().contains_key(username)
    }

    pub fn authenticate(
        &self,
        username: String,
        password: String,
        action: Action,
        stream: Option<&str>,
    ) -> bool {
        let key = (username, password);
        // try fetch from auth map for faster auth flow
        if let Some(res) = auth_map().check_auth(&key, action, stream) {
            return res;
        }

        // if not found in auth map, look into user map
        let (username, password) = key;
        if let Some(user) = user_map().get(&username) {
            // if user exists and password matches
            // add this user to auth map
            if user.verify_password(&password) {
                let mut auth_map = mut_auth_map();
                auth_map.add_user(username.clone(), password.clone(), user.permissions());
                // verify from auth map and return
                let key = (username, password);
                return auth_map
                    .check_auth(&key, action, stream)
                    .expect("entry for this key just added");
            }
        }

        false
    }
}
