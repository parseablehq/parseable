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

use std::sync::RwLock;

use once_cell::sync::OnceCell;

use self::{
    role::{Action, Permission},
    user::{get_admin_user, verify, User, UserMap, UserPermMap},
};

pub mod role;
pub mod user;

pub static USER_AUTHENTICATION_MAP: OnceCell<RwLock<UserMap>> = OnceCell::new();
pub static USER_AUTHORIZATION_MAP: OnceCell<RwLock<UserPermMap>> = OnceCell::new();

pub struct Users;

impl Users {
    pub fn put_user(&self, user: User) {
        USER_AUTHORIZATION_MAP
            .get()
            .expect("map is set")
            .write()
            .unwrap()
            .insert(&user);

        USER_AUTHENTICATION_MAP
            .get()
            .expect("map is set")
            .write()
            .unwrap()
            .insert(user);
    }

    pub fn list_users(&self) -> Vec<String> {
        USER_AUTHORIZATION_MAP
            .get()
            .expect("map is set")
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    pub fn delete_user(&self, username: &str) {
        USER_AUTHORIZATION_MAP
            .get()
            .expect("map is set")
            .write()
            .unwrap()
            .remove(username);

        USER_AUTHENTICATION_MAP
            .get()
            .expect("map is set")
            .write()
            .unwrap()
            .remove(username);
    }

    pub fn change_password_hash(&self, username: &str, hash: &String) {
        if let Some(entry) = USER_AUTHENTICATION_MAP
            .get()
            .expect("map is set")
            .write()
            .unwrap()
            .get_mut(username)
        {
            entry.clone_from(hash)
        };
    }

    pub fn put_permissions(&self, username: &str, roles: &Vec<Permission>) {
        if let Some(entry) = USER_AUTHORIZATION_MAP
            .get()
            .expect("map is set")
            .write()
            .unwrap()
            .get_mut(username)
        {
            entry.clone_from(roles)
        };
    }

    pub fn contains(&self, username: &str) -> bool {
        USER_AUTHENTICATION_MAP
            .get()
            .expect("map is set")
            .read()
            .unwrap()
            .contains_key(username)
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        if let Some(hash) = USER_AUTHENTICATION_MAP
            .get()
            .expect("map is set")
            .read()
            .unwrap()
            .get(username)
        {
            verify(hash, password)
        } else {
            false
        }
    }

    pub fn check_permission(&self, username: &str, action: Action, stream: Option<&str>) -> bool {
        USER_AUTHORIZATION_MAP
            .get()
            .expect("map is set")
            .read()
            .unwrap()
            .has_perm(username, action, stream)
    }
}

pub fn set_user_map(users: Vec<User>) {
    let mut perm_map = UserPermMap::from(&users);
    let mut user_map = UserMap::from(users);
    let admin = get_admin_user();
    perm_map.insert(&admin);
    user_map.insert(admin);

    USER_AUTHENTICATION_MAP
        .set(RwLock::new(user_map))
        .expect("map is only set once");

    USER_AUTHORIZATION_MAP
        .set(RwLock::new(perm_map))
        .expect("map is only set once");
}
