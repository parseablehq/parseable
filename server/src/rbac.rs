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

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use once_cell::sync::OnceCell;

use crate::option::CONFIG;

use self::{
    auth::AuthMap,
    role::{model::DefaultPrivilege, Action},
    user::{get_admin_user, User, UserMap},
};

pub mod auth;
pub mod role;
pub mod user;

pub static USER_MAP: OnceCell<RwLock<UserMap>> = OnceCell::new();
pub static AUTH_MAP: OnceCell<RwLock<AuthMap>> = OnceCell::new();

fn user_map() -> RwLockReadGuard<'static, UserMap> {
    USER_MAP
        .get()
        .expect("map is set")
        .read()
        .expect("not poisoned")
}

fn mut_user_map() -> RwLockWriteGuard<'static, UserMap> {
    USER_MAP
        .get()
        .expect("map is set")
        .write()
        .expect("not poisoned")
}

fn auth_map() -> RwLockReadGuard<'static, AuthMap> {
    AUTH_MAP
        .get()
        .expect("map is set")
        .read()
        .expect("not poisoned")
}

fn mut_auth_map() -> RwLockWriteGuard<'static, AuthMap> {
    AUTH_MAP
        .get()
        .expect("map is set")
        .write()
        .expect("not poisoned")
}

pub struct Users;

impl Users {
    pub fn put_user(&self, user: User) {
        mut_auth_map().remove(&user.username);
        mut_user_map().insert(user);
    }

    pub fn list_users(&self) -> Vec<String> {
        user_map().keys().cloned().collect()
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

        let (username, password) = key;
        // verify pass and add this user's perm to auth map
        if let Some(user) = user_map().get(&username) {
            if user.verify_password(&password) {
                let mut auth_map = mut_auth_map();
                auth_map.add_user(username.clone(), password.clone(), user.permissions());
                // verify auth and return
                let key = (username, password);
                return auth_map
                    .check_auth(&key, action, stream)
                    .expect("entry for this key just added");
            }
        }

        false
    }
}

pub fn set_user_map(users: Vec<User>) {
    let mut user_map = UserMap::from(users);
    let mut auth_map = AuthMap::default();
    let admin = get_admin_user();
    let admin_permissions = admin.permissions();
    user_map.insert(admin);
    auth_map.add_user(
        CONFIG.parseable.username.clone(),
        CONFIG.parseable.password.clone(),
        admin_permissions,
    );

    USER_MAP
        .set(RwLock::new(user_map))
        .expect("map is only set once");
    AUTH_MAP
        .set(RwLock::new(auth_map))
        .expect("map is only set once");
}
