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

use crate::option::CONFIG;
use crate::rbac::user::User;
use std::collections::HashMap;

use super::{
    role::{Action, Permission},
    user,
};
use once_cell::sync::OnceCell;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub static USER_MAP: OnceCell<RwLock<UserMap>> = OnceCell::new();
pub static AUTH_MAP: OnceCell<RwLock<AuthMap>> = OnceCell::new();

pub fn user_map() -> RwLockReadGuard<'static, UserMap> {
    USER_MAP
        .get()
        .expect("map is set")
        .read()
        .expect("not poisoned")
}

pub fn mut_user_map() -> RwLockWriteGuard<'static, UserMap> {
    USER_MAP
        .get()
        .expect("map is set")
        .write()
        .expect("not poisoned")
}

pub fn auth_map() -> RwLockReadGuard<'static, AuthMap> {
    AUTH_MAP
        .get()
        .expect("map is set")
        .read()
        .expect("not poisoned")
}

pub fn mut_auth_map() -> RwLockWriteGuard<'static, AuthMap> {
    AUTH_MAP
        .get()
        .expect("map is set")
        .write()
        .expect("not poisoned")
}

// initialize the user and auth maps
// the user_map is initialized from the config file and has a list of all users
// the auth_map is initialized with admin user only and then gets lazily populated
// as users authenticate
pub fn init_auth_maps(users: Vec<User>) {
    let mut user_map = UserMap::from(users);
    let mut auth_map = AuthMap::default();
    let admin = user::get_admin_user();
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

// AuthMap is a map of [(username, password) --> permissions]
// This map is populated lazily as users send auth requests.
// First auth request for a user will populate the map with
// the user info (password and permissions) and subsequent
// requests will check the map for the user.
// If user is present in the map then we use this map for both
// authentication and authorization.
#[derive(Debug, Default)]
pub struct AuthMap {
    inner: HashMap<(String, String), Vec<Permission>>,
}

impl AuthMap {
    pub fn add_user(&mut self, username: String, password: String, permissions: Vec<Permission>) {
        self.inner.insert((username, password), permissions);
    }

    pub fn remove(&mut self, username: &str) {
        self.inner.retain(|(x, _), _| x != username)
    }

    // returns None if user is not in the map
    // Otherwise returns Some(is_authenticated)
    pub fn check_auth(
        &self,
        key: &(String, String),
        required_action: Action,
        on_stream: Option<&str>,
    ) -> Option<bool> {
        self.inner.get(key).map(|perms| {
            perms.iter().any(|user_perm| {
                match *user_perm {
                    // if any action is ALL then we we authorize
                    Permission::Unit(action) => action == required_action || action == Action::All,
                    Permission::Stream(action, ref stream) => {
                        let ok_stream = if let Some(on_stream) = on_stream {
                            stream == on_stream || stream == "*"
                        } else {
                            // if no stream to match then stream check is not needed
                            true
                        };
                        (action == required_action || action == Action::All) && ok_stream
                    }
                }
            })
        })
    }
}

// UserMap is a map of [username --> User]
// This map is populated at startup with the list of users from parseable.json file
#[derive(Debug, Default, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct UserMap(HashMap<String, User>);

impl UserMap {
    pub fn insert(&mut self, user: User) {
        self.0.insert(user.username.clone(), user);
    }
}

impl From<Vec<User>> for UserMap {
    fn from(users: Vec<User>) -> Self {
        let mut map = Self::default();
        map.extend(users.into_iter().map(|user| (user.username.clone(), user)));
        map
    }
}
