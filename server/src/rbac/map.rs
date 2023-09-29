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

use crate::rbac::user::User;
use crate::{option::CONFIG, storage::StorageMetadata};
use std::{collections::HashMap, sync::Mutex};

use super::{
    role::{model::DefaultPrivilege, Action, Permission, RoleBuilder},
    user,
};
use chrono::{DateTime, Utc};
use once_cell::sync::{Lazy, OnceCell};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub type Roles = HashMap<String, Vec<DefaultPrivilege>>;

pub static USERS: OnceCell<RwLock<Users>> = OnceCell::new();
pub static ROLES: OnceCell<RwLock<Roles>> = OnceCell::new();
pub static DEFAULT_ROLE: Lazy<Mutex<Option<String>>> = Lazy::new(|| Mutex::new(None));
pub static SESSIONS: OnceCell<RwLock<Sessions>> = OnceCell::new();

pub fn users() -> RwLockReadGuard<'static, Users> {
    USERS
        .get()
        .expect("map is set")
        .read()
        .expect("not poisoned")
}

pub fn mut_users() -> RwLockWriteGuard<'static, Users> {
    USERS
        .get()
        .expect("map is set")
        .write()
        .expect("not poisoned")
}

pub fn roles() -> RwLockReadGuard<'static, Roles> {
    ROLES
        .get()
        .expect("map is set")
        .read()
        .expect("not poisoned")
}

pub fn mut_roles() -> RwLockWriteGuard<'static, Roles> {
    ROLES
        .get()
        .expect("map is set")
        .write()
        .expect("not poisoned")
}

pub fn sessions() -> RwLockReadGuard<'static, Sessions> {
    SESSIONS
        .get()
        .expect("map is set")
        .read()
        .expect("not poisoned")
}

pub fn mut_sessions() -> RwLockWriteGuard<'static, Sessions> {
    SESSIONS
        .get()
        .expect("map is set")
        .write()
        .expect("not poisoned")
}

// initialize the user and auth maps
// the user_map is initialized from the config file and has a list of all users
// the auth_map is initialized with admin user only and then gets lazily populated
// as users authenticate
pub fn init(metadata: &StorageMetadata) {
    let users = metadata.users.clone();
    let mut roles = metadata.roles.clone();

    DEFAULT_ROLE
        .lock()
        .unwrap()
        .clone_from(&metadata.default_role);

    let admin_privilege = DefaultPrivilege::Admin;
    let admin_permissions = RoleBuilder::from(&admin_privilege).build();
    roles.insert("admin".to_string(), vec![admin_privilege]);

    let mut users = Users::from(users);
    let admin = user::get_admin_user();
    let admin_username = admin.username().to_owned();
    users.insert(admin);

    let mut sessions = Sessions::default();
    sessions.track_new(
        admin_username,
        SessionKey::BasicAuth {
            username: CONFIG.parseable.username.clone(),
            password: CONFIG.parseable.password.clone(),
        },
        chrono::DateTime::<Utc>::MAX_UTC,
        admin_permissions,
    );

    ROLES.set(RwLock::new(roles)).expect("map is only set once");
    USERS.set(RwLock::new(users)).expect("map is only set once");
    SESSIONS
        .set(RwLock::new(sessions))
        .expect("map is only set once");
}

// A session is loosly active mapping to permissions
// this is lazily initialized and
// cleanup of unused session is done when a new session is added
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum SessionKey {
    BasicAuth { username: String, password: String },
    SessionId(ulid::Ulid),
}

#[derive(Debug, Default)]
pub struct Sessions {
    // map session key to user and their permission
    active_sessions: HashMap<SessionKey, (String, Vec<Permission>)>,
    // map user to one or more session
    // this tracks session based on session id. Not basic auth
    // Ulid time contains expiration datetime
    user_sessions: HashMap<String, Vec<(SessionKey, DateTime<Utc>)>>,
}

impl Sessions {
    // track new session key
    pub fn track_new(
        &mut self,
        user: String,
        key: SessionKey,
        expiry: DateTime<Utc>,
        permissions: Vec<Permission>,
    ) {
        self.remove_expired_session(&user);
        self.user_sessions
            .entry(user.clone())
            .and_modify(|sessions| sessions.push((key.clone(), expiry)))
            .or_default();
        self.active_sessions.insert(key, (user, permissions));
    }

    // remove a specific session
    pub fn remove_session(&mut self, key: &SessionKey) -> Option<String> {
        let Some((user, _)) = self.active_sessions.remove(key) else {
            return None;
        };

        if let Some(items) = self.user_sessions.get_mut(&user) {
            items.retain(|(session, _)| session != key);
            Some(user)
        } else {
            None
        }
    }

    // remove sessions related to a user
    pub fn remove_user(&mut self, username: &str) {
        let sessions = self.user_sessions.remove(username);
        if let Some(sessions) = sessions {
            sessions.into_iter().for_each(|(key, _)| {
                self.active_sessions.remove(&key);
            })
        }
    }

    fn remove_expired_session(&mut self, user: &str) {
        let now = Utc::now();
        let Some(sessions) = self.user_sessions.get_mut(user) else {
            return;
        };
        sessions.retain(|(_, expiry)| expiry < &now);
    }

    // get permission related to this session
    pub fn get(&self, key: &SessionKey) -> Option<&Vec<Permission>> {
        self.active_sessions.get(key).map(|(_, perms)| perms)
    }

    // returns None if user is not in the map
    // Otherwise returns Some(is_authenticated)
    pub fn check_auth(
        &self,
        key: &SessionKey,
        required_action: Action,
        context_stream: Option<&str>,
        context_user: Option<&str>,
    ) -> Option<bool> {
        self.active_sessions.get(key).map(|(username, perms)| {
            perms.iter().any(|user_perm| {
                match *user_perm {
                    // if any action is ALL then we we authorize
                    Permission::Unit(action) => action == required_action || action == Action::All,
                    Permission::Stream(action, ref stream)
                    | Permission::StreamWithTag(action, ref stream, _) => {
                        let ok_stream = if let Some(context_stream) = context_stream {
                            stream == context_stream || stream == "*"
                        } else {
                            // if no stream to match then stream check is not needed
                            true
                        };
                        (action == required_action || action == Action::All) && ok_stream
                    }
                    Permission::SelfRole if required_action == Action::GetUserRoles => {
                        context_user.map(|x| x == username).unwrap_or_default()
                    }
                    _ => false,
                }
            })
        })
    }
}

// UserMap is a map of [username --> User]
// This map is populated at startup with the list of users from parseable.json file
#[derive(Debug, Default, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct Users(HashMap<String, User>);

impl Users {
    pub fn insert(&mut self, user: User) {
        self.0.insert(user.username().to_owned(), user);
    }
}

impl From<Vec<User>> for Users {
    fn from(users: Vec<User>) -> Self {
        let mut map = Self::default();
        map.extend(
            users
                .into_iter()
                .map(|user| (user.username().to_owned(), user)),
        );
        map
    }
}
