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

use crate::rbac::role::ParseableResourceType;
use crate::rbac::user::{User, UserGroup};
use crate::{parseable::PARSEABLE, storage::StorageMetadata};
use std::collections::HashSet;
use std::{collections::HashMap, sync::Mutex};

use super::Response;
use super::{
    role::{Action, Permission, RoleBuilder, model::DefaultPrivilege},
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
pub static USER_GROUPS: OnceCell<RwLock<UserGroups>> = OnceCell::new();

pub fn read_user_groups() -> RwLockReadGuard<'static, UserGroups> {
    USER_GROUPS
        .get()
        .expect("UserGroups map not created")
        .read()
        .expect("UserGroups map is poisoned")
}

pub fn write_user_groups() -> RwLockWriteGuard<'static, UserGroups> {
    USER_GROUPS
        .get()
        .expect("UserGroups map not created")
        .write()
        .expect("UserGroups map is poisoned")
}

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
    let user_groups = metadata.user_groups.clone();
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
            username: PARSEABLE.options.username.clone(),
            password: PARSEABLE.options.password.clone(),
        },
        chrono::DateTime::<Utc>::MAX_UTC,
        admin_permissions,
    );

    ROLES.set(RwLock::new(roles)).expect("map is only set once");
    USERS.set(RwLock::new(users)).expect("map is only set once");
    SESSIONS
        .set(RwLock::new(sessions))
        .expect("map is only set once");
    USER_GROUPS
        .set(RwLock::new(UserGroups::from(user_groups)))
        .expect("Unable to create UserGroups map from storage");
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
        let sessions = self.user_sessions.entry(user.clone()).or_default();
        sessions.push((key.clone(), expiry));
        self.active_sessions.insert(key, (user, permissions));
    }

    // remove a specific session
    pub fn remove_session(&mut self, key: &SessionKey) -> Option<String> {
        let (user, _) = self.active_sessions.remove(key)?;

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
    // Otherwise returns Some(Response) where response is authorized/unauthorized
    pub fn check_auth(
        &self,
        key: &SessionKey,
        required_action: Action,
        context_resource: Option<&str>,
        context_user: Option<&str>,
    ) -> Option<Response> {
        self.active_sessions.get(key).map(|(username, perms)| {
            let mut perms: HashSet<Permission> = HashSet::from_iter(perms.clone());
            perms.extend(aggregate_group_permissions(username));

            if perms.iter().any(|user_perm| {
                match *user_perm {
                    // if any action is ALL then we we authorize
                    Permission::Unit(action) => action == required_action || action == Action::All,
                    Permission::Resource(action, ref resource_type) => {
                        match resource_type {
                            ParseableResourceType::Stream(resource_id)
                            | ParseableResourceType::Llm(resource_id) => {
                                let ok_resource =
                                    if let Some(context_resource_id) = context_resource {
                                        let is_internal = PARSEABLE
                                            .get_stream(context_resource_id)
                                            .is_ok_and(|stream| {
                                                stream
                                                    .get_stream_type()
                                                    .eq(&crate::storage::StreamType::Internal)
                                            });
                                        resource_id == context_resource_id
                                            || resource_id == "*"
                                            || is_internal
                                    } else {
                                        // if no resource to match then resource check is not needed
                                        // WHEN IS THIS VALID??
                                        true
                                    };
                                (action == required_action || action == Action::All) && ok_resource
                            }
                            ParseableResourceType::All => {
                                action == required_action || action == Action::All
                            }
                        }
                    }
                    Permission::SelfUser if required_action == Action::GetUserRoles => {
                        context_user.map(|x| x == username).unwrap_or_default()
                    }
                    _ => false,
                }
            }) {
                Response::Authorized
            } else {
                Response::UnAuthorized
            }
        })
    }

    pub fn get_username(&self, key: &SessionKey) -> Option<&String> {
        self.active_sessions.get(key).map(|(username, _)| username)
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

fn aggregate_group_permissions(username: &str) -> HashSet<Permission> {
    let mut group_perms = HashSet::new();

    let Some(user) = users().get(username).cloned() else {
        return group_perms;
    };

    if user.user_groups.is_empty() {
        return group_perms;
    }

    for group_name in &user.user_groups {
        let Some(group) = read_user_groups().get(group_name).cloned() else {
            continue;
        };

        for role_name in &group.roles {
            let Some(privileges) = roles().get(role_name).cloned() else {
                continue;
            };

            for privilege in privileges {
                group_perms.extend(RoleBuilder::from(&privilege).build());
            }
        }
    }

    group_perms
}
// Map of [user group ID --> UserGroup]
// This map is populated at startup with the list of user groups from parseable.json file
#[derive(Debug, Default, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct UserGroups(HashMap<String, UserGroup>);

impl UserGroups {
    pub fn insert(&mut self, user_group: UserGroup) {
        self.0.insert(user_group.name.clone(), user_group);
    }
}

impl From<Vec<UserGroup>> for UserGroups {
    fn from(user_groups: Vec<UserGroup>) -> Self {
        let mut map = Self::default();
        map.extend(
            user_groups
                .into_iter()
                .map(|group| (group.name.to_owned(), group)),
        );
        map
    }
}
