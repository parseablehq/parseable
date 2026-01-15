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

use crate::parseable::DEFAULT_TENANT;
use crate::rbac::role::ParseableResourceType;
use crate::rbac::user::{User, UserGroup};
use crate::{parseable::PARSEABLE, storage::StorageMetadata};
use std::collections::HashMap;
use std::collections::HashSet;

use super::Response;
use super::{
    role::{Action, Permission, RoleBuilder, model::DefaultPrivilege},
    user,
};
use actix_web::dev::ServiceRequest;
use actix_web::http::header::{HeaderName, HeaderValue};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use once_cell::sync::{Lazy, OnceCell};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub type Roles = HashMap<String, HashMap<String, Vec<DefaultPrivilege>>>;

pub static USERS: OnceCell<parking_lot::RwLock<Users>> = OnceCell::new();
pub static ROLES: OnceCell<RwLock<Roles>> = OnceCell::new();
pub static DEFAULT_ROLE: Lazy<RwLock<HashMap<String, Option<String>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
pub static SESSIONS: OnceCell<parking_lot::RwLock<Sessions>> = OnceCell::new();
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

pub fn users() -> parking_lot::RwLockReadGuard<'static, Users> {
    {
        USERS.get().expect("map is set").read()
        // .expect("not poisoned")
    }
}

pub fn mut_users() -> parking_lot::RwLockWriteGuard<'static, Users> {
    USERS.get().expect("map is set").write()
    // .expect("not poisoned")
}

pub fn roles() -> RwLockReadGuard<'static, Roles> {
    {
        ROLES
            .get()
            .expect("map is set")
            .read()
            .expect("not poisoned")
    }
}

pub fn mut_roles() -> RwLockWriteGuard<'static, Roles> {
    ROLES
        .get()
        .expect("map is set")
        .write()
        .expect("not poisoned")
}

pub fn sessions() -> parking_lot::RwLockReadGuard<'static, Sessions> {
    SESSIONS.get().expect("map is set").read()
    // .expect("not poisoned")
}

pub fn mut_sessions() -> parking_lot::RwLockWriteGuard<'static, Sessions> {
    SESSIONS.get().expect("map is set").write()
    // .expect("not poisoned")
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
        .write()
        .unwrap()
        .insert(DEFAULT_TENANT.to_owned(), metadata.default_role.clone());

    // DEFAULT_ROLE
    //     .lock()
    //     .unwrap()
    //     .clone_from(&metadata.default_role);

    let admin_privilege = DefaultPrivilege::SuperAdmin;
    let admin_permissions = RoleBuilder::from(&admin_privilege).build();
    roles.insert("super-admin".to_string(), vec![admin_privilege]);

    let mut users = Users::from(users);
    let admin = user::get_super_admin_user();
    let admin_username = admin.userid().to_owned();
    users.insert(admin);

    let key = SessionKey::BasicAuth {
        username: PARSEABLE.options.username.clone(),
        password: PARSEABLE.options.password.clone(),
    };
    let mut sessions = Sessions::default();
    // sessions.track_new(
    //     admin_username.clone(),
    //     SessionKey::BasicAuth {
    //         username: PARSEABLE.options.username.clone(),
    //         password: PARSEABLE.options.password.clone(),
    //     },
    //     chrono::DateTime::<Utc>::MAX_UTC,
    //     admin_permissions,
    // );

    PARSEABLE
        .streams
        .write()
        .unwrap()
        .entry(DEFAULT_TENANT.to_owned())
        .or_default();

    sessions
        .user_sessions
        .entry(DEFAULT_TENANT.to_owned())
        .or_default()
        .insert(
            admin_username.clone(),
            vec![(key.clone(), chrono::DateTime::<Utc>::MAX_UTC)],
        );
    sessions.active_sessions.insert(
        key,
        (admin_username, DEFAULT_TENANT.to_owned(), admin_permissions),
    );
    let mut map = HashMap::new();
    map.insert(DEFAULT_TENANT.to_owned(), roles);
    ROLES.set(RwLock::new(map)).expect("map is only set once");

    // let all_users = Users::from(users.clone());
    // let basic_users = all_users.0.iter()
    //     .map(|(k,v)| {
    //         for (uid,user) in v.iter() {
    //             match &user.ty {
    //                 user::UserType::Native(basic) => {
    //                     (basic.username,)
    //                 },
    //                 _ => {},
    //             }
    //         }
    //     })
    // USERS2
    //     .set(parking_lot::RwLock::new(Users2 {
    //         all_users: all_users.0,
    //         basic_users: (),
    //     }))
    //     .unwrap();

    USERS
        .set(parking_lot::RwLock::new(users))
        .expect("map is only set once");

    SESSIONS
        .set(parking_lot::RwLock::new(sessions))
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
    // map session key to user, tenant, and their permission
    active_sessions: HashMap<SessionKey, (String, String, Vec<Permission>)>,
    // map (tenant, user) to one or more session
    // this tracks session based on session id. Not basic auth
    // Ulid time contains expiration datetime
    user_sessions: HashMap<String, HashMap<String, Vec<(SessionKey, DateTime<Utc>)>>>,
}

impl Sessions {
    pub fn get_active_sessions(&self) -> Vec<SessionKey> {
        self.active_sessions.keys().cloned().collect_vec()
    }

    pub fn remove_tenant_sessions(&mut self, tenant_id: &str) {
        self.active_sessions
            .retain(|_, (_, tenantid, _)| !tenant_id.eq(tenantid));
        self.user_sessions.remove(tenant_id);
    }

    // only checks if the session is expired or not
    pub fn is_session_expired(&self, key: &SessionKey) -> bool {
        // fetch userid from session key
        let (userid, tenant_id) = if let Some((user, tenant_id, _)) = self.active_sessions.get(key)
        {
            (user, tenant_id)
        } else {
            return false;
        };

        // check against user sessions if this session is still valid
        let session = if let Some(tenant_sessions) = self.user_sessions.get(tenant_id)
            && let Some(session) = tenant_sessions.get(userid)
        {
            session
        } else {
            return false;
        };

        session
            .par_iter()
            .find_first(|(sessionid, expiry)| sessionid.eq(key) && expiry < &Utc::now())
            .is_some()
    }

    // track new session key
    pub fn track_new(
        &mut self,
        user: String,
        key: SessionKey,
        expiry: DateTime<Utc>,
        permissions: Vec<Permission>,
        tenant_id: &Option<String>,
    ) {
        // let tenant_id = get_tenant_id_from_key(&key);
        let tenant_id = tenant_id.as_ref().map_or(DEFAULT_TENANT, |v| v);
        self.remove_expired_session(&user, &tenant_id);

        let sessions = self.user_sessions.entry(tenant_id.to_owned()).or_default();
        sessions.insert(user.clone(), vec![(key.clone(), expiry)]);
        // sessions.push((key.clone(), expiry));
        self.active_sessions
            .insert(key, (user, tenant_id.to_string(), permissions));
    }

    // remove a specific session
    pub fn remove_session(&mut self, key: &SessionKey) -> Option<String> {
        let (user, tenant_id, _) = self.active_sessions.remove(key)?;

        if let Some(tenant_sessions) = self.user_sessions.get_mut(&tenant_id)
            && let Some(sessions) = tenant_sessions.get_mut(&user)
        {
            sessions.retain(|(session, _)| session != key);
            Some(user)
        } else {
            None
        }
    }

    // remove sessions related to a user
    pub fn remove_user(&mut self, username: &str, tenant_id: &str) {
        // tracing::warn!("removing user- {username}, tenant_id- {tenant_id}");
        // tracing::warn!("active sessions- {:?}", self.active_sessions);
        // tracing::warn!("user sessions- {:?}", self.user_sessions);
        let sessions = if let Some(tenant_sessions) = self.user_sessions.get_mut(tenant_id) {
            // tracing::warn!("found session for tenant- {tenant_id}");
            tenant_sessions.remove(username)
        } else {
            // tracing::warn!("not found session for tenant- {tenant_id}");
            None
        };
        if let Some(sessions) = sessions {
            // tracing::warn!("found active sessions for user {username}-   {sessions:?}");
            sessions.into_iter().for_each(|(key, _)| {
                self.active_sessions.remove(&key);
            })
        }
    }

    fn remove_expired_session(&mut self, user: &str, tenant_id: &str) {
        let now = Utc::now();

        let sessions = if let Some(tenant_sessions) = self.user_sessions.get_mut(tenant_id)
            && let Some(sessions) = tenant_sessions.get_mut(user)
        {
            sessions
        } else {
            return;
        };
        sessions.retain(|(_, expiry)| expiry < &now);
    }

    // get permission related to this session
    pub fn get(&self, key: &SessionKey) -> Option<&Vec<Permission>> {
        self.active_sessions.get(key).map(|(_, _, perms)| perms)
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
        // tracing::warn!(
        //     "required_action- {required_action:?} context_resource- {context_resource:?}, context_user usr- {context_user:?}"
        // );
        self.active_sessions
            .get(key)
            .map(|(username, tenant_id, perms)| {
                let mut perms: HashSet<Permission> = HashSet::from_iter(perms.clone());
                perms.extend(aggregate_group_permissions(username, tenant_id));

                if perms.iter().any(|user_perm| {
                    // tracing::warn!("user-perm- {user_perm:?}");
                    match *user_perm {
                        // if any action is ALL then we we authorize
                        Permission::Unit(action) => {
                            action == required_action || action == Action::All
                        }
                        Permission::Resource(action, ref resource_type) => {
                            if let Some(resource_type) = resource_type.as_ref() {
                                // default flow for all actions other than global-ingestion (ingestion action without any dataset restriction)
                                match resource_type {
                                    ParseableResourceType::Stream(resource_id)
                                    | ParseableResourceType::Llm(resource_id) => {
                                        let ok_resource =
                                            if let Some(context_resource_id) = context_resource {
                                                let is_internal = PARSEABLE
                                                    .get_stream(
                                                        context_resource_id,
                                                        &Some(tenant_id.to_owned()),
                                                    )
                                                    .is_ok_and(|stream| {
                                                        stream.get_stream_type().eq(
                                                            &crate::storage::StreamType::Internal,
                                                        )
                                                    });
                                                resource_id == context_resource_id
                                                    || resource_id == "*"
                                                    || is_internal
                                            } else {
                                                // if no resource to match then resource check is not needed
                                                // WHEN IS THIS VALID??
                                                true
                                            };
                                        (action == required_action || action == Action::All)
                                            && ok_resource
                                    }
                                    ParseableResourceType::All => {
                                        action == required_action || action == Action::All
                                    }
                                }
                            } else if resource_type.is_none() && action.eq(&Action::Ingest) {
                                // tracing::warn!("resource_type is None");
                                // flow for global-ingestion
                                let ok_resource =
                                    if let Some(context_resource_id) = context_resource {
                                        let is_internal = PARSEABLE
                                            .get_stream(
                                                context_resource_id,
                                                &Some(tenant_id.to_owned()),
                                            )
                                            .is_ok_and(|stream| {
                                                stream
                                                    .get_stream_type()
                                                    .eq(&crate::storage::StreamType::Internal)
                                            });
                                        !is_internal
                                    } else {
                                        // if no resource to match then resource check is not needed
                                        // WHEN IS THIS VALID??
                                        true
                                    };
                                // tracing::warn!(ok_resource=?ok_resource);
                                action == required_action && ok_resource
                            } else {
                                // the default flow (some resource_type and an action) was covered in the first if
                                // if the resource type is also None and action is not ingest then return with false
                                false
                            }
                        }
                        Permission::SelfUser if required_action == Action::GetUserRoles => {
                            context_user.map(|x| x == username).unwrap_or_default()
                        }
                        _ => false,
                    }
                }) {
                    // tracing::warn!("Authorized");
                    Response::Authorized
                } else {
                    // tracing::warn!("UnAuthorized");
                    Response::UnAuthorized
                }
            })
    }

    pub fn get_user_and_tenant_id(&self, key: &SessionKey) -> Option<(String, String)> {
        self.active_sessions
            .get(key)
            .map(|(userid, tenant_id, _)| (userid.clone(), tenant_id.clone()))
    }

    pub fn mutate_request_with_tenant(&self, key: &SessionKey, req: &mut ServiceRequest) {
        if let Some((_, tenant, _)) = self.active_sessions.get(key) {
            req.headers_mut().insert(
                HeaderName::from_static("tenant"),
                HeaderValue::from_bytes(tenant.as_bytes()).unwrap(),
            );
        }
    }
}

// UserMap is a map of [(username, tenant_id) --> User]
// This map is populated at startup with the list of users from parseable.json file
#[derive(Debug, Default, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct Users(HashMap<String, HashMap<String, User>>);

impl Users {
    pub fn insert(&mut self, user: User) {
        // tracing::warn!("inserting user- {user:?}");
        let tenant_id = user.tenant.as_ref().map_or(DEFAULT_TENANT, |v| v);
        self.0
            .entry(tenant_id.to_owned())
            .or_default()
            .insert(user.userid().to_owned(), user);
        // self.0.insert(user.userid().to_owned(), user);
    }
}

impl From<Vec<User>> for Users {
    fn from(users: Vec<User>) -> Self {
        let mut map = Self::default();
        for user in users {
            let tenant_id = user.tenant.as_ref().map_or(DEFAULT_TENANT, |v| v);
            map.entry(tenant_id.to_owned())
                .or_default()
                .insert(user.userid().to_owned(), user);
        }
        // map.extend(
        //     users
        //         .into_iter()
        //         .map(|user| (user.userid().to_owned(), user)),
        // );
        map
    }
}

fn aggregate_group_permissions(username: &str, tenant_id: &String) -> HashSet<Permission> {
    let mut group_perms = HashSet::new();

    let user = if let Some(tenant_users) = users().get(tenant_id)
        && let Some(user) = tenant_users.get(username)
    {
        user.to_owned()
    } else {
        return group_perms;
    };
    // let Some(user) = users().get(username).cloned() else {
    //     return group_perms;
    // };

    if user.user_groups.is_empty() {
        return group_perms;
    }

    for group_name in &user.user_groups {
        if let Some(groups) = read_user_groups().get(tenant_id)
            && let Some(group) = groups.get(group_name)
        {
            for role_name in group.roles.iter() {
                let privileges = if let Some(roles) = roles().get(tenant_id)
                    && let Some(privileges) = roles.get(role_name)
                {
                    privileges.clone()
                } else {
                    continue;
                };
                // let Some(privileges) = roles().get(role_name).cloned() else {
                //     continue;
                // };

                for privilege in privileges {
                    group_perms.extend(RoleBuilder::from(&privilege).build());
                }
            }
        } else {
            continue;
        };
        // let Some(group) = read_user_groups().get(group_name).cloned() else {
        //     continue;
        // };
    }

    group_perms
}
// Map of [user group ID --> UserGroup]
// This map is populated at startup with the list of user groups from parseable.json file
#[derive(Debug, Default, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct UserGroups(HashMap<String, HashMap<String, UserGroup>>);

impl UserGroups {
    pub fn insert(&mut self, user_group: UserGroup, tenant_id: &str) {
        self.0
            .entry(tenant_id.to_owned())
            .or_default()
            .insert(user_group.name.clone(), user_group);
        // self.0.insert(user_group.name.clone(), user_group);
    }
}

impl From<Vec<UserGroup>> for UserGroups {
    // only gets called for parseable metadata hence default tenant value
    fn from(user_groups: Vec<UserGroup>) -> Self {
        let mut map = Self::default();
        for group in user_groups.into_iter() {
            map.insert(group, DEFAULT_TENANT);
        }
        // map.extend(
        //     user_groups
        //         .into_iter()
        //         .map(|group| (group.name.to_owned(), group)),
        // );
        map
    }
}
