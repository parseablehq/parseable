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

pub mod map;
pub mod role;
pub mod user;
pub mod utils;

use std::collections::{HashMap, HashSet};

use actix_web::dev::ServiceRequest;
use actix_web::http::header::{HeaderName, HeaderValue};
use chrono::{DateTime, Duration, TimeDelta, Utc};
use itertools::Itertools;
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use role::model::DefaultPrivilege;
use serde::Serialize;
use url::Url;

use crate::parseable::DEFAULT_TENANT;
use crate::rbac::map::{mut_sessions, mut_users, read_user_groups, roles, sessions, users};
use crate::rbac::role::Action;
use crate::rbac::user::User;
use crate::utils::get_tenant_id_from_key;

use self::map::SessionKey;
use self::role::{Permission, RoleBuilder};
use self::user::UserType;

pub const EXPIRY_DURATION: Duration = Duration::hours(1);

#[derive(PartialEq, Debug)]
pub enum Response {
    Authorized,
    UnAuthorized,
    ReloadRequired,
    Suspended(String),
}

// This type encapsulates both the user_map and auth_map
// so other entities deal with only this type
pub struct Users;

impl Users {
    pub fn put_user(&self, user: User) {
        let tenant_id = user.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
        mut_sessions().remove_user(user.userid(), tenant_id);
        mut_users().insert(user);
    }

    pub fn get_user_groups(&self, userid: &str, tenant_id: &Option<String>) -> HashSet<String> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        users()
            .get(tenant_id)
            .filter(|users| users.get(userid).is_some())
            .map(|users| users.get(userid).unwrap().user_groups.clone())
            .unwrap_or_default()
    }

    pub fn get_user(&self, userid: &str, tenant_id: &Option<String>) -> Option<User> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);

        users()
            .get(tenant_id)
            .filter(|users| users.get(userid).is_some())
            .map(|users| users.get(userid).unwrap().to_owned())
        // .get(userid).cloned()
    }

    pub fn is_oauth(&self, userid: &str, tenant_id: &Option<String>) -> Option<bool> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        users()
            .get(tenant_id)
            .filter(|users| users.get(userid).is_some())
            .map(|users| users.get(userid).unwrap().is_oauth())
        // users().get(userid).map(|user| user.is_oauth())
    }

    pub fn collect_user<T: for<'a> From<&'a User> + 'static>(
        &self,
        tenant_id: &Option<String>,
    ) -> Vec<T> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        match users().get(tenant_id) {
            Some(users) => users.values().map(|user| user.into()).collect_vec(),
            None => vec![],
        }
    }

    pub fn get_role(&self, userid: &str, tenant_id: &Option<String>) -> Vec<String> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        users()
            .get(tenant_id)
            .filter(|users| users.get(userid).is_some())
            .map(|users| users.get(userid).unwrap().roles.iter().cloned().collect())
            // .get(userid)
            // .map(|user| user.roles.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn delete_user(&mut self, userid: &str, tenant_id: &Option<String>) {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        self.remove_user(userid, tenant_id);
        mut_sessions().remove_user(userid, tenant_id);
    }

    fn remove_user(&mut self, userid: &str, tenant_id: &str) {
        if let Some(users) = mut_users().get_mut(tenant_id) {
            users.remove(userid);
        }
    }

    // caller ensures that this operation is valid for the user
    pub fn change_password_hash(&self, userid: &str, hash: &String, tenant_id: &Option<String>) {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(users) = mut_users().get_mut(tenant_id)
            && let Some(User {
                ty: UserType::Native(user),
                ..
            }) = users.get_mut(userid)
        {
            user.password_hash.clone_from(hash);
            mut_sessions().remove_user(userid, tenant_id);
        };
    }

    pub fn add_roles(&self, userid: &str, roles: HashSet<String>, tenant_id: &Option<String>) {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(users) = mut_users().get_mut(tenant_id)
            && let Some(user) = users.get_mut(userid)
        {
            user.roles.extend(roles);
            mut_sessions().remove_user(userid, tenant_id)
        };
    }

    pub fn remove_roles(&self, userid: &str, roles: HashSet<String>, tenant_id: &Option<String>) {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(users) = mut_users().get_mut(tenant_id)
            && let Some(user) = users.get_mut(userid)
        {
            let diff = HashSet::from_iter(user.roles.difference(&roles).cloned());
            user.roles = diff;
            mut_sessions().remove_user(userid, tenant_id)
        };
    }

    pub fn contains(&self, userid: &str, tenant_id: &Option<String>) -> bool {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        match users().get(tenant_id) {
            Some(users) => users.contains_key(userid),
            None => false,
        }
    }

    pub fn get_permissions(&self, session: &SessionKey) -> Vec<Permission> {
        let mut permissions = sessions().get(session).cloned().unwrap_or_default();

        let Some((userid, tenant_id)) = self.get_userid_from_session(session) else {
            return permissions.into_iter().collect_vec();
        };

        let user_groups = self.get_user_groups(&userid, &Some(tenant_id.clone()));
        for group in user_groups {
            if let Some(groups) = read_user_groups().get(&tenant_id)
                && let Some(group) = groups.get(&group)
            {
                let group_roles = &group.roles;
                for role in group_roles {
                    if let Some(roles) = roles().get(&tenant_id)
                        && let Some(privilege_list) = roles.get(role)
                    {
                        for privelege in privilege_list {
                            permissions.extend(RoleBuilder::from(privelege).build());
                        }
                    }
                }
            }
        }
        permissions.into_iter().collect_vec()
    }

    pub fn session_exists(&self, session: &SessionKey) -> bool {
        sessions().get(session).is_some()
    }

    pub fn remove_session(&self, session: &SessionKey) -> Option<String> {
        mut_sessions().remove_session(session)
    }

    pub fn new_session(&self, user: &User, session: SessionKey, expires_in: TimeDelta) {
        let tenant_id = &user.tenant;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        mut_sessions().track_new(
            user.userid().to_owned(),
            session,
            Utc::now() + expires_in,
            roles_to_permission(user.roles(), tenant),
            tenant_id,
        );
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
            return res;
        }
        // attempt reloading permissions into new session for basic auth user
        // id user will be reloaded only through login endpoint
        let SessionKey::BasicAuth { username, password } = &key else {
            return Response::ReloadRequired;
        };

        let tenant_id = if let Some(tenant) = self.get_user_tenant_from_basic(username, password) {
            Some(tenant)
        } else {
            get_tenant_id_from_key(&key)
        };
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(users) = users().get(tenant)
            && let Some(
                user @ User {
                    ty: UserType::Native(basic_user),
                    ..
                },
            ) = users.get(username)
        {
            // if user exists and password matches
            // add this user to auth map
            if basic_user.verify_password(password) {
                let mut sessions = mut_sessions();
                sessions.track_new(
                    username.clone(),
                    key.clone(),
                    DateTime::<Utc>::MAX_UTC,
                    roles_to_permission(user.roles(), tenant),
                    &user.tenant,
                );
                return sessions
                    .check_auth(&key, action, context_stream, context_user)
                    .expect("entry for this key just added");
            }
        }

        Response::UnAuthorized
    }

    #[inline(always)]
    pub fn validate_basic_user_tenant_id(
        &self,
        username: &str,
        password: &str,
        tenant_id: &str,
    ) -> bool {
        users()
            .get(tenant_id)
            .is_some_and(|tenant_users| {
                tenant_users
                    .values()
                    .par_bridge()
                    .find_any(|user| {
                        matches!(&user.ty, UserType::Native(basic) if basic.username.eq(username) && basic.verify_password(password))
                    })
                    .is_some()
            })
    }

    pub fn mutate_request_with_basic_user(
        &self,
        username: &str,
        password: &str,
        req: &mut ServiceRequest,
    ) {
        if let Some((tenant, _)) = users().par_iter().find_any(|(_, usermap)| {
            usermap
                .par_iter()
                .find_any(|(_, user)| {
                    matches!(&user.ty, UserType::Native(basic) if basic.username.eq(username) && basic.verify_password(password)) && user.tenant.is_some()
                })
                .is_some()
        }) {
            req.headers_mut().insert(
                HeaderName::from_static("tenant"),
                HeaderValue::from_bytes(tenant.as_bytes()).unwrap(),
            );
        };
    }

    pub fn get_userid_from_session(&self, session: &SessionKey) -> Option<(String, String)> {
        sessions().get_user_and_tenant_id(session)
    }

    pub fn get_user_tenant_from_basic(&self, username: &str, password: &str) -> Option<String> {
        for (_, usermap) in users().iter() {
            for (_, user) in usermap.iter() {
                if let UserType::Native(basic) = &user.ty
                    && basic.username.eq(username)
                    && basic.verify_password(password)
                    && let Some(tenant) = &user.tenant
                {
                    return Some(tenant.clone());
                }
            }
        }
        None
    }
}

/// This struct represents a user along with their roles, email, etc
///
/// TODO: rename this after deprecating the older struct
#[derive(Debug, Serialize, Clone)]
#[serde(rename = "camelCase")]
pub struct UsersPrism {
    // sub
    pub id: String,
    // username
    pub username: String,
    // oaith or native
    pub method: String,
    // email only if method is oauth
    pub email: Option<String>,
    // picture only if oauth
    pub picture: Option<Url>,
    // roles given directly to the user
    pub roles: HashMap<String, Vec<DefaultPrivilege>>,
    // roles inherited by the user from their usergroups
    pub group_roles: HashMap<String, HashMap<String, Vec<DefaultPrivilege>>>,
    // user groups
    pub user_groups: HashSet<String>,
}

pub fn roles_to_permission(roles: Vec<String>, tenant_id: &str) -> Vec<Permission> {
    let mut perms = HashSet::new();
    for role in &roles {
        let role_map = &map::roles();
        if let Some(roles) = role_map.get(tenant_id)
            && let Some(privilege_list) = roles.get(role)
        {
            for privs in privilege_list {
                perms.extend(RoleBuilder::from(privs).build())
            }
        } else {
            continue;
        };
    }
    perms.into_iter().collect()
}
