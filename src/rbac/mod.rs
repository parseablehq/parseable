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
use parking_lot::RwLockWriteGuard;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Serialize;
use url::Url;

use crate::handlers::TENANT_ID;
use crate::parseable::DEFAULT_TENANT;
use crate::rbac::map::{mut_sessions, mut_users, read_user_groups, roles, sessions, users};
use crate::rbac::role::Action;
use crate::rbac::role::model::RoleUI;
use crate::rbac::user::User;
use crate::utils::get_tenant_id_from_key;

use self::map::SessionKey;
use self::role::{Permission, RoleBuilder};
use self::user::UserType;

pub const EXPIRY_DURATION: Duration = Duration::hours(1);

// A Basic login re-verifies when the user changes mid-login; bound the
// retries so a stream of password changes cannot spin it.
const BASIC_LOGIN_ATTEMPTS: usize = 3;

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
    /// Replaces the live user with `user` if it is still present, without
    /// touching sessions. Callers hold `UPDATE_LOCK`.
    pub fn replace_user(&self, user: User) {
        let tenant_id = user.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(tenant_users) = mut_users().get_mut(tenant_id)
            && let Some(live) = tenant_users.get_mut(user.userid())
        {
            *live = user;
        }
    }

    pub fn put_user(&self, user: User) {
        let tenant_id = user.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
        // Holding SESSIONS across both steps keeps a concurrent Basic login
        // (`new_basic_session`) from tracking a session for the user being replaced.
        let mut sessions = mut_sessions();
        sessions.remove_user(user.userid(), tenant_id);
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

    pub fn is_protected(&self, userid: &str, tenant_id: &Option<String>) -> Option<bool> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        users()
            .get(tenant_id)
            .filter(|users| users.get(userid).is_some())
            .map(|users| users.get(userid).unwrap().protected)
    }

    /// Whether a protected user in the tenant holds any of `role_names`.
    /// Takes the USERS lock, so callers must not hold ROLES or USER_GROUPS.
    pub fn any_role_held_by_protected_user(
        &self,
        role_names: &HashSet<String>,
        tenant_id: &Option<String>,
    ) -> bool {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        users().get(tenant_id).is_some_and(|users| {
            users
                .values()
                .any(|user| user.protected && !user.roles.is_disjoint(role_names))
        })
    }

    pub fn collect_user<T: for<'a> From<&'a User> + 'static>(
        &self,
        tenant_id: &Option<String>,
    ) -> Vec<T> {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        match users().get(tenant_id) {
            Some(users) => users
                .values()
                .filter_map(|user| {
                    // Skip protected users and API-key-backed users — the
                    // latter are managed via the /apikeys endpoints, not
                    // surfaced in the regular users list.
                    if user.protected || user.is_api_key() {
                        None
                    } else {
                        Some(user.into())
                    }
                })
                .collect_vec(),
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
        // SESSIONS before USERS; see the lock order in `map`.
        let mut sessions = mut_sessions();
        if let Some(users) = mut_users().get_mut(tenant_id)
            && let Some(User {
                ty: UserType::Native(user),
                ..
            }) = users.get_mut(userid)
        {
            user.password_hash.clone_from(hash);
            sessions.remove_user(userid, tenant_id);
        };
    }

    pub fn add_roles(&self, userid: &str, roles: HashSet<String>, tenant_id: &Option<String>) {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        // SESSIONS before USERS; see the lock order in `map`.
        let mut sessions = mut_sessions();
        if let Some(users) = mut_users().get_mut(tenant_id)
            && let Some(user) = users.get_mut(userid)
        {
            let mut manual = user.manual_roles();
            manual.extend(roles);
            user.set_manual_roles(manual);
            let new_perms = roles_to_permission(user.roles(), tenant_id);
            sessions.refresh_user_permissions(userid, tenant_id, &new_perms);
        };
    }

    pub fn remove_roles(&self, userid: &str, roles: HashSet<String>, tenant_id: &Option<String>) {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        // SESSIONS before USERS; see the lock order in `map`.
        let mut sessions = mut_sessions();
        if let Some(users) = mut_users().get_mut(tenant_id)
            && let Some(user) = users.get_mut(userid)
        {
            let manual = user.manual_roles();
            user.set_manual_roles(HashSet::from_iter(manual.difference(&roles).cloned()));
            let new_perms = roles_to_permission(user.roles(), tenant_id);
            sessions.refresh_user_permissions(userid, tenant_id, &new_perms);
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
        let mut permissions = match session {
            SessionKey::Ephemeral(ephemeral) => ephemeral.permissions.clone(),
            session => sessions().get(session).cloned().unwrap_or_default(),
        };

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
                        for privelege in privilege_list.privileges() {
                            permissions.extend(RoleBuilder::from(privelege).build());
                        }
                    }
                }
            }
        }
        permissions.into_iter().collect_vec()
    }

    /// A request-local session exists for as long as it has not expired.
    pub fn session_exists(&self, session: &SessionKey) -> bool {
        match session {
            SessionKey::Ephemeral(ephemeral) => !ephemeral.is_expired(),
            session => sessions().get(session).is_some(),
        }
    }

    /// A request-local session was never tracked, so there is nothing to remove.
    pub fn remove_session(&self, session: &SessionKey) -> Option<String> {
        if matches!(session, SessionKey::Ephemeral(_)) {
            return None;
        }
        mut_sessions().remove_session(session)
    }

    pub fn new_session(&self, user: &User, session: SessionKey, expires_in: TimeDelta) {
        let tenant_id = &user.tenant;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let expiry = Utc::now() + expires_in;
        let expiry = user
            .as_api_key()
            .map_or(expiry, |api_key| api_key.cap_session_expiry(expiry));

        mut_sessions().track_new(
            user.userid().to_owned(),
            session,
            expiry,
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
        // A request-local session carries its own grants: no SESSIONS lock.
        if let SessionKey::Ephemeral(ephemeral) = &key {
            return map::check_ephemeral_auth(ephemeral, action, context_stream, context_user);
        }
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
        // add this user to auth map
        let Some(sessions) = self.new_basic_session(
            username,
            password,
            tenant,
            key.clone(),
            DateTime::<Utc>::MAX_UTC,
        ) else {
            return Response::UnAuthorized;
        };
        sessions
            .check_auth(&key, action, context_stream, context_user)
            .expect("entry for this key just added")
    }

    /// Verifies a native user's password and tracks `key` as their session.
    ///
    /// The slow verification runs without holding SESSIONS; the user is then
    /// re-read under it (lock order SESSIONS -> USERS, see `map`), so a password
    /// change, role change or deletion racing the verification cannot leave a
    /// stale session. Returns the SESSIONS guard so the caller can use the new
    /// session before any later write changes it.
    pub fn new_basic_session(
        &self,
        username: &str,
        password: &str,
        tenant: &str,
        key: SessionKey,
        expiry: DateTime<Utc>,
    ) -> Option<RwLockWriteGuard<'static, map::Sessions>> {
        for _ in 0..BASIC_LOGIN_ATTEMPTS {
            let verified_hash = self.verified_password_hash(username, password, tenant)?;
            let mut sessions = mut_sessions();
            let current = users()
                .get(tenant)
                .and_then(|users| users.get(username))
                .filter(|user| {
                    matches!(&user.ty, UserType::Native(basic) if basic.password_hash == verified_hash)
                })
                .map(|user| {
                    (
                        roles_to_permission(user.roles(), tenant),
                        user.tenant.clone(),
                    )
                });
            // The user changed after verification; verify against the new state.
            let Some((permissions, user_tenant)) = current else {
                continue;
            };
            sessions.track_new(username.to_owned(), key, expiry, permissions, &user_tenant);
            return Some(sessions);
        }

        None
    }

    /// Returns the stored hash if `password` matches the native user's. The
    /// slow hash check runs without holding any RBAC lock.
    fn verified_password_hash(
        &self,
        username: &str,
        password: &str,
        tenant: &str,
    ) -> Option<String> {
        let basic = match users().get(tenant).and_then(|users| users.get(username)) {
            Some(User {
                ty: UserType::Native(basic),
                ..
            }) => basic.clone(),
            _ => return None,
        };
        basic
            .verify_password(password)
            .then_some(basic.password_hash)
    }

    /// Copies out the native users named `username`, with the tenant each is
    /// stored under, so callers can run the slow password check after dropping
    /// USERS. Optionally limited to one tenant.
    fn native_users_named(
        &self,
        username: &str,
        tenant_id: Option<&str>,
    ) -> Vec<(String, Option<String>, user::Basic)> {
        users()
            .iter()
            .filter(|(tenant, _)| tenant_id.is_none_or(|tenant_id| tenant_id == tenant.as_str()))
            .flat_map(|(tenant, usermap)| {
                usermap.values().filter_map(move |user| match &user.ty {
                    UserType::Native(basic) if basic.username.eq(username) => {
                        Some((tenant.clone(), user.tenant.clone(), basic.clone()))
                    }
                    _ => None,
                })
            })
            .collect()
    }

    /// For Basic credentials that `authorize` already verified and tracked as a
    /// session: the tenant that session is in and the stored tenant of the
    /// native user it belongs to, provided that user is still stored there.
    ///
    /// Such a session is only created after the password verified, is checked
    /// for expiry here, and is removed whenever the user's password changes or
    /// the user is replaced or deleted, so it is the same trust `authorize`
    /// itself extends to repeat requests. It lets tenant lookups skip the
    /// Argon2 verification on every request after the first.
    fn live_basic_session_user(
        &self,
        username: &str,
        password: &str,
    ) -> Option<(String, Option<String>)> {
        let key = SessionKey::BasicAuth {
            username: username.to_owned(),
            password: password.to_owned(),
        };
        let tenant = {
            let sessions = sessions();
            let (userid, tenant) = sessions.get_user_and_tenant_id(&key)?;
            if userid != username || sessions.is_session_expired(&key) {
                return None;
            }
            tenant
        };
        // SESSIONS is released before USERS is read; see the lock order in `map`.
        let users = users();
        let user = users.get(&tenant)?.get(username)?;
        match &user.ty {
            UserType::Native(basic) if basic.username == username => {
                Some((tenant, user.tenant.clone()))
            }
            _ => None,
        }
    }

    #[inline(always)]
    pub fn validate_basic_user_tenant_id(
        &self,
        username: &str,
        password: &str,
        tenant_id: &str,
    ) -> bool {
        // A live session in another tenant proves nothing here: the same
        // username may exist in several tenants, so verify as before.
        if self
            .live_basic_session_user(username, password)
            .is_some_and(|(session_tenant, _)| session_tenant == tenant_id)
        {
            return true;
        }
        self.native_users_named(username, Some(tenant_id))
            .par_iter()
            .any(|(_, _, basic)| basic.verify_password(password))
    }

    pub fn mutate_request_with_basic_user(
        &self,
        username: &str,
        password: &str,
        req: &mut ServiceRequest,
    ) {
        if let Some((tenant, _, _)) = self.native_users_named(username, None).par_iter().find_any(
            |(_, user_tenant, basic)| user_tenant.is_some() && basic.verify_password(password),
        ) {
            req.headers_mut().insert(
                HeaderName::from_static(TENANT_ID),
                HeaderValue::from_bytes(tenant.as_bytes()).unwrap(),
            );
        };
    }

    pub fn get_userid_from_session(&self, session: &SessionKey) -> Option<(String, String)> {
        match session {
            SessionKey::Ephemeral(ephemeral) => {
                Some((ephemeral.userid.clone(), ephemeral.tenant.clone()))
            }
            session => sessions().get_user_and_tenant_id(session),
        }
    }

    pub fn get_user_tenant_from_basic(&self, username: &str, password: &str) -> Option<String> {
        // Only a session of a user with a stored tenant answers directly; a
        // tenant-less user never matches, so another tenant's user of the
        // same name and password must still be searched for.
        if let Some((_, Some(user_tenant))) = self.live_basic_session_user(username, password) {
            return Some(user_tenant);
        }
        // A user without a tenant never matches, so skip its password check.
        self.native_users_named(username, None)
            .into_iter()
            .find_map(|(_, user_tenant, basic)| {
                user_tenant.filter(|_| basic.verify_password(password))
            })
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
    // email if set
    pub email: Option<String>,
    // picture only if oauth
    pub picture: Option<Url>,
    // roles given directly to the user
    pub roles: HashMap<String, RoleUI>,
    // roles inherited by the user from their usergroups
    pub group_roles: HashMap<String, HashMap<String, RoleUI>>,
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
            for privs in privilege_list.privileges() {
                perms.extend(RoleBuilder::from(privs).build())
            }
        } else {
            continue;
        };
    }
    perms.into_iter().collect()
}

#[cfg(test)]
mod lock_order_tests {
    //! Deadlock regressions for the process-wide RBAC maps. Each test forces an
    //! interleaving at the lock boundaries and bounds every wait, so a lock-order
    //! cycle fails the test instead of hanging the run.

    use std::io::Write;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::thread;
    use std::time::{Duration, Instant};

    use actix_web::cookie::Cookie;
    use argon2::{Algorithm, Argon2, Params, PasswordHasher, Version, password_hash::SaltString};
    use parking_lot::RwLock;
    use tokio::sync::MutexGuard;
    use ulid::Ulid;

    use super::*;
    use crate::handlers::http::modal::query::querier_rbac::remove_user_from_groups;
    use crate::handlers::http::oidc::exchange_basic_for_cookie;
    use crate::rbac::map::{
        ROLES, SESSIONS, Sessions, TEST_SERIAL, USER_GROUPS, USERS, UserGroups, mut_roles,
        write_user_groups,
    };
    use crate::rbac::role::model::{DefaultPrivilege, Role};
    use crate::rbac::user::{Basic, GroupUser, UserGroup};

    const ROLE: &str = "lock-order-admin";
    // Generous for debug builds; a deadlocked thread never finishes.
    const DEADLINE: Duration = Duration::from_secs(20);

    // `map::init` needs `PARSEABLE`, so tests create the maps directly. They hold
    // the shared locks on purpose, so they run one at a time and apart from every
    // other test that writes the maps (`map::TEST_SERIAL`).
    fn auth_state() -> MutexGuard<'static, ()> {
        let serial = TEST_SERIAL.blocking_lock();
        USERS.get_or_init(|| RwLock::new(map::Users::default()));
        SESSIONS.get_or_init(|| RwLock::new(Sessions::default()));
        USER_GROUPS.get_or_init(|| RwLock::new(UserGroups::default()));
        ROLES.get_or_init(|| RwLock::new(HashMap::new()));
        serial
    }

    /// A fresh tenant with an admin role, so tests never see each other's users.
    fn tenant() -> String {
        let tenant = format!("lock-order-{}", Ulid::new());
        mut_roles().entry(tenant.clone()).or_default().insert(
            ROLE.to_owned(),
            Role::create_internal_role(vec![DefaultPrivilege::Admin]),
        );
        tenant
    }

    // Default Argon2 parameters take seconds per hash in debug builds.
    fn cheap_hash(password: &str) -> String {
        let salt = SaltString::encode_b64(&Ulid::new().to_bytes()).unwrap();
        let params = Params::new(Params::MIN_M_COST, 1, 1, None).unwrap();
        Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
            .hash_password(password.as_bytes(), &salt)
            .unwrap()
            .to_string()
    }

    /// A native user (not yet stored) and its Basic-auth key.
    fn basic_user(tenant: &str, roles: &[&str]) -> (User, SessionKey) {
        let username = format!("user-{}", Ulid::new());
        let password = format!("password-{}", Ulid::new());
        let user = User {
            ty: UserType::Native(Basic {
                username: username.clone(),
                password_hash: cheap_hash(&password),
                email: None,
            }),
            roles: roles.iter().map(|role| role.to_string()).collect(),
            user_groups: HashSet::new(),
            tenant: Some(tenant.to_owned()),
            protected: false,
        };
        (user, SessionKey::BasicAuth { username, password })
    }

    fn native_user(tenant: &str, roles: &[&str]) -> (User, SessionKey) {
        let (user, key) = basic_user(tenant, roles);
        mut_users().insert(user.clone());
        (user, key)
    }

    /// Tracks a browser-style session for `user`; its permissions come from roles.
    fn session_for(user: &User) -> SessionKey {
        let key = SessionKey::SessionId(Ulid::new());
        Users.new_session(user, key.clone(), TimeDelta::minutes(5));
        key
    }

    fn spawn<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx
    }

    // A deadlocked thread holds the process-wide maps forever and would hang
    // every later test, so abort the run instead of panicking. The abort may
    // also write a core dump, which can add several seconds to the failure.
    fn deadlock(what: &str) -> ! {
        // Bypass libtest's output capture, which an abort would discard. libtest
        // prints no result after an abort, so name the test; every caller runs
        // on the test's own thread, which libtest names after it.
        let current = thread::current();
        let test = current.name().unwrap_or("?");
        let _ = writeln!(
            std::io::stderr(),
            "{test}: {what} within {DEADLINE:?}: lock-order deadlock, aborting"
        );
        std::process::abort()
    }

    fn finish<T>(rx: &mpsc::Receiver<T>, what: &str) -> T {
        match rx.recv_timeout(DEADLINE) {
            Ok(value) => value,
            Err(mpsc::RecvTimeoutError::Timeout) => deadlock(&format!("{what} did not finish")),
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{what} panicked"),
        }
    }

    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let start = Instant::now();
        while !done() {
            if start.elapsed() >= DEADLINE {
                deadlock(&format!("timed out waiting for {what}"));
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    // parking_lot's RwLock is task-fair: a queued writer sets the exclusive bit
    // and blocks every new reader until it has run.
    fn writer_queued<T>(lock: &RwLock<T>) -> bool {
        lock.is_locked_exclusive()
    }

    fn users_lock() -> &'static RwLock<map::Users> {
        USERS.get().unwrap()
    }

    fn sessions_lock() -> &'static RwLock<Sessions> {
        SESSIONS.get().unwrap()
    }

    /// Spawns `write` and waits until it has finished or is queued for USERS.
    fn queue_user_writer(write: impl FnOnce() + Send + 'static) -> mpsc::Receiver<()> {
        let done = Arc::new(AtomicBool::new(false));
        let rx = spawn({
            let done = done.clone();
            move || {
                write();
                done.store(true, Ordering::SeqCst);
            }
        });
        wait_until("the user writer to queue or finish", || {
            done.load(Ordering::SeqCst) || writer_queued(users_lock())
        });
        rx
    }

    /// Stands in for a session-authorized request inside `Sessions::check_auth`:
    /// it takes SESSIONS(read), then waits for `go` before it reads the user maps.
    fn paused_session_check(
        key: SessionKey,
    ) -> (mpsc::Sender<()>, mpsc::Receiver<Option<Response>>) {
        let (held_tx, held_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let result = spawn(move || {
            let sessions = sessions();
            held_tx.send(()).unwrap();
            go_rx.recv().unwrap();
            sessions.check_auth(&key, Action::Query, None, None)
        });
        finish(&held_rx, "the session check to take SESSIONS");
        (go_tx, result)
    }

    fn first_login(key: &SessionKey) -> mpsc::Receiver<Response> {
        let key = key.clone();
        spawn(move || Users.authorize(key, Action::Query, None, None))
    }

    // Reproduction: a first Basic login kept its USERS read guard while it took
    // SESSIONS(write) and then re-read USERS inside `check_auth`. A user write
    // queued in between waited for the outer guard and blocked the inner read.
    #[test]
    fn first_basic_login_survives_a_queued_user_write() {
        let _serial = auth_state();
        let tenant = tenant();
        let (_, login) = native_user(&tenant, &[ROLE]);
        let (newcomer, _) = basic_user(&tenant, &[ROLE]);

        // Another request reading sessions holds the login back at SESSIONS.
        let reader = sessions();
        let login_rx = first_login(&login);
        wait_until("the login to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        // For example an OIDC login storing its user.
        let writer = queue_user_writer(move || mut_users().insert(newcomer));
        drop(reader);

        assert_eq!(finish(&login_rx, "first Basic login"), Response::Authorized);
        finish(&writer, "user write");
        assert!(Users.session_exists(&login));
    }

    // Reproduction of the reported three-way cycle: login holds USERS and waits
    // for SESSIONS, a session check holds SESSIONS and waits for USERS, and a
    // queued user write blocks that read.
    #[test]
    fn first_login_session_check_and_user_write_do_not_cycle() {
        let _serial = auth_state();
        let tenant = tenant();
        let (_, login) = native_user(&tenant, &[ROLE]);
        let (member, _) = native_user(&tenant, &[ROLE]);
        let member_session = session_for(&member);
        let (newcomer, _) = basic_user(&tenant, &[ROLE]);

        let (go, check) = paused_session_check(member_session);
        let login_rx = first_login(&login);
        wait_until("the login to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        let writer = queue_user_writer(move || mut_users().insert(newcomer));
        go.send(()).unwrap();

        assert_eq!(finish(&check, "session check"), Some(Response::Authorized));
        assert_eq!(finish(&login_rx, "first Basic login"), Response::Authorized);
        finish(&writer, "user write");
    }

    // Reproduction: role and password changes took USERS(write) and then
    // SESSIONS(write), the reverse of `check_auth`.
    fn assert_admin_write_survives_session_check(write: fn(&User)) {
        let _serial = auth_state();
        let tenant = tenant();
        let (target, _) = native_user(&tenant, &[]);
        let (member, _) = native_user(&tenant, &[ROLE]);
        let member_session = session_for(&member);

        let (go, check) = paused_session_check(member_session);
        let writer = spawn(move || write(&target));
        wait_until("the admin write to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        go.send(()).unwrap();

        assert_eq!(finish(&check, "session check"), Some(Response::Authorized));
        finish(&writer, "admin write");
    }

    #[test]
    fn adding_roles_does_not_deadlock_with_a_session_check() {
        assert_admin_write_survives_session_check(|user| {
            Users.add_roles(
                user.userid(),
                HashSet::from([ROLE.to_owned()]),
                &user.tenant,
            )
        });
    }

    #[test]
    fn removing_roles_does_not_deadlock_with_a_session_check() {
        assert_admin_write_survives_session_check(|user| {
            Users.remove_roles(
                user.userid(),
                HashSet::from([ROLE.to_owned()]),
                &user.tenant,
            )
        });
    }

    #[test]
    fn password_change_does_not_deadlock_with_a_session_check() {
        assert_admin_write_survives_session_check(|user| {
            Users.change_password_hash(user.userid(), &cheap_hash("changed"), &user.tenant)
        });
    }

    // Not a reproduction: `put_user` used to release SESSIONS before writing
    // USERS. This guards the lock order now that it holds both.
    #[test]
    fn replacing_a_user_does_not_deadlock_with_a_session_check() {
        assert_admin_write_survives_session_check(|user| Users.put_user(user.clone()));
    }

    // If `put_user` released SESSIONS between removing the user's sessions and
    // storing the replacement, a first login that had verified the old password
    // could track a session in that gap, and nothing would remove it. The gap
    // has no lock acquisition a test could stop a thread at, so this checks
    // that SESSIONS stays held across it, then that a racing login gets nothing.
    #[test]
    fn replacing_a_user_during_first_login_leaves_no_stale_session() {
        let _serial = auth_state();
        let tenant = tenant();
        let (user, login) = native_user(&tenant, &[ROLE]);
        let mut replacement = user;
        if let UserType::Native(basic) = &mut replacement.ty {
            basic.password_hash = cheap_hash("changed");
        }

        // Stops `put_user` after it removed the sessions, before it stores the user.
        let reader = users();
        let writer = spawn(move || Users.put_user(replacement));
        wait_until("the replacement to queue for USERS", || {
            writer_queued(users_lock())
        });
        assert!(
            sessions_lock().is_locked_exclusive(),
            "put_user released SESSIONS before storing the replacement"
        );
        let login_rx = first_login(&login);
        drop(reader);

        finish(&writer, "user replacement");
        assert_eq!(finish(&login_rx, "login"), Response::UnAuthorized);
        assert!(!Users.session_exists(&login));
    }

    // Reproduction: deleting a user on the querier held USER_GROUPS(write) (and
    // USERS) while refreshing sessions, the reverse of `check_auth`.
    #[test]
    fn group_removal_does_not_deadlock_with_a_session_check() {
        let _serial = auth_state();
        let tenant = tenant();
        let group_name = format!("group-{}", Ulid::new());
        let (mut leaving, _) = basic_user(&tenant, &[]);
        let (mut member, _) = basic_user(&tenant, &[ROLE]);
        for user in [&mut leaving, &mut member] {
            user.user_groups.insert(group_name.clone());
            mut_users().insert(user.clone());
        }
        let group = UserGroup::new(
            group_name.clone(),
            HashSet::from([ROLE.to_owned()]),
            HashSet::from([
                GroupUser::from_user(&leaving),
                GroupUser::from_user(&member),
            ]),
        );
        write_user_groups().insert(group, &tenant);
        let member_session = session_for(&member);

        let (go, check) = paused_session_check(member_session);
        let removal = spawn({
            let userid = leaving.userid().to_owned();
            let tenant = leaving.tenant.clone();
            move || remove_user_from_groups(&userid, &tenant)
        });
        wait_until("the group removal to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        go.send(()).unwrap();

        assert_eq!(finish(&check, "session check"), Some(Response::Authorized));
        let updated_groups = finish(&removal, "group removal").unwrap();
        assert_eq!(updated_groups.len(), 1);
        let leaving_member = GroupUser::from_user(&leaving);
        assert!(!updated_groups[0].users.contains(&leaving_member));
        assert!(
            read_user_groups()[&tenant][&group_name]
                .users
                .contains(&leaving_member)
        );
        crate::handlers::http::modal::query::querier_rbac::apply_user_removal_to_groups(
            leaving.userid(),
            &leaving.tenant,
            &updated_groups,
        );
        let groups = read_user_groups();
        let users = &groups[&tenant][&group_name].users;
        assert!(!users.contains(&GroupUser::from_user(&leaving)));
        assert!(users.contains(&GroupUser::from_user(&member)));
    }

    #[test]
    fn basic_login_authorizes_once_then_reuses_the_session() {
        let _serial = auth_state();
        let tenant = tenant();
        let (_, login) = native_user(&tenant, &[ROLE]);

        assert_eq!(finish(&first_login(&login), "login"), Response::Authorized);
        assert!(Users.session_exists(&login));
        assert_eq!(finish(&first_login(&login), "login"), Response::Authorized);

        let SessionKey::BasicAuth { username, .. } = &login else {
            unreachable!()
        };
        let wrong = SessionKey::BasicAuth {
            username: username.clone(),
            password: "wrong".into(),
        };
        assert_eq!(
            finish(&first_login(&wrong), "login"),
            Response::UnAuthorized
        );
        assert!(!Users.session_exists(&wrong));
        let unknown = SessionKey::BasicAuth {
            username: format!("missing-{}", Ulid::new()),
            password: "whatever".into(),
        };
        assert_eq!(
            finish(&first_login(&unknown), "login"),
            Response::UnAuthorized
        );
        assert_eq!(
            Users.authorize(
                SessionKey::SessionId(Ulid::new()),
                Action::Query,
                None,
                None
            ),
            Response::ReloadRequired
        );
    }

    #[test]
    fn basic_login_without_roles_is_tracked_but_unauthorized() {
        let _serial = auth_state();
        let tenant = tenant();
        let (_, login) = native_user(&tenant, &[]);

        assert_eq!(
            finish(&first_login(&login), "login"),
            Response::UnAuthorized
        );
        assert!(Users.session_exists(&login));
    }

    #[test]
    fn session_check_includes_group_permissions() {
        let _serial = auth_state();
        let tenant = tenant();
        let group_name = format!("group-{}", Ulid::new());
        let (mut grouped, _) = basic_user(&tenant, &[]);
        grouped.user_groups.insert(group_name.clone());
        mut_users().insert(grouped.clone());
        let (outsider, _) = native_user(&tenant, &[]);
        write_user_groups().insert(
            UserGroup::new(
                group_name,
                HashSet::from([ROLE.to_owned()]),
                HashSet::from([GroupUser::from_user(&grouped)]),
            ),
            &tenant,
        );

        let grouped_session = session_for(&grouped);
        let outsider_session = session_for(&outsider);
        assert_eq!(
            Users.authorize(grouped_session, Action::Query, None, None),
            Response::Authorized
        );
        assert_eq!(
            Users.authorize(outsider_session, Action::Query, None, None),
            Response::UnAuthorized
        );
    }

    /// A request-local session for `user`, as the API-key and intra-cluster
    /// paths build one: its grants are the user's roles at that moment.
    fn ephemeral_for(user: &User, expires_in: TimeDelta) -> SessionKey {
        let tenant = user.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
        SessionKey::Ephemeral(Arc::new(map::EphemeralSession {
            id: Ulid::new(),
            userid: user.userid().to_owned(),
            tenant: tenant.to_owned(),
            expires_at: Utc::now() + expires_in,
            permissions: roles_to_permission(user.roles(), tenant),
        }))
    }

    // Request-local sessions are answered from the session itself: every
    // lookup finishes while another thread holds SESSIONS for writing, and
    // none of them enters SESSIONS. Decisions match a tracked session's,
    // including grants that only come from a group.
    #[test]
    fn ephemeral_sessions_never_touch_sessions() {
        let _serial = auth_state();
        let tenant = tenant();
        let group_name = format!("group-{}", Ulid::new());
        let (direct, _) = native_user(&tenant, &[ROLE]);
        let (mut grouped, _) = basic_user(&tenant, &[]);
        grouped.user_groups.insert(group_name.clone());
        mut_users().insert(grouped.clone());
        let (outsider, _) = native_user(&tenant, &[]);
        write_user_groups().insert(
            UserGroup::new(
                group_name,
                HashSet::from([ROLE.to_owned()]),
                HashSet::from([GroupUser::from_user(&grouped)]),
            ),
            &tenant,
        );
        let tracked = sessions().get_active_sessions().len();

        let keys = [&direct, &grouped, &outsider].map(|user| {
            (
                user.userid().to_owned(),
                ephemeral_for(user, TimeDelta::minutes(5)),
            )
        });
        let expired = ephemeral_for(&direct, TimeDelta::seconds(-1));
        let tenant_for_check = tenant.clone();

        let writer = mut_sessions();
        let checks = spawn(move || {
            let decisions = keys
                .iter()
                .map(|(userid, key)| {
                    assert_eq!(
                        Users.get_userid_from_session(key),
                        Some((userid.clone(), tenant_for_check.clone()))
                    );
                    assert!(Users.session_exists(key));
                    assert_eq!(Users.remove_session(key), None);
                    let _ = Users.get_permissions(key);
                    Users.authorize(key.clone(), Action::Query, None, None)
                })
                .collect::<Vec<_>>();
            // Expiry is the session's own; like a tracked session's
            // `check_auth`, the decision itself does not look at it.
            assert!(!Users.session_exists(&expired));
            let expired_decision = Users.authorize(expired, Action::Query, None, None);
            (decisions, expired_decision)
        });
        let (decisions, expired_decision) = finish(&checks, "request-local session checks");
        drop(writer);

        assert_eq!(
            decisions,
            vec![
                Response::Authorized,
                Response::Authorized,
                Response::UnAuthorized
            ]
        );
        assert_eq!(expired_decision, Response::Authorized);
        assert_eq!(sessions().get_active_sessions().len(), tracked);

        // The SESSIONS-level helpers agree without needing an entry.
        let key = ephemeral_for(&direct, TimeDelta::minutes(5));
        let expired = ephemeral_for(&direct, TimeDelta::seconds(-1));
        let sessions = sessions();
        assert!(!sessions.is_session_expired(&key));
        assert!(sessions.is_session_expired(&expired));
        assert!(sessions.get(&key).is_some());
        assert_eq!(
            sessions.check_auth(&key, Action::Query, None, None),
            Some(Response::Authorized)
        );
        assert_eq!(
            sessions.get_user_and_tenant_id(&key),
            Some((direct.userid().to_owned(), tenant.clone()))
        );
    }

    // Request-local sessions compare and hash by their id alone, and are
    // never equal to a tracked session key with the same id.
    #[test]
    fn ephemeral_session_keys_compare_by_id() {
        use std::hash::{BuildHasher, RandomState};
        let session = |id| {
            SessionKey::Ephemeral(Arc::new(map::EphemeralSession {
                id,
                userid: format!("user-{}", Ulid::new()),
                tenant: DEFAULT_TENANT.to_owned(),
                expires_at: Utc::now(),
                permissions: Vec::new(),
            }))
        };
        let id = Ulid::new();
        let (first, second) = (session(id), session(id));
        let hasher = RandomState::new();
        assert_eq!(first, second);
        assert_eq!(hasher.hash_one(&first), hasher.hash_one(&second));
        assert_ne!(first, session(Ulid::new()));
        assert_ne!(first, SessionKey::SessionId(id));
    }

    // A password change that lands while a first login is verifying the old
    // password must not leave a session keyed by the old password.
    #[test]
    fn password_change_during_first_login_leaves_no_stale_session() {
        let _serial = auth_state();
        let tenant = tenant();
        let (user, login) = native_user(&tenant, &[ROLE]);

        let reader = sessions();
        let login_rx = first_login(&login);
        wait_until("the login to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        let new_hash = cheap_hash("changed");
        let writer = queue_user_writer(move || {
            if let Some(users) = mut_users().get_mut(user.tenant.as_deref().unwrap())
                && let Some(User {
                    ty: UserType::Native(basic),
                    ..
                }) = users.get_mut(user.userid())
            {
                basic.password_hash = new_hash;
            }
        });
        finish(&writer, "password change");
        drop(reader);

        assert_eq!(finish(&login_rx, "login"), Response::UnAuthorized);
        assert!(!Users.session_exists(&login));
    }

    /// Runs `/login`'s exchange of a Basic key for a session cookie.
    fn cookie_login(key: &SessionKey, tenant: &str) -> mpsc::Receiver<Option<Cookie<'static>>> {
        let (key, tenant) = (key.clone(), Some(tenant.to_owned()));
        spawn(move || {
            let (username, password) = credentials(&key);
            exchange_basic_for_cookie(username, password, &tenant, EXPIRY_DURATION)
        })
    }

    /// The session a `/login` cookie names, checked to belong to `login`'s user
    /// and tenant.
    fn issued_session(
        cookie: Option<Cookie<'static>>,
        login: &SessionKey,
        tenant: &str,
    ) -> SessionKey {
        let cookie = cookie.expect("the cookie login to issue a cookie");
        let session = SessionKey::SessionId(cookie.value().parse().unwrap());
        assert_eq!(
            Users.get_userid_from_session(&session),
            Some((credentials(login).0.to_owned(), tenant.to_owned()))
        );
        session
    }

    /// How many sessions, of any kind, are tracked for `login`'s user.
    fn tracked_sessions(login: &SessionKey, tenant: &str) -> usize {
        let username = credentials(login).0;
        sessions()
            .get_active_sessions()
            .into_iter()
            .filter(|(_, userid, tenant_id)| userid == username && tenant_id == tenant)
            .count()
    }

    // `/login` verified the password and then tracked its cookie session from a
    // copy of the user, so a password change in between left a session that
    // outlived the reset.
    #[test]
    fn password_change_during_cookie_login_leaves_no_session() {
        let _serial = auth_state();
        let tenant = tenant();
        let (user, login) = native_user(&tenant, &[ROLE]);

        let reader = sessions();
        let login_rx = cookie_login(&login, &tenant);
        wait_until("the cookie login to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        let new_hash = cheap_hash("changed");
        let writer = queue_user_writer(move || {
            if let Some(users) = mut_users().get_mut(user.tenant.as_deref().unwrap())
                && let Some(User {
                    ty: UserType::Native(basic),
                    ..
                }) = users.get_mut(user.userid())
            {
                basic.password_hash = new_hash;
            }
        });
        finish(&writer, "password change");
        drop(reader);

        assert!(finish(&login_rx, "cookie login").is_none());
        assert_eq!(tracked_sessions(&login, &tenant), 0);
    }

    #[test]
    fn cookie_login_tracks_a_session_with_the_users_roles_and_tenant() {
        let _serial = auth_state();
        let tenant = tenant();
        let (_, login) = native_user(&tenant, &[ROLE]);
        let (_, roleless) = native_user(&tenant, &[]);

        // `/login` authorizes the Basic key, which tracks it, before exchanging it.
        finish(&first_login(&login), "login");
        assert!(Users.session_exists(&login));
        let cookie = finish(&cookie_login(&login, &tenant), "cookie login");
        let session = issued_session(cookie, &login, &tenant);
        assert_eq!(
            Users.authorize(session, Action::Query, None, None),
            Response::Authorized
        );
        let cookie = finish(&cookie_login(&roleless, &tenant), "cookie login");
        let session = issued_session(cookie, &roleless, &tenant);
        assert_eq!(
            Users.authorize(session, Action::Query, None, None),
            Response::UnAuthorized
        );

        let (username, _) = credentials(&login);
        let wrong = SessionKey::BasicAuth {
            username: username.to_owned(),
            password: "wrong".into(),
        };
        let before = tracked_sessions(&login, &tenant);
        assert!(finish(&cookie_login(&wrong, &tenant), "cookie login").is_none());
        assert_eq!(tracked_sessions(&login, &tenant), before);
    }

    /// Runs the query handler's dataset check for `key`.
    fn query_auth(key: &SessionKey) -> Result<(), actix_web::Error> {
        futures::executor::block_on(crate::utils::user_auth_for_query(
            key,
            "select * from lock_order_stream",
        ))
    }

    // Reproduction: `/login` dropped the Basic session it authorized through, so
    // a query with the same credentials that had passed the middleware then
    // read no permissions and failed with "does not have access".
    #[test]
    fn cookie_login_keeps_the_basic_session_of_an_in_flight_query() {
        let _serial = auth_state();
        let tenant = tenant();
        let (_, login) = native_user(&tenant, &[ROLE]);

        // The query passes the middleware, then `/login` exchanges the same
        // credentials before the query handler reads permissions.
        assert_eq!(finish(&first_login(&login), "query"), Response::Authorized);
        let cookie = finish(&cookie_login(&login, &tenant), "cookie login");
        let session = issued_session(cookie, &login, &tenant);

        assert!(Users.session_exists(&login));
        assert!(!Users.get_permissions(&login).is_empty());
        assert!(query_auth(&login).is_ok());
        assert!(query_auth(&session).is_ok());
        // The cookie session expires on its own; the Basic one never does.
        assert!(!sessions().is_session_expired(&session));
        assert!(!sessions().is_session_expired(&login));
        assert_eq!(tracked_sessions(&login, &tenant), 2);

        let cookie = finish(
            &spawn({
                let (login, tenant) = (login.clone(), Some(tenant.clone()));
                move || {
                    let (username, password) = credentials(&login);
                    exchange_basic_for_cookie(username, password, &tenant, TimeDelta::seconds(-1))
                }
            }),
            "expired cookie login",
        );
        let expired = issued_session(cookie, &login, &tenant);
        // The middleware rejects expired sessions; the Basic one is unaffected.
        assert!(sessions().is_session_expired(&expired));
        assert!(!sessions().is_session_expired(&login));
        assert!(query_auth(&login).is_ok());
    }

    // Keeping the Basic session must not let it outlive a logout, password
    // change, role removal or deletion that also ends the cookie session.
    #[test]
    fn revocations_after_a_cookie_login_reach_the_basic_session() {
        let _serial = auth_state();
        let tenant = tenant();
        let roles = HashSet::from([ROLE.to_owned()]);
        let cookie_and_basic = |login: &SessionKey| {
            assert_eq!(finish(&first_login(login), "query"), Response::Authorized);
            let cookie = finish(&cookie_login(login, &tenant), "cookie login");
            issued_session(cookie, login, &tenant)
        };

        // Logging out one session leaves the other.
        let (_, login) = native_user(&tenant, &[ROLE]);
        let session = cookie_and_basic(&login);
        Users.remove_session(&session);
        assert!(!Users.session_exists(&session));
        assert!(query_auth(&login).is_ok());
        let session = finish(&cookie_login(&login, &tenant), "cookie login");
        let session = issued_session(session, &login, &tenant);
        Users.remove_session(&login);
        assert!(!Users.session_exists(&login));
        assert!(query_auth(&session).is_ok());

        let (user, login) = native_user(&tenant, &[ROLE]);
        let session = cookie_and_basic(&login);
        Users.change_password_hash(user.userid(), &cheap_hash("changed"), &user.tenant);
        assert_eq!(tracked_sessions(&login, &tenant), 0);
        assert!(query_auth(&login).is_err());
        assert!(query_auth(&session).is_err());
        assert_eq!(
            finish(&first_login(&login), "query"),
            Response::UnAuthorized
        );

        let (user, login) = native_user(&tenant, &[ROLE]);
        let session = cookie_and_basic(&login);
        Users.remove_roles(user.userid(), roles.clone(), &user.tenant);
        for key in [&login, &session] {
            assert!(Users.get_permissions(key).is_empty());
            assert!(query_auth(key).is_err());
            assert_eq!(
                Users.authorize(key.clone(), Action::Query, None, None),
                Response::UnAuthorized
            );
        }
        Users.add_roles(user.userid(), roles, &user.tenant);
        assert!(query_auth(&login).is_ok());
        assert!(query_auth(&session).is_ok());

        let (user, login) = native_user(&tenant, &[ROLE]);
        let session = cookie_and_basic(&login);
        Users.delete_user(user.userid(), &user.tenant);
        assert_eq!(tracked_sessions(&login, &tenant), 0);
        assert!(query_auth(&login).is_err());
        assert!(query_auth(&session).is_err());
        assert_eq!(
            finish(&first_login(&login), "query"),
            Response::UnAuthorized
        );
    }

    // The Basic lookups verify passwords after dropping USERS; they must still
    // pick the user's tenant and reject a wrong password or tenant.
    #[test]
    fn basic_tenant_lookups_match_the_users_tenant() {
        let _serial = auth_state();
        let tenant = tenant();
        let (_, login) = native_user(&tenant, &[ROLE]);
        let (username, password) = credentials(&login);

        assert_eq!(
            Users.get_user_tenant_from_basic(username, password),
            Some(tenant.clone())
        );
        assert_eq!(Users.get_user_tenant_from_basic(username, "wrong"), None);
        assert!(Users.validate_basic_user_tenant_id(username, password, &tenant));
        assert!(!Users.validate_basic_user_tenant_id(username, "wrong", &tenant));
        assert!(!Users.validate_basic_user_tenant_id(username, password, "elsewhere"));

        let mut req = actix_web::test::TestRequest::default().to_srv_request();
        Users.mutate_request_with_basic_user(username, password, &mut req);
        assert_eq!(req.headers().get(TENANT_ID).unwrap(), tenant.as_str());
        let mut req = actix_web::test::TestRequest::default().to_srv_request();
        Users.mutate_request_with_basic_user(username, "wrong", &mut req);
        assert!(req.headers().get(TENANT_ID).is_none());
    }

    // After a first Basic login, tenant lookups for the same credentials are
    // answered from its session without re-verifying the password. The
    // stored hash below is of a different password, so any answer that
    // re-ran the verification would be a rejection.
    #[test]
    fn basic_tenant_lookups_use_a_live_session_before_rehashing() {
        let _serial = auth_state();
        let other_tenant = tenant();
        let tenant = tenant();
        let (mut user, login) = basic_user(&tenant, &[ROLE]);
        if let UserType::Native(basic) = &mut user.ty {
            basic.password_hash = cheap_hash("a-different-password");
        }
        mut_users().insert(user.clone());
        let (username, password) = credentials(&login);
        let track = |expiry: DateTime<Utc>, session_tenant: &str| {
            mut_sessions().track_new(
                username.to_owned(),
                login.clone(),
                expiry,
                Vec::new(),
                &Some(session_tenant.to_owned()),
            );
        };

        // No session yet: the password is verified, and does not match.
        assert!(!Users.validate_basic_user_tenant_id(username, password, &tenant));
        assert_eq!(Users.get_user_tenant_from_basic(username, password), None);

        // A live session for these credentials answers both lookups.
        track(DateTime::<Utc>::MAX_UTC, &tenant);
        assert!(Users.validate_basic_user_tenant_id(username, password, &tenant));
        assert_eq!(
            Users.get_user_tenant_from_basic(username, password),
            Some(tenant.clone())
        );
        // Only for its own tenant and its exact credentials.
        assert!(!Users.validate_basic_user_tenant_id(username, password, &other_tenant));
        assert!(!Users.validate_basic_user_tenant_id(username, "wrong", &tenant));
        assert_eq!(Users.get_user_tenant_from_basic(username, "wrong"), None);

        // An expired session is not trusted.
        track(Utc::now() - TimeDelta::seconds(1), &tenant);
        assert!(!Users.validate_basic_user_tenant_id(username, password, &tenant));
        assert_eq!(Users.get_user_tenant_from_basic(username, password), None);

        // Nor a session whose user is no longer stored in its tenant.
        track(DateTime::<Utc>::MAX_UTC, &tenant);
        assert!(Users.validate_basic_user_tenant_id(username, password, &tenant));
        if let Some(users) = mut_users().get_mut(&tenant) {
            users.remove(username);
        }
        assert!(!Users.validate_basic_user_tenant_id(username, password, &tenant));
        assert_eq!(Users.get_user_tenant_from_basic(username, password), None);

        // Revocation through the normal paths removes the session, so the
        // lookups fall back to verifying the stored hash again.
        mut_users().insert(user.clone());
        assert!(Users.validate_basic_user_tenant_id(username, password, &tenant));
        Users.change_password_hash(
            user.userid(),
            &cheap_hash("a-different-password"),
            &user.tenant,
        );
        assert!(!Users.session_exists(&login));
        assert!(!Users.validate_basic_user_tenant_id(username, password, &tenant));
        assert_eq!(Users.get_user_tenant_from_basic(username, password), None);
    }

    // A real first login creates the session the lookups then reuse, and a
    // wrong password never gets one.
    #[test]
    fn first_basic_login_enables_cached_tenant_lookups() {
        let _serial = auth_state();
        let tenant = tenant();
        let (_, login) = native_user(&tenant, &[ROLE]);
        let (username, password) = credentials(&login);
        assert!(!Users.session_exists(&login));
        assert!(Users.validate_basic_user_tenant_id(username, password, &tenant));

        assert_eq!(finish(&first_login(&login), "login"), Response::Authorized);
        assert!(Users.session_exists(&login));
        assert!(Users.validate_basic_user_tenant_id(username, password, &tenant));
        assert_eq!(
            Users.get_user_tenant_from_basic(username, password),
            Some(tenant.clone())
        );
        assert!(!Users.validate_basic_user_tenant_id(username, "wrong", &tenant));
    }

    // A role change that lands while a first login is verifying must apply to
    // the session that login creates.
    #[test]
    fn role_change_during_first_login_applies_to_the_session() {
        let _serial = auth_state();
        let tenant = tenant();
        let (user, login) = native_user(&tenant, &[ROLE]);

        let reader = sessions();
        let login_rx = first_login(&login);
        wait_until("the login to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        let writer = queue_user_writer(move || {
            let mut demoted = user;
            demoted.roles.clear();
            mut_users().insert(demoted);
        });
        finish(&writer, "role change");
        drop(reader);

        assert_eq!(finish(&login_rx, "login"), Response::UnAuthorized);
        assert_eq!(sessions().get(&login), Some(&Vec::new()));
    }

    // Resetting the password to the same value mid-login changes the stored
    // hash; the login verifies again and still succeeds.
    #[test]
    fn same_password_reset_during_first_login_retries_and_succeeds() {
        let _serial = auth_state();
        let tenant = tenant();
        let (user, login) = native_user(&tenant, &[ROLE]);

        let reader = sessions();
        let login_rx = first_login(&login);
        wait_until("the login to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        let new_hash = cheap_hash(credentials(&login).1);
        let writer = queue_user_writer(move || {
            if let Some(users) = mut_users().get_mut(user.tenant.as_deref().unwrap())
                && let Some(User {
                    ty: UserType::Native(basic),
                    ..
                }) = users.get_mut(user.userid())
            {
                basic.password_hash = new_hash;
            }
        });
        finish(&writer, "password reset");
        drop(reader);

        assert_eq!(finish(&login_rx, "login"), Response::Authorized);
        assert!(Users.session_exists(&login));
    }

    // Reproduction: `GET /users` rendered each user while holding USERS(read),
    // and rendering read USERS again for the user's roles and groups.
    #[test]
    fn rendering_users_survives_a_queued_user_write() {
        let _serial = auth_state();
        let tenant = tenant();
        let group_name = format!("group-{}", Ulid::new());
        let (mut user, _) = basic_user(&tenant, &[ROLE]);
        user.user_groups.insert(group_name.clone());
        mut_users().insert(user.clone());
        write_user_groups().insert(
            UserGroup::new(
                group_name.clone(),
                HashSet::from([ROLE.to_owned()]),
                HashSet::from([GroupUser::from_user(&user)]),
            ),
            &tenant,
        );
        let (newcomer, _) = basic_user(&tenant, &[ROLE]);

        let (held_tx, held_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let render = spawn({
            let tenant = tenant.clone();
            let userid = user.userid().to_owned();
            move || {
                // Like `list_users_prism`, which renders inside `users().get(..)`.
                let users = users();
                held_tx.send(()).unwrap();
                go_rx.recv().unwrap();
                utils::prism_roles(&users[&tenant][&userid])
            }
        });
        finish(&held_rx, "the render to take USERS");
        let writer = queue_user_writer(move || mut_users().insert(newcomer));
        go_tx.send(()).unwrap();

        let (direct, groups, group_names) = finish(&render, "user render");
        finish(&writer, "user write");
        assert_eq!(direct.into_keys().collect::<Vec<_>>(), [ROLE]);
        assert_eq!(groups[&group_name].keys().collect::<Vec<_>>(), [ROLE]);
        assert_eq!(group_names, HashSet::from([group_name]));
    }

    #[test]
    fn deletion_during_first_login_leaves_no_session() {
        let _serial = auth_state();
        let tenant = tenant();
        let (user, login) = native_user(&tenant, &[ROLE]);

        let reader = sessions();
        let login_rx = first_login(&login);
        wait_until("the login to queue for SESSIONS", || {
            writer_queued(sessions_lock())
        });
        let deletion = spawn(move || {
            let mut users = Users;
            users.delete_user(user.userid(), &user.tenant)
        });
        wait_until("the user to be removed", || {
            users_lock()
                .try_read()
                .is_some_and(|users| !users[&tenant].contains_key(credentials(&login).0))
        });
        drop(reader);

        assert_eq!(finish(&login_rx, "login"), Response::UnAuthorized);
        finish(&deletion, "deletion");
        assert!(!Users.session_exists(&login));
    }

    fn credentials(key: &SessionKey) -> (&str, &str) {
        match key {
            SessionKey::BasicAuth { username, password } => (username, password),
            SessionKey::SessionId(_) | SessionKey::Ephemeral(_) => unreachable!(),
        }
    }

    // Logins, session checks and user writes racing freely must all finish.
    #[test]
    fn concurrent_logins_session_checks_and_user_writes_finish() {
        const ROUNDS: usize = 40;
        let _serial = auth_state();
        let tenant = tenant();
        // Set everything up first: a deadlocked worker would block the test thread.
        let logins: Vec<_> = (0..4).map(|_| native_user(&tenant, &[ROLE]).1).collect();
        let sessions: Vec<_> = (0..4)
            .map(|_| session_for(&native_user(&tenant, &[ROLE]).0))
            .collect();
        let targets: Vec<_> = (0..2).map(|_| native_user(&tenant, &[]).0).collect();
        let newcomers: Vec<Vec<_>> = (0..2)
            .map(|_| {
                (0..ROUNDS)
                    .map(|_| basic_user(&tenant, &[ROLE]).0)
                    .collect()
            })
            .collect();
        let new_hash = cheap_hash("changed");

        let mut workers = Vec::new();
        for login in logins {
            workers.push(spawn(move || {
                for _ in 0..ROUNDS {
                    // Forget the session so every round is a first login.
                    Users.remove_session(&login);
                    assert_eq!(
                        Users.authorize(login.clone(), Action::Query, None, None),
                        Response::Authorized
                    );
                }
            }));
        }
        for session in sessions {
            workers.push(spawn(move || {
                for _ in 0..ROUNDS {
                    assert_eq!(
                        Users.authorize(session.clone(), Action::Query, None, None),
                        Response::Authorized
                    );
                }
            }));
        }
        for (target, newcomers) in targets.into_iter().zip(newcomers) {
            let new_hash = new_hash.clone();
            workers.push(spawn(move || {
                let roles = HashSet::from([ROLE.to_owned()]);
                for newcomer in newcomers {
                    Users.add_roles(target.userid(), roles.clone(), &target.tenant);
                    Users.remove_roles(target.userid(), roles.clone(), &target.tenant);
                    Users.change_password_hash(target.userid(), &new_hash, &target.tenant);
                    mut_users().insert(newcomer);
                    Users.put_user(target.clone());
                }
            }));
        }
        for (index, worker) in workers.iter().enumerate() {
            finish(worker, &format!("worker {index}"));
        }
    }
}

#[cfg(test)]
mod protected_holder_tests {
    use parking_lot::RwLock;
    use ulid::Ulid;

    use super::*;
    use crate::rbac::map::{TEST_SERIAL, USERS};
    use crate::rbac::user::Basic;

    fn insert_user(tenant: Option<String>, roles: &[&str], protected: bool) {
        mut_users().insert(User {
            ty: UserType::Native(Basic {
                username: format!("user-{}", Ulid::new()),
                password_hash: String::new(),
                email: None,
            }),
            roles: roles.iter().map(|role| role.to_string()).collect(),
            user_groups: HashSet::new(),
            tenant,
            protected,
        });
    }

    fn names(roles: &[&str]) -> HashSet<String> {
        roles.iter().map(|role| role.to_string()).collect()
    }

    /// Unique role names keep these tests apart from others sharing the default tenant.
    fn role() -> String {
        format!("role-{}", Ulid::new())
    }

    #[test]
    fn only_protected_holders_of_a_requested_role_count() {
        let _serial = TEST_SERIAL.blocking_lock();
        USERS.get_or_init(|| RwLock::new(map::Users::default()));
        let tenant = Some(format!("protected-holder-{}", Ulid::new()));
        let (held, unprotected, other) = (role(), role(), role());
        insert_user(tenant.clone(), &[&unprotected], false);
        insert_user(tenant.clone(), &[&held], true);

        assert!(!Users.any_role_held_by_protected_user(&names(&[&unprotected]), &tenant));
        assert!(!Users.any_role_held_by_protected_user(&names(&[&other]), &tenant));
        assert!(Users.any_role_held_by_protected_user(&names(&[&other, &held]), &tenant));
    }

    #[test]
    fn protected_holders_in_other_tenants_do_not_count() {
        let _serial = TEST_SERIAL.blocking_lock();
        USERS.get_or_init(|| RwLock::new(map::Users::default()));
        let held = role();
        insert_user(
            Some(format!("protected-holder-{}", Ulid::new())),
            &[&held],
            true,
        );

        let other_tenant = Some(format!("protected-holder-{}", Ulid::new()));
        assert!(!Users.any_role_held_by_protected_user(&names(&[&held]), &other_tenant));
        assert!(!Users.any_role_held_by_protected_user(&names(&[&held]), &None));
    }

    #[test]
    fn no_tenant_means_the_default_tenant() {
        let _serial = TEST_SERIAL.blocking_lock();
        USERS.get_or_init(|| RwLock::new(map::Users::default()));
        let held = role();
        insert_user(None, &[&held], true);

        assert!(Users.any_role_held_by_protected_user(&names(&[&held]), &None));
        assert!(
            Users.any_role_held_by_protected_user(
                &names(&[&held]),
                &Some(DEFAULT_TENANT.to_owned())
            )
        );
    }
}
