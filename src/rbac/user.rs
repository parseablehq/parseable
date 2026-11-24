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

use std::{collections::HashSet, sync::LazyLock};

use argon2::{
    Argon2, PasswordHash, PasswordVerifier,
    password_hash::{PasswordHasher, SaltString, rand_core::OsRng},
};

use chrono::{DateTime, Utc};
use openid::Bearer;
use rand::{
    RngCore,
    distributions::{Alphanumeric, DistString},
};
use regex::Regex;
use serde::Deserialize;
use ulid::Ulid;

use crate::{
    handlers::http::rbac::{InvalidUserGroupError, RBACError},
    parseable::{DEFAULT_TENANT, PARSEABLE},
    rbac::{
        Users,
        map::{mut_sessions, read_user_groups, roles, users},
        role::model::RoleType,
        roles_to_permission,
    },
};

static EMAIL_VALIDATOR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?i)[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
        .expect("email validation regex is invalid")
});

#[derive(Debug, Deserialize)]
pub struct UpdateUser {
    pub email: Option<String>,
}

impl UpdateUser {
    /// Prepare a changed user value without publishing it to the live map.
    /// The caller must persist this value before making it visible.
    pub fn update(
        &self,
        userid: &str,
        tenant_id: &Option<String>,
    ) -> Result<Option<User>, RBACError> {
        let mut user = None;
        if self.email.is_some() {
            if self.is_valid_email() {
                let mut updated_user = Users
                    .get_user(userid, tenant_id)
                    .ok_or(RBACError::UserDoesNotExist)?;
                self.apply(&mut updated_user)?;
                user = Some(updated_user);
            } else {
                return Err(RBACError::Anyhow(anyhow::Error::msg("Invalid Email")));
            }
        }

        Ok(user)
    }
    /// Apply only the patched fields to `user`, leaving everything else as is.
    pub fn apply(&self, user: &mut User) -> Result<(), RBACError> {
        if let Some(email) = &self.email {
            user.insert_email(email.clone())?;
        }
        Ok(())
    }

    fn is_valid_email(&self) -> bool {
        if let Some(email) = &self.email {
            EMAIL_VALIDATOR_RE.is_match(email)
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum UserType {
    // Order matters for `#[serde(untagged)]`: the most specific variant must
    // come first so it wins deserialization when its distinctive fields are
    // present. `ApiKey` carries `key_id` and `key_hash` (`api_key` in records
    // written before keys were hashed) which neither `Native` nor `OAuth` have.
    ApiKey(Box<ApiKeyUser>),
    OAuth(Box<OAuth>),
    Native(Basic),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct User {
    #[serde(flatten)]
    pub ty: UserType,
    pub roles: HashSet<String>,
    pub user_groups: HashSet<String>,
    pub tenant: Option<String>,
    #[serde(default)]
    pub protected: bool,
}

impl User {
    // create a new User and return self with password generated for said user.
    pub fn new_basic(username: String, tenant: Option<String>, protected: bool) -> (Self, String) {
        let PassCode { password, hash } = Basic::gen_new_password();
        (
            Self {
                ty: UserType::Native(Basic {
                    username,
                    password_hash: hash,
                    email: None,
                }),
                roles: HashSet::new(),
                user_groups: HashSet::new(),
                tenant,
                protected,
            },
            password,
        )
    }

    /// A user for the API key `api_key`. Only the hash of the key and its
    /// last characters are kept, so the caller must hand the key to its owner
    /// now: it cannot be read back.
    pub fn new_api_key(
        key_id: Ulid,
        api_key: String,
        key_name: String,
        roles: HashSet<String>,
        created_by: String,
        tenant: Option<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            ty: UserType::ApiKey(Box::new(ApiKeyUser {
                userid: key_id.to_string(),
                key_id,
                key_hash: hash_api_key(&api_key),
                key_hint: api_key_hint(&api_key),
                key_name,
                created_by,
                created_at: now,
                modified_at: now,
                expires_at: None,
            })),
            roles,
            user_groups: HashSet::new(),
            tenant,
            protected: false,
        }
    }

    pub fn new_oauth(
        userid: String,
        roles: HashSet<String>,
        user_info: UserInfo,
        bearer: Option<Bearer>,
        tenant: Option<String>,
        protected: bool,
    ) -> Self {
        Self {
            ty: UserType::OAuth(Box::new(OAuth {
                userid,
                issuer: None,
                role_grants: None,
                user_info,
                bearer,
            })),
            roles,
            user_groups: HashSet::new(),
            tenant,
            protected,
        }
    }

    pub fn insert_email(&mut self, email: String) -> Result<(), RBACError> {
        match self.ty {
            UserType::Native(ref mut user) => user.email = Some(email),
            UserType::OAuth(ref mut oauth) => oauth.user_info.email = Some(email),
            _ => {
                return Err(RBACError::Anyhow(anyhow::Error::msg(
                    "Can't add email to API key",
                )));
            }
        }
        Ok(())
    }

    pub fn userid(&self) -> &str {
        match self.ty {
            UserType::Native(Basic { ref username, .. }) => username,
            UserType::OAuth(ref oauth) => &oauth.userid,
            UserType::ApiKey(ref api_key) => &api_key.userid,
        }
    }

    pub fn username_by_userid(&self) -> String {
        match &self.ty {
            UserType::Native(basic) => basic.username.clone(),
            UserType::OAuth(oauth) => {
                let user_info = oauth.user_info.clone();
                user_info.name.clone().unwrap_or_else(|| {
                    user_info
                        .email
                        .clone()
                        .unwrap_or_else(|| oauth.userid.clone())
                })
            }
            UserType::ApiKey(api_key) => api_key.key_name.clone(),
        }
    }

    pub fn is_oauth(&self) -> bool {
        matches!(self.ty, UserType::OAuth(_))
    }

    pub fn is_api_key(&self) -> bool {
        matches!(self.ty, UserType::ApiKey(_))
    }

    /// Return the underlying API key data if this user represents an API key.
    pub fn as_api_key(&self) -> Option<&ApiKeyUser> {
        match &self.ty {
            UserType::ApiKey(api_key) => Some(api_key),
            _ => None,
        }
    }

    pub fn is_super_admin(&self) -> bool {
        self.roles.contains("super-admin")
    }

    /// Explicit administrative grants, excluding provider/default grants.
    /// Legacy OAuth users have no provenance, so administrative edits treat
    /// their flat roles as manual; this keeps adds non-destructive and lets
    /// any assigned role be revoked.
    pub fn manual_roles(&self) -> HashSet<String> {
        match &self.ty {
            UserType::OAuth(oauth) => oauth
                .role_grants
                .as_ref()
                .map_or_else(|| self.roles.clone(), |grants| grants.manual_roles.clone()),
            _ => self.roles.clone(),
        }
    }

    pub fn set_manual_roles(&mut self, roles: HashSet<String>) {
        match &mut self.ty {
            UserType::OAuth(oauth) => {
                let grants = oauth
                    .role_grants
                    .get_or_insert_with(OAuthRoleGrants::default);
                grants.manual_roles = roles;
                self.roles = grants.effective_roles();
            }
            _ => self.roles = roles,
        }
    }

    pub fn roles(&self) -> Vec<String> {
        self.roles.iter().cloned().collect()
    }
}

// Represents a User in the system
// can be the root admin user (set with env vars at startup / restart)
// or user(s) created by the root user
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Basic {
    pub username: String,
    pub password_hash: String,
    pub email: Option<String>,
}

impl Basic {
    // generate a new password
    pub fn gen_new_password() -> PassCode {
        let password = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
        let hash = gen_hash(&password);
        PassCode { password, hash }
    }

    pub fn verify_password(&self, password: &str) -> bool {
        verify(&self.password_hash, password)
    }
}

// Take the password and compare with the hash stored internally (PHC format ==>
// $<id>[$v=<version>][$<param>=<value>(,<param>=<value>)*][$<salt>[$<hash>]])
// ref https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md#specification
pub fn verify(password_hash: &str, password: &str) -> bool {
    let parsed_hash = PasswordHash::new(password_hash).unwrap();
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok()
}

// generate a one way hash for password to be stored in metadata file
// ref https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md
pub fn gen_hash(password: &str) -> String {
    let mut bytes = [0u8; 32];
    let r = &mut OsRng;
    r.fill_bytes(&mut bytes);
    let salt = SaltString::encode_b64(&bytes).unwrap();
    // let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2
        .hash_password(password.as_bytes(), &salt)
        .expect("can hash random alphanumeric")
        .to_string()
}

pub struct PassCode {
    pub password: String,
    pub hash: String,
}

pub fn get_super_admin_user() -> User {
    let username = PARSEABLE.options.username.clone();
    let password = PARSEABLE.options.password.clone();
    let hashcode = gen_hash(&password);

    User {
        ty: UserType::Native(Basic {
            username,
            password_hash: hashcode,
            email: None,
        }),
        roles: ["super-admin".to_string()].into(),
        user_groups: HashSet::new(),
        tenant: None,
        protected: false,
    }
}

/// A user backed by an API key. `userid` is the stable identifier used
/// everywhere a userid is expected (sessions, groups, audit logs); it equals
/// `key_id.to_string()` (same value as `keyId`, kept as `String` so
/// `User::userid()` can return `&str` alongside the Native/OAuth variants).
/// Permissions for this user are derived from the `roles` set on the parent
/// `User`, exactly like native and OAuth users.
///
/// The key itself is not kept. It is shown once, when it is created; what is
/// stored, held in memory and sent to the other nodes is `key_hash`, so reading
/// the metadata does not give anyone a credential. Records written before keys
/// were hashed hold the key as `apiKey`: they are read (`StoredApiKeyUser`), and
/// written back hashed the next time the metadata is written.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", try_from = "StoredApiKeyUser")]
pub struct ApiKeyUser {
    pub userid: String,
    pub key_id: Ulid,
    /// SHA-256 of the key, in lowercase hex, see `hash_api_key`
    pub key_hash: String,
    /// The last characters of the key, for listings (`****abcd`), see
    /// `api_key_hint`
    pub key_hint: String,
    pub key_name: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

impl ApiKeyUser {
    /// The expiry instant itself is already expired.
    pub fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }

    pub fn matches_active_key(&self, value: &str, now: DateTime<Utc>) -> bool {
        self.matches_active_hash(&hash_api_key(value), now)
    }

    /// As `matches_active_key`, for a key already hashed with `hash_api_key`:
    /// callers that compare one key with many users hash it once.
    pub fn matches_active_hash(&self, hash: &str, now: DateTime<Utc>) -> bool {
        hashes_equal(&self.key_hash, hash) && !self.is_expired_at(now)
    }

    /// The key as listings show it: its last characters after a mask
    pub fn masked_key(&self) -> String {
        format!("****{}", self.key_hint)
    }

    /// Ephemeral sessions never outlive the credential that created them.
    pub fn session_expires_at(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        self.cap_session_expiry(now + chrono::TimeDelta::minutes(5))
    }

    pub fn cap_session_expiry(&self, session_expiry: DateTime<Utc>) -> DateTime<Utc> {
        self.expires_at
            .map_or(session_expiry, |expiry| expiry.min(session_expiry))
    }
}

/// What is stored of an API key: its SHA-256 in lowercase hex.
///
/// Keys are generated by the server from 122 random bits, so a plain fast hash
/// is enough: there is no small set of likely keys to try, which is what the
/// salt and the cost of a password hash defend against, and the hash is computed
/// for every request the key authenticates.
pub fn hash_api_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(key.as_bytes()))
}

/// The last four characters of a key, kept so that listings can tell keys apart.
/// Empty for a key under eight characters, of which it would give away half or
/// more (generated keys have 36).
pub fn api_key_hint(key: &str) -> String {
    let chars = key.chars().count();
    if chars < 8 {
        return String::new();
    }
    key.chars().skip(chars - 4).collect()
}

/// Whether two hashes are the same, in time that does not depend on where they
/// differ
fn hashes_equal(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |differing, (a, b)| differing | (a ^ b))
            == 0
}

/// Whether `metadata`, the stored JSON of a tenant's metadata, holds an API key
/// written before keys were hashed. Reading it hashes the key, so writing the
/// metadata back removes the key from storage.
pub fn stores_unhashed_api_key(metadata: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(metadata).is_ok_and(|metadata| {
        metadata["users"].as_array().is_some_and(|users| {
            users
                .iter()
                .any(|user| user.get("keyId").is_some() && user.get("apiKey").is_some())
        })
    })
}

/// An API key user as stored: with `keyHash` and `keyHint`, or, in records
/// written before keys were hashed, with the key itself as `apiKey`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredApiKeyUser {
    userid: String,
    key_id: Ulid,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    key_hash: Option<String>,
    #[serde(default)]
    key_hint: Option<String>,
    key_name: String,
    created_by: String,
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
    #[serde(default)]
    expires_at: Option<DateTime<Utc>>,
}

impl TryFrom<StoredApiKeyUser> for ApiKeyUser {
    type Error = &'static str;

    fn try_from(stored: StoredApiKeyUser) -> Result<Self, Self::Error> {
        // A record that has a hash is never rehashed from `apiKey`: the hash is
        // what authenticated since it was written.
        let (key_hash, key_hint) = match (stored.key_hash, stored.api_key) {
            (Some(hash), _) => (hash, stored.key_hint.unwrap_or_default()),
            (None, Some(key)) => (hash_api_key(&key), api_key_hint(&key)),
            (None, None) => return Err("an API key user needs keyHash"),
        };
        Ok(Self {
            userid: stored.userid,
            key_id: stored.key_id,
            key_hash,
            key_hint,
            key_name: stored.key_name,
            created_by: stored.created_by,
            created_at: stored.created_at,
            modified_at: stored.modified_at,
            expires_at: stored.expires_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OAuth {
    pub userid: String,
    pub user_info: UserInfo,
    pub bearer: Option<Bearer>,
    /// Verified OIDC issuer. Legacy records have no binding until reauthenticated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    /// Provenance for the effective roles stored on `User`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_grants: Option<OAuthRoleGrants>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OAuthRoleGrants {
    #[serde(default)]
    pub observed_groups: HashSet<String>,
    #[serde(default)]
    pub manual_roles: HashSet<String>,
    #[serde(default)]
    pub provider_roles: HashSet<String>,
    #[serde(default)]
    pub default_role: Option<String>,
}

impl OAuthRoleGrants {
    pub fn effective_roles(&self) -> HashSet<String> {
        let mut roles = self.manual_roles.clone();
        roles.extend(self.provider_roles.iter().cloned());
        if roles.is_empty() {
            roles.extend(self.default_role.iter().cloned());
        }
        roles
    }
}

impl AsRef<Bearer> for Box<OAuth> {
    /// Returns a reference to the bearer token.
    ///
    /// # Panics
    /// Panics if bearer is None. This should never happen in practice as
    /// bearer is always set to Some when OIDC is configured and this trait
    /// is only called by refresh_token after verifying bearer.is_some().
    fn as_ref(&self) -> &Bearer {
        self.bearer.as_ref().unwrap()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserInfo {
    #[serde(default)]
    /// Subject - Identifier for the End-User at the Issuer.
    pub sub: Option<String>,
    #[serde(default)]
    /// User's full name for display purposes.
    pub name: Option<String>,
    #[serde(default)]
    pub preferred_username: Option<String>,
    #[serde(default)]
    pub picture: Option<url::Url>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub gender: Option<String>,
    #[serde(default)]
    pub updated_at: Option<i64>,
}

impl From<openid::Userinfo> for UserInfo {
    fn from(user: openid::Userinfo) -> Self {
        UserInfo {
            sub: user.sub,
            name: user.name,
            preferred_username: user.preferred_username,
            picture: user.picture,
            email: user.email,
            gender: user.gender,
            updated_at: user.updated_at,
        }
    }
}

impl From<crate::oauth::ProviderUserInfo> for UserInfo {
    fn from(info: crate::oauth::ProviderUserInfo) -> Self {
        UserInfo {
            sub: info.sub,
            name: info.name,
            preferred_username: info.preferred_username,
            picture: info.picture.as_deref().and_then(|s| s.parse().ok()),
            email: info.email,
            gender: None,
            updated_at: None,
        }
    }
}

/// Represents a user in a UserGroup - simplified structure for both user types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupUser {
    pub userid: String,
    pub username: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
}

impl PartialEq for GroupUser {
    fn eq(&self, other: &Self) -> bool {
        self.userid == other.userid
    }
}
impl Eq for GroupUser {}
impl std::hash::Hash for GroupUser {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.userid.hash(state)
    }
}

impl GroupUser {
    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn userid(&self) -> &str {
        &self.userid
    }

    pub fn is_oauth(&self) -> bool {
        self.method == "oauth"
    }

    pub fn user_type(&self) -> &str {
        if self.is_oauth() { "oauth" } else { "native" }
    }

    pub fn from_user(user: &User) -> Self {
        match &user.ty {
            UserType::Native(Basic { username, .. }) => GroupUser {
                userid: username.clone(), // Same value for basic users
                username: username.clone(),
                method: "native".to_string(),
                tenant_id: user.tenant.clone(),
            },
            UserType::OAuth(oauth) => {
                // For OAuth users, derive the display username from user_info
                let user_info = &oauth.user_info;
                let userid = &oauth.userid;
                let display_username = user_info
                    .name
                    .clone()
                    .or_else(|| user_info.email.clone())
                    .unwrap_or_else(|| userid.clone()); // fallback to userid if nothing else available

                GroupUser {
                    userid: userid.clone(),
                    username: display_username,
                    method: "oauth".to_string(),
                    tenant_id: user.tenant.clone(),
                }
            }
            UserType::ApiKey(api_key) => GroupUser {
                userid: api_key.userid.clone(),
                username: api_key.key_name.clone(),
                method: "apikey".to_string(),
                tenant_id: user.tenant.clone(),
            },
        }
    }
}

/// Logically speaking, UserGroup is a collection of roles and is applied to a collection of users.
///
/// The users present in a group inherit all the roles present in the group for as long as they are a part of the group.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserGroup {
    pub name: String,
    // #[serde(default = "crate::utils::uid::gen")]
    // pub id: Ulid,
    pub roles: HashSet<String>,
    pub users: HashSet<GroupUser>,
}

fn is_valid_group_name(name: &str) -> bool {
    let re = regex::Regex::new(r"^[A-Za-z0-9_-]+$").unwrap();
    re.is_match(name)
}

impl UserGroup {
    pub fn mask(self) -> Self {
        let users = self
            .users
            .into_iter()
            .map(|mut u| {
                u.tenant_id = None;
                u
            })
            .collect();
        Self {
            name: self.name,
            roles: self.roles,
            users,
        }
    }
    pub fn validate(&self, tenant_id: &Option<String>) -> Result<(), RBACError> {
        let valid_name = is_valid_group_name(&self.name);
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        if let Some(tenant_ug) = read_user_groups().get(tenant)
            && tenant_ug.contains_key(&self.name)
        {
            return Err(RBACError::UserGroupExists(self.name.clone()));
        }
        let mut non_existent_roles = Vec::new();
        if !self.roles.is_empty() {
            // validate that the roles exist
            // can't add internal roles to a group
            if let Some(tenant_roles) = roles().get(tenant) {
                for role in &self.roles {
                    if let Some(role) = tenant_roles.get(role)
                        && role.role_type().eq(&RoleType::User)
                    {
                    } else {
                        non_existent_roles.push(role.clone());
                    }
                }
            } else {
                non_existent_roles.extend(self.roles.iter().cloned());
            }
        }
        let mut non_existent_users = Vec::new();
        if !self.users.is_empty() {
            // validate that the users exist and no protected user is included
            if let Some(tenant_users) = users().get(tenant) {
                for group_user in &self.users {
                    if let Some(user) = tenant_users.get(group_user.userid())
                        && !user.protected
                        && user.tenant.eq(tenant_id)
                    {
                        // user is alright
                    } else {
                        non_existent_users.push(group_user.userid().to_string());
                    }
                }
            } else {
                non_existent_users.extend(self.users.iter().map(|u| u.userid().to_string()));
            }
        }

        if !non_existent_roles.is_empty() || !non_existent_users.is_empty() || !valid_name {
            let comments = if !valid_name {
                "The name should follow this regex- `^[A-Za-z0-9_-]+$`".to_string()
            } else {
                "".to_string()
            };
            Err(RBACError::InvalidUserGroupRequest(Box::new(
                InvalidUserGroupError {
                    valid_name,
                    non_existent_roles,
                    non_existent_users,
                    roles_not_in_group: vec![],
                    users_not_in_group: vec![],
                    comments,
                },
            )))
        } else {
            // check if any role is being used by a protected user
            if let Some(tenant_roles) = roles().get(tenant) {
                for name in &self.roles {
                    if let Some(role) = tenant_roles.get(name)
                        && role.role_type().eq(&RoleType::Internal)
                    {
                        return Err(RBACError::ProtectedRole);
                    }
                }
            }
            Ok(())
        }
    }
    pub fn new(name: String, roles: HashSet<String>, users: HashSet<GroupUser>) -> Self {
        UserGroup { name, roles, users }
    }

    pub fn add_roles(
        &mut self,
        roles_to_add: HashSet<String>,
        tenant_id: &str,
    ) -> Result<(), RBACError> {
        if roles_to_add.is_empty() {
            return Ok(());
        }
        if let Some(tenant_roles) = roles().get(tenant_id) {
            for role in &roles_to_add {
                if let Some(role) = tenant_roles.get(role)
                    && role.role_type().eq(&RoleType::Internal)
                {
                    return Err(RBACError::ProtectedRole);
                }
            }
        }
        self.roles.extend(roles_to_add);
        // refresh permissions for all user sessions in this group
        let mut sessions = mut_sessions();
        for group_user in &self.users {
            if let Some(tenant_users) = users().get(tenant_id)
                && let Some(user) = tenant_users.get(group_user.userid())
            {
                let new_perms = roles_to_permission(user.roles(), tenant_id);
                sessions.refresh_user_permissions(group_user.userid(), tenant_id, &new_perms);
            }
        }
        Ok(())
    }

    pub fn add_users(&mut self, users: HashSet<GroupUser>) -> Result<(), RBACError> {
        if users.is_empty() {
            return Ok(());
        }
        self.users.extend(users.clone());
        // refresh permissions for newly added user sessions
        let mut sessions = mut_sessions();
        let all_users = super::map::users();
        for group_user in &users {
            let tid = group_user.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
            if let Some(tenant_users) = all_users.get(tid)
                && let Some(user) = tenant_users.get(group_user.userid())
            {
                let new_perms = roles_to_permission(user.roles(), tid);
                sessions.refresh_user_permissions(group_user.userid(), tid, &new_perms);
            }
        }
        Ok(())
    }

    pub fn remove_roles(&mut self, roles: HashSet<String>) -> Result<(), RBACError> {
        if roles.is_empty() {
            return Ok(());
        }
        let old_roles = &self.roles;
        let new_roles = HashSet::from_iter(self.roles.difference(&roles).cloned());

        if old_roles.eq(&new_roles) {
            return Ok(());
        }
        self.roles.clone_from(&new_roles);

        // refresh permissions for all user sessions in this group
        let mut sessions = mut_sessions();
        for group_user in &self.users {
            let tid = group_user.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
            if let Some(tenant_users) = users().get(tid)
                && let Some(user) = tenant_users.get(group_user.userid())
            {
                let new_perms = roles_to_permission(user.roles(), tid);
                sessions.refresh_user_permissions(group_user.userid(), tid, &new_perms);
            }
        }
        Ok(())
    }

    pub fn remove_users(&mut self, users: HashSet<GroupUser>) -> Result<(), RBACError> {
        if users.is_empty() {
            return Ok(());
        }
        let new_users = HashSet::from_iter(self.users.difference(&users).cloned());
        let removed_users: HashSet<GroupUser> = self.users.intersection(&users).cloned().collect();
        if removed_users.is_empty() {
            return Ok(());
        }
        // refresh permissions for removed user sessions
        let mut sessions = mut_sessions();
        let all_users = super::map::users();
        for group_user in &removed_users {
            let tid = group_user.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
            if let Some(tenant_users) = all_users.get(tid)
                && let Some(user) = tenant_users.get(group_user.userid())
            {
                let new_perms = roles_to_permission(user.roles(), tid);
                sessions.refresh_user_permissions(group_user.userid(), tid, &new_perms);
            }
        }
        self.users.clone_from(&new_users);

        Ok(())
    }

    /// Get all user IDs in this group
    pub fn get_userids(&self) -> Vec<String> {
        self.users.iter().map(|u| u.userid().to_string()).collect()
    }

    /// Add users by converting from User references
    pub fn add_users_from_user_refs(&mut self, user_refs: &[&User]) -> Result<(), RBACError> {
        let group_users: HashSet<GroupUser> =
            user_refs.iter().map(|u| GroupUser::from_user(u)).collect();
        self.add_users(group_users)
    }

    /// Remove users by user ID
    pub fn remove_users_by_user_ids(&mut self, user_ids: HashSet<String>) -> Result<(), RBACError> {
        if user_ids.is_empty() {
            return Ok(());
        }

        let users_to_remove: HashSet<GroupUser> = self
            .users
            .iter()
            .filter(|u| user_ids.contains(u.userid()))
            .cloned()
            .collect();

        self.remove_users(users_to_remove)
    }
}

#[cfg(test)]
mod update_tests {
    use super::*;
    use crate::rbac::map::{TEST_SERIAL, USERS, mut_users};
    use actix_web::{ResponseError, http::StatusCode};
    use parking_lot::RwLock;

    #[test]
    fn preparing_user_patch_does_not_publish_before_persistence() {
        let _serial = TEST_SERIAL.blocking_lock();
        USERS.get_or_init(|| RwLock::new(crate::rbac::map::Users::default()));
        let original = User {
            ty: UserType::Native(Basic {
                username: "patch-user".to_owned(),
                password_hash: "unchanged-hash".to_owned(),
                email: Some("before@example.com".to_owned()),
            }),
            roles: HashSet::from(["reader".to_owned()]),
            user_groups: HashSet::from(["engineering".to_owned()]),
            tenant: Some(format!("patch-test-{}", Ulid::new())),
            protected: false,
        };
        mut_users().insert(original.clone());
        let update = UpdateUser {
            email: Some("after@example.com".to_owned()),
        };

        // This is the handler's step before get_metadata/put_metadata. If either
        // fails, the prepared value is discarded and live state must be intact.
        let prepared = update
            .update(original.userid(), &original.tenant)
            .unwrap()
            .unwrap();
        let mut expected = original.clone();
        expected
            .insert_email("after@example.com".to_owned())
            .unwrap();
        assert_eq!(prepared, expected);
        assert_eq!(
            Users.get_user(original.userid(), &original.tenant),
            Some(original),
            "preparing a PATCH must not publish it before metadata persists"
        );
    }

    #[test]
    fn preparing_oauth_patch_keeps_live_profile_unchanged() {
        let _serial = TEST_SERIAL.blocking_lock();
        USERS.get_or_init(|| RwLock::new(crate::rbac::map::Users::default()));
        let original = User::new_oauth(
            "oauth-user".to_owned(),
            HashSet::from(["reader".to_owned()]),
            UserInfo {
                email: Some("before@example.com".to_owned()),
                name: Some("Unchanged Name".to_owned()),
                ..Default::default()
            },
            None,
            Some(format!("patch-test-{}", Ulid::new())),
            false,
        );
        mut_users().insert(original.clone());
        let prepared = UpdateUser {
            email: Some("after@example.com".to_owned()),
        }
        .update(original.userid(), &original.tenant)
        .unwrap()
        .unwrap();
        let mut expected = original.clone();
        expected
            .insert_email("after@example.com".to_owned())
            .unwrap();
        assert_eq!(prepared, expected);
        assert_eq!(
            Users.get_user(original.userid(), &original.tenant),
            Some(original)
        );
    }

    #[test]
    fn user_patch_preserves_noop_and_validation_order() {
        let _serial = TEST_SERIAL.blocking_lock();
        USERS.get_or_init(|| RwLock::new(crate::rbac::map::Users::default()));
        let tenant = Some(format!("patch-test-{}", Ulid::new()));
        assert!(
            UpdateUser { email: None }
                .update("missing", &tenant)
                .unwrap()
                .is_none()
        );
        let invalid = UpdateUser {
            email: Some("invalid".to_owned()),
        }
        .update("missing", &tenant)
        .unwrap_err();
        assert_eq!(invalid.to_string(), "Error: Invalid Email");
        assert_eq!(invalid.status_code(), StatusCode::INTERNAL_SERVER_ERROR);

        let valid = UpdateUser {
            email: Some("after@example.com".to_owned()),
        };
        let missing = valid.update("missing", &tenant).unwrap_err();
        assert!(matches!(missing, RBACError::UserDoesNotExist));
        assert_eq!(missing.status_code(), StatusCode::NOT_FOUND);

        let api_key = User::new_api_key(
            Ulid::new(),
            "test-key".to_owned(),
            "test".to_owned(),
            HashSet::new(),
            "creator".to_owned(),
            tenant.clone(),
        );
        mut_users().insert(api_key.clone());
        let unsupported = valid.update(api_key.userid(), &tenant).unwrap_err();
        assert_eq!(unsupported.to_string(), "Error: Can't add email to API key");
        assert_eq!(unsupported.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(Users.get_user(api_key.userid(), &tenant), Some(api_key));
    }
}

#[cfg(test)]
mod apikey_serde_tests {
    use super::*;

    #[test]
    fn user_roundtrip_covers_api_key_variant() {
        let key_id = Ulid::new();
        let roles: HashSet<String> = ["admin".to_string()].into_iter().collect();
        let user = User::new_api_key(
            key_id,
            "secret-token".into(),
            "my-key".into(),
            roles.clone(),
            "admin-user".into(),
            Some("tenant-a".into()),
        );

        // userid is derived from key_id with no prefix.
        assert_eq!(user.userid(), key_id.to_string());

        let json = serde_json::to_string(&user).expect("serialize User(ApiKey)");
        // The key is not in what is stored and sent to other nodes: its hash is
        assert!(!json.contains("secret-token"), "json = {json}");
        assert!(!json.contains("\"apiKey\""), "json = {json}");
        assert!(
            json.contains(&format!("\"keyHash\":\"{}\"", hash_api_key("secret-token"))),
            "json = {json}"
        );
        assert!(json.contains("\"keyHint\":\"oken\""), "json = {json}");
        assert!(json.contains("\"roles\""), "json = {json}");
        assert!(json.contains("\"userid\""), "json = {json}");

        let back: User = serde_json::from_str(&json).expect("deserialize User");
        assert!(back.is_api_key());
        assert_eq!(back.userid(), user.userid());
        assert_eq!(back.roles, roles);
        let ak = back.as_api_key().expect("ApiKey variant");
        assert_eq!(ak.key_id, key_id);
        assert_eq!(ak.userid, key_id.to_string());
        assert_eq!(ak, user.as_api_key().unwrap());
        let now = Utc::now();
        assert!(ak.matches_active_key("secret-token", now));
        assert!(!ak.matches_active_key("secret-toke", now));
        assert!(!ak.matches_active_key("", now));
        // What is stored is not itself a key
        assert!(!ak.matches_active_key(&ak.key_hash, now));
        assert_eq!(ak.masked_key(), "****oken");
    }

    #[test]
    fn api_key_hash_is_sha256_in_lowercase_hex() {
        // FIPS 180-2 test vector
        assert_eq!(
            hash_api_key("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(hash_api_key("").len(), 64);
    }

    #[test]
    fn hint_is_the_last_four_characters_of_a_long_enough_key() {
        assert_eq!(api_key_hint("550e8400-e29b-41d4-a716-446655440000"), "0000");
        assert_eq!(api_key_hint("12345678"), "5678");
        // Not half or more of a short key, and never cut inside a character
        assert_eq!(api_key_hint("1234567"), "");
        assert_eq!(api_key_hint(""), "");
        assert_eq!(api_key_hint("ключ-ключ"), "ключ");
    }

    /// A record written before keys were hashed, exactly as it was stored
    fn unhashed_record(key: &str) -> serde_json::Value {
        serde_json::json!({
            "userid": "01KPWAKVAQVXJGTKRKZCSVVCDF",
            "keyId": "01KPWAKVAQVXJGTKRKZCSVVCDF",
            "apiKey": key,
            "keyName": "diag-test-key",
            "createdBy": "admin",
            "createdAt": "2026-01-01T12:00:00Z",
            "modifiedAt": "2026-01-01T12:00:00Z",
            "expiresAt": null,
            "roles": ["admin"],
            "user_groups": [],
            "tenant": null,
            "protected": false
        })
    }

    #[test]
    fn record_stored_with_the_key_is_read_and_written_back_hashed() {
        let key = "550e8400-e29b-41d4-a716-446655440000";
        let user: User = serde_json::from_value(unhashed_record(key)).unwrap();
        let api_key = user.as_api_key().expect("ApiKey variant");
        assert_eq!(api_key.key_hash, hash_api_key(key));
        assert_eq!(api_key.key_hint, "0000");
        assert_eq!(api_key.key_name, "diag-test-key");
        // The key keeps working
        assert!(api_key.matches_active_key(key, Utc::now()));

        let written = serde_json::to_string(&user).unwrap();
        assert!(!written.contains(key), "{written}");
        assert!(!written.contains("apiKey"), "{written}");
        let back: User = serde_json::from_str(&written).unwrap();
        assert_eq!(back, user);
    }

    #[test]
    fn stored_hash_wins_over_a_key_stored_beside_it() {
        let mut record = unhashed_record("the-key-in-the-record");
        record["keyHash"] = hash_api_key("the-key-that-was-hashed").into();
        record["keyHint"] = "shed".into();
        let user: User = serde_json::from_value(record).unwrap();
        let api_key = user.as_api_key().unwrap();
        let now = Utc::now();
        assert!(api_key.matches_active_key("the-key-that-was-hashed", now));
        assert!(!api_key.matches_active_key("the-key-in-the-record", now));
        assert_eq!(api_key.key_hint, "shed");
    }

    #[test]
    fn api_key_record_without_hash_or_key_is_rejected() {
        let mut record = unhashed_record("unused");
        record.as_object_mut().unwrap().remove("apiKey");
        assert!(serde_json::from_value::<User>(record.clone()).is_err());
        // A hash without a hint is enough: the listing shows the mask alone
        record["keyHash"] = hash_api_key("some-key").into();
        let user: User = serde_json::from_value(record).unwrap();
        assert_eq!(user.as_api_key().unwrap().masked_key(), "****");
    }

    #[test]
    fn unhashed_keys_are_found_in_stored_metadata() {
        let metadata = |users: serde_json::Value| {
            serde_json::to_vec(&serde_json::json!({ "version": "v7", "users": users })).unwrap()
        };
        let unhashed = unhashed_record("550e8400-e29b-41d4-a716-446655440000");
        let hashed =
            serde_json::to_value(serde_json::from_value::<User>(unhashed.clone()).unwrap())
                .unwrap();
        let (native, _) = User::new_basic("alice".into(), None, false);
        let native = serde_json::to_value(native).unwrap();

        assert!(stores_unhashed_api_key(&metadata(serde_json::json!([
            native, hashed, unhashed
        ]))));
        assert!(!stores_unhashed_api_key(&metadata(serde_json::json!([
            native, hashed
        ]))));
        assert!(!stores_unhashed_api_key(&metadata(serde_json::json!([]))));
        assert!(!stores_unhashed_api_key(b"not json"));
        assert!(!stores_unhashed_api_key(b"{}"));
    }

    #[test]
    fn api_key_expiration_roundtrip_and_legacy_missing_field() {
        let mut user = User::new_api_key(
            Ulid::new(),
            "test-secret".into(),
            "test".into(),
            HashSet::new(),
            "admin".into(),
            None,
        );
        let UserType::ApiKey(key) = &mut user.ty else {
            unreachable!()
        };
        let expiry = DateTime::parse_from_rfc3339("2026-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        key.expires_at = Some(expiry);
        let value = serde_json::to_value(&user).unwrap();
        let back: User = serde_json::from_value(value).unwrap();
        assert_eq!(back.as_api_key().unwrap().expires_at, Some(expiry));

        let mut value = serde_json::to_value(user.as_api_key().unwrap()).unwrap();
        value.as_object_mut().unwrap().remove("expiresAt");
        let legacy: ApiKeyUser = serde_json::from_value(value).unwrap();
        assert_eq!(legacy.expires_at, None);
        assert!(serde_json::to_value(legacy).unwrap()["expiresAt"].is_null());
    }

    #[test]
    fn expiration_boundary_and_session_deadline() {
        let mut user = User::new_api_key(
            Ulid::new(),
            "test-secret".into(),
            "test".into(),
            HashSet::new(),
            "admin".into(),
            None,
        );
        let UserType::ApiKey(key) = &mut user.ty else {
            unreachable!()
        };
        let now = DateTime::parse_from_rfc3339("2026-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!key.is_expired_at(now));
        assert_eq!(
            key.session_expires_at(now),
            now + chrono::TimeDelta::minutes(5)
        );
        key.expires_at = Some(now);
        assert!(!key.is_expired_at(now - chrono::TimeDelta::nanoseconds(1)));
        assert!(key.is_expired_at(now));
        assert!(key.is_expired_at(now + chrono::TimeDelta::nanoseconds(1)));
        assert!(!key.matches_active_key("test-secret", now));
        key.expires_at = Some(now + chrono::TimeDelta::seconds(1));
        assert!(key.matches_active_key("test-secret", now));
        assert!(!key.matches_active_key("wrong-secret", now));
        assert_eq!(key.session_expires_at(now), key.expires_at.unwrap());
        let later = now + chrono::TimeDelta::seconds(20);
        assert_eq!(key.cap_session_expiry(later), key.expires_at.unwrap());
        assert_eq!(key.cap_session_expiry(now), now);
        key.expires_at = None;
        assert_eq!(key.cap_session_expiry(later), later);
    }

    #[test]
    fn api_key_rejects_malformed_stored_expiration() {
        let user = User::new_api_key(
            Ulid::new(),
            "test-secret".into(),
            "test".into(),
            HashSet::new(),
            "admin".into(),
            None,
        );
        for invalid in [
            serde_json::json!("not-a-timestamp"),
            serde_json::json!("2026-99-99T00:00:00Z"),
            serde_json::json!(123),
        ] {
            let mut value = serde_json::to_value(&user).unwrap();
            value["expiresAt"] = invalid;
            assert!(serde_json::from_value::<User>(value).is_err());
        }
    }

    #[test]
    fn native_user_still_roundtrips_with_api_key_variant_present() {
        let (user, _pw) = User::new_basic("alice".into(), Some("tenant-a".into()), false);
        let json = serde_json::to_string(&user).unwrap();
        let back: User = serde_json::from_str(&json).unwrap();
        assert!(matches!(back.ty, UserType::Native(_)));
        assert_eq!(back.userid(), "alice");
    }

    #[test]
    fn dump_api_key_json() {
        let key_id = Ulid::from_string("01KPWAKVAQVXJGTKRKZCSVVCDF").unwrap();
        let roles: HashSet<String> = ["admin".to_string()].into_iter().collect();
        let user = User::new_api_key(
            key_id,
            "550e8400-e29b-41d4-a716-446655440000".into(),
            "diag-test-key".into(),
            roles,
            "admin".into(),
            None,
        );
        let pretty = serde_json::to_string_pretty(&user).unwrap();
        println!(
            "\n=== user json (persisted shape) ===\n{pretty}\n====================================\n"
        );
    }
}

#[cfg(test)]
mod publish_tests {
    use super::*;
    use crate::rbac::map::{TEST_SERIAL, USERS, mut_users};
    use parking_lot::RwLock;

    fn native(tenant: &Option<String>, email: &str) -> User {
        User {
            ty: UserType::Native(Basic {
                username: format!("publish-{}", Ulid::new()),
                password_hash: String::new(),
                email: Some(email.to_owned()),
            }),
            roles: HashSet::new(),
            user_groups: HashSet::new(),
            tenant: tenant.clone(),
            protected: false,
        }
    }

    #[test]
    fn apply_changes_only_the_patched_email() {
        let mut user = native(&None, "before@example.com");
        let mut expected = user.clone();
        expected
            .insert_email("after@example.com".to_owned())
            .unwrap();
        UpdateUser {
            email: Some("after@example.com".to_owned()),
        }
        .apply(&mut user)
        .unwrap();
        assert_eq!(user, expected);
    }

    #[test]
    fn replace_user_updates_a_live_user_but_never_recreates_one() {
        let _serial = TEST_SERIAL.blocking_lock();
        USERS.get_or_init(|| RwLock::new(crate::rbac::map::Users::default()));
        let tenant = Some(format!("publish-test-{}", Ulid::new()));
        let live = native(&tenant, "before@example.com");
        mut_users().insert(live.clone());
        let mut patched = live.clone();
        patched
            .insert_email("after@example.com".to_owned())
            .unwrap();

        Users.replace_user(patched.clone());
        assert_eq!(Users.get_user(live.userid(), &tenant), Some(patched));

        let removed = native(&tenant, "gone@example.com");
        Users.replace_user(removed.clone());
        assert_eq!(Users.get_user(removed.userid(), &tenant), None);
    }
}
