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

use std::collections::HashSet;

use argon2::{
    Argon2, PasswordHash, PasswordVerifier,
    password_hash::{PasswordHasher, SaltString, rand_core::OsRng},
};

use rand::distributions::{Alphanumeric, DistString};

use crate::{
    handlers::http::{
        modal::utils::rbac_utils::{get_metadata, put_metadata},
        rbac::{InvalidUserGroupError, RBACError},
    },
    parseable::PARSEABLE,
    rbac::map::{mut_sessions, read_user_groups, roles, users},
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum UserType {
    Native(Basic),
    OAuth(OAuth),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct User {
    #[serde(flatten)]
    pub ty: UserType,
    pub roles: HashSet<String>,
    pub user_groups: HashSet<String>,
}

impl User {
    // create a new User and return self with password generated for said user.
    pub fn new_basic(username: String) -> (Self, String) {
        let PassCode { password, hash } = Basic::gen_new_password();
        (
            Self {
                ty: UserType::Native(Basic {
                    username,
                    password_hash: hash,
                }),
                roles: HashSet::new(),
                user_groups: HashSet::new(),
            },
            password,
        )
    }

    pub fn new_oauth(userid: String, roles: HashSet<String>, user_info: UserInfo) -> Self {
        Self {
            ty: UserType::OAuth(OAuth {
                userid: user_info.sub.clone().unwrap_or(userid),
                user_info,
            }),
            roles,
            user_groups: HashSet::new(),
        }
    }

    pub fn userid(&self) -> &str {
        match self.ty {
            UserType::Native(Basic { ref username, .. }) => username,
            UserType::OAuth(OAuth { ref userid, .. }) => userid,
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
        }
    }

    pub fn is_oauth(&self) -> bool {
        matches!(self.ty, UserType::OAuth(_))
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
}

impl Basic {
    // generate a new password
    pub fn gen_new_password() -> PassCode {
        let password = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
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
fn gen_hash(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
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

pub fn get_admin_user() -> User {
    let username = PARSEABLE.options.username.clone();
    let password = PARSEABLE.options.password.clone();
    let hashcode = gen_hash(&password);

    User {
        ty: UserType::Native(Basic {
            username,
            password_hash: hashcode,
        }),
        roles: ["admin".to_string()].into(),
        user_groups: HashSet::new(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OAuth {
    pub userid: String,
    pub user_info: UserInfo,
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

/// Represents a user in a UserGroup - simplified structure for both user types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupUser {
    pub userid: String,
    pub username: String,
    pub method: String,
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
            },
            UserType::OAuth(OAuth { userid, user_info }) => {
                // For OAuth users, derive the display username from user_info
                let display_username = user_info
                    .name
                    .clone()
                    .or_else(|| user_info.email.clone())
                    .unwrap_or_else(|| userid.clone()); // fallback to userid if nothing else available

                GroupUser {
                    userid: userid.clone(),
                    username: display_username,
                    method: "oauth".to_string(),
                }
            }
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
    pub fn validate(&self) -> Result<(), RBACError> {
        let valid_name = is_valid_group_name(&self.name);

        if read_user_groups().contains_key(&self.name) {
            return Err(RBACError::UserGroupExists(self.name.clone()));
        }
        let mut non_existent_roles = Vec::new();
        if !self.roles.is_empty() {
            // validate that the roles exist
            for role in &self.roles {
                if !roles().contains_key(role) {
                    non_existent_roles.push(role.clone());
                }
            }
        }
        let mut non_existent_users = Vec::new();
        if !self.users.is_empty() {
            // validate that the users exist
            for group_user in &self.users {
                if !users().contains_key(group_user.userid()) {
                    non_existent_users.push(group_user.userid().to_string());
                }
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
            Ok(())
        }
    }
    pub fn new(name: String, roles: HashSet<String>, users: HashSet<GroupUser>) -> Self {
        UserGroup { name, roles, users }
    }

    pub fn add_roles(&mut self, roles: HashSet<String>) -> Result<(), RBACError> {
        if roles.is_empty() {
            return Ok(());
        }
        self.roles.extend(roles);
        // also refresh all user sessions
        for group_user in &self.users {
            mut_sessions().remove_user(group_user.userid());
        }
        Ok(())
    }

    pub fn add_users(&mut self, users: HashSet<GroupUser>) -> Result<(), RBACError> {
        if users.is_empty() {
            return Ok(());
        }
        self.users.extend(users.clone());
        // also refresh all user sessions
        for group_user in &users {
            mut_sessions().remove_user(group_user.userid());
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

        // also refresh all user sessions
        for group_user in &self.users {
            mut_sessions().remove_user(group_user.userid());
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
        // also refresh all user sessions
        for group_user in &removed_users {
            mut_sessions().remove_user(group_user.userid());
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

    pub async fn update_in_metadata(&self) -> Result<(), RBACError> {
        let mut metadata = get_metadata().await?;
        metadata.user_groups.retain(|x| x.name != self.name);
        metadata.user_groups.push(self.clone());
        put_metadata(&metadata).await?;
        Ok(())
    }
}
