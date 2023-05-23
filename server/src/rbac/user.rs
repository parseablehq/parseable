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

use std::collections::{HashMap, HashSet};

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2, PasswordHash, PasswordVerifier,
};

use rand::distributions::{Alphanumeric, DistString};

use crate::option::CONFIG;

use super::role::{model::DefaultPrivilege, Action, Permission, RoleBuilder};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub roles: Vec<DefaultPrivilege>,
}

impl User {
    // create a new User and return self with password generated for said user.
    pub fn create_new(username: String) -> (Self, String) {
        let PassCode { password, hash } = Self::gen_new_password();
        (
            Self {
                username,
                password_hash: hash,
                roles: Vec::new(),
            },
            password,
        )
    }

    // generate a new password
    pub fn gen_new_password() -> PassCode {
        let password = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let hash = gen_hash(&password);
        PassCode { password, hash }
    }

    pub fn permissions(&self) -> Vec<Permission> {
        let perms: HashSet<Permission> = self
            .roles
            .iter()
            .flat_map(|role| RoleBuilder::from(role).build())
            .collect();
        perms.into_iter().collect()
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
    let hashcode = argon2
        .hash_password(password.as_bytes(), &salt)
        .expect("can hash random alphanumeric")
        .to_string();

    hashcode
}

#[derive(Debug, Default, derive_more::Deref, derive_more::DerefMut)]
pub struct UserMap(HashMap<String, String>);

impl UserMap {
    pub fn insert(&mut self, user: User) {
        self.0.insert(user.username, user.password_hash);
    }
}

impl From<Vec<User>> for UserMap {
    fn from(users: Vec<User>) -> Self {
        let mut user_map = Self::default();
        for user in users {
            user_map.insert(user);
        }
        user_map
    }
}

pub struct PassCode {
    pub password: String,
    pub hash: String,
}

pub fn get_admin_user() -> User {
    let username = CONFIG.parseable.username.clone();
    let password = CONFIG.parseable.password.clone();
    let hashcode = gen_hash(&password);

    User {
        username,
        password_hash: hashcode,
        roles: vec![DefaultPrivilege::Admin],
    }
}

#[derive(Debug, Default, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct UserPermMap(HashMap<String, Vec<Permission>>);

impl UserPermMap {
    pub fn insert(&mut self, user: &User) {
        self.0.insert(user.username.clone(), user.permissions());
    }

    pub fn has_perm(
        &self,
        username: &str,
        required_action: Action,
        on_stream: Option<&str>,
    ) -> bool {
        if let Some(perms) = self.get(username) {
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
        } else {
            // NO permission set for this user
            false
        }
    }
}

impl From<&Vec<User>> for UserPermMap {
    fn from(users: &Vec<User>) -> Self {
        let mut map = Self::default();
        for user in users {
            map.insert(user)
        }
        map
    }
}
