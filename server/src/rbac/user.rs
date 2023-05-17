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

use std::collections::HashMap;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2, PasswordHash, PasswordVerifier,
};

use rand::distributions::{Alphanumeric, DistString};

use crate::option::CONFIG;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    // fill this
    pub role: Vec<()>,
}

impl User {
    // create a new User and return self with password generated for said user.
    pub fn create_new(username: String) -> (Self, String) {
        let PassCode { password, hash } = Self::gen_new_password();
        (
            Self {
                username,
                password_hash: hash,
                role: Vec::new(),
            },
            password,
        )
    }

    // Verification works because the PasswordHash is in PHC format
    // $<id>[$v=<version>][$<param>=<value>(,<param>=<value>)*][$<salt>[$<hash>]]
    // ref https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md#specification
    pub fn verify(&self, password: &str) -> bool {
        let parsed_hash = PasswordHash::new(&self.password_hash).unwrap();
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }

    // gen new password
    pub fn gen_new_password() -> PassCode {
        let password = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let hash = gen_hash(&password);
        PassCode { password, hash }
    }
}

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
pub struct UserMap(HashMap<String, User>);

impl UserMap {
    pub fn insert(&mut self, user: User) {
        self.0.insert(user.username.clone(), user);
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
        role: Vec::new(),
    }
}
