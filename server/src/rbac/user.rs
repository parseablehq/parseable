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
    pub password: String,
    // fill this
    pub role: Vec<()>,
}

impl User {
    // create a new User and return self with password generated for said user.
    pub fn create_new(username: String) -> (Self, String) {
        let PassCode { password, hashcode } = Self::gen_new_password();
        (
            Self {
                username,
                password: hashcode,
                role: Vec::new(),
            },
            password,
        )
    }

    // Verification works because the PasswordHash is in PHC format
    // $<id>[$v=<version>][$<param>=<value>(,<param>=<value>)*][$<salt>[$<hash>]]
    // ref https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md#specification
    pub fn verify(&self, password: &str) -> bool {
        let parsed_hash = PasswordHash::new(&self.password).unwrap();
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }

    // gen new password
    pub fn gen_new_password() -> PassCode {
        let password = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let hashcode = gen_hash(&password);
        PassCode { password, hashcode }
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
    pub hashcode: String,
}

pub fn get_admin_user() -> User {
    let username = CONFIG.parseable.username.clone();
    let password = CONFIG.parseable.password.clone();
    let hashcode = gen_hash(&password);

    User {
        username,
        password: hashcode,
        role: Vec::new(),
    }
}
