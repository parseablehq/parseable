use std::sync::RwLock;

use once_cell::sync::OnceCell;

use self::user::{get_admin_user, User, UserMap};

pub mod user;

pub static USERS: OnceCell<RwLock<UserMap>> = OnceCell::new();

pub fn get_user_map() -> &'static RwLock<UserMap> {
    USERS.get().expect("user map is set")
}

pub fn set_user_map(users: Vec<User>) {
    let mut map = UserMap::default();
    for user in users {
        map.insert(user)
    }
    map.insert(get_admin_user());
    USERS.set(RwLock::new(map)).expect("map is only set once")
}
