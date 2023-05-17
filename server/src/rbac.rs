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
