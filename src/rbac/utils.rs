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
use std::collections::HashMap;

use super::{
    map::roles,
    role::model::DefaultPrivilege,
    user::{User, UserType},
    Users, UsersPrism,
};

pub fn to_prism_user(user: &User) -> UsersPrism {
    let (id, method, email, picture) = match &user.ty {
        UserType::Native(_) => (user.username(), "native", None, None),
        UserType::OAuth(oauth) => (
            user.username(),
            "oauth",
            oauth.user_info.email.clone(),
            oauth.user_info.picture.clone(),
        ),
    };
    let roles: HashMap<String, Vec<DefaultPrivilege>> = Users
        .get_role(id)
        .iter()
        .filter_map(|role_name| {
            roles()
                .get(role_name)
                .map(|role| (role_name.to_owned(), role.clone()))
        })
        .collect();

    UsersPrism {
        id: id.into(),
        method: method.into(),
        email,
        picture,
        roles,
    }
}
