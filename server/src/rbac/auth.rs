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

use super::role::{Action, Permission};

// quicker way to authenticate user based on username and password
// All users in this map are always authenticated
#[derive(Debug, Default)]
pub struct AuthMap {
    inner: HashMap<(String, String), Vec<Permission>>,
}

impl AuthMap {
    pub fn add_user(&mut self, username: String, password: String, permissions: Vec<Permission>) {
        self.inner.insert((username, password), permissions);
    }

    pub fn remove(&mut self, username: &str) {
        self.inner.retain(|(x, _), _| x != username)
    }

    // returns None if user is not in the map
    // Otherwise returns Some(is_authenticated)
    pub fn check_auth(
        &self,
        key: &(String, String),
        required_action: Action,
        on_stream: Option<&str>,
    ) -> Option<bool> {
        self.inner.get(key).map(|perms| {
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
        })
    }
}
