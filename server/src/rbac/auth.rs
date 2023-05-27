use std::collections::HashMap;

use super::role::{Action, Permission};

// A session is quicker way to authenticate user based on username and password
// All users in this map are always authenticated
#[derive(Debug, Default)]
pub struct AuthMap {
    inner: HashMap<(String, String), Vec<Permission>>,
}

impl AuthMap {
    // add user to the session store and return session token
    // does not care about session from existing user
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
