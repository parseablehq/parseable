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

use rand::distributions::DistString;
use serde_json::{Map, Value};

pub fn v1_v3(mut storage_metadata: serde_json::Value) -> Value {
    let metadata = storage_metadata.as_object_mut().unwrap();
    *metadata.get_mut("version").unwrap() = Value::String("v3".to_string());
    metadata.remove("user");
    metadata.remove("stream");
    metadata.insert("users".to_string(), Value::Array(vec![]));
    metadata.insert("streams".to_string(), Value::Array(vec![]));
    metadata.insert("roles".to_string(), Value::Array(vec![]));
    storage_metadata
}

pub fn v2_v3(mut storage_metadata: serde_json::Value) -> Value {
    let metadata = storage_metadata.as_object_mut().unwrap();
    *metadata.get_mut("version").unwrap() = Value::String("v3".to_string());
    let users = metadata
        .get_mut("users")
        .expect("users field is present")
        .as_array_mut()
        .unwrap();

    // role names - role value
    let mut privileges_map = Vec::new();

    for user in users {
        // user is an object
        let user = user.as_object_mut().unwrap();
        // take out privileges
        let Value::Array(privileges) = user.remove("role").expect("role exists for v2") else {
            panic!("privileges is an arrray")
        };

        let mut roles = Vec::new();

        if !privileges.is_empty() {
            let role_name =
                rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
            privileges_map.push((role_name.clone(), Value::Array(privileges)));
            roles.push(Value::from(role_name));
        }
        user.insert("roles".to_string(), roles.into());
    }

    metadata.insert(
        "roles".to_string(),
        Value::Object(Map::from_iter(privileges_map)),
    );
    storage_metadata
}
