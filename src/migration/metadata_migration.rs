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

use rand::distributions::DistString;
use serde_json::{Map, Value as JsonValue, json};

use crate::parseable::PARSEABLE;

/*
v1
{
    "version": "v1",
    "mode": "drive"
    "user": string,
    "staging": "string",
    "storage": "string",
    "deployment_id": "string"
    "stream": string,
    "default_role": null
}
*/
pub fn v1_v3(mut storage_metadata: JsonValue) -> JsonValue {
    let metadata = storage_metadata.as_object_mut().unwrap();
    *metadata.get_mut("version").unwrap() = JsonValue::String("v3".to_string());
    metadata.remove("user");
    metadata.remove("stream");
    metadata.insert("users".to_string(), JsonValue::Array(vec![]));
    metadata.insert("streams".to_string(), JsonValue::Array(vec![]));
    metadata.insert("roles".to_string(), JsonValue::Array(vec![]));
    metadata.insert("server_mode".to_string(), json!(PARSEABLE.options.mode));
    storage_metadata
}

/*
v2
{
    "version": "v2",
    "users": [
        {
            "role": ["privilege1", "privilege2", ...]
        },
    ...
    ]
    ...
}
*/
pub fn v2_v3(mut storage_metadata: JsonValue) -> JsonValue {
    let metadata = storage_metadata.as_object_mut().unwrap();
    *metadata.get_mut("version").unwrap() = JsonValue::String("v3".to_string());
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
        let JsonValue::Array(mut privileges) = user.remove("role").expect("role exists for v2")
        else {
            panic!("privileges is an arrray")
        };

        let mut roles = Vec::new();

        if !privileges.is_empty() {
            for privilege in privileges.iter_mut() {
                let privilege_value = privilege.get_mut("privilege");
                if let Some(value) = privilege_value {
                    if value.as_str().unwrap() == "ingester" {
                        *value = JsonValue::String("ingestor".to_string());
                    }
                }
            }
            let role_name =
                rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
            privileges_map.push((role_name.clone(), JsonValue::Array(privileges)));
            roles.push(JsonValue::from(role_name));
        }
        user.insert("roles".to_string(), roles.into());
    }

    metadata.insert(
        "roles".to_string(),
        JsonValue::Object(Map::from_iter(privileges_map)),
    );
    metadata.insert("server_mode".to_string(), json!(PARSEABLE.options.mode));
    storage_metadata
}

// maybe rename
pub fn v3_v4(mut storage_metadata: JsonValue) -> JsonValue {
    let metadata = storage_metadata.as_object_mut().unwrap();
    *metadata.get_mut("version").unwrap() = JsonValue::String("v4".to_string());
    let sm = metadata.get("server_mode");

    if sm.is_none() || sm.unwrap().as_str().unwrap() == "All" {
        metadata.insert("server_mode".to_string(), json!(PARSEABLE.options.mode));
    }

    let roles = metadata.get_mut("roles").unwrap().as_object_mut().unwrap();
    for (_, privileges) in roles.iter_mut() {
        let JsonValue::Array(privileges) = privileges else {
            panic!("privileges is an array")
        };
        for privilege in privileges.iter_mut() {
            let privilege_value = privilege.get_mut("privilege");
            if let Some(value) = privilege_value {
                if value.as_str().unwrap() == "ingester" {
                    *value = JsonValue::String("ingestor".to_string());
                }
            }
        }
    }
    storage_metadata
}

pub fn v4_v5(mut storage_metadata: JsonValue) -> JsonValue {
    let metadata = storage_metadata.as_object_mut().unwrap();
    metadata.remove_entry("version");
    metadata.insert("version".to_string(), JsonValue::String("v5".to_string()));

    match metadata.get("server_mode") {
        None => {
            metadata.insert("server_mode".to_string(), json!(PARSEABLE.options.mode));
        }
        Some(JsonValue::String(mode)) => match mode.as_str() {
            "Query" => {
                metadata.insert(
                    "querier_endpoint".to_string(),
                    JsonValue::String(PARSEABLE.options.address.clone()),
                );
            }
            "All" => {
                metadata.insert("server_mode".to_string(), json!(PARSEABLE.options.mode));
            }
            _ => (),
        },
        _ => (),
    }

    storage_metadata
}

pub fn v5_v6(mut storage_metadata: JsonValue) -> JsonValue {
    let metadata = storage_metadata.as_object_mut().unwrap();
    metadata.remove_entry("version");
    metadata.insert("version".to_string(), JsonValue::String("v6".to_string()));

    // If user_groups is missing, add an empty array or your default structure
    if !metadata.contains_key("user_groups") {
        metadata.insert("user_groups".to_string(), JsonValue::Array(vec![]));
    }

    // introduce user groups entry for all users
    let users = metadata.get_mut("users").unwrap().as_array_mut().unwrap();
    for user in users.iter_mut() {
        if !user.as_object_mut().unwrap().contains_key("user_groups") {
            user.as_object_mut()
                .unwrap()
                .insert("user_groups".to_string(), JsonValue::Array(vec![]));
        }
    }

    if let Some(JsonValue::Object(roles)) = metadata.get_mut("roles") {
        for (_, role_permissions) in roles.iter_mut() {
            if let JsonValue::Array(permissions) = role_permissions {
                for permission in permissions.iter_mut() {
                    if let JsonValue::Object(perm_obj) = permission {
                        if let Some(JsonValue::Object(resource)) = perm_obj.get_mut("resource") {
                            resource.remove("tag");
                        }
                    }
                }
            }
        }
    }

    storage_metadata
}

/// Remove the querier endpoint and auth token from the storage metadata
pub fn remove_querier_metadata(mut storage_metadata: JsonValue) -> JsonValue {
    let metadata = storage_metadata.as_object_mut().unwrap();
    metadata.remove("querier_endpoint");
    metadata.remove("querier_auth_token");
    storage_metadata
}
