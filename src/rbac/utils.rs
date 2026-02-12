/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use url::Url;

use crate::{
    parseable::{DEFAULT_TENANT, PARSEABLE},
    rbac::map::read_user_groups,
};

use super::{
    Users, UsersPrism,
    map::roles,
    role::model::DefaultPrivilege,
    user::{User, UserType},
};

pub fn to_prism_user(user: &User) -> UsersPrism {
    let tenant_id = user.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
    let (id, username, method, email, picture) = match &user.ty {
        UserType::Native(_) => (user.userid(), user.userid(), "native", None, None),
        UserType::OAuth(oauth) => {
            let userid = user.userid();
            let display_name = oauth.user_info.name.as_deref().unwrap_or(userid);
            (
                userid,
                display_name,
                "oauth",
                oauth.user_info.email.clone(),
                oauth.user_info.picture.clone(),
            )
        }
    };
    let direct_roles: HashMap<String, Vec<DefaultPrivilege>> = Users
        .get_role(id, &user.tenant)
        .iter()
        .filter_map(|role_name| {
            roles()
                .get(tenant_id)
                .filter(|roles| roles.get(role_name).is_some())
                // .map(|roles| {
                //     if let Some(role) = roles.get(role_name) {
                //         (role_name.to_owned(), role.clone())
                //     }
                // })
                // .get(role_name)
                .map(|roles| {
                    let role = roles.get(role_name).unwrap();
                    (role_name.to_owned(), role.clone())
                })
        })
        .collect();

    let mut group_roles: HashMap<String, HashMap<String, Vec<DefaultPrivilege>>> = HashMap::new();
    let mut user_groups = HashSet::new();
    // user might be part of some user groups, fetch the roles from there as well
    for user_group in Users.get_user_groups(user.userid(), &user.tenant) {
        if let Some(groups) = read_user_groups().get(tenant_id)
            && let Some(group) = groups.get(&user_group)
        {
            let ug_roles: HashMap<String, Vec<DefaultPrivilege>> = group
                .roles
                .iter()
                .filter_map(|role_name| {
                    roles()
                        .get(tenant_id)
                        .filter(|roles| roles.get(role_name).is_some())
                        .map(|roles| {
                            let role = roles.get(role_name).unwrap();
                            (role_name.to_owned(), role.clone())
                        })
                })
                .collect();
            group_roles.insert(group.name.clone(), ug_roles);
            user_groups.insert(user_group);
        }
    }

    UsersPrism {
        id: id.into(),
        username: username.into(),
        method: method.into(),
        email: mask_pii_string(email),
        picture: mask_pii_url(picture),
        roles: direct_roles,
        group_roles,
        user_groups,
    }
}

//mask PII string if the P_MASK_PII is set
fn mask_pii_string(input: Option<String>) -> Option<String> {
    if !PARSEABLE.options.mask_pii {
        return input;
    }
    mask_string(input)
}

//mask PII url if the P_MASK_PII is set
fn mask_pii_url(input: Option<Url>) -> Option<Url> {
    if !PARSEABLE.options.mask_pii {
        return input;
    }
    None
}

fn mask_string(input: Option<String>) -> Option<String> {
    let input = input.as_ref()?;
    if input.contains('@') {
        // masking an email
        let parts: Vec<&str> = input.split('@').collect();
        //mask everything if not a proper email format
        if parts.len() != 2 {
            return Some("X".repeat(input.len()));
        }

        let username = parts[0];

        // Mask the username - first letter capitalized, rest are X
        let masked_username = if !username.is_empty() {
            let first = username.chars().next().unwrap().to_uppercase().to_string();
            format!("{}{}", first, "X".repeat(username.len() - 1))
        } else {
            return Some("X".repeat(input.len()));
        };

        // mask to XXX for everything after the @ symbol
        Some(format!("{masked_username}@XXX"))
    } else {
        // mask all other strings with X
        Some("X".repeat(input.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_string_with_email() {
        // Test masking a valid email
        let email = Some("test@example.com".to_string());
        let masked_email = mask_string(email);
        assert_eq!(masked_email, Some("TXXX@XXX".to_string()));
    }

    #[test]
    fn test_mask_string_with_invalid_email() {
        // Test masking an invalid email
        let invalid_email = Some("invalid-email".to_string());
        let masked_email = mask_string(invalid_email);
        assert_eq!(masked_email, Some("XXXXXXXXXXXXX".to_string()));
    }

    #[test]
    fn test_mask_string_with_empty_string() {
        // Test masking an empty string
        let empty_string = Some("".to_string());
        let masked_string = mask_string(empty_string);
        assert_eq!(masked_string, Some("".to_string()));
    }

    #[test]
    fn test_mask_string_with_generic_string() {
        // Test masking a generic string
        let generic_string = Some("sensitive_data".to_string());
        let masked_string = mask_string(generic_string);
        assert_eq!(masked_string, Some("XXXXXXXXXXXXXX".to_string()));
    }

    #[test]
    fn test_mask_string_with_none() {
        // Test masking a None value
        let none_value: Option<String> = None;
        let masked_value = mask_string(none_value);
        assert_eq!(masked_value, None);
    }
}
