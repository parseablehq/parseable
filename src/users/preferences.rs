/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

use std::collections::HashMap;

use once_cell::sync::Lazy;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::RwLock;

use crate::{
    handlers::http::users::USERS_ROOT_DIR,
    metastore::{MetastoreError, metastore_traits::MetastoreObject},
    parseable::{DEFAULT_TENANT, PARSEABLE},
};

pub const USER_PREFERENCES_FILE: &str = "preferences.json";
pub static USER_PREFERENCES: Lazy<UserPreferencesStore> = Lazy::new(UserPreferencesStore::default);

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct UserPreferences {
    #[serde(flatten)]
    pub other_fields: Option<Map<String, Value>>,
    #[serde(skip)]
    user_id: Option<String>,
    #[serde(skip)]
    tenant_id: Option<String>,
}

impl UserPreferences {
    pub fn set_metadata(&mut self, user_id: String, tenant_id: Option<String>) {
        self.user_id = Some(user_id);
        self.tenant_id = tenant_id;
    }
}

pub fn user_preferences_path(user_id: &str, tenant_id: &Option<String>) -> RelativePathBuf {
    RelativePathBuf::from_iter([
        tenant_id.as_deref().unwrap_or(""),
        USERS_ROOT_DIR,
        user_id,
        USER_PREFERENCES_FILE,
    ])
}

impl MetastoreObject for UserPreferences {
    fn get_object_path(&self) -> String {
        user_preferences_path(
            self.user_id.as_deref().expect("user metadata must be set"),
            &self.tenant_id,
        )
        .to_string()
    }

    fn get_object_id(&self) -> String {
        self.user_id
            .as_ref()
            .expect("user metadata must be set")
            .clone()
    }
}

#[derive(Debug, Default)]
pub struct UserPreferencesStore(RwLock<HashMap<String, HashMap<String, UserPreferences>>>);

impl UserPreferencesStore {
    pub async fn load(&self) -> Result<(), MetastoreError> {
        let stored_preferences = PARSEABLE.metastore.get_all_user_preferences().await?;
        let mut preferences = HashMap::new();

        for (tenant_id, users) in stored_preferences {
            let tenant_preferences = preferences.entry(tenant_id).or_insert_with(HashMap::new);
            for (user_id, bytes) in users {
                match serde_json::from_slice(&bytes) {
                    Ok(user_preferences) => {
                        tenant_preferences.insert(user_id, user_preferences);
                    }
                    Err(error) => {
                        tracing::warn!(
                            "Failed to deserialize preferences for user {user_id}: {error}"
                        );
                    }
                }
            }
        }

        *self.0.write().await = preferences;
        Ok(())
    }

    pub async fn get(&self, user_id: &str, tenant_id: &Option<String>) -> UserPreferences {
        let tenant_id = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        self.0
            .read()
            .await
            .get(tenant_id)
            .and_then(|users| users.get(user_id))
            .cloned()
            .unwrap_or_default()
    }

    pub async fn put(
        &self,
        preferences: &UserPreferences,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let mut stored_preferences = self.0.write().await;
        PARSEABLE
            .metastore
            .put_user_preferences(preferences, tenant_id)
            .await?;

        stored_preferences
            .entry(tenant_id.as_deref().unwrap_or(DEFAULT_TENANT).to_owned())
            .or_default()
            .insert(
                preferences
                    .user_id
                    .as_ref()
                    .expect("user metadata must be set")
                    .clone(),
                preferences.clone(),
            );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preference_json_contains_generic_fields_only() {
        let mut preferences = UserPreferences {
            other_fields: Some(
                serde_json::json!({
                    "timezone": "Asia/Kolkata",
                    "theme": "dark"
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
            ..Default::default()
        };
        preferences.set_metadata("user-hash".to_owned(), Some("tenant-a".to_owned()));

        assert_eq!(
            serde_json::to_value(preferences).unwrap(),
            serde_json::json!({
                "timezone": "Asia/Kolkata",
                "theme": "dark"
            })
        );
    }

    #[test]
    fn preference_path_is_inside_user_directory() {
        assert_eq!(
            user_preferences_path("user-hash", &Some("tenant-a".to_owned())).as_str(),
            "tenant-a/.users/user-hash/preferences.json"
        );
        assert_eq!(
            user_preferences_path("user-hash", &None).as_str(),
            ".users/user-hash/preferences.json"
        );
    }

    #[tokio::test]
    async fn missing_preference_is_empty() {
        let store = UserPreferencesStore::default();
        assert_eq!(
            store.get("unknown", &None).await,
            UserPreferences::default()
        );
    }
}
