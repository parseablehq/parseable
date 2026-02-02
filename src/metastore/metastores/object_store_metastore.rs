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

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use actix_web::http::StatusCode;
use arrow_schema::Schema;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use relative_path::RelativePathBuf;
use tonic::async_trait;
use tracing::warn;
use ulid::Ulid;

use crate::{
    alerts::{
        alert_structs::{AlertStateEntry, MTTRHistory},
        target::Target,
    },
    catalog::{manifest::Manifest, partition_path},
    handlers::http::{
        modal::{Metadata, NodeMetadata, NodeType},
        users::USERS_ROOT_DIR,
    },
    metastore::{
        MetastoreError,
        metastore_traits::{Metastore, MetastoreObject},
    },
    option::Mode,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    storage::{
        ALERTS_ROOT_DIRECTORY, ObjectStorage, ObjectStorageError, PARSEABLE_ROOT_DIRECTORY,
        SETTINGS_ROOT_DIRECTORY, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
        TARGETS_ROOT_DIRECTORY,
        object_storage::{
            alert_json_path, alert_state_json_path, filter_path, manifest_path, mttr_json_path,
            parseable_json_path, schema_path, stream_json_path, to_bytes,
        },
    },
    users::filters::{Filter, migrate_v1_v2},
};

/// Using PARSEABLE's storage as a metastore (default)
#[derive(Debug)]
pub struct ObjectStoreMetastore {
    pub storage: Arc<dyn ObjectStorage>,
}

#[async_trait]
impl Metastore for ObjectStoreMetastore {
    /// Since Parseable already starts with a connection to an object store, no need to implement this
    async fn initiate_connection(&self) -> Result<(), MetastoreError> {
        unimplemented!()
    }

    /// Fetch mutiple .json objects
    async fn get_objects(
        &self,
        parent_path: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Bytes>, MetastoreError> {
        Ok(self
            .storage
            .get_objects(
                Some(&RelativePathBuf::from(parent_path)),
                Box::new(|file_name| file_name.ends_with(".json")),
                tenant_id,
            )
            .await?)
    }

    /// This function fetches all the overviews from the underlying object store
    async fn get_overviews(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<HashMap<String, Option<Bytes>>, MetastoreError> {
        let streams = self.list_streams(tenant_id).await?;

        let mut all_overviews = HashMap::new();
        for stream in streams {
            let root = tenant_id.as_deref().unwrap_or("");
            let overview_path = RelativePathBuf::from_iter([root, &stream, "overview"]);

            // if the file doesn't exist, load an empty overview
            let overview = (self.storage.get_object(&overview_path, tenant_id).await).ok();

            all_overviews.insert(stream, overview);
        }

        Ok(all_overviews)
    }

    /// This function puts an overview in the object store at the given path
    async fn put_overview(
        &self,
        obj: &dyn MetastoreObject,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            RelativePathBuf::from_iter([tenant_id, stream, "overview"])
        } else {
            RelativePathBuf::from_iter([stream, "overview"])
        };
        Ok(self
            .storage
            .put_object(&path, to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete an overview
    async fn delete_overview(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            RelativePathBuf::from_iter([tenant_id, stream, "overview"])
        } else {
            RelativePathBuf::from_iter([stream, "overview"])
        };
        Ok(self.storage.delete_object(&path, tenant_id).await?)
    }

    /// This function fetches all the keystones from the underlying object store
    async fn get_keystones(&self) -> Result<HashMap<String, Vec<Bytes>>, MetastoreError> {
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        let mut keystones = HashMap::new();
        for mut tenant in base_paths {
            let keystone_path = RelativePathBuf::from_iter([&tenant, ".keystone"]);
            let objs = self
                .storage
                .get_objects(
                    Some(&keystone_path),
                    Box::new(|file_name| {
                        file_name.ends_with(".json") && !file_name.starts_with("conv_")
                    }),
                    &Some(tenant.clone()),
                )
                .await?;
            if tenant.is_empty() {
                tenant.clone_from(&DEFAULT_TENANT.to_string());
            }
            keystones.insert(tenant, objs);
        }

        Ok(keystones)
    }

    /// This function puts a keystone in the object store at the given path
    async fn put_keystone(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let id = obj.get_object_id();
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            RelativePathBuf::from_iter([tenant_id, ".keystone", &format!("{id}.json")])
        } else {
            RelativePathBuf::from_iter([".keystone", &format!("{id}.json")])
        };
        Ok(self
            .storage
            .put_object(&path, to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete a keystone
    async fn delete_keystone(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let id = obj.get_object_id();
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            RelativePathBuf::from_iter([tenant_id, ".keystone", &format!("{id}.json")])
        } else {
            RelativePathBuf::from_iter([".keystone", &format!("{id}.json")])
        };
        Ok(self.storage.delete_object(&path, tenant_id).await?)
    }

    /// This function fetches all the conversations from the underlying object store
    async fn get_conversations(&self) -> Result<HashMap<String, Vec<Bytes>>, MetastoreError> {
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        let mut conversations = HashMap::new();
        for mut tenant in base_paths {
            let conv_path = RelativePathBuf::from_iter([&tenant, ".keystone"]);
            let objs = self
                .storage
                .get_objects(
                    Some(&conv_path),
                    Box::new(|file_name| {
                        file_name.ends_with(".json") && file_name.starts_with("conv_")
                    }),
                    &Some(tenant.clone()),
                )
                .await?;
            if tenant.is_empty() {
                tenant.clone_from(&DEFAULT_TENANT.to_string());
            }
            conversations.insert(tenant, objs);
        }

        Ok(conversations)
    }

    /// This function puts a conversation in the object store at the given path
    async fn put_conversation(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let id = obj.get_object_id();
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            RelativePathBuf::from_iter([tenant_id, ".keystone", &format!("conv_{id}.json")])
        } else {
            RelativePathBuf::from_iter([".keystone", &format!("conv_{id}.json")])
        };
        Ok(self
            .storage
            .put_object(&path, to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete a conversation
    async fn delete_conversation(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let id = obj.get_object_id();
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            RelativePathBuf::from_iter([tenant_id, ".keystone", &format!("conv_{id}.json")])
        } else {
            RelativePathBuf::from_iter([".keystone", &format!("conv_{id}.json")])
        };
        Ok(self.storage.delete_object(&path, tenant_id).await?)
    }

    /// This function fetches all the alerts from the underlying object store
    async fn get_alerts(&self) -> Result<HashMap<String, Vec<Bytes>>, MetastoreError> {
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        let mut all_alerts = HashMap::new();
        for mut tenant in base_paths {
            let alerts_path = RelativePathBuf::from_iter([&tenant, ALERTS_ROOT_DIRECTORY]);
            let alerts = self
                .storage
                .get_objects(
                    Some(&alerts_path),
                    Box::new(|file_name| {
                        !file_name.starts_with("alert_state_") && file_name.ends_with(".json")
                    }),
                    &Some(tenant.clone()),
                )
                .await?;
            if tenant.is_empty() {
                tenant.clone_from(&DEFAULT_TENANT.to_string());
            }
            all_alerts.insert(tenant, alerts);
        }
        Ok(all_alerts)
    }

    /// This function puts an alert in the object store at the given path
    async fn put_alert(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let id = Ulid::from_string(&obj.get_object_id()).map_err(|e| MetastoreError::Error {
            status_code: StatusCode::BAD_REQUEST,
            message: e.to_string(),
            flow: "put_alert".into(),
        })?;
        let path = alert_json_path(id, tenant_id);

        Ok(self
            .storage
            .put_object(&path, to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete an alert
    async fn delete_alert(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path), tenant_id)
            .await?)
    }

    /// alerts state
    async fn get_alert_states(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<AlertStateEntry>, MetastoreError> {
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let base_path = RelativePathBuf::from_iter([tenant, ALERTS_ROOT_DIRECTORY]);
        let alert_state_bytes = self
            .storage
            .get_objects(
                Some(&base_path),
                Box::new(|file_name| {
                    file_name.starts_with("alert_state_") && file_name.ends_with(".json")
                }),
                tenant_id,
            )
            .await?;

        let mut alert_states = Vec::new();
        for bytes in alert_state_bytes {
            if let Ok(entry) = serde_json::from_slice::<AlertStateEntry>(&bytes) {
                alert_states.push(entry);
            }
        }

        Ok(alert_states)
    }

    async fn get_alert_state_entry(
        &self,
        alert_id: &Ulid,
        tenant_id: &Option<String>,
    ) -> Result<Option<AlertStateEntry>, MetastoreError> {
        let path = alert_state_json_path(*alert_id);
        match self.storage.get_object(&path, tenant_id).await {
            Ok(bytes) => {
                if let Ok(entry) = serde_json::from_slice::<AlertStateEntry>(&bytes) {
                    Ok(Some(entry))
                } else {
                    Ok(None)
                }
            }
            Err(ObjectStorageError::NoSuchKey(_)) => Ok(None),
            Err(e) => Err(MetastoreError::ObjectStorageError(e)),
        }
    }

    async fn put_alert_state(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let id = Ulid::from_string(&obj.get_object_id()).map_err(|e| MetastoreError::Error {
            status_code: StatusCode::BAD_REQUEST,
            message: e.to_string(),
            flow: "put_alert_state".into(),
        })?;
        let path = alert_state_json_path(id);

        // Parse the new state entry from the MetastoreObject
        let new_state_entry: AlertStateEntry = serde_json::from_slice(&to_bytes(obj))?;
        let new_state = new_state_entry
            .current_state()
            .ok_or_else(|| MetastoreError::InvalidJsonStructure {
                expected: "AlertStateEntry with at least one state".to_string(),
                found: "AlertStateEntry with empty states".to_string(),
            })?
            .state;

        // Try to read and parse existing file
        if let Ok(existing_bytes) = self.storage.get_object(&path, tenant_id).await {
            // File exists - try to parse and update
            if let Ok(mut existing_entry) =
                serde_json::from_slice::<AlertStateEntry>(&existing_bytes)
            {
                // Update the state and only save if it actually changed
                let state_changed = existing_entry.update_state(new_state);

                if state_changed {
                    let updated_bytes = serde_json::to_vec(&existing_entry)
                        .map_err(MetastoreError::JsonParseError)?;

                    self.storage
                        .put_object(&path, updated_bytes.into(), tenant_id)
                        .await?;
                }
                return Ok(());
            }
        }

        // Create and save new entry (either file didn't exist or parsing failed)
        let new_entry = AlertStateEntry::new(id, new_state);
        let new_bytes = serde_json::to_vec(&new_entry).map_err(MetastoreError::JsonParseError)?;

        self.storage
            .put_object(&path, new_bytes.into(), tenant_id)
            .await?;

        Ok(())
    }

    /// Delete an alert state file
    async fn delete_alert_state(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path), tenant_id)
            .await?)
    }

    /// Get MTTR history from storage
    async fn get_mttr_history(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Option<MTTRHistory>, MetastoreError> {
        let path = mttr_json_path(tenant_id);
        match self.storage.get_object(&path, tenant_id).await {
            Ok(bytes) => {
                if let Ok(mut history) = serde_json::from_slice::<MTTRHistory>(&bytes) {
                    if let Some(tenant) = tenant_id.as_ref() {
                        history.tenant_id = Some(tenant.clone());
                    }
                    Ok(Some(history))
                } else {
                    Ok(None)
                }
            }
            Err(ObjectStorageError::NoSuchKey(_)) => Ok(None),
            Err(e) => Err(MetastoreError::ObjectStorageError(e)),
        }
    }

    /// Put MTTR history to storage
    async fn put_mttr_history(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = RelativePathBuf::from(obj.get_object_path());
        Ok(self
            .storage
            .put_object(&path, to_bytes(obj), tenant_id)
            .await?)
    }

    /// This function fetches all the llmconfigs from the underlying object store
    async fn get_llmconfigs(&self) -> Result<HashMap<String, Vec<Bytes>>, MetastoreError> {
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        let mut all_configs = HashMap::new();
        for mut tenant in base_paths {
            let base_path =
                RelativePathBuf::from_iter([&tenant, SETTINGS_ROOT_DIRECTORY, "llmconfigs"]);
            let conf_bytes = self
                .storage
                .get_objects(
                    Some(&base_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                    &Some(tenant.clone()),
                )
                .await?;
            if tenant.is_empty() {
                tenant.clone_from(&DEFAULT_TENANT.to_string());
            }
            all_configs.insert(tenant, conf_bytes);
        }
        Ok(all_configs)
    }

    /// This function puts an llmconfig in the object store at the given path
    async fn put_llmconfig(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete an llmconfig
    async fn delete_llmconfig(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path), tenant_id)
            .await?)
    }

    /// Fetch all dashboards
    async fn get_dashboards(&self) -> Result<HashMap<String, Vec<Bytes>>, MetastoreError> {
        let mut dashboards = HashMap::new();
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        for mut tenant in base_paths {
            let tenant_id = &Some(tenant.clone());
            let users_dir = RelativePathBuf::from_iter([&tenant, USERS_ROOT_DIR]);
            for user in self
                .storage
                .list_dirs_relative(&users_dir, tenant_id)
                .await?
            {
                let dashboards_path = users_dir.join(&user).join("dashboards");
                let dashboard_bytes = self
                    .storage
                    .get_objects(
                        Some(&dashboards_path),
                        Box::new(|file_name| file_name.ends_with(".json")),
                        tenant_id,
                    )
                    .await?;
                if tenant.is_empty() {
                    tenant.clone_from(&DEFAULT_TENANT.to_string());
                }
                dashboards.insert(tenant.to_owned(), dashboard_bytes);
                // dashboards.extend(dashboard_bytes);
            }
        }

        Ok(dashboards)
    }

    /// Save a dashboard
    async fn put_dashboard(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete a dashboard
    async fn delete_dashboard(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path), tenant_id)
            .await?)
    }

    /// Fetch all chats
    async fn get_chats(
        &self,
    ) -> Result<DashMap<String, DashMap<String, Vec<Bytes>>>, MetastoreError> {
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        let all_chats = DashMap::new();
        for tenant in base_paths {
            let all_user_chats = DashMap::new();
            let tenant_id = if tenant.is_empty() {
                None
            } else {
                Some(tenant.clone())
            };
            let users_dir = RelativePathBuf::from_iter([&tenant, USERS_ROOT_DIR]);
            for user in self
                .storage
                .list_dirs_relative(&users_dir, &tenant_id)
                .await?
            {
                if user.starts_with(".") {
                    continue;
                }
                let mut chats = Vec::new();
                let chats_path = users_dir.join(&user).join("chats");
                let user_chats = self
                    .storage
                    .get_objects(
                        Some(&chats_path),
                        Box::new(|file_name| file_name.ends_with(".json")),
                        &tenant_id,
                    )
                    .await?;
                for chat in user_chats {
                    chats.push(chat);
                }

                all_user_chats.insert(user, chats);
            }
            all_chats.insert(
                tenant_id.as_deref().unwrap_or(DEFAULT_TENANT).into(),
                all_user_chats,
            );
        }
        Ok(all_chats)
    }

    /// Save a chat
    async fn put_chat(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete a chat
    async fn delete_chat(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path), tenant_id)
            .await?)
    }

    // for get filters, take care of migration and removal of incorrect/old filters
    // return deserialized filter
    async fn get_filters(&self) -> Result<HashMap<String, Vec<Filter>>, MetastoreError> {
        let mut this = HashMap::new();
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);

        for mut tenant in base_paths {
            let users_dir = RelativePathBuf::from_iter([&tenant, USERS_ROOT_DIR]);
            let tenant_id = &Some(tenant.clone());
            let mut filters = Vec::new();
            for user in self
                .storage
                .list_dirs_relative(&users_dir, tenant_id)
                .await?
            {
                let stream_dir = users_dir.join(&user).join("filters");

                for stream in self
                    .storage
                    .list_dirs_relative(&stream_dir, tenant_id)
                    .await?
                {
                    let filters_path = stream_dir.join(&stream);

                    // read filter object
                    let filter_bytes = self
                        .storage
                        .get_objects(
                            Some(&filters_path),
                            Box::new(|file_name| file_name.ends_with(".json")),
                            tenant_id,
                        )
                        .await?;

                    for filter in filter_bytes {
                        // deserialize into Value
                        let mut filter_value =
                            serde_json::from_slice::<serde_json::Value>(&filter)?;

                        if let Some(meta) = filter_value.clone().as_object() {
                            let version = meta.get("version").and_then(|version| version.as_str());

                            if version == Some("v1") {
                                // delete older version of the filter
                                self.storage.delete_object(&filters_path, tenant_id).await?;

                                filter_value = migrate_v1_v2(filter_value);
                                let user_id = filter_value
                                    .as_object()
                                    .unwrap()
                                    .get("user_id")
                                    .and_then(|user_id| user_id.as_str());
                                let filter_id = filter_value
                                    .as_object()
                                    .unwrap()
                                    .get("filter_id")
                                    .and_then(|filter_id| filter_id.as_str());
                                let stream_name = filter_value
                                    .as_object()
                                    .unwrap()
                                    .get("stream_name")
                                    .and_then(|stream_name| stream_name.as_str());

                                // if these values are present, create a new file
                                if let (Some(user_id), Some(stream_name), Some(filter_id)) =
                                    (user_id, stream_name, filter_id)
                                {
                                    let path = filter_path(
                                        user_id,
                                        stream_name,
                                        &format!("{filter_id}.json"),
                                    );
                                    let filter_bytes = to_bytes(&filter_value);
                                    self.storage
                                        .put_object(&path, filter_bytes.clone(), tenant_id)
                                        .await?;
                                }
                            }

                            if let Ok(filter) = serde_json::from_value::<Filter>(filter_value) {
                                filters.retain(|f: &Filter| f.filter_id != filter.filter_id);
                                filters.push(filter);
                            }
                        }
                    }
                }
            }
            if tenant.is_empty() {
                tenant.clone_from(&DEFAULT_TENANT.to_string());
            }
            this.insert(tenant, filters);
        }

        Ok(this)
    }

    /// Save a filter
    async fn put_filter(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete a filter
    async fn delete_filter(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();

        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path), tenant_id)
            .await?)
    }

    /// Get all correlations
    async fn get_correlations(&self) -> Result<HashMap<String, Vec<Bytes>>, MetastoreError> {
        let mut correlations = HashMap::new();
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        for mut tenant in base_paths {
            let tenant_id = &Some(tenant.clone());
            let mut corrs = Vec::new();
            let users_dir = RelativePathBuf::from_iter([&tenant, USERS_ROOT_DIR]);
            for user in self
                .storage
                .list_dirs_relative(&users_dir, tenant_id)
                .await?
            {
                let correlations_path = users_dir.join(&user).join("correlations");
                let correlation_bytes = self
                    .storage
                    .get_objects(
                        Some(&correlations_path),
                        Box::new(|file_name| file_name.ends_with(".json")),
                        tenant_id,
                    )
                    .await?;

                corrs.extend(correlation_bytes);
            }
            if tenant.is_empty() {
                tenant.clone_from(&DEFAULT_TENANT.to_string());
            }
            correlations.insert(tenant, corrs);
        }
        Ok(correlations)
    }

    /// Save a correlation
    async fn put_correlation(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj), tenant_id)
            .await?)
    }

    /// Delete a correlation
    async fn delete_correlation(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();

        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path), tenant_id)
            .await?)
    }

    /// Fetch an `ObjectStoreFormat` file
    ///
    /// If `get_base` is true, get the one at the base of the stream directory else depends on Mode
    async fn get_stream_json(
        &self,
        stream_name: &str,
        get_base: bool,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, MetastoreError> {
        let tenant = tenant_id.as_deref().unwrap_or("");
        let path = if get_base {
            RelativePathBuf::from_iter([
                tenant,
                stream_name,
                STREAM_ROOT_DIRECTORY,
                STREAM_METADATA_FILE_NAME,
            ])
        } else {
            stream_json_path(stream_name, tenant_id)
        };
        Ok(self.storage.get_object(&path, tenant_id).await?)
    }

    /// Fetch all `ObjectStoreFormat` present in a stream folder
    async fn get_all_stream_jsons(
        &self,
        stream_name: &str,
        mode: Option<Mode>,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Bytes>, MetastoreError> {
        let root = tenant_id.as_deref().unwrap_or("");
        let path = RelativePathBuf::from_iter([root, stream_name, STREAM_ROOT_DIRECTORY]);
        if let Some(mode) = mode {
            if mode.eq(&Mode::Ingest) {
                Ok(self
                    .storage
                    .get_objects(
                        Some(&path),
                        Box::new(|file_name| {
                            file_name.starts_with(".ingestor") && file_name.ends_with("stream.json")
                        }),
                        tenant_id,
                    )
                    .await?)
            } else {
                return Err(MetastoreError::Error {
                    status_code: StatusCode::BAD_REQUEST,
                    message: "Incorrect server mode passed as input. Only `Ingest` is allowed."
                        .into(),
                    flow: "get_all_streams with mode".into(),
                });
            }
        } else {
            Ok(self
                .storage
                .get_objects(
                    Some(&path),
                    Box::new(|file_name| file_name.ends_with("stream.json")),
                    tenant_id,
                )
                .await?)
        }
    }

    /// Save an `ObjectStoreFormat` file
    async fn put_stream_json(
        &self,
        obj: &dyn MetastoreObject,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = stream_json_path(stream_name, tenant_id);

        Ok(self
            .storage
            .put_object(&path, to_bytes(obj), tenant_id)
            .await?)
    }

    /// Fetch all `Manifest` files
    async fn get_all_manifest_files(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<BTreeMap<String, Vec<Manifest>>, MetastoreError> {
        let mut result_file_list: BTreeMap<String, Vec<Manifest>> = BTreeMap::new();
        let root = if let Some(tenant) = tenant_id {
            format!("{tenant}/{stream_name}")
        } else {
            stream_name.into()
        };
        let resp = self.storage.list_with_delimiter(Some(root.into())).await?;

        let dates = resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .filter(|name| name.as_ref() != stream_name && name.as_ref() != STREAM_ROOT_DIRECTORY)
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>();

        for date in dates {
            let date_path = object_store::path::Path::from(format!("{}/{}", stream_name, &date));
            let resp = self.storage.list_with_delimiter(Some(date_path)).await?;

            let manifest_paths: Vec<String> = resp
                .objects
                .iter()
                .filter(|name| name.location.filename().unwrap().ends_with("manifest.json"))
                .map(|name| name.location.to_string())
                .collect();

            for path in manifest_paths {
                let bytes = self
                    .storage
                    .get_object(&RelativePathBuf::from(path), tenant_id)
                    .await?;

                result_file_list
                    .entry(date.clone())
                    .or_default()
                    .push(serde_json::from_slice::<Manifest>(&bytes)?);
            }
        }
        Ok(result_file_list)
    }

    /// Fetch a specific `Manifest` file
    async fn get_manifest(
        &self,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
        manifest_url: Option<String>,
        tenant_id: &Option<String>,
    ) -> Result<Option<Manifest>, MetastoreError> {
        let path = match manifest_url {
            Some(url) => RelativePathBuf::from(url),
            None => {
                let path = partition_path(stream_name, lower_bound, upper_bound, tenant_id);
                manifest_path(path.as_str())
            }
        };
        match self.storage.get_object(&path, tenant_id).await {
            Ok(bytes) => {
                let manifest = serde_json::from_slice(&bytes)?;
                Ok(Some(manifest))
            }
            Err(ObjectStorageError::NoSuchKey(_)) => Ok(None),
            Err(err) => Err(MetastoreError::ObjectStorageError(err)),
        }
        // let path = partition_path(stream_name, lower_bound, upper_bound);
        // // // need a 'ends with `manifest.json` condition here'
        // // let obs = self
        // //     .storage
        // //     .get_objects(
        // //         path,
        // //         Box::new(|file_name| file_name.ends_with("manifest.json")),
        // //     )
        // //     .await?;
        // warn!(partition_path=?path);
        // let path = manifest_path(path.as_str());
        // warn!(manifest_path=?path);
    }

    /// Get the path for a specific `Manifest` file
    async fn get_manifest_path(
        &self,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
        tenant_id: &Option<String>,
    ) -> Result<String, MetastoreError> {
        let path = partition_path(stream_name, lower_bound, upper_bound, tenant_id);
        Ok(self
            .storage
            .absolute_url(&manifest_path(path.as_str()))
            .to_string())
    }

    async fn put_manifest(
        &self,
        obj: &dyn MetastoreObject,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let manifest_file_name = manifest_path("").to_string();
        let path = partition_path(stream_name, lower_bound, upper_bound, tenant_id)
            .join(&manifest_file_name);

        Ok(self
            .storage
            .put_object(&path, to_bytes(obj), tenant_id)
            .await?)
    }

    async fn delete_manifest(
        &self,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let manifest_file_name = manifest_path("").to_string();
        let path = partition_path(stream_name, lower_bound, upper_bound, tenant_id)
            .join(&manifest_file_name);
        Ok(self.storage.delete_object(&path, tenant_id).await?)
    }

    /// targets
    async fn get_targets(&self) -> Result<HashMap<String, Vec<Target>>, MetastoreError> {
        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        let mut all_targets = HashMap::new();
        for mut tenant in base_paths {
            let targets_path = RelativePathBuf::from_iter([
                &tenant,
                SETTINGS_ROOT_DIRECTORY,
                TARGETS_ROOT_DIRECTORY,
            ]);
            let targets = self
                .storage
                .get_objects(
                    Some(&targets_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                    &Some(tenant.clone()),
                )
                .await?
                .iter()
                .filter_map(|bytes| {
                    serde_json::from_slice(bytes)
                        .inspect_err(|err| warn!("Expected compatible json, error = {err}"))
                        .ok()
                })
                .collect();
            if tenant.is_empty() {
                tenant.clone_from(&DEFAULT_TENANT.to_string());
            }
            all_targets.insert(tenant, targets);
        }
        Ok(all_targets)
    }

    async fn put_target(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj), tenant_id)
            .await?)
    }

    async fn delete_target(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path), tenant_id)
            .await?)
    }

    async fn get_all_schemas(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Schema>, MetastoreError> {
        let path_prefix = if let Some(tenant) = tenant_id {
            relative_path::RelativePathBuf::from(format!(
                "{tenant}/{stream_name}/{STREAM_ROOT_DIRECTORY}"
            ))
        } else {
            relative_path::RelativePathBuf::from(format!("{stream_name}/{STREAM_ROOT_DIRECTORY}"))
        };
        Ok(self
            .storage
            .get_objects(
                Some(&path_prefix),
                Box::new(|file_name: String| file_name.contains(".schema")),
                tenant_id,
            )
            .await?
            .iter()
            // we should be able to unwrap as we know the data is valid schema
            .map(|byte_obj| {
                serde_json::from_slice(byte_obj)
                    .unwrap_or_else(|_| panic!("got an invalid schema for stream: {stream_name}"))
            })
            .collect())
    }

    async fn get_schema(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, MetastoreError> {
        Ok(self
            .storage
            .get_object(&schema_path(stream_name, tenant_id), tenant_id)
            .await?)
    }

    async fn put_schema(
        &self,
        obj: Schema,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = schema_path(stream_name, tenant_id);
        Ok(self
            .storage
            .put_object(&path, to_bytes(&obj), tenant_id)
            .await?)
    }

    async fn get_parseable_metadata(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Option<Bytes>, MetastoreError> {
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            RelativePathBuf::from_iter([tenant_id, ".parseable.json"])
        } else {
            parseable_json_path()
        };

        let parseable_metadata: Option<Bytes> =
            match self.storage.get_object(&path, tenant_id).await {
                Ok(bytes) => Some(bytes),
                Err(err) => {
                    if matches!(err, ObjectStorageError::NoSuchKey(_)) {
                        None
                    } else {
                        return Err(MetastoreError::ObjectStorageError(err));
                    }
                }
            };

        Ok(parseable_metadata)
    }

    async fn delete_tenant(&self, tenant_id: &str) -> Result<(), MetastoreError> {
        self.storage
            .delete_prefix(&RelativePathBuf::from(tenant_id), &None)
            .await
            .map_err(MetastoreError::ObjectStorageError)
    }

    async fn get_ingestor_metadata(&self) -> Result<Vec<Bytes>, MetastoreError> {
        let base_path = RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY);
        Ok(self
            .storage
            .get_objects(
                Some(&base_path),
                Box::new(|file_name| file_name.starts_with("ingestor")),
                &None,
            )
            .await?)
    }

    async fn put_parseable_metadata(
        &self,
        obj: &dyn MetastoreObject,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let path = if let Some(tenant_id) = tenant_id.as_ref() {
            RelativePathBuf::from_iter([tenant_id, ".parseable.json"])
        } else {
            parseable_json_path()
        };

        self.storage
            .put_object(&path, to_bytes(obj), tenant_id)
            .await
            .map_err(MetastoreError::ObjectStorageError)
    }

    async fn get_node_metadata(
        &self,
        node_type: NodeType,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Bytes>, MetastoreError> {
        let root_path = RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY);
        let prefix_owned = node_type.to_string();

        let metadata = self
            .storage
            .get_objects(
                Some(&root_path),
                Box::new(move |file_name| file_name.starts_with(&prefix_owned)), // Use the owned copy
                tenant_id,
            )
            .await?
            .into_iter()
            .collect();

        Ok(metadata)
    }

    async fn put_node_metadata(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        self.storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj), &None)
            .await?;
        Ok(())
    }

    async fn delete_node_metadata(
        &self,
        domain_name: &str,
        node_type: NodeType,
    ) -> Result<bool, MetastoreError> {
        let metadatas = self
            .storage
            .get_objects(
                Some(&RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY)),
                Box::new(move |file_name| file_name.starts_with(&node_type.to_string())),
                &None,
            )
            .await?;

        let node_metadatas = metadatas
            .iter()
            .filter_map(|elem| match serde_json::from_slice::<NodeMetadata>(elem) {
                Ok(meta) if meta.domain_name() == domain_name => Some(meta),
                _ => None,
            })
            .collect::<Vec<_>>();

        if node_metadatas.is_empty() {
            return Ok(false);
        }

        let node_meta_filename = node_metadatas[0].file_path().to_string();
        let file = RelativePathBuf::from(&node_meta_filename);

        match self.storage.delete_object(&file, &None).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if matches!(err, ObjectStorageError::IoError(_)) {
                    Ok(false)
                } else {
                    Err(MetastoreError::ObjectStorageError(err))
                }
            }
        }
    }

    async fn list_streams(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<HashSet<String>, MetastoreError> {
        // using LocalFS list_streams because it doesn't implement list_with_delimiter
        if PARSEABLE.storage.name() == "drive" {
            PARSEABLE
                .storage
                .get_object_store()
                .list_streams()
                .await
                .map_err(MetastoreError::ObjectStorageError)
        } else {
            // not local-disk, object storage
            let mut result_file_list = HashSet::new();
            let root = tenant_id
                .as_ref()
                .map(|tenant| object_store::path::Path::from_iter([tenant.clone()]));

            let resp = self.storage.list_with_delimiter(root.clone()).await?;

            let streams = resp
                .common_prefixes
                .iter()
                .flat_map(|path| path.parts())
                .map(|name| name.as_ref().to_string())
                .filter(|name| {
                    name != PARSEABLE_ROOT_DIRECTORY
                        && name != USERS_ROOT_DIR
                        && name != SETTINGS_ROOT_DIRECTORY
                        && name != ALERTS_ROOT_DIRECTORY
                })
                .collect::<Vec<_>>();
            for stream in streams {
                let stream_path = if let Some(root) = root.as_ref() {
                    object_store::path::Path::from_iter([
                        root.as_ref(),
                        &stream,
                        STREAM_ROOT_DIRECTORY,
                    ])
                } else {
                    object_store::path::Path::from(format!("{}/{}", &stream, STREAM_ROOT_DIRECTORY))
                };
                let resp = self.storage.list_with_delimiter(Some(stream_path)).await?;
                if resp
                    .objects
                    .iter()
                    .any(|name| name.location.filename().unwrap().ends_with("stream.json"))
                {
                    result_file_list.insert(stream);
                }
            }
            Ok(result_file_list)
        }
    }
}
