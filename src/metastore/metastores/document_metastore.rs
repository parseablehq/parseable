macro_rules! scoped_delimiter {
    (true, $s:ident, $p:expr, $t:expr) => {
        $s.storage.list_with_delimiter($p).await
    };
    (false, $s:ident, $p:expr, $t:expr) => {
        $s.storage.list_with_delimiter($p, $t).await
    };
}
macro_rules! postgres_list_streams {
    (true, $s:ident, $t:expr, $body:block) => {
        $body
    };
    (false, $s:ident, $t:expr, $body:block) => {
        Ok($s.storage.list_streams($t).await?)
    };
}
// Shared path, encoding and behavioral contract for document metastores.
macro_rules! document_metastore {
($name:ident, $storage:ty, $local_listing:tt, {$($lifecycle:tt)*}) => {
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
use futures::{StreamExt, TryStreamExt};
use relative_path::{RelativePath, RelativePathBuf};
use tonic::async_trait;
use tracing::{info, warn};
use ulid::Ulid;

use crate::{
    alerts::{
        alert_structs::{AlertStateEntry, MTTRHistory},
        outbound_http_policy::AlertTargetPolicyConfig,
        target::Target,
    },
    catalog::{
        manifest::{Manifest, decode_manifest_blocking, encode_manifest_blocking},
        partition_path,
    },
    handlers::http::{
        modal::{Metadata, NodeMetadata, NodeType},
        users::USERS_ROOT_DIR,
    },
    metastore::{
        MetastoreError,
        metastore_traits::{
            ConditionalMetadataError, MetadataVersion, MetadataWriteCondition, Metastore,
            MetastoreObject, VersionedMetadata,
        },
    },
    option::Mode,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    storage::{
        ALERTS_ROOT_DIRECTORY, ObjectStorageError, PARSEABLE_ROOT_DIRECTORY,
        SETTINGS_ROOT_DIRECTORY, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
        TARGETS_ROOT_DIRECTORY,
        object_storage::{
            alert_json_path, alert_state_json_path, filter_path, manifest_path, mttr_json_path,
            outbound_http_policy_json_path, parseable_json_path, schema_path, stream_json_path,
            to_bytes,
        },
    },
    users::filters::{Filter, FilterType, SQL_FILTER_STREAM_NAME, migrate_v1_v2},
};

/// Using PARSEABLE's storage as a metastore (default)
#[derive(Debug)]
pub struct $name {
    pub storage: Arc<$storage>,
}

impl $name {
    async fn delete_filter_path(
        &self,
        path: &RelativePathBuf,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        match self.storage.delete_object(path, tenant_id).await {
            Ok(()) => Ok(()),
            Err(error) if is_missing_optional_dir(&error) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Enumerate only this owner's ID, never a filter belonging to another user.
    async fn filter_aliases(
        &self,
        filter: &Filter,
        tenant_id: &Option<String>,
    ) -> Result<Vec<(RelativePathBuf, Filter)>, MetastoreError> {
        let owner = filter
            .user_id
            .as_deref()
            .expect("stored filters have an owner");
        let id = filter
            .filter_id
            .as_deref()
            .expect("stored filters have an ID");
        let root = filter_path(owner, "", "", tenant_id);
        let mut copies = Vec::new();
        for stream in self.storage.list_dirs_relative(&root, tenant_id).await? {
            let path = root.join(&stream).join(format!("{id}.json"));
            let bytes = match self.storage.get_object(&path, tenant_id).await {
                Ok(bytes) => bytes,
                Err(error) if is_missing_optional_dir(&error) => continue,
                Err(error) => return Err(error.into()),
            };
            let copy: Filter = serde_json::from_slice(&bytes)?;
            if copy.filter_id == filter.filter_id && copy.user_id == filter.user_id {
                copies.push((path, copy));
            }
        }
        copies.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(copies)
    }

    async fn load_tenant_filters(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Filter>, MetastoreError> {
        let users_dir =
            RelativePathBuf::from_iter([tenant_id.as_deref().filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT).unwrap_or(""), USERS_ROOT_DIR]);
        let mut grouped: BTreeMap<(String, String), Vec<(RelativePathBuf, Filter)>> =
            BTreeMap::new();
        for user in self
            .storage
            .list_dirs_relative(&users_dir, tenant_id)
            .await?
        {
            let root = users_dir.join(&user).join("filters");
            for stream in self.storage.list_dirs_relative(&root, tenant_id).await? {
                let directory = root.join(&stream);
                let objects = self
                    .storage
                    .get_objects(
                        Some(&directory),
                        Box::new(|name| name.ends_with(".json")),
                        tenant_id,
                    )
                    .await?;
                for bytes in objects {
                    let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
                    let is_v1 = value.get("version").and_then(|v| v.as_str()) == Some("v1");
                    if is_v1 {
                        value = migrate_v1_v2(value, tenant_id);
                    }
                    let Ok(mut filter) = serde_json::from_value::<Filter>(value) else {
                        continue;
                    };
                    // The discovered tenant directory is authoritative, including
                    // default-tenant records whose payload predates tenant metadata.
                    filter.tenant_id = tenant_id.clone().filter(|tenant| !tenant.is_empty());
                    let (Some(owner), Some(id)) = (&filter.user_id, &filter.filter_id) else {
                        continue;
                    };
                    // Stored identity fields must remain single path components.
                    if !safe_filter_component(owner)
                        || !safe_filter_component(id)
                        || !safe_filter_component(&filter.stream_name)
                        || filter.stream_name != stream
                        || (!is_v1 && owner != &user)
                    {
                        warn!(
                            "Ignoring saved filter whose identity does not match its storage directory"
                        );
                        continue;
                    }
                    let source = directory.join(format!("{id}.json"));
                    // get_objects returns bytes without filenames. Verify the inferred
                    // filename before any cleanup; never delete an unrelated object.
                    match self.storage.get_object(&source, tenant_id).await {
                        Ok(stored) if stored == bytes => {}
                        Ok(_) => continue,
                        Err(error) if is_missing_optional_dir(&error) => continue,
                        Err(error) => return Err(error.into()),
                    }
                    if is_v1 {
                        // Persist before removing the original so interrupted migration is safe.
                        self.put_filter(&filter, tenant_id).await?;
                        if source.as_str() != filter.get_object_path() {
                            self.delete_filter_path(&source, tenant_id).await?;
                        }
                    }
                    grouped
                        .entry((owner.clone(), id.clone()))
                        .or_default()
                        .push((
                            if is_v1 {
                                RelativePathBuf::from(filter.get_object_path())
                            } else {
                                source
                            },
                            filter,
                        ));
                }
            }
        }
        let mut filters = Vec::new();
        for (_, mut copies) in grouped {
            // A v1 migration may write a hashed-owner directory that is visited
            // later in this same scan. It is still one physical object.
            copies.sort_by(|a, b| a.0.cmp(&b.0));
            copies.dedup_by(|a, b| a.0 == b.0);
            copies.sort_by(|a, b| {
                let canonical = |filter: &Filter| {
                    filter.query.filter_type == FilterType::SQL
                        && filter.stream_name == SQL_FILTER_STREAM_NAME
                };
                canonical(&b.1).cmp(&canonical(&a.1)).then(a.0.cmp(&b.0))
            });
            let (canonical_path, filter) = &copies[0];
            if filter.query.filter_type == FilterType::SQL
                && filter.stream_name == SQL_FILTER_STREAM_NAME
            {
                for (path, alias) in copies.iter().skip(1) {
                    if path != canonical_path && alias.query.filter_type == FilterType::SQL {
                        // The canonical object is already durable. Retain it in memory
                        // even if cleanup is temporarily unavailable; retry on restart.
                        if let Err(error) = self.delete_filter_path(path, tenant_id).await {
                            warn!("Failed to clean saved SQL filter alias {path}: {error}");
                        }
                    } else if alias.query.filter_type != FilterType::SQL {
                        warn!(
                            "Retaining non-SQL saved filter copy {path} with the same owner {:?} and ID {:?} as canonical SQL filter {canonical_path}",
                            filter.user_id, filter.filter_id
                        );
                    }
                }
            }
            if copies.len() > 1
                && !(filter.query.filter_type == FilterType::SQL
                    && filter.stream_name == SQL_FILTER_STREAM_NAME)
            {
                warn!(
                    "Multiple saved filter copies for owner {:?}, ID {:?}; using deterministic path {canonical_path}",
                    filter.user_id, filter.filter_id
                );
            }
            filters.push(filter.clone());
        }
        Ok(filters)
    }
}

fn stored_filter_path(filter: &Filter, tenant_id: &Option<String>) -> RelativePathBuf {
    filter_path(
        filter
            .user_id
            .as_deref()
            .expect("stored filters have an owner"),
        &filter.stream_name,
        &format!(
            "{}.json",
            filter
                .filter_id
                .as_deref()
                .expect("stored filters have an ID")
        ),
        tenant_id,
    )
}

fn safe_filter_component(component: &str) -> bool {
    !component.is_empty()
        && component != "."
        && component != ".."
        && !component.contains(['/', '\\'])
}

pub fn is_missing_optional_dir(err: &ObjectStorageError) -> bool {
    match err {
        ObjectStorageError::NoSuchKey(_) => true,
        ObjectStorageError::IoError(err) => err.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

#[async_trait]
impl Metastore for $name {
    /// Since Parseable already starts with a connection to an object store, no need to implement this
    async fn initiate_connection(&self) -> Result<(), MetastoreError> {
        if $local_listing {
            self.storage.check_migration_guard().await?;
            Ok(())
        } else { Ok(self.storage.check(&None).await?) }
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
        let path = if let Some(tenant_id) = tenant_id.as_ref().filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT) {
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
        let path = if let Some(tenant_id) = tenant_id.as_ref().filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT) {
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
        let path = if let Some(tenant_id) = tenant_id.as_ref().filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT) {
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
        let path = if let Some(tenant_id) = tenant_id.as_ref().filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT) {
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
            let alerts = match self
                .storage
                .get_objects(
                    Some(&alerts_path),
                    Box::new(|file_name| {
                        !file_name.starts_with("alert_state_") && file_name != "mttr.json" && file_name.ends_with(".json")
                    }),
                    &Some(tenant.clone()),
                )
                .await
            {
                Ok(alerts) => alerts,
                Err(err) if is_missing_optional_dir(&err) => Vec::new(),
                Err(err) => return Err(MetastoreError::ObjectStorageError(err)),
            };
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
        let id = Ulid::from_string(&obj.get_object_id()).map_err(|e| MetastoreError::Error {
            status_code: StatusCode::BAD_REQUEST,
            message: e.to_string(),
            flow: "delete_alert".into(),
        })?;
        let path = alert_json_path(id, tenant_id);
        match self.storage.delete_object(&path, tenant_id).await {
            Ok(()) => Ok(()),
            Err(err) if is_missing_optional_dir(&err) => Ok(()),
            Err(err) => Err(MetastoreError::ObjectStorageError(err)),
        }
    }

    /// alerts state
    async fn get_alert_states(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<AlertStateEntry>, MetastoreError> {
        let tenant = tenant_id.as_deref().filter(|t| !t.is_empty() && *t != DEFAULT_TENANT).unwrap_or("");
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
        let path = alert_state_json_path(*alert_id, tenant_id);
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
        let path = alert_state_json_path(id, tenant_id);

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
        let new_entry = AlertStateEntry::new(id, new_state, tenant_id.clone());
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
        let id = Ulid::from_string(&obj.get_object_id()).map_err(|e| MetastoreError::Error {
            status_code: StatusCode::BAD_REQUEST,
            message: e.to_string(),
            flow: "delete_alert_state".into(),
        })?;
        let path = alert_state_json_path(id, tenant_id);
        match self.storage.delete_object(&path, tenant_id).await {
            Ok(()) => Ok(()),
            Err(err) if is_missing_optional_dir(&err) => Ok(()),
            Err(err) => Err(MetastoreError::ObjectStorageError(err)),
        }
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

    async fn get_dashboard_versioned(
        &self,
        path: &str,
        tenant_id: &Option<String>,
    ) -> Result<Option<VersionedMetadata>, ConditionalMetadataError> {
        self.storage
            .get_metadata_versioned(RelativePath::new(path), tenant_id)
            .await
    }

    async fn put_dashboard_conditional(
        &self,
        path: &str,
        bytes: Bytes,
        condition: MetadataWriteCondition,
        tenant_id: &Option<String>,
    ) -> Result<MetadataVersion, ConditionalMetadataError> {
        self.storage
            .put_metadata_conditional(RelativePath::new(path), bytes, condition, tenant_id)
            .await
    }

    async fn delete_dashboard_conditional(
        &self,
        path: &str,
        version: MetadataVersion,
        tenant_id: &Option<String>,
    ) -> Result<(), ConditionalMetadataError> {
        self.storage
            .delete_metadata_conditional(RelativePath::new(path), version, tenant_id)
            .await
    }

    /// Fetch all dashboards; hidden service indexes are not dashboard owners.
    async fn get_dashboards(&self) -> Result<HashMap<String, Vec<Bytes>>, MetastoreError> {
        let mut dashboards = HashMap::new();
        let tenants = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        for tenant in tenants {
            let tenant_id = if tenant.is_empty() {
                None
            } else {
                Some(tenant.clone())
            };
            let bytes = self.get_tenant_dashboards(&tenant_id).await?;
            dashboards.insert(
                tenant_id.unwrap_or_else(|| DEFAULT_TENANT.to_string()),
                bytes,
            );
        }
        Ok(dashboards)
    }

    async fn get_tenant_dashboards(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Bytes>, MetastoreError> {
        let users =
            RelativePathBuf::from_iter([tenant_id.as_deref().filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT).unwrap_or(""), USERS_ROOT_DIR]);
        let mut dashboards = Vec::new();
        for owner in self.storage.list_dirs_relative(&users, tenant_id).await? {
            if owner.starts_with('.') {
                continue;
            }
            let path = users.join(owner).join("dashboards");
            match self
                .storage
                .get_objects(
                    Some(&path),
                    Box::new(|name| name.ends_with(".json")),
                    tenant_id,
                )
                .await
            {
                Ok(bytes) => dashboards.extend(bytes),
                Err(error) if is_missing_optional_dir(&error) => {}
                Err(error) => return Err(error.into()),
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

    // Reconcile duplicate SQL paths before exposing filters to the cache.
    async fn get_filters(&self) -> Result<HashMap<String, Vec<Filter>>, MetastoreError> {
        let mut all = HashMap::new();
        let tenants = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);
        for tenant in tenants {
            let tenant_id = Some(tenant.clone());
            let filters = self.load_tenant_filters(&tenant_id).await?;
            all.insert(
                if tenant.is_empty() {
                    DEFAULT_TENANT.to_owned()
                } else {
                    tenant
                },
                filters,
            );
        }
        Ok(all)
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

    async fn reconcile_filter(
        &self,
        filter: &Filter,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let canonical = stored_filter_path(filter, tenant_id);
        for (path, _) in self.filter_aliases(filter, tenant_id).await? {
            if path != canonical {
                self.delete_filter_path(&path, tenant_id).await?;
            }
        }
        Ok(())
    }

    /// Delete aliases first. If any cleanup fails, the canonical copy remains
    /// available for a retry instead of allowing an old alias to reappear.
    async fn delete_filter(
        &self,
        filter: &Filter,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        self.reconcile_filter(filter, tenant_id).await?;
        self.delete_filter_path(&stored_filter_path(filter, tenant_id), tenant_id)
            .await
    }

    /// Fetch an `ObjectStoreFormat` file
    ///
    /// If `get_base` is true, get the one at the base of the stream directory else depends on Mode
    async fn get_stream_json(
        &self,
        stream_name: &str,
        get_base: bool,
        tenant_id: &Option<String>,
        _is_migration: bool,
    ) -> Result<Bytes, MetastoreError> {
        let tenant = tenant_id.as_deref().filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT).unwrap_or("");
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
        _is_migration: bool,
    ) -> Result<Vec<Bytes>, MetastoreError> {
        let root = tenant_id.as_deref().filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT).unwrap_or("");
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

    /// Fetch manifest files for the given date keys in parallel.
    ///
    /// Per-date work runs concurrently via `futures::future::try_join_all`
    /// so a slow LIST on one date does not block the others. Each date's
    /// timing is logged at info level so any pathologically slow dates
    /// (S3 retries, oversized prefixes) are visible without needing extra
    /// span instrumentation.
    async fn get_manifest_files_for_dates(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
        dates: &[String],
    ) -> Result<BTreeMap<String, Vec<Manifest>>, MetastoreError> {
        let inventory_limit = Arc::new(tokio::sync::Semaphore::new(16));
        let date_futures = dates.iter().map(|date| {
            let stream = stream_name.to_string();
            let tenant = tenant_id.clone();
            let date = date.clone();
            let inventory_limit = inventory_limit.clone();
            async move {
                let t_start = std::time::Instant::now();
                let date_path = if let Some(tenant) = tenant.as_ref().filter(|t| !t.is_empty() && t.as_str() != DEFAULT_TENANT) {
                    object_store::path::Path::from(format!("{}/{}/{}", tenant, stream, date))
                } else {
                    object_store::path::Path::from(format!("{}/{}", stream, date))
                };

                let t_list = std::time::Instant::now();
                let list_permit = inventory_limit
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("inventory semaphore remains open");
                let list_result = scoped_delimiter!($local_listing, self, Some(date_path), &tenant);
                drop(list_permit);
                let resp = match list_result {
                    Ok(r) => r,
                    Err(ObjectStorageError::NoSuchKey(_)) => {
                        info!(
                            stream = %stream,
                            tenant = ?tenant,
                            date = %date,
                            list_ms = t_list.elapsed().as_millis() as u64,
                            "manifest fetch: date prefix not found"
                        );
                        return Ok::<(String, Vec<Manifest>), MetastoreError>((date, Vec::new()));
                    }
                    Err(e) => return Err(e.into()),
                };
                let list_ms = t_list.elapsed().as_millis() as u64;

                let manifest_paths: Vec<String> = resp
                    .objects
                    .iter()
                    .filter(|name| name.location.filename().unwrap().ends_with("manifest.json"))
                    .map(|name| name.location.to_string())
                    .collect();
                let manifest_count = manifest_paths.len();

                let t_gets = std::time::Instant::now();
                let manifests = futures::stream::iter(manifest_paths)
                    .map(|path| {
                        let tenant = tenant.clone();
                        let inventory_limit = inventory_limit.clone();
                        async move {
                            let _permit = inventory_limit
                                .acquire_owned()
                                .await
                                .expect("inventory semaphore remains open");
                            let bytes = self
                                .storage
                                .get_object(&RelativePathBuf::from(path), &tenant)
                                .await?;
                            Ok::<Manifest, MetastoreError>(decode_manifest_blocking(bytes).await?)
                        }
                    })
                    .buffer_unordered(16)
                    .try_collect::<Vec<_>>()
                    .await?;
                let gets_ms = t_gets.elapsed().as_millis() as u64;
                let total_ms = t_start.elapsed().as_millis() as u64;

                if total_ms > 100 {
                    info!(
                        stream = %stream,
                        tenant = ?tenant,
                        date = %date,
                        list_ms,
                        gets_ms,
                        total_ms,
                        manifest_count,
                        "manifest fetch per-date timing"
                    );
                }
                Ok((date, manifests))
            }
        });

        let results: Vec<(String, Vec<Manifest>)> =
            futures::future::try_join_all(date_futures).await?;

        let mut result_file_list: BTreeMap<String, Vec<Manifest>> = BTreeMap::new();
        for (date, manifests) in results {
            if !manifests.is_empty() {
                result_file_list.insert(date, manifests);
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
                let manifest = decode_manifest_blocking(bytes).await?;
                Ok(Some(manifest))
            }
            Err(ObjectStorageError::NoSuchKey(_)) => Ok(None),
            Err(err) => Err(MetastoreError::ObjectStorageError(err)),
        }
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
        manifest: &Manifest,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
        tenant_id: &Option<String>,
    ) -> Result<(), MetastoreError> {
        let manifest_file_name = manifest_path("").to_string();
        let path = partition_path(stream_name, lower_bound, upper_bound, tenant_id)
            .join(&manifest_file_name);

        let encoded = encode_manifest_blocking(manifest.clone()).await?;

        Ok(self.storage.put_object(&path, encoded, tenant_id).await?)
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
            let target_bytes = match self
                .storage
                .get_objects(
                    Some(&targets_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                    &Some(tenant.clone()),
                )
                .await
            {
                Ok(targets) => targets,
                Err(err) if is_missing_optional_dir(&err) => Vec::new(),
                Err(err) => return Err(MetastoreError::ObjectStorageError(err)),
            };
            let targets = target_bytes
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

    async fn get_outbound_policies(
        &self,
    ) -> Result<HashMap<String, AlertTargetPolicyConfig>, MetastoreError> {
        let mut all_policies = HashMap::new();

        let base_paths = PARSEABLE.list_tenants().unwrap_or_else(|| vec!["".into()]);

        for tenant in base_paths {
            let tenant_id = if tenant.is_empty() {
                None
            } else {
                Some(tenant.clone())
            };
            let path = outbound_http_policy_json_path(&tenant_id);
            let policy = match self.storage.get_object(&path, &tenant_id).await {
                Ok(bytes) => match serde_json::from_slice::<AlertTargetPolicyConfig>(&bytes) {
                    Ok(policy) => policy,
                    Err(err) => {
                        warn!(
                            "Failed to deserialize outbound HTTP policy for tenant {tenant}: {err}"
                        );
                        continue;
                    }
                },
                Err(err) if is_missing_optional_dir(&err) => continue,
                Err(err) => return Err(MetastoreError::ObjectStorageError(err)),
            };

            all_policies.insert(tenant, policy);
        }

        Ok(all_policies)
    }

    async fn put_outbound_policy(
        &self,
        tenant_id: &Option<String>,
        policy: &AlertTargetPolicyConfig,
    ) -> Result<(), MetastoreError> {
        let path = outbound_http_policy_json_path(tenant_id);
        Ok(self
            .storage
            .put_object(&path, to_bytes(policy), tenant_id)
            .await?)
    }

    async fn get_all_schemas(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Schema>, MetastoreError> {
        let path_prefix = if let Some(tenant) = tenant_id.as_ref().filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT) {
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

    async fn refresh_schema(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
        schema_file: &str,
    ) -> Result<Schema, MetastoreError> {
        let objects = self.storage.schema_objects(stream_name, tenant_id).await?;
        let mut schemas = Vec::with_capacity(objects.len());
        let mut canonical = None;
        for (name, bytes) in objects {
            let schema: Schema = serde_json::from_slice(&bytes)
                .map_err(|e| ObjectStorageError::Custom(e.to_string()))?;
            if name == schema_file {
                canonical = Some(schema.clone());
            }
            schemas.push(schema);
        }
        let merged =
            Schema::try_merge(schemas).map_err(|e| ObjectStorageError::Custom(e.to_string()))?;
        // Field order is immaterial, but nested types, nullability and field
        // metadata all matter. Normalize only the top-level ordering.
        let normalize = |schema: &Schema| {
            let mut schema = schema.clone();
            let mut fields = schema.fields.to_vec();
            fields.sort_by(|a, b| a.name().cmp(b.name()));
            schema.fields = fields.into();
            schema
        };
        if canonical.as_ref().map(normalize) != Some(normalize(&merged)) {
            let path = RelativePathBuf::from_iter([
                tenant_id.as_deref().filter(|t| !t.is_empty() && *t != crate::parseable::DEFAULT_TENANT).unwrap_or(""),
                stream_name,
                STREAM_ROOT_DIRECTORY,
                schema_file,
            ]);
            self.storage
                .put_object(&path, to_bytes(&merged), tenant_id)
                .await?;
        }
        Ok(merged)
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
        let path = if let Some(tenant_id) = tenant_id.as_ref().filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT) {
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
        let path = if let Some(tenant_id) = tenant_id.as_ref().filter(|t| !t.is_empty() && t.as_str() != crate::parseable::DEFAULT_TENANT) {
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

    $($lifecycle)*

    async fn list_streams(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<HashSet<String>, MetastoreError> {
        postgres_list_streams!($local_listing, self, tenant_id, {
        // using LocalFS list_streams because it doesn't implement list_with_delimiter
        if $local_listing && self.storage.store_url().scheme() == "file" {
            self.storage.list_streams()
                .await
                .map_err(MetastoreError::ObjectStorageError)
        } else {
            // not local-disk, object storage
            let root = tenant_id
                .as_ref()
                .map(|tenant| object_store::path::Path::from_iter([tenant.clone()]));

            let resp = scoped_delimiter!($local_listing, self, root.clone(), tenant_id)?;

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
                        && name != crate::storage::TOMBSTONE_ROOT_DIRECTORY
                })
                .collect::<Vec<_>>();
            let results = super::document_metastore::collect_dataset_probes(
                streams.into_iter().map(|stream| {
                    let root = root.clone();
                    async move {
                        if !$local_listing && self.is_stream_deleting(&stream, tenant_id).await? {
                            return Ok::<_, MetastoreError>(None);
                        }
                        let stream_path = if let Some(root) = root.as_ref() {
                            object_store::path::Path::from_iter([
                                root.as_ref(),
                                &stream,
                                STREAM_ROOT_DIRECTORY,
                            ])
                        } else {
                            object_store::path::Path::from(format!(
                                "{}/{}",
                                stream, STREAM_ROOT_DIRECTORY
                            ))
                        };
                        let resp =
                            scoped_delimiter!($local_listing, self, Some(stream_path), tenant_id)?;
                        if resp
                            .objects
                            .iter()
                            .any(|name| name.location.filename().unwrap().ends_with("stream.json"))
                        {
                            Ok(Some(stream))
                        } else {
                            Ok(None)
                        }
                    }
                }),
            )
            .await?;
            Ok(results.into_iter().flatten().collect())
        }
            })
    }
}


};
}
pub(crate) use document_metastore;

/// Runs dataset probes sixteen at a time. All-or-error collection: a failed
/// probe cannot publish a partial inventory.
pub(crate) async fn collect_dataset_probes<T, E>(
    probes: impl IntoIterator<Item = impl std::future::Future<Output = Result<T, E>>>,
) -> Result<Vec<T>, E> {
    use futures::{StreamExt, TryStreamExt};
    futures::stream::iter(probes)
        .buffer_unordered(16)
        .try_collect()
        .await
}

/// Classification belongs to the shared document layer. Never reconstruct a key.
pub(crate) fn document_scope(key: &str) -> Option<(String, &'static str)> {
    if key == super::migration::PAIR || key == super::migration::BUSY || key.ends_with(".lock") {
        return None;
    }
    let parts: Vec<_> = key.split('/').collect();
    if key.ends_with("manifest.json") {
        let date = parts.iter().position(|p| p.starts_with("date="))?;
        return match date {
            1 => Some((String::new(), "manifest")),
            2 => Some((
                if parts[0] == crate::parseable::DEFAULT_TENANT {
                    String::new()
                } else {
                    parts[0].to_owned()
                },
                "manifest",
            )),
            _ => None,
        };
    }
    let hidden = parts.iter().position(|p| p.starts_with('.'))?;
    let tenant = match hidden {
        0 => "",
        1 if parts[1] == crate::storage::STREAM_ROOT_DIRECTORY
            || parts[1].ends_with("stream.json")
            || parts[1].contains(".schema") =>
        {
            ""
        }
        1 => parts[0],
        2 if parts[2] == crate::storage::STREAM_ROOT_DIRECTORY
            || parts[2].ends_with("stream.json")
            || parts[2].contains(".schema") =>
        {
            parts[0]
        }
        _ => return None,
    };
    Some((
        if tenant == crate::parseable::DEFAULT_TENANT {
            String::new()
        } else {
            tenant.to_owned()
        },
        super::postgres_documents::kind(relative_path::RelativePath::new(key)),
    ))
}
