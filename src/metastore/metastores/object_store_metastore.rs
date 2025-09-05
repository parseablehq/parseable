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

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use arrow_schema::Schema;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::StatusCode;
use relative_path::RelativePathBuf;
use tonic::async_trait;
use tracing::warn;
use ulid::Ulid;

use crate::{
    alerts::target::Target,
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
    parseable::PARSEABLE,
    storage::{
        ALERTS_ROOT_DIRECTORY, ObjectStorage, ObjectStorageError, PARSEABLE_ROOT_DIRECTORY,
        SETTINGS_ROOT_DIRECTORY, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
        TARGETS_ROOT_DIRECTORY,
        object_storage::{
            alert_json_path, filter_path, manifest_path, parseable_json_path, schema_path,
            stream_json_path, to_bytes,
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

    /// Might implement later
    async fn list_objects(&self) -> Result<(), MetastoreError> {
        unimplemented!()
    }

    /// Might implement later
    async fn get_object(&self) -> Result<(), MetastoreError> {
        unimplemented!()
    }

    /// Fetch mutiple .json objects
    async fn get_objects(&self, parent_path: &str) -> Result<Vec<Bytes>, MetastoreError> {
        Ok(self
            .storage
            .get_objects(
                Some(&RelativePathBuf::from(parent_path)),
                Box::new(|file_name| file_name.ends_with(".json")),
            )
            .await?)
    }

    /// This function fetches all the alerts from the underlying object store
    async fn get_alerts(&self) -> Result<Vec<Bytes>, MetastoreError> {
        let alerts_path = RelativePathBuf::from(ALERTS_ROOT_DIRECTORY);
        let alerts = self
            .storage
            .get_objects(
                Some(&alerts_path),
                Box::new(|file_name| file_name.ends_with(".json")),
            )
            .await?;

        Ok(alerts)
    }

    /// This function puts an alert in the object store at the given path
    async fn put_alert(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = alert_json_path(Ulid::from_string(&obj.get_object_id()).unwrap());

        Ok(self.storage.put_object(&path, to_bytes(obj)).await?)
    }

    /// Delete an alert
    async fn delete_alert(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }

    /// Fetch all dashboards
    async fn get_dashboards(&self) -> Result<Vec<Bytes>, MetastoreError> {
        let mut dashboards = Vec::new();

        let users_dir = RelativePathBuf::from(USERS_ROOT_DIR);
        for user in self.storage.list_dirs_relative(&users_dir).await? {
            let dashboards_path = users_dir.join(&user).join("dashboards");
            let dashboard_bytes = self
                .storage
                .get_objects(
                    Some(&dashboards_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                )
                .await?;

            dashboards.extend(dashboard_bytes);
        }

        Ok(dashboards)
    }

    /// Save a dashboard
    async fn put_dashboard(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj))
            .await?)
    }

    /// Delete a dashboard
    async fn delete_dashboard(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }

    // for get filters, take care of migration and removal of incorrect/old filters
    // return deserialized filter
    async fn get_filters(&self) -> Result<Vec<Filter>, MetastoreError> {
        let mut this = Vec::new();

        let users_dir = RelativePathBuf::from(USERS_ROOT_DIR);

        for user in self.storage.list_dirs_relative(&users_dir).await? {
            let stream_dir = users_dir.join(&user).join("filters");

            for stream in self.storage.list_dirs_relative(&stream_dir).await? {
                let filters_path = stream_dir.join(&stream);

                // read filter object
                let filter_bytes = self
                    .storage
                    .get_objects(
                        Some(&filters_path),
                        Box::new(|file_name| file_name.ends_with(".json")),
                    )
                    .await?;

                for filter in filter_bytes {
                    // deserialize into Value
                    let mut filter_value = serde_json::from_slice::<serde_json::Value>(&filter)?;

                    if let Some(meta) = filter_value.clone().as_object() {
                        let version = meta.get("version").and_then(|version| version.as_str());

                        if version == Some("v1") {
                            // delete older version of the filter
                            self.storage.delete_object(&filters_path).await?;

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
                                let path =
                                    filter_path(user_id, stream_name, &format!("{filter_id}.json"));
                                let filter_bytes = to_bytes(&filter_value);
                                self.storage.put_object(&path, filter_bytes.clone()).await?;
                            }
                        }

                        if let Ok(filter) = serde_json::from_value::<Filter>(filter_value) {
                            this.retain(|f: &Filter| f.filter_id != filter.filter_id);
                            this.push(filter);
                        }
                    }
                }
            }
        }

        Ok(this)
    }

    /// Save a filter
    async fn put_filter(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj))
            .await?)
    }

    /// Delete a filter
    async fn delete_filter(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();

        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }

    /// Get all correlations
    async fn get_correlations(&self) -> Result<Vec<Bytes>, MetastoreError> {
        let mut correlations = Vec::new();

        let users_dir = RelativePathBuf::from(USERS_ROOT_DIR);
        for user in self.storage.list_dirs_relative(&users_dir).await? {
            let correlations_path = users_dir.join(&user).join("correlations");
            let correlation_bytes = self
                .storage
                .get_objects(
                    Some(&correlations_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                )
                .await?;

            correlations.extend(correlation_bytes);
        }

        Ok(correlations)
    }

    /// Save a correlation
    async fn put_correlation(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj))
            .await?)
    }

    /// Delete a correlation
    async fn delete_correlation(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();

        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }

    /// Fetch an `ObjectStoreFormat` file
    ///
    /// If `get_base` is true, get the one at the base of the stream directory else depends on Mode
    async fn get_stream_json(
        &self,
        stream_name: &str,
        get_base: bool,
    ) -> Result<Bytes, MetastoreError> {
        let path = if get_base {
            RelativePathBuf::from_iter([
                stream_name,
                STREAM_ROOT_DIRECTORY,
                STREAM_METADATA_FILE_NAME,
            ])
        } else {
            stream_json_path(stream_name)
        };
        Ok(self.storage.get_object(&path).await?)
    }

    /// Fetch all `ObjectStoreFormat` present in a stream folder
    async fn get_all_stream_jsons(
        &self,
        stream_name: &str,
        mode: Option<Mode>,
    ) -> Result<Vec<Bytes>, MetastoreError> {
        let path = RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY]);
        if let Some(mode) = mode {
            if mode.eq(&Mode::Ingest) {
                Ok(self
                    .storage
                    .get_objects(
                        Some(&path),
                        Box::new(|file_name| {
                            file_name.starts_with(".ingestor") && file_name.ends_with("stream.json")
                        }),
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
                )
                .await?)
        }
    }

    /// Save an `ObjectStoreFormat` file
    async fn put_stream_json(
        &self,
        obj: &dyn MetastoreObject,
        stream_name: &str,
    ) -> Result<(), MetastoreError> {
        Ok(self
            .storage
            .put_object(&stream_json_path(stream_name), to_bytes(obj))
            .await?)
    }

    /// Fetch all `Manifest` files
    async fn get_all_manifest_files(
        &self,
        stream_name: &str,
    ) -> Result<BTreeMap<String, Vec<Manifest>>, MetastoreError> {
        let mut result_file_list: BTreeMap<String, Vec<Manifest>> = BTreeMap::new();
        let resp = self
            .storage
            .list_with_delimiter(Some(stream_name.into()))
            .await?;

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
                    .get_object(&RelativePathBuf::from(path))
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
    ) -> Result<Option<Manifest>, MetastoreError> {
        let path = partition_path(stream_name, lower_bound, upper_bound);
        let path = manifest_path(path.as_str());
        match self.storage.get_object(&path).await {
            Ok(bytes) => {
                let manifest = serde_json::from_slice(&bytes)?;
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
    ) -> Result<String, MetastoreError> {
        let path = partition_path(stream_name, lower_bound, upper_bound);
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
    ) -> Result<(), MetastoreError> {
        let manifest_file_name = manifest_path("").to_string();
        let path = partition_path(stream_name, lower_bound, upper_bound).join(&manifest_file_name);
        Ok(self.storage.put_object(&path, to_bytes(obj)).await?)
    }

    async fn delete_manifest(
        &self,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
    ) -> Result<(), MetastoreError> {
        let manifest_file_name = manifest_path("").to_string();
        let path = partition_path(stream_name, lower_bound, upper_bound).join(&manifest_file_name);
        Ok(self.storage.delete_object(&path).await?)
    }

    /// targets
    async fn get_targets(&self) -> Result<Vec<Target>, MetastoreError> {
        let targets_path =
            RelativePathBuf::from_iter([SETTINGS_ROOT_DIRECTORY, TARGETS_ROOT_DIRECTORY]);
        let targets = self
            .storage
            .get_objects(
                Some(&targets_path),
                Box::new(|file_name| file_name.ends_with(".json")),
            )
            .await?
            .iter()
            .filter_map(|bytes| {
                serde_json::from_slice(bytes)
                    .inspect_err(|err| warn!("Expected compatible json, error = {err}"))
                    .ok()
            })
            .collect();

        Ok(targets)
    }

    async fn put_target(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj))
            .await?)
    }

    async fn delete_target(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_object_path();

        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }

    async fn get_all_schemas(&self, stream_name: &str) -> Result<Vec<Schema>, MetastoreError> {
        let path_prefix =
            relative_path::RelativePathBuf::from(format!("{stream_name}/{STREAM_ROOT_DIRECTORY}"));
        Ok(self
            .storage
            .get_objects(
                Some(&path_prefix),
                Box::new(|file_name: String| file_name.contains(".schema")),
            )
            .await?
            .iter()
            // we should be able to unwrap as we know the data is valid schema
            .map(|byte_obj| serde_json::from_slice(byte_obj).expect("data is valid json"))
            .collect())
    }

    async fn get_schema(&self, stream_name: &str) -> Result<Bytes, MetastoreError> {
        Ok(self.storage.get_object(&schema_path(stream_name)).await?)
    }

    async fn put_schema(&self, obj: Schema, stream_name: &str) -> Result<(), MetastoreError> {
        let path = schema_path(stream_name);
        Ok(self.storage.put_object(&path, to_bytes(&obj)).await?)
    }

    async fn get_parseable_metadata(&self) -> Result<Option<Bytes>, MetastoreError> {
        let parseable_metadata: Option<Bytes> =
            match self.storage.get_object(&parseable_json_path()).await {
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

    async fn get_ingestor_metadata(&self) -> Result<Vec<Bytes>, MetastoreError> {
        let base_path = RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY);
        Ok(self
            .storage
            .get_objects(
                Some(&base_path),
                Box::new(|file_name| file_name.starts_with("ingestor")),
            )
            .await?)
    }

    async fn put_parseable_metadata(
        &self,
        obj: &dyn MetastoreObject,
    ) -> Result<(), MetastoreError> {
        self.storage
            .put_object(&parseable_json_path(), to_bytes(obj))
            .await
            .map_err(MetastoreError::ObjectStorageError)
    }

    async fn get_node_metadata(&self, node_type: NodeType) -> Result<Vec<Bytes>, MetastoreError> {
        let root_path = RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY);
        let prefix_owned = node_type.to_string();

        let metadata = self
            .storage
            .get_objects(
                Some(&root_path),
                Box::new(move |file_name| file_name.starts_with(&prefix_owned)), // Use the owned copy
            )
            .await?
            .into_iter()
            .collect();

        Ok(metadata)
    }

    async fn put_node_metadata(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_object_path();
        self.storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj))
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

        match self.storage.delete_object(&file).await {
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

    async fn list_streams(&self) -> Result<HashSet<String>, MetastoreError> {
        // using LocalFS list_streams because it doesn't implement list_with_delimiter
        if PARSEABLE.get_storage_mode_string() == "drive" {
            PARSEABLE
                .storage
                .get_object_store()
                .list_streams()
                .await
                .map_err(MetastoreError::ObjectStorageError)
        } else {
            let mut result_file_list = HashSet::new();
            let resp = self.storage.list_with_delimiter(None).await?;

            let streams = resp
                .common_prefixes
                .iter()
                .flat_map(|path| path.parts())
                .map(|name| name.as_ref().to_string())
                .filter(|name| name != PARSEABLE_ROOT_DIRECTORY && name != USERS_ROOT_DIR)
                .collect::<Vec<_>>();

            for stream in streams {
                let stream_path = object_store::path::Path::from(format!(
                    "{}/{}",
                    &stream, STREAM_ROOT_DIRECTORY
                ));
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
