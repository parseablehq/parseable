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

use std::sync::Arc;

use bytes::Bytes;
use relative_path::RelativePathBuf;
use tonic::async_trait;
use ulid::Ulid;

use crate::{
    handlers::http::users::USERS_ROOT_DIR,
    metastore::{
        MetastoreError,
        metastore_traits::{Metastore, MetastoreObject},
    },
    storage::{
        ALERTS_ROOT_DIRECTORY, ObjectStorage,
        object_storage::{alert_json_path, to_bytes},
    },
};

/// Using PARSEABLE's storage as a metastore (default)
#[derive(Debug)]
pub struct ObjectStoreMetastore {
    pub storage: Arc<dyn ObjectStorage>,
}

#[async_trait]
impl Metastore for ObjectStoreMetastore {
    async fn initiate_connection(&self) -> Result<(), MetastoreError> {
        unimplemented!()
    }
    async fn list_objects(&self) -> Result<(), MetastoreError> {
        unimplemented!()
    }
    async fn get_object(&self) -> Result<(), MetastoreError> {
        unimplemented!()
    }

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
        let path = alert_json_path(Ulid::from_string(&obj.get_id()).unwrap());

        Ok(self.storage.put_object(&path, to_bytes(obj)).await?)
    }

    async fn delete_alert(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }

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

    async fn put_dashboard(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        // we need the path to store in obj store
        let path = obj.get_path();

        Ok(self
            .storage
            .put_object(&RelativePathBuf::from(path), to_bytes(obj))
            .await?)
    }

    async fn delete_dashboard(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        let path = obj.get_path();
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }

    async fn get_correlations(&self) -> Result<Vec<Bytes>, MetastoreError> {
        unimplemented!()
    }

    async fn put_correlation(&self, _obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        unimplemented!()
    }

    async fn delete_correlation(&self, _obj: &dyn MetastoreObject) -> Result<(), MetastoreError> {
        unimplemented!()
    }

    async fn delete_object(&self, path: &str) -> Result<(), MetastoreError> {
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }
}
