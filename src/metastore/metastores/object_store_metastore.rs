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
    metastore::{
        MetastoreError,
        metastore_traits::{Metastore, MetastoreObject},
    },
    storage::{
        ObjectStorage,
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

    async fn create_object(
        &self,
        obj: &dyn MetastoreObject,
        path: &str,
    ) -> Result<(), MetastoreError> {
        // use the path provided
        // pass it to storage
        // write the object

        Ok(self
            .storage
            .put_object(
                &alert_json_path(Ulid::from_string(path).unwrap()),
                to_bytes(obj),
            )
            .await?)
    }
    async fn update_object(
        &self,
        obj: &dyn MetastoreObject,
        path: &str,
    ) -> Result<(), MetastoreError> {
        Ok(self
            .storage
            .put_object(
                &alert_json_path(Ulid::from_string(path).unwrap()),
                to_bytes(obj),
            )
            .await?)
    }
    async fn delete_object(&self, path: &str) -> Result<(), MetastoreError> {
        Ok(self
            .storage
            .delete_object(&RelativePathBuf::from(path))
            .await?)
    }
}
