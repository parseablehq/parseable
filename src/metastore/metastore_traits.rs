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

use bytes::Bytes;
use erased_serde::Serialize as ErasedSerialize;
use tonic::async_trait;

use crate::metastore::MetastoreError;

/// A metastore is a logically separated compartment to store metadata for Parseable.
///
/// Before this, the object store (be it S3, local store, azure) was being used as a metastore. With this trait, we do not
/// need different methods for different kinds of metadata.
#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    async fn initiate_connection(&self) -> Result<(), MetastoreError>;
    async fn list_objects(&self) -> Result<(), MetastoreError>;
    async fn get_object(&self) -> Result<(), MetastoreError>;
    async fn get_objects(&self, parent_path: &str) -> Result<Vec<Bytes>, MetastoreError>;
    async fn create_object(
        &self,
        obj: &dyn MetastoreObject,
        path: &str,
    ) -> Result<(), MetastoreError>;
    async fn update_object(
        &self,
        obj: &dyn MetastoreObject,
        path: &str,
    ) -> Result<(), MetastoreError>;
    async fn delete_object(&self, path: &str) -> Result<(), MetastoreError>;
}

/// This trait allows a struct to get treated as a Metastore Object
///
/// A metastore object can be anything like configurations, user preferences, etc. Basically
/// anything that  has a defined structure can possibly be treated as an object.
pub trait MetastoreObject: ErasedSerialize + Sync {
    // fn get_object(self) -> T;
}

// This macro makes the trait dyn-compatible
erased_serde::serialize_trait_object!(MetastoreObject);
