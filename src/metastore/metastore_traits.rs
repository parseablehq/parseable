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

use crate::{metastore::MetastoreError, option::Mode, users::filters::Filter};

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

    /// alerts
    async fn get_alerts(&self) -> Result<Vec<Bytes>, MetastoreError>;
    async fn put_alert(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_alert(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// dashboards
    async fn get_dashboards(&self) -> Result<Vec<Bytes>, MetastoreError>;
    async fn put_dashboard(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_dashboard(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// filters
    async fn get_filters(&self) -> Result<Vec<Filter>, MetastoreError>;
    async fn put_filter(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_filter(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// correlations
    async fn get_correlations(&self) -> Result<Vec<Bytes>, MetastoreError>;
    async fn put_correlation(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_correlation(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// stream metadata
    /// `get_base` when set to true, will fetch the stream.json present at the base of
    /// the stream (independent of Mode of server)
    ///
    /// Otherwise the metastore will fetch whichever file is relevant to the current server mode
    async fn get_stream_json(
        &self,
        stream_name: &str,
        get_base: bool,
    ) -> Result<Bytes, MetastoreError>;
    async fn put_stream_json(
        &self,
        obj: &dyn MetastoreObject,
        stream_name: &str,
    ) -> Result<(), MetastoreError>;
    /// This function will fetch multiple stream jsons
    ///
    /// If mode is set to `Some(Ingest)`, then it will fetch all the ingestor stream jsons for the given stream
    ///
    /// If set to `None`, it will fetch all the stream jsons present in that stream
    async fn get_all_stream_jsons(
        &self,
        stream_name: &str,
        mode: Option<Mode>,
    ) -> Result<Vec<Bytes>, MetastoreError>;
    // async fn delete_correlation(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
}

/// This trait allows a struct to get treated as a Metastore Object
///
/// A metastore object can be anything like configurations, user preferences, etc. Basically
/// anything that  has a defined structure can possibly be treated as an object.
pub trait MetastoreObject: ErasedSerialize + Sync {
    fn get_object_path(&self) -> String;
    fn get_object_id(&self) -> String;
}

// This macro makes the trait dyn-compatible
erased_serde::serialize_trait_object!(MetastoreObject);
