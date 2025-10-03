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

use std::collections::{BTreeMap, HashSet};

use arrow_schema::Schema;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use erased_serde::Serialize as ErasedSerialize;
use tonic::async_trait;
use ulid::Ulid;

use crate::{
    alerts::{alert_structs::AlertStateEntry, target::Target},
    catalog::manifest::Manifest,
    handlers::http::modal::NodeType,
    metastore::MetastoreError,
    option::Mode,
    users::filters::Filter,
};

/// A metastore is a logically separated compartment to store metadata for Parseable.
///
/// Before this, the object store (be it S3, local store, azure) was being used as a metastore. With this trait, we do not
/// need different methods for different kinds of metadata.
#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    async fn initiate_connection(&self) -> Result<(), MetastoreError>;
    async fn get_objects(&self, parent_path: &str) -> Result<Vec<Bytes>, MetastoreError>;

    /// alerts
    async fn get_alerts(&self) -> Result<Vec<Bytes>, MetastoreError>;
    async fn put_alert(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_alert(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// alerts state
    async fn get_alert_states(&self) -> Result<Vec<AlertStateEntry>, MetastoreError>;
    async fn get_alert_state_entry(
        &self,
        alert_id: &Ulid,
    ) -> Result<Option<AlertStateEntry>, MetastoreError>;
    async fn put_alert_state(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_alert_state(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// llmconfig
    async fn get_llmconfigs(&self) -> Result<Vec<Bytes>, MetastoreError>;
    async fn put_llmconfig(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_llmconfig(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// targets
    async fn get_targets(&self) -> Result<Vec<Target>, MetastoreError>;
    async fn put_target(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_target(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// dashboards
    async fn get_dashboards(&self) -> Result<Vec<Bytes>, MetastoreError>;
    async fn put_dashboard(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_dashboard(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

    /// chats
    async fn get_chats(&self) -> Result<DashMap<String, Vec<Bytes>>, MetastoreError>;
    async fn put_chat(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn delete_chat(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;

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

    /// manifest
    async fn get_all_manifest_files(
        &self,
        stream_name: &str,
    ) -> Result<BTreeMap<String, Vec<Manifest>>, MetastoreError>;
    async fn get_manifest(
        &self,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
        manifest_url: Option<String>,
    ) -> Result<Option<Manifest>, MetastoreError>;
    async fn put_manifest(
        &self,
        obj: &dyn MetastoreObject,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
    ) -> Result<(), MetastoreError>;
    async fn delete_manifest(
        &self,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
    ) -> Result<(), MetastoreError>;
    async fn get_manifest_path(
        &self,
        stream_name: &str,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
    ) -> Result<String, MetastoreError>;

    /// schema
    /// This function will fetch all schemas for the given stream
    async fn get_all_schemas(&self, stream_name: &str) -> Result<Vec<Schema>, MetastoreError>;
    async fn get_schema(&self, stream_name: &str) -> Result<Bytes, MetastoreError>;
    async fn put_schema(&self, obj: Schema, stream_name: &str) -> Result<(), MetastoreError>;

    /// parseable metadata
    async fn get_parseable_metadata(&self) -> Result<Option<Bytes>, MetastoreError>;
    async fn get_ingestor_metadata(&self) -> Result<Vec<Bytes>, MetastoreError>;
    async fn put_parseable_metadata(&self, obj: &dyn MetastoreObject)
    -> Result<(), MetastoreError>;

    /// node metadata
    async fn get_node_metadata(&self, node_type: NodeType) -> Result<Vec<Bytes>, MetastoreError>;
    async fn delete_node_metadata(
        &self,
        domain_name: &str,
        node_type: NodeType,
    ) -> Result<bool, MetastoreError>;
    async fn put_node_metadata(&self, obj: &dyn MetastoreObject) -> Result<(), MetastoreError>;
    async fn list_streams(&self) -> Result<HashSet<String>, MetastoreError>;
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
