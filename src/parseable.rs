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
 *
 */

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use arrow_schema::{Field, Schema};
use bytes::Bytes;
use clap::{error::ErrorKind, Parser};
use once_cell::sync::Lazy;

use crate::{
    cli::{Cli, Options, StorageOptions},
    handlers::http::logstream::error::StreamError,
    metadata::{error::stream_info::LoadError, LogStreamMetadata, StreamInfo, LOCK_EXPECT},
    option::Mode,
    storage::{
        object_storage::parseable_json_path, ObjectStorageError, ObjectStorageProvider,
        ObjectStoreFormat,
    },
};

pub const JOIN_COMMUNITY: &str =
    "Join us on Parseable Slack community for questions : https://logg.ing/community";

/// Shared state of the Parseable server.
pub static PARSEABLE: Lazy<Parseable> = Lazy::new(|| match Cli::parse().storage {
    StorageOptions::Local(args) => {
        if args.options.local_staging_path == args.storage.root {
            clap::Error::raw(
                ErrorKind::ValueValidation,
                "Cannot use same path for storage and staging",
            )
            .exit();
        }

        if args.options.hot_tier_storage_path.is_some() {
            clap::Error::raw(
                ErrorKind::ValueValidation,
                "Cannot use hot tier with local-store subcommand.",
            )
            .exit();
        }

        Parseable::new(args.options, Arc::new(args.storage), "drive")
    }
    StorageOptions::S3(args) => Parseable::new(args.options, Arc::new(args.storage), "s3"),
    StorageOptions::Blob(args) => {
        Parseable::new(args.options, Arc::new(args.storage), "blob-store")
    }
});

/// All state related to parseable, in one place.
pub struct Parseable {
    /// Configuration variables for parseable
    pub options: Options,
    /// Storage engine backing parseable
    storage: Arc<dyn ObjectStorageProvider>,
    /// Either "drive"/"s3"/"blob-store"
    pub storage_name: &'static str,
    /// Metadata relating to logstreams
    pub streams: StreamInfo,
}

impl Parseable {
    pub fn new(
        options: Options,
        storage: Arc<dyn ObjectStorageProvider>,
        storage_name: &'static str,
    ) -> Self {
        Parseable {
            options,
            storage,
            storage_name,
            streams: StreamInfo::default(),
        }
    }

    // validate the storage, if the proper path for staging directory is provided
    // if the proper data directory is provided, or s3 bucket is provided etc
    pub async fn validate_storage(&self) -> Result<Option<Bytes>, ObjectStorageError> {
        let obj_store = self.storage.get_object_store();
        let rel_path = parseable_json_path();
        let mut has_parseable_json = false;
        let parseable_json_result = obj_store.get_object(&rel_path).await;
        if parseable_json_result.is_ok() {
            has_parseable_json = true;
        }

        // Lists all the directories in the root of the bucket/directory
        // can be a stream (if it contains .stream.json file) or not
        let has_dirs = match obj_store.list_dirs().await {
            Ok(dirs) => !dirs.is_empty(),
            Err(_) => false,
        };

        let has_streams = obj_store.list_streams().await.is_ok();
        if !has_dirs && !has_parseable_json {
            return Ok(None);
        }
        if has_streams {
            return Ok(Some(parseable_json_result.unwrap()));
        }

        if self.storage_name == "drive" {
            return Err(ObjectStorageError::Custom(format!("Could not start the server because directory '{}' contains stale data, please use an empty directory, and restart the server.\n{}", self.storage.get_endpoint(), JOIN_COMMUNITY)));
        }

        // S3 bucket mode
        Err(ObjectStorageError::Custom(format!("Could not start the server because bucket '{}' contains stale data, please use an empty bucket and restart the server.\n{}", self.storage.get_endpoint(), JOIN_COMMUNITY)))
    }

    pub fn storage(&self) -> Arc<dyn ObjectStorageProvider> {
        self.storage.clone()
    }

    pub fn staging_dir(&self) -> &PathBuf {
        &self.options.local_staging_path
    }

    pub fn hot_tier_dir(&self) -> &Option<PathBuf> {
        &self.options.hot_tier_storage_path
    }

    // returns the string representation of the storage mode
    // drive --> Local drive
    // s3 --> S3 bucket
    // azure_blob --> Azure Blob Storage
    pub fn get_storage_mode_string(&self) -> &str {
        if self.storage_name == "drive" {
            return "Local drive";
        } else if self.storage_name == "s3" {
            return "S3 bucket";
        } else if self.storage_name == "blob_store" {
            return "Azure Blob Storage";
        }
        "Unknown"
    }

    pub fn get_server_mode_string(&self) -> &str {
        match self.options.mode {
            Mode::Query => "Distributed (Query)",
            Mode::Ingest => "Distributed (Ingest)",
            Mode::All => "Standalone",
        }
    }

    /// list all streams from storage
    /// if stream exists in storage, create stream and schema from storage
    /// and add it to the memory map
    pub async fn create_stream_and_schema_from_storage(
        &self,
        stream_name: &str,
    ) -> Result<bool, StreamError> {
        // Proceed to create log stream if it doesn't exist
        let storage = self.storage.get_object_store();
        let streams = storage.list_streams().await?;
        if !streams.contains(stream_name) {
            return Ok(false);
        }

        let mut stream_metadata = ObjectStoreFormat::default();
        let stream_metadata_bytes = storage.create_stream_from_ingestor(stream_name).await?;
        if !stream_metadata_bytes.is_empty() {
            stream_metadata = serde_json::from_slice::<ObjectStoreFormat>(&stream_metadata_bytes)?;
        }

        let mut schema = Arc::new(Schema::empty());
        let schema_bytes = storage.create_schema_from_ingestor(stream_name).await?;
        if !schema_bytes.is_empty() {
            schema = serde_json::from_slice::<Arc<Schema>>(&schema_bytes)?;
        }

        let static_schema: HashMap<String, Arc<Field>> = schema
            .fields
            .into_iter()
            .map(|field| (field.name().to_string(), field.clone()))
            .collect();

        let time_partition = stream_metadata.time_partition.as_deref().unwrap_or("");
        let time_partition_limit = stream_metadata
            .time_partition_limit
            .and_then(|limit| limit.parse().ok());
        let custom_partition = stream_metadata.custom_partition.as_deref().unwrap_or("");
        let static_schema_flag = stream_metadata.static_schema_flag;
        let stream_type = stream_metadata.stream_type;
        let schema_version = stream_metadata.schema_version;
        let log_source = stream_metadata.log_source;
        self.streams.add_stream(
            stream_name.to_string(),
            stream_metadata.created_at,
            time_partition.to_string(),
            time_partition_limit,
            custom_partition.to_string(),
            static_schema_flag,
            static_schema,
            stream_type,
            schema_version,
            log_source,
        );

        Ok(true)
    }

    /// Stores the provided stream metadata in memory mapping
    pub async fn set_stream_meta(
        &self,
        stream_name: &str,
        metadata: LogStreamMetadata,
    ) -> Result<(), LoadError> {
        let mut map = self.streams.write().expect(LOCK_EXPECT);

        map.insert(stream_name.to_string(), metadata);

        Ok(())
    }
}
