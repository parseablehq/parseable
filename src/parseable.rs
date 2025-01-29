use std::{collections::HashMap, path::PathBuf, sync::Arc};

use arrow_schema::{Field, Schema};
use bytes::Bytes;
use clap::{error::ErrorKind, Parser};
use once_cell::sync::Lazy;
use serde_json::Value;

use crate::{
    cli::{Cli, Options, StorageOptions},
    handlers::http::logstream::error::StreamError,
    metadata::{
        error::stream_info::LoadError, load_daily_metrics, update_data_type_time_partition,
        update_schema_from_staging, LogStreamMetadata, StreamInfo, LOCK_EXPECT,
    },
    metrics::fetch_stats_from_storage,
    option::Mode,
    storage::{
        object_storage::parseable_json_path, LogStream, ObjectStorageError, ObjectStorageProvider,
        ObjectStoreFormat, StreamType,
    },
};

pub const JOIN_COMMUNITY: &str =
    "Join us on Parseable Slack community for questions : https://logg.ing/community";

/// Shared state of the Parseable server.
pub static PARSEABLE: Lazy<Parseable> = Lazy::new(Parseable::new);

/// All state related to parseable, in one place.
pub struct Parseable {
    pub options: Options,
    storage: Arc<dyn ObjectStorageProvider>,
    pub storage_name: &'static str,
    pub streams: StreamInfo,
}

impl Parseable {
    fn new() -> Self {
        match Cli::parse().storage {
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

                Self {
                    options: args.options,
                    storage: Arc::new(args.storage),
                    storage_name: "drive",
                    streams: Default::default(),
                }
            }
            StorageOptions::S3(args) => Self {
                options: args.options,
                storage: Arc::new(args.storage),
                storage_name: "s3",
                streams: Default::default(),
            },
            StorageOptions::Blob(args) => Self {
                options: args.options,
                storage: Arc::new(args.storage),
                storage_name: "blob_store",
                streams: Default::default(),
            },
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
        if !streams.contains(&LogStream {
            name: stream_name.to_owned(),
        }) {
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

        let mut static_schema: HashMap<String, Arc<Field>> = HashMap::new();

        for (field_name, field) in schema
            .fields()
            .iter()
            .map(|field| (field.name().to_string(), field.clone()))
        {
            static_schema.insert(field_name, field);
        }

        let time_partition = stream_metadata.time_partition.as_deref().unwrap_or("");
        let time_partition_limit = stream_metadata
            .time_partition_limit
            .and_then(|limit| limit.parse().ok());
        let custom_partition = stream_metadata.custom_partition.as_deref().unwrap_or("");
        let static_schema_flag = stream_metadata.static_schema_flag;
        let stream_type = stream_metadata
            .stream_type
            .map(|s| StreamType::from(s.as_str()))
            .unwrap_or_default();
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

    pub async fn load_stream_metadata_on_server_start(
        &self,
        stream_name: &str,
        schema: Schema,
        stream_metadata_value: Value,
    ) -> Result<(), LoadError> {
        let ObjectStoreFormat {
            schema_version,
            created_at,
            first_event_at,
            retention,
            snapshot,
            stats,
            time_partition,
            time_partition_limit,
            custom_partition,
            static_schema_flag,
            hot_tier_enabled,
            stream_type,
            log_source,
            ..
        } = if !stream_metadata_value.is_null() {
            serde_json::from_slice(&serde_json::to_vec(&stream_metadata_value).unwrap()).unwrap()
        } else {
            ObjectStoreFormat::default()
        };
        let storage = self.storage.get_object_store();
        let schema = update_data_type_time_partition(
            &*storage,
            stream_name,
            schema,
            time_partition.as_ref(),
        )
        .await?;
        storage.put_schema(stream_name, &schema).await?;
        //load stats from storage
        fetch_stats_from_storage(stream_name, stats).await;
        load_daily_metrics(&snapshot.manifest_list, stream_name);

        let alerts = storage.get_alerts(stream_name).await?;
        let schema = update_schema_from_staging(stream_name, schema);
        let schema = HashMap::from_iter(
            schema
                .fields
                .iter()
                .map(|v| (v.name().to_owned(), v.clone())),
        );

        let metadata = LogStreamMetadata {
            schema_version,
            schema,
            alerts,
            retention,
            created_at,
            first_event_at,
            time_partition,
            time_partition_limit: time_partition_limit.and_then(|limit| limit.parse().ok()),
            custom_partition,
            static_schema_flag,
            hot_tier_enabled,
            stream_type,
            log_source,
        };

        let mut map = self.streams.write().expect(LOCK_EXPECT);

        map.insert(stream_name.to_string(), metadata);

        Ok(())
    }
}
