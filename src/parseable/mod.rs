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

use std::{collections::HashMap, num::NonZeroU32, path::PathBuf, str::FromStr, sync::Arc};

use actix_web::http::header::HeaderMap;
use arrow_schema::{Field, Schema};
use bytes::Bytes;
use chrono::Utc;
use clap::{error::ErrorKind, Parser};
use http::{header::CONTENT_TYPE, HeaderName, HeaderValue, StatusCode};
use once_cell::sync::Lazy;
pub use staging::StagingError;
use streams::StreamRef;
pub use streams::{Stream, StreamNotFound, Streams};
use tracing::error;

#[cfg(feature = "kafka")]
use crate::connectors::kafka::config::KafkaConfig;
use crate::{
    cli::{Cli, Options, StorageOptions},
    event::format::LogSource,
    handlers::{
        http::{
            cluster::{sync_streams_with_ingestors, INTERNAL_STREAM_NAME},
            ingest::PostError,
            logstream::error::{CreateStreamError, StreamError},
            modal::{utils::logstream_utils::PutStreamHeaders, IngestorMetadata},
        },
        STREAM_TYPE_KEY,
    },
    metadata::{LogStreamMetadata, SchemaVersion},
    option::Mode,
    static_schema::{convert_static_schema_to_arrow_schema, StaticSchema},
    storage::{
        object_storage::parseable_json_path, ObjectStorageError, ObjectStorageProvider,
        ObjectStoreFormat, Owner, Permisssion, StreamType,
    },
    validator,
};

mod staging;
mod streams;

/// File extension for arrow files in staging
const ARROW_FILE_EXTENSION: &str = "arrows";

/// Name of a Stream
/// NOTE: this used to be a struct, flattened out for simplicity
pub type LogStream = String;

pub const JOIN_COMMUNITY: &str =
    "Join us on Parseable Slack community for questions : https://logg.ing/community";
pub const STREAM_EXISTS: &str = "Stream exists";

/// Shared state of the Parseable server.
pub static PARSEABLE: Lazy<Parseable> = Lazy::new(|| match Cli::parse().storage {
    StorageOptions::Local(args) => {
        if args.options.staging_dir() == &args.storage.root {
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

        Parseable::new(
            args.options,
            #[cfg(feature = "kafka")]
            args.kafka,
            Arc::new(args.storage),
        )
    }
    StorageOptions::S3(args) => Parseable::new(
        args.options,
        #[cfg(feature = "kafka")]
        args.kafka,
        Arc::new(args.storage),
    ),
    StorageOptions::Blob(args) => Parseable::new(
        args.options,
        #[cfg(feature = "kafka")]
        args.kafka,
        Arc::new(args.storage),
    ),
});

/// All state related to parseable, in one place.
pub struct Parseable {
    /// Configuration variables for parseable
    pub options: Arc<Options>,
    /// Storage engine backing parseable
    pub storage: Arc<dyn ObjectStorageProvider>,
    /// Metadata and staging realting to each logstreams
    /// A globally shared mapping of `Streams` that parseable is aware of.
    pub streams: Streams,
    /// Metadata associated only with an ingestor
    pub ingestor_metadata: Option<Arc<IngestorMetadata>>,
    /// Used to configure the kafka connector
    #[cfg(feature = "kafka")]
    pub kafka_config: KafkaConfig,
}

impl Parseable {
    pub fn new(
        options: Options,
        #[cfg(feature = "kafka")] kafka_config: KafkaConfig,
        storage: Arc<dyn ObjectStorageProvider>,
    ) -> Self {
        let ingestor_metadata = match &options.mode {
            Mode::Ingest => Some(IngestorMetadata::load(&options, storage.as_ref())),
            _ => None,
        };
        Parseable {
            options: Arc::new(options),
            storage,
            streams: Streams::default(),
            ingestor_metadata,
            #[cfg(feature = "kafka")]
            kafka_config,
        }
    }

    /// Try to get the handle of a stream in staging, if it doesn't exist return `None`.
    pub fn get_stream(&self, stream_name: &str) -> Result<StreamRef, StreamNotFound> {
        self.streams
            .read()
            .unwrap()
            .get(stream_name)
            .ok_or_else(|| StreamNotFound(stream_name.to_owned()))
            .cloned()
    }

    /// Get the handle to a stream in staging, create one if it doesn't exist
    pub fn get_or_create_stream(&self, stream_name: &str) -> StreamRef {
        if let Ok(staging) = self.get_stream(stream_name) {
            return staging;
        }

        // Gets write privileges only for creating the stream when it doesn't already exist.
        self.streams.create(
            self.options.clone(),
            stream_name.to_owned(),
            LogStreamMetadata::default(),
            self.ingestor_metadata
                .as_ref()
                .map(|meta| meta.get_ingestor_id()),
        )
    }

    /// Checks for the stream in memory, or loads it from storage when in distributed mode
    pub async fn check_or_load_stream(&self, stream_name: &str) -> bool {
        !self.streams.contains(stream_name)
            && (self.options.mode != Mode::Query
                || !self
                    .create_stream_and_schema_from_storage(stream_name)
                    .await
                    .unwrap_or_default())
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

        if self.storage.name() == "drive" {
            return Err(ObjectStorageError::Custom(format!("Could not start the server because directory '{}' contains stale data, please use an empty directory, and restart the server.\n{}", self.storage.get_endpoint(), JOIN_COMMUNITY)));
        }

        // S3 bucket mode
        Err(ObjectStorageError::Custom(format!("Could not start the server because bucket '{}' contains stale data, please use an empty bucket and restart the server.\n{}", self.storage.get_endpoint(), JOIN_COMMUNITY)))
    }

    pub fn storage(&self) -> Arc<dyn ObjectStorageProvider> {
        self.storage.clone()
    }

    pub fn hot_tier_dir(&self) -> &Option<PathBuf> {
        &self.options.hot_tier_storage_path
    }

    // returns the string representation of the storage mode
    // drive --> Local drive
    // s3 --> S3 bucket
    // azure_blob --> Azure Blob Storage
    pub fn get_storage_mode_string(&self) -> &str {
        if self.storage.name() == "drive" {
            return "Local drive";
        } else if self.storage.name() == "s3" {
            return "S3 bucket";
        } else if self.storage.name() == "blob_store" {
            return "Azure Blob Storage";
        }
        "Unknown"
    }

    pub fn get_server_mode_string(&self) -> &str {
        match self.options.mode {
            Mode::Query => "Distributed (Query)",
            Mode::Ingest => "Distributed (Ingest)",
            Mode::Index => "Distributed (Index)",
            Mode::All => "Standalone",
        }
    }

    // create the ingestor metadata and put the .ingestor.json file in the object store
    pub async fn store_ingestor_metadata(&self) -> anyhow::Result<()> {
        let Some(meta) = self.ingestor_metadata.as_ref() else {
            return Ok(());
        };
        let storage_ingestor_metadata = meta.migrate().await?;
        let store = self.storage.get_object_store();

        // use the id that was generated/found in the staging and
        // generate the path for the object store
        let path = meta.file_path();

        // we are considering that we can always get from object store
        if let Some(mut store_data) = storage_ingestor_metadata {
            if store_data.domain_name != meta.domain_name {
                store_data.domain_name.clone_from(&meta.domain_name);
                store_data.port.clone_from(&meta.port);

                let resource = Bytes::from(serde_json::to_vec(&store_data)?);

                // if pushing to object store fails propagate the error
                store.put_object(&path, resource).await?;
            }
        } else {
            let resource = serde_json::to_vec(&meta)?.into();

            store.put_object(&path, resource).await?;
        }

        Ok(())
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

        let created_at = stream_metadata.created_at;
        let time_partition = stream_metadata.time_partition.unwrap_or_default();
        let time_partition_limit = stream_metadata
            .time_partition_limit
            .and_then(|limit| limit.parse().ok());
        let custom_partition = stream_metadata.custom_partition;
        let static_schema_flag = stream_metadata.static_schema_flag;
        let stream_type = stream_metadata.stream_type;
        let schema_version = stream_metadata.schema_version;
        let log_source = stream_metadata.log_source;
        let metadata = LogStreamMetadata::new(
            created_at,
            time_partition,
            time_partition_limit,
            custom_partition,
            static_schema_flag,
            static_schema,
            stream_type,
            schema_version,
            log_source,
        );
        self.streams.create(
            self.options.clone(),
            stream_name.to_string(),
            metadata,
            self.ingestor_metadata
                .as_ref()
                .map(|meta| meta.get_ingestor_id()),
        );

        Ok(true)
    }

    pub async fn create_internal_stream_if_not_exists(&self) -> Result<(), StreamError> {
        match self
            .create_stream_if_not_exists(
                INTERNAL_STREAM_NAME,
                StreamType::Internal,
                LogSource::Pmeta,
            )
            .await
        {
            Err(_) | Ok(true) => return Ok(()),
            _ => {}
        }

        let mut header_map = HeaderMap::new();
        header_map.insert(
            HeaderName::from_str(STREAM_TYPE_KEY).unwrap(),
            HeaderValue::from_str(&StreamType::Internal.to_string()).unwrap(),
        );
        header_map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        sync_streams_with_ingestors(header_map, Bytes::new(), INTERNAL_STREAM_NAME).await?;

        Ok(())
    }

    // Check if the stream exists and create a new stream if doesn't exist
    pub async fn create_stream_if_not_exists(
        &self,
        stream_name: &str,
        stream_type: StreamType,
        log_source: LogSource,
    ) -> Result<bool, PostError> {
        if self.streams.contains(stream_name) {
            return Ok(true);
        }

        // For distributed deployments, if the stream not found in memory map,
        //check if it exists in the storage
        //create stream and schema from storage
        if self.options.mode != Mode::All
            && self
                .create_stream_and_schema_from_storage(stream_name)
                .await?
        {
            return Ok(false);
        }

        self.create_stream(
            stream_name.to_string(),
            "",
            None,
            None,
            false,
            Arc::new(Schema::empty()),
            stream_type,
            log_source,
        )
        .await?;

        Ok(false)
    }

    pub async fn create_update_stream(
        &self,
        headers: &HeaderMap,
        body: &Bytes,
        stream_name: &str,
    ) -> Result<HeaderMap, StreamError> {
        let PutStreamHeaders {
            time_partition,
            time_partition_limit,
            custom_partition,
            static_schema_flag,
            update_stream_flag,
            stream_type,
            log_source,
        } = headers.into();

        let stream_in_memory_dont_update =
            self.streams.contains(stream_name) && !update_stream_flag;
        let stream_in_storage_only_for_query_node = !self.streams.contains(stream_name)     // check if stream in storage only if not in memory
            && self.options.mode == Mode::Query                                                   // and running in query mode
            && self
                .create_stream_and_schema_from_storage(stream_name)
                .await?;
        if stream_in_memory_dont_update || stream_in_storage_only_for_query_node {
            return Err(StreamError::Custom {
                 msg: format!(
                     "Logstream {stream_name} already exists, please create a new log stream with unique name"
                 ),
                 status: StatusCode::BAD_REQUEST,
             });
        }

        if update_stream_flag {
            return self
                .update_stream(
                    headers,
                    stream_name,
                    &time_partition,
                    static_schema_flag,
                    &time_partition_limit,
                    custom_partition.as_ref(),
                )
                .await;
        }

        let time_partition_in_days = if !time_partition_limit.is_empty() {
            Some(validate_time_partition_limit(&time_partition_limit)?)
        } else {
            None
        };

        if let Some(custom_partition) = &custom_partition {
            validate_custom_partition(custom_partition)?;
        }

        if !time_partition.is_empty() && custom_partition.is_some() {
            return Err(StreamError::Custom {
                msg: "Cannot set both time partition and custom partition".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }

        let schema = validate_static_schema(
            body,
            stream_name,
            &time_partition,
            custom_partition.as_ref(),
            static_schema_flag,
        )?;

        self.create_stream(
            stream_name.to_string(),
            &time_partition,
            time_partition_in_days,
            custom_partition.as_ref(),
            static_schema_flag,
            schema,
            stream_type,
            log_source,
        )
        .await?;

        Ok(headers.clone())
    }

    async fn update_stream(
        &self,
        headers: &HeaderMap,
        stream_name: &str,
        time_partition: &str,
        static_schema_flag: bool,
        time_partition_limit: &str,
        custom_partition: Option<&String>,
    ) -> Result<HeaderMap, StreamError> {
        if !self.streams.contains(stream_name) {
            return Err(StreamNotFound(stream_name.to_string()).into());
        }
        if !time_partition.is_empty() {
            return Err(StreamError::Custom {
                msg: "Altering the time partition of an existing stream is restricted.".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }
        if static_schema_flag {
            return Err(StreamError::Custom {
                msg: "Altering the schema of an existing stream is restricted.".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }
        if !time_partition_limit.is_empty() {
            let time_partition_days = validate_time_partition_limit(time_partition_limit)?;
            self.update_time_partition_limit_in_stream(
                stream_name.to_string(),
                time_partition_days,
            )
            .await?;
            return Ok(headers.clone());
        }
        self.validate_and_update_custom_partition(stream_name, custom_partition)
            .await?;

        Ok(headers.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_stream(
        &self,
        stream_name: String,
        time_partition: &str,
        time_partition_limit: Option<NonZeroU32>,
        custom_partition: Option<&String>,
        static_schema_flag: bool,
        schema: Arc<Schema>,
        stream_type: StreamType,
        log_source: LogSource,
    ) -> Result<(), CreateStreamError> {
        // fail to proceed if invalid stream name
        if stream_type != StreamType::Internal {
            validator::stream_name(&stream_name, stream_type)?;
        }
        // Proceed to create log stream if it doesn't exist
        let storage = self.storage.get_object_store();

        let meta = ObjectStoreFormat {
            created_at: Utc::now().to_rfc3339(),
            permissions: vec![Permisssion::new(PARSEABLE.options.username.clone())],
            stream_type,
            time_partition: (!time_partition.is_empty()).then(|| time_partition.to_string()),
            time_partition_limit: time_partition_limit.map(|limit| limit.to_string()),
            custom_partition: custom_partition.cloned(),
            static_schema_flag,
            schema_version: SchemaVersion::V1, // NOTE: Newly created streams are all V1
            owner: Owner {
                id: PARSEABLE.options.username.clone(),
                group: PARSEABLE.options.username.clone(),
            },
            log_source: log_source.clone(),
            ..Default::default()
        };

        match storage
            .create_stream(&stream_name, meta, schema.clone())
            .await
        {
            Ok(created_at) => {
                let mut static_schema: HashMap<String, Arc<Field>> = HashMap::new();

                for (field_name, field) in schema
                    .fields()
                    .iter()
                    .map(|field| (field.name().to_string(), field.clone()))
                {
                    static_schema.insert(field_name, field);
                }

                let metadata = LogStreamMetadata::new(
                    created_at,
                    time_partition.to_owned(),
                    time_partition_limit,
                    custom_partition.cloned(),
                    static_schema_flag,
                    static_schema,
                    stream_type,
                    SchemaVersion::V1, // New stream
                    log_source,
                );
                self.streams.create(
                    self.options.clone(),
                    stream_name.to_string(),
                    metadata,
                    self.ingestor_metadata
                        .as_ref()
                        .map(|meta| meta.get_ingestor_id()),
                );
            }
            Err(err) => {
                return Err(CreateStreamError::Storage { stream_name, err });
            }
        }
        Ok(())
    }

    async fn validate_and_update_custom_partition(
        &self,
        stream_name: &str,
        custom_partition: Option<&String>,
    ) -> Result<(), StreamError> {
        let stream = self.get_stream(stream_name).expect(STREAM_EXISTS);
        if stream.get_time_partition().is_some() {
            return Err(StreamError::Custom {
                msg: "Cannot set both time partition and custom partition".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }
        if let Some(custom_partition) = custom_partition {
            validate_custom_partition(custom_partition)?;
        }

        self.update_custom_partition_in_stream(stream_name.to_string(), custom_partition)
            .await?;

        Ok(())
    }

    pub async fn update_time_partition_limit_in_stream(
        &self,
        stream_name: String,
        time_partition_limit: NonZeroU32,
    ) -> Result<(), CreateStreamError> {
        let storage = self.storage.get_object_store();
        if let Err(err) = storage
            .update_time_partition_limit_in_stream(&stream_name, time_partition_limit)
            .await
        {
            return Err(CreateStreamError::Storage { stream_name, err });
        }

        if let Ok(stream) = self.get_stream(&stream_name) {
            stream.set_time_partition_limit(time_partition_limit)
        } else {
            return Err(CreateStreamError::Custom {
                msg: "failed to update time partition limit in metadata".to_string(),
                status: StatusCode::EXPECTATION_FAILED,
            });
        }

        Ok(())
    }

    pub async fn update_custom_partition_in_stream(
        &self,
        stream_name: String,
        custom_partition: Option<&String>,
    ) -> Result<(), CreateStreamError> {
        let stream = self.get_stream(&stream_name).expect(STREAM_EXISTS);
        let static_schema_flag = stream.get_static_schema_flag();
        let time_partition = stream.get_time_partition();
        if static_schema_flag {
            let schema = stream.get_schema();

            if let Some(custom_partition) = custom_partition {
                let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
                for partition in custom_partition_list.iter() {
                    if !schema
                        .fields()
                        .iter()
                        .any(|field| field.name() == partition)
                    {
                        return Err(CreateStreamError::Custom {
                         msg: format!("custom partition field {partition} does not exist in the schema for the stream {stream_name}"),
                         status: StatusCode::BAD_REQUEST,
                     });
                    }
                }

                for partition in custom_partition_list {
                    if time_partition
                        .as_ref()
                        .is_some_and(|time| time == partition)
                    {
                        return Err(CreateStreamError::Custom {
                            msg: format!(
                                "time partition {} cannot be set as custom partition",
                                partition
                            ),
                            status: StatusCode::BAD_REQUEST,
                        });
                    }
                }
            }
        }
        let storage = self.storage.get_object_store();
        if let Err(err) = storage
            .update_custom_partition_in_stream(&stream_name, custom_partition)
            .await
        {
            return Err(CreateStreamError::Storage { stream_name, err });
        }

        stream.set_custom_partition(custom_partition);

        Ok(())
    }

    /// Updates the first-event-at in storage and logstream metadata for the specified stream.
    ///
    /// This function updates the `first-event-at` in both the object store and the stream info metadata.
    /// If either update fails, an error is logged, but the function will still return the `first-event-at`.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The name of the stream to update.
    /// * `first_event_at` - The value of first-event-at.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - Returns `Some(String)` with the provided timestamp if the update is successful,
    ///   or `None` if an error occurs.
    ///
    /// # Errors
    ///
    /// This function logs an error if:
    /// * The `first-event-at` cannot be updated in the object store.
    /// * The `first-event-at` cannot be updated in the stream info.
    ///
    /// # Examples
    ///```ignore
    /// ```rust
    /// use parseable::handlers::http::modal::utils::logstream_utils::update_first_event_at;
    /// let result = update_first_event_at("my_stream", "2023-01-01T00:00:00Z").await;
    /// match result {
    ///     Some(timestamp) => println!("first-event-at: {}", timestamp),
    ///     None => eprintln!("Failed to update first-event-at"),
    /// }
    /// ```
    pub async fn update_first_event_at(
        &self,
        stream_name: &str,
        first_event_at: &str,
    ) -> Option<String> {
        let storage = self.storage.get_object_store();
        if let Err(err) = storage
            .update_first_event_in_stream(stream_name, first_event_at)
            .await
        {
            error!(
                "Failed to update first_event_at in storage for stream {stream_name:?}: {err:?}"
            );
        }

        match self.get_stream(stream_name) {
            Ok(stream) => stream.set_first_event_at(first_event_at),
            Err(err) => error!(
                "Failed to update first_event_at in stream info for stream {stream_name:?}: {err:?}"
            ),
        }

        Some(first_event_at.to_string())
    }
}

pub fn validate_static_schema(
    body: &Bytes,
    stream_name: &str,
    time_partition: &str,
    custom_partition: Option<&String>,
    static_schema_flag: bool,
) -> Result<Arc<Schema>, CreateStreamError> {
    if !static_schema_flag {
        return Ok(Arc::new(Schema::empty()));
    }

    if body.is_empty() {
        return Err(CreateStreamError::Custom {
                 msg: format!(
                     "Please provide schema in the request body for static schema logstream {stream_name}"
                 ),
                 status: StatusCode::BAD_REQUEST,
             });
    }

    let static_schema: StaticSchema = serde_json::from_slice(body)?;
    let parsed_schema =
        convert_static_schema_to_arrow_schema(static_schema, time_partition, custom_partition)
            .map_err(|_| CreateStreamError::Custom {
                msg: format!("Unable to commit static schema, logstream {stream_name} not created"),
                status: StatusCode::BAD_REQUEST,
            })?;

    Ok(parsed_schema)
}

pub fn validate_time_partition_limit(
    time_partition_limit: &str,
) -> Result<NonZeroU32, CreateStreamError> {
    if !time_partition_limit.ends_with('d') {
        return Err(CreateStreamError::Custom {
            msg: "Missing 'd' suffix for duration value".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    let days = &time_partition_limit[0..time_partition_limit.len() - 1];
    let Ok(days) = days.parse::<NonZeroU32>() else {
        return Err(CreateStreamError::Custom {
            msg: "Could not convert duration to an unsigned number".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    };

    Ok(days)
}

pub fn validate_custom_partition(custom_partition: &str) -> Result<(), CreateStreamError> {
    let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
    if custom_partition_list.len() > 1 {
        return Err(CreateStreamError::Custom {
            msg: "Maximum 1 custom partition key is supported".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    Ok(())
}
