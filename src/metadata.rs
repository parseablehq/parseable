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

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use chrono::{Local, NaiveDateTime};
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};

use self::error::stream_info::{LoadError, MetadataError};
use crate::catalog::snapshot::ManifestItem;
use crate::event::format::LogSource;
use crate::metrics::{
    fetch_stats_from_storage, EVENTS_INGESTED, EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE,
    EVENTS_INGESTED_SIZE_DATE, EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_INGESTED,
    LIFETIME_EVENTS_INGESTED_SIZE,
};
use crate::staging::STAGING;
use crate::storage::retention::Retention;
use crate::storage::{ObjectStorage, ObjectStoreFormat, StreamType};
use derive_more::{Deref, DerefMut};

// TODO: make return type be of 'static lifetime instead of cloning
// A read-write lock to allow multiple reads while and isolated write
pub static STREAM_INFO: Lazy<StreamInfo> = Lazy::new(StreamInfo::default);

#[derive(Debug, Deref, DerefMut, Default)]
pub struct StreamInfo(RwLock<HashMap<String, LogStreamMetadata>>);

/// In order to support backward compatability with streams created before v1.6.4,
/// we will consider past versions of stream schema to be v0. Streams created with
/// v1.6.4+ will be v1.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum SchemaVersion {
    #[default]
    V0,
    /// Applies generic JSON flattening, ignores null data, handles all numbers as
    /// float64 and uses the timestamp type to store compatible time information.
    V1,
}

#[derive(Debug, Default)]
pub struct LogStreamMetadata {
    pub schema_version: SchemaVersion,
    pub schema: HashMap<String, Arc<Field>>,
    pub retention: Option<Retention>,
    pub created_at: String,
    pub first_event_at: Option<String>,
    pub time_partition: Option<String>,
    pub time_partition_limit: Option<NonZeroU32>,
    pub custom_partition: Option<String>,
    pub static_schema_flag: bool,
    pub hot_tier_enabled: bool,
    pub stream_type: StreamType,
    pub log_source: LogSource,
}

// It is very unlikely that panic will occur when dealing with metadata.
pub const LOCK_EXPECT: &str = "no method in metadata should panic while holding a lock";

// STREAM_INFO should be updated
// 1. During server start up
// 2. When a new stream is created (make a new entry in the map)
// 3. When a stream is deleted (remove the entry from the map)
// 4. When first event is sent to stream (update the schema)
// 5. When set alert API is called (update the alert)
impl StreamInfo {
    pub fn stream_exists(&self, stream_name: &str) -> bool {
        let map = self.read().expect(LOCK_EXPECT);
        map.contains_key(stream_name)
    }

    pub fn get_first_event(&self, stream_name: &str) -> Result<Option<String>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.first_event_at.clone())
    }

    pub fn get_time_partition(&self, stream_name: &str) -> Result<Option<String>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.time_partition.clone())
    }

    pub fn get_time_partition_limit(
        &self,
        stream_name: &str,
    ) -> Result<Option<NonZeroU32>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.time_partition_limit)
    }

    pub fn get_custom_partition(&self, stream_name: &str) -> Result<Option<String>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.custom_partition.clone())
    }

    pub fn get_static_schema_flag(&self, stream_name: &str) -> Result<bool, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.static_schema_flag)
    }

    pub fn get_retention(&self, stream_name: &str) -> Result<Option<Retention>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.retention.clone())
    }

    pub fn get_schema_version(&self, stream_name: &str) -> Result<SchemaVersion, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.schema_version)
    }

    pub fn schema(&self, stream_name: &str) -> Result<Arc<Schema>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        let schema = map
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| &metadata.schema)?;

        // sort fields on read from hashmap as order of fields can differ.
        // This provides a stable output order if schema is same between calls to this function
        let fields: Fields = schema
            .values()
            .sorted_by_key(|field| field.name())
            .cloned()
            .collect();

        let schema = Schema::new(fields);

        Ok(Arc::new(schema))
    }

    #[allow(dead_code)]
    pub fn schema_raw(
        &self,
        stream_name: &str,
    ) -> Result<HashMap<String, Arc<Field>>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);

        let schema = map
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.schema.clone())?;

        Ok(schema)
    }

    pub fn set_retention(
        &self,
        stream_name: &str,
        retention: Retention,
    ) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.retention = Some(retention);
            })
    }

    pub fn set_first_event_at(
        &self,
        stream_name: &str,
        first_event_at: &str,
    ) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.first_event_at = Some(first_event_at.to_owned());
            })
    }

    /// Removes the `first_event_at` timestamp for the specified stream from the LogStreamMetadata.
    ///
    /// This function is called during the retention task, when the parquet files along with the manifest files are deleted from the storage.
    /// The manifest path is removed from the snapshot in the stream.json
    /// and the first_event_at value in the stream.json is removed.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The name of the stream for which the `first_event_at` timestamp is to be removed.
    ///
    /// # Returns
    ///
    /// * `Result<(), MetadataError>` - Returns `Ok(())` if the `first_event_at` timestamp is successfully removed,
    ///   or a `MetadataError` if the stream metadata is not found.
    ///
    /// # Examples
    /// ```ignore
    /// ```rust
    /// let result = metadata.remove_first_event_at("my_stream");
    /// match result {
    ///     Ok(()) => println!("first-event-at removed successfully"),
    ///     Err(e) => eprintln!("Error removing first-event-at from STREAM_INFO: {}", e),
    /// }
    /// ```
    pub fn reset_first_event_at(&self, stream_name: &str) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.first_event_at.take();
            })
    }

    pub fn update_time_partition_limit(
        &self,
        stream_name: &str,
        time_partition_limit: NonZeroU32,
    ) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.time_partition_limit = Some(time_partition_limit);
            })
    }

    pub fn update_custom_partition(
        &self,
        stream_name: &str,
        custom_partition: String,
    ) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                if custom_partition.is_empty() {
                    metadata.custom_partition = None;
                    return;
                }
                metadata.custom_partition = Some(custom_partition);
            })
    }

    pub fn set_hot_tier(&self, stream_name: &str, enable: bool) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        let stream = map
            .get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))?;
        stream.hot_tier_enabled = enable;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_stream(
        &self,
        stream_name: String,
        created_at: String,
        time_partition: String,
        time_partition_limit: Option<NonZeroU32>,
        custom_partition: String,
        static_schema_flag: bool,
        static_schema: HashMap<String, Arc<Field>>,
        stream_type: StreamType,
        schema_version: SchemaVersion,
        log_source: LogSource,
    ) {
        let mut map = self.write().expect(LOCK_EXPECT);
        let metadata = LogStreamMetadata {
            created_at: if created_at.is_empty() {
                Local::now().to_rfc3339()
            } else {
                created_at
            },
            time_partition: if time_partition.is_empty() {
                None
            } else {
                Some(time_partition)
            },
            time_partition_limit,
            custom_partition: if custom_partition.is_empty() {
                None
            } else {
                Some(custom_partition)
            },
            static_schema_flag,
            schema: if static_schema.is_empty() {
                HashMap::new()
            } else {
                static_schema
            },
            stream_type,
            schema_version,
            log_source,
            ..Default::default()
        };
        map.insert(stream_name, metadata);
    }

    pub fn delete_stream(&self, stream_name: &str) {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.remove(stream_name);
    }

    pub fn list_streams(&self) -> Vec<String> {
        self.read()
            .expect(LOCK_EXPECT)
            .keys()
            .map(String::clone)
            .collect()
    }

    pub fn list_internal_streams(&self) -> Vec<String> {
        self.read()
            .expect(LOCK_EXPECT)
            .iter()
            .filter(|(_, v)| v.stream_type == StreamType::Internal)
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn stream_type(&self, stream_name: &str) -> Result<StreamType, MetadataError> {
        self.read()
            .expect(LOCK_EXPECT)
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.stream_type)
    }

    pub fn update_stats(
        &self,
        stream_name: &str,
        origin: &'static str,
        size: u64,
        num_rows: usize,
        parsed_timestamp: NaiveDateTime,
    ) -> Result<(), MetadataError> {
        let parsed_date = parsed_timestamp.date().to_string();
        EVENTS_INGESTED
            .with_label_values(&[stream_name, origin])
            .add(num_rows as i64);
        EVENTS_INGESTED_DATE
            .with_label_values(&[stream_name, origin, parsed_date.as_str()])
            .add(num_rows as i64);
        EVENTS_INGESTED_SIZE
            .with_label_values(&[stream_name, origin])
            .add(size as i64);
        EVENTS_INGESTED_SIZE_DATE
            .with_label_values(&[stream_name, origin, parsed_date.as_str()])
            .add(size as i64);
        LIFETIME_EVENTS_INGESTED
            .with_label_values(&[stream_name, origin])
            .add(num_rows as i64);
        LIFETIME_EVENTS_INGESTED_SIZE
            .with_label_values(&[stream_name, origin])
            .add(size as i64);
        Ok(())
    }
}

///this function updates the data type of time partition field
/// from utf-8 to timestamp if it is not already timestamp
/// and updates the schema in the storage
/// required only when migrating from version 1.2.0 and below
/// this function will be removed in the future
pub async fn update_data_type_time_partition(
    storage: &(impl ObjectStorage + ?Sized),
    stream_name: &str,
    schema: Schema,
    time_partition: Option<&String>,
) -> anyhow::Result<Schema> {
    let mut schema = schema.clone();
    if let Some(time_partition) = time_partition {
        if let Ok(time_partition_field) = schema.field_with_name(time_partition) {
            if time_partition_field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
            {
                let mut fields = schema
                    .fields()
                    .iter()
                    .filter(|field| field.name() != time_partition)
                    .cloned()
                    .collect::<Vec<Arc<Field>>>();
                let time_partition_field = Arc::new(Field::new(
                    time_partition,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ));
                fields.push(time_partition_field);
                schema = Schema::new(fields);
                storage.put_schema(stream_name, &schema).await?;
            }
        }
    }
    Ok(schema)
}

pub async fn load_stream_metadata_on_server_start(
    storage: &(impl ObjectStorage + ?Sized),
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
    let schema =
        update_data_type_time_partition(storage, stream_name, schema, time_partition.as_ref())
            .await?;
    storage.put_schema(stream_name, &schema).await?;
    //load stats from storage
    fetch_stats_from_storage(stream_name, stats).await;
    load_daily_metrics(&snapshot.manifest_list, stream_name);

    let schema = STAGING
        .get_or_create_stream(stream_name)
        .updated_schema(schema);
    let schema = HashMap::from_iter(
        schema
            .fields
            .iter()
            .map(|v| (v.name().to_owned(), v.clone())),
    );

    let metadata = LogStreamMetadata {
        schema_version,
        schema,
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

    let mut map = STREAM_INFO.write().expect(LOCK_EXPECT);

    map.insert(stream_name.to_string(), metadata);

    Ok(())
}

fn load_daily_metrics(manifests: &Vec<ManifestItem>, stream_name: &str) {
    for manifest in manifests {
        let manifest_date = manifest.time_lower_bound.date_naive().to_string();
        let events_ingested = manifest.events_ingested;
        let ingestion_size = manifest.ingestion_size;
        let storage_size = manifest.storage_size;
        EVENTS_INGESTED_DATE
            .with_label_values(&[stream_name, "json", &manifest_date])
            .set(events_ingested as i64);
        EVENTS_INGESTED_SIZE_DATE
            .with_label_values(&[stream_name, "json", &manifest_date])
            .set(ingestion_size as i64);
        EVENTS_STORAGE_SIZE_DATE
            .with_label_values(&["data", stream_name, "parquet", &manifest_date])
            .set(storage_size as i64);
    }
}

pub mod error {
    pub mod stream_info {
        use crate::storage::ObjectStorageError;

        #[derive(Debug, thiserror::Error)]
        pub enum MetadataError {
            #[error("Metadata for stream {0} not found. Please create the stream and try again")]
            StreamMetaNotFound(String),
            #[error("Metadata Error: {0}")]
            StandaloneWithDistributed(String),
        }

        #[derive(Debug, thiserror::Error)]
        pub enum LoadError {
            #[error("Error while loading from object storage: {0}")]
            ObjectStorage(#[from] ObjectStorageError),
            #[error(" Error: {0}")]
            Anyhow(#[from] anyhow::Error),
        }
    }
}
