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

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use chrono::{Local, NaiveDateTime};
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use self::error::stream_info::{CheckAlertError, LoadError, MetadataError};
use crate::alerts::Alerts;
use crate::metrics::{
    fetch_stats_from_storage, EVENTS_INGESTED, EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE,
    EVENTS_INGESTED_SIZE_DATE, EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_INGESTED,
    LIFETIME_EVENTS_INGESTED_SIZE,
};
use crate::storage::retention::Retention;
use crate::storage::{LogStream, ObjectStorage, ObjectStoreFormat, StorageDir, StreamType};
use crate::utils::arrow::MergedRecordReader;
use derive_more::{Deref, DerefMut};

// TODO: make return type be of 'static lifetime instead of cloning
// A read-write lock to allow multiple reads while and isolated write
pub static STREAM_INFO: Lazy<StreamInfo> = Lazy::new(StreamInfo::default);

#[derive(Debug, Deref, DerefMut, Default)]
pub struct StreamInfo(RwLock<HashMap<String, LogStreamMetadata>>);

#[derive(Debug, Default)]
pub struct LogStreamMetadata {
    pub schema: HashMap<String, Arc<Field>>,
    pub alerts: Alerts,
    pub retention: Option<Retention>,
    pub cache_enabled: bool,
    pub created_at: String,
    pub first_event_at: Option<String>,
    pub time_partition: Option<String>,
    pub time_partition_limit: Option<String>,
    pub custom_partition: Option<String>,
    pub static_schema_flag: Option<String>,
    pub hot_tier_enabled: Option<bool>,
    pub stream_type: Option<String>,
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
    pub async fn check_alerts(
        &self,
        stream_name: &str,
        rb: &RecordBatch,
    ) -> Result<(), CheckAlertError> {
        let map = self.read().expect(LOCK_EXPECT);
        let meta = map
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_owned()))?;

        for alert in &meta.alerts.alerts {
            alert.check_alert(stream_name, rb.clone())
        }

        Ok(())
    }

    pub fn stream_exists(&self, stream_name: &str) -> bool {
        let map = self.read().expect(LOCK_EXPECT);
        map.contains_key(stream_name)
    }

    pub fn stream_initialized(&self, stream_name: &str) -> Result<bool, MetadataError> {
        Ok(!self.schema(stream_name)?.fields.is_empty())
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
    ) -> Result<Option<String>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.time_partition_limit.clone())
    }

    pub fn get_custom_partition(&self, stream_name: &str) -> Result<Option<String>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.custom_partition.clone())
    }

    pub fn get_static_schema_flag(
        &self,
        stream_name: &str,
    ) -> Result<Option<String>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.static_schema_flag.clone())
    }

    pub fn get_retention(&self, stream_name: &str) -> Result<Option<Retention>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.retention.clone())
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

    pub fn set_alert(&self, stream_name: &str, alerts: Alerts) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.alerts = alerts;
            })
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
        first_event_at: Option<String>,
    ) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.first_event_at = first_event_at;
            })
    }

    pub fn update_time_partition_limit(
        &self,
        stream_name: &str,
        time_partition_limit: String,
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

    pub fn get_cache_enabled(&self, stream_name: &str) -> Result<bool, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.cache_enabled)
    }

    pub fn set_cache_enabled(&self, stream_name: &str, enable: bool) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        let stream = map
            .get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))?;
        stream.cache_enabled = enable;
        Ok(())
    }

    pub fn set_hot_tier(&self, stream_name: &str, enable: bool) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        let stream = map
            .get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))?;
        stream.hot_tier_enabled = Some(enable);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_stream(
        &self,
        stream_name: String,
        created_at: String,
        time_partition: String,
        time_partition_limit: String,
        custom_partition: String,
        static_schema_flag: String,
        static_schema: HashMap<String, Arc<Field>>,
        stream_type: &str,
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
            time_partition_limit: if time_partition_limit.is_empty() {
                None
            } else {
                Some(time_partition_limit)
            },
            custom_partition: if custom_partition.is_empty() {
                None
            } else {
                Some(custom_partition)
            },
            static_schema_flag: if static_schema_flag != "true" {
                None
            } else {
                Some(static_schema_flag)
            },
            schema: if static_schema.is_empty() {
                HashMap::new()
            } else {
                static_schema
            },
            stream_type: Some(stream_type.to_string()),
            ..Default::default()
        };
        map.insert(stream_name, metadata);
    }

    pub fn delete_stream(&self, stream_name: &str) {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.remove(stream_name);
    }

    pub async fn upsert_stream_info(
        &self,
        storage: &(impl ObjectStorage + ?Sized),
        stream: LogStream,
    ) -> Result<(), LoadError> {
        let alerts = storage.get_alerts(&stream.name).await?;

        let schema = storage.upsert_schema_to_storage(&stream.name).await?;
        let meta = storage.upsert_stream_metadata(&stream.name).await?;
        let retention = meta.retention;
        let schema = update_schema_from_staging(&stream.name, schema);
        let schema = HashMap::from_iter(
            schema
                .fields
                .iter()
                .map(|v| (v.name().to_owned(), v.clone())),
        );

        let metadata = LogStreamMetadata {
            schema,
            alerts,
            retention,
            cache_enabled: meta.cache_enabled,
            created_at: meta.created_at,
            first_event_at: meta.first_event_at,
            time_partition: meta.time_partition,
            time_partition_limit: meta.time_partition_limit,
            custom_partition: meta.custom_partition,
            static_schema_flag: meta.static_schema_flag,
            hot_tier_enabled: meta.hot_tier_enabled,
            stream_type: meta.stream_type,
        };

        let mut map = self.write().expect(LOCK_EXPECT);

        map.insert(stream.name, metadata);
        Ok(())
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
            .filter(|(_, v)| v.stream_type.clone().unwrap() == StreamType::Internal.to_string())
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn stream_type(&self, stream_name: &str) -> Result<Option<String>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.stream_type.clone())
    }

    pub fn update_stats(
        &self,
        stream_name: &str,
        origin: &'static str,
        size: u64,
        num_rows: u64,
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

fn update_schema_from_staging(stream_name: &str, current_schema: Schema) -> Schema {
    let staging_files = StorageDir::new(stream_name).arrow_files();
    let schema = MergedRecordReader::try_new(&staging_files)
        .unwrap()
        .merged_schema();

    Schema::try_merge(vec![schema, current_schema]).unwrap()
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
    meta: ObjectStoreFormat,
) -> anyhow::Result<Schema> {
    let mut schema = schema.clone();
    if meta.time_partition.is_some() {
        let time_partition = meta.time_partition.unwrap();
        if let Ok(time_partition_field) = schema.field_with_name(&time_partition) {
            if time_partition_field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
            {
                let mut fields = schema
                    .fields()
                    .iter()
                    .filter(|field| *field.name() != time_partition)
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
    let mut meta: ObjectStoreFormat = ObjectStoreFormat::default();
    if !stream_metadata_value.is_null() {
        meta =
            serde_json::from_slice(&serde_json::to_vec(&stream_metadata_value).unwrap()).unwrap();
    }
    let schema =
        update_data_type_time_partition(storage, stream_name, schema, meta.clone()).await?;

    //load stats from storage
    let stats = meta.stats;
    fetch_stats_from_storage(stream_name, stats).await;
    load_daily_metrics(&meta, stream_name);

    let alerts = storage.get_alerts(stream_name).await?;
    let schema = update_schema_from_staging(stream_name, schema);
    let schema = HashMap::from_iter(
        schema
            .fields
            .iter()
            .map(|v| (v.name().to_owned(), v.clone())),
    );

    let metadata = LogStreamMetadata {
        schema,
        alerts,
        retention: meta.retention,
        cache_enabled: meta.cache_enabled,
        created_at: meta.created_at,
        first_event_at: meta.first_event_at,
        time_partition: meta.time_partition,
        time_partition_limit: meta.time_partition_limit,
        custom_partition: meta.custom_partition,
        static_schema_flag: meta.static_schema_flag,
        hot_tier_enabled: meta.hot_tier_enabled,
        stream_type: meta.stream_type,
    };

    let mut map = STREAM_INFO.write().expect(LOCK_EXPECT);

    map.insert(stream_name.to_string(), metadata);

    Ok(())
}

fn load_daily_metrics(meta: &ObjectStoreFormat, stream_name: &str) {
    let manifests = &meta.snapshot.manifest_list;
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
        pub enum CheckAlertError {
            #[error("Serde Json Error: {0}")]
            Serde(#[from] serde_json::Error),
            #[error("Metadata Error: {0}")]
            Metadata(#[from] MetadataError),
        }

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
