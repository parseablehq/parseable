/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use datafusion::arrow::datatypes::Schema;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::alerts::Alerts;
use crate::event::Event;
use crate::metrics::{EVENTS_INGESTED, EVENTS_INGESTED_SIZE};
use crate::stats::{Stats, StatsCounter};
use crate::storage::ObjectStorage;

use self::error::stream_info::{CheckAlertError, LoadError, MetadataError};

// TODO: make return type be of 'static lifetime instead of cloning
lazy_static! {
    #[derive(Debug)]
    // A read-write lock to allow multiple reads while and isolated write
    pub static ref STREAM_INFO: RwLock<HashMap<String, LogStreamMetadata>> =
        RwLock::new(HashMap::new());
}

#[derive(Debug, Default)]
pub struct LogStreamMetadata {
    pub schema: HashMap<String, Arc<Schema>>,
    pub alerts: Alerts,
    pub stats: StatsCounter,
}

// It is very unlikely that panic will occur when dealing with metadata.
pub const LOCK_EXPECT: &str = "no method in metadata should panic while holding a lock";

// STREAM_INFO should be updated
// 1. During server start up
// 2. When a new stream is created (make a new entry in the map)
// 3. When a stream is deleted (remove the entry from the map)
// 4. When first event is sent to stream (update the schema)
// 5. When set alert API is called (update the alert)
#[allow(clippy::all)]
impl STREAM_INFO {
    pub async fn check_alerts(&self, event: &Event) -> Result<(), CheckAlertError> {
        let map = self.read().expect(LOCK_EXPECT);
        let meta = map
            .get(&event.stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(
                event.stream_name.to_owned(),
            ))?;

        for alert in &meta.alerts.alerts {
            alert.check_alert(event.stream_name.clone(), &event.body)
        }

        Ok(())
    }

    pub fn stream_exists(&self, stream_name: &str) -> bool {
        let map = self.read().expect(LOCK_EXPECT);
        map.contains_key(stream_name)
    }

    pub fn stream_initialized(&self, stream_name: &str) -> Result<bool, MetadataError> {
        Ok(!self.schema_map(stream_name)?.is_empty())
    }

    pub fn schema(
        &self,
        stream_name: &str,
        schema_key: &str,
    ) -> Result<Option<Arc<Schema>>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        let schemas = map
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| &metadata.schema)?;

        Ok(schemas.get(schema_key).cloned())
    }

    pub fn schema_map(
        &self,
        stream_name: &str,
    ) -> Result<HashMap<String, Arc<Schema>>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        let schemas = map
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.schema.clone())?;

        Ok(schemas)
    }

    pub fn merged_schema(&self, stream_name: &str) -> Result<Schema, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        let schemas = &map
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))?
            .schema;
        let schema = Schema::try_merge(schemas.values().map(|schema| &**schema).cloned())
            .expect("mergeable schemas");

        Ok(schema)
    }

    pub fn set_alert(&self, stream_name: &str, alerts: Alerts) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.alerts = alerts;
            })
    }

    pub fn add_stream(&self, stream_name: String) {
        let mut map = self.write().expect(LOCK_EXPECT);
        let metadata = LogStreamMetadata {
            ..Default::default()
        };
        map.insert(stream_name, metadata);
    }

    pub fn delete_stream(&self, stream_name: &str) {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.remove(stream_name);
    }

    pub async fn load(&self, storage: &(impl ObjectStorage + ?Sized)) -> Result<(), LoadError> {
        // When loading streams this funtion will assume list_streams only returns valid streams.
        // a valid stream would have a .schema file.
        // .schema file could be empty in that case it will be treated as an uninitialized stream.
        // return error in case of an error from object storage itself.

        for stream in storage.list_streams().await? {
            let alerts = storage.get_alerts(&stream.name).await?;
            let schema = storage.get_schema_map(&stream.name).await?;
            let stats = storage.get_stats(&stream.name).await?;

            let metadata = LogStreamMetadata {
                schema,
                alerts,
                stats: stats.into(),
            };

            let mut map = self.write().expect(LOCK_EXPECT);

            map.insert(stream.name, metadata);
        }

        Ok(())
    }

    pub fn list_streams(&self) -> Vec<String> {
        self.read()
            .expect(LOCK_EXPECT)
            .keys()
            .map(String::clone)
            .collect()
    }

    pub fn update_stats(&self, stream_name: &str, size: u64) -> Result<(), MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        let stream = map
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_owned()))?;

        stream.stats.add_ingestion_size(size);
        stream.stats.increase_event_by_one();
        EVENTS_INGESTED
            .with_label_values(&[stream_name.clone(), "json"])
            .inc();
        EVENTS_INGESTED_SIZE
            .with_label_values(&[stream_name.clone(), "json"])
            .set(size as i64);

        Ok(())
    }

    pub fn get_stats(&self, stream_name: &str) -> Result<Stats, MetadataError> {
        self.read()
            .expect(LOCK_EXPECT)
            .get(stream_name)
            .map(|metadata| Stats::from(&metadata.stats))
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_owned()))
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
            #[error("Metadata for stream {0} not found. Maybe the stream does not exist")]
            StreamMetaNotFound(String),
        }

        #[derive(Debug, thiserror::Error)]
        pub enum LoadError {
            #[error("Error while loading from object storage: {0}")]
            ObjectStorage(#[from] ObjectStorageError),
        }
    }
}
