/*
 * Parseable Server (C) 2022 Parseable, Inc.
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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::alerts::Alerts;
use crate::event::Event;
use crate::storage::ObjectStorage;

use self::error::stream_info::{CheckAlertError, LoadError, MetadataError};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LogStreamMetadata {
    pub schema: Option<Schema>,
    pub alerts: Alerts,
    pub stats: Stats,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, Eq)]
pub struct Stats {
    pub size: u64,
    pub compressed_size: u64,
    #[serde(skip)]
    pub prev_compressed: u64,
}

impl Stats {
    /// Update stats considering the following facts about params:
    /// - `size`: The event body's binary size.
    /// - `compressed_size`: Binary size of parquet file, total compressed_size is this plus size of all past parquet files.
    pub fn update(&mut self, size: u64, compressed_size: u64) {
        self.size += size;
        self.compressed_size = self.prev_compressed + compressed_size;
    }
}

lazy_static! {
    #[derive(Debug)]
    // A read-write lock to allow multiple reads while and isolated write
    pub static ref STREAM_INFO: RwLock<HashMap<String, LogStreamMetadata>> =
        RwLock::new(HashMap::new());
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
        let event_json: serde_json::Value = serde_json::from_str(&event.body)?;

        let mut map = self.write().expect(LOCK_EXPECT);
        let meta = map
            .get_mut(&event.stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(
                event.stream_name.to_owned(),
            ))?;

        for alert in meta.alerts.alerts.iter_mut() {
            if alert.check_alert(&event_json).await.is_err() {
                log::error!("Error while parsing event against alerts");
            }
        }

        Ok(())
    }

    pub fn set_schema(&self, stream_name: &str, schema: Schema) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.schema.replace(schema);
            })
    }

    pub fn schema(&self, stream_name: &str) -> Result<Option<Schema>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| metadata.schema.to_owned())
    }

    pub fn set_alert(&self, stream_name: &str, alerts: Alerts) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| {
                metadata.alerts = alerts;
            })
    }

    pub fn alert(&self, stream_name: &str) -> Result<Alerts, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        map.get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_owned()))
            .map(|metadata| metadata.alerts.to_owned())
    }

    pub fn add_stream(&self, stream_name: String, schema: Option<Schema>, alerts: Alerts) {
        let mut map = self.write().expect(LOCK_EXPECT);
        let metadata = LogStreamMetadata {
            schema,
            alerts,
            ..Default::default()
        };
        map.insert(stream_name, metadata);
    }

    pub fn delete_stream(&self, stream_name: &str) {
        let mut map = self.write().expect(LOCK_EXPECT);
        map.remove(stream_name);
    }

    pub async fn load(&self, storage: &impl ObjectStorage) -> Result<(), LoadError> {
        // When loading streams this funtion will assume list_streams only returns valid streams.
        // a valid stream would have a .schema file.
        // .schema file could be empty in that case it will be treated as an uninitialized stream.
        // return error in case of an error from object storage itself.

        for stream in storage.list_streams().await? {
            let alerts = storage.get_alerts(&stream.name).await?;
            let schema = storage.get_schema(&stream.name).await?;

            let metadata = LogStreamMetadata {
                schema,
                alerts,
                ..LogStreamMetadata::default()
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

    #[allow(dead_code)]
    pub fn update_stats(
        &self,
        stream_name: &str,
        size: u64,
        compressed_size: u64,
    ) -> Result<(), MetadataError> {
        let mut map = self.write().expect(LOCK_EXPECT);
        let stream = map
            .get_mut(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_owned()))?;

        stream.stats.update(size, compressed_size);

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use maplit::hashmap;
    use rstest::*;
    use serial_test::serial;

    #[fixture]
    fn schema() -> Schema {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Boolean, false);
        Schema::new(vec![field_a, field_b])
    }

    #[rstest]
    #[case::zero(0, 0, 0)]
    #[case::some(1024, 512, 2048)]
    fn update_stats(#[case] size: u64, #[case] compressed_size: u64, #[case] prev_compressed: u64) {
        let mut stats = Stats {
            size,
            compressed_size,
            prev_compressed,
        };

        stats.update(2056, 2000);

        assert_eq!(
            stats,
            Stats {
                size: size + 2056,
                compressed_size: prev_compressed + 2000,
                prev_compressed
            }
        )
    }

    fn clear_map() {
        STREAM_INFO.write().unwrap().clear();
    }

    #[rstest]
    #[case::stream_schema_alert("teststream", Some(schema()))]
    #[case::stream_only("teststream", None)]
    #[serial]
    fn test_add_stream(#[case] stream_name: String, #[case] schema: Option<Schema>) {
        let alerts = Alerts { alerts: vec![] };
        clear_map();
        STREAM_INFO.add_stream(stream_name.clone(), schema.clone(), alerts.clone());

        let left = STREAM_INFO.read().unwrap().clone();
        let right = hashmap! {
            stream_name => LogStreamMetadata {
                schema,
                alerts,
                ..Default::default()
            }
        };
        assert_eq!(left, right);
    }

    #[rstest]
    #[case::stream_only("teststream")]
    #[serial]
    fn test_delete_stream(#[case] stream_name: String) {
        clear_map();
        STREAM_INFO.add_stream(stream_name.clone(), None, Alerts { alerts: vec![] });

        STREAM_INFO.delete_stream(&stream_name);
        let map = STREAM_INFO.read().unwrap();
        assert!(!map.contains_key(&stream_name));
    }
}
