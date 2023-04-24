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

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::alerts::Alerts;
use crate::metrics::{EVENTS_INGESTED, EVENTS_INGESTED_SIZE};
use crate::storage::{ObjectStorage, StorageDir};
use crate::utils::arrow::MergedRecordReader;

use self::error::stream_info::{CheckAlertError, LoadError, MetadataError};
use derive_more::{Deref, DerefMut};

// TODO: make return type be of 'static lifetime instead of cloning
// A read-write lock to allow multiple reads while and isolated write
pub static STREAM_INFO: Lazy<StreamInfo> = Lazy::new(StreamInfo::default);

#[derive(Debug, Deref, DerefMut, Default)]
pub struct StreamInfo(RwLock<HashMap<String, LogStreamMetadata>>);

#[derive(Debug)]
pub struct LogStreamMetadata {
    pub schema: Arc<Schema>,
    pub alerts: Alerts,
}

impl Default for LogStreamMetadata {
    fn default() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            alerts: Alerts::default(),
        }
    }
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
        rb: RecordBatch,
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

    pub fn schema(&self, stream_name: &str) -> Result<Arc<Schema>, MetadataError> {
        let map = self.read().expect(LOCK_EXPECT);
        let schema = map
            .get(stream_name)
            .ok_or(MetadataError::StreamMetaNotFound(stream_name.to_string()))
            .map(|metadata| &metadata.schema)?;

        Ok(Arc::clone(schema))
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
            let schema = storage.get_schema(&stream.name).await?;

            let schema = Arc::new(update_schema_from_staging(&stream.name, schema));

            let metadata = LogStreamMetadata { schema, alerts };

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

    pub fn update_stats(
        &self,
        stream_name: &str,
        origin: &'static str,
        size: u64,
        num_rows: u64,
    ) -> Result<(), MetadataError> {
        EVENTS_INGESTED
            .with_label_values(&[stream_name, origin])
            .inc_by(num_rows);
        EVENTS_INGESTED_SIZE
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
