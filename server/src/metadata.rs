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

use bytes::Bytes;
use lazy_static::lazy_static;
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::Error;
use crate::storage::ObjectStorage;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LogStreamMetadata {
    pub schema: String,
    pub alert_config: String,
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

// STREAM_INFO should be updated
// 1. During server start up
// 2. When a new stream is created (make a new entry in the map)
// 3. When a stream is deleted (remove the entry from the map)
// 4. When first event is sent to stream (update the schema)
// 5. When set alert API is called (update the alert)
#[allow(clippy::all)]
impl STREAM_INFO {
    pub fn set_schema(&self, stream_name: String, schema: String) -> Result<(), Error> {
        let alert_config = self.alert(&stream_name)?;
        self.add_stream(stream_name, schema, alert_config)
    }

    pub fn schema(&self, stream_name: &str) -> Result<String, Error> {
        let map = self.read().unwrap();
        let meta = map
            .get(stream_name)
            .ok_or(Error::StreamMetaNotFound(stream_name.to_string()))?;

        Ok(meta.schema.clone())
    }

    pub fn set_alert(&self, stream_name: String, alert_config: String) -> Result<(), Error> {
        let schema = self.schema(&stream_name)?;
        self.add_stream(stream_name, schema, alert_config)
    }

    pub fn alert(&self, stream_name: &str) -> Result<String, Error> {
        let map = self.read().unwrap();
        let meta = map
            .get(stream_name)
            .ok_or(Error::StreamMetaNotFound(stream_name.to_owned()))?;

        Ok(meta.alert_config.clone())
    }

    pub fn add_stream(
        &self,
        stream_name: String,
        schema: String,
        alert_config: String,
    ) -> Result<(), Error> {
        let mut map = self.write().unwrap();
        let metadata = LogStreamMetadata {
            schema,
            alert_config,
            ..Default::default()
        };
        // TODO: Add check to confirm data insertion
        map.insert(stream_name, metadata);

        Ok(())
    }

    pub fn delete_stream(&self, stream_name: &str) -> Result<(), Error> {
        let mut map = self.write().unwrap();
        // TODO: Add check to confirm data deletion
        map.remove(stream_name);

        Ok(())
    }

    pub async fn load(&self, storage: &impl ObjectStorage) -> Result<(), Error> {
        for stream in storage.list_streams().await? {
            // Ignore S3 errors here, because we are just trying
            // to load the stream metadata based on whatever is available.
            //
            // TODO: ignore failure(s) if any and skip to next stream
            let alert_config = parse_string(storage.get_alert(&stream.name).await)
                .map_err(|_| Error::AlertNotInStore(stream.name.to_owned()))?;
            let schema = parse_string(storage.get_schema(&stream.name).await)
                .map_err(|_| Error::SchemaNotInStore(stream.name.to_owned()))?;
            let metadata = LogStreamMetadata {
                schema,
                alert_config,
                ..Default::default()
            };
            let mut map = self.write().unwrap();
            map.insert(stream.name.to_owned(), metadata);
        }

        Ok(())
    }

    pub fn update_stats(
        &self,
        stream_name: &str,
        size: u64,
        compressed_size: u64,
    ) -> Result<(), Error> {
        let mut map = self.write().unwrap();
        let stream = map
            .get_mut(stream_name)
            .ok_or(Error::StreamMetaNotFound(stream_name.to_owned()))?;

        stream.stats.update(size, compressed_size);

        Ok(())
    }
}

fn parse_string(result: Result<Bytes, Error>) -> Result<String, Error> {
    let mut string = String::new();
    let bytes = match result {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("Storage error: {}", e);
            return Ok(string);
        }
    };

    if !bytes.is_empty() {
        string = String::from_utf8(bytes.to_vec())?;
    }

    Ok(string)
}
