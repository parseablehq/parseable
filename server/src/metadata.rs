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

use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::Error;
use crate::storage;

#[derive(Debug)]
pub struct LogStreamMetadata {
    pub schema: String,
    pub alert_config: String,
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
    pub fn schema(&self, stream_name: String) -> Result<String, Error> {
        let map = self.read().unwrap();
        let meta = map
            .get(&stream_name)
            .ok_or(Error::StreamMetaNotFound(stream_name))?;

        Ok(meta.schema.clone())
    }

    pub fn alert(&self, stream_name: String) -> Result<String, Error> {
        let map = self.read().unwrap();
        let meta = map
            .get(&stream_name)
            .ok_or(Error::StreamMetaNotFound(stream_name))?;

        Ok(meta.alert_config.clone())
    }

    pub fn set_alert(&self, stream_name: String, alert_config: String) -> Result<(), Error> {
        let schema = self.schema(stream_name.clone())?;
        self.add_stream(stream_name, schema, alert_config)
    }

    pub fn set_schema(&self, stream_name: String, schema: String) -> Result<(), Error> {
        let alert_config = self.alert(stream_name.clone())?;
        self.add_stream(stream_name, schema, alert_config)
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
        };
        // TODO: Add check to confirm data insertion
        map.insert(stream_name, metadata);

        Ok(())
    }

    pub fn delete_stream(&self, stream_name: String) -> Result<(), Error> {
        let mut map = self.write().unwrap();
        // TODO: Add check to confirm data deletion
        map.remove(&stream_name);

        Ok(())
    }

    pub fn load(&self) -> Result<(), Error> {
        let streams = storage::list_streams()?;

        for stream in streams {
            // Ignore S3 errors here, because we are just trying
            // to load the stream metadata based on whatever is available.
            //
            // TODO: ignore failure(s) if any and skip to next stream
            let mut alert_config = String::new();
            if let Ok(x) = storage::alert_exists(&stream.name) {
                if !x.is_empty() {
                    alert_config = format!("{:?}", x);
                }
            }
            let mut schema = String::new();
            if let Ok(x) = storage::stream_exists(&stream.name) {
                if !x.is_empty() {
                    schema = format!("{:?}", x);
                }
            }
            let metadata = LogStreamMetadata {
                schema,
                alert_config,
            };
            let mut map = self.write().unwrap();
            map.insert(stream.name.to_owned(), metadata);
        }

        Ok(())
    }
}
