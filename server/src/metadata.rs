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
use std::sync::Mutex;

use crate::error::Error;
use crate::storage;

#[derive(Debug)]
pub struct LogStreamMetadata {
    pub schema: String,
    pub alert: String,
}

lazy_static! {
    #[derive(Debug)]
    pub static ref STREAM_INFO: Mutex<HashMap<String, Box<LogStreamMetadata>>> =
        Mutex::new(HashMap::new());
}

// This will be updated
// 1. During server start up
// 2. When a new stream is created (make a new entry in the map)
// 3. When a stream is deleted (remove the entry from the map)
// 4. When first event is sent to stream (update the schema)
// 5. In set_alert API (update the alert)
#[allow(clippy::all)]
impl STREAM_INFO {
    pub fn schema(&self, stream_name: String) -> Result<String, Error> {
        let map = self.lock().map_err(|_| Error::StreamLock)?;
        let meta = map.get(&stream_name);
        drop(&map);

        match meta {
            Some(meta) => Ok(meta.schema.clone()),
            None => Err(Error::StreamMetaNotFound(stream_name)),
        }
    }

    pub fn alert(&self, stream_name: String) -> Result<String, Error> {
        let map = self.lock().map_err(|_| Error::StreamLock)?;
        let meta = map.get(&stream_name);
        drop(&map);

        match meta {
            Some(meta) => Ok(meta.alert.clone()),
            None => Err(Error::StreamMetaNotFound(stream_name)),
        }
    }

    pub fn set_alert(&self, stream_name: String, alert_config: String) -> Result<(), Error> {
        let schema = self.schema(stream_name.clone())?;
        let mut map = self.lock().map_err(|_| Error::StreamLock)?;
        let metadata = LogStreamMetadata {
            schema: schema,
            alert: alert_config,
        };
        map.insert(stream_name, Box::new(metadata));
        drop(&map);
        Ok(())
    }

    pub fn set_schema(&self, stream_name: String, schema: String) -> Result<(), Error> {
        let alert_config = self.alert(stream_name.clone())?;
        let mut map = self.lock().map_err(|_| Error::StreamLock)?;
        let metadata = LogStreamMetadata {
            schema: schema,
            alert: alert_config,
        };
        map.insert(stream_name, Box::new(metadata));
        drop(&map);
        Ok(())
    }

    pub fn add_stream(
        &self,
        stream_name: String,
        schema: String,
        alert_config: String,
    ) -> Result<(), Error> {
        let mut map = self.lock().map_err(|_| Error::StreamLock)?;
        let metadata = LogStreamMetadata {
            schema: schema,
            alert: alert_config,
        };
        map.insert(stream_name, Box::new(metadata));
        drop(&map);
        Ok(())
    }

    pub fn delete_stream(&self, stream_name: String) -> Result<(), Error> {
        let mut map = self.lock().map_err(|_| Error::StreamLock)?;
        map.remove(&stream_name);
        drop(&map);
        Ok(())
    }

    pub fn load(&self) -> Result<(), Error> {
        match storage::list_streams() {
            Ok(streams) => {
                for stream in streams {
                    let stream_name = stream.name;
                    let mut alert_config = String::new();
                    let mut schema = String::new();
                    match storage::alert_exists(&stream_name.clone()) {
                        Ok(x) => {
                            if x.is_empty() {
                                alert_config = "".to_string();
                            } else {
                                alert_config = format!("{:?}", x);
                            }
                        }
                        Err(_) => {
                            // Ignore S3 errors here, because
                            // we are just trying to load the
                            // stream metadata based on whatever is available
                        }
                    };
                    match storage::stream_exists(&stream_name) {
                        Ok(x) => {
                            if x.is_empty() {
                                schema = "".to_string();
                            } else {
                                schema = format!("{:?}", x);
                            }
                        }
                        Err(_) => {
                            // Ignore S3 errors here, because
                            // we are just trying to load the
                            // stream metadata based on whatever is available
                        }
                    };
                    let metadata = LogStreamMetadata {
                        schema: schema,
                        alert: alert_config,
                    };
                    let mut map = self.lock().map_err(|_| Error::StreamLock)?;
                    map.insert(stream_name, Box::new(metadata));
                    drop(&map);
                }
                Ok(())
            }
            Err(e) => Err(Error::S3(e)),
        }
    }
}
