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

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::Field;
use chrono::NaiveDateTime;
use error::EventError;
use itertools::Itertools;

use crate::{metadata::update_stats, parseable::Stream, storage::StreamType};

pub mod format;

pub const DEFAULT_TIMESTAMP_KEY: &str = "p_timestamp";

#[derive(Clone)]
pub struct Event {
    pub stream_name: String,
    pub rb: RecordBatch,
    pub origin_format: &'static str,
    pub origin_size: u64,
    pub is_first_event: bool,
    pub parsed_timestamp: NaiveDateTime,
    pub time_partition: Option<String>,
    pub custom_partition_values: HashMap<String, String>,
    pub stream_type: StreamType,
}

// Events holds the schema related to a each event for a single log stream
impl Event {
    pub async fn process(self, stream: &Stream) -> Result<(), EventError> {
        let mut key = get_schema_key(&self.rb.schema().fields);
        if self.time_partition.is_some() {
            let parsed_timestamp_to_min = self.parsed_timestamp.format("%Y%m%dT%H%M").to_string();
            key.push_str(&parsed_timestamp_to_min);
        }

        if !self.custom_partition_values.is_empty() {
            for (k, v) in self.custom_partition_values.iter().sorted_by_key(|v| v.0) {
                key.push_str(&format!("&{k}={v}"));
            }
        }

        if self.is_first_event {
            stream.commit_schema(self.rb.schema())?;
        }

        stream.push(
            &key,
            &self.rb,
            self.parsed_timestamp,
            &self.custom_partition_values,
            self.stream_type,
        )?;

        update_stats(
            &self.stream_name,
            self.origin_format,
            self.origin_size,
            self.rb.num_rows(),
            self.parsed_timestamp,
        );

        crate::livetail::LIVETAIL.process(&self.stream_name, &self.rb);

        Ok(())
    }

    pub fn process_unchecked(&self, stream: &Stream) -> Result<(), EventError> {
        let key = get_schema_key(&self.rb.schema().fields);

        stream.push(
            &key,
            &self.rb,
            self.parsed_timestamp,
            &self.custom_partition_values,
            self.stream_type,
        )?;

        Ok(())
    }
}

pub fn get_schema_key(fields: &[Arc<Field>]) -> String {
    // Fields must be sorted
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for field in fields.iter().sorted_by_key(|v| v.name()) {
        hasher.update(field.name().as_bytes());
    }
    let hash = hasher.digest();
    format!("{hash:x}")
}

pub mod error {
    use arrow_schema::ArrowError;

    use crate::{parseable::StagingError, storage::ObjectStorageError};

    #[derive(Debug, thiserror::Error)]
    pub enum EventError {
        #[error("Stream Writer Failed: {0}")]
        StreamWriter(#[from] StagingError),
        #[error("Stream Writer Failed: {0}")]
        Arrow(#[from] ArrowError),
        #[error("ObjectStorage Error: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
    }
}
