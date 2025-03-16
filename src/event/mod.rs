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

pub mod format;

use arrow_array::RecordBatch;
use arrow_schema::Field;
use itertools::Itertools;
use std::sync::Arc;

use self::error::EventError;
use crate::{metadata::update_stats, parseable::Stream, storage::StreamType};
use chrono::NaiveDateTime;
use std::collections::HashMap;

pub const DEFAULT_TIMESTAMP_KEY: &str = "p_timestamp";

#[derive(Debug)]
pub struct PartitionEvent {
    pub rb: RecordBatch,
    pub parsed_timestamp: NaiveDateTime,
}

#[derive(Debug)]
pub struct Event {
    pub origin_format: &'static str,
    pub origin_size: usize,
    pub is_first_event: bool,
    pub partitions: HashMap<String, PartitionEvent>,
    pub stream_type: StreamType,
}

// Events holds the schema related to a each event for a single log stream
impl Event {
    pub fn process(self, stream: &Stream) -> Result<(), EventError> {
        for (
            prefix,
            PartitionEvent {
                rb,
                parsed_timestamp,
            },
        ) in self.partitions
        {
            if self.is_first_event {
                stream.commit_schema(rb.schema().as_ref().clone())?;
            }
            stream.push(&prefix, parsed_timestamp, &rb, self.stream_type)?;

            update_stats(
                &stream.stream_name,
                self.origin_format,
                self.origin_size,
                rb.num_rows(),
                parsed_timestamp.date(),
            );

            crate::livetail::LIVETAIL.process(&stream.stream_name, &rb);
        }
        Ok(())
    }

    pub fn process_unchecked(&self, stream: &Stream) -> Result<(), EventError> {
        for (prefix, partition) in &self.partitions {
            stream.push(
                prefix,
                partition.parsed_timestamp,
                &partition.rb,
                self.stream_type,
            )?;
        }

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

    use crate::{parseable::StagingError, storage::ObjectStorageError};

    #[derive(Debug, thiserror::Error)]
    pub enum EventError {
        #[error("Staging Failed: {0}")]
        Staging(#[from] StagingError),
        #[error("ObjectStorage Error: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
    }
}
