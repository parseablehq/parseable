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
use arrow_schema::{Field, Fields, Schema};
use itertools::Itertools;
use std::sync::Arc;

use self::error::EventError;
use crate::{
    metadata::update_stats,
    parseable::{StagingError, PARSEABLE},
    storage::StreamType,
    LOCK_EXPECT,
};
use chrono::NaiveDateTime;
use std::collections::HashMap;

pub const DEFAULT_TIMESTAMP_KEY: &str = "p_timestamp";

pub struct PartitionEvent {
    pub rb: RecordBatch,
    pub parsed_timestamp: NaiveDateTime,
    pub custom_partition_values: HashMap<String, String>,
}

pub struct Event {
    pub stream_name: String,
    pub origin_format: &'static str,
    pub origin_size: u64,
    pub is_first_event: bool,
    pub time_partition: Option<String>,
    pub partitions: Vec<PartitionEvent>,
    pub stream_type: StreamType,
}

// Events holds the schema related to a each event for a single log stream
impl Event {
    pub fn process(self) -> Result<(), EventError> {
        for partition in self.partitions {
            let mut key = get_schema_key(&partition.rb.schema().fields);
            if self.time_partition.is_some() {
                let parsed_timestamp_to_min =
                    partition.parsed_timestamp.format("%Y%m%dT%H%M").to_string();
                key.push_str(&parsed_timestamp_to_min);
            }

            for (k, v) in partition
                .custom_partition_values
                .iter()
                .sorted_by_key(|v| v.0)
            {
                key.push_str(&format!("&{k}={v}"));
            }

            if self.is_first_event {
                commit_schema(&self.stream_name, partition.rb.schema())?;
            }

            PARSEABLE.get_or_create_stream(&self.stream_name).push(
                &key,
                &partition.rb,
                partition.parsed_timestamp,
                &partition.custom_partition_values,
                self.stream_type,
            )?;

            update_stats(
                &self.stream_name,
                self.origin_format,
                self.origin_size,
                partition.rb.num_rows(),
                partition.parsed_timestamp.date(),
            );

            crate::livetail::LIVETAIL.process(&self.stream_name, &partition.rb);
        }
        Ok(())
    }

    pub fn process_unchecked(&self) -> Result<(), EventError> {
        for partition in &self.partitions {
            let key = get_schema_key(&partition.rb.schema().fields);

            PARSEABLE.get_or_create_stream(&self.stream_name).push(
                &key,
                &partition.rb,
                partition.parsed_timestamp,
                &partition.custom_partition_values,
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

pub fn commit_schema(stream_name: &str, schema: Arc<Schema>) -> Result<(), StagingError> {
    let mut stream_metadata = PARSEABLE.streams.write().expect("lock poisoned");

    let map = &mut stream_metadata
        .get_mut(stream_name)
        .expect("map has entry for this stream name")
        .metadata
        .write()
        .expect(LOCK_EXPECT)
        .schema;
    let current_schema = Schema::new(map.values().cloned().collect::<Fields>());
    let schema = Schema::try_merge(vec![current_schema, schema.as_ref().clone()])?;
    map.clear();
    map.extend(schema.fields.iter().map(|f| (f.name().clone(), f.clone())));
    Ok(())
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
