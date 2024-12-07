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

pub mod detect_schema;
pub mod format;
pub mod kubernetes_events;
mod writer;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Fields, Schema};
use itertools::Itertools;
use std::sync::Arc;

use self::error::EventError;
pub use self::writer::STREAM_WRITERS;
use crate::{handlers::http::ingest::PostError, metadata, storage::StreamType};
use chrono::NaiveDateTime;
use std::collections::HashMap;

pub const DEFAULT_TIMESTAMP_KEY: &str = "p_timestamp";
pub const DEFAULT_TAGS_KEY: &str = "p_tags";
pub const DEFAULT_METADATA_KEY: &str = "p_metadata";

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
    pub async fn process(&self) -> Result<(), EventError> {
        let mut key = get_schema_key(&self.rb.schema().fields);
        if self.time_partition.is_some() {
            let parsed_timestamp_to_min = self.parsed_timestamp.format("%Y%m%dT%H%M").to_string();
            key = format!("{key}{parsed_timestamp_to_min}");
        }

        if !self.custom_partition_values.is_empty() {
            let mut custom_partition_key = String::default();
            for (k, v) in self.custom_partition_values.iter().sorted_by_key(|v| v.0) {
                custom_partition_key = format!("{custom_partition_key}&{k}={v}");
            }
            key = format!("{key}{custom_partition_key}");
        }

        let num_rows = self.rb.num_rows() as u64;
        if self.is_first_event {
            commit_schema(&self.stream_name, self.rb.schema())?;
        }

        Self::process_event(
            &self.stream_name,
            &key,
            self.rb.clone(),
            self.parsed_timestamp,
            &self.custom_partition_values,
            &self.stream_type,
        )?;

        metadata::STREAM_INFO.update_stats(
            &self.stream_name,
            self.origin_format,
            self.origin_size,
            num_rows,
            self.parsed_timestamp,
        )?;

        crate::livetail::LIVETAIL.process(&self.stream_name, &self.rb);

        if let Err(e) = metadata::STREAM_INFO
            .check_alerts(&self.stream_name, &self.rb)
            .await
        {
            log::error!("Error checking for alerts. {:?}", e);
        }

        Ok(())
    }

    pub fn process_unchecked(&self) -> Result<(), PostError> {
        let key = get_schema_key(&self.rb.schema().fields);

        Self::process_event(
            &self.stream_name,
            &key,
            self.rb.clone(),
            self.parsed_timestamp,
            &self.custom_partition_values,
            &self.stream_type,
        )
        .map_err(PostError::Event)
    }

    pub fn clear(&self, stream_name: &str) {
        STREAM_WRITERS.clear(stream_name);
    }

    // event process all events after the 1st event. Concatenates record batches
    // and puts them in memory store for each event.
    fn process_event(
        stream_name: &str,
        schema_key: &str,
        rb: RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: &StreamType,
    ) -> Result<(), EventError> {
        STREAM_WRITERS.append_to_local(
            stream_name,
            schema_key,
            rb,
            parsed_timestamp,
            custom_partition_values.clone(),
            stream_type,
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

pub fn commit_schema(stream_name: &str, schema: Arc<Schema>) -> Result<(), EventError> {
    let mut stream_metadata = metadata::STREAM_INFO.write().expect("lock poisoned");

    let map = &mut stream_metadata
        .get_mut(stream_name)
        .expect("map has entry for this stream name")
        .schema;
    let current_schema = Schema::new(map.values().cloned().collect::<Fields>());
    let schema = Schema::try_merge(vec![current_schema, schema.as_ref().clone()])?;
    map.clear();
    map.extend(schema.fields.iter().map(|f| (f.name().clone(), f.clone())));
    Ok(())
}

pub mod error {
    use arrow_schema::ArrowError;

    use crate::metadata::error::stream_info::MetadataError;
    use crate::storage::ObjectStorageError;

    use super::writer::errors::StreamWriterError;

    #[derive(Debug, thiserror::Error)]
    pub enum EventError {
        #[error("Stream Writer Failed: {0}")]
        StreamWriter(#[from] StreamWriterError),
        #[error("Metadata Error: {0}")]
        Metadata(#[from] MetadataError),
        #[error("Stream Writer Failed: {0}")]
        Arrow(#[from] ArrowError),
        #[error("ObjectStorage Error: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
    }
}
