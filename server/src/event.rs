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
*
*/

pub mod format;
mod writer;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};

use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;

use crate::metadata;

use self::error::EventError;
pub use self::writer::STREAM_WRITERS;

pub const DEFAULT_TIMESTAMP_KEY: &str = "p_timestamp";
pub const DEFAULT_TAGS_KEY: &str = "p_tags";
pub const DEFAULT_METADATA_KEY: &str = "p_metadata";

#[derive(Clone)]
pub struct Event {
    pub stream_name: String,
    pub rb: RecordBatch,
    pub origin_format: &'static str,
    pub origin_size: u64,
}

// Events holds the schema related to a each event for a single log stream
impl Event {
    pub async fn process(self) -> Result<(), EventError> {
        let key = get_schema_key(&self.rb.schema().fields);
        let num_rows = self.rb.num_rows() as u64;

        if self.is_first_event(metadata::STREAM_INFO.schema(&self.stream_name)?.as_ref()) {
            commit_schema(&self.stream_name, self.rb.schema())?;
        }

        Self::process_event(&self.stream_name, &key, self.rb.clone())?;

        metadata::STREAM_INFO.update_stats(
            &self.stream_name,
            self.origin_format,
            self.origin_size,
            num_rows,
        )?;

        if let Err(e) = metadata::STREAM_INFO
            .check_alerts(&self.stream_name, self.rb)
            .await
        {
            log::error!("Error checking for alerts. {:?}", e);
        }

        Ok(())
    }

    fn is_first_event(&self, stream_schema: &Schema) -> bool {
        let mut stream_fields = stream_schema.fields().iter();
        let event_schema = self.rb.schema();
        let event_fields = event_schema.fields();

        for field in event_fields {
            loop {
                let Some(stream_field) = stream_fields.next() else { return true };
                if stream_field.name() == field.name() {
                    break;
                } else {
                    continue;
                }
            }
        }

        false
    }

    // event process all events after the 1st event. Concatenates record batches
    // and puts them in memory store for each event.
    fn process_event(
        stream_name: &str,
        schema_key: &str,
        rb: RecordBatch,
    ) -> Result<(), EventError> {
        STREAM_WRITERS.append_to_local(stream_name, schema_key, rb)?;
        Ok(())
    }
}

pub fn get_schema_key(fields: &Vec<Field>) -> String {
    // Fields must be sorted
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for field in fields {
        hasher.update(field.name().as_bytes())
    }
    let hash = hasher.digest();
    format!("{hash:x}")
}

pub fn commit_schema(stream_name: &str, schema: Arc<Schema>) -> Result<(), EventError> {
    let mut stream_metadata = metadata::STREAM_INFO.write().expect("lock poisoned");

    let mut schema = Schema::try_merge(vec![
        schema.as_ref().clone(),
        stream_metadata.get_unchecked(stream_name).as_ref().clone(),
    ])
    .unwrap();
    schema.fields.sort_by(|a, b| a.name().cmp(b.name()));

    stream_metadata.set_unchecked(stream_name, Arc::new(schema));
    Ok(())
}

trait UncheckedOp: DerefMut<Target = HashMap<String, metadata::LogStreamMetadata>> {
    fn get_unchecked(&self, stream_name: &str) -> Arc<Schema> {
        let schema = &self
            .get(stream_name)
            .expect("map has entry for this stream name")
            .schema;

        Arc::clone(schema)
    }

    fn set_unchecked(&mut self, stream_name: &str, schema: Arc<Schema>) {
        self.get_mut(stream_name)
            .expect("map has entry for this stream name")
            .schema = schema
    }
}

impl<T: DerefMut<Target = HashMap<String, metadata::LogStreamMetadata>>> UncheckedOp for T {}

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};

    use super::Event;

    fn test_rb(fields: Vec<Field>) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::new(fields)))
    }

    fn test_event(fields: Vec<Field>) -> Event {
        Event {
            stream_name: "".to_string(),
            rb: test_rb(fields),
            origin_format: "none",
            origin_size: 0,
        }
    }

    #[test]
    fn new_field_is_new_event() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);

        let new_event = test_event(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]);

        assert!(new_event.is_first_event(&schema));
    }

    #[test]
    fn same_field_not_is_new_event() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]);

        let new_event = test_event(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]);

        assert!(!new_event.is_first_event(&schema));
    }
}
