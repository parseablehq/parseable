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

mod writer;

use chrono::Utc;
use datafusion::arrow::array::{Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::reader::{infer_json_schema_from_iterator, Decoder, DecoderOptions};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;

use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;

use crate::metadata;
use crate::metadata::LOCK_EXPECT;
use crate::option::CONFIG;

use self::error::EventError;
pub use self::writer::STREAM_WRITERS;

const DEFAULT_TIMESTAMP_KEY: &str = "p_timestamp";

#[derive(Clone)]
pub struct Event {
    pub body: Value,
    pub stream_name: String,
    pub schema_key: String,
}

// Events holds the schema related to a each event for a single log stream
impl Event {
    pub async fn process(self) -> Result<(), EventError> {
        let stream_schema = metadata::STREAM_INFO.schema(&self.stream_name, &self.schema_key)?;
        if let Some(schema) = stream_schema {
            // validate schema before processing the event
            let Ok(mut event) = self.get_record(Arc::clone(&schema)) else {
                return Err(EventError::SchemaMismatch);
            };

            let rows = event.num_rows();
            let timestamp_array = Arc::new(get_timestamp_array(rows));
            event = replace(schema, event, DEFAULT_TIMESTAMP_KEY, timestamp_array);

            self.process_event(&event)?;
        } else {
            // if stream schema is none then it is first event,
            // process first event and store schema in obect store
            let schema = add_default_timestamp_field(self.infer_schema()?)?;
            let schema_ref = Arc::new(schema.clone());
            let event = self.get_record(schema_ref.clone())?;
            let timestamp_array = Arc::new(get_timestamp_array(event.num_rows()));
            let event = replace(schema_ref, event, DEFAULT_TIMESTAMP_KEY, timestamp_array);
            self.process_first_event(&event, schema)?;
        };

        metadata::STREAM_INFO.update_stats(
            &self.stream_name,
            serde_json::to_vec(&self.body)
                .map(|v| std::mem::size_of_val(v.as_slice()))
                .unwrap_or(0) as u64,
        )?;

        if let Err(e) = metadata::STREAM_INFO.check_alerts(&self).await {
            log::error!("Error checking for alerts. {:?}", e);
        }

        Ok(())
    }

    // This is called when the first event of a log stream is received. The first event is
    // special because we parse this event to generate the schema for the log stream. This
    // schema is then enforced on rest of the events sent to this log stream.
    fn process_first_event(&self, event: &RecordBatch, schema: Schema) -> Result<(), EventError> {
        // note for functions _schema_with_map and _set_schema_with_map,
        // these are to be called while holding a write lock specifically.
        // this guarantees two things
        // - no other metadata operation can happen in between
        // - map always have an entry for this stream

        let stream_name = &self.stream_name;
        let schema_key = &self.schema_key;

        commit_schema(stream_name, schema_key, Arc::new(schema))?;
        self.process_event(event)
    }

    // event process all events after the 1st event. Concatenates record batches
    // and puts them in memory store for each event.
    fn process_event(&self, rb: &RecordBatch) -> Result<(), EventError> {
        STREAM_WRITERS::append_to_local(&self.stream_name, &self.schema_key, rb)?;
        Ok(())
    }

    // inferSchema is a constructor to Schema
    // returns raw arrow schema type and arrow schema to string type.
    fn infer_schema(&self) -> Result<Schema, ArrowError> {
        let iter = std::iter::once(Ok(self.body.clone()));
        infer_json_schema_from_iterator(iter)
    }

    fn get_record(&self, schema: Arc<Schema>) -> Result<RecordBatch, EventError> {
        let mut iter = std::iter::once(Ok(self.body.clone()));
        if fields_mismatch(&schema, &self.body) {
            return Err(EventError::SchemaMismatch);
        }
        let record = Decoder::new(schema, DecoderOptions::new()).next_batch(&mut iter)?;

        record.ok_or(EventError::MissingRecord)
    }
}

fn add_default_timestamp_field(schema: Schema) -> Result<Schema, ArrowError> {
    let schema = Schema::try_merge(vec![
        Schema::new(vec![Field::new(
            DEFAULT_TIMESTAMP_KEY,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]),
        schema,
    ])?;

    Ok(schema)
}

pub fn get_schema_key(body: &Value) -> String {
    let mut list_of_fields: Vec<_> = body.as_object().unwrap().keys().collect();
    list_of_fields.sort();
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for field in list_of_fields {
        hasher.update(field.as_bytes())
    }
    let hash = hasher.digest();
    format!("{hash:x}")
}

fn fields_mismatch(schema: &Schema, body: &Value) -> bool {
    for (name, val) in body.as_object().expect("body is of object variant") {
        let Ok(field) = schema.field_with_name(name) else { return true };

        // datatype check only some basic cases
        let valid_datatype = match field.data_type() {
            DataType::Boolean => val.is_boolean(),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => val.is_i64(),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                val.is_u64()
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => val.is_f64(),
            DataType::Utf8 => val.is_string(),
            _ => false,
        };

        if !valid_datatype {
            return true;
        }
    }

    false
}

fn commit_schema(
    stream_name: &str,
    schema_key: &str,
    schema: Arc<Schema>,
) -> Result<(), EventError> {
    // note for methods .get_unchecked and .set_unchecked,
    // these are to be called while holding a write lock specifically.
    // this guarantees two things
    // - no other metadata operation can happen in between
    // - map always have an entry for this stream

    let mut stream_metadata = metadata::STREAM_INFO.write().expect(LOCK_EXPECT);
    // if the metadata is not none after acquiring lock
    // then some other thread has already completed this function.
    if stream_metadata
        .get_unchecked(stream_name, schema_key)
        .is_some()
    {
        // drop the lock
        drop(stream_metadata);
        // Nothing to do
        Ok(())
    } else {
        // set to map
        stream_metadata.set_unchecked(stream_name, schema_key, schema);
        // serialize map
        let schema_map = serde_json::to_string(
            &stream_metadata
                .get(stream_name)
                .expect("map has entry for this stream name")
                .schema,
        )
        .expect("map of schemas is serializable");
        // try to put to storage
        let storage = CONFIG.storage().get_object_store();

        let _stream_name = stream_name.to_owned();
        let handle = std::thread::spawn(move || {
            let rt = actix_web::rt::System::new();
            rt.block_on(storage.put_schema_map(&_stream_name, &schema_map))
        });

        let res = match handle.join() {
            Ok(res) => res.map_err(EventError::ObjectStorage),
            Err(_) => {
                log::error!("commit schema thread panicked");
                Err(EventError::InternalError)
            }
        };
        // revert if err
        if let Err(ref err) = res {
            stream_metadata.remove_unchecked(stream_name, schema_key);
            log::error!(
                "Failed to commit schema during new event ingestion: {}",
                err
            )
        }

        res
    }
}

fn replace(
    schema: Arc<Schema>,
    batch: RecordBatch,
    column: &str,
    arr: Arc<dyn Array + 'static>,
) -> RecordBatch {
    let (index, _) = schema.column_with_name(column).unwrap();
    let mut arrays = batch.columns().to_vec();
    arrays[index] = arr;

    RecordBatch::try_new(schema, arrays).unwrap()
}

fn get_timestamp_array(size: usize) -> TimestampMillisecondArray {
    let time = Utc::now();
    TimestampMillisecondArray::from_value(time.timestamp_millis(), size)
}

trait UncheckedOp: DerefMut<Target = HashMap<String, metadata::LogStreamMetadata>> {
    fn get_unchecked(&self, stream_name: &str, schema_key: &str) -> Option<Arc<Schema>> {
        self.get(stream_name)
            .expect("map has entry for this stream name")
            .schema
            .get(schema_key)
            .cloned()
    }

    fn set_unchecked(&mut self, stream_name: &str, schema_key: &str, schema: Arc<Schema>) {
        self.get_mut(stream_name)
            .expect("map has entry for this stream name")
            .schema
            .insert(schema_key.to_string(), schema)
            .is_some()
            .then(|| panic!("collision"));
    }

    fn remove_unchecked(&mut self, stream_name: &str, schema_key: &str) {
        self.get_mut(stream_name)
            .expect("map has entry for this stream name")
            .schema
            .remove(schema_key);
    }
}

impl<T: DerefMut<Target = HashMap<String, metadata::LogStreamMetadata>>> UncheckedOp for T {}

pub mod error {
    use crate::metadata::error::stream_info::MetadataError;
    use crate::storage::ObjectStorageError;
    use datafusion::arrow::error::ArrowError;

    use super::writer::errors::StreamWriterError;

    #[derive(Debug, thiserror::Error)]
    pub enum EventError {
        #[error("Missing Record from event body")]
        MissingRecord,
        #[error("Stream Writer Failed: {0}")]
        StreamWriter(#[from] StreamWriterError),
        #[error("Metadata Error: {0}")]
        Metadata(#[from] MetadataError),
        #[error("Stream Writer Failed: {0}")]
        Arrow(#[from] ArrowError),
        #[error("Schema Mismatch")]
        SchemaMismatch,
        #[error("ObjectStorage Error: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
        #[error("Internal Error")]
        InternalError,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::{
        array::{Array, Int32Array},
        record_batch::RecordBatch,
    };

    use crate::event::replace;

    #[test]
    fn check_replace() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let schema_ref = Arc::new(schema);

        let rb = RecordBatch::try_new(
            schema_ref.clone(),
            vec![
                Arc::new(Int32Array::from_value(0, 3)),
                Arc::new(Int32Array::from_value(0, 3)),
                Arc::new(Int32Array::from_value(0, 3)),
            ],
        )
        .unwrap();

        let arr: Arc<dyn Array + 'static> = Arc::new(Int32Array::from_value(0, 3));

        let new_rb = replace(schema_ref.clone(), rb, "c", arr);

        assert_eq!(new_rb.schema(), schema_ref);
        assert_eq!(new_rb.num_columns(), 3);
        assert_eq!(new_rb.num_rows(), 3)
    }
}
