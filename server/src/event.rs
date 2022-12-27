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
use actix_web::rt::spawn;
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::StreamWriter;

use datafusion::arrow::json::reader::{infer_json_schema_from_iterator, Decoder, DecoderOptions};
use datafusion::arrow::record_batch::RecordBatch;
use lazy_static::lazy_static;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::RwLock;

use crate::metadata;
use crate::metadata::LOCK_EXPECT;
use crate::option::CONFIG;
use crate::storage::StorageDir;
use crate::utils::merge;

use self::error::{EventError, StreamWriterError};

type LocalWriter = Mutex<Option<StreamWriter<std::fs::File>>>;
type LocalWriterGuard<'a> = MutexGuard<'a, Option<StreamWriter<std::fs::File>>>;

lazy_static! {
    #[derive(Default)]
    pub static ref STREAM_WRITERS: RwLock<HashMap<String, LocalWriter>> = RwLock::new(HashMap::new());
}

impl STREAM_WRITERS {
    // append to a existing stream
    fn append_to_local(stream: &str, record: &RecordBatch) -> Result<(), StreamWriterError> {
        let hashmap_guard = STREAM_WRITERS
            .read()
            .map_err(|_| StreamWriterError::RwPoisoned)?;

        match hashmap_guard.get(stream) {
            Some(localwriter) => {
                let mut writer_guard = localwriter
                    .lock()
                    .map_err(|_| StreamWriterError::MutexPoisoned)?;

                // if it's some writer then we write without dropping any lock
                // hashmap cannot be brought mutably at any point until this finishes
                if let Some(ref mut writer) = *writer_guard {
                    writer.write(record).map_err(StreamWriterError::Writer)?;
                } else {
                    // pass on this mutex to set entry so that it can be reused
                    // we have a guard for underlying entry thus
                    // hashmap must not be availible as mutable to any other thread
                    STREAM_WRITERS::set_entry(writer_guard, stream, record)?;
                }
            }
            // entry is not present thus we create it
            None => {
                // this requires mutable borrow of the map so we drop this read lock and wait for write lock
                drop(hashmap_guard);
                STREAM_WRITERS::create_entry(stream.to_string(), record)?;
            }
        };
        Ok(())
    }

    // create a new entry with new stream_writer
    // Only create entry for valid streams
    fn create_entry(stream: String, record: &RecordBatch) -> Result<(), StreamWriterError> {
        let mut hashmap_guard = STREAM_WRITERS
            .write()
            .map_err(|_| StreamWriterError::RwPoisoned)?;

        let stream_writer = init_new_stream_writer_file(&stream, record)?;

        hashmap_guard.insert(stream, Mutex::new(Some(stream_writer)));

        Ok(())
    }

    // Deleting a logstream requires that metadata is deleted first
    pub fn delete_entry(stream: &str) -> Result<(), StreamWriterError> {
        let mut hashmap_guard = STREAM_WRITERS
            .write()
            .map_err(|_| StreamWriterError::RwPoisoned)?;

        hashmap_guard.remove(stream);

        Ok(())
    }

    fn set_entry(
        mut writer_guard: LocalWriterGuard,
        stream: &str,
        record: &RecordBatch,
    ) -> Result<(), StreamWriterError> {
        let stream_writer = init_new_stream_writer_file(stream, record)?;

        writer_guard.replace(stream_writer); // replace the stream writer behind this mutex

        Ok(())
    }

    pub fn unset_all() -> Result<(), StreamWriterError> {
        let map = STREAM_WRITERS
            .read()
            .map_err(|_| StreamWriterError::RwPoisoned)?;

        for writer in map.values() {
            if let Some(mut streamwriter) = writer
                .lock()
                .map_err(|_| StreamWriterError::MutexPoisoned)?
                .take()
            {
                let _ = streamwriter.finish();
            }
        }

        Ok(())
    }
}

fn init_new_stream_writer_file(
    stream_name: &str,
    record: &RecordBatch,
) -> Result<StreamWriter<std::fs::File>, StreamWriterError> {
    let dir = StorageDir::new(stream_name);
    let path = dir.path_by_current_time();

    std::fs::create_dir_all(dir.data_path)?;

    let file = OpenOptions::new().create(true).append(true).open(path)?;

    let mut stream_writer = StreamWriter::try_new(file, &record.schema())
        .expect("File and RecordBatch both are checked");

    stream_writer
        .write(record)
        .map_err(StreamWriterError::Writer)?;

    Ok(stream_writer)
}

#[derive(Clone)]
pub struct Event {
    pub body: Value,
    pub stream_name: String,
}

// Events holds the schema related to a each event for a single log stream
impl Event {
    pub async fn process(mut self) -> Result<(), EventError> {
        let stream_schema = metadata::STREAM_INFO.schema(&self.stream_name)?;

        if let Some(existing_schema) = stream_schema {
            // validate schema before processing the event
            if existing_schema.column_with_name("p_datetime").is_some() {
                let time = Utc::now();
                merge(
                    &mut self.body,
                    std::iter::once((
                        "p_datetime".to_string(),
                        Value::from(time.timestamp_millis()),
                    )),
                )
            }

            let Ok(event) = self.get_record(existing_schema) else {
                return Err(EventError::SchemaMismatch(self.stream_name.clone()));
            };
            self.process_event(&event)?;
        } else {
            // if stream schema is none then it is first event,
            // process first event and store schema in obect store

            let mut schema = self.infer_schema()?;
            let mut event = self.get_record(schema.clone())?;

            // check for a possible datetime field
            let time_field = get_datetime_field(&self.body);

            if time_field.is_none() {
                schema = Schema::try_merge(vec![
                    Schema::new(vec![Field::new(
                        "p_timestamp",
                        DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".to_string())),
                        false,
                    )]),
                    schema,
                ])?;

                let array: Vec<(&str, Arc<dyn Array + 'static>)> = vec![(
                    "p_timestamp",
                    Arc::new(get_timestamp_array(event.num_rows())),
                )];
                event = merge_records(Arc::new(schema.clone()), event, array);
            }

            self.process_first_event(&event, schema)?
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

        let mut stream_metadata = metadata::STREAM_INFO.write().expect(LOCK_EXPECT);
        // if the metadata is not none after acquiring lock
        // then some other thread has already completed this function.
        if _schema_with_map(stream_name, &stream_metadata).is_some() {
            // drop the lock
            drop(stream_metadata);
            // Try to post event usual way
            log::info!("first event is redirected to process_event");
            self.process_event(event)
        } else {
            // stream metadata is still none,
            // this means this execution should be considered as first event.

            // Store record batch on local cache
            log::info!("creating local writer for this first event");
            self.process_event(event)?;

            log::info!("schema is set in memory map for logstream {}", stream_name);
            _set_schema_with_map(stream_name, schema.clone(), &mut stream_metadata);
            // drop mutex before going across await point
            drop(stream_metadata);

            log::info!(
                "setting schema on objectstore for logstream {}",
                stream_name
            );
            let storage = CONFIG.storage().get_object_store();

            let stream_name = stream_name.clone();
            spawn(async move {
                if let Err(e) = storage.put_schema(&stream_name, &schema).await {
                    // If this call has failed then currently there is no right way to make local state consistent
                    // this needs a fix after more constraints are safety guarentee is provided by localwriter and objectstore_sync.
                    // Reasoning -
                    // - After dropping lock many events may process through
                    // - Processed events may sync before metadata deletion
                    log::error!(
                        "Parseable failed to upload schema to objectstore due to error {}",
                        e
                    );
                    log::error!("Please manually delete this logstream and create a new one.");
                    metadata::STREAM_INFO.delete_stream(&stream_name);
                }
            });

            Ok(())
        }
    }

    // event process all events after the 1st event. Concatenates record batches
    // and puts them in memory store for each event.
    fn process_event(&self, rb: &RecordBatch) -> Result<(), EventError> {
        STREAM_WRITERS::append_to_local(&self.stream_name, rb)?;
        Ok(())
    }

    // inferSchema is a constructor to Schema
    // returns raw arrow schema type and arrow schema to string type.
    fn infer_schema(&self) -> Result<Schema, ArrowError> {
        let iter = std::iter::once(Ok(self.body.clone()));
        infer_json_schema_from_iterator(iter)
    }

    fn get_record(&self, arrow_schema: Schema) -> Result<RecordBatch, EventError> {
        let mut iter = std::iter::once(Ok(self.body.clone()));
        let record =
            Decoder::new(Arc::new(arrow_schema), DecoderOptions::new()).next_batch(&mut iter)?;

        record.ok_or(EventError::MissingRecord)
    }
}

fn merge_records(
    schema: Arc<Schema>,
    batch: RecordBatch,
    arrays: Vec<(&str, Arc<dyn Array + 'static>)>,
) -> RecordBatch {
    let mut new_batch: Vec<Arc<dyn Array>> = batch.columns().to_vec();
    let arrays_iter: Vec<(usize, Arc<dyn Array>)> = arrays
        .into_iter()
        .map(|(s, v)| (schema.index_of(s).unwrap(), v))
        .collect();

    for (index, arr) in arrays_iter {
        new_batch.insert(index, arr);
    }

    RecordBatch::try_new(schema, new_batch).unwrap()
}

fn get_timestamp_array(size: usize) -> TimestampMillisecondArray {
    let time = Utc::now().naive_utc();
    TimestampMillisecondArray::from_value(time.timestamp_millis(), size)
        .with_timezone("+00:00".to_string())
}

fn get_datetime_field(json: &Value) -> Option<&str> {
    const TIME_KEYS: &[&str] = &["time", "datetime", "timestamp"];

    let Value::Object(object) = json else { panic!() };
    for (key, value) in object {
        if TIME_KEYS.contains(&key.as_str()) {
            if let Value::String(maybe_datetime) = value {
                if DateTime::parse_from_rfc3339(maybe_datetime).is_ok() {
                    return Some(key);
                }
            }
        }
    }
    None
}

//  Special functions which reads from metadata map while holding the lock
#[inline]
pub fn _schema_with_map(
    stream_name: &str,
    map: &impl Deref<Target = HashMap<String, metadata::LogStreamMetadata>>,
) -> Option<Schema> {
    map.get(stream_name)
        .expect("map has entry for this stream name")
        .schema
        .to_owned()
}

#[inline]
//  Special functions which writes to metadata map while holding the lock
pub fn _set_schema_with_map(
    stream_name: &str,
    schema: Schema,
    map: &mut impl DerefMut<Target = HashMap<String, metadata::LogStreamMetadata>>,
) {
    map.get_mut(stream_name)
        .expect("map has entry for this stream name")
        .schema
        .replace(schema);
}

pub mod error {
    use crate::metadata::error::stream_info::MetadataError;
    use crate::storage::ObjectStorageError;
    use datafusion::arrow::error::ArrowError;

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
        #[error("Schema Mismatch: {0}")]
        SchemaMismatch(String),
        #[error("Schema Mismatch: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum StreamWriterError {
        #[error("Arrow writer failed: {0}")]
        Writer(#[from] ArrowError),
        #[error("Io Error when creating new file: {0}")]
        Io(#[from] std::io::Error),
        #[error("RwLock was poisoned")]
        RwPoisoned,
        #[error("Mutex was poisoned")]
        MutexPoisoned,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{Field, Schema};
    use datafusion::arrow::{
        array::{Array, Int32Array},
        record_batch::RecordBatch,
    };

    use super::merge_records;

    #[test]
    fn check_merge() {
        let schema = Schema::new(vec![
            Field::new("a", arrow_schema::DataType::Int32, false),
            Field::new("b", arrow_schema::DataType::Int32, false),
        ]);

        let rb = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from_value(0, 3)),
                Arc::new(Int32Array::from_value(0, 3)),
            ],
        )
        .unwrap();

        let new_schema = Schema::try_merge(vec![
            schema,
            Schema::new(vec![Field::new("c", arrow_schema::DataType::Int32, false)]),
        ])
        .unwrap();

        let arr: Vec<(&str, Arc<dyn Array + 'static>)> =
            vec![("c", Arc::new(Int32Array::from_value(0, 3)))];

        let new_rb = merge_records(Arc::new(new_schema.clone()), rb, arr);

        assert_eq!((*new_rb.schema()).clone(), new_schema);
        assert_eq!(new_rb.num_columns(), 3);
        assert_eq!(new_rb.num_rows(), 3)
    }

    #[test]
    fn check_merge_empty_record() {
        let schema = Schema::new(vec![
            Field::new("a", arrow_schema::DataType::Int32, false),
            Field::new("b", arrow_schema::DataType::Int32, false),
        ]);

        let rb = RecordBatch::new_empty(Arc::new(schema.clone()));

        let new_schema = Schema::try_merge(vec![
            schema,
            Schema::new(vec![Field::new("c", arrow_schema::DataType::Int32, false)]),
        ])
        .unwrap();

        let arr: Vec<(&str, Arc<dyn Array + 'static>)> =
            vec![("c", Arc::new(Int32Array::from_value(0, 0)))];

        let new_rb = merge_records(Arc::new(new_schema.clone()), rb, arr);

        assert_eq!((*new_rb.schema()).clone(), new_schema);
        assert_eq!(new_rb.num_columns(), 3);
        assert_eq!(new_rb.num_rows(), 0)
    }
}
