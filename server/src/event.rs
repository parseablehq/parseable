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
 *
 */
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json;
use datafusion::arrow::json::reader::infer_json_schema;
use datafusion::arrow::record_batch::RecordBatch;
use lazy_static::lazy_static;
use log::error;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::RwLock;

use crate::metadata;
use crate::option::CONFIG;
use crate::storage::ObjectStorage;

use self::error::EventError;

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

        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(data_file_path(&stream))
            .map_err(StreamWriterError::Io)?;

        let mut stream_writer = StreamWriter::try_new(file, &record.schema())
            .expect("File and RecordBatch both are checked");

        stream_writer
            .write(record)
            .map_err(StreamWriterError::Writer)?;

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
        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(data_file_path(stream))
            .map_err(StreamWriterError::Io)?;

        let mut stream_writer = StreamWriter::try_new(file, &record.schema())
            .expect("File and RecordBatch both are checked");

        stream_writer
            .write(record)
            .map_err(StreamWriterError::Writer)?;

        writer_guard.replace(stream_writer); // replace the stream writer behind this mutex

        Ok(())
    }

    // Unset the entry so that
    pub fn unset_entry(stream: &str) -> Result<(), StreamWriterError> {
        let guard = STREAM_WRITERS
            .read()
            .map_err(|_| StreamWriterError::RwPoisoned)?;
        let stream_writer = match guard.get(stream) {
            Some(writer) => writer,
            None => return Ok(()),
        };
        stream_writer
            .lock()
            .map_err(|_| StreamWriterError::MutexPoisoned)?
            .take();

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StreamWriterError {
    #[error("Arrow writer failed: {0}")]
    Writer(arrow::error::ArrowError),
    #[error("Io Error when creating new file: {0}")]
    Io(std::io::Error),
    #[error("RwLock was poisoned")]
    RwPoisoned,
    #[error("Mutex was poisoned")]
    MutexPoisoned,
}

fn data_file_path(stream_name: &str) -> String {
    format!(
        "{}/{}",
        CONFIG
            .parseable
            .local_stream_data_path(stream_name)
            .to_string_lossy(),
        "data.records"
    )
}

#[derive(Clone)]
pub struct Event {
    pub body: String,
    pub stream_name: String,
}

// Events holds the schema related to a each event for a single log stream

impl Event {
    pub async fn process(&self, storage: &impl ObjectStorage) -> Result<(), EventError> {
        let inferred_schema = self.infer_schema()?;

        let event = self.get_reader(inferred_schema.clone());

        let stream_schema = metadata::STREAM_INFO.schema(&self.stream_name)?;

        if let Some(existing_schema) = stream_schema {
            // validate schema before processing the event
            if existing_schema != inferred_schema {
                return Err(EventError::SchemaMismatch(self.stream_name.clone()));
            } else {
                self.process_event(event)?
            }
        } else {
            // if stream schema is none then it is first event,
            // process first event and store schema in obect store
            self.process_first_event(event, inferred_schema, storage)
                .await?
        };

        if let Err(e) = metadata::STREAM_INFO.check_alerts(self).await {
            error!("Error checking for alerts. {:?}", e);
        }

        Ok(())
    }

    // This is called when the first event of a log stream is received. The first event is
    // special because we parse this event to generate the schema for the log stream. This
    // schema is then enforced on rest of the events sent to this log stream.
    async fn process_first_event<R: std::io::Read>(
        &self,
        mut event: json::Reader<R>,
        schema: Schema,
        storage: &impl ObjectStorage,
    ) -> Result<u64, EventError> {
        let rb = event.next()?.ok_or(EventError::MissingRecord)?;
        let stream_name = &self.stream_name;

        // Store record batch on local cache
        STREAM_WRITERS::create_entry(stream_name.clone(), &rb).unwrap();

        // Put the inferred schema to object store
        storage.put_schema(stream_name.clone(), &schema).await?;

        // set the schema in memory for this stream
        metadata::STREAM_INFO.set_schema(stream_name, schema)?;

        Ok(0)
    }

    // event process all events after the 1st event. Concatenates record batches
    // and puts them in memory store for each event.
    fn process_event<R: std::io::Read>(
        &self,
        mut event: json::Reader<R>,
    ) -> Result<u64, EventError> {
        let rb = event.next()?.ok_or(EventError::MissingRecord)?;
        let stream_name = &self.stream_name;

        STREAM_WRITERS::append_to_local(stream_name, &rb)?;

        Ok(0)
    }

    // inferSchema is a constructor to Schema
    // returns raw arrow schema type and arrow schema to string type.
    fn infer_schema(&self) -> Result<Schema, ArrowError> {
        let reader = self.body.as_bytes();
        let mut buf_reader = BufReader::new(reader);
        infer_json_schema(&mut buf_reader, None)
    }

    fn get_reader(&self, arrow_schema: arrow::datatypes::Schema) -> json::Reader<&[u8]> {
        json::Reader::new(
            self.body.as_bytes(),
            Arc::new(arrow_schema),
            json::reader::DecoderOptions::new().with_batch_size(1024),
        )
    }
}

pub mod error {
    use crate::metadata::error::stream_info::MetadataError;
    use crate::storage::ObjectStorageError;
    use datafusion::arrow::error::ArrowError;

    use super::StreamWriterError;

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
}
