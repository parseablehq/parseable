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

mod file_writer;
mod mem_writer;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, RwLockWriteGuard},
};

use crate::{
    option::{Mode, CONFIG},
    storage::StreamType,
    utils,
};

use self::{errors::StreamWriterError, file_writer::FileWriter, mem_writer::MemWriter};
use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_schema::Schema;
use chrono::NaiveDateTime;
use chrono::Utc;
use derive_more::{Deref, DerefMut};
use once_cell::sync::Lazy;

pub static STREAM_WRITERS: Lazy<WriterTable> = Lazy::new(WriterTable::default);

#[derive(Default)]
pub struct Writer {
    pub mem: MemWriter<16384>,
    pub disk: FileWriter,
}

impl Writer {
    fn push(
        &mut self,
        stream_name: &str,
        schema_key: &str,
        rb: RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
    ) -> Result<(), StreamWriterError> {
        let rb = utils::arrow::replace_columns(
            rb.schema(),
            &rb,
            &[0],
            &[Arc::new(get_timestamp_array(rb.num_rows()))],
        );

        self.disk.push(
            stream_name,
            schema_key,
            &rb,
            parsed_timestamp,
            custom_partition_values,
        )?;
        self.mem.push(schema_key, rb);
        Ok(())
    }

    fn push_mem(&mut self, schema_key: &str, rb: RecordBatch) -> Result<(), StreamWriterError> {
        self.mem.push(schema_key, rb);
        Ok(())
    }
}

#[derive(Deref, DerefMut, Default)]
pub struct WriterTable(RwLock<HashMap<String, Mutex<Writer>>>);

impl WriterTable {
    // append to a existing stream
    pub fn append_to_local(
        &self,
        stream_name: &str,
        schema_key: &str,
        record: RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: HashMap<String, String>,
        stream_type: &StreamType,
    ) -> Result<(), StreamWriterError> {
        let hashmap_guard = self.read().unwrap();

        match hashmap_guard.get(stream_name) {
            Some(stream_writer) => {
                self.handle_existing_writer(
                    stream_writer,
                    stream_name,
                    schema_key,
                    record,
                    parsed_timestamp,
                    &custom_partition_values,
                    stream_type,
                )?;
            }
            None => {
                drop(hashmap_guard);
                let map = self.write().unwrap();
                // check for race condition
                // if map contains entry then just
                self.handle_missing_writer(
                    map,
                    stream_name,
                    schema_key,
                    record,
                    parsed_timestamp,
                    &custom_partition_values,
                    stream_type,
                )?;
            }
        };
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_existing_writer(
        &self,
        stream_writer: &Mutex<Writer>,
        stream_name: &str,
        schema_key: &str,
        record: RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: &StreamType,
    ) -> Result<(), StreamWriterError> {
        if CONFIG.options.mode != Mode::Query || *stream_type == StreamType::Internal {
            stream_writer.lock().unwrap().push(
                stream_name,
                schema_key,
                record,
                parsed_timestamp,
                custom_partition_values,
            )?;
        } else {
            stream_writer
                .lock()
                .unwrap()
                .push_mem(stream_name, record)?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_missing_writer(
        &self,
        mut map: RwLockWriteGuard<HashMap<String, Mutex<Writer>>>,
        stream_name: &str,
        schema_key: &str,
        record: RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: &StreamType,
    ) -> Result<(), StreamWriterError> {
        match map.get(stream_name) {
            Some(writer) => {
                if CONFIG.options.mode != Mode::Query || *stream_type == StreamType::Internal {
                    writer.lock().unwrap().push(
                        stream_name,
                        schema_key,
                        record,
                        parsed_timestamp,
                        custom_partition_values,
                    )?;
                } else {
                    writer.lock().unwrap().push_mem(stream_name, record)?;
                }
            }
            None => {
                if CONFIG.options.mode != Mode::Query || *stream_type == StreamType::Internal {
                    let mut writer = Writer::default();
                    writer.push(
                        stream_name,
                        schema_key,
                        record,
                        parsed_timestamp,
                        custom_partition_values,
                    )?;
                    map.insert(stream_name.to_owned(), Mutex::new(writer));
                } else {
                    let mut writer = Writer::default();
                    writer.push_mem(schema_key, record)?;
                    map.insert(stream_name.to_owned(), Mutex::new(writer));
                }
            }
        }
        Ok(())
    }

    pub fn clear(&self, stream_name: &str) {
        let map = self.write().unwrap();
        if let Some(writer) = map.get(stream_name) {
            let w = &mut writer.lock().unwrap().mem;
            w.clear();
        }
    }

    pub fn delete_stream(&self, stream_name: &str) {
        self.write().unwrap().remove(stream_name);
    }

    pub fn unset_all(&self) {
        let mut table = self.write().unwrap();
        let map = std::mem::take(&mut *table);
        drop(table);
        for writer in map.into_values() {
            let writer = writer.into_inner().unwrap();
            writer.disk.close_all();
        }
    }

    pub fn recordbatches_cloned(
        &self,
        stream_name: &str,
        schema: &Arc<Schema>,
    ) -> Option<Vec<RecordBatch>> {
        let records = self
            .0
            .read()
            .unwrap()
            .get(stream_name)?
            .lock()
            .unwrap()
            .mem
            .recordbatch_cloned(schema);

        Some(records)
    }
}

fn get_timestamp_array(size: usize) -> TimestampMillisecondArray {
    TimestampMillisecondArray::from_value(Utc::now().timestamp_millis(), size)
}

pub mod errors {

    #[derive(Debug, thiserror::Error)]
    pub enum StreamWriterError {
        #[error("Arrow writer failed: {0}")]
        Writer(#[from] arrow_schema::ArrowError),
        #[error("Io Error when creating new file: {0}")]
        Io(#[from] std::io::Error),
    }
}
