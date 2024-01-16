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
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use crate::utils;

use self::{errors::StreamWriterError, file_writer::FileWriter, mem_writer::MemWriter};
use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_schema::Schema;
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
    ) -> Result<(), StreamWriterError> {
        let rb = utils::arrow::replace_columns(
            rb.schema(),
            &rb,
            &[0],
            &[Arc::new(get_timestamp_array(rb.num_rows()))],
        );

        self.disk.push(stream_name, schema_key, &rb)?;
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
    ) -> Result<(), StreamWriterError> {
        let hashmap_guard = self.read().unwrap();

        match hashmap_guard.get(stream_name) {
            Some(stream_writer) => {
                stream_writer
                    .lock()
                    .unwrap() // /yyyyyyyyyyy
                    .push(stream_name, schema_key, record)?;
            }
            None => {
                drop(hashmap_guard);
                let mut map = self.write().unwrap();
                // check for race condition
                // if map contains entry then just
                if let Some(writer) = map.get(stream_name) {
                    writer
                        .lock()
                        .unwrap()
                        .push(stream_name, schema_key, record)?;
                } else {
                    let mut writer = Writer::default();
                    writer.push(stream_name, schema_key, record)?;
                    map.insert(stream_name.to_owned(), Mutex::new(writer));
                }
            }
        };
        Ok(())
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
        let read_guard = self.0.read().unwrap();
        let stream_guard = read_guard.get(stream_name)?;

        loop {
            match stream_guard.lock() {
                Ok(guard) => {
                    let records = guard.mem.recordbatch_cloned(schema);
                    return Some(records);
                }
                Err(_ /*poisoned */) => {
                    std::thread::sleep(Duration::from_millis(1000 * 10));
                    continue;
                }
            }
        }
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
