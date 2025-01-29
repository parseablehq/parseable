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
};

use crate::{option::Mode, parseable::PARSEABLE, storage::StreamType};

use self::{errors::StreamWriterError, file_writer::FileWriter, mem_writer::MemWriter};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use chrono::NaiveDateTime;
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
        rb: &RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
    ) -> Result<(), StreamWriterError> {
        self.disk.push(
            stream_name,
            schema_key,
            rb,
            parsed_timestamp,
            custom_partition_values,
        )?;
        self.mem.push(schema_key, rb);
        Ok(())
    }

    fn push_mem(&mut self, schema_key: &str, rb: &RecordBatch) -> Result<(), StreamWriterError> {
        self.mem.push(schema_key, rb);
        Ok(())
    }
}

#[derive(Deref, DerefMut, Default)]
pub struct WriterTable(RwLock<HashMap<String, Mutex<Writer>>>);

impl WriterTable {
    // Concatenates record batches and puts them in memory store for each event.
    pub fn append_to_local(
        &self,
        stream_name: &str,
        schema_key: &str,
        record: &RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: &StreamType,
    ) -> Result<(), StreamWriterError> {
        if !self.read().unwrap().contains_key(stream_name) {
            // Gets write privileges only for inserting a writer
            self.write()
                .unwrap()
                .insert(stream_name.to_owned(), Mutex::new(Writer::default()));
        }

        // Updates the writer with only read privileges
        self.handle_existing_writer(
            stream_name,
            schema_key,
            record,
            parsed_timestamp,
            custom_partition_values,
            stream_type,
        )?;

        Ok(())
    }

    /// Update writer for stream when it already exists
    fn handle_existing_writer(
        &self,
        stream_name: &str,
        schema_key: &str,
        record: &RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: &StreamType,
    ) -> Result<(), StreamWriterError> {
        let hashmap_guard = self.read().unwrap();
        let mut writer = hashmap_guard
            .get(stream_name)
            .expect("Stream exists")
            .lock()
            .unwrap();
        if PARSEABLE.options.mode != Mode::Query || *stream_type == StreamType::Internal {
            writer.push(
                stream_name,
                schema_key,
                record,
                parsed_timestamp,
                custom_partition_values,
            )?;
        } else {
            writer.push_mem(stream_name, record)?;
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

pub mod errors {

    #[derive(Debug, thiserror::Error)]
    pub enum StreamWriterError {
        #[error("Arrow writer failed: {0}")]
        Writer(#[from] arrow_schema::ArrowError),
        #[error("Io Error when creating new file: {0}")]
        Io(#[from] std::io::Error),
    }
}
