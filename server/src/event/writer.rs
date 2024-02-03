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

use crate::{
    storage::{
        staging::{get_staged_records, ARROW_FILE_EXTENSION},
        StorageDir,
    },
    utils::arrow::{adapt_batch, merged_reader::MergedReverseRecordReader, replace_columns},
};

use self::{errors::StreamWriterError, file_writer::FileWriter, mem_writer::MemWriter};
use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_schema::{ArrowError, Schema};
use chrono::Utc;
use derive_more::{Deref, DerefMut};
use itertools::Itertools;
use once_cell::sync::Lazy;

pub static STREAM_WRITERS: Lazy<WriterTable> = Lazy::new(WriterTable::default);

#[derive(Default)]
pub struct Writer {
    pub disk: FileWriter,
    pub mem: MemWriter,
}

impl Writer {
    fn push(
        &mut self,
        stream_name: &str,
        schema_key: &str,
        rb: RecordBatch,
    ) -> Result<(), StreamWriterError> {
        let rb = replace_columns(
            rb.schema(),
            &rb,
            &[0],
            &[Arc::new(get_timestamp_array(rb.num_rows()))],
        );

        self.disk.push(stream_name, schema_key, &rb)?;
        self.mem.push(schema_key, rb);
        Ok(())
    }

    fn get_staged_records(&self, stream_name: &str, schema: &Arc<Schema>) -> Result<Vec<RecordBatch>, &'static str> {
        // Get the files data
        let staging_dir = &StorageDir::new(stream_name);
        let hot_filename =
            StorageDir::file_time_suffix(&chrono::Utc::now().naive_utc(), ARROW_FILE_EXTENSION);

            // get all the paths for arrow files
        let mut arrow_files = staging_dir.arrow_files();

        // filter the files that are hot
        arrow_files.retain(|path| {
            !path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(&hot_filename)
        });

        let record_reader = match MergedReverseRecordReader::try_new(&arrow_files) {
            Ok(reader) => reader,
            Err(_) => return Err("Failed to create Merged Record Reader"),
        };

        let merged_schema = Arc::new(record_reader.merged_schema());
        let mut records = record_reader.merged_iter(merged_schema.clone()).collect_vec();

        let mut mem_records = self.mem.recordbatches_cloned(&merged_schema);

        records.append(&mut mem_records);

        Ok(records)
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
                    .unwrap()
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
            let mut writer = writer.into_inner().unwrap();
            writer.disk.close_all();
            writer.mem.clear();
        }
    }

    pub fn recordbatches_cloned(
        &self,
        stream_name: &str,
        schema: &Arc<Schema>,
    ) -> Option<Vec<RecordBatch>> {
        // let records = get_staged_records(&StorageDir::new(stream_name))
        //     .ok()?
        //     .into_iter()
        //     .map(|rb| adapt_batch(schema, &rb))
        //     .collect();
        let dir = StorageDir::new(stream_name);
        let exclude = chrono::Utc::now().naive_utc();
        let mut records = get_staged_records(&dir, &exclude).unwrap();
        let in_mem_records = self.get_in_memory_records(stream_name, schema);
        let mut in_mem_records = in_mem_records
            .iter()
            .map(|rb| adapt_batch(schema, &rb))
            .collect_vec();
        records.append(&mut in_mem_records);

        Some(records)
    }

    fn get_in_memory_records(&self, stream_name: &str, schema: &Arc<Schema>) -> Vec<RecordBatch> {
        self.read()
            .unwrap()
            .get(stream_name)
            .unwrap()
            .lock()
            .unwrap()
            .mem
            .recordbatches_cloned(schema)
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
