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

mod file_writer;
mod mem_writer;
mod mutable;

use std::{
    collections::HashMap,
    sync::{Mutex, RwLock},
};

use crate::{
    option::CONFIG,
    storage::staging::{self, ReadBuf},
};

use self::{errors::StreamWriterError, file_writer::FileWriter};
use arrow_array::RecordBatch;
use chrono::{NaiveDateTime, Utc};
use derive_more::{Deref, DerefMut};
use mem_writer::MemWriter;
use once_cell::sync::Lazy;

type InMemWriter = MemWriter<8192>;

pub static STREAM_WRITERS: Lazy<WriterTable> = Lazy::new(WriterTable::default);

pub enum StreamWriter {
    Mem(InMemWriter),
    Disk(FileWriter, InMemWriter),
}

impl StreamWriter {
    pub fn push(
        &mut self,
        stream_name: &str,
        schema_key: &str,
        rb: RecordBatch,
    ) -> Result<(), StreamWriterError> {
        match self {
            StreamWriter::Mem(mem) => {
                mem.push(rb);
            }
            StreamWriter::Disk(disk, mem) => {
                disk.push(stream_name, schema_key, &rb)?;
                mem.push(rb);
            }
        }
        Ok(())
    }
}

// Each entry in writer table is initialized with some context
// This is helpful for generating prefix when writer is finalized
pub struct WriterContext {
    stream_name: String,
    time: NaiveDateTime,
}

#[derive(Deref, DerefMut, Default)]
pub struct WriterTable(RwLock<HashMap<String, (Mutex<StreamWriter>, WriterContext)>>);

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
            Some((stream_writer, _)) => {
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
                if let Some((writer, _)) = map.get(stream_name) {
                    writer
                        .lock()
                        .unwrap()
                        .push(stream_name, schema_key, record)?;
                } else {
                    // there is no entry so this can be inserted safely
                    let context = WriterContext {
                        stream_name: stream_name.to_owned(),
                        time: Utc::now().naive_utc(),
                    };
                    let mut writer = if CONFIG.parseable.in_mem_ingestion {
                        StreamWriter::Mem(InMemWriter::default())
                    } else {
                        StreamWriter::Disk(FileWriter::default(), InMemWriter::default())
                    };

                    writer.push(stream_name, schema_key, record)?;
                    map.insert(stream_name.to_owned(), (Mutex::new(writer), context));
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
        for (writer, context) in map.into_values() {
            let writer = writer.into_inner().unwrap();
            match writer {
                StreamWriter::Mem(mem) => {
                    let rb = mem.finalize();
                    let mut read_bufs = staging::MEMORY_READ_BUFFERS.write().unwrap();

                    read_bufs
                        .entry(context.stream_name)
                        .or_insert(Vec::default())
                        .push(ReadBuf {
                            time: context.time,
                            buf: rb,
                        });
                }
                StreamWriter::Disk(disk, _) => disk.close_all(),
            }
        }
    }

    pub fn clone_read_buf(&self, stream_name: &str) -> Option<ReadBuf> {
        let hashmap_guard = self.read().unwrap();
        let (writer, context) = hashmap_guard.get(stream_name)?;
        let writer = writer.lock().unwrap();
        let mem = match &*writer {
            StreamWriter::Mem(mem) => mem,
            StreamWriter::Disk(_, mem) => mem,
        };

        Some(ReadBuf {
            time: context.time,
            buf: mem.recordbatch_cloned(),
        })
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
