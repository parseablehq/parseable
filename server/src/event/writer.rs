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

use datafusion::arrow::{ipc::writer::StreamWriter, record_batch::RecordBatch};
use lazy_static::lazy_static;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Mutex, RwLock};

use crate::storage::StorageDir;

use self::errors::StreamWriterError;

type ArrowWriter<T> = StreamWriter<T>;
type LocalWriter<T, S> = Mutex<Option<(usize, Vec<S>, ArrowWriter<T>)>>;

lazy_static! {
    #[derive(Default)]
    pub static ref STREAM_WRITERS: RwLock<WriterTable<String, String, File>> = RwLock::new(WriterTable::new());
}

impl STREAM_WRITERS {
    // append to a existing stream
    pub fn append_to_local(
        stream: &str,
        schema_key: &String,
        record: &RecordBatch,
    ) -> Result<(), StreamWriterError> {
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
                if let Some((ref mut order, ref mut hashes, ref mut writer)) = *writer_guard {
                    if hashes.contains(schema_key) {
                        writer.write(record).map_err(StreamWriterError::Writer)?;
                    } else {
                        *order += 1;
                        hashes.push(schema_key.to_owned());
                        *writer = init_new_stream_writer_file(stream, *order, record)?;
                    }
                } else {
                    // pass on this mutex to set entry so that it can be reused
                    // we have a guard for underlying entry thus
                    // hashmap must not be availible as mutable to any other thread
                    let order = 0;
                    let writer = init_new_stream_writer_file(stream, order, record)?;
                    writer_guard.replace((order, vec![schema_key.to_owned()], writer));
                    // replace the stream writer behind this mutex
                }
            }
            // entry is not present thus we create it
            None => {
                // this requires mutable borrow of the map so we drop this read lock and wait for write lock
                drop(hashmap_guard);
                STREAM_WRITERS::create_entry(stream.to_owned(), schema_key.to_owned(), record)?;
            }
        };
        Ok(())
    }

    // create a new entry with new stream_writer
    // Only create entry for valid streams
    fn create_entry(
        stream: String,
        schema_key: String,
        record: &RecordBatch,
    ) -> Result<(), StreamWriterError> {
        let mut hashmap_guard = STREAM_WRITERS
            .write()
            .map_err(|_| StreamWriterError::RwPoisoned)?;

        let order = StorageDir::new(&stream)
            .last_order_by_current_time()
            .unwrap_or_default();

        let writer = init_new_stream_writer_file(&stream, order, record)?;

        hashmap_guard.insert(stream, Mutex::new(Some((order, vec![schema_key], writer))));

        Ok(())
    }

    pub fn delete_stream(stream: &str) {
        STREAM_WRITERS.write().unwrap().delete_stream(stream);
    }

    pub fn unset_all() -> Result<(), StreamWriterError> {
        let table = STREAM_WRITERS
            .read()
            .map_err(|_| StreamWriterError::RwPoisoned)?;

        for writer in table.iter() {
            if let Some((_, _, mut streamwriter)) = writer
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

pub struct WriterTable<A, B, T>
where
    A: Eq + std::hash::Hash,
    T: Write,
{
    table: HashMap<A, LocalWriter<T, B>>,
}

impl<A, B, T> WriterTable<A, B, T>
where
    A: Eq + std::hash::Hash,
    T: Write,
{
    pub fn new() -> Self {
        let table = HashMap::new();
        Self { table }
    }

    fn get<X>(&self, a: &X) -> Option<&LocalWriter<T, B>>
    where
        A: Borrow<X>,
        X: Eq + std::hash::Hash + ?Sized,
    {
        self.table.get(a)
    }

    fn insert(&mut self, a: A, v: LocalWriter<T, B>) {
        self.table.insert(a, v);
    }

    pub fn delete_stream<X>(&mut self, stream: &X)
    where
        A: Borrow<X>,
        X: Eq + std::hash::Hash + ?Sized,
    {
        self.table.remove(stream);
    }

    fn iter(&self) -> impl Iterator<Item = &LocalWriter<T, B>> {
        self.table.values()
    }
}

fn init_new_stream_writer_file(
    stream_name: &str,
    order: usize,
    record: &RecordBatch,
) -> Result<ArrowWriter<std::fs::File>, StreamWriterError> {
    let dir = StorageDir::new(stream_name);
    let path = dir.path_by_current_time(order);

    std::fs::create_dir_all(dir.data_path)?;

    let file = OpenOptions::new().create(true).append(true).open(path)?;

    let mut stream_writer = StreamWriter::try_new(file, &record.schema())
        .expect("File and RecordBatch both are checked");

    stream_writer
        .write(record)
        .map_err(StreamWriterError::Writer)?;

    Ok(stream_writer)
}

pub mod errors {
    use datafusion::arrow::error::ArrowError;

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
