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

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use once_cell::sync::Lazy;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, RwLock};

use crate::storage::StorageDir;

use self::errors::StreamWriterError;

type ArrowWriter<T> = StreamWriter<T>;
type LocalWriter<T> = Mutex<Option<ArrowWriter<T>>>;

pub static STREAM_WRITERS: Lazy<InnerStreamWriter> =
    Lazy::new(|| InnerStreamWriter(RwLock::new(WriterTable::new())));

/*
    A wrapper type for global struct to implement methods over
*/
pub struct InnerStreamWriter(RwLock<WriterTable<String, String, File>>);

impl Deref for InnerStreamWriter {
    type Target = RwLock<WriterTable<String, String, File>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for InnerStreamWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
/*
   Manually implmenting for the Type
   since it depends on the types which are missing it
*/
impl Debug for InnerStreamWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("InnerStreamWriter { __private_field: () }")
    }
}

impl InnerStreamWriter {
    // append to a existing stream
    pub fn append_to_local(
        &self,
        stream: &str,
        schema_key: &str,
        record: &RecordBatch,
    ) -> Result<(), StreamWriterError> {
        let hashmap_guard = self.read().map_err(|_| StreamWriterError::RwPoisoned)?;

        match hashmap_guard.get(stream, schema_key) {
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
                    let writer = init_new_stream_writer_file(stream, schema_key, record)?;
                    writer_guard.replace(writer); // replace the stream writer behind this mutex
                }
            }
            // entry is not present thus we create it
            None => {
                // this requires mutable borrow of the map so we drop this read lock and wait for write lock
                drop(hashmap_guard);
                self.create_entry(stream.to_owned(), schema_key.to_owned(), record)?;
            }
        };
        Ok(())
    }

    // create a new entry with new stream_writer
    // Only create entry for valid streams
    fn create_entry(
        &self,
        stream: String,
        schema_key: String,
        record: &RecordBatch,
    ) -> Result<(), StreamWriterError> {
        let mut hashmap_guard = self.write().map_err(|_| StreamWriterError::RwPoisoned)?;

        let writer = init_new_stream_writer_file(&stream, &schema_key, record)?;

        hashmap_guard.insert(stream, schema_key, Mutex::new(Some(writer)));

        Ok(())
    }

    pub fn delete_stream(&self, stream: &str) {
        self.write().unwrap().delete_stream(stream);
    }

    pub fn unset_all(&self) -> Result<(), StreamWriterError> {
        let table = self.read().map_err(|_| StreamWriterError::RwPoisoned)?;

        for writer in table.iter() {
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

pub struct WriterTable<A, B, T>
where
    A: Eq + std::hash::Hash,
    B: Eq + std::hash::Hash,
    T: Write,
{
    table: HashMap<A, HashMap<B, LocalWriter<T>>>,
}

impl<A, B, T> WriterTable<A, B, T>
where
    A: Eq + std::hash::Hash,
    B: Eq + std::hash::Hash,
    T: Write,
{
    pub fn new() -> Self {
        let table = HashMap::new();
        Self { table }
    }

    fn get<X, Y>(&self, a: &X, b: &Y) -> Option<&LocalWriter<T>>
    where
        A: Borrow<X>,
        B: Borrow<Y>,
        X: Eq + std::hash::Hash + ?Sized,
        Y: Eq + std::hash::Hash + ?Sized,
    {
        self.table.get(a)?.get(b)
    }

    fn insert(&mut self, a: A, b: B, v: LocalWriter<T>) {
        let inner = self.table.entry(a).or_default();
        inner.insert(b, v);
    }

    pub fn delete_stream<X>(&mut self, stream: &X)
    where
        A: Borrow<X>,
        X: Eq + std::hash::Hash + ?Sized,
    {
        self.table.remove(stream);
    }

    fn iter(&self) -> impl Iterator<Item = &LocalWriter<T>> {
        self.table.values().flat_map(|inner| inner.values())
    }
}

fn init_new_stream_writer_file(
    stream_name: &str,
    schema_key: &str,
    record: &RecordBatch,
) -> Result<ArrowWriter<std::fs::File>, StreamWriterError> {
    let dir = StorageDir::new(stream_name);
    let path = dir.path_by_current_time(schema_key);

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
    use arrow_schema::ArrowError;

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
