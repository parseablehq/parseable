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
use derive_more::{Deref, DerefMut};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use crate::storage::staging::StorageDir;

use super::errors::StreamWriterError;

pub struct ArrowWriter {
    pub file_path: PathBuf,
    pub writer: StreamWriter<File>,
}

#[derive(Deref, DerefMut, Default)]
pub struct FileWriter(HashMap<String, ArrowWriter>);

impl FileWriter {
    // append to a existing stream
    pub fn push(
        &mut self,
        stream_name: &str,
        schema_key: &str,
        record: &RecordBatch,
    ) -> Result<(), StreamWriterError> {
        match self.get_mut(schema_key) {
            Some(writer) => {
                writer
                    .writer
                    .write(record)
                    .map_err(StreamWriterError::Writer)?;
            }
            // entry is not present thus we create it
            None => {
                // this requires mutable borrow of the map so we drop this read lock and wait for write lock
                let (path, writer) = init_new_stream_writer_file(stream_name, schema_key, record)?;
                self.insert(
                    schema_key.to_owned(),
                    ArrowWriter {
                        file_path: path,
                        writer,
                    },
                );
            }
        };

        Ok(())
    }

    pub fn close_all(self) {
        for mut writer in self.0.into_values() {
            _ = writer.writer.finish();
        }
    }
}

fn init_new_stream_writer_file(
    stream_name: &str,
    schema_key: &str,
    record: &RecordBatch,
) -> Result<(PathBuf, StreamWriter<std::fs::File>), StreamWriterError> {
    let dir = StorageDir::new(stream_name);
    let path = dir.path_by_current_time(schema_key);

    std::fs::create_dir_all(dir.data_path)?;

    let file = OpenOptions::new().create(true).append(true).open(&path)?;

    let mut stream_writer = StreamWriter::try_new(file, &record.schema())
        .expect("File and RecordBatch both are checked");

    stream_writer
        .write(record)
        .map_err(StreamWriterError::Writer)?;

    Ok((path, stream_writer))
}
