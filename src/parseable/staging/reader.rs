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

use std::{
    fs::{File, remove_file},
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    vec::IntoIter,
};

use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_ipc::{MessageHeader, reader::StreamReader, root_as_message_unchecked};
use arrow_schema::Schema;
use byteorder::{LittleEndian, ReadBytesExt};
use itertools::kmerge_by;
use tracing::error;

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    utils::arrow::{adapt_batch, reverse},
};

#[derive(Debug)]
pub struct MergedRecordReader {
    pub readers: Vec<StreamReader<BufReader<File>>>,
}

impl MergedRecordReader {
    pub fn try_new(files: &[PathBuf]) -> Result<Self, ()> {
        let mut readers = Vec::with_capacity(files.len());

        for file in files {
            //remove empty files before reading
            match file.metadata() {
                Err(err) => {
                    error!("Error when trying to read file: {file:?}; error = {err}");
                    continue;
                }
                Ok(metadata) if metadata.len() == 0 => {
                    error!("Empty file detected, removing it: {:?}", file);
                    remove_file(file).unwrap();
                    continue;
                }
                Ok(_) => {
                    let Ok(reader) =
                        StreamReader::try_new(BufReader::new(File::open(file).unwrap()), None)
                    else {
                        error!("Invalid file detected, ignoring it: {:?}", file);
                        continue;
                    };

                    readers.push(reader);
                }
            }
        }

        Ok(Self { readers })
    }

    pub fn merged_schema(&self) -> Schema {
        Schema::try_merge(
            self.readers
                .iter()
                .map(|reader| reader.schema().as_ref().clone()),
        )
        .unwrap()
    }
}

#[derive(Debug)]
pub struct MergedReverseRecordReader {
    pub readers: Vec<StreamReader<BufReader<OffsetReader<File>>>>,
}

impl MergedReverseRecordReader {
    pub fn try_new(file_paths: &[PathBuf]) -> Self {
        let mut readers = Vec::with_capacity(file_paths.len());
        for path in file_paths {
            match File::open(path) {
                Err(err) => {
                    error!("Error when trying to read file: {path:?}; error = {err}");
                    continue;
                }
                Ok(file) => {
                    let reader = match get_reverse_reader(file) {
                        Ok(r) => r,
                        Err(err) => {
                            error!("Invalid file detected, ignoring it: {path:?}; error = {err}");
                            continue;
                        }
                    };
                    readers.push(reader);
                }
            }
        }

        Self { readers }
    }

    pub fn merged_iter(
        self,
        schema: Arc<Schema>,
        time_partition: Option<String>,
    ) -> impl Iterator<Item = RecordBatch> {
        let adapted_readers = self.readers.into_iter().map(|reader| reader.flatten());
        kmerge_by(adapted_readers, move |a: &RecordBatch, b: &RecordBatch| {
            // Capture time_partition by value
            let a_time = get_timestamp_millis(a, time_partition.clone());
            let b_time = get_timestamp_millis(b, time_partition.clone());
            a_time > b_time
        })
        .map(|batch| reverse(&batch))
        .map(move |batch| adapt_batch(&schema, &batch))
    }

    pub fn merged_schema(&self) -> Schema {
        Schema::try_merge(
            self.readers
                .iter()
                .map(|reader| reader.schema().as_ref().clone()),
        )
        .unwrap()
    }
}

fn get_timestamp_millis(batch: &RecordBatch, time_partition: Option<String>) -> i64 {
    match time_partition {
        Some(time_partition) => {
            let time_partition = time_partition.as_str();
            match batch.column_by_name(time_partition) {
                Some(column) => column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(0),
                None => get_default_timestamp_millis(batch),
            }
        }
        None => get_default_timestamp_millis(batch),
    }
}
fn get_default_timestamp_millis(batch: &RecordBatch) -> i64 {
    match batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
    {
        // Ideally we expect the first column to be a timestamp (because we add the timestamp column first in the writer)
        Some(array) => array.value(0),
        // In case the first column is not a timestamp, we fallback to look for default timestamp column across all columns
        None => batch
            .column_by_name(DEFAULT_TIMESTAMP_KEY)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
    }
}

/// OffsetReader takes in a reader and list of offset and sizes and
/// provides a reader over the file by reading only the offsets
/// from start of the list to end.
///
/// Safety Invariant: Reader is already validated and all offset and limit are valid to read.
///
/// On empty list the reader returns no bytes read.
pub struct OffsetReader<R: Read + Seek> {
    reader: R,
    offset_list: IntoIter<(u64, usize)>,
    current_offset: u64,
    current_size: usize,
    buffer: Vec<u8>,
    buffer_position: usize,
    finished: bool,
}

impl<R: Read + Seek> OffsetReader<R> {
    fn new(reader: R, offset_list: Vec<(u64, usize)>) -> Self {
        let mut offset_list = offset_list.into_iter();
        let mut finished = false;

        let (current_offset, current_size) = offset_list.next().unwrap_or_default();
        if current_offset == 0 && current_size == 0 {
            finished = true
        }

        OffsetReader {
            reader,
            offset_list,
            current_offset,
            current_size,
            buffer: vec![0; 4096],
            buffer_position: 0,
            finished,
        }
    }
}

impl<R: Read + Seek> Read for OffsetReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let offset = self.current_offset;
        let size = self.current_size;

        if self.finished {
            return Ok(0);
        }
        // on empty buffer load current data represented by
        // current_offset and current_size into self buffer
        if self.buffer_position == 0 {
            self.reader.seek(SeekFrom::Start(offset))?;
            // resize for current message
            if self.buffer.len() < size {
                self.buffer.resize(size, 0)
            }
            self.reader.read_exact(&mut self.buffer[0..size])?;
        }

        let remaining_bytes = size - self.buffer_position;
        let max_read = usize::min(remaining_bytes, buf.len());

        // Copy data from the buffer to the provided buffer
        let read_data = &self.buffer[self.buffer_position..self.buffer_position + max_read];
        buf[..max_read].copy_from_slice(read_data);

        self.buffer_position += max_read;

        if self.buffer_position >= size {
            // If we've read the entire section, move to the next offset
            match self.offset_list.next() {
                Some((offset, size)) => {
                    self.current_offset = offset;
                    self.current_size = size;
                    self.buffer_position = 0;
                }
                None => {
                    // iter is exhausted, no more read can be done
                    self.finished = true
                }
            }
        }

        Ok(max_read)
    }
}

pub fn get_reverse_reader<T: Read + Seek>(
    mut reader: T,
) -> Result<StreamReader<BufReader<OffsetReader<T>>>, io::Error> {
    let mut offset = 0;
    let mut messages = Vec::new();

    while let Some(res) = find_limit_and_type(&mut reader).transpose() {
        match res {
            Ok((header, size)) => {
                messages.push((header, offset, size));
                offset += size;
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof && !messages.is_empty() => break,
            Err(err) => return Err(err),
        }
    }

    // reverse everything leaving the first because it has schema message.
    messages[1..].reverse();
    let messages = messages
        .into_iter()
        .map(|(_, offset, size)| (offset as u64, size))
        .collect();

    // reset reader
    reader.rewind()?;

    Ok(StreamReader::try_new(BufReader::new(OffsetReader::new(reader, messages)), None).unwrap())
}

// return limit for
fn find_limit_and_type(
    reader: &mut (impl Read + Seek),
) -> Result<Option<(MessageHeader, usize)>, io::Error> {
    let mut size = 0;
    let marker = reader.read_u32::<LittleEndian>()?;
    size += 4;

    if marker != 0xFFFFFFFF {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid Continuation Marker",
        ));
    }

    let metadata_size = reader.read_u32::<LittleEndian>()? as usize;
    size += 4;

    if metadata_size == 0x00000000 {
        return Ok(None);
    }

    let mut message = vec![0u8; metadata_size];
    reader.read_exact(&mut message)?;
    size += metadata_size;

    let message = unsafe { root_as_message_unchecked(&message) };
    let header = message.header_type();
    let message_size = message.bodyLength();
    size += message_size as usize;

    let padding = (8 - (size % 8)) % 8;
    reader.seek(SeekFrom::Current(padding as i64 + message_size))?;
    size += padding;

    Ok(Some((header, size)))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Cursor, Read},
        path::Path,
        sync::Arc,
    };

    use arrow_array::{
        Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, cast::AsArray,
        types::Int64Type,
    };
    use arrow_ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter, write_message,
    };
    use arrow_schema::{DataType, Field, Schema};
    use chrono::Utc;
    use temp_dir::TempDir;

    use crate::{
        OBJECT_STORE_DATA_GRANULARITY,
        parseable::staging::{
            reader::{MergedReverseRecordReader, OffsetReader},
            writer::DiskWriter,
        },
        utils::time::TimeRange,
    };

    use super::get_reverse_reader;

    fn rb(rows: usize) -> RecordBatch {
        let array1: Arc<dyn Array> = Arc::new(Int64Array::from_iter(0..(rows as i64)));
        let array2: Arc<dyn Array> = Arc::new(Float64Array::from_iter((0..rows).map(|x| x as f64)));
        let array3: Arc<dyn Array> = Arc::new(StringArray::from_iter(
            (0..rows).map(|x| Some(format!("str {}", x))),
        ));

        RecordBatch::try_from_iter_with_nullable([
            ("a", array1, true),
            ("b", array2, true),
            ("c", array3, true),
        ])
        .unwrap()
    }

    fn write_mem(rbs: &[RecordBatch]) -> Vec<u8> {
        let buf = Vec::new();
        let mut writer = StreamWriter::try_new(buf, &rbs[0].schema()).unwrap();

        for rb in rbs {
            writer.write(rb).unwrap()
        }

        writer.into_inner().unwrap()
    }

    #[test]
    fn test_empty_row() {
        let rb = rb(0);
        let buf = write_mem(&[rb]);
        let reader = Cursor::new(buf);
        let mut reader = get_reverse_reader(reader).unwrap();
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 0);
    }

    #[test]
    fn test_one_row() {
        let rb = rb(1);
        let buf = write_mem(&[rb]);
        let reader = Cursor::new(buf);
        let mut reader = get_reverse_reader(reader).unwrap();
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 1);
    }

    #[test]
    fn test_multiple_row_multiple_rbs() {
        let buf = write_mem(&[rb(1), rb(2), rb(3)]);
        let reader = Cursor::new(buf);
        let mut reader = get_reverse_reader(reader).unwrap();
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 3);
        let col1_val: Vec<i64> = rb
            .column(0)
            .as_primitive::<Int64Type>()
            .iter()
            .flatten()
            .collect();
        assert_eq!(col1_val, vec![0, 1, 2]);

        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 2);

        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 1);
    }

    #[test]
    fn manual_write() {
        let error_on_replacement = true;
        let options = IpcWriteOptions::default();
        let mut dictionary_tracker = DictionaryTracker::new(error_on_replacement);
        let data_gen = IpcDataGenerator {};

        let mut buf = Vec::new();
        let rb1 = rb(1);

        let schema = data_gen.schema_to_bytes_with_dictionary_tracker(
            &rb1.schema(),
            &mut dictionary_tracker,
            &options,
        );
        write_message(&mut buf, schema, &options).unwrap();

        for i in (1..=3).cycle().skip(1).take(10000) {
            let (_, encoded_message) = data_gen
                .encoded_batch(&rb(i), &mut dictionary_tracker, &options)
                .unwrap();
            write_message(&mut buf, encoded_message, &options).unwrap();
        }

        let schema = data_gen.schema_to_bytes_with_dictionary_tracker(
            &rb1.schema(),
            &mut dictionary_tracker,
            &options,
        );
        write_message(&mut buf, schema, &options).unwrap();

        let buf = Cursor::new(buf);
        let reader = get_reverse_reader(buf).unwrap().flatten();

        let mut sum = 0;
        for rb in reader {
            sum += 1;
            assert!(rb.num_rows() > 0);
        }

        assert_eq!(sum, 10000);
    }

    // Helper function to create test record batches
    fn create_test_batches(schema: &Arc<Schema>, count: usize) -> Vec<RecordBatch> {
        let mut batches = Vec::with_capacity(count);

        for batch_num in 1..=count as i32 {
            let id_array = Int32Array::from_iter(batch_num * 10..=batch_num * 10 + 1);
            let name_array = StringArray::from(vec![
                format!("Name {batch_num}-1"),
                format!("Name {batch_num}-2"),
            ]);

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(id_array), Arc::new(name_array)],
            )
            .expect("Failed to create test batch");

            batches.push(batch);
        }

        batches
    }

    // Helper function to write batches to a file
    fn write_test_batches(
        path: &Path,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> io::Result<()> {
        let range = TimeRange::granularity_range(Utc::now(), OBJECT_STORE_DATA_GRANULARITY);
        let mut writer =
            DiskWriter::try_new(path, schema, range).expect("Failed to create StreamWriter");

        for batch in batches {
            writer.write(batch).expect("Failed to write batch");
        }

        Ok(())
    }

    #[test]
    fn test_offset_reader() {
        // Create a simple binary file in memory
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let cursor = Cursor::new(data);

        // Define offset list: (offset, size)
        let offsets = vec![(2, 3), (7, 2)]; // Read bytes 2-4 (3, 4, 5) and then 7-8 (8, 9)

        let mut reader = OffsetReader::new(cursor, offsets);
        let mut buffer = [0u8; 10];

        // First read should get bytes 3, 4, 5
        let read_bytes = reader.read(&mut buffer).unwrap();
        assert_eq!(read_bytes, 3);
        assert_eq!(&buffer[..read_bytes], &[3, 4, 5]);

        // Second read should get bytes 8, 9
        let read_bytes = reader.read(&mut buffer).unwrap();
        assert_eq!(read_bytes, 2);
        assert_eq!(&buffer[..read_bytes], &[8, 9]);

        // No more data
        let read_bytes = reader.read(&mut buffer).unwrap();
        assert_eq!(read_bytes, 0);
    }

    #[test]
    fn test_merged_reverse_record_reader() -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.data.arrows");

        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create test batches (3 batches)
        let batches = create_test_batches(&schema, 3);

        // Write batches to file
        write_test_batches(&file_path, &schema, &batches)?;

        // Now read them back in reverse order
        let mut reader = MergedReverseRecordReader::try_new(&[file_path]).merged_iter(schema, None);

        // We should get batches in reverse order: 3, 2, 1
        // But first message should be schema, so we'll still read them in order

        // Read batch 3
        let batch = reader.next().expect("Failed to read batch");
        assert_eq!(batch.num_rows(), 2);
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 31); // affect of reverse on each recordbatch
        assert_eq!(id_array.value(1), 30);

        // Read batch 2
        let batch = reader.next().expect("Failed to read batch");
        assert_eq!(batch.num_rows(), 2);
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 21);
        assert_eq!(id_array.value(1), 20);

        // Read batch 1
        let batch = reader.next().expect("Failed to read batch");
        assert_eq!(batch.num_rows(), 2);
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 11);
        assert_eq!(id_array.value(1), 10);

        // No more batches
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn test_empty_offset_list() {
        // Test with empty offset list
        let data = vec![1, 2, 3, 4, 5];
        let cursor = Cursor::new(data);

        let mut reader = OffsetReader::new(cursor, vec![]);
        let mut buffer = [0u8; 10];

        // Should return 0 bytes read
        let read_bytes = reader.read(&mut buffer).unwrap();
        assert_eq!(read_bytes, 0);
    }

    #[test]
    fn test_partial_reads() {
        // Test reading with a buffer smaller than the section size
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let cursor = Cursor::new(data);

        // One offset of 5 bytes
        let offsets = vec![(2, 5)]; // Read bytes 2-6 (3, 4, 5, 6, 7)

        let mut reader = OffsetReader::new(cursor, offsets);
        let mut buffer = [0u8; 3]; // Buffer smaller than the 5 bytes we want to read

        // First read should get first 3 bytes: 3, 4, 5
        let read_bytes = reader.read(&mut buffer).unwrap();
        assert_eq!(read_bytes, 3);
        assert_eq!(&buffer[..read_bytes], &[3, 4, 5]);

        // Second read should get remaining 2 bytes: 6, 7
        let read_bytes = reader.read(&mut buffer).unwrap();
        assert_eq!(read_bytes, 2);
        assert_eq!(&buffer[..read_bytes], &[6, 7]);

        // No more data
        let read_bytes = reader.read(&mut buffer).unwrap();
        assert_eq!(read_bytes, 0);
    }

    #[test]
    fn test_get_reverse_reader_single_message() -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test_single.data.arrows");

        // Create a schema
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // Create a single batch
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![42]))])
                .expect("Failed to create batch");

        // Write batch to file
        write_test_batches(&file_path, &schema, &[batch])?;

        let mut reader = MergedReverseRecordReader::try_new(&[file_path]).merged_iter(schema, None);

        // Should get the batch
        let result_batch = reader.next().expect("Failed to read batch");
        let id_array = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 42);

        // No more batches
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn test_large_buffer_resizing() {
        // Test that buffer resizes correctly for large sections
        let data = vec![1; 10000]; // 10KB of data
        let cursor = Cursor::new(data);

        // One large offset (8KB)
        let offsets = vec![(1000, 8000)];

        let mut reader = OffsetReader::new(cursor, offsets);
        let mut buffer = [0u8; 10000];

        // Should read 8KB
        let read_bytes = reader.read(&mut buffer).unwrap();
        assert_eq!(read_bytes, 8000);

        // All bytes should be 1
        for i in 0..read_bytes {
            assert_eq!(buffer[i], 1);
        }
    }
}
