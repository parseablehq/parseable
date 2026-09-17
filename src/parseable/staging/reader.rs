/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    vec::IntoIter,
};

#[cfg(test)]
use std::io::Take;

use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_ipc::{MessageHeader, reader::StreamReader, root_as_message};
use arrow_schema::{ArrowError, Schema};
use byteorder::{LittleEndian, ReadBytesExt};
use itertools::kmerge_by;
use tracing::{error, info_span};

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    utils::arrow::{adapt_batch, reverse},
};

#[derive(Debug)]
/// Merges reverse readers while retaining the source-file validation result.
pub struct MergedReverseRecordReader {
    pub readers: Vec<StreamReader<BufReader<OffsetReader<File>>>>,
    pub readable_files: Vec<PathBuf>,
    pub invalid_files: Vec<PathBuf>,
}

#[derive(Debug)]
/// Reads Arrow files in their original write order. Used by metric conversion,
/// which explicitly sorts every Parquet row group and does not need row reversal.
pub struct MergedForwardRecordReader {
    file_plans: Vec<ForwardFilePlan>,
    pub readable_files: Vec<PathBuf>,
    pub invalid_files: Vec<PathBuf>,
}

#[derive(Debug)]
pub(crate) struct ForwardFilePlan {
    path: PathBuf,
    schema: Arc<Schema>,
    messages: Vec<(MessageHeader, usize, usize)>,
    record_batches: usize,
}

pub(crate) type ForwardStreamReader = StreamReader<BufReader<MessageRangeReader<File>>>;

pub(crate) struct ForwardReaderLane {
    pub reader: ForwardStreamReader,
    pub record_batches: usize,
}

impl ForwardFilePlan {
    /// Opens independent virtual Arrow streams over striped record batches.
    /// Every stream begins with the original schema and receives every
    /// dictionary update, preserving Arrow IPC decoder state.
    pub(crate) fn reader_lane_count(&self, readers_per_file: usize) -> usize {
        readers_per_file.max(1).min(self.record_batches)
    }

    pub(crate) fn open_lanes(
        self,
        readers_per_file: usize,
    ) -> Result<Vec<ForwardReaderLane>, io::Error> {
        let lane_count = readers_per_file.max(1).min(self.record_batches);
        let (_, schema_offset, schema_size) = self.messages[0];
        let schema_range = (schema_offset as u64, schema_size);
        let mut lane_ranges = vec![vec![schema_range]; lane_count];
        let mut lane_record_batches = vec![0; lane_count];
        let mut record_batch_index = 0;

        for (header, offset, size) in self.messages.into_iter().skip(1) {
            let range = (offset as u64, size);
            match header {
                MessageHeader::DictionaryBatch => {
                    for ranges in &mut lane_ranges {
                        ranges.push(range);
                    }
                }
                MessageHeader::RecordBatch => {
                    let lane = record_batch_index % lane_count;
                    lane_ranges[lane].push(range);
                    lane_record_batches[lane] += 1;
                    record_batch_index += 1;
                }
                unsupported => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unsupported Arrow IPC message: {unsupported:?}"),
                    ));
                }
            }
        }

        lane_ranges
            .into_iter()
            .zip(lane_record_batches)
            .map(|(ranges, record_batches)| {
                let file = File::open(&self.path)?;
                let reader = StreamReader::try_new(
                    BufReader::new(MessageRangeReader::new(file, ranges)),
                    None,
                )
                .map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid arrow stream: {err}"),
                    )
                })?;
                Ok(ForwardReaderLane {
                    reader,
                    record_batches,
                })
            })
            .collect()
    }
}

impl MergedForwardRecordReader {
    /// Opens valid Arrow files and excludes any crash-truncated tail message.
    pub fn try_new(file_paths: &[PathBuf]) -> Self {
        let _span = info_span!("open_forward_arrow_files", file_count = file_paths.len()).entered();
        let mut file_plans = Vec::with_capacity(file_paths.len());
        let mut readable_files = Vec::with_capacity(file_paths.len());
        let mut invalid_files = Vec::new();
        for path in file_paths {
            match File::open(path) {
                Err(err) => {
                    error!("Error when trying to read file: {path:?}; error = {err}");
                    continue;
                }
                Ok(mut file) => {
                    let messages = match complete_messages(&mut file) {
                        Ok(messages) => messages,
                        Err(err) => {
                            error!("Invalid file detected, ignoring it: {path:?}; error = {err}");
                            invalid_files.push(path.clone());
                            continue;
                        }
                    };
                    let (_, schema_offset, schema_size) = messages[0];
                    let schema_reader = match StreamReader::try_new(
                        BufReader::new(MessageRangeReader::new(
                            file,
                            vec![(schema_offset as u64, schema_size)],
                        )),
                        None,
                    ) {
                        Ok(reader) => reader,
                        Err(err) => {
                            error!("Invalid file detected, ignoring it: {path:?}; error = {err}");
                            invalid_files.push(path.clone());
                            continue;
                        }
                    };
                    let record_batches = messages
                        .iter()
                        .filter(|(header, _, _)| *header == MessageHeader::RecordBatch)
                        .count();
                    file_plans.push(ForwardFilePlan {
                        path: path.clone(),
                        schema: schema_reader.schema(),
                        messages,
                        record_batches,
                    });
                    readable_files.push(path.clone());
                }
            }
        }

        Self {
            file_plans,
            readable_files,
            invalid_files,
        }
    }

    pub(crate) fn file_count(&self) -> usize {
        self.file_plans.len()
    }

    pub(crate) fn into_file_plans(self) -> IntoIter<ForwardFilePlan> {
        self.file_plans.into_iter()
    }

    /// Returns the union of schemas exposed by all readable Arrow streams.
    pub fn merged_schema(&self) -> Schema {
        Schema::try_merge(
            self.file_plans
                .iter()
                .map(|plan| plan.schema.as_ref().clone()),
        )
        .unwrap()
    }
}

impl MergedReverseRecordReader {
    /// Opens valid Arrow files and separates files that fail structural validation.
    pub fn try_new(file_paths: &[PathBuf]) -> Self {
        let _span = info_span!("open_arrow_files", file_count = file_paths.len()).entered();
        let mut readers = Vec::with_capacity(file_paths.len());
        let mut readable_files = Vec::with_capacity(file_paths.len());
        let mut invalid_files = Vec::new();
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
                            invalid_files.push(path.clone());
                            continue;
                        }
                    };
                    readers.push(reader);
                    readable_files.push(path.clone());
                }
            }
        }

        Self {
            readers,
            readable_files,
            invalid_files,
        }
    }

    /// Merges all readers in reverse timestamp order and adapts each batch to `schema`.
    pub fn merged_iter(
        self,
        schema: Arc<Schema>,
        time_partition: Option<String>,
    ) -> impl Iterator<Item = Result<RecordBatch, ArrowError>> {
        let adapted_readers = self.readers;
        kmerge_by(
            adapted_readers,
            move |a: &Result<RecordBatch, ArrowError>, b: &Result<RecordBatch, ArrowError>| {
                match (a, b) {
                    (Ok(a), Ok(b)) => {
                        let a_time = get_timestamp_millis(a, time_partition.as_deref());
                        let b_time = get_timestamp_millis(b, time_partition.as_deref());
                        a_time > b_time
                    }
                    // Surface decoding errors through the iterator instead of silently
                    // dropping them with Iterator::flatten.
                    (Err(_), _) => true,
                    (_, Err(_)) => false,
                }
            },
        )
        .map(|batch| batch.map(|batch| reverse(&batch)))
        .map(move |batch| batch.map(|batch| adapt_batch(schema.clone(), &batch)))
    }

    /// Returns the union of schemas exposed by all readable Arrow streams.
    pub fn merged_schema(&self) -> Schema {
        Schema::try_merge(
            self.readers
                .iter()
                .map(|reader| reader.schema().as_ref().clone()),
        )
        .unwrap()
    }
}

fn get_timestamp_millis(batch: &RecordBatch, time_partition: Option<&str>) -> i64 {
    match time_partition {
        Some(time_partition) => match batch.column_by_name(time_partition) {
            Some(column) => column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            None => get_default_timestamp_millis(batch),
        },
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

/// Presents disjoint IPC message ranges as one continuous Arrow stream.
/// Unlike `OffsetReader`, bytes are read directly into the caller's buffer;
/// an entire record-batch message is never copied into an intermediate buffer.
#[derive(Debug)]
pub(crate) struct MessageRangeReader<R: Read + Seek> {
    reader: R,
    ranges: IntoIter<(u64, usize)>,
    current_offset: u64,
    remaining: usize,
    positioned: bool,
    finished: bool,
}

impl<R: Read + Seek> MessageRangeReader<R> {
    fn new(reader: R, ranges: Vec<(u64, usize)>) -> Self {
        let mut ranges = ranges.into_iter();
        let first = ranges.next();
        let (current_offset, remaining) = first.unwrap_or_default();
        Self {
            reader,
            ranges,
            current_offset,
            remaining,
            positioned: false,
            finished: first.is_none(),
        }
    }

    fn advance(&mut self) {
        match self.ranges.next() {
            Some((offset, size)) => {
                self.current_offset = offset;
                self.remaining = size;
                self.positioned = false;
            }
            None => self.finished = true,
        }
    }
}

impl<R: Read + Seek> Read for MessageRangeReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.finished {
            return Ok(0);
        }

        while self.remaining == 0 {
            self.advance();
            if self.finished {
                return Ok(0);
            }
        }

        if !self.positioned {
            self.reader.seek(SeekFrom::Start(self.current_offset))?;
            self.positioned = true;
        }

        let max_read = self.remaining.min(buf.len());
        let read = self.reader.read(&mut buf[..max_read])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Arrow IPC message ended before its declared boundary",
            ));
        }

        self.current_offset += read as u64;
        self.remaining -= read;
        Ok(read)
    }
}

/// Finds all complete IPC messages, excluding a crash-truncated tail.
fn complete_messages<T: Read + Seek>(
    reader: &mut T,
) -> Result<Vec<(MessageHeader, usize, usize)>, io::Error> {
    let file_len = reader.seek(SeekFrom::End(0))?;
    reader.rewind()?;

    let mut offset: usize = 0;
    let mut messages = Vec::new();

    loop {
        match find_limit_and_type(reader) {
            Ok(Some((header, size))) => {
                let next_offset = offset.checked_add(size).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Arrow message size overflow")
                })?;

                // Seeking beyond EOF succeeds for regular files. Check the declared
                // message boundary explicitly so a crash-truncated record batch is
                // never handed to StreamReader as if it were complete.
                if next_offset as u64 > file_len {
                    break;
                }
                messages.push((header, offset, size));
                offset = next_offset;
            }
            Ok(None) => break,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof && !messages.is_empty() => {
                break;
            }
            Err(err) => return Err(err),
        }
    }

    if messages
        .first()
        .is_none_or(|(header, _, _)| *header != MessageHeader::Schema)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Arrow stream has no complete schema message",
        ));
    }

    if !messages
        .iter()
        .any(|(header, _, _)| *header == MessageHeader::RecordBatch)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Arrow stream has no complete record batch",
        ));
    }

    Ok(messages)
}

/// Builds a forward Arrow stream reader over only complete IPC messages.
#[cfg(test)]
pub fn get_forward_reader<T: Read + Seek>(
    mut reader: T,
) -> Result<StreamReader<BufReader<Take<T>>>, io::Error> {
    let messages = complete_messages(&mut reader)?;
    let complete_len = messages
        .last()
        .map(|(_, offset, size)| offset + size)
        .expect("complete_messages requires a schema and record batch");
    reader.rewind()?;

    StreamReader::try_new(BufReader::new(reader.take(complete_len as u64)), None).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid arrow stream: {e}"),
        )
    })
}

/// Builds a reverse Arrow stream reader from complete IPC messages.
pub fn get_reverse_reader<T: Read + Seek>(
    mut reader: T,
) -> Result<StreamReader<BufReader<OffsetReader<T>>>, io::Error> {
    let mut messages = complete_messages(&mut reader)?;

    // Reverse everything leaving the first because it has schema message.
    messages[1..].reverse();
    let messages = messages
        .into_iter()
        .map(|(_, offset, size)| (offset as u64, size))
        .collect();

    // reset reader
    reader.rewind()?;

    StreamReader::try_new(BufReader::new(OffsetReader::new(reader, messages)), None).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid arrow stream: {e}"),
        )
    })
}

/// Returns the IPC message type and its padded byte length.
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

    let message = root_as_message(&message).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid Arrow IPC message: {err}"),
        )
    })?;
    let header = message.header_type();
    let message_size = usize::try_from(message.bodyLength())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid Arrow IPC body length"))?;
    size = size
        .checked_add(message_size)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Arrow message size overflow"))?;

    let padding = (8 - (size % 8)) % 8;
    let seek_by = message_size
        .checked_add(padding)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Arrow message size overflow"))?;
    reader.seek(SeekFrom::Current(seek_by as i64))?;
    size = size
        .checked_add(padding)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Arrow message size overflow"))?;

    Ok(Some((header, size)))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        io::{self, Cursor, Read},
        path::Path,
        sync::Arc,
    };

    use arrow_array::{
        Array, DictionaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        cast::AsArray,
        types::{Int32Type, Int64Type},
    };
    use arrow_ipc::{
        MessageHeader,
        writer::{
            DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
            write_message,
        },
    };
    use arrow_schema::{DataType, Field, Schema};
    use chrono::Utc;
    use temp_dir::TempDir;

    use crate::{
        OBJECT_STORE_DATA_GRANULARITY,
        parseable::staging::{
            reader::{MergedForwardRecordReader, MergedReverseRecordReader, OffsetReader},
            writer::DiskWriter,
        },
        utils::time::TimeRange,
    };

    use super::{find_limit_and_type, get_forward_reader, get_reverse_reader};

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

    fn truncate_last_record_batch(bytes: &mut Vec<u8>) {
        let mut cursor = Cursor::new(bytes.as_slice());
        let mut offset = 0usize;
        let mut last_record_batch = None;
        while let Some((header, size)) = find_limit_and_type(&mut cursor).unwrap() {
            if header == MessageHeader::RecordBatch {
                last_record_batch = Some((offset, size));
            }
            offset += size;
        }

        let (offset, size) = last_record_batch.expect("record batch message");
        bytes.truncate(offset + size / 2);
    }

    #[test]
    fn reverse_reader_recovers_complete_batches_before_truncated_tail() {
        let mut bytes = write_mem(&[rb(2), rb(3)]);
        truncate_last_record_batch(&mut bytes);

        let reader = get_reverse_reader(Cursor::new(bytes)).unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn forward_reader_recovers_complete_batches_before_truncated_tail() {
        let mut bytes = write_mem(&[rb(2), rb(3)]);
        truncate_last_record_batch(&mut bytes);

        let reader = get_forward_reader(Cursor::new(bytes)).unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn forward_reader_preserves_batch_order() {
        let bytes = write_mem(&[rb(1), rb(2), rb(3)]);

        let reader = get_forward_reader(Cursor::new(bytes)).unwrap();
        let rows = reader
            .map(|batch| batch.unwrap().num_rows())
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![1, 2, 3]);
    }

    #[test]
    fn reverse_reader_rejects_stream_without_complete_batch() {
        let mut bytes = write_mem(&[rb(2)]);
        truncate_last_record_batch(&mut bytes);

        let err = get_reverse_reader(Cursor::new(bytes)).unwrap_err();
        assert!(err.to_string().contains("no complete record batch"));
    }

    #[test]
    fn reverse_reader_accepts_complete_batches_without_eos() {
        let mut bytes = write_mem(&[rb(2)]);
        bytes.truncate(bytes.len() - 8);

        let reader = get_reverse_reader(Cursor::new(bytes)).unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
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
        let mut compression_context = IpcWriteContext::default();

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
                .encode(
                    &rb(i),
                    &mut dictionary_tracker,
                    &options,
                    &mut compression_context,
                )
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
        let mut reader =
            MergedReverseRecordReader::try_new(&[file_path.into()]).merged_iter(schema, None);

        // We should get batches in reverse order: 3, 2, 1
        // But first message should be schema, so we'll still read them in order

        // Read batch 3
        let batch = reader
            .next()
            .expect("Failed to read batch")
            .expect("Invalid batch");
        assert_eq!(batch.num_rows(), 2);
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 31); // affect of reverse on each recordbatch
        assert_eq!(id_array.value(1), 30);

        // Read batch 2
        let batch = reader
            .next()
            .expect("Failed to read batch")
            .expect("Invalid batch");
        assert_eq!(batch.num_rows(), 2);
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 21);
        assert_eq!(id_array.value(1), 20);

        // Read batch 1
        let batch = reader
            .next()
            .expect("Failed to read batch")
            .expect("Invalid batch");
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
    fn forward_reader_lanes_preserve_original_batch_order() -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("striped.data.arrows");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        write_test_batches(&file_path, &schema, &create_test_batches(&schema, 10))?;

        let plan = MergedForwardRecordReader::try_new(&[file_path])
            .into_file_plans()
            .next()
            .unwrap();
        let lanes = plan.open_lanes(4)?;
        assert_eq!(
            lanes
                .iter()
                .map(|lane| lane.record_batches)
                .collect::<Vec<_>>(),
            vec![3, 3, 2, 2]
        );

        let mut batches_by_lane = lanes
            .into_iter()
            .map(|lane| lane.reader.collect::<Result<VecDeque<_>, _>>().unwrap())
            .collect::<Vec<_>>();
        let first_ids = (0..10)
            .map(|batch_index| {
                let batch = batches_by_lane[batch_index % 4].pop_front().unwrap();
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0)
            })
            .collect::<Vec<_>>();
        assert_eq!(first_ids, vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);

        Ok(())
    }

    #[test]
    fn forward_reader_lanes_receive_dictionary_messages() -> io::Result<()> {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("dictionary.data.arrows");
        let dictionary = DictionaryArray::<Int32Type>::from_iter(["alpha", "beta"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", dictionary.data_type().clone(), false),
        ]));
        let batches = (0..8)
            .map(|batch| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![batch * 2, batch * 2 + 1])),
                        Arc::new(DictionaryArray::<Int32Type>::from_iter(["alpha", "beta"])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        write_test_batches(&file_path, &schema, &batches)?;

        let plan = MergedForwardRecordReader::try_new(&[file_path])
            .into_file_plans()
            .next()
            .unwrap();
        let decoded_batches = plan
            .open_lanes(4)?
            .into_iter()
            .map(|lane| lane.reader.collect::<Result<Vec<_>, _>>().unwrap().len())
            .sum::<usize>();
        assert_eq!(decoded_batches, 8);

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
    fn testget_reverse_reader_single_message() -> io::Result<()> {
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

        let mut reader =
            MergedReverseRecordReader::try_new(&[file_path.into()]).merged_iter(schema, None);

        // Should get the batch
        let result_batch = reader
            .next()
            .expect("Failed to read batch")
            .expect("Invalid batch");
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
