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
 */

use std::{
    io::{self, BufReader, Read, Seek, SeekFrom},
    vec::IntoIter,
};

use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::{reader::StreamReader, root_as_message_unchecked, MessageHeader};
use arrow_select::take::take;
use byteorder::{LittleEndian, ReadBytesExt};

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
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
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

    Ok(StreamReader::try_new(OffsetReader::new(reader, messages), None).unwrap())
}

pub fn reverse(rb: &RecordBatch) -> RecordBatch {
    let indices = UInt64Array::from_iter_values((0..rb.num_rows()).rev().map(|x| x as u64));
    let arrays = rb
        .columns()
        .iter()
        .map(|col| take(&col, &indices, None).unwrap())
        .collect();
    RecordBatch::try_new(rb.schema(), arrays).unwrap()
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
    use std::{io::Cursor, sync::Arc};

    use arrow_array::{
        cast::AsArray, types::Int64Type, Array, Float64Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_ipc::writer::{
        write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter,
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

        let schema = data_gen.schema_to_bytes(&rb1.schema(), &options);
        write_message(&mut buf, schema, &options).unwrap();

        for i in (1..=3).cycle().skip(1).take(10000) {
            let (_, encoded_message) = data_gen
                .encoded_batch(&rb(i), &mut dictionary_tracker, &options)
                .unwrap();
            write_message(&mut buf, encoded_message, &options).unwrap();
        }

        let schema = data_gen.schema_to_bytes(&rb1.schema(), &options);
        write_message(&mut buf, schema, &options).unwrap();

        let buf = Cursor::new(buf);
        let mut reader = get_reverse_reader(buf).unwrap().flatten();

        let mut sum = 0;
        while let Some(rb) = reader.next() {
            sum += 1;
            assert!(rb.num_rows() > 0);
        }

        assert_eq!(sum, 10000);
    }
}
