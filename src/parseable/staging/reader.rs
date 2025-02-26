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
    fs::{remove_file, File},
    io::BufReader,
    path::PathBuf,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use tracing::error;

use crate::utils::arrow::{adapt_batch, reverse};

#[derive(Debug)]
pub struct MergedRecordReader {
    pub readers: Vec<StreamReader<BufReader<File>>>,
}

impl MergedRecordReader {
    pub fn try_new(files: &[PathBuf]) -> Self {
        let mut readers = Vec::with_capacity(files.len());

        for file in files {
            //remove empty files before reading
            if file.metadata().unwrap().len() == 0 {
                error!("Invalid file detected, removing it: {:?}", file);
                remove_file(file).unwrap();
            } else {
                let Ok(reader) =
                    StreamReader::try_new(BufReader::new(File::open(file).unwrap()), None)
                else {
                    error!("Invalid file detected, ignoring it: {:?}", file);
                    continue;
                };

                readers.push(reader);
            }
        }

        Self { readers }
    }

    pub fn merged_schema(&self) -> Schema {
        Schema::try_merge(
            self.readers
                .iter()
                .map(|reader| reader.schema().as_ref().clone()),
        )
        .unwrap()
    }

    pub fn merged_iter(self, schema: Arc<Schema>) -> impl Iterator<Item = RecordBatch> {
        self.readers
            .into_iter()
            .flat_map(|reader| reader.flatten())
            .map(|batch| reverse(&batch))
            .map(move |batch| adapt_batch(&schema, &batch))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufReader, Cursor, Read, Seek},
        sync::Arc,
    };

    use arrow_array::{
        cast::AsArray, types::Int64Type, Array, Float64Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_ipc::{
        reader::StreamReader,
        writer::{
            write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter,
        },
    };

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

    fn get_reverse_reader<T: Read + Seek>(reader: T) -> StreamReader<BufReader<T>> {
        StreamReader::try_new(BufReader::new(reader), None).unwrap()
    }

    #[test]
    fn test_empty_row() {
        let rb = rb(0);
        let buf = write_mem(&[rb]);
        let reader = Cursor::new(buf);
        let mut reader = get_reverse_reader(reader);
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 0);
    }

    #[test]
    fn test_one_row() {
        let rb = rb(1);
        let buf = write_mem(&[rb]);
        let reader = Cursor::new(buf);
        let mut reader = get_reverse_reader(reader);
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 1);
    }

    #[test]
    fn test_multiple_row_multiple_rbs() {
        let buf = write_mem(&[rb(1), rb(2), rb(3)]);
        let reader = Cursor::new(buf);
        let mut reader = get_reverse_reader(reader);
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 1);
        let col1_val: Vec<i64> = rb
            .column(0)
            .as_primitive::<Int64Type>()
            .iter()
            .flatten()
            .collect();
        assert_eq!(col1_val, vec![0]);

        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 2);
        let col1_val: Vec<i64> = rb
            .column(0)
            .as_primitive::<Int64Type>()
            .iter()
            .flatten()
            .collect();
        assert_eq!(col1_val, vec![0, 1]);

        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 3);
        let col1_val: Vec<i64> = rb
            .column(0)
            .as_primitive::<Int64Type>()
            .iter()
            .flatten()
            .collect();
        assert_eq!(col1_val, vec![0, 1, 2]);
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
        let reader = get_reverse_reader(buf).flatten();

        let mut sum = 0;
        for rb in reader {
            sum += 1;
            assert!(rb.num_rows() > 0);
        }

        assert_eq!(sum, 10000);
    }
}
