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

use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_ipc::reader::FileReader;
use arrow_schema::Schema;
use itertools::kmerge_by;
use tracing::error;

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    utils::arrow::{adapt_batch, reverse},
};

#[derive(Debug)]
pub struct MergedRecordReader {
    pub readers: Vec<FileReader<BufReader<File>>>,
}

impl MergedRecordReader {
    pub fn new(files: &[PathBuf]) -> Self {
        let mut readers = Vec::with_capacity(files.len());

        for file in files {
            //remove empty files before reading
            if file.metadata().unwrap().len() == 0 {
                error!("Invalid file detected, removing it: {:?}", file);
                remove_file(file).unwrap();
            } else {
                let Ok(reader) =
                    FileReader::try_new(BufReader::new(File::open(file).unwrap()), None)
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

#[cfg(test)]
mod tests {
    use std::{fs::File, path::Path, sync::Arc};

    use arrow_array::{
        cast::AsArray, types::Int64Type, Array, Float64Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_ipc::{reader::FileReader, writer::FileWriter};
    use temp_dir::TempDir;

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

    fn write_file(rbs: &[RecordBatch], path: &Path) {
        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new_buffered(file, &rbs[0].schema()).unwrap();

        for rb in rbs {
            writer.write(rb).unwrap()
        }

        writer.finish().unwrap();
    }

    #[test]
    fn test_empty_row() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.arrows");
        let rb = rb(0);
        write_file(&[rb], &path);
        let reader = File::open(path).unwrap();
        let mut reader = FileReader::try_new_buffered(reader, None).unwrap();
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 0);
    }

    #[test]
    fn test_one_row() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.arrows");
        let rb = rb(1);
        write_file(&[rb], &path);
        let reader = File::open(path).unwrap();
        let mut reader = FileReader::try_new_buffered(reader, None).unwrap();
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 1);
    }

    #[test]
    fn test_multiple_row_multiple_rbs() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.arrows");
        write_file(&[rb(1), rb(2), rb(3)], &path);
        let reader = File::open(path).unwrap();
        let mut reader = FileReader::try_new_buffered(reader, None).unwrap();
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
}
