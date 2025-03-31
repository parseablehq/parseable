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
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_ipc::reader::FileReader;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use itertools::kmerge_by;
use tracing::error;

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    utils::arrow::{adapt_batch, reverse},
};

/// `ReverseReader` provides an iterator over record batches in an Arrow IPC file format
/// in reverse order (from the last batch to the first).
///
/// This is useful for scenarios where you need to process the most recent data first,
/// or when implementing time-series data exploration that starts with the latest records.
#[derive(Debug)]
pub struct ReverseReader {
    inner: FileReader<BufReader<File>>,
    /// Current index for iteration (starts from the last batch)
    idx: usize,
}

impl ReverseReader {
    /// Creates a new `ReverseReader` from given path.
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self, ArrowError> {
        let inner = FileReader::try_new(BufReader::new(File::open(path).unwrap()), None)?;
        let idx = inner.num_batches();

        Ok(Self { inner, idx })
    }

    /// Returns the schema of the underlying Arrow file.
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Iterator for ReverseReader {
    type Item = Result<RecordBatch, ArrowError>;

    /// Returns the next record batch in reverse order(latest to the first) from arrows file.
    ///
    /// Returns `None` when all batches have been processed.
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == 0 {
            return None;
        }

        self.idx -= 1;
        if let Err(e) = self.inner.set_index(self.idx) {
            return Some(Err(e));
        }

        self.inner.next()
    }
}

#[derive(Debug)]
pub struct MergedRecordReader {
    pub readers: Vec<ReverseReader>,
}

impl MergedRecordReader {
    pub fn new(paths: &[PathBuf]) -> Self {
        let mut readers = Vec::with_capacity(paths.len());

        for path in paths {
            //remove empty files before reading
            if path.metadata().unwrap().len() == 0 {
                error!("Invalid file detected, removing it: {path:?}");
                remove_file(path).unwrap();
            } else {
                let reader = ReverseReader::try_new(path).unwrap();
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
    use std::{
        fs::File,
        io::{self, Write},
        path::{Path, PathBuf},
        sync::Arc,
    };

    use arrow_array::{
        cast::AsArray, types::Int64Type, Array, Float64Array, Int32Array, Int64Array, RecordBatch,
        StringArray,
    };
    use arrow_ipc::{reader::FileReader, writer::FileWriter};
    use arrow_schema::{ArrowError, DataType, Field, Schema};
    use chrono::Utc;
    use temp_dir::TempDir;

    use crate::{
        parseable::staging::{
            reader::{MergedRecordReader, ReverseReader},
            writer::DiskWriter,
        },
        utils::time::TimeRange,
        OBJECT_STORE_DATA_GRANULARITY,
    };

    fn rb(rows: usize) -> RecordBatch {
        let array1: Arc<dyn Array> = Arc::new(Int64Array::from_iter(0..rows as i64));
        let array2: Arc<dyn Array> = Arc::new(Float64Array::from_iter((0..rows as i64).map(|i| {
            if i == 0 {
                0.0
            } else {
                1.0 / i as f64
            }
        })));
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
        let mut reader = MergedRecordReader::new(&[file_path]).merged_iter(schema, None);

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

        let mut reader = MergedRecordReader::new(&[file_path]).merged_iter(schema, None);

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

    fn create_test_arrow_file(path: &PathBuf, num_batches: usize) -> Result<(), ArrowError> {
        // Create schema
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let schema_ref = std::sync::Arc::new(schema);

        // Create file and writer
        let file = File::create(path)?;
        let mut writer = FileWriter::try_new(file, &schema_ref)?;

        // Create and write batches
        for i in 0..num_batches {
            let id_array =
                Int32Array::from(vec![i as i32 * 10, i as i32 * 10 + 1, i as i32 * 10 + 2]);
            let name_array = StringArray::from(vec![
                format!("batch_{i}_name_0"),
                format!("batch_{i}_name_1"),
                format!("batch_{i}_name_2"),
            ]);

            let batch = RecordBatch::try_new(
                schema_ref.clone(),
                vec![
                    std::sync::Arc::new(id_array),
                    std::sync::Arc::new(name_array),
                ],
            )?;

            writer.write(&batch)?;
        }

        writer.finish()?;
        Ok(())
    }

    #[test]
    fn test_reverse_reader_creation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.arrow");

        // Create test file with 3 batches
        create_test_arrow_file(&file_path, 3).unwrap();

        // Test successful creation
        let reader = ReverseReader::try_new(&file_path);
        assert!(reader.is_ok());

        // Test schema retrieval
        let reader = reader.unwrap();
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_reverse_reader_iteration() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.arrow");

        // Create test file with 3 batches
        create_test_arrow_file(&file_path, 3).unwrap();

        // Create reader and iterate
        let reader = ReverseReader::try_new(&file_path).unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        // Verify correct number of batches
        assert_eq!(batches.len(), 3);

        // Verify reverse order
        // Batch 2 (last written, first read)
        let batch0 = &batches[0];
        assert_eq!(batch0.num_columns(), 2);
        let id_array = batch0
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 20);

        // Batch 1 (middle)
        let batch1 = &batches[1];
        let id_array = batch1
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 10);

        // Batch 0 (first written, last read)
        let batch2 = &batches[2];
        let id_array = batch2
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 0);
    }

    #[test]
    fn test_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.arrow");

        // Create empty file with schema but no batches
        create_test_arrow_file(&file_path, 0).unwrap();

        let reader = ReverseReader::try_new(&file_path).unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        // Should be empty
        assert_eq!(batches.len(), 0);
    }

    #[test]
    fn test_invalid_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("invalid.txt");

        // Create a non-Arrow file
        let mut file = File::create(&file_path).unwrap();
        writeln!(&mut file, "This is not an Arrow file").unwrap();

        // Attempting to create a reader should fail
        let reader = ReverseReader::try_new(&file_path);
        assert!(reader.is_err());
    }

    #[test]
    fn test_num_batches() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.arrow");

        // Create test file with 5 batches
        create_test_arrow_file(&file_path, 5).unwrap();

        let reader = ReverseReader::try_new(&file_path).unwrap();
        assert_eq!(reader.count(), 5);
    }
}
