use std::sync::Arc;

use arrow_array::RecordBatch;
use itertools::Itertools;

use crate::{storage::StorageDir, utils::arrow::merged_reader::MergedReverseRecordReader};

/// Reads data from an Arrow file.
///
/// This function takes a shared reference to `StorageDir` struct and reads all the data from
/// the Arrows files located at that path. With a MergedReverseRecordReader
/// It returns the data as a `RecordBatch`.
///
/// # Arguments
///
/// * `&dir` - A shared reference to `StorageDir` struct .
///
/// # Returns
///
/// A `Result<RecordBatch, &str>` representing the data read from the Arrow file. Returns an `ArrowError` if the file could not be read.
///
/// # Example
///
/// ```
/// let dir = StorageDir::new("stream_name");
/// let data = get_staged_records(dir);
/// match data {
///     Ok(record_batch) => println!("Data: {:?}", record_batch),
///     Err(e) => println!("Error reading data: {:?}", e),
/// }
/// ```
///
/// See [StorageDir](server/src/storage/staging.rs) for more information about StorageDir struct
/// See [MergedReverseRecordReader](server/src/utils/arrow/merged_reader.rs) for more information about MergedReverseRecordReader struct
/// This function is defined in [server/src/reader.rs](server/src/reader.rs).
pub fn get_staged_records(dir: &StorageDir) -> Result<Vec<RecordBatch>, &'static str> {
    let staging_files = dir.arrow_files();

    let record_reader = match MergedReverseRecordReader::try_new(&staging_files) {
        Ok(mrbr) => mrbr,
        Err(_) => {
            return Err("Cannot Get Merged Iterator");
        }
    };

    let schema = Arc::new(record_reader.merged_schema());
    Ok(record_reader.merged_iter(schema).collect_vec())
}
