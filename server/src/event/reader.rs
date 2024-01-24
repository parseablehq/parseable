use std::sync::Arc;

use itertools::Itertools;

use crate::{storage::StorageDir, utils::arrow::merged_reader::MergedReverseRecordReader};

#[allow(dead_code)]
pub fn get_data_from_arrows_file(dir: &StorageDir) -> Result<(), ()> {
    // I need the files in staging
    let time = chrono::Utc::now().naive_utc();
    let staging_files = dir.arrow_files_grouped_exclude_time(time);

    for (_, files) in staging_files {
        let record_reader = MergedReverseRecordReader::try_new(&files)?;

        let schema = Arc::new(record_reader.merged_schema());

        let record_batches = record_reader.merged_iter(schema).collect_vec();

        dbg!(record_batches.len());
        // for ref some in record_reader.merged_iter(schema) {
        //     dbg!("{:?}", some);
        // }
    }
    Ok(())
}
