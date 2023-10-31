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

use std::{fs::File, io::BufReader, path::PathBuf, sync::Arc};

use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use itertools::kmerge_by;

use super::{
    adapt_batch,
    reverse_reader::{reverse, OffsetReader},
};
use crate::{event::DEFAULT_TIMESTAMP_KEY, utils};

#[derive(Debug)]
pub struct MergedRecordReader {
    pub readers: Vec<StreamReader<BufReader<File>>>,
}

impl MergedRecordReader {
    pub fn try_new(files: &[PathBuf]) -> Result<Self, ()> {
        let mut readers = Vec::with_capacity(files.len());

        for file in files {
            let reader = StreamReader::try_new(File::open(file).unwrap(), None).map_err(|_| ())?;
            readers.push(reader);
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
    pub fn try_new(files: &[PathBuf]) -> Result<Self, ()> {
        let mut readers = Vec::with_capacity(files.len());
        for file in files {
            let reader =
                utils::arrow::reverse_reader::get_reverse_reader(File::open(file).unwrap())
                    .map_err(|_| ())?;
            readers.push(reader);
        }

        Ok(Self { readers })
    }

    pub fn merged_iter(self, schema: Arc<Schema>) -> impl Iterator<Item = RecordBatch> {
        let adapted_readers = self.readers.into_iter().map(|reader| reader.flatten());
        kmerge_by(adapted_readers, |a: &RecordBatch, b: &RecordBatch| {
            let a_time = get_timestamp_millis(a);
            let b_time = get_timestamp_millis(b);
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

fn get_timestamp_millis(batch: &RecordBatch) -> i64 {
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
