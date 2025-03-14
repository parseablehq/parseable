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
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::BufWriter,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_ipc::writer::FileWriter;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use itertools::Itertools;
use tracing::trace;

use crate::parseable::ARROW_FILE_EXTENSION;
use crate::utils::arrow::adapt_batch;

use super::StagingError;

/// Context regarding `.arrows` file being persisted onto disk
pub struct DiskWriter {
    inner: FileWriter<BufWriter<File>>,
    /// Used to ensure un"finish"ed arrow files are renamed on "finish"
    path_prefix: String,
}

impl DiskWriter {
    pub fn new(path_prefix: String, schema: &Schema) -> Result<Self, StagingError> {
        // Live writes happen into partfile
        let partfile_path = format!("{path_prefix}.part.{ARROW_FILE_EXTENSION}");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(partfile_path)?;

        Ok(Self {
            inner: FileWriter::try_new_buffered(file, schema)?,
            path_prefix,
        })
    }

    /// Appends records into a `.part.arrows` file
    pub fn write(&mut self, rb: &RecordBatch) -> Result<(), StagingError> {
        self.inner.write(rb)?;

        Ok(())
    }

    /// Ensures `.arrows`` file in staging directory is "finish"ed and renames it from "part".
    pub fn finish(&mut self) -> Result<(), StagingError> {
        self.inner.finish()?;

        let partfile_path = format!("{}.part.{ARROW_FILE_EXTENSION}", self.path_prefix);
        let arrows_path = format!("{}.data.{ARROW_FILE_EXTENSION}", self.path_prefix);

        // Rename from part file to finished arrows file
        std::fs::rename(partfile_path, &arrows_path)?;
        trace!("Finished arrows file: {arrows_path}");

        Ok(())
    }
}

/// Structure to keep recordbatches in memory.
///
/// Any new schema is updated in the schema map.
/// Recordbatches are pushed to mutable buffer first and then concated together and pushed to read buffer
#[derive(Debug)]
pub struct MemWriter<const N: usize> {
    schema: Schema,
    // for checking uniqueness of schema
    schema_map: HashSet<String>,
    pub read_buffer: Vec<RecordBatch>,
    pub mutable_buffer: MutableBuffer<N>,
}

impl<const N: usize> Default for MemWriter<N> {
    fn default() -> Self {
        Self {
            schema: Schema::empty(),
            schema_map: HashSet::default(),
            read_buffer: Vec::default(),
            mutable_buffer: MutableBuffer::default(),
        }
    }
}

impl<const N: usize> MemWriter<N> {
    pub fn push(&mut self, schema_key: &str, rb: &RecordBatch) {
        if !self.schema_map.contains(schema_key) {
            self.schema_map.insert(schema_key.to_owned());
            self.schema = Schema::try_merge([self.schema.clone(), (*rb.schema()).clone()]).unwrap();
        }

        if let Some(record) = self.mutable_buffer.push(rb) {
            let record = concat_records(&Arc::new(self.schema.clone()), &record);
            self.read_buffer.push(record);
        }
    }

    pub fn clear(&mut self) {
        self.schema = Schema::empty();
        self.schema_map.clear();
        self.read_buffer.clear();
        self.mutable_buffer.inner.clear();
    }

    pub fn recordbatch_cloned(&self, schema: &Arc<Schema>) -> Vec<RecordBatch> {
        let mut read_buffer = self.read_buffer.clone();
        if !self.mutable_buffer.inner.is_empty() {
            let rb = concat_records(schema, &self.mutable_buffer.inner);
            read_buffer.push(rb)
        }

        read_buffer
            .into_iter()
            .map(|rb| adapt_batch(schema, &rb))
            .collect()
    }
}

fn concat_records(schema: &Arc<Schema>, record: &[RecordBatch]) -> RecordBatch {
    let records = record.iter().map(|x| adapt_batch(schema, x)).collect_vec();
    let record = concat_batches(schema, records.iter()).unwrap();
    record
}

#[derive(Debug, Default)]
pub struct MutableBuffer<const N: usize> {
    pub inner: Vec<RecordBatch>,
}

impl<const N: usize> MutableBuffer<N> {
    fn push(&mut self, rb: &RecordBatch) -> Option<Vec<RecordBatch>> {
        if self.inner.len() + rb.num_rows() >= N {
            let left = N - self.inner.len();
            let left_slice = rb.slice(0, left);
            self.inner.push(left_slice);
            // take all records
            let src = Vec::with_capacity(self.inner.len());
            let inner = std::mem::replace(&mut self.inner, src);

            if left < rb.num_rows() {
                let right = rb.num_rows() - left;
                self.inner.push(rb.slice(left, right));
            }

            Some(inner)
        } else {
            self.inner.push(rb.clone());
            None
        }
    }
}

#[derive(Default)]
pub struct Writer<const N: usize> {
    pub mem: MemWriter<N>,
    pub disk: HashMap<String, DiskWriter>,
}
