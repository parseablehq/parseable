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
    fs::File,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use itertools::Itertools;

use crate::utils::arrow::adapt_batch;

#[derive(Default)]
pub struct Writer {
    pub mem: MemWriter<16384>,
    pub disk: HashMap<String, StreamWriter<File>>,
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
            let right = rb.num_rows() - left;
            let left_slice = rb.slice(0, left);
            let right_slice = if left < rb.num_rows() {
                Some(rb.slice(left, right))
            } else {
                None
            };
            self.inner.push(left_slice);
            // take all records
            let src = Vec::with_capacity(self.inner.len());
            let inner = std::mem::replace(&mut self.inner, src);

            if let Some(right_slice) = right_slice {
                self.inner.push(right_slice);
            }

            Some(inner)
        } else {
            self.inner.push(rb.clone());
            None
        }
    }
}
