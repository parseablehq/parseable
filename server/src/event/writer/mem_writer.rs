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

use std::{collections::HashMap, sync::Arc};

use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use itertools::kmerge_by;

use crate::utils::arrow::adapt_batch;

#[derive(Default)]
pub struct MemWriter<const N: usize> {
    read_buffer: Vec<RecordBatch>,
    mutable_buffer: HashMap<String, Vec<RecordBatch>>,
}

impl<const N: usize> MemWriter<N> {
    pub fn push(&mut self, schema_key: &str, rb: RecordBatch) {
        if self.mutable_buffer.len() + rb.num_rows() > N {
            // init new mutable columns with schema of current
            let schema = self.current_mutable_schema();
            // replace new mutable buffer with current one as that is full
            let mutable_buffer = std::mem::take(&mut self.mutable_buffer);
            let batches = mutable_buffer.values().collect();
            self.read_buffer.push(merge_rb(batches, Arc::new(schema)));
        }

        if let Some(buf) = self.mutable_buffer.get_mut(schema_key) {
            buf.push(rb);
        } else {
            self.mutable_buffer.insert(schema_key.to_owned(), vec![rb]);
        }
    }

    pub fn recordbatch_cloned(&self) -> Vec<RecordBatch> {
        let mut read_buffer = self.read_buffer.clone();
        let schema = self.current_mutable_schema();
        let batches = self.mutable_buffer.values().collect();
        let rb = merge_rb(batches, Arc::new(schema));
        let schema = rb.schema();
        if rb.num_rows() > 0 {
            read_buffer.push(rb)
        }

        read_buffer
            .into_iter()
            .map(|rb| adapt_batch(&schema, rb))
            .collect()
    }

    pub fn finalize(self) -> Vec<RecordBatch> {
        let schema = self.current_mutable_schema();
        let mut read_buffer = self.read_buffer;
        let batches = self.mutable_buffer.values().collect();
        let rb = merge_rb(batches, Arc::new(schema));
        let schema = rb.schema();
        if rb.num_rows() > 0 {
            read_buffer.push(rb)
        }
        read_buffer
            .into_iter()
            .map(|rb| adapt_batch(&schema, rb))
            .collect()
    }

    fn current_mutable_schema(&self) -> Schema {
        Schema::try_merge(
            self.mutable_buffer
                .values()
                .flat_map(|rb| rb.first())
                .map(|rb| rb.schema().as_ref().clone()),
        )
        .unwrap()
    }
}

fn merge_rb(rb: Vec<&Vec<RecordBatch>>, schema: Arc<Schema>) -> RecordBatch {
    let sorted_rb: Vec<RecordBatch> = kmerge_by(rb, |a: &&RecordBatch, b: &&RecordBatch| {
        let a: &TimestampMillisecondArray = a
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        let b: &TimestampMillisecondArray = b
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        a.value(0) < b.value(0)
    })
    .map(|batch| adapt_batch(&schema, batch.clone()))
    .collect();

    // must be true for this to work
    // each rb is of same schema. ( adapt_schema should do this )
    // datatype is same
    concat_batches(&schema, sorted_rb.iter()).unwrap()
}
