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

use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::utils::arrow::adapt_batch;

use super::mutable::MutableColumns;

#[derive(Default)]
pub struct MemWriter<const N: usize> {
    read_buffer: Vec<RecordBatch>,
    mutable_buffer: MutableColumns,
}

impl<const N: usize> MemWriter<N> {
    pub fn push(&mut self, rb: RecordBatch) {
        if self.mutable_buffer.len() + rb.num_rows() > N {
            // init new mutable columns with schema of current
            let schema = self.mutable_buffer.current_schema();
            let mut new_mutable_buffer = MutableColumns::default();
            new_mutable_buffer.push(RecordBatch::new_empty(Arc::new(schema)));
            // replace new mutable buffer with current one as that is full
            let mutable_buffer = std::mem::replace(&mut self.mutable_buffer, new_mutable_buffer);
            let filled_rb = mutable_buffer.into_recordbatch();
            self.read_buffer.push(filled_rb);
        }
        self.mutable_buffer.push(rb)
    }

    pub fn recordbatch_cloned(&self) -> Vec<RecordBatch> {
        let mut read_buffer = self.read_buffer.clone();
        let rb = self.mutable_buffer.recordbatch_cloned();
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
        let mut read_buffer = self.read_buffer;
        let rb = self.mutable_buffer.into_recordbatch();
        let schema = rb.schema();
        if rb.num_rows() > 0 {
            read_buffer.push(rb)
        }
        read_buffer
            .into_iter()
            .map(|rb| adapt_batch(&schema, rb))
            .collect()
    }
}
