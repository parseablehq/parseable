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
        if self.mutable_buffer.len() + rb.num_rows() < N {
            self.mutable_buffer.push(rb)
        } else {
            let schema = self.mutable_buffer.current_schema();
            let mut new_mutable_buffer = MutableColumns::default();
            new_mutable_buffer.push(RecordBatch::new_empty(Arc::new(schema)));
            let mutable_buffer = std::mem::replace(&mut self.mutable_buffer, new_mutable_buffer);
            let rb = mutable_buffer.into_recordbatch();
            self.read_buffer.push(rb);
        }
    }

    #[allow(unused)]
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
