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

use std::{collections::HashSet, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema};
use itertools::Itertools;

use crate::utils::arrow::adapt_batch;


/// Structure to keep recordbatches in memory.
///
/// Any new schema is updated in the schema map.
/// Recordbatches are pushed to mutable buffer first and then concated together and pushed to read buffer
/// Note Change Name to something more meaningful
#[derive(Debug)]
pub struct MemWriter {
    schema: Schema,
    schema_map: HashSet<String>,    // not sure what this is used for yet
    in_memory: Vec<RecordBatch>,
}

impl Default for MemWriter {
    fn default() -> Self {
        MemWriter {
            schema: Schema::empty(),
            schema_map: HashSet::default(), // why default
            in_memory: Vec::default(),      // why default  probably with capacity will be better? or will it be more mem alloc
        }
    }
}

impl MemWriter {
    /// Push the a recordbatch to the in memory buffer
    /// to fetch it back when we need it
    ///
    /// * `schema_key` - The schema key to be used to store the recordbatch
    /// * `rb` - The recordbatch to be stored
    ///
    /// # Retruns
    /// * Result<(), ArrowError> - Schema Merge is failable
    /// * Ok(()) - If the recordbatch is successfully pushed to the in memory buffer
    ///
    /// this function is in mem_writer.rs(server/src/event/writer/mem_writer.rs)
    pub fn push(&mut self, schema_key: &str, rb: RecordBatch) {
        if !self.contains_schema(schema_key) {
            self.insert_schema_map(schema_key);
            self.schema = Schema::try_merge([self.schema.clone(), (*rb.schema()).clone()]).unwrap();
        }

        self.append_record(rb);
    }

    pub fn recordbatches_cloned(&self, schema: &Arc<Schema>) -> Vec<RecordBatch> {
        let mem = self.in_memory.clone();
        let omem = Self::concat_records(schema, &mem);
        let mem = mem.into_iter().map(|x| adapt_batch(schema, &x)).collect_vec();

        mem
    }

    pub fn clear(&mut self) {
        self.in_memory.clear();
    }

    fn contains_schema(&self, schema_key: &str) -> bool {
        self.schema_map.contains(schema_key)
    }

    fn insert_schema_map(&mut self, schema_key: &str) -> bool {
        self.schema_map.insert(schema_key.to_owned())
    }

    fn append_record(&mut self, rb: RecordBatch) {
        self.in_memory.push(rb);
    }

    fn concat_records(schema: &Arc<Schema>, record: &[RecordBatch]) -> RecordBatch {
        let records = record.iter().map(|x| adapt_batch(schema, x)).collect_vec();

        // can this fail?
        let record = arrow_select::concat::concat_batches(schema, records.iter()).unwrap();
        record
    }
}