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

use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use itertools::Itertools;

pub mod batch_adapter;
pub mod merged_reader;
pub mod reverse_reader;

pub use batch_adapter::adapt_batch;
pub use merged_reader::MergedRecordReader;

pub fn replace_columns(
    schema: Arc<Schema>,
    batch: &RecordBatch,
    indexes: &[usize],
    arrays: &[Arc<dyn Array + 'static>],
) -> RecordBatch {
    let mut batch_arrays = batch.columns().iter().map(Arc::clone).collect_vec();
    for (&index, arr) in indexes.iter().zip(arrays.iter()) {
        batch_arrays[index] = Arc::clone(arr);
    }
    RecordBatch::try_new(schema, batch_arrays).unwrap()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::replace_columns;

    #[test]
    fn check_replace() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let schema_ref = Arc::new(schema);

        let rb = RecordBatch::try_new(
            schema_ref.clone(),
            vec![
                Arc::new(Int32Array::from_value(0, 3)),
                Arc::new(Int32Array::from_value(0, 3)),
                Arc::new(Int32Array::from_value(0, 3)),
            ],
        )
        .unwrap();

        let arr: Arc<dyn Array + 'static> = Arc::new(Int32Array::from_value(0, 3));

        let new_rb = replace_columns(schema_ref.clone(), &rb, &[2], &[arr]);

        assert_eq!(new_rb.schema(), schema_ref);
        assert_eq!(new_rb.num_columns(), 3);
        assert_eq!(new_rb.num_rows(), 3)
    }
}

pub fn get_field<'a>(
    fields: &'a [impl AsRef<arrow_schema::Field>],
    name: &str,
) -> Option<&'a arrow_schema::Field> {
    fields
        .iter()
        .map(|x| x.as_ref())
        .find(|field| field.name() == name)
}
