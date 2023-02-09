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
 */

use datafusion::arrow::array::new_null_array;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;

use std::sync::Arc;

pub fn adapt_batch(table_schema: &Schema, batch: RecordBatch) -> RecordBatch {
    let batch_schema = &*batch.schema();

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(table_schema.fields().len());
    let batch_cols = batch.columns().to_vec();

    for table_field in table_schema.fields() {
        if let Some((batch_idx, _)) = batch_schema.column_with_name(table_field.name().as_str()) {
            cols.push(Arc::clone(&batch_cols[batch_idx]));
        } else {
            cols.push(new_null_array(table_field.data_type(), batch.num_rows()))
        }
    }

    let merged_schema = Arc::new(table_schema.clone());

    RecordBatch::try_new(merged_schema, cols).unwrap()
}
