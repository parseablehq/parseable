/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array},
    compute::cast,
    datatypes::Schema,
};
use arrow_schema::ArrowError;

// This function takes a new event's record batch and the
// current schema of the log stream. It returns a new record
// with nulls added to the fields that don't exist
// in the record batch (i.e. the event) but are present in the
// log stream schema.
// This is necessary because all the record batches in a log
// stream need to have all the fields.
pub fn adapt_batch(table_schema: Arc<Schema>, batch: &RecordBatch) -> RecordBatch {
    let batch_schema = batch.schema();
    let mut cols = Vec::with_capacity(table_schema.fields().len());
    for table_field in table_schema.fields() {
        if let Some((batch_idx, _)) = batch_schema.column_with_name(table_field.name()) {
            cols.push(Arc::clone(batch.column(batch_idx)));
        } else {
            cols.push(new_null_array(table_field.data_type(), batch.num_rows()));
        }
    }
    RecordBatch::try_new(table_schema, cols).unwrap()
}

/// Adapts a batch to `table_schema`, preserving arrays where their types already match.
///
/// Missing fields are represented by null arrays. Fields with compatible but differing
/// types are cast to the merged schema type before being returned.
pub fn try_adapt_batch(
    table_schema: Arc<Schema>,
    batch: &RecordBatch,
) -> Result<RecordBatch, ArrowError> {
    let batch_schema = batch.schema();
    let batch_cols = batch.columns();

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(table_schema.fields().len());
    for table_field in table_schema.fields() {
        if let Some((batch_idx, batch_field)) = batch_schema.column_with_name(table_field.name()) {
            let column = &batch_cols[batch_idx];
            if batch_field.data_type() == table_field.data_type() {
                cols.push(Arc::clone(column));
            } else {
                // Only normalize types compatible with the local schema merge;
                // never cast away a concrete field-type conflict.
                let mut merged_field = table_field.as_ref().clone();
                merged_field.try_merge(batch_field)?;
                if merged_field.data_type() != table_field.data_type() {
                    return Err(ArrowError::SchemaError(format!(
                        "field '{}' is not covered by the merged local schema",
                        table_field.name()
                    )));
                }
                cols.push(cast(column.as_ref(), table_field.data_type())?);
            }
        } else {
            cols.push(new_null_array(table_field.data_type(), batch.num_rows()));
        }
    }

    RecordBatch::try_new_with_options(
        table_schema,
        cols,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

#[cfg(test)]
mod tests {
    use super::try_adapt_batch;
    use arrow::{
        array::{Array, Int32Array, NullArray, StructArray},
        datatypes::{DataType, Field, Schema},
    };
    use std::sync::Arc;

    #[test]
    fn preserves_row_count_when_adapting_to_an_empty_schema() {
        let batch = arrow::array::RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef,
        )])
        .unwrap();

        let adapted = try_adapt_batch(Arc::new(Schema::empty()), &batch).unwrap();

        assert_eq!(adapted.num_columns(), 0);
        assert_eq!(adapted.num_rows(), 2);
    }

    #[test]
    fn rejects_concrete_conflicts_instead_of_casting_them() {
        let batch = arrow::array::RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        assert!(try_adapt_batch(schema, &batch).is_err());
    }

    #[test]
    fn exact_datatypes_retain_the_original_array() {
        let values: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let batch = arrow::array::RecordBatch::try_from_iter([("value", values.clone())]).unwrap();
        let adapted = try_adapt_batch(batch.schema(), &batch).unwrap();
        assert!(Arc::ptr_eq(adapted.column(0), &values));
    }

    #[test]
    fn casts_nested_null_struct_fields_to_the_merged_type() {
        let null_field = Arc::new(Field::new("value", DataType::Null, true));
        let source_type = DataType::Struct(vec![null_field.clone()].into());
        let source = StructArray::try_new(
            vec![null_field].into(),
            vec![Arc::new(NullArray::new(2))],
            None,
        )
        .unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("payload", source_type, true)])),
            vec![Arc::new(source)],
        )
        .unwrap();

        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let target_type = DataType::Struct(vec![value_field].into());
        let adapted = try_adapt_batch(
            Arc::new(Schema::new(vec![Field::new("payload", target_type, true)])),
            &batch,
        )
        .unwrap();

        let payload = adapted
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let values = payload
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.null_count(), 2);
    }
}
