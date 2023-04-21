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

use std::{cmp::Ordering, sync::Arc};

use arrow_array::{
    builder::{
        BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
        TimestampMillisecondBuilder, UInt64Builder,
    },
    new_null_array, Array, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch,
    StringArray, TimestampMillisecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use itertools::Itertools;

macro_rules! nested_list {
    ($t:expr) => {
        DataType::List(Box::new(Field::new(
            "item",
            DataType::List(Box::new(Field::new("item", $t, true))),
            true,
        )))
    };
}

macro_rules! unit_list {
    ($t:expr) => {
        DataType::List(Box::new(Field::new("item", $t, true)))
    };
}

#[derive(Debug)]
pub enum NestedListBuilder {
    Utf8(ListBuilder<ListBuilder<StringBuilder>>),
    Boolean(ListBuilder<ListBuilder<BooleanBuilder>>),
    Int64(ListBuilder<ListBuilder<Int64Builder>>),
    UInt64(ListBuilder<ListBuilder<UInt64Builder>>),
    Timestamp(ListBuilder<ListBuilder<TimestampMillisecondBuilder>>),
    Float64(ListBuilder<ListBuilder<Float64Builder>>),
}

impl NestedListBuilder {
    pub fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => NestedListBuilder::Boolean(ListBuilder::new(ListBuilder::new(
                BooleanBuilder::new(),
            ))),
            DataType::Int64 => {
                NestedListBuilder::Int64(ListBuilder::new(ListBuilder::new(Int64Builder::new())))
            }
            DataType::UInt64 => {
                NestedListBuilder::UInt64(ListBuilder::new(ListBuilder::new(UInt64Builder::new())))
            }
            DataType::Float64 => NestedListBuilder::Float64(ListBuilder::new(ListBuilder::new(
                Float64Builder::new(),
            ))),
            DataType::Timestamp(_, _) => NestedListBuilder::Timestamp(ListBuilder::new(
                ListBuilder::new(TimestampMillisecondBuilder::new()),
            )),
            DataType::Utf8 => {
                NestedListBuilder::Utf8(ListBuilder::new(ListBuilder::new(StringBuilder::new())))
            }
            _ => unreachable!(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            NestedListBuilder::Utf8(_) => nested_list!(DataType::Utf8),
            NestedListBuilder::Boolean(_) => nested_list!(DataType::Boolean),
            NestedListBuilder::Int64(_) => nested_list!(DataType::Int64),
            NestedListBuilder::UInt64(_) => nested_list!(DataType::UInt64),
            NestedListBuilder::Timestamp(_) => {
                nested_list!(DataType::Timestamp(TimeUnit::Millisecond, None))
            }
            NestedListBuilder::Float64(_) => nested_list!(DataType::Float64),
        }
    }
}

#[derive(Debug)]
pub enum UnitListBuilder {
    Utf8(ListBuilder<StringBuilder>),
    Boolean(ListBuilder<BooleanBuilder>),
    Int64(ListBuilder<Int64Builder>),
    UInt64(ListBuilder<UInt64Builder>),
    Timestamp(ListBuilder<TimestampMillisecondBuilder>),
    Float64(ListBuilder<Float64Builder>),
    List(NestedListBuilder),
}

impl UnitListBuilder {
    pub fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => UnitListBuilder::Boolean(ListBuilder::new(BooleanBuilder::new())),
            DataType::Int64 => UnitListBuilder::Int64(ListBuilder::new(Int64Builder::new())),
            DataType::UInt64 => UnitListBuilder::UInt64(ListBuilder::new(UInt64Builder::new())),
            DataType::Float64 => UnitListBuilder::Float64(ListBuilder::new(Float64Builder::new())),
            DataType::Timestamp(_, _) => {
                UnitListBuilder::Timestamp(ListBuilder::new(TimestampMillisecondBuilder::new()))
            }
            DataType::Utf8 => UnitListBuilder::Utf8(ListBuilder::new(StringBuilder::new())),
            DataType::List(field) => {
                UnitListBuilder::List(NestedListBuilder::new(field.data_type()))
            }
            _ => unreachable!(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            UnitListBuilder::Utf8(_) => unit_list!(DataType::Utf8),
            UnitListBuilder::Boolean(_) => unit_list!(DataType::Boolean),
            UnitListBuilder::Int64(_) => unit_list!(DataType::Int64),
            UnitListBuilder::UInt64(_) => unit_list!(DataType::UInt64),
            UnitListBuilder::Timestamp(_) => {
                unit_list!(DataType::Timestamp(TimeUnit::Millisecond, None))
            }
            UnitListBuilder::Float64(_) => unit_list!(DataType::Float64),
            UnitListBuilder::List(inner) => inner.data_type(),
        }
    }
}

#[derive(Debug)]
pub enum MutableColumnArray {
    Utf8(StringBuilder),
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Timestamp(TimestampMillisecondBuilder),
    Float64(Float64Builder),
    List(UnitListBuilder),
}

impl MutableColumnArray {
    pub fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => MutableColumnArray::Boolean(BooleanBuilder::new()),
            DataType::Int64 => MutableColumnArray::Int64(Int64Builder::new()),
            DataType::UInt64 => MutableColumnArray::UInt64(UInt64Builder::new()),
            DataType::Float64 => MutableColumnArray::Float64(Float64Builder::new()),
            DataType::Timestamp(_, _) => {
                MutableColumnArray::Timestamp(TimestampMillisecondBuilder::new())
            }
            DataType::Utf8 => MutableColumnArray::Utf8(StringBuilder::new()),
            DataType::List(field) => {
                MutableColumnArray::List(UnitListBuilder::new(field.data_type()))
            }
            _ => unreachable!(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            MutableColumnArray::Utf8(_) => DataType::Utf8,
            MutableColumnArray::Boolean(_) => DataType::Boolean,
            MutableColumnArray::Int64(_) => DataType::Int64,
            MutableColumnArray::UInt64(_) => DataType::UInt64,
            MutableColumnArray::Timestamp(_) => DataType::Timestamp(TimeUnit::Millisecond, None),
            MutableColumnArray::Float64(_) => DataType::Float64,
            MutableColumnArray::List(inner) => inner.data_type(),
        }
    }

    fn push_nulls(&mut self, n: usize) {
        match self {
            MutableColumnArray::Utf8(col) => (0..n).for_each(|_| col.append_null()),
            MutableColumnArray::Boolean(col) => col.append_nulls(n),
            MutableColumnArray::Int64(col) => col.append_nulls(n),
            MutableColumnArray::UInt64(col) => col.append_nulls(n),
            MutableColumnArray::Timestamp(col) => col.append_nulls(n),
            MutableColumnArray::Float64(col) => col.append_nulls(n),
            MutableColumnArray::List(col) => match col {
                UnitListBuilder::Utf8(col) => (0..n).for_each(|_| col.append(false)),
                UnitListBuilder::Boolean(col) => (0..n).for_each(|_| col.append(false)),
                UnitListBuilder::Int64(col) => (0..n).for_each(|_| col.append(false)),
                UnitListBuilder::UInt64(col) => (0..n).for_each(|_| col.append(false)),
                UnitListBuilder::Timestamp(col) => (0..n).for_each(|_| col.append(false)),
                UnitListBuilder::Float64(col) => (0..n).for_each(|_| col.append(false)),
                UnitListBuilder::List(col) => match col {
                    NestedListBuilder::Utf8(col) => (0..n).for_each(|_| col.append(false)),
                    NestedListBuilder::Boolean(col) => (0..n).for_each(|_| col.append(false)),
                    NestedListBuilder::Int64(col) => (0..n).for_each(|_| col.append(false)),
                    NestedListBuilder::UInt64(col) => (0..n).for_each(|_| col.append(false)),
                    NestedListBuilder::Timestamp(col) => (0..n).for_each(|_| col.append(false)),
                    NestedListBuilder::Float64(col) => (0..n).for_each(|_| col.append(false)),
                },
            },
        }
    }

    fn cloned_array(&self) -> Arc<dyn Array> {
        match self {
            MutableColumnArray::Utf8(col) => Arc::new(col.finish_cloned()),
            MutableColumnArray::Boolean(col) => Arc::new(col.finish_cloned()),
            MutableColumnArray::Int64(col) => Arc::new(col.finish_cloned()),
            MutableColumnArray::UInt64(col) => Arc::new(col.finish_cloned()),
            MutableColumnArray::Timestamp(col) => Arc::new(col.finish_cloned()),
            MutableColumnArray::Float64(col) => Arc::new(col.finish_cloned()),
            MutableColumnArray::List(col) => match col {
                UnitListBuilder::Utf8(col) => Arc::new(col.finish_cloned()),
                UnitListBuilder::Boolean(col) => Arc::new(col.finish_cloned()),
                UnitListBuilder::Int64(col) => Arc::new(col.finish_cloned()),
                UnitListBuilder::UInt64(col) => Arc::new(col.finish_cloned()),
                UnitListBuilder::Timestamp(col) => Arc::new(col.finish_cloned()),
                UnitListBuilder::Float64(col) => Arc::new(col.finish_cloned()),
                UnitListBuilder::List(col) => match col {
                    NestedListBuilder::Utf8(col) => Arc::new(col.finish_cloned()),
                    NestedListBuilder::Boolean(col) => Arc::new(col.finish_cloned()),
                    NestedListBuilder::Int64(col) => Arc::new(col.finish_cloned()),
                    NestedListBuilder::UInt64(col) => Arc::new(col.finish_cloned()),
                    NestedListBuilder::Timestamp(col) => Arc::new(col.finish_cloned()),
                    NestedListBuilder::Float64(col) => Arc::new(col.finish_cloned()),
                },
            },
        }
    }

    fn into_array(mut self) -> Arc<dyn Array> {
        match &mut self {
            MutableColumnArray::Utf8(col) => Arc::new(col.finish()),
            MutableColumnArray::Boolean(col) => Arc::new(col.finish()),
            MutableColumnArray::Int64(col) => Arc::new(col.finish()),
            MutableColumnArray::UInt64(col) => Arc::new(col.finish()),
            MutableColumnArray::Timestamp(col) => Arc::new(col.finish()),
            MutableColumnArray::Float64(col) => Arc::new(col.finish()),
            MutableColumnArray::List(col) => match col {
                UnitListBuilder::Utf8(col) => Arc::new(col.finish()),
                UnitListBuilder::Boolean(col) => Arc::new(col.finish()),
                UnitListBuilder::Int64(col) => Arc::new(col.finish()),
                UnitListBuilder::UInt64(col) => Arc::new(col.finish()),
                UnitListBuilder::Timestamp(col) => Arc::new(col.finish()),
                UnitListBuilder::Float64(col) => Arc::new(col.finish()),
                UnitListBuilder::List(col) => match col {
                    NestedListBuilder::Utf8(col) => Arc::new(col.finish()),
                    NestedListBuilder::Boolean(col) => Arc::new(col.finish()),
                    NestedListBuilder::Int64(col) => Arc::new(col.finish()),
                    NestedListBuilder::UInt64(col) => Arc::new(col.finish()),
                    NestedListBuilder::Timestamp(col) => Arc::new(col.finish()),
                    NestedListBuilder::Float64(col) => Arc::new(col.finish()),
                },
            },
        }
    }
}

#[derive(Debug)]
pub struct MutableColumn {
    name: String,
    column: MutableColumnArray,
}

impl MutableColumn {
    pub fn new(name: String, column: MutableColumnArray) -> Self {
        Self { name, column }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn feild(&self) -> Field {
        Field::new(&self.name, self.column.data_type(), true)
    }
}

#[derive(Debug, Default)]
pub struct MutableColumns {
    columns: Vec<MutableColumn>,
    len: usize,
}

impl MutableColumns {
    pub fn push(&mut self, rb: RecordBatch) {
        let num_rows = rb.num_rows();
        let schema = rb.schema();
        let rb = schema.fields().iter().zip(rb.columns().iter());

        // start index map to next location in self columns
        let mut index = 0;
        'rb: for (field, arr) in rb {
            // for field in rb look at same field in columns or insert.
            // fill with null while traversing if rb field name is greater than column name
            while let Some(col) = self.columns.get_mut(index) {
                match col.name().cmp(field.name()) {
                    Ordering::Equal => {
                        update_column(&mut col.column, Arc::clone(arr));
                        // goto next field in rb
                        index += 1;
                        continue 'rb;
                    }
                    Ordering::Greater => {
                        let mut new_column = MutableColumn::new(
                            field.name().to_owned(),
                            MutableColumnArray::new(field.data_type()),
                        );
                        update_column(
                            &mut new_column.column,
                            new_null_array(field.data_type(), self.len),
                        );
                        update_column(&mut new_column.column, Arc::clone(arr));
                        self.columns.insert(index, new_column);
                        index += 1;
                        continue 'rb;
                    }
                    Ordering::Less => {
                        col.column.push_nulls(num_rows);
                        index += 1;
                    }
                }
            }

            // if inner loop finishes this means this column is suppose to be at the end of columns
            let mut new_column = MutableColumn::new(
                field.name().to_owned(),
                MutableColumnArray::new(field.data_type()),
            );
            update_column(
                &mut new_column.column,
                new_null_array(field.data_type(), self.len),
            );
            update_column(&mut new_column.column, Arc::clone(arr));
            self.columns.push(new_column);
            index += 1;
        }

        // fill any columns yet to be updated with nulls
        for col in self.columns[index..].iter_mut() {
            col.column.push_nulls(num_rows)
        }

        self.len += num_rows
    }

    pub fn into_recordbatch(self) -> RecordBatch {
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());

        for MutableColumn { name, column } in self.columns {
            let field = Field::new(name, column.data_type(), true);
            fields.push(field);
            arrays.push(column.into_array());
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    pub fn recordbatch_cloned(&self) -> RecordBatch {
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());

        for MutableColumn { name, column } in &self.columns {
            let field = Field::new(name, column.data_type(), true);
            fields.push(field);
            arrays.push(column.cloned_array());
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn current_schema(&self) -> Schema {
        Schema::new(self.columns.iter().map(|x| x.feild()).collect_vec())
    }
}

fn update_column(col: &mut MutableColumnArray, arr: Arc<dyn Array>) {
    match col {
        MutableColumnArray::Utf8(col) => downcast::<StringArray>(&arr)
            .iter()
            .for_each(|v| col.append_option(v)),
        MutableColumnArray::Boolean(col) => downcast::<BooleanArray>(&arr)
            .iter()
            .for_each(|v| col.append_option(v)),
        MutableColumnArray::Int64(col) => downcast::<Int64Array>(&arr)
            .iter()
            .for_each(|v| col.append_option(v)),
        MutableColumnArray::UInt64(col) => downcast::<UInt64Array>(&arr)
            .iter()
            .for_each(|v| col.append_option(v)),
        MutableColumnArray::Timestamp(col) => downcast::<TimestampMillisecondArray>(&arr)
            .iter()
            .for_each(|v| col.append_option(v)),
        MutableColumnArray::Float64(col) => downcast::<Float64Array>(&arr)
            .iter()
            .for_each(|v| col.append_option(v)),
        MutableColumnArray::List(col) => match col {
            UnitListBuilder::Utf8(col) => {
                let arr = into_vec_array(&arr);
                let iter = arr
                    .iter()
                    .map(|x| x.as_ref().map(|x| downcast::<StringArray>(x).iter()));
                col.extend(iter);
            }
            UnitListBuilder::Boolean(col) => {
                let arr = into_vec_array(&arr);
                let iter = arr
                    .iter()
                    .map(|x| x.as_ref().map(|x| downcast::<BooleanArray>(x).iter()));
                col.extend(iter);
            }
            UnitListBuilder::Int64(col) => {
                let arr = into_vec_array(&arr);
                let iter = arr
                    .iter()
                    .map(|x| x.as_ref().map(|x| downcast::<Int64Array>(x).iter()));
                col.extend(iter);
            }
            UnitListBuilder::UInt64(col) => {
                let arr = into_vec_array(&arr);
                let iter = arr
                    .iter()
                    .map(|x| x.as_ref().map(|x| downcast::<UInt64Array>(x).iter()));
                col.extend(iter);
            }
            UnitListBuilder::Timestamp(col) => {
                let arr = into_vec_array(&arr);
                let iter = arr.iter().map(|x| {
                    x.as_ref()
                        .map(|x| downcast::<TimestampMillisecondArray>(x).iter())
                });
                col.extend(iter);
            }
            UnitListBuilder::Float64(col) => {
                let arr = into_vec_array(&arr);
                let iter = arr
                    .iter()
                    .map(|x| x.as_ref().map(|x| downcast::<Float64Array>(x).iter()));
                col.extend(iter);
            }
            UnitListBuilder::List(col) => match col {
                NestedListBuilder::Utf8(col) => {
                    let arr = into_vec_vec_array(&arr);
                    let iter = arr.iter().map(|x| {
                        x.as_ref().map(|arr| {
                            arr.iter()
                                .map(|x| x.as_ref().map(|x| downcast::<StringArray>(x).iter()))
                        })
                    });

                    col.extend(iter)
                }
                NestedListBuilder::Boolean(col) => {
                    let arr = into_vec_vec_array(&arr);
                    let iter = arr.iter().map(|x| {
                        x.as_ref().map(|arr| {
                            arr.iter()
                                .map(|x| x.as_ref().map(|x| downcast::<BooleanArray>(x).iter()))
                        })
                    });

                    col.extend(iter)
                }
                NestedListBuilder::Int64(col) => {
                    let arr = into_vec_vec_array(&arr);

                    let iter = arr.iter().map(|x| {
                        x.as_ref().map(|arr| {
                            arr.iter()
                                .map(|x| x.as_ref().map(|x| downcast::<Int64Array>(x).iter()))
                        })
                    });

                    col.extend(iter)
                }
                NestedListBuilder::UInt64(col) => {
                    let arr = into_vec_vec_array(&arr);

                    let iter = arr.iter().map(|x| {
                        x.as_ref().map(|arr| {
                            arr.iter()
                                .map(|x| x.as_ref().map(|x| downcast::<UInt64Array>(x).iter()))
                        })
                    });

                    col.extend(iter)
                }
                NestedListBuilder::Timestamp(col) => {
                    let arr = into_vec_vec_array(&arr);

                    let iter = arr.iter().map(|x| {
                        x.as_ref().map(|arr| {
                            arr.iter().map(|x| {
                                x.as_ref()
                                    .map(|x| downcast::<TimestampMillisecondArray>(x).iter())
                            })
                        })
                    });

                    col.extend(iter)
                }
                NestedListBuilder::Float64(col) => {
                    let arr = into_vec_vec_array(&arr);

                    let iter = arr.iter().map(|x| {
                        x.as_ref().map(|arr| {
                            arr.iter()
                                .map(|x| x.as_ref().map(|x| downcast::<Float64Array>(x).iter()))
                        })
                    });

                    col.extend(iter)
                }
            },
        },
    };
}

fn downcast<T: 'static>(arr: &dyn Array) -> &T {
    arr.as_any().downcast_ref::<T>().unwrap()
}

type VecArray = Vec<Option<Arc<dyn Array>>>;

fn into_vec_array(arr: &dyn Array) -> VecArray {
    arr.as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .iter()
        .collect()
}

fn into_vec_vec_array(arr: &dyn Array) -> Vec<Option<VecArray>> {
    arr.as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .iter()
        .map(|arr| {
            arr.map(|arr| {
                arr.as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .iter()
                    .collect()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{MutableColumnArray, MutableColumns};

    macro_rules! check_array_builder {
        ($t:expr) => {
            assert_eq!(MutableColumnArray::new(&$t).data_type(), $t)
        };
    }

    macro_rules! check_unit_list_builder {
        ($t:expr) => {
            assert_eq!(
                MutableColumnArray::new(&DataType::List(Box::new(Field::new("item", $t, true))))
                    .data_type(),
                DataType::List(Box::new(Field::new("item", $t, true)))
            )
        };
    }

    macro_rules! check_nested_list_builder {
        ($t:expr) => {
            assert_eq!(
                MutableColumnArray::new(&DataType::List(Box::new(Field::new(
                    "item",
                    DataType::List(Box::new(Field::new("item", $t, true))),
                    true
                ))))
                .data_type(),
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::List(Box::new(Field::new("item", $t, true))),
                    true
                )))
            )
        };
    }

    #[test]
    fn create_mutable_col_and_check_datatype() {
        check_array_builder!(DataType::Boolean);
        check_array_builder!(DataType::Int64);
        check_array_builder!(DataType::UInt64);
        check_array_builder!(DataType::Float64);
        check_array_builder!(DataType::Utf8);
        check_array_builder!(DataType::Timestamp(TimeUnit::Millisecond, None));
        check_unit_list_builder!(DataType::Boolean);
        check_unit_list_builder!(DataType::Int64);
        check_unit_list_builder!(DataType::UInt64);
        check_unit_list_builder!(DataType::Float64);
        check_unit_list_builder!(DataType::Utf8);
        check_unit_list_builder!(DataType::Timestamp(TimeUnit::Millisecond, None));
        check_nested_list_builder!(DataType::Boolean);
        check_nested_list_builder!(DataType::Int64);
        check_nested_list_builder!(DataType::UInt64);
        check_nested_list_builder!(DataType::Float64);
        check_nested_list_builder!(DataType::Utf8);
        check_nested_list_builder!(DataType::Timestamp(TimeUnit::Millisecond, None));
    }

    #[test]
    fn empty_columns_push_single_col() {
        let mut columns = MutableColumns::default();

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let col1 = Arc::new(BooleanArray::from(vec![true, false, true]));
        let rb = RecordBatch::try_new(Arc::new(schema), vec![col1]).unwrap();

        columns.push(rb);

        assert_eq!(columns.columns.len(), 1)
    }

    #[test]
    fn empty_columns_push_empty_rb() {
        let mut columns = MutableColumns::default();

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let rb = RecordBatch::new_empty(Arc::new(schema));

        columns.push(rb);

        assert_eq!(columns.columns.len(), 1);
        assert_eq!(columns.len, 0);
    }

    #[test]
    fn one_empty_column_push_new_empty_column_before() {
        let mut columns = MutableColumns::default();

        let schema = Schema::new(vec![Field::new("b", DataType::Boolean, true)]);
        let rb = RecordBatch::new_empty(Arc::new(schema));
        columns.push(rb);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let rb = RecordBatch::new_empty(Arc::new(schema));
        columns.push(rb);

        assert_eq!(columns.columns.len(), 2);
        assert_eq!(columns.len, 0);
    }

    #[test]
    fn one_column_push_new_column_before() {
        let mut columns = MutableColumns::default();

        let schema = Schema::new(vec![Field::new("b", DataType::Boolean, true)]);
        let col2 = Arc::new(BooleanArray::from(vec![true, false, true]));
        let rb = RecordBatch::try_new(Arc::new(schema), vec![col2]).unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 1);
        assert_eq!(columns.len, 3);

        let MutableColumnArray::Boolean(builder) = &columns.columns[0].column else {unreachable!()};
        {
            let arr = builder.finish_cloned();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(true), Some(false), Some(true)]
            )
        }

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let col1 = Arc::new(BooleanArray::from(vec![true, true, true]));
        let rb = RecordBatch::try_new(Arc::new(schema), vec![col1]).unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 2);
        assert_eq!(columns.len, 6);

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[0].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![None, None, None, Some(true), Some(true), Some(true)]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[1].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(true), Some(false), Some(true), None, None, None]
            )
        }
    }

    #[test]
    fn two_column_push_new_column_before() {
        let mut columns = MutableColumns::default();
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("c", DataType::Boolean, true),
        ]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(vec![false, true, false])),
                Arc::new(BooleanArray::from(vec![false, false, true])),
            ],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 2);
        assert_eq!(columns.len, 3);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![true, false, false]))],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 3);
        assert_eq!(columns.len, 6);

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[0].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![None, None, None, Some(true), Some(false), Some(false)]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[1].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(false), Some(true), Some(false), None, None, None]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[2].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(false), Some(false), Some(true), None, None, None]
            )
        }
    }

    #[test]
    fn two_column_push_new_column_middle() {
        let mut columns = MutableColumns::default();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("c", DataType::Boolean, true),
        ]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(vec![true, false, false])),
                Arc::new(BooleanArray::from(vec![false, false, true])),
            ],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 2);
        assert_eq!(columns.len, 3);

        let schema = Schema::new(vec![Field::new("b", DataType::Boolean, true)]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![false, true, false]))],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 3);
        assert_eq!(columns.len, 6);

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[0].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(true), Some(false), Some(false), None, None, None]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[1].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![None, None, None, Some(false), Some(true), Some(false)]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[2].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(false), Some(false), Some(true), None, None, None]
            )
        }
    }

    #[test]
    fn two_column_push_new_column_after() {
        let mut columns = MutableColumns::default();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(vec![true, false, false])),
                Arc::new(BooleanArray::from(vec![false, true, false])),
            ],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 2);
        assert_eq!(columns.len, 3);

        let schema = Schema::new(vec![Field::new("c", DataType::Boolean, true)]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![false, false, true]))],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 3);
        assert_eq!(columns.len, 6);

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[0].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(true), Some(false), Some(false), None, None, None]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[1].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(false), Some(true), Some(false), None, None, None]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[2].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![None, None, None, Some(false), Some(false), Some(true)]
            )
        }
    }

    #[test]
    fn two_empty_column_push_new_column_before() {
        let mut columns = MutableColumns::default();
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("c", DataType::Boolean, true),
        ]);
        let rb = RecordBatch::new_empty(Arc::new(schema));
        columns.push(rb);

        assert_eq!(columns.columns.len(), 2);
        assert_eq!(columns.len, 0);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![true, false, false]))],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 3);
        assert_eq!(columns.len, 3);

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[0].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(true), Some(false), Some(false)]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[1].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(arr.iter().collect::<Vec<_>>(), vec![None, None, None])
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[2].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(arr.iter().collect::<Vec<_>>(), vec![None, None, None])
        }
    }

    #[test]
    fn two_empty_column_push_new_column_middle() {
        let mut columns = MutableColumns::default();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("c", DataType::Boolean, true),
        ]);
        let rb = RecordBatch::new_empty(Arc::new(schema));
        columns.push(rb);

        assert_eq!(columns.columns.len(), 2);
        assert_eq!(columns.len, 0);

        let schema = Schema::new(vec![Field::new("b", DataType::Boolean, true)]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![false, true, false]))],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 3);
        assert_eq!(columns.len, 3);

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[0].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(arr.iter().collect::<Vec<_>>(), vec![None, None, None])
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[1].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(false), Some(true), Some(false)]
            )
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[2].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(arr.iter().collect::<Vec<_>>(), vec![None, None, None])
        }
    }

    #[test]
    fn two_empty_column_push_new_column_after() {
        let mut columns = MutableColumns::default();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let rb = RecordBatch::new_empty(Arc::new(schema));
        columns.push(rb);

        assert_eq!(columns.columns.len(), 2);
        assert_eq!(columns.len, 0);

        let schema = Schema::new(vec![Field::new("c", DataType::Boolean, true)]);
        let rb = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![false, false, true]))],
        )
        .unwrap();
        columns.push(rb);

        assert_eq!(columns.columns.len(), 3);
        assert_eq!(columns.len, 3);

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[0].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(arr.iter().collect::<Vec<_>>(), vec![None, None, None])
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[1].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(arr.iter().collect::<Vec<_>>(), vec![None, None, None])
        }

        let MutableColumnArray::Boolean(builder) = &mut columns.columns[2].column else {unreachable!()};
        {
            let arr = builder.finish();
            assert_eq!(
                arr.iter().collect::<Vec<_>>(),
                vec![Some(false), Some(false), Some(true)]
            )
        }
    }
}
