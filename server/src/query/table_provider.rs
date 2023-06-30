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

use arrow_select::concat::concat;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::streaming::{PartitionStream, StreamingTable};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};
use datafusion::prelude::Expr;
use futures_util::{Stream, StreamExt};

use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use std::vec;

use crate::utils::arrow::MergedRecordReader;

pub struct QueryTableProvider {
    // ( arrow files ) sorted by time asc
    staging_arrows: Vec<Vec<PathBuf>>,
    // remote table
    storage: Option<Arc<ListingTable>>,
    schema: Arc<Schema>,
}

impl QueryTableProvider {
    pub fn new(
        staging_arrows: Vec<Vec<PathBuf>>,
        storage: Option<Arc<ListingTable>>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            staging_arrows,
            storage,
            schema,
        }
    }

    async fn create_physical_plan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut exec = vec![];
        let local_table = get_streaming_table(self.staging_arrows.clone(), self.schema.clone());
        if let Some(table) = local_table {
            exec.push(table.scan(ctx, projection, filters, limit).await?)
        }
        if let Some(storage_listing) = self.storage.clone() {
            exec.push(
                storage_listing
                    .scan(ctx, projection, filters, limit)
                    .await?,
            );
        }

        let exec: Arc<dyn ExecutionPlan> = if exec.is_empty() {
            Arc::new(EmptyExec::new(false, self.schema.clone()))
        } else if exec.len() == 1 {
            exec.pop().unwrap()
        } else {
            Arc::new(UnionExec::new(exec))
        };

        Ok(exec)
    }
}

#[async_trait]
impl TableProvider for QueryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(ctx, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

// create streaming table from arrow files on staging.
// partitions are created in sorted order.
// if partition cannot be created mid way then only latest available partitions are returned
fn get_streaming_table(
    staging_arrow: Vec<Vec<PathBuf>>,
    schema: Arc<Schema>,
) -> Option<StreamingTable> {
    let mut partitions: Vec<Arc<dyn PartitionStream>> = Vec::new();

    for files in staging_arrow {
        let partition = ArrowFilesPartition {
            schema: schema.clone(),
            files,
        };
        partitions.push(Arc::new(partition))
    }

    if partitions.is_empty() {
        None
    } else {
        //todo remove unwrap
        Some(StreamingTable::try_new(schema, partitions).unwrap())
    }
}

struct ConcatIterator<T: Iterator<Item = RecordBatch>> {
    buffer: Vec<RecordBatch>,
    schema: Arc<Schema>,
    max_size: usize,
    iter: T,
}

impl<T: Iterator<Item = RecordBatch>> Iterator for ConcatIterator<T> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut size = 0;

        while size < self.max_size {
            if let Some(rb) = self.iter.next() {
                size += rb.num_rows();
                self.buffer.push(rb);
            } else {
                break;
            }
        }

        if self.buffer.is_empty() {
            return None;
        }

        // create new batch
        let field_num = self.schema.fields().len();
        let mut arrays = Vec::with_capacity(field_num);
        for i in 0..field_num {
            let array = concat(
                &self
                    .buffer
                    .iter()
                    .map(|batch| batch.column(i).as_ref())
                    .collect::<Vec<_>>(),
            )
            .expect("all records are of same schema and valid");
            arrays.push(array);
        }
        let res = RecordBatch::try_new(self.schema.clone(), arrays).unwrap();

        // clear buffer
        self.buffer.clear();

        //return resulting batch
        Some(res)
    }
}

struct ArrowRecordBatchStream<T: Stream<Item = RecordBatch> + Unpin> {
    schema: Arc<Schema>,
    stream: T,
}

impl<T> Stream for ArrowRecordBatchStream<T>
where
    T: Stream<Item = RecordBatch> + Unpin,
{
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx).map(|x| x.map(Ok))
    }
}

impl<T> RecordBatchStream for ArrowRecordBatchStream<T>
where
    T: Stream<Item = RecordBatch> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct ArrowFilesPartition {
    schema: Arc<Schema>,
    files: Vec<PathBuf>,
}

impl PartitionStream for ArrowFilesPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let reader = MergedRecordReader::try_new(&self.files).unwrap();
        let reader = reader.merged_iter(self.schema.clone());
        let reader = ConcatIterator {
            buffer: Vec::new(),
            schema: self.schema.clone(),
            max_size: 10000,
            iter: reader,
        };

        Box::pin(ArrowRecordBatchStream {
            schema: self.schema.clone(),
            stream: futures::stream::iter(reader),
        })
    }
}
