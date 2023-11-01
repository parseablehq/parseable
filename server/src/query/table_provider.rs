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

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTable;

use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use std::any::Any;
use std::sync::Arc;
use std::vec;

use crate::utils::arrow::reverse_reader::reverse;

pub struct QueryTableProvider {
    staging: Option<MemTable>,
    // remote table
    storage: Option<Arc<ListingTable>>,
    schema: Arc<Schema>,
}

impl QueryTableProvider {
    pub fn try_new(
        staging: Option<Vec<RecordBatch>>,
        storage: Option<Arc<ListingTable>>,
        schema: Arc<Schema>,
    ) -> Result<Self, DataFusionError> {
        // in place reverse transform
        let staging = if let Some(mut staged_batches) = staging {
            staged_batches[..].reverse();
            staged_batches
                .iter_mut()
                .for_each(|batch| *batch = reverse(batch));
            Some(staged_batches)
        } else {
            None
        };

        let memtable = staging
            .map(|records| MemTable::try_new(schema.clone(), vec![records]))
            .transpose()?;

        Ok(Self {
            staging: memtable,
            storage,
            schema,
        })
    }

    async fn create_physical_plan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut exec = vec![];

        if let Some(table) = &self.staging {
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
            let schema = match projection {
                Some(projection) => Arc::new(self.schema.project(projection)?),
                None => self.schema.clone(),
            };
            Arc::new(EmptyExec::new(false, schema))
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
