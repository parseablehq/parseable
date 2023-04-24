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
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use std::any::Any;
use std::sync::Arc;

use crate::storage::staging::ReadBuf;
use crate::storage::ObjectStorage;
use crate::utils::arrow::adapt_batch;

pub struct QueryTableProvider {
    storage_prefixes: Vec<String>,
    storage: Arc<dyn ObjectStorage + Send>,
    readable_buffer: Vec<ReadBuf>,
    schema: Arc<Schema>,
}

impl QueryTableProvider {
    pub fn new(
        storage_prefixes: Vec<String>,
        storage: Arc<dyn ObjectStorage + Send>,
        readable_buffer: Vec<ReadBuf>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            storage_prefixes,
            storage,
            readable_buffer,
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
        let memexec = self.get_mem_exec(ctx, projection, filters, limit).await?;
        let table = self
            .storage
            .query_table(self.storage_prefixes.clone(), Arc::clone(&self.schema))?;

        let mut exec = Vec::new();
        if let Some(memexec) = memexec {
            exec.push(memexec);
        }

        if let Some(ref storage_listing) = table {
            exec.push(
                storage_listing
                    .scan(ctx, projection, filters, limit)
                    .await?,
            );
        }

        if exec.is_empty() {
            Ok(Arc::new(EmptyExec::new(false, Arc::clone(&self.schema))))
        } else {
            Ok(Arc::new(UnionExec::new(exec)))
        }
    }

    async fn get_mem_exec(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if self.readable_buffer.is_empty() {
            return Ok(None);
        }

        let mem_records: Vec<Vec<_>> = self
            .readable_buffer
            .iter()
            .map(|r| {
                r.buf
                    .iter()
                    .cloned()
                    .map(|rb| adapt_batch(&self.schema, rb))
                    .collect()
            })
            .collect();

        let memtable = MemTable::try_new(Arc::clone(&self.schema), mem_records)?;
        let memexec = memtable.scan(ctx, projection, filters, limit).await?;
        Ok(Some(memexec))
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
}
