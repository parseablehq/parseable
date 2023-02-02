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
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use std::any::Any;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

pub struct QueryTableProvider {
    arrow_files: Vec<PathBuf>,
    parquet_files: Vec<PathBuf>,
    storage: Option<ListingTable>,
    schema: Arc<Schema>,
}

impl QueryTableProvider {
    pub fn new(
        arrow_files: Vec<PathBuf>,
        parquet_files: Vec<PathBuf>,
        storage: Option<ListingTable>,
        schema: Arc<Schema>,
    ) -> Self {
        // By the time this query executes the arrow files could be converted to parquet files
        // we want to preserve these files as well in case

        let mut parquet_cached = crate::storage::CACHED_FILES.lock().expect("no poisoning");
        for file in &parquet_files {
            parquet_cached.upsert(file)
        }

        Self {
            arrow_files,
            parquet_files,
            storage,
            schema,
        }
    }

    async fn create_physical_plan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut mem_records: Vec<Vec<RecordBatch>> = Vec::new();
        let mut parquet_files = self.parquet_files.clone();
        for file in &self.arrow_files {
            load_arrows(file, &mut mem_records, &mut parquet_files);
        }

        let memtable = MemTable::try_new(Arc::clone(&self.schema), mem_records)?;
        let memexec = memtable
            .scan(ctx, projection.as_ref(), filters, limit)
            .await?;

        let cache_exec = if parquet_files.is_empty() {
            memexec
        } else {
            let listtable = local_parquet_table(&parquet_files, &self.schema)?;
            let listexec = listtable
                .scan(ctx, projection.as_ref(), filters, limit)
                .await?;
            Arc::new(UnionExec::new(vec![memexec, listexec]))
        };

        let mut exec = vec![cache_exec];
        if let Some(ref storage_listing) = self.storage {
            exec.push(
                storage_listing
                    .scan(ctx, projection.as_ref(), filters, limit)
                    .await?,
            );
        }

        Ok(Arc::new(UnionExec::new(exec)))
    }
}

impl Drop for QueryTableProvider {
    fn drop(&mut self) {
        let mut parquet_cached = crate::storage::CACHED_FILES.lock().expect("no poisoning");
        for file in &self.parquet_files {
            parquet_cached.remove(file)
        }
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
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(ctx, &projection, filters, limit)
            .await
    }
}

fn local_parquet_table(
    parquet_files: &[PathBuf],
    schema: &SchemaRef,
) -> Result<ListingTable, DataFusionError> {
    let listing_options = ListingOptions {
        file_extension: ".parquet".to_owned(),
        format: Arc::new(ParquetFormat::default().with_enable_pruning(Some(true))),
        file_sort_order: None,
        infinite_source: false,
        table_partition_cols: vec![],
        collect_stat: true,
        target_partitions: 1,
    };

    let paths = parquet_files
        .iter()
        .map(|path| {
            ListingTableUrl::parse(path.to_str().expect("path should is valid unicode"))
                .expect("path is valid for filesystem listing")
        })
        .collect();

    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(listing_options)
        .with_schema(Arc::clone(schema));

    ListingTable::try_new(config)
}

fn load_arrows(
    file: &PathBuf,
    mem_records: &mut Vec<Vec<RecordBatch>>,
    parquet_files: &mut Vec<PathBuf>,
) {
    let Ok(arrow_file) = File::open(file) else { return; };
    let Ok(reader)= StreamReader::try_new(arrow_file, None) else { return; };
    let records = reader
        .filter_map(|record| match record {
            Ok(record) => Some(record),
            Err(e) => {
                log::warn!("warning from arrow stream {:?}", e);
                None
            }
        })
        .collect();
    mem_records.push(records);
    let mut file = file.clone();
    file.set_extension("parquet");
    parquet_files.retain(|p| p != &file);
}
