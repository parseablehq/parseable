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

#![allow(unused)]

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
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
use itertools::Itertools;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::{MergedRecordReader, ObjectStorage};

pub struct QueryTableProvider {
    // parquet - ( arrow files )
    staging_arrows: Vec<(PathBuf, Vec<PathBuf>)>,
    other_staging_parquet: Vec<PathBuf>,
    storage_prefixes: Vec<String>,
    storage: Arc<dyn ObjectStorage + Send>,
    schema: Arc<Schema>,
}

impl QueryTableProvider {
    pub fn new(
        staging_arrows: Vec<(PathBuf, Vec<PathBuf>)>,
        other_staging_parquet: Vec<PathBuf>,
        storage_prefixes: Vec<String>,
        storage: Arc<dyn ObjectStorage + Send>,
        schema: Arc<Schema>,
    ) -> Self {
        // By the time this query executes the arrow files could be converted to parquet files
        // we want to preserve these files as well in case
        let mut parquet_cached = crate::storage::CACHED_FILES.lock().expect("no poisoning");

        for file in staging_arrows
            .iter()
            .map(|(p, _)| p)
            .chain(other_staging_parquet.iter())
        {
            parquet_cached.upsert(file)
        }

        Self {
            staging_arrows,
            other_staging_parquet,
            storage_prefixes,
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
        let mut mem_records: Vec<Vec<RecordBatch>> = Vec::new();
        let mut parquet_files = Vec::new();

        for (staging_parquet, arrow_files) in &self.staging_arrows {
            if !load_arrows(arrow_files, Arc::clone(&self.schema), &mut mem_records) {
                parquet_files.push(staging_parquet.clone())
            }
        }

        parquet_files.extend(self.other_staging_parquet.clone());

        let memtable = MemTable::try_new(Arc::clone(&self.schema), mem_records)?;
        let memexec = memtable.scan(ctx, projection, filters, limit).await?;

        let cache_exec = if parquet_files.is_empty() {
            memexec
        } else {
            match local_parquet_table(&parquet_files, &self.schema) {
                Some(table) => {
                    let listexec = table.scan(ctx, projection, filters, limit).await?;
                    Arc::new(UnionExec::new(vec![memexec, listexec]))
                }
                None => memexec,
            }
        };

        let mut exec = vec![cache_exec];

        let table = self
            .storage
            .query_table(self.storage_prefixes.clone(), Arc::clone(&self.schema))?;

        if let Some(ref storage_listing) = table {
            exec.push(
                storage_listing
                    .scan(ctx, projection, filters, limit)
                    .await?,
            );
        }

        Ok(Arc::new(UnionExec::new(exec)))
    }
}

impl Drop for QueryTableProvider {
    fn drop(&mut self) {
        let mut parquet_cached = crate::storage::CACHED_FILES.lock().expect("no poisoning");
        for file in self
            .staging_arrows
            .iter()
            .map(|(p, _)| p)
            .chain(self.other_staging_parquet.iter())
        {
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(ctx, projection, filters, limit)
            .await
    }
}

fn local_parquet_table(parquet_files: &[PathBuf], schema: &SchemaRef) -> Option<ListingTable> {
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
        .flat_map(|path| {
            ListingTableUrl::parse(path.to_str().expect("path should is valid unicode"))
        })
        .collect_vec();

    if paths.is_empty() {
        return None;
    }

    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(listing_options)
        .with_schema(Arc::clone(schema));

    match ListingTable::try_new(config) {
        Ok(table) => Some(table),
        Err(err) => {
            log::error!("Local parquet query failed due to err: {err}");
            None
        }
    }
}

fn load_arrows(
    files: &[PathBuf],
    schema: Arc<Schema>,
    mem_records: &mut Vec<Vec<RecordBatch>>,
) -> bool {
    let Ok(reader) = MergedRecordReader::try_new(files.to_owned()) else { return false };
    let Ok(iter ) = reader.get_owned_iterator(schema) else { return false };
    mem_records.push(iter.collect());
    true
}
