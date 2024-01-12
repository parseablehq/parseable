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
 */

mod filter_optimizer;
mod listing_table_builder;
mod stream_schema_provider;

use chrono::{DateTime, Utc};
use chrono::{NaiveDateTime, TimeZone};
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{Explain, Filter, LogicalPlan, PlanType, ToStringifiedPlan};
use datafusion::prelude::*;
use itertools::Itertools;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sysinfo::{System, SystemExt};

use crate::event;
use crate::option::CONFIG;
use crate::storage::{ObjectStorageProvider, StorageDir};

use self::error::ExecuteError;

use self::stream_schema_provider::GlobalSchemaProvider;
pub use self::stream_schema_provider::PartialTimeFilter;

pub static QUERY_SESSION: Lazy<SessionContext> =
    Lazy::new(|| Query::create_session_context(CONFIG.storage()));

// A query request by client
pub struct Query {
    pub raw_logical_plan: LogicalPlan,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub filter_tag: Option<Vec<String>>,
}

impl Query {
    // create session context for this query
    pub fn create_session_context(
        storage: Arc<dyn ObjectStorageProvider + Send>,
    ) -> SessionContext {
        let runtime_config = storage
            .get_datafusion_runtime()
            .with_disk_manager(DiskManagerConfig::NewOs);

        let (pool_size, fraction) = match CONFIG.parseable.query_memory_pool_size {
            Some(size) => (size, 1.),
            None => {
                let mut system = System::new();
                system.refresh_memory();
                let available_mem = system.available_memory();
                (available_mem as usize, 0.85)
            }
        };

        let runtime_config = runtime_config.with_memory_limit(pool_size, fraction);
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

        let config = SessionConfig::default()
            .with_parquet_pruning(true)
            .with_prefer_existing_sort(true)
            .with_round_robin_repartition(true);

        let state = SessionState::new_with_config_rt(config, runtime);
        let schema_provider = Arc::new(GlobalSchemaProvider {
            storage: storage.get_object_store(),
        });
        state
            .catalog_list()
            .catalog(&state.config_options().catalog.default_catalog)
            .expect("default catalog is provided by datafusion")
            .register_schema(
                &state.config_options().catalog.default_schema,
                schema_provider,
            )
            .unwrap();

        SessionContext::new_with_state(state)
    }

    pub async fn execute(&self) -> Result<(Vec<RecordBatch>, Vec<String>), ExecuteError> {
        let df = QUERY_SESSION
            .execute_logical_plan(self.final_logical_plan())
            .await?;

        let fields = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .cloned()
            .collect_vec();

        let results = df.collect().await?;
        Ok((results, fields))
    }

    /// return logical plan with all time filters applied through
    fn final_logical_plan(&self) -> LogicalPlan {
        let filters = self.filter_tag.clone().and_then(tag_filter);
        // see https://github.com/apache/arrow-datafusion/pull/8400
        // this can be eliminated in later version of datafusion but with slight caveat
        // transform cannot modify stringified plans by itself
        // we by knowing this plan is not in the optimization procees chose to overwrite the stringified plan
        match self.raw_logical_plan.clone() {
            LogicalPlan::Explain(plan) => {
                let transformed = transform(
                    plan.plan.as_ref().clone(),
                    self.start.naive_utc(),
                    self.end.naive_utc(),
                    filters,
                );
                LogicalPlan::Explain(Explain {
                    verbose: plan.verbose,
                    stringified_plans: vec![
                        transformed.to_stringified(PlanType::InitialLogicalPlan)
                    ],
                    plan: Arc::new(transformed),
                    schema: plan.schema,
                    logical_optimization_succeeded: plan.logical_optimization_succeeded,
                })
            }
            x => transform(x, self.start.naive_utc(), self.end.naive_utc(), filters),
        }
    }

    pub fn table_name(&self) -> Option<String> {
        let mut visitor = TableScanVisitor::default();
        let _ = self.raw_logical_plan.visit(&mut visitor);
        visitor.into_inner().pop()
    }
}

#[derive(Debug, Default)]
struct TableScanVisitor {
    tables: Vec<String>,
}

impl TableScanVisitor {
    fn into_inner(self) -> Vec<String> {
        self.tables
    }
}

impl TreeNodeVisitor for TableScanVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> Result<VisitRecursion, DataFusionError> {
        match node {
            LogicalPlan::TableScan(table) => {
                self.tables.push(table.table_name.table().to_string());
                Ok(VisitRecursion::Stop)
            }
            _ => Ok(VisitRecursion::Continue),
        }
    }
}

fn tag_filter(filters: Vec<String>) -> Option<Expr> {
    filters
        .iter()
        .map(|literal| {
            Expr::Column(Column::from_name(event::DEFAULT_TAGS_KEY))
                .like(lit(format!("%{}%", literal)))
        })
        .reduce(or)
}

fn transform(
    plan: LogicalPlan,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
    filters: Option<Expr>,
) -> LogicalPlan {
    plan.transform(&|plan| match plan {
        LogicalPlan::TableScan(table) => {
            let mut new_filters = vec![];
            if !table_contains_any_time_filters(&table) {
                let start_time_filter = PartialTimeFilter::Low(std::ops::Bound::Included(
                    start_time,
                ))
                .binary_expr(Expr::Column(Column::new(
                    Some(table.table_name.to_owned_reference()),
                    event::DEFAULT_TIMESTAMP_KEY,
                )));
                let end_time_filter = PartialTimeFilter::High(std::ops::Bound::Excluded(end_time))
                    .binary_expr(Expr::Column(Column::new(
                        Some(table.table_name.to_owned_reference()),
                        event::DEFAULT_TIMESTAMP_KEY,
                    )));
                new_filters.push(start_time_filter);
                new_filters.push(end_time_filter);
            }

            if let Some(tag_filters) = filters.clone() {
                new_filters.push(tag_filters)
            }

            let new_filter = new_filters.into_iter().reduce(and);

            if let Some(new_filter) = new_filter {
                let filter =
                    Filter::try_new(new_filter, Arc::new(LogicalPlan::TableScan(table))).unwrap();
                Ok(Transformed::Yes(LogicalPlan::Filter(filter)))
            } else {
                Ok(Transformed::No(LogicalPlan::TableScan(table)))
            }
        }
        x => Ok(Transformed::No(x)),
    })
    .expect("transform only transforms the tablescan")
}

fn table_contains_any_time_filters(table: &datafusion::logical_expr::TableScan) -> bool {
    table
        .filters
        .iter()
        .filter_map(|x| {
            if let Expr::BinaryExpr(binexpr) = x {
                Some(binexpr)
            } else {
                None
            }
        })
        .any(|expr| matches!(&*expr.left, Expr::Column(Column { name, .. }) if (name == event::DEFAULT_TIMESTAMP_KEY)))
}

#[allow(dead_code)]
fn get_staging_prefixes(
    stream_name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> HashMap<PathBuf, Vec<PathBuf>> {
    let dir = StorageDir::new(stream_name);
    let mut files = dir.arrow_files_grouped_by_time();
    files.retain(|k, _| path_intersects_query(k, start, end));
    files
}

fn path_intersects_query(path: &Path, starttime: DateTime<Utc>, endtime: DateTime<Utc>) -> bool {
    let time = time_from_path(path);
    starttime <= time && time <= endtime
}

fn time_from_path(path: &Path) -> DateTime<Utc> {
    let prefix = path
        .file_name()
        .expect("all given path are file")
        .to_str()
        .expect("filename is valid");

    // Next three in order will be date, hour and minute
    let mut components = prefix.splitn(3, '.');

    let date = components.next().expect("date=xxxx-xx-xx");
    let hour = components.next().expect("hour=xx");
    let minute = components.next().expect("minute=xx");

    let year = date[5..9].parse().unwrap();
    let month = date[10..12].parse().unwrap();
    let day = date[13..15].parse().unwrap();
    let hour = hour[5..7].parse().unwrap();
    let minute = minute[7..9].parse().unwrap();

    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .unwrap()
}

pub mod error {
    use crate::storage::ObjectStorageError;
    use datafusion::error::DataFusionError;

    #[derive(Debug, thiserror::Error)]
    pub enum ExecuteError {
        #[error("Query Execution failed due to error in object storage: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
        #[error("Query Execution failed due to error in datafusion: {0}")]
        Datafusion(#[from] DataFusionError),
    }
}

#[cfg(test)]
mod tests {
    use super::time_from_path;
    use std::path::PathBuf;

    #[test]
    fn test_time_from_parquet_path() {
        let path = PathBuf::from("date=2022-01-01.hour=00.minute=00.hostname.data.parquet");
        let time = time_from_path(path.as_path());
        assert_eq!(time.timestamp(), 1640995200);
    }
}
