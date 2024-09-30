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
pub mod stream_schema_provider;

use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{Explain, Filter, LogicalPlan, PlanType, ToStringifiedPlan};
use datafusion::prelude::*;
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sysinfo::System;

use self::error::ExecuteError;
use self::stream_schema_provider::GlobalSchemaProvider;
pub use self::stream_schema_provider::PartialTimeFilter;
use crate::event;
use crate::metadata::STREAM_INFO;
use crate::option::CONFIG;
use crate::storage::{ObjectStorageProvider, StorageDir};

pub static QUERY_SESSION: Lazy<SessionContext> =
    Lazy::new(|| Query::create_session_context(CONFIG.storage()));

// A query request by client
#[derive(Debug)]
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

    pub async fn execute(
        &self,
        stream_name: String,
    ) -> Result<(Vec<RecordBatch>, Vec<String>), ExecuteError> {
        let time_partition = STREAM_INFO.get_time_partition(&stream_name)?;

        let df = QUERY_SESSION
            .execute_logical_plan(self.final_logical_plan(&time_partition))
            .await?;

        let fields = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .cloned()
            .collect_vec();

        if fields.is_empty() {
            return Ok((vec![], fields));
        }

        let results = df.collect().await?;
        Ok((results, fields))
    }

    /// return logical plan with all time filters applied through
    fn final_logical_plan(&self, time_partition: &Option<String>) -> LogicalPlan {
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
                    time_partition,
                );
                LogicalPlan::Explain(Explain {
                    verbose: plan.verbose,
                    stringified_plans: vec![transformed
                        .data
                        .to_stringified(PlanType::InitialLogicalPlan)],
                    plan: Arc::new(transformed.data),
                    schema: plan.schema,
                    logical_optimization_succeeded: plan.logical_optimization_succeeded,
                })
            }
            x => {
                transform(
                    x,
                    self.start.naive_utc(),
                    self.end.naive_utc(),
                    filters,
                    time_partition,
                )
                .data
            }
        }
    }

    pub fn first_table_name(&self) -> Option<String> {
        let mut visitor = TableScanVisitor::default();
        let _ = self.raw_logical_plan.visit(&mut visitor);
        visitor.into_inner().pop()
    }
}

#[derive(Debug, Default)]
pub(crate) struct TableScanVisitor {
    tables: Vec<String>,
}

impl TableScanVisitor {
    pub fn into_inner(self) -> Vec<String> {
        self.tables
    }
    pub fn top(&self) -> Option<&str> {
        self.tables.first().map(|s| s.as_ref())
    }
}

impl TreeNodeVisitor<'_> for TableScanVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        match node {
            LogicalPlan::TableScan(table) => {
                self.tables.push(table.table_name.table().to_string());
                Ok(TreeNodeRecursion::Jump)
            }
            _ => Ok(TreeNodeRecursion::Continue),
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
    time_partition: &Option<String>,
) -> Transformed<LogicalPlan> {
    plan.transform(&|plan| match plan {
        LogicalPlan::TableScan(table) => {
            let mut new_filters = vec![];
            if !table_contains_any_time_filters(&table, time_partition) {
                let mut _start_time_filter: Expr;
                let mut _end_time_filter: Expr;
                match time_partition {
                    Some(time_partition) => {
                        _start_time_filter =
                            PartialTimeFilter::Low(std::ops::Bound::Included(start_time))
                                .binary_expr(Expr::Column(Column::new(
                                    Some(table.table_name.to_owned()),
                                    time_partition.clone(),
                                )));
                        _end_time_filter =
                            PartialTimeFilter::High(std::ops::Bound::Excluded(end_time))
                                .binary_expr(Expr::Column(Column::new(
                                    Some(table.table_name.to_owned()),
                                    time_partition,
                                )));
                    }
                    None => {
                        _start_time_filter =
                            PartialTimeFilter::Low(std::ops::Bound::Included(start_time))
                                .binary_expr(Expr::Column(Column::new(
                                    Some(table.table_name.to_owned()),
                                    event::DEFAULT_TIMESTAMP_KEY,
                                )));
                        _end_time_filter =
                            PartialTimeFilter::High(std::ops::Bound::Excluded(end_time))
                                .binary_expr(Expr::Column(Column::new(
                                    Some(table.table_name.to_owned()),
                                    event::DEFAULT_TIMESTAMP_KEY,
                                )));
                    }
                }

                new_filters.push(_start_time_filter);
                new_filters.push(_end_time_filter);
            }

            if let Some(tag_filters) = filters.clone() {
                new_filters.push(tag_filters)
            }
            let new_filter = new_filters.into_iter().reduce(and);
            if let Some(new_filter) = new_filter {
                let filter =
                    Filter::try_new(new_filter, Arc::new(LogicalPlan::TableScan(table))).unwrap();
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            } else {
                Ok(Transformed::no(LogicalPlan::TableScan(table)))
            }
        }
        x => Ok(Transformed::no(x)),
    })
    .expect("transform only transforms the tablescan")
}

fn table_contains_any_time_filters(
    table: &datafusion::logical_expr::TableScan,
    time_partition: &Option<String>,
) -> bool {
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
        .any(|expr| {
            matches!(&*expr.left, Expr::Column(Column { name, .. })
            if ((time_partition.is_some() && name == time_partition.as_ref().unwrap()) ||
            (!time_partition.is_some() && name == event::DEFAULT_TIMESTAMP_KEY)))
        })
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
    let path_str = path
        .file_name()
        .expect("all given paths are files")
        .to_str()
        .expect("filename is valid");

    const DATETIME_FORMAT: &str = "date=%Y-%m-%d.hour=%H.minute=%M";

    // Parse the date-time using the format
    NaiveDateTime::parse_and_remainder(path_str, DATETIME_FORMAT)
        .expect("valid date-time")
        .0
        .and_utc()
}

/// unused for now might need it later
#[allow(unused)]
pub fn flatten_objects_for_count(objects: Vec<Value>) -> Vec<Value> {
    if objects.is_empty() {
        return objects;
    }

    let first_object_first_key = objects
        .first()
        .and_then(Value::as_object)
        .and_then(|obj| obj.keys().next())
        .unwrap();

    // check if all the keys start with "COUNT"
    let flag = objects.iter().all(|obj| {
        obj.as_object()
            .map(|obj| {
                obj.keys()
                    .all(|key| key.starts_with("COUNT") && key == first_object_first_key)
            })
            .unwrap_or_default()
    });

    if flag {
        let accum = objects
            .iter()
            .filter_map(Value::as_object)
            .flat_map(|obj| obj.values())
            .filter_map(Value::as_u64)
            .sum::<u64>();

        vec![json!({
            first_object_first_key: accum
        })]
    } else {
        objects
    }
}

pub mod error {
    use crate::{metadata::error::stream_info::MetadataError, storage::ObjectStorageError};
    use datafusion::error::DataFusionError;

    #[derive(Debug, thiserror::Error)]
    pub enum ExecuteError {
        #[error("Query Execution failed due to error in object storage: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
        #[error("Query Execution failed due to error in datafusion: {0}")]
        Datafusion(#[from] DataFusionError),
        #[error("Query Execution failed due to error in fetching metadata: {0}")]
        Metadata(#[from] MetadataError),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::query::flatten_objects_for_count;

    use super::time_from_path;
    use std::path::PathBuf;

    #[test]
    fn test_time_from_parquet_path() {
        let path = PathBuf::from("date=2022-01-01.hour=00.minute=00.hostname.data.parquet");
        let time = time_from_path(path.as_path());
        assert_eq!(time.timestamp(), 1640995200);
    }

    #[test]
    fn test_flat_simple() {
        let val = vec![
            json!({
                "COUNT(*)": 1
            }),
            json!({
                "COUNT(*)": 2
            }),
            json!({
                "COUNT(*)": 3
            }),
        ];

        let out = flatten_objects_for_count(val);
        assert_eq!(out, vec![json!({"COUNT(*)": 6})]);
    }

    #[test]
    fn test_flat_empty() {
        let val = vec![];
        let out = flatten_objects_for_count(val.clone());
        assert_eq!(val, out);
    }

    #[test]
    fn test_flat_same_multi() {
        let val = vec![json!({"COUNT(ALPHA)": 1}), json!({"COUNT(ALPHA)": 2})];
        let out = flatten_objects_for_count(val.clone());
        assert_eq!(vec![json!({"COUNT(ALPHA)": 3})], out);
    }

    #[test]
    fn test_flat_diff_multi() {
        let val = vec![json!({"COUNT(ALPHA)": 1}), json!({"COUNT(BETA)": 2})];
        let out = flatten_objects_for_count(val.clone());
        assert_eq!(out, val);
    }

    #[test]
    fn test_flat_fail() {
        let val = vec![
            json!({
                "Num": 1
            }),
            json!({
                "Num": 2
            }),
            json!({
                "Num": 3
            }),
        ];

        let out = flatten_objects_for_count(val.clone());
        assert_eq!(val, out);
    }

    #[test]
    fn test_flat_multi_key() {
        let val = vec![
            json!({
                "Num": 1,
                "COUNT(*)": 1
            }),
            json!({
                "Num": 2,
                "COUNT(*)": 2
            }),
            json!({
                "Num": 3,
                "COUNT(*)": 3
            }),
        ];

        let out = flatten_objects_for_count(val.clone());
        assert_eq!(val, out);
    }
}
