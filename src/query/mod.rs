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

use chrono::{DateTime, Utc};
use chrono::{NaiveDateTime, TimeZone};
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::SessionStateBuilder;
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
use crate::storage::StorageDir;
use crate::utils::time::TimeRange;

pub static QUERY_SESSION: Lazy<SessionContext> = Lazy::new(|| {
    let storage = CONFIG.storage();
    let (pool_size, fraction) = match CONFIG.parseable.query_memory_pool_size {
        Some(size) => (size, 1.),
        None => {
            let mut system = System::new();
            system.refresh_memory();
            let available_mem = system.available_memory();
            (available_mem as usize, 0.85)
        }
    };

    let runtime_config = storage
        .get_datafusion_runtime()
        .with_disk_manager(DiskManagerConfig::NewOs)
        .with_memory_limit(pool_size, fraction);
    let runtime = Arc::new(runtime_config.build().unwrap());

    let mut config = SessionConfig::default()
        .with_parquet_pruning(true)
        .with_prefer_existing_sort(true)
        .with_round_robin_repartition(true);

    // For more details refer https://datafusion.apache.org/user-guide/configs.html

    // Reduce the number of rows read (if possible)
    config.options_mut().execution.parquet.enable_page_index = true;

    // Pushdown filters allows DF to push the filters as far down in the plan as possible
    // and thus, reducing the number of rows decoded
    config.options_mut().execution.parquet.pushdown_filters = true;

    // Reorder filters allows DF to decide the order of filters minimizing the cost of filter evaluation
    config.options_mut().execution.parquet.reorder_filters = true;

    // Enable StringViewArray
    // https://www.influxdata.com/blog/faster-queries-with-stringview-part-one-influxdb/
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = true;

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(runtime)
        .build();

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
});

// A query request by client
#[derive(Debug)]
pub struct Query {
    pub raw_logical_plan: LogicalPlan,
    pub time_range: TimeRange,
    pub filter_tag: Option<Vec<String>>,
    pub stream_names: Vec<String>,
}

impl Query {
    pub async fn execute(&self) -> Result<(Vec<RecordBatch>, Vec<String>), ExecuteError> {
        let time_partitions = self.get_time_partitions()?;
        let logical_plan = self.final_logical_plan(time_partitions);
        let df = QUERY_SESSION.execute_logical_plan(logical_plan).await?;

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

    /// Get the time partitions for the streams mentioned in the query
    fn get_time_partitions(&self) -> Result<HashMap<String, String>, ExecuteError> {
        let mut time_partitions = HashMap::default();
        for stream_name in self.stream_names.iter() {
            let Some(time_partition) = STREAM_INFO.get_time_partition(stream_name)? else {
                continue;
            };
            time_partitions.insert(stream_name.clone(), time_partition);
        }

        Ok(time_partitions)
    }

    /// return logical plan with all time filters applied through
    fn final_logical_plan(&self, time_partitions: HashMap<String, String>) -> LogicalPlan {
        // see https://github.com/apache/arrow-datafusion/pull/8400
        // this can be eliminated in later version of datafusion but with slight caveat
        // transform cannot modify stringified plans by itself
        // we by knowing this plan is not in the optimization procees chose to overwrite the stringified plan

        match self.raw_logical_plan.clone() {
            LogicalPlan::Explain(plan) => {
                let transformed = transform(
                    plan.plan.as_ref().clone(),
                    self.time_range.start.naive_utc(),
                    self.time_range.end.naive_utc(),
                    &time_partitions,
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
                    self.time_range.start.naive_utc(),
                    self.time_range.end.naive_utc(),
                    &time_partitions,
                )
                .data
            }
        }
    }

    // name of the main/first stream in the query
    pub fn first_stream_name(&self) -> Option<&String> {
        self.stream_names.first()
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
    #[allow(dead_code)]
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

// transform the plan to apply time filters
fn transform(
    plan: LogicalPlan,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
    time_partitions: &HashMap<String, String>,
) -> Transformed<LogicalPlan> {
    plan.transform(|plan| {
        let LogicalPlan::TableScan(table) = &plan else {
            return Ok(Transformed::no(plan));
        };

        // Early return if filters already exist
        if query_can_be_filtered_on_stream_time_partition(&table, time_partitions) {
            return Ok(Transformed::no(plan));
        }

        let stream = table.table_name.clone();
        let time_partition = time_partitions
            .get(stream.table())
            .map(|x| x.as_str())
            .unwrap_or(event::DEFAULT_TIMESTAMP_KEY);
        let column_expr = Expr::Column(Column::new(Some(stream), time_partition));

        // Build filters
        let low_filter = PartialTimeFilter::Low(std::ops::Bound::Included(start_time))
            .binary_expr(column_expr.clone());
        let high_filter =
            PartialTimeFilter::High(std::ops::Bound::Excluded(end_time)).binary_expr(column_expr);

        Ok(Transformed::yes(LogicalPlan::Filter(
            Filter::try_new(and(low_filter, high_filter), Arc::new(plan)).unwrap(),
        )))
    })
    .expect("transform only transforms the tablescan")
}

// check if the query contains the streams's time partition as filter
fn query_can_be_filtered_on_stream_time_partition(
    table: &datafusion::logical_expr::TableScan,
    time_partitions: &HashMap<String, String>,
) -> bool {
    table
        .filters
        .iter()
        .filter_map(|x| match x {
            Expr::BinaryExpr(binexpr) => Some(binexpr),
            _ => None,
        })
        .any(|expr| match &*expr.left {
            Expr::Column(Column {
                name: column_name, ..
            }) => {
                time_partitions
                    .get(table.table_name.table())
                    .map(|x| x.as_str())
                    .unwrap_or(event::DEFAULT_TIMESTAMP_KEY)
                    == column_name
            }
            _ => false,
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

/// unused for now might need it later
#[allow(unused)]
pub fn flatten_objects_for_count(objects: Vec<Value>) -> Vec<Value> {
    if objects.is_empty() {
        return objects;
    }

    // check if all the keys start with "COUNT"
    let flag = objects.iter().all(|obj| {
        obj.as_object()
            .unwrap()
            .keys()
            .all(|key| key.starts_with("COUNT"))
    }) && objects.iter().all(|obj| {
        obj.as_object()
            .unwrap()
            .keys()
            .all(|key| key == objects[0].as_object().unwrap().keys().next().unwrap())
    });

    if flag {
        let mut accum = 0u64;
        let key = objects[0]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();

        for obj in objects {
            let count = obj.as_object().unwrap().keys().fold(0, |acc, key| {
                let value = obj.as_object().unwrap().get(key).unwrap().as_u64().unwrap();
                acc + value
            });
            accum += count;
        }

        vec![json!({
            key: accum
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
