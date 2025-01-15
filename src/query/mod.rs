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

use chrono::{DateTime, Duration, Utc};
use chrono::{NaiveDateTime, TimeZone};
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    Aggregate, Explain, Filter, LogicalPlan, PlanType, Projection, ToStringifiedPlan,
};
use datafusion::prelude::*;
use itertools::Itertools;
use once_cell::sync::Lazy;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use stream_schema_provider::collect_manifest_files;
use sysinfo::System;

use self::error::ExecuteError;
use self::stream_schema_provider::GlobalSchemaProvider;
pub use self::stream_schema_provider::PartialTimeFilter;
use crate::catalog::manifest::Manifest;
use crate::catalog::snapshot::Snapshot;
use crate::catalog::Snapshot as CatalogSnapshot;
use crate::event;
use crate::handlers::http::query::QueryError;
use crate::metadata::STREAM_INFO;
use crate::option::{Mode, CONFIG};
use crate::storage::{ObjectStorageProvider, ObjectStoreFormat, StorageDir, STREAM_ROOT_DIRECTORY};
use crate::utils::time::TimeRange;
pub static QUERY_SESSION: Lazy<SessionContext> =
    Lazy::new(|| Query::create_session_context(CONFIG.storage()));

// A query request by client
#[derive(Debug)]
pub struct Query {
    pub raw_logical_plan: LogicalPlan,
    pub time_range: TimeRange,
    pub filter_tag: Option<Vec<String>>,
}

impl Query {
    // create session context for this query
    pub fn create_session_context(storage: Arc<dyn ObjectStorageProvider>) -> SessionContext {
        let runtime_config = storage
            .get_datafusion_runtime()
            .with_disk_manager(DiskManagerConfig::NewOs);

        let (pool_size, fraction) = match CONFIG.options.query_memory_pool_size {
            Some(size) => (size, 1.),
            None => {
                let mut system = System::new();
                system.refresh_memory();
                let available_mem = system.available_memory();
                (available_mem as usize, 0.85)
            }
        };

        let runtime_config = runtime_config.with_memory_limit(pool_size, fraction);
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
                    self.time_range.start.naive_utc(),
                    self.time_range.end.naive_utc(),
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

    /// Evaluates to Some("count(*)") | Some("column_name") if the logical plan is a Projection: SELECT COUNT(*) | SELECT COUNT(*) as column_name
    pub fn is_logical_plan_count_without_filters(&self) -> Option<&String> {
        // Check if the raw logical plan is a Projection: SELECT
        let LogicalPlan::Projection(Projection { input, expr, .. }) = &self.raw_logical_plan else {
            return None;
        };
        // Check if the input of the Projection is an Aggregate: COUNT(*)
        let LogicalPlan::Aggregate(Aggregate { input, .. }) = &**input else {
            return None;
        };

        // Ensure the input of the Aggregate is a TableScan and there is exactly one expression: SELECT COUNT(*)
        if !matches!(&**input, LogicalPlan::TableScan { .. }) || expr.len() == 1 {
            return None;
        }

        // Check if the expression is a column or an alias for COUNT(*)
        match &expr[0] {
            // Direct column check
            Expr::Column(Column { name, .. }) if name == "count(*)" => Some(name),
            // Alias for COUNT(*)
            Expr::Alias(Alias {
                expr: inner_expr,
                name: alias_name,
                ..
            }) => {
                if let Expr::Column(Column { name, .. }) = &**inner_expr {
                    if name == "count(*)" {
                        return Some(alias_name);
                    }
                }
                None
            }
            // Unsupported expression type
            _ => None,
        }
    }
}

/// DateBinRecord
#[derive(Debug, Serialize, Clone)]
pub struct DateBinRecord {
    pub date_bin_timestamp: String,
    pub log_count: u64,
}

/// DateBin Request.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DateBinRequest {
    pub stream: String,
    pub start_time: String,
    pub end_time: String,
    pub num_bins: u64,
}

impl DateBinRequest {
    /// This function is supposed to read maninfest files for the given stream,
    /// get the sum of `num_rows` between the `startTime` and `endTime`,
    /// divide that by number of bins and return in a manner acceptable for the console
    pub async fn get_bin_density(&self) -> Result<Vec<DateBinRecord>, QueryError> {
        let time_partition = STREAM_INFO
            .get_time_partition(&self.stream.clone())
            .map_err(|err| anyhow::Error::msg(err.to_string()))?
            .unwrap_or_else(|| event::DEFAULT_TIMESTAMP_KEY.to_owned());

        // get time range
        let time_range = TimeRange::parse_human_time(&self.start_time, &self.end_time)?;
        let all_manifest_files = get_manifest_list(&self.stream, &time_range).await?;
        // get final date bins
        let final_date_bins = self.get_bins(&time_range);

        // we have start and end times for each bin
        // we also have all the manifest files for the given time range
        // now we iterate over start and end times for each bin
        // then we iterate over the manifest files which are within that time range
        // we sum up the num_rows
        let mut date_bin_records = Vec::new();

        for bin in final_date_bins {
            let date_bin_timestamp = match &bin[0] {
                PartialTimeFilter::Low(Bound::Included(ts)) => ts.and_utc().timestamp_millis(),
                _ => unreachable!(),
            };

            // extract start and end time to compare
            let bin_start = match &bin[0] {
                PartialTimeFilter::Low(Bound::Included(ts)) => ts,
                _ => unreachable!(),
            };
            let bin_end = match &bin[1] {
                PartialTimeFilter::High(Bound::Included(ts) | Bound::Excluded(ts)) => ts,
                _ => unreachable!(),
            };

            let total_num_rows: u64 = all_manifest_files
                .iter()
                .flat_map(|m| &m.files)
                .filter(|f| {
                    f.columns.iter().any(|c| {
                        c.name == time_partition
                            && match &c.stats {
                                Some(crate::catalog::column::TypedStatistics::Int(int64_type)) => {
                                    let min = DateTime::from_timestamp_millis(int64_type.min)
                                        .unwrap()
                                        .naive_utc();
                                    bin_start <= &min && bin_end >= &min
                                }
                                _ => false,
                            }
                    })
                })
                .map(|f| f.num_rows)
                .sum();

            date_bin_records.push(DateBinRecord {
                date_bin_timestamp: DateTime::from_timestamp_millis(date_bin_timestamp)
                    .unwrap()
                    .to_rfc3339(),
                log_count: total_num_rows,
            });
        }
        Ok(date_bin_records)
    }

    /// calculate the endTime for each bin based on num bins
    fn get_bins(&self, time_range: &TimeRange) -> Vec<[PartialTimeFilter; 2]> {
        // get total minutes elapsed between start and end time
        let total_minutes = time_range
            .end
            .signed_duration_since(time_range.start)
            .num_minutes() as u64;

        // divide minutes by num bins to get minutes per bin
        let quotient = (total_minutes / self.num_bins) as i64;
        let remainder = (total_minutes % self.num_bins) as i64;
        let have_remainder = remainder > 0;

        // now create multiple bins [startTime, endTime)
        // Should we exclude the last one???
        let mut final_date_bins = vec![];

        let mut start = time_range.start;

        let loop_end = if have_remainder {
            self.num_bins
        } else {
            self.num_bins - 1
        };

        for _ in 0..loop_end {
            let bin_end = start + Duration::minutes(quotient);
            final_date_bins.push([
                PartialTimeFilter::Low(Bound::Included(start.naive_utc())),
                PartialTimeFilter::High(Bound::Excluded(bin_end.naive_utc())),
            ]);

            start = bin_end;
        }

        // construct the last bin
        // if we have remainder, then the last bin will be as long as the remainder
        // else it will be as long as the quotient
        if have_remainder {
            final_date_bins.push([
                PartialTimeFilter::Low(Bound::Included(start.naive_utc())),
                PartialTimeFilter::High(Bound::Excluded(
                    (start + Duration::minutes(remainder)).naive_utc(),
                )),
            ]);
        } else {
            final_date_bins.push([
                PartialTimeFilter::Low(Bound::Included(start.naive_utc())),
                PartialTimeFilter::High(Bound::Excluded(
                    (start + Duration::minutes(quotient)).naive_utc(),
                )),
            ]);
        }

        final_date_bins
    }
}

/// DateBin Response.
#[derive(Debug, Serialize, Clone)]
pub struct DateBinResponse {
    pub fields: Vec<String>,
    pub records: Vec<DateBinRecord>,
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

pub async fn get_manifest_list(
    stream_name: &str,
    time_range: &TimeRange,
) -> Result<Vec<Manifest>, QueryError> {
    let glob_storage = CONFIG.storage().get_object_store();

    let object_store = QUERY_SESSION
        .state()
        .runtime_env()
        .object_store_registry
        .get_store(&glob_storage.store_url())
        .unwrap();

    // get object store
    let object_store_format = glob_storage
        .get_object_store_format(stream_name)
        .await
        .map_err(|err| DataFusionError::Plan(err.to_string()))?;

    // all the manifests will go here
    let mut merged_snapshot: Snapshot = Snapshot::default();

    // get a list of manifests
    if CONFIG.options.mode == Mode::Query {
        let path = RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY]);
        let obs = glob_storage
            .get_objects(
                Some(&path),
                Box::new(|file_name| file_name.ends_with("stream.json")),
            )
            .await;
        if let Ok(obs) = obs {
            for ob in obs {
                if let Ok(object_store_format) = serde_json::from_slice::<ObjectStoreFormat>(&ob) {
                    let snapshot = object_store_format.snapshot;
                    for manifest in snapshot.manifest_list {
                        merged_snapshot.manifest_list.push(manifest);
                    }
                }
            }
        }
    } else {
        merged_snapshot = object_store_format.snapshot;
    }

    // Download all the manifest files
    let time_filter = [
        PartialTimeFilter::Low(Bound::Included(time_range.start.naive_utc())),
        PartialTimeFilter::High(Bound::Included(time_range.end.naive_utc())),
    ];

    let all_manifest_files = collect_manifest_files(
        object_store,
        merged_snapshot
            .manifests(&time_filter)
            .into_iter()
            .sorted_by_key(|file| file.time_lower_bound)
            .map(|item| item.manifest_path)
            .collect(),
    )
    .await
    .map_err(|err| anyhow::Error::msg(err.to_string()))?;

    Ok(all_manifest_files)
}

fn transform(
    plan: LogicalPlan,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
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
