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

use crate::catalog::manifest::File;
use crate::hottier::HotTierManager;
use crate::Mode;
use crate::{
    catalog::snapshot::{self, Snapshot},
    storage::{ObjectStoreFormat, STREAM_ROOT_DIRECTORY},
};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef, SortOptions};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::logical_expr::utils::conjunction;
use datafusion::{
    catalog::SchemaProvider,
    common::{
        tree_node::{TreeNode, TreeNodeRecursion},
        ToDFSchema,
    },
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        physical_plan::FileScanConfig,
        MemTable, TableProvider,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, TableType},
    physical_expr::{create_physical_expr, PhysicalSortExpr},
    physical_plan::{self, empty::EmptyExec, union::UnionExec, ExecutionPlan, Statistics},
    prelude::Expr,
    scalar::ScalarValue,
};

use futures_util::{stream::FuturesOrdered, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use object_store::{path::Path, ObjectStore};
use relative_path::RelativePathBuf;
use std::{any::Any, collections::HashMap, ops::Bound, sync::Arc};
use url::Url;

use crate::{
    catalog::{
        self, column::TypedStatistics, manifest::Manifest, snapshot::ManifestItem, ManifestFile,
    },
    event::{self, DEFAULT_TIMESTAMP_KEY},
    localcache::LocalCacheManager,
    metadata::STREAM_INFO,
    metrics::QUERY_CACHE_HIT,
    option::CONFIG,
    storage::ObjectStorage,
};

use super::listing_table_builder::ListingTableBuilder;
use crate::catalog::Snapshot as CatalogSnapshot;

// schema provider for stream based on global data
pub struct GlobalSchemaProvider {
    pub storage: Arc<dyn ObjectStorage + Send>,
}

#[async_trait::async_trait]
impl SchemaProvider for GlobalSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        STREAM_INFO.list_streams()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name) {
            Ok(Some(Arc::new(StandardTableProvider {
                schema: STREAM_INFO.schema(name).unwrap(),
                stream: name.to_owned(),
                url: self.storage.store_url(),
            })))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        STREAM_INFO.stream_exists(name)
    }
}

#[derive(Debug)]
struct StandardTableProvider {
    schema: SchemaRef,
    // prefix under which to find snapshot
    stream: String,
    // url to find right instance of object store
    url: Url,
}

#[allow(clippy::too_many_arguments)]
async fn create_parquet_physical_plan(
    object_store_url: ObjectStoreUrl,
    partitions: Vec<Vec<PartitionedFile>>,
    statistics: Statistics,
    schema: Arc<Schema>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    state: &dyn Session,
    time_partition: Option<String>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let filters = if let Some(expr) = conjunction(filters.to_vec()) {
        let table_df_schema = schema.as_ref().clone().to_dfschema()?;
        let filters = create_physical_expr(&expr, &table_df_schema, state.execution_props())?;
        Some(filters)
    } else {
        None
    };

    let sort_expr = PhysicalSortExpr {
        expr: if let Some(time_partition) = time_partition {
            physical_plan::expressions::col(&time_partition, &schema)?
        } else {
            physical_plan::expressions::col(DEFAULT_TIMESTAMP_KEY, &schema)?
        },
        options: SortOptions {
            descending: true,
            nulls_first: true,
        },
    };
    let file_format = ParquetFormat::default().with_enable_pruning(true);

    // create the execution plan
    let plan = file_format
        .create_physical_plan(
            state.as_any().downcast_ref::<SessionState>().unwrap(), // Remove this when ParquetFormat catches up
            FileScanConfig {
                object_store_url,
                file_schema: schema.clone(),
                file_groups: partitions,
                statistics,
                projection: projection.cloned(),
                limit,
                output_ordering: vec![vec![sort_expr]],
                table_partition_cols: Vec::new(),
            },
            filters.as_ref(),
        )
        .await?;
    Ok(plan)
}

async fn collect_from_snapshot(
    snapshot: &catalog::snapshot::Snapshot,
    time_filters: &[PartialTimeFilter],
    object_store: Arc<dyn ObjectStore>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Vec<catalog::manifest::File>, DataFusionError> {
    let items = snapshot.manifests(time_filters);
    let manifest_files = collect_manifest_files(
        object_store,
        items
            .into_iter()
            .sorted_by_key(|file| file.time_lower_bound)
            .map(|item| item.manifest_path)
            .collect(),
    )
    .await?;

    let mut manifest_files: Vec<_> = manifest_files
        .into_iter()
        .flat_map(|file| file.files)
        .rev()
        .collect();
    for filter in filters {
        manifest_files.retain(|file| !file.can_be_pruned(filter))
    }
    if let Some(limit) = limit {
        let limit = limit as u64;
        let mut curr_limit = 0;
        let mut pos = None;

        for (index, file) in manifest_files.iter().enumerate() {
            curr_limit += file.num_rows();
            if curr_limit >= limit {
                pos = Some(index);
                break;
            }
        }

        if let Some(pos) = pos {
            manifest_files.truncate(pos + 1);
        }
    }

    Ok(manifest_files)
}

fn partitioned_files(
    manifest_files: Vec<catalog::manifest::File>,
    table_schema: &Schema,
    target_partition: usize,
) -> (Vec<Vec<PartitionedFile>>, datafusion::common::Statistics) {
    let mut partitioned_files = Vec::from_iter((0..target_partition).map(|_| Vec::new()));
    let mut column_statistics = HashMap::<String, Option<catalog::column::TypedStatistics>>::new();
    let mut count = 0;
    for (index, file) in manifest_files
        .into_iter()
        .enumerate()
        .map(|(x, y)| (x % target_partition, y))
    {
        let catalog::manifest::File {
            file_path,
            num_rows,
            columns,
            ..
        } = file;
        partitioned_files[index].push(PartitionedFile::new(file_path, file.file_size));
        columns.into_iter().for_each(|col| {
            column_statistics
                .entry(col.name)
                .and_modify(|x| {
                    if let Some((stats, col_stats)) = x.as_ref().cloned().zip(col.stats.clone()) {
                        *x = Some(stats.update(col_stats));
                    }
                })
                .or_insert_with(|| col.stats.as_ref().cloned());
        });
        count += num_rows;
    }
    let statistics = table_schema
        .fields()
        .iter()
        .map(|field| {
            column_statistics
                .get(field.name())
                .and_then(|stats| stats.as_ref())
                .and_then(|stats| stats.clone().min_max_as_scalar(field.data_type()))
                .map(|(min, max)| datafusion::common::ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Exact(max),
                    min_value: Precision::Exact(min),
                    distinct_count: Precision::Absent,
                })
                .unwrap_or_default()
        })
        .collect();

    let statistics = datafusion::common::Statistics {
        num_rows: Precision::Exact(count as usize),
        total_byte_size: Precision::Absent,
        column_statistics: statistics,
    };

    (partitioned_files, statistics)
}

#[async_trait::async_trait]
impl TableProvider for StandardTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut memory_exec = None;
        let mut cache_exec = None;
        let mut hot_tier_exec = None;
        let mut listing_exec = None;
        let object_store = state
            .runtime_env()
            .object_store_registry
            .get_store(&self.url)
            .unwrap();
        let glob_storage = CONFIG.storage().get_object_store();

        let object_store_format = glob_storage
            .get_object_store_format(&self.stream)
            .await
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
        let time_partition = object_store_format.time_partition;
        let mut time_filters = extract_primary_filter(filters, time_partition.clone());
        if time_filters.is_empty() {
            return Err(DataFusionError::Plan("potentially unbounded query on time range. Table scanning requires atleast one time bound".to_string()));
        }

        if include_now(filters, time_partition.clone()) {
            if let Some(records) =
                event::STREAM_WRITERS.recordbatches_cloned(&self.stream, &self.schema)
            {
                let reversed_mem_table = reversed_mem_table(records, self.schema.clone())?;
                memory_exec = Some(
                    reversed_mem_table
                        .scan(state, projection, filters, limit)
                        .await?,
                );
            }
        };
        let mut merged_snapshot: snapshot::Snapshot = Snapshot::default();
        if CONFIG.parseable.mode == Mode::Query {
            let path = RelativePathBuf::from_iter([&self.stream, STREAM_ROOT_DIRECTORY]);
            let obs = glob_storage
                .get_objects(
                    Some(&path),
                    Box::new(|file_name| file_name.ends_with("stream.json")),
                )
                .await;
            if let Ok(obs) = obs {
                for ob in obs {
                    if let Ok(object_store_format) =
                        serde_json::from_slice::<ObjectStoreFormat>(&ob)
                    {
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

        // Is query timerange is overlapping with older data.
        // if true, then get listing table time filters and execution plan separately
        if is_overlapping_query(&merged_snapshot.manifest_list, &time_filters) {
            let listing_time_fiters =
                return_listing_time_filters(&merged_snapshot.manifest_list, &mut time_filters);

            listing_exec = if let Some(listing_time_filter) = listing_time_fiters {
                legacy_listing_table(
                    self.stream.clone(),
                    glob_storage.clone(),
                    object_store.clone(),
                    &listing_time_filter,
                    self.schema.clone(),
                    state,
                    projection,
                    filters,
                    limit,
                    time_partition.clone(),
                )
                .await?
            } else {
                None
            };
        }

        let mut manifest_files = collect_from_snapshot(
            &merged_snapshot,
            &time_filters,
            object_store,
            filters,
            limit,
        )
        .await?;

        if manifest_files.is_empty() {
            return final_plan(
                vec![listing_exec, memory_exec],
                projection,
                self.schema.clone(),
            );
        }

        // Based on entries in the manifest files, find them in the cache and create a physical plan.
        if let Some(cache_manager) = LocalCacheManager::global() {
            cache_exec = get_cache_exectuion_plan(
                cache_manager,
                &self.stream,
                &mut manifest_files,
                self.schema.clone(),
                projection,
                filters,
                limit,
                state,
                time_partition.clone(),
            )
            .await?;
        }

        // Hot tier data fetch
        if let Some(hot_tier_manager) = HotTierManager::global() {
            if hot_tier_manager.check_stream_hot_tier_exists(&self.stream) {
                hot_tier_exec = get_hottier_exectuion_plan(
                    hot_tier_manager,
                    &self.stream,
                    &mut manifest_files,
                    self.schema.clone(),
                    projection,
                    filters,
                    limit,
                    state,
                    time_partition.clone(),
                )
                .await?;
            }
        }
        if manifest_files.is_empty() {
            QUERY_CACHE_HIT.with_label_values(&[&self.stream]).inc();
            return final_plan(
                vec![listing_exec, memory_exec, cache_exec, hot_tier_exec],
                projection,
                self.schema.clone(),
            );
        }

        let (partitioned_files, statistics) = partitioned_files(manifest_files, &self.schema, 1);
        let remote_exec = create_parquet_physical_plan(
            ObjectStoreUrl::parse(glob_storage.store_url()).unwrap(),
            partitioned_files,
            statistics,
            self.schema.clone(),
            projection,
            filters,
            limit,
            state,
            time_partition.clone(),
        )
        .await?;

        Ok(final_plan(
            vec![
                listing_exec,
                memory_exec,
                cache_exec,
                hot_tier_exec,
                Some(remote_exec),
            ],
            projection,
            self.schema.clone(),
        )?)
    }

    /*
    Updated the function signature (and name)
    Now it handles multiple filters
    */
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let res_vec = filters
            .iter()
            .map(|filter| {
                if expr_in_boundary(filter) {
                    // if filter can be handled by time partiton pruning, it is exact
                    TableProviderFilterPushDown::Exact
                } else {
                    // otherwise, we still might be able to handle the filter with file
                    // level mechanisms such as Parquet row group pruning.
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect_vec();
        Ok(res_vec)
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_cache_exectuion_plan(
    cache_manager: &LocalCacheManager,
    stream: &str,
    manifest_files: &mut Vec<File>,
    schema: Arc<Schema>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    state: &dyn Session,
    time_partition: Option<String>,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let (cached, remainder) = cache_manager
        .partition_on_cached(stream, manifest_files.clone(), |file: &File| {
            &file.file_path
        })
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    // Assign remaining entries back to manifest list
    // This is to be used for remote query
    *manifest_files = remainder;

    let cached = cached
        .into_iter()
        .map(|(mut file, cache_path)| {
            let cache_path = object_store::path::Path::from_absolute_path(cache_path).unwrap();
            file.file_path = cache_path.to_string();
            file
        })
        .collect();

    let (partitioned_files, statistics) = partitioned_files(cached, &schema, 1);
    let plan = create_parquet_physical_plan(
        ObjectStoreUrl::parse("file:///").unwrap(),
        partitioned_files,
        statistics,
        schema.clone(),
        projection,
        filters,
        limit,
        state,
        time_partition.clone(),
    )
    .await?;

    Ok(Some(plan))
}

#[allow(clippy::too_many_arguments)]
async fn get_hottier_exectuion_plan(
    hot_tier_manager: &HotTierManager,
    stream: &str,
    manifest_files: &mut Vec<File>,
    schema: Arc<Schema>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    state: &dyn Session,
    time_partition: Option<String>,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let (hot_tier_files, remainder) = hot_tier_manager
        .get_hot_tier_manifest_files(stream, manifest_files.clone())
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    // Assign remaining entries back to manifest list
    // This is to be used for remote query
    *manifest_files = remainder;

    let hot_tier_files = hot_tier_files
        .into_iter()
        .map(|mut file| {
            let path = CONFIG
                .parseable
                .hot_tier_storage_path
                .as_ref()
                .unwrap()
                .join(&file.file_path);
            file.file_path = path.to_str().unwrap().to_string();
            file
        })
        .collect();

    let (partitioned_files, statistics) = partitioned_files(hot_tier_files, &schema, 1);
    let plan = create_parquet_physical_plan(
        ObjectStoreUrl::parse("file:///").unwrap(),
        partitioned_files,
        statistics,
        schema.clone(),
        projection,
        filters,
        limit,
        state,
        time_partition.clone(),
    )
    .await?;

    Ok(Some(plan))
}

#[allow(clippy::too_many_arguments)]
async fn legacy_listing_table(
    stream: String,
    glob_storage: Arc<dyn ObjectStorage + Send>,
    object_store: Arc<dyn ObjectStore>,
    time_filters: &[PartialTimeFilter],
    schema: Arc<Schema>,
    state: &dyn Session,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    time_partition: Option<String>,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let remote_table = ListingTableBuilder::new(stream)
        .populate_via_listing(glob_storage.clone(), object_store, time_filters)
        .and_then(|builder| async {
            let table = builder.build(
                schema.clone(),
                |x| glob_storage.query_prefixes(x),
                time_partition,
            )?;
            let res = match table {
                Some(table) => Some(table.scan(state, projection, filters, limit).await?),
                _ => None,
            };
            Ok(res)
        })
        .await?;

    Ok(remote_table)
}

fn final_plan(
    execution_plans: Vec<Option<Arc<dyn ExecutionPlan>>>,
    projection: Option<&Vec<usize>>,
    schema: Arc<Schema>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let mut execution_plans = execution_plans.into_iter().flatten().collect_vec();

    let exec: Arc<dyn ExecutionPlan> = if execution_plans.is_empty() {
        let schema = match projection {
            Some(projection) => Arc::new(schema.project(projection)?),
            None => schema,
        };
        Arc::new(EmptyExec::new(schema))
    } else if execution_plans.len() == 1 {
        execution_plans.pop().unwrap()
    } else {
        Arc::new(UnionExec::new(execution_plans))
    };
    Ok(exec)
}

fn reversed_mem_table(
    mut records: Vec<RecordBatch>,
    schema: Arc<Schema>,
) -> Result<MemTable, DataFusionError> {
    records[..].reverse();
    records
        .iter_mut()
        .for_each(|batch| *batch = crate::utils::arrow::reverse_reader::reverse(batch));
    MemTable::try_new(schema, vec![records])
}

#[derive(Debug, Clone)]
pub enum PartialTimeFilter {
    Low(Bound<NaiveDateTime>),
    High(Bound<NaiveDateTime>),
    Eq(NaiveDateTime),
}

impl PartialTimeFilter {
    fn try_from_expr(expr: &Expr, time_partition: Option<String>) -> Option<Self> {
        let Expr::BinaryExpr(binexpr) = expr else {
            return None;
        };
        let (op, time) = extract_timestamp_bound(binexpr.clone(), time_partition)?;
        let value = match op {
            Operator::Gt => PartialTimeFilter::Low(Bound::Excluded(time)),
            Operator::GtEq => PartialTimeFilter::Low(Bound::Included(time)),
            Operator::Lt => PartialTimeFilter::High(Bound::Excluded(time)),
            Operator::LtEq => PartialTimeFilter::High(Bound::Included(time)),
            Operator::Eq => PartialTimeFilter::Eq(time),
            Operator::IsNotDistinctFrom => PartialTimeFilter::Eq(time),
            _ => return None,
        };
        Some(value)
    }

    pub fn binary_expr(&self, left: Expr) -> Expr {
        let (op, right) = match self {
            PartialTimeFilter::Low(Bound::Excluded(time)) => {
                (Operator::Gt, time.and_utc().timestamp_millis())
            }
            PartialTimeFilter::Low(Bound::Included(time)) => {
                (Operator::GtEq, time.and_utc().timestamp_millis())
            }
            PartialTimeFilter::High(Bound::Excluded(time)) => {
                (Operator::Lt, time.and_utc().timestamp_millis())
            }
            PartialTimeFilter::High(Bound::Included(time)) => {
                (Operator::LtEq, time.and_utc().timestamp_millis())
            }
            PartialTimeFilter::Eq(time) => (Operator::Eq, time.and_utc().timestamp_millis()),
            _ => unimplemented!(),
        };

        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            op,
            Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(right),
                None,
            ))),
        ))
    }
}

fn is_overlapping_query(
    manifest_list: &[ManifestItem],
    time_filters: &[PartialTimeFilter],
) -> bool {
    // This is for backwards compatiblity. Older table format relies on listing.
    // if the start time is lower than lower bound of first file then we consider it overlapping
    let Some(first_entry_lower_bound) =
        manifest_list.iter().map(|file| file.time_lower_bound).min()
    else {
        return true;
    };

    for filter in time_filters {
        match filter {
            PartialTimeFilter::Low(Bound::Excluded(time))
            | PartialTimeFilter::Low(Bound::Included(time)) => {
                if time < &first_entry_lower_bound.naive_utc() {
                    return true;
                }
            }
            _ => {}
        }
    }

    false
}

/// This function will accept time filters provided to the query and will split them
/// into listing time filters and manifest time filters
/// This makes parseable backwards compatible for when it did not have manifests
/// Logic-
/// The control flow will only come to this function if there exists data without manifest files
/// Two new time filter vec![] are created
/// For listing table time filters, we will use OG time filter low bound and either OG time filter upper bound
/// or manifest lower bound
/// For manifest time filter, we will manifest lower bound and OG upper bound
fn return_listing_time_filters(
    manifest_list: &[ManifestItem],
    time_filters: &mut Vec<PartialTimeFilter>,
) -> Option<Vec<PartialTimeFilter>> {
    // vec to hold timestamps for listing
    let mut vec_listing_timestamps = Vec::new();

    let mut first_entry_lower_bound = manifest_list
        .iter()
        .map(|file| file.time_lower_bound.naive_utc())
        .min()?;

    let mut new_time_filters = vec![PartialTimeFilter::Low(Bound::Included(
        first_entry_lower_bound,
    ))];

    time_filters.iter_mut().for_each(|filter| {
        match filter {
            // since we've already determined that there is a need to list tables,
            // we just need to check whether the filter's upper bound is < manifest lower bound
            PartialTimeFilter::High(Bound::Included(upper))
            | PartialTimeFilter::High(Bound::Excluded(upper)) => {
                if upper.lt(&&mut first_entry_lower_bound) {
                    // filter upper bound is less than manifest lower bound, continue using filter upper bound
                    vec_listing_timestamps.push(filter.clone());
                } else {
                    // use manifest lower bound as excluded
                    vec_listing_timestamps.push(PartialTimeFilter::High(Bound::Excluded(
                        first_entry_lower_bound,
                    )));
                }
                new_time_filters.push(filter.clone());
            }
            _ => {
                vec_listing_timestamps.push(filter.clone());
            }
        }
    });

    // update time_filters
    *time_filters = new_time_filters;

    if vec_listing_timestamps.len().gt(&0) {
        Some(vec_listing_timestamps)
    } else {
        None
    }
}

pub fn include_now(filters: &[Expr], time_partition: Option<String>) -> bool {
    let current_minute = Utc::now()
        .with_second(0)
        .and_then(|x| x.with_nanosecond(0))
        .expect("zeroed value is valid")
        .naive_utc();

    let time_filters = extract_primary_filter(filters, time_partition);

    let upper_bound_matches = time_filters.iter().any(|filter| match filter {
        PartialTimeFilter::High(Bound::Excluded(time))
        | PartialTimeFilter::High(Bound::Included(time))
        | PartialTimeFilter::Eq(time) => time > &current_minute,
        _ => false,
    });

    if upper_bound_matches {
        return true;
    }

    // does it even have a higher bound
    let has_upper_bound = time_filters
        .iter()
        .any(|filter| matches!(filter, PartialTimeFilter::High(_)));

    !has_upper_bound
}

fn expr_in_boundary(filter: &Expr) -> bool {
    let Expr::BinaryExpr(binexpr) = filter else {
        return false;
    };
    let Some((op, time)) = extract_timestamp_bound(binexpr.clone(), None) else {
        return false;
    };

    // this is due to knowlege of prefixes being minute long always.
    // Without a consistent partition spec this cannot be guarenteed.
    time.second() == 0
        && time.nanosecond() == 0
        && matches!(
            op,
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
        )
}

fn extract_from_lit(expr: BinaryExpr, time_partition: Option<String>) -> Option<NaiveDateTime> {
    let mut column_name: String = String::default();
    if let Expr::Column(column) = *expr.left {
        column_name = column.name;
    }
    if let Expr::Literal(value) = *expr.right {
        match value {
            ScalarValue::TimestampMillisecond(Some(value), _) => {
                Some(DateTime::from_timestamp_millis(value).unwrap().naive_utc())
            }
            ScalarValue::TimestampNanosecond(Some(value), _) => {
                Some(DateTime::from_timestamp_nanos(value).naive_utc())
            }
            ScalarValue::Utf8(Some(str_value)) => {
                if time_partition.is_some() && column_name == time_partition.unwrap() {
                    Some(str_value.parse::<NaiveDateTime>().unwrap())
                } else {
                    None
                }
            }
            _ => None,
        }
    } else {
        None
    }
}

/* `BinaryExp` doesn't implement `Copy` */
fn extract_timestamp_bound(
    binexpr: BinaryExpr,
    time_partition: Option<String>,
) -> Option<(Operator, NaiveDateTime)> {
    Some((binexpr.op, extract_from_lit(binexpr, time_partition)?))
}

async fn collect_manifest_files(
    storage: Arc<dyn ObjectStore>,
    manifest_urls: Vec<String>,
) -> Result<Vec<Manifest>, object_store::Error> {
    let tasks = manifest_urls.into_iter().map(|path| {
        let path = Path::parse(path).unwrap();
        let storage = Arc::clone(&storage);
        async move { storage.get(&path).await }
    });

    let resp = FuturesOrdered::from_iter(tasks)
        .and_then(|res| res.bytes())
        .collect::<Vec<object_store::Result<Bytes>>>()
        .await;

    Ok(resp
        .into_iter()
        .flat_map(|res| res.ok())
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        .collect())
}

// extract start time and end time from filter preficate
fn extract_primary_filter(
    filters: &[Expr],
    time_partition: Option<String>,
) -> Vec<PartialTimeFilter> {
    let mut time_filters = Vec::new();
    filters.iter().for_each(|expr| {
        let _ = expr.apply(&mut |expr| {
            let time = PartialTimeFilter::try_from_expr(expr, time_partition.clone());
            if let Some(time) = time {
                time_filters.push(time);
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Jump)
            }
        });
    });
    time_filters
}

trait ManifestExt: ManifestFile {
    fn find_matching_column(&self, partial_filter: &Expr) -> Option<&catalog::column::Column> {
        let name = match partial_filter {
            Expr::BinaryExpr(binary_expr) => {
                let Expr::Column(col) = binary_expr.left.as_ref() else {
                    return None;
                };
                &col.name
            }
            _ => {
                return None;
            }
        };

        self.columns().iter().find(|col| &col.name == name)
    }

    fn can_be_pruned(&self, partial_filter: &Expr) -> bool {
        fn extract_op_scalar(expr: &Expr) -> Option<(Operator, &ScalarValue)> {
            let Expr::BinaryExpr(expr) = expr else {
                return None;
            };
            let Expr::Literal(value) = &*expr.right else {
                return None;
            };
            /* `BinaryExp` doesn't implement `Copy` */
            Some((expr.op, value))
        }

        let Some(col) = self.find_matching_column(partial_filter) else {
            return false;
        };

        let Some((op, value)) = extract_op_scalar(partial_filter) else {
            return false;
        };

        let Some(value) = cast_or_none(value) else {
            return false;
        };

        let Some(stats) = &col.stats else {
            return false;
        };

        !satisfy_constraints(value, op, stats).unwrap_or(true)
    }
}

impl<T: ManifestFile> ManifestExt for T {}

enum CastRes<'a> {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(&'a str),
}

fn cast_or_none(scalar: &ScalarValue) -> Option<CastRes<'_>> {
    match scalar {
        ScalarValue::Null => None,
        ScalarValue::Boolean(val) => val.map(CastRes::Bool),
        ScalarValue::Float32(val) => val.map(|val| CastRes::Float(val as f64)),
        ScalarValue::Float64(val) => val.map(CastRes::Float),
        ScalarValue::Int8(val) => val.map(|val| CastRes::Int(val as i64)),
        ScalarValue::Int16(val) => val.map(|val| CastRes::Int(val as i64)),
        ScalarValue::Int32(val) => val.map(|val| CastRes::Int(val as i64)),
        ScalarValue::Int64(val) => val.map(CastRes::Int),
        ScalarValue::UInt8(val) => val.map(|val| CastRes::Int(val as i64)),
        ScalarValue::UInt16(val) => val.map(|val| CastRes::Int(val as i64)),
        ScalarValue::UInt32(val) => val.map(|val| CastRes::Int(val as i64)),
        ScalarValue::UInt64(val) => val.map(|val| CastRes::Int(val as i64)),
        ScalarValue::Utf8(val) => val.as_ref().map(|val| CastRes::String(val)),
        ScalarValue::TimestampMillisecond(val, _) => val.map(CastRes::Int),
        _ => None,
    }
}

fn satisfy_constraints(value: CastRes, op: Operator, stats: &TypedStatistics) -> Option<bool> {
    fn matches<T: std::cmp::PartialOrd>(value: T, min: T, max: T, op: Operator) -> Option<bool> {
        let val = match op {
            Operator::Eq | Operator::IsNotDistinctFrom => value >= min && value <= max,
            Operator::NotEq => value < min && value > max,
            Operator::Lt => value > min,
            Operator::LtEq => value >= min,
            Operator::Gt => value < max,
            Operator::GtEq => value <= max,
            _ => return None,
        };
        Some(val)
    }

    match (value, stats) {
        (CastRes::Bool(val), TypedStatistics::Bool(stats)) => {
            matches(val, stats.min, stats.max, op)
        }
        (CastRes::Int(val), TypedStatistics::Int(stats)) => matches(val, stats.min, stats.max, op),
        (CastRes::Float(val), TypedStatistics::Float(stats)) => {
            matches(val, stats.min, stats.max, op)
        }
        (CastRes::String(val), TypedStatistics::String(stats)) => {
            matches(val, &stats.min, &stats.max, op)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};

    use crate::catalog::snapshot::ManifestItem;

    use super::{is_overlapping_query, PartialTimeFilter};

    fn datetime_min(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_time(NaiveTime::MIN)
            .and_utc()
    }

    fn datetime_max(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_milli_opt(23, 59, 59, 99)
            .unwrap()
            .and_utc()
    }

    fn manifest_items() -> Vec<ManifestItem> {
        vec![
            ManifestItem {
                manifest_path: "1".to_string(),
                time_lower_bound: datetime_min(2023, 12, 15),
                time_upper_bound: datetime_max(2023, 12, 15),
                events_ingested: 0,
                ingestion_size: 0,
                storage_size: 0,
            },
            ManifestItem {
                manifest_path: "2".to_string(),
                time_lower_bound: datetime_min(2023, 12, 16),
                time_upper_bound: datetime_max(2023, 12, 16),
                events_ingested: 0,
                ingestion_size: 0,
                storage_size: 0,
            },
            ManifestItem {
                manifest_path: "3".to_string(),
                time_lower_bound: datetime_min(2023, 12, 17),
                time_upper_bound: datetime_max(2023, 12, 17),
                events_ingested: 0,
                ingestion_size: 0,
                storage_size: 0,
            },
        ]
    }

    #[test]
    fn bound_min_is_overlapping() {
        let res = is_overlapping_query(
            &manifest_items(),
            &[PartialTimeFilter::Low(std::ops::Bound::Included(
                datetime_min(2023, 12, 14).naive_utc(),
            ))],
        );

        assert!(res)
    }

    #[test]
    fn bound_min_plus_hour_is_overlapping() {
        let res = is_overlapping_query(
            &manifest_items(),
            &[PartialTimeFilter::Low(std::ops::Bound::Included(
                datetime_min(2023, 12, 14)
                    .naive_utc()
                    .add(Duration::hours(3)),
            ))],
        );

        assert!(res)
    }

    #[test]
    fn bound_next_day_min_is_not_overlapping() {
        let res = is_overlapping_query(
            &manifest_items(),
            &[PartialTimeFilter::Low(std::ops::Bound::Included(
                datetime_min(2023, 12, 16).naive_utc(),
            ))],
        );

        assert!(!res)
    }
}
