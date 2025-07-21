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

use std::{any::Any, collections::HashMap, ops::Bound, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef, SortOptions};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, TimeDelta, Timelike, Utc};
use datafusion::{
    catalog::{SchemaProvider, Session},
    common::{
        Constraints, ToDFSchema,
        stats::Precision,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    datasource::{
        MemTable, TableProvider,
        file_format::{FileFormat, parquet::ParquetFormat},
        listing::PartitionedFile,
        physical_plan::FileScanConfig,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::{
        BinaryExpr, Operator, TableProviderFilterPushDown, TableType, utils::conjunction,
    },
    physical_expr::{LexOrdering, PhysicalSortExpr, create_physical_expr, expressions::col},
    physical_plan::{ExecutionPlan, Statistics, empty::EmptyExec, union::UnionExec},
    prelude::Expr,
    scalar::ScalarValue,
};
use futures_util::{StreamExt, TryFutureExt, TryStreamExt, stream::FuturesOrdered};
use itertools::Itertools;
use object_store::{ObjectStore, path::Path};
use relative_path::RelativePathBuf;
use url::Url;

use crate::{
    catalog::{
        ManifestFile, Snapshot as CatalogSnapshot,
        column::{Column, TypedStatistics},
        manifest::{File, Manifest},
        snapshot::{ManifestItem, Snapshot},
    },
    event::DEFAULT_TIMESTAMP_KEY,
    hottier::HotTierManager,
    metrics::QUERY_CACHE_HIT,
    option::Mode,
    parseable::{PARSEABLE, STREAM_EXISTS},
    storage::{ObjectStorage, ObjectStoreFormat, STREAM_ROOT_DIRECTORY},
};

use super::listing_table_builder::ListingTableBuilder;

// schema provider for stream based on global data
#[derive(Debug)]
pub struct GlobalSchemaProvider {
    pub storage: Arc<dyn ObjectStorage>,
}

#[async_trait::async_trait]
impl SchemaProvider for GlobalSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        PARSEABLE.streams.list()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name) {
            Ok(Some(Arc::new(StandardTableProvider {
                schema: PARSEABLE
                    .get_stream(name)
                    .expect(STREAM_EXISTS)
                    .get_schema(),
                stream: name.to_owned(),
                url: self.storage.store_url(),
            })))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        PARSEABLE.streams.contains(name)
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

impl StandardTableProvider {
    #[allow(clippy::too_many_arguments)]
    async fn create_parquet_physical_plan(
        &self,
        execution_plans: &mut Vec<Arc<dyn ExecutionPlan>>,
        object_store_url: ObjectStoreUrl,
        partitions: Vec<Vec<PartitionedFile>>,
        statistics: Statistics,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        state: &dyn Session,
        time_partition: Option<String>,
    ) -> Result<(), DataFusionError> {
        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            let table_df_schema = self.schema.as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(&expr, &table_df_schema, state.execution_props())?;
            Some(filters)
        } else {
            None
        };

        let sort_expr = PhysicalSortExpr {
            expr: if let Some(time_partition) = time_partition {
                col(&time_partition, &self.schema)?
            } else {
                col(DEFAULT_TIMESTAMP_KEY, &self.schema)?
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
                    file_schema: self.schema.clone(),
                    file_groups: partitions,
                    statistics,
                    projection: projection.cloned(),
                    limit,
                    output_ordering: vec![LexOrdering::from_iter([sort_expr])],
                    table_partition_cols: Vec::new(),
                    constraints: Constraints::empty(),
                },
                filters.as_ref(),
            )
            .await?;
        execution_plans.push(plan);

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_hottier_exectuion_plan(
        &self,
        execution_plans: &mut Vec<Arc<dyn ExecutionPlan>>,
        hot_tier_manager: &HotTierManager,
        manifest_files: &mut Vec<File>,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        state: &dyn Session,
        time_partition: Option<String>,
    ) -> Result<(), DataFusionError> {
        let hot_tier_files = hot_tier_manager
            .get_hot_tier_manifest_files(&self.stream, manifest_files)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let hot_tier_files = hot_tier_files
            .into_iter()
            .map(|mut file| {
                let path = PARSEABLE
                    .options
                    .hot_tier_storage_path
                    .as_ref()
                    .unwrap()
                    .join(&file.file_path);
                file.file_path = path.to_str().unwrap().to_string();
                file
            })
            .collect();

        let (partitioned_files, statistics) = self.partitioned_files(hot_tier_files);
        self.create_parquet_physical_plan(
            execution_plans,
            ObjectStoreUrl::parse("file:///").unwrap(),
            partitioned_files,
            statistics,
            projection,
            filters,
            limit,
            state,
            time_partition.clone(),
        )
        .await?;

        Ok(())
    }

    /// Create an execution plan over the records in arrows and parquet that are still in staging, awaiting push to object storage
    async fn get_staging_execution_plan(
        &self,
        execution_plans: &mut Vec<Arc<dyn ExecutionPlan>>,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        state: &dyn Session,
        time_partition: Option<&String>,
    ) -> Result<(), DataFusionError> {
        let Ok(staging) = PARSEABLE.get_stream(&self.stream) else {
            return Ok(());
        };

        // Staging arrow exection plan
        let records = staging.recordbatches_cloned(&self.schema);
        let arrow_exec = reversed_mem_table(records, self.schema.clone())?
            .scan(state, projection, filters, limit)
            .await?;
        execution_plans.push(arrow_exec);

        // Get a list of parquet files still in staging, order by filename
        let mut parquet_files = staging.parquet_files();
        parquet_files.sort_by(|a, b| a.cmp(b).reverse());

        // NOTE: We don't partition among CPUs to ensure consistent results.
        // i.e. We were seeing in-consistent ordering when querying over parquets in staging.
        let mut partitioned_files = Vec::with_capacity(parquet_files.len());
        for file_path in parquet_files {
            let Ok(file_meta) = file_path.metadata() else {
                continue;
            };
            let file = PartitionedFile::new(file_path.display().to_string(), file_meta.len());
            partitioned_files.push(file)
        }

        // NOTE: There is the possibility of a parquet file being pushed to object store
        // and deleted from staging in the time it takes for datafusion to get to it.
        // Staging parquet execution plan
        self.create_parquet_physical_plan(
            execution_plans,
            ObjectStoreUrl::parse("file:///").unwrap(),
            vec![partitioned_files],
            Statistics::new_unknown(&self.schema),
            projection,
            filters,
            limit,
            state,
            time_partition.cloned(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn legacy_listing_table(
        &self,
        execution_plans: &mut Vec<Arc<dyn ExecutionPlan>>,
        glob_storage: Arc<dyn ObjectStorage>,
        object_store: Arc<dyn ObjectStore>,
        time_filters: &[PartialTimeFilter],
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        time_partition: Option<String>,
    ) -> Result<(), DataFusionError> {
        ListingTableBuilder::new(self.stream.to_owned())
            .populate_via_listing(glob_storage.clone(), object_store, time_filters)
            .and_then(|builder| async {
                let table = builder.build(
                    self.schema.clone(),
                    |x| glob_storage.query_prefixes(x),
                    time_partition,
                )?;
                if let Some(table) = table {
                    let plan = table.scan(state, projection, filters, limit).await?;
                    execution_plans.push(plan);
                }

                Ok(())
            })
            .await?;

        Ok(())
    }

    fn final_plan(
        &self,
        mut execution_plans: Vec<Arc<dyn ExecutionPlan>>,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec: Arc<dyn ExecutionPlan> = if execution_plans.is_empty() {
            let schema = match projection {
                Some(projection) => Arc::new(self.schema.project(projection)?),
                None => self.schema.to_owned(),
            };
            Arc::new(EmptyExec::new(schema))
        } else if execution_plans.len() == 1 {
            execution_plans.pop().unwrap()
        } else {
            Arc::new(UnionExec::new(execution_plans))
        };
        Ok(exec)
    }

    fn partitioned_files(
        &self,
        manifest_files: Vec<File>,
    ) -> (Vec<Vec<PartitionedFile>>, datafusion::common::Statistics) {
        let target_partition = num_cpus::get();
        let mut partitioned_files = Vec::from_iter((0..target_partition).map(|_| Vec::new()));
        let mut column_statistics = HashMap::<String, Option<TypedStatistics>>::new();
        let mut count = 0;
        for (index, file) in manifest_files
            .into_iter()
            .enumerate()
            .map(|(x, y)| (x % target_partition, y))
        {
            #[allow(unused_mut)]
            let File {
                mut file_path,
                num_rows,
                columns,
                ..
            } = file;

            // object_store::path::Path doesn't automatically deal with Windows path separators
            // to do that, we are using from_absolute_path() which takes into consideration the underlying filesystem
            // before sending the file path to PartitionedFile
            // the github issue- https://github.com/parseablehq/parseable/issues/824
            // For some reason, the `from_absolute_path()` doesn't work for macos, hence the ugly solution
            // TODO: figure out an elegant solution to this
            #[cfg(windows)]
            {
                if PARSEABLE.storage.name() == "drive" {
                    file_path = object_store::path::Path::from_absolute_path(file_path)
                        .unwrap()
                        .to_string();
                }
            }
            let pf = PartitionedFile::new(file_path, file.file_size);
            partitioned_files[index].push(pf);

            columns.into_iter().for_each(|col| {
                column_statistics
                    .entry(col.name)
                    .and_modify(|x| {
                        if let Some((stats, col_stats)) = x.as_ref().cloned().zip(col.stats.clone())
                        {
                            *x = Some(stats.update(col_stats));
                        }
                    })
                    .or_insert_with(|| col.stats.as_ref().cloned());
            });
            count += num_rows;
        }
        let statistics = self
            .schema
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
                        sum_value: Precision::Absent,
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
}

async fn collect_from_snapshot(
    snapshot: &Snapshot,
    time_filters: &[PartialTimeFilter],
    object_store: Arc<dyn ObjectStore>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Vec<File>, DataFusionError> {
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
        let mut execution_plans = vec![];
        let object_store = state
            .runtime_env()
            .object_store_registry
            .get_store(&self.url)
            .unwrap();
        let glob_storage = PARSEABLE.storage.get_object_store();

        let object_store_format = glob_storage
            .get_object_store_format(&self.stream)
            .await
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
        let time_partition = object_store_format.time_partition;
        let mut time_filters = extract_primary_filter(filters, &time_partition);
        if time_filters.is_empty() {
            return Err(DataFusionError::Plan("potentially unbounded query on time range. Table scanning requires atleast one time bound".to_string()));
        }

        if is_within_staging_window(&time_filters) {
            self.get_staging_execution_plan(
                &mut execution_plans,
                projection,
                filters,
                limit,
                state,
                time_partition.as_ref(),
            )
            .await?;
        };
        let mut merged_snapshot = Snapshot::default();
        if PARSEABLE.options.mode == Mode::Query || PARSEABLE.options.mode == Mode::Prism {
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

            if let Some(listing_time_filter) = listing_time_fiters {
                self.legacy_listing_table(
                    &mut execution_plans,
                    glob_storage.clone(),
                    object_store.clone(),
                    &listing_time_filter,
                    state,
                    projection,
                    filters,
                    limit,
                    time_partition.clone(),
                )
                .await?;
            }
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
            return self.final_plan(execution_plans, projection);
        }

        // Hot tier data fetch
        if let Some(hot_tier_manager) = HotTierManager::global() {
            if hot_tier_manager.check_stream_hot_tier_exists(&self.stream) {
                self.get_hottier_exectuion_plan(
                    &mut execution_plans,
                    hot_tier_manager,
                    &mut manifest_files,
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
            return self.final_plan(execution_plans, projection);
        }

        let (partitioned_files, statistics) = self.partitioned_files(manifest_files);
        self.create_parquet_physical_plan(
            &mut execution_plans,
            ObjectStoreUrl::parse(glob_storage.store_url()).unwrap(),
            partitioned_files,
            statistics,
            projection,
            filters,
            limit,
            state,
            time_partition.clone(),
        )
        .await?;

        Ok(self.final_plan(execution_plans, projection)?)
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

fn reversed_mem_table(
    mut records: Vec<RecordBatch>,
    schema: Arc<Schema>,
) -> Result<MemTable, DataFusionError> {
    records[..].reverse();
    records
        .iter_mut()
        .for_each(|batch| *batch = crate::utils::arrow::reverse(batch));
    MemTable::try_new(schema, vec![records])
}

#[derive(Debug, Clone)]
pub enum PartialTimeFilter {
    Low(Bound<NaiveDateTime>),
    High(Bound<NaiveDateTime>),
    Eq(NaiveDateTime),
}

impl PartialTimeFilter {
    fn try_from_expr(expr: &Expr, time_partition: &Option<String>) -> Option<Self> {
        let Expr::BinaryExpr(binexpr) = expr else {
            return None;
        };
        let (op, time) = extract_timestamp_bound(binexpr, time_partition)?;
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
    if manifest_list.is_empty() {
        return Some(time_filters.clone());
    }

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

/// We should consider data in staging for queries concerning a time period,
/// ending within 5 minutes from now. e.g. If current time is 5
pub fn is_within_staging_window(time_filters: &[PartialTimeFilter]) -> bool {
    let five_minutes_back = (Utc::now() - TimeDelta::minutes(5))
        .with_second(0)
        .and_then(|x| x.with_nanosecond(0))
        .expect("zeroed value is valid")
        .naive_utc();

    if time_filters.iter().any(|filter| match filter {
        PartialTimeFilter::High(Bound::Excluded(time))
        | PartialTimeFilter::High(Bound::Included(time))
        | PartialTimeFilter::Eq(time) => time >= &five_minutes_back,
        _ => false,
    }) {
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
    let Some((op, time)) = extract_timestamp_bound(binexpr, &None) else {
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

fn extract_timestamp_bound(
    binexpr: &BinaryExpr,
    time_partition: &Option<String>,
) -> Option<(Operator, NaiveDateTime)> {
    let Expr::Literal(value) = binexpr.right.as_ref() else {
        return None;
    };

    let is_time_partition = match (binexpr.left.as_ref(), time_partition) {
        (Expr::Column(column), Some(time_partition)) => &column.name == time_partition,
        _ => false,
    };

    match value {
        ScalarValue::TimestampMillisecond(Some(value), _) => Some((
            binexpr.op,
            DateTime::from_timestamp_millis(*value).unwrap().naive_utc(),
        )),
        ScalarValue::TimestampNanosecond(Some(value), _) => Some((
            binexpr.op,
            DateTime::from_timestamp_nanos(*value).naive_utc(),
        )),
        ScalarValue::Utf8(Some(str_value)) if is_time_partition => {
            Some((binexpr.op, str_value.parse::<NaiveDateTime>().unwrap()))
        }
        _ => None,
    }
}

pub async fn collect_manifest_files(
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

// Extract start time and end time from filter predicate
pub fn extract_primary_filter(
    filters: &[Expr],
    time_partition: &Option<String>,
) -> Vec<PartialTimeFilter> {
    filters
        .iter()
        .filter_map(|expr| {
            let mut time_filter = None;
            let _ = expr.apply(&mut |expr| {
                if let Some(time) = PartialTimeFilter::try_from_expr(expr, time_partition) {
                    time_filter = Some(time);
                    Ok(TreeNodeRecursion::Stop) // Stop further traversal
                } else {
                    Ok(TreeNodeRecursion::Jump) // Skip this node
                }
            });
            time_filter
        })
        .collect()
}

pub trait ManifestExt: ManifestFile {
    fn find_matching_column(&self, partial_filter: &Expr) -> Option<&Column> {
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
        ScalarValue::Date32(val) => val.map(|val| CastRes::Int(val as i64)),
        ScalarValue::TimestampMillisecond(val, _) => val.map(CastRes::Int),
        _ => None,
    }
}

fn satisfy_constraints(value: CastRes, op: Operator, stats: &TypedStatistics) -> Option<bool> {
    fn matches<T: std::cmp::PartialOrd>(value: T, min: T, max: T, op: Operator) -> Option<bool> {
        let val = match op {
            Operator::Eq | Operator::IsNotDistinctFrom => value >= min && value <= max,
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

    use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use datafusion::{
        logical_expr::{BinaryExpr, Operator},
        prelude::Expr,
        scalar::ScalarValue,
    };

    use crate::catalog::snapshot::ManifestItem;

    use super::{PartialTimeFilter, extract_timestamp_bound, is_overlapping_query};

    fn datetime_min(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_time(NaiveTime::MIN)
            .and_utc()
    }

    fn datetime_max(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_milli_opt(23, 59, 59, 999)
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

    #[test]
    fn timestamp_in_milliseconds() {
        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("timestamp_column".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(1672531200000),
                None,
            ))),
        };

        let time_partition = Some("timestamp_column".to_string());
        let result = extract_timestamp_bound(&binexpr, &time_partition);

        let expected = Some((
            Operator::Eq,
            NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ));

        assert_eq!(result, expected);
    }

    #[test]
    fn timestamp_in_nanoseconds() {
        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("timestamp_column".into())),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::TimestampNanosecond(
                Some(1672531200000000000),
                None,
            ))),
        };

        let time_partition = Some("timestamp_column".to_string());
        let result = extract_timestamp_bound(&binexpr, &time_partition);

        let expected = Some((
            Operator::Gt,
            NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ));

        assert_eq!(result, expected);
    }

    #[test]
    fn string_timestamp() {
        let timestamp = "2023-01-01T00:00:00";
        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("timestamp_column".into())),
            op: Operator::Lt,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some(timestamp.to_owned())))),
        };

        let time_partition = Some("timestamp_column".to_string());
        let result = extract_timestamp_bound(&binexpr, &time_partition);

        let expected = Some((
            Operator::Lt,
            NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%S").unwrap(),
        ));

        assert_eq!(result, expected);
    }

    #[test]
    fn unexpected_utf8_column() {
        let timestamp = "2023-01-01T00:00:00";
        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("other_column".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some(timestamp.to_owned())))),
        };

        let time_partition = Some("timestamp_column".to_string());
        let result = extract_timestamp_bound(&binexpr, &time_partition);

        assert!(result.is_none());
    }

    #[test]
    fn unsupported_literal_type() {
        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("timestamp_column".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(42)))),
        };

        let time_partition = Some("timestamp_column".to_string());
        let result = extract_timestamp_bound(&binexpr, &time_partition);

        assert!(result.is_none());
    }

    #[test]
    fn no_literal_on_right() {
        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("timestamp_column".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Column("other_column".into())),
        };

        let time_partition = Some("timestamp_column".to_string());
        let result = extract_timestamp_bound(&binexpr, &time_partition);

        assert!(result.is_none());
    }

    #[test]
    fn non_time_partition_timestamps() {
        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("timestamp_column".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(1672531200000),
                None,
            ))),
        };

        let time_partition = None;
        let result = extract_timestamp_bound(&binexpr, &time_partition);
        let expected = Some((
            Operator::Eq,
            NaiveDateTime::parse_from_str("2023-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(),
        ));

        assert_eq!(result, expected);

        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("timestamp_column".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::TimestampNanosecond(
                Some(1672531200000000000),
                None,
            ))),
        };
        let result = extract_timestamp_bound(&binexpr, &time_partition);

        assert_eq!(result, expected);
    }
}
