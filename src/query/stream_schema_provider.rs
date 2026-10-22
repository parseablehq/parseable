/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::{any::Any, cmp::Reverse, collections::HashMap, ops::Bound, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef, SortOptions};
use chrono::{DateTime, NaiveDateTime, TimeDelta, Timelike, Utc};
use datafusion::{
    catalog::{SchemaProvider, Session},
    common::{
        Constraints, ToDFSchema,
        stats::Precision,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    config::ConfigOptions,
    datasource::{
        DefaultTableSource, MemTable, TableProvider,
        file_format::{FileFormat, parquet::ParquetFormat},
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource},
        provider_as_source,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{TaskContext, object_store::ObjectStoreUrl},
    logical_expr::{
        BinaryExpr, Operator, TableProviderFilterPushDown, TableSource, TableType,
        physical_planning_context::PhysicalPlanningContext, utils::conjunction,
    },
    physical_expr::{
        EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr, create_physical_expr,
        expressions::col,
    },
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, Statistics,
        empty::EmptyExec,
        filter_pushdown::{FilterDescription, FilterPushdownPhase},
        statistics::{ChildStats, StatisticsArgs},
        union::UnionExec,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use futures_util::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use rayon::prelude::*;

/// Number of manifest files fetched from the metastore in parallel while
/// planning a query. Each fetch is one object store GET followed by a decode
/// on the blocking pool, so this bounds both the in-flight transfers and the
/// manifests held decompressed in memory at once.
const MANIFEST_FETCH_CONCURRENCY: usize = 64;

use crate::{
    catalog::{
        ManifestFile, Snapshot as CatalogSnapshot,
        column::{Column, TypedStatistics},
        manifest::File,
        snapshot::{ManifestItem, Snapshot},
    },
    event::DEFAULT_TIMESTAMP_KEY,
    hottier::{GLOBAL_HOTTIER, HotTierManager},
    metrics::{
        QUERY_CACHE_HIT, increment_files_scanned_in_hottier_by_date,
        increment_files_scanned_in_query_by_date,
    },
    option::Mode,
    parseable::{DEFAULT_TENANT, PARSEABLE, STREAM_EXISTS},
    storage::{ObjectStorage, ObjectStoreFormat},
};

use super::listing_table_builder::ListingTableBuilder;

// schema provider for stream based on global data
#[derive(Debug)]
pub struct GlobalSchemaProvider {
    pub storage: Arc<dyn ObjectStorage>,
    pub tenant_id: Option<String>,
}

#[async_trait::async_trait]
impl SchemaProvider for GlobalSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        PARSEABLE.streams.list(&self.tenant_id)
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name) {
            Ok(Some(Arc::new(StandardTableProvider {
                schema: PARSEABLE
                    .get_stream(name, &self.tenant_id)
                    .expect(STREAM_EXISTS)
                    .get_schema(),
                stream: name.to_owned(),
                tenant_id: self.tenant_id.clone(),
                hot_tier_guarded: false,
            })))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        PARSEABLE.get_stream(name, &self.tenant_id).is_ok()
    }
}

#[derive(Debug)]
struct StandardTableProvider {
    schema: SchemaRef,
    // prefix under which to find snapshot
    stream: String,
    tenant_id: Option<String>,
    hot_tier_guarded: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct HotTierStreamKey {
    pub tenant_id: Option<String>,
    pub stream: String,
}

fn standard_table_provider(source: &Arc<dyn TableSource>) -> Option<&StandardTableProvider> {
    let source = (source.as_ref() as &dyn Any).downcast_ref::<DefaultTableSource>()?;
    (source.table_provider.as_ref() as &dyn Any).downcast_ref::<StandardTableProvider>()
}

pub(super) fn hot_tier_stream_key(source: &Arc<dyn TableSource>) -> Option<HotTierStreamKey> {
    let provider = standard_table_provider(source)?;
    Some(HotTierStreamKey {
        tenant_id: provider.tenant_id.clone(),
        stream: provider.stream.clone(),
    })
}

/// Creates a query-specific source that may read hot-tier files because its guard is held.
pub(super) fn guarded_hot_tier_table_source(
    source: &Arc<dyn TableSource>,
) -> Option<Arc<dyn TableSource>> {
    let provider = standard_table_provider(source)?;
    Some(provider_as_source(Arc::new(StandardTableProvider {
        schema: provider.schema.clone(),
        stream: provider.stream.clone(),
        tenant_id: provider.tenant_id.clone(),
        hot_tier_guarded: true,
    })))
}

pub fn exact_source_filters(filters: &[Expr]) -> Vec<Expr> {
    filters
        .iter()
        .filter(|filter| expr_in_boundary(filter))
        .cloned()
        .collect()
}

pub fn build_parquet_scan_components(
    schema: SchemaRef,
    filters: &[Expr],
    state: &dyn Session,
) -> Result<(ParquetFormat, ParquetSource), DataFusionError> {
    build_parquet_scan_components_with_filters(schema, exact_source_filters(filters), state)
}

/// Build Parquet scan components with every filter attached to the source.
///
/// Use this only when the caller still retains DataFusion's filter above the
/// scan (for example, filters reported as `Inexact` by a table provider). The
/// source predicate is then an additional, safe pruning/pushdown opportunity,
/// while the retained filter remains responsible for exact query semantics.
pub fn build_parquet_scan_components_with_full_filters(
    schema: SchemaRef,
    filters: &[Expr],
    state: &dyn Session,
) -> Result<(ParquetFormat, ParquetSource), DataFusionError> {
    build_parquet_scan_components_with_filters(schema, filters.to_vec(), state)
}

fn build_parquet_scan_components_with_filters(
    schema: SchemaRef,
    source_filters: Vec<Expr>,
    state: &dyn Session,
) -> Result<(ParquetFormat, ParquetSource), DataFusionError> {
    let parquet_options = state.default_table_options().parquet;
    let file_format = ParquetFormat::default().with_options(parquet_options.clone());
    let mut file_source =
        ParquetSource::new(schema.clone()).with_table_parquet_options(parquet_options);

    if let Some(expr) = conjunction(source_filters) {
        let table_df_schema = schema.as_ref().clone().to_dfschema()?;
        let predicate = create_physical_expr(
            &expr,
            &table_df_schema,
            state.execution_props(),
            &PhysicalPlanningContext::default(),
        )?;
        file_source = file_source.with_predicate(predicate);
    }

    Ok((file_format, file_source))
}

pub fn balanced_file_groups(manifest_files: Vec<File>, target_partitions: usize) -> Vec<Vec<File>> {
    let group_count = target_partitions.min(manifest_files.len());
    if group_count == 0 {
        return Vec::new();
    }

    // Longest-processing-time scheduling avoids a large file being assigned near the end and
    // leaving one scan partition as the tail. Keep each group's original file order after the
    // assignment to retain the manifests' newest-first scan preference, without claiming row order.
    let mut files = manifest_files.into_iter().enumerate().collect_vec();
    files.sort_by_key(|(original_index, file)| (Reverse(file.file_size), *original_index));

    let mut groups = Vec::from_iter((0..group_count).map(|_| Vec::new()));
    let mut group_bytes = vec![0_u64; group_count];
    for (original_index, file) in files {
        let group = group_bytes
            .iter()
            .enumerate()
            .min_by_key(|(index, bytes)| (**bytes, *index))
            .map(|(index, _)| index)
            .expect("at least one file group");
        group_bytes[group] = group_bytes[group].saturating_add(file.file_size);
        groups[group].push((original_index, file));
    }

    groups
        .into_iter()
        .map(|mut group| {
            group.sort_by_key(|(original_index, _)| *original_index);
            group.into_iter().map(|(_, file)| file).collect()
        })
        .collect()
}

/// Discard unverified footer ordering so sorts and TopK cannot trust the scan.
fn without_output_ordering(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if plan.output_ordering().is_none() {
        return plan;
    }
    let properties = PlanProperties::new(
        EquivalenceProperties::new(plan.schema()),
        plan.output_partitioning().clone(),
        plan.properties().emission_type,
        plan.properties().boundedness,
    );
    Arc::new(UnorderedExec {
        input: plan,
        properties: Arc::new(properties),
    })
}

#[derive(Debug)]
struct UnorderedExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for UnorderedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UnorderedExec")
    }
}

impl ExecutionPlan for UnorderedExec {
    fn name(&self) -> &str {
        "UnorderedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "UnorderedExec requires one child".to_owned(),
            ));
        }
        Ok(without_output_ordering(children.remove(0)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    // Only the advertised ordering changes, so statistics, repartitioning
    // and filter pushdown pass through to the wrapped scan. Input order is
    // not reported as kept, or sort pushdown would trust the child ordering.
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> DataFusionResult<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .input
            .repartitioned(target_partitions, config)?
            .map(without_output_ordering))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DataFusionResult<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }
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
        output_ordered_by_time: bool,
    ) -> Result<(), DataFusionError> {
        let (file_format, file_source) =
            build_parquet_scan_components(self.schema.clone(), filters, state)?;

        // create file groups from vec file partitions
        let file_groups = partitions.into_iter().map(FileGroup::new).collect_vec();

        // Unit scans must not initialize the global server or parse its CLI.
        #[cfg(test)]
        let batch_size: usize = state.config_options().execution.batch_size.into();
        #[cfg(not(test))]
        let batch_size = PARSEABLE.options.execution_batch_size;

        let mut conf_builder = FileScanConfigBuilder::new(object_store_url, file_source.into())
            .with_statistics(statistics)
            .with_batch_size(Some(batch_size))
            .with_constraints(Constraints::default())
            .with_file_groups(file_groups);

        if output_ordered_by_time {
            let time_column = time_partition.as_deref().unwrap_or(DEFAULT_TIMESTAMP_KEY);
            let sort_expr = PhysicalSortExpr {
                expr: col(time_column, &self.schema)?,
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            };
            conf_builder = conf_builder
                .with_preserve_order(true)
                .with_output_ordering(vec![LexOrdering::new([sort_expr]).unwrap()]);
        }

        // Set projection if provided
        if let Some(proj_indices) = projection {
            conf_builder = conf_builder.with_projection_indices(Some(proj_indices.clone()))?;
        }

        // Set limit if provided
        if let Some(lim) = limit {
            conf_builder = conf_builder.with_limit(Some(lim));
        }

        let conf = conf_builder.build();

        // create the execution plan
        let plan = file_format
            .create_physical_plan(
                state,
                // .as_any().downcast_ref::<SessionState>().unwrap(), // Remove this when ParquetFormat catches up
                conf,
            )
            .await?;

        execution_plans.push(without_output_ordering(plan));

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
            .get_hot_tier_manifest_files(manifest_files)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        increment_files_scanned_in_hottier_by_date(
            hot_tier_files.len() as u64,
            &chrono::Utc::now().date_naive().to_string(),
            self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
            &self.stream,
        );

        let hot_tier_files: Vec<File> = hot_tier_files
            .into_iter()
            .map(|mut file| {
                let disk_path = hot_tier_manager
                    .disk_path(&file.file_path)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                #[cfg(windows)]
                {
                    file.file_path = object_store::path::Path::from_absolute_path(disk_path)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?
                        .to_string();
                }
                #[cfg(not(windows))]
                {
                    file.file_path = disk_path.to_string_lossy().into_owned();
                }
                Ok(file)
            })
            .collect::<Result<_, DataFusionError>>()?;

        let (partitioned_files, statistics, output_ordered_by_time) = self.partitioned_files(
            hot_tier_files,
            state.config_options().execution.target_partitions,
            true,
            time_partition.as_deref().unwrap_or(DEFAULT_TIMESTAMP_KEY),
        );

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
            output_ordered_by_time,
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
        let Ok(staging) = PARSEABLE.get_stream(&self.stream, &self.tenant_id) else {
            return Ok(());
        };

        // Staging arrow exection plan
        let records = staging
            .recordbatches_cloned(&self.schema)
            .map_err(|e| DataFusionError::Internal(format!("Error during staging exec- {e}")))?;
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

        // // NOTE: There is the possibility of a parquet file being pushed to object store
        // // and deleted from staging in the time it takes for datafusion to get to it.
        // // Staging parquet execution plan
        // let object_store_url = if let Some(tenant_id) = self.tenant_id.as_ref() {
        //     &format!("file://{tenant_id}/")
        // } else {
        //     "file:///"
        // };
        let object_store_url = "file:///";
        self.create_parquet_physical_plan(
            execution_plans,
            ObjectStoreUrl::parse(object_store_url).unwrap(),
            vec![partitioned_files],
            Statistics::new_unknown(&self.schema),
            projection,
            filters,
            limit,
            state,
            time_partition.cloned(),
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn legacy_listing_table(
        &self,
        execution_plans: &mut Vec<Arc<dyn ExecutionPlan>>,
        glob_storage: Arc<dyn ObjectStorage>,
        time_filters: &[PartialTimeFilter],
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<(), DataFusionError> {
        ListingTableBuilder::new(self.stream.to_owned())
            .populate_via_listing(glob_storage.clone(), time_filters)
            .and_then(|builder| async {
                let table =
                    builder.build(self.schema.clone(), |x| glob_storage.query_prefixes(x))?;
                if let Some(table) = table {
                    let plan = table.scan(state, projection, filters, limit).await?;
                    execution_plans.push(without_output_ordering(plan));
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
            UnionExec::try_new(execution_plans)?
        };
        Ok(exec)
    }

    fn partitioned_files(
        &self,
        manifest_files: Vec<File>,
        target_partitions: usize,
        _is_hot_tier: bool,
        time_column: &str,
    ) -> (
        Vec<Vec<PartitionedFile>>,
        datafusion::common::Statistics,
        bool,
    ) {
        partitioned_files(
            &self.schema,
            &self.tenant_id,
            manifest_files,
            target_partitions,
            _is_hot_tier,
            time_column,
        )
    }
}

#[inline(always)]
pub fn partitioned_files(
    schema: &SchemaRef,
    tenant_id: &Option<String>,
    manifest_files: Vec<File>,
    target_partitions: usize,
    _is_hot_tier: bool,
    _time_column: &str,
) -> (
    Vec<Vec<PartitionedFile>>,
    datafusion::common::Statistics,
    bool,
) {
    let file_groups = balanced_file_groups(manifest_files, target_partitions);
    // Writer metadata does not prove intra-file order, even for a single file.
    let output_ordered_by_time = false;
    let mut partitioned_files = Vec::with_capacity(file_groups.len());
    let mut column_statistics = HashMap::<String, Option<TypedStatistics>>::new();
    let mut count = 0;
    let mut file_count = 0u64;
    for group in file_groups {
        let mut partition = Vec::with_capacity(group.len());
        for file in group {
            #[allow(unused_mut)]
            let File {
                mut file_path,
                num_rows,
                columns,
                file_size,
                ..
            } = file;

            // Track billing metrics for files scanned in query
            file_count += 1;

            // object_store::path::Path doesn't automatically deal with Windows path separators
            // to do that, we are using from_absolute_path() which takes into consideration the underlying filesystem
            // before sending the file path to PartitionedFile
            // the github issue- https://github.com/parseablehq/parseable/issues/824
            // For some reason, the `from_absolute_path()` doesn't work for macos, hence the ugly solution
            // TODO: figure out an elegant solution to this
            #[cfg(windows)]
            {
                if !_is_hot_tier && PARSEABLE.storage.name() == "drive" {
                    file_path = object_store::path::Path::from_absolute_path(file_path)
                        .unwrap()
                        .to_string();
                }
            }
            let pf = PartitionedFile::new(file_path, file_size);
            partition.push(pf);

            columns.into_iter().for_each(|col| {
                column_statistics
                    .entry(col.name)
                    .and_modify(|x| {
                        if let Some((stats, col_stats)) = x.as_ref().cloned().zip(col.stats.clone())
                        {
                            // update() returns None on type mismatch (e.g. column
                            // historically written as both Utf8 and Timestamp(ms)).
                            // Dropping to None here makes the planner skip min/max
                            // pushdown for this column instead of crashing the worker.
                            *x = stats.update(col_stats);
                        }
                    })
                    .or_insert_with(|| col.stats.as_ref().cloned());
            });
            count += num_rows;
        }
        partitioned_files.push(partition);
    }
    let statistics = schema
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
                    byte_size: Precision::Absent,
                })
                .unwrap_or_default()
        })
        .collect();

    let statistics = datafusion::common::Statistics {
        num_rows: Precision::Exact(count as usize),
        total_byte_size: Precision::Absent,
        column_statistics: statistics,
    };

    // Track billing metrics for query scan
    let current_date = chrono::Utc::now().date_naive().to_string();
    increment_files_scanned_in_query_by_date(
        file_count,
        &current_date,
        tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
    );

    (partitioned_files, statistics, output_ordered_by_time)
}

pub async fn collect_from_snapshot(
    snapshot: &Snapshot,
    time_filters: &[PartialTimeFilter],
    filters: &[Expr],
    limit: Option<usize>,
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<Vec<File>, DataFusionError> {
    // Manifest fetches are one object store GET each and are pure round trip
    // latency, so run them concurrently instead of serially. `buffered` keeps
    // the snapshot ordering, which the `rev()` below depends on.
    let manifest_files = futures_util::stream::iter(snapshot.manifests(time_filters))
        .map(|manifest_item| {
            let ManifestItem {
                time_lower_bound,
                time_upper_bound,
                manifest_path,
                ..
            } = manifest_item;
            async move {
                let manifest_opt = PARSEABLE
                    .metastore
                    .get_manifest(
                        stream_name,
                        time_lower_bound,
                        time_upper_bound,
                        Some(manifest_path),
                        tenant_id,
                    )
                    .await
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?;
                if manifest_opt.is_none() {
                    tracing::warn!(
                        "Manifest missing for stream={} [{:?} - {:?}]",
                        stream_name,
                        time_lower_bound,
                        time_upper_bound
                    );
                }
                Ok::<_, DataFusionError>(manifest_opt)
            }
        })
        .buffered(MANIFEST_FETCH_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    let manifest_files: Vec<_> = manifest_files
        .into_iter()
        .flatten()
        .flat_map(|file| file.files)
        .rev()
        .collect();

    // One parallel pass rather than a `retain` per filter. Every
    // `can_be_pruned` call linearly scans the file's column list (hundreds of
    // entries on wide streams), so a sequential pass per filter over tens of
    // thousands of files is hundreds of milliseconds of single threaded work.
    // `position` matches the sequential chain: a file is dropped by the first
    // filter that rejects it.
    let prune_reason: Vec<Option<usize>> = manifest_files
        .par_iter()
        .map(|file| filters.iter().position(|filter| file.can_be_pruned(filter)))
        .collect();

    // Parallel because of the drops, not the filtering. Pruning discards the
    // overwhelming majority of files, and every discarded `File` owns a `Vec`
    // of hundreds of `Column`s, each with its own heap allocated name and
    // stats. Tens of millions of frees on one thread is hundreds of
    // milliseconds; `into_par_iter` spreads them. Order is preserved — the
    // `rev()` above establishes the ordering the limit below depends on.
    let mut manifest_files: Vec<_> = manifest_files
        .into_par_iter()
        .zip(prune_reason)
        .filter_map(|(file, reason)| reason.is_none().then_some(file))
        .collect();

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
        let glob_storage = PARSEABLE.storage.get_object_store();

        let object_store_format: ObjectStoreFormat = serde_json::from_slice(
            &PARSEABLE
                .metastore
                .get_stream_json(&self.stream, false, &self.tenant_id, false)
                .await
                .map_err(|e| DataFusionError::Plan(e.to_string()))?,
        )
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;

        let time_partition = object_store_format.time_partition;
        let mut time_filters = extract_primary_filter(filters, &time_partition);
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
            let obs = PARSEABLE
                .metastore
                .get_all_stream_jsons(&self.stream, None, &self.tenant_id, false)
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
                    &listing_time_filter,
                    state,
                    projection,
                    filters,
                    limit,
                )
                .await?;
            }
        }

        let mut manifest_files = collect_from_snapshot(
            &merged_snapshot,
            &time_filters,
            filters,
            limit,
            &self.stream,
            &self.tenant_id,
        )
        .await?;

        if manifest_files.is_empty() {
            return self.final_plan(execution_plans, projection);
        }

        // Hot tier data fetch
        if self.hot_tier_guarded
            && let Some(hot_tier_manager) = GLOBAL_HOTTIER.get()
            && hot_tier_manager.check_stream_hot_tier_exists(&self.stream, &self.tenant_id)
        {
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
        if manifest_files.is_empty() {
            QUERY_CACHE_HIT
                .with_label_values(&[
                    &self.stream,
                    self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
                ])
                .inc();
            return self.final_plan(execution_plans, projection);
        }

        let (partitioned_files, statistics, output_ordered_by_time) = self.partitioned_files(
            manifest_files,
            state.config_options().execution.target_partitions,
            false,
            time_partition.as_deref().unwrap_or(DEFAULT_TIMESTAMP_KEY),
        );

        let object_store_url = glob_storage.store_url();

        self.create_parquet_physical_plan(
            &mut execution_plans,
            ObjectStoreUrl::parse(object_store_url).unwrap(),
            partitioned_files,
            statistics,
            projection,
            filters,
            limit,
            state,
            time_partition.clone(),
            output_ordered_by_time,
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

#[inline(always)]
pub fn reversed_mem_table(
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
    pub fn try_from_expr(expr: &Expr, time_partition: &Option<String>) -> Option<Self> {
        let Expr::BinaryExpr(binexpr) = expr else {
            return None;
        };

        let is_p_timestamp = expr
            .column_refs()
            .iter()
            .map(|c| &c.name)
            .contains(&DEFAULT_TIMESTAMP_KEY.to_string());
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
        if is_p_timestamp { Some(value) } else { None }
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
            Box::new(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(right), None),
                None,
            )),
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
            | PartialTimeFilter::Low(Bound::Included(time))
                if time < &first_entry_lower_bound.naive_utc() =>
            {
                return true;
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

pub fn expr_in_boundary(filter: &Expr) -> bool {
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

pub fn extract_timestamp_bound(
    binexpr: &BinaryExpr,
    time_partition: &Option<String>,
) -> Option<(Operator, NaiveDateTime)> {
    let Expr::Literal(value, None) = binexpr.right.as_ref() else {
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
            match str_value.parse::<NaiveDateTime>() {
                Ok(dt) => Some((binexpr.op, dt)),
                Err(_) => None,
            }
        }
        _ => None,
    }
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
            let Expr::Literal(value, None) = &*expr.right else {
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
    use std::{ops::Add, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};
    use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use datafusion::{
        datasource::{
            TableProvider,
            empty::EmptyTable,
            listing::PartitionedFile,
            physical_plan::{FileScanConfigBuilder, FileSource},
            provider_as_source,
            source::DataSourceExec,
        },
        execution::context::{SessionConfig, SessionContext},
        execution::object_store::ObjectStoreUrl,
        logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, TableType},
        physical_plan::{
            ExecutionPlan, displayable,
            statistics::{StatisticsArgs, StatisticsContext},
        },
        prelude::Expr,
        scalar::ScalarValue,
    };

    use crate::catalog::{
        column::{Column, Int64Type, TypedStatistics},
        manifest::{File, SortOrder},
        snapshot::ManifestItem,
    };

    use super::{
        HotTierStreamKey, PartialTimeFilter, StandardTableProvider, balanced_file_groups,
        build_parquet_scan_components, build_parquet_scan_components_with_full_filters,
        exact_source_filters, extract_timestamp_bound, guarded_hot_tier_table_source,
        hot_tier_stream_key, is_overlapping_query, standard_table_provider,
    };

    #[test]
    fn guarded_hot_tier_source_preserves_provider_identity() {
        let source = provider_as_source(Arc::new(StandardTableProvider {
            schema: Arc::new(Schema::empty()),
            stream: "logs".to_owned(),
            tenant_id: Some("other-tenant".to_owned()),
            hot_tier_guarded: false,
        }));

        assert_eq!(
            hot_tier_stream_key(&source),
            Some(HotTierStreamKey {
                tenant_id: Some("other-tenant".to_owned()),
                stream: "logs".to_owned(),
            })
        );

        let guarded_source = guarded_hot_tier_table_source(&source).unwrap();
        let guarded_provider = standard_table_provider(&guarded_source).unwrap();
        assert_eq!(guarded_provider.stream, "logs");
        assert_eq!(guarded_provider.tenant_id.as_deref(), Some("other-tenant"));
        assert!(guarded_provider.hot_tier_guarded);
    }

    #[test]
    fn hot_tier_source_rewrite_ignores_non_standard_providers() {
        let source = provider_as_source(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))));

        assert!(hot_tier_stream_key(&source).is_none());
        assert!(guarded_hot_tier_table_source(&source).is_none());
    }

    fn file(path: &str, size: u64) -> File {
        File {
            file_path: path.to_owned(),
            file_size: size,
            ..File::default()
        }
    }

    fn time_sorted_file(path: &str, size: u64, min: i64, max: i64) -> File {
        File {
            file_path: path.to_owned(),
            file_size: size,
            columns: vec![Column {
                name: "p_timestamp".to_owned(),
                stats: Some(TypedStatistics::Int(Int64Type { min, max })),
                null_count: Some(0),
                uncompressed_size: 0,
                compressed_size: 0,
            }],
            sort_order_id: vec![("p_timestamp".to_owned(), SortOrder::DescNullsLast)],
            ..File::default()
        }
    }

    fn group_paths(groups: &[Vec<File>]) -> Vec<Vec<&str>> {
        groups
            .iter()
            .map(|group| group.iter().map(|file| file.file_path.as_str()).collect())
            .collect()
    }

    #[derive(Debug)]
    struct TestParquetProvider {
        schema: Arc<Schema>,
    }

    #[async_trait::async_trait]
    impl TableProvider for TestParquetProvider {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            let (_, source) =
                build_parquet_scan_components(Arc::clone(&self.schema), filters, state)?;
            let mut builder =
                FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source.into())
                    .with_file(PartitionedFile::new("test.parquet", 10))
                    .with_limit(limit);
            if let Some(projection) = projection {
                builder = builder.with_projection_indices(Some(projection.clone()))?;
            }
            Ok(DataSourceExec::from_data_source(builder.build()))
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
            Ok(filters
                .iter()
                .map(|filter| {
                    if super::expr_in_boundary(filter) {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Inexact
                    }
                })
                .collect())
        }
    }

    #[test]
    fn parquet_scan_components_preserve_session_options() {
        let mut config = SessionConfig::new();
        let parquet = &mut config.options_mut().execution.parquet;
        parquet.pruning = false;
        parquet.pushdown_filters = true;
        parquet.reorder_filters = true;
        parquet.binary_as_string = true;
        parquet.metadata_size_hint = Some(2 * 1024 * 1024);
        parquet.enable_page_index = true;
        parquet.max_predicate_cache_size = Some(987_654);
        parquet.schema_force_view_types = true;
        let context = SessionContext::new_with_config(config);
        let state = context.state();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "p_timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            false,
        )]));

        let (format, source) =
            build_parquet_scan_components(schema, &[], &state).expect("components build");

        let format_options = format.options();
        let source_options = source.table_parquet_options();
        for options in [format_options, source_options] {
            assert!(!options.global.pruning);
            assert!(options.global.pushdown_filters);
            assert!(options.global.reorder_filters);
            assert!(options.global.binary_as_string);
            assert_eq!(options.global.metadata_size_hint, Some(2 * 1024 * 1024));
            assert!(options.global.enable_page_index);
            assert_eq!(options.global.max_predicate_cache_size, Some(987_654));
            assert!(options.global.schema_force_view_types);
        }
    }

    #[test]
    fn only_exact_time_filters_are_preinstalled_in_parquet_source() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "p_timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("job", DataType::Utf8, false),
        ]));
        let exact_time = Expr::Column("p_timestamp".into()).gt_eq(Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(1_672_531_200_000), None),
            None,
        ));
        let inexact_label = Expr::Column("job".into()).eq(Expr::Literal(
            ScalarValue::Utf8(Some("api".to_owned())),
            None,
        ));
        let filters = vec![exact_time.clone(), inexact_label.clone()];

        assert_eq!(exact_source_filters(&filters), vec![exact_time]);

        let context = SessionContext::new();
        let state = context.state();
        let (_, source) =
            build_parquet_scan_components(schema, &filters, &state).expect("components build");
        let predicate = source.filter().expect("exact predicate").to_string();
        assert!(predicate.contains("p_timestamp"));
        assert!(!predicate.contains("job"));
        assert!(!predicate.contains("api"));
    }

    #[test]
    fn full_filters_are_preinstalled_in_parquet_source_when_requested() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "p_timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("job", DataType::Utf8, false),
        ]));
        let filters = vec![
            Expr::Column("p_timestamp".into()).gt_eq(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(1_672_531_200_000), None),
                None,
            )),
            Expr::Column("job".into()).eq(Expr::Literal(
                ScalarValue::Utf8(Some("api".to_owned())),
                None,
            )),
        ];

        let context = SessionContext::new();
        let state = context.state();
        let (_, source) = build_parquet_scan_components_with_full_filters(schema, &filters, &state)
            .expect("components build");
        let predicate = source.filter().expect("full predicate").to_string();
        assert!(predicate.contains("p_timestamp"));
        assert!(predicate.contains("job"));
        assert!(predicate.contains("api"));
    }

    #[tokio::test]
    async fn optimized_plan_enforces_exact_time_once_without_duplicating_inexact_filter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "p_timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("job", DataType::Utf8, false),
        ]));
        let context = SessionContext::new();
        context
            .register_table(
                "metrics",
                Arc::new(TestParquetProvider {
                    schema: Arc::clone(&schema),
                }),
            )
            .unwrap();
        let exact_time = Expr::Column("p_timestamp".into()).gt_eq(Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(1_672_531_200_000), None),
            None,
        ));
        let inexact_label = Expr::Column("job".into()).eq(Expr::Literal(
            ScalarValue::Utf8(Some("api".to_owned())),
            None,
        ));

        let plan = context
            .table("metrics")
            .await
            .unwrap()
            .filter(exact_time.and(inexact_label))
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        let filter_lines = plan
            .lines()
            .filter(|line| line.contains("FilterExec"))
            .collect::<Vec<_>>();
        let source_line = plan
            .lines()
            .find(|line| line.contains("DataSourceExec"))
            .expect("parquet source line");

        assert_eq!(filter_lines.len(), 1, "{plan}");
        assert!(filter_lines[0].contains("job"), "{plan}");
        assert!(!filter_lines[0].contains("p_timestamp"), "{plan}");
        assert!(source_line.contains("predicate=p_timestamp"), "{plan}");
        assert!(!source_line.contains("predicate=job"), "{plan}");
    }

    #[test]
    fn byte_balancing_uses_least_loaded_group_and_preserves_input_order() {
        let files = vec![
            file("z.parquet", 10),
            file("y.parquet", 9),
            file("x.parquet", 8),
            file("w.parquet", 7),
            file("v.parquet", 6),
        ];

        let groups = balanced_file_groups(files, 3);

        assert_eq!(
            group_paths(&groups),
            vec![
                vec!["z.parquet"],
                vec!["y.parquet", "v.parquet"],
                vec!["x.parquet", "w.parquet"],
            ]
        );
        assert_eq!(
            groups
                .iter()
                .map(|group| group.iter().map(|file| file.file_size).sum::<u64>())
                .collect::<Vec<_>>(),
            vec![10, 15, 15]
        );
    }

    #[test]
    fn byte_balancing_omits_empty_groups_and_handles_empty_input() {
        assert_eq!(
            group_paths(&balanced_file_groups(
                vec![file("z.parquet", 2), file("y.parquet", 1)],
                8,
            )),
            vec![vec!["z.parquet"], vec!["y.parquet"]]
        );
        assert!(balanced_file_groups(Vec::new(), 4).is_empty());
        assert!(balanced_file_groups(vec![file("z.parquet", 1)], 0).is_empty());
    }

    #[test]
    fn byte_balancing_uses_largest_files_first_without_reordering_each_group() {
        let files = vec![
            file("first.parquet", 6),
            file("second.parquet", 10),
            file("third.parquet", 7),
            file("fourth.parquet", 9),
            file("fifth.parquet", 8),
        ];

        let groups = balanced_file_groups(files, 3);

        assert_eq!(
            group_paths(&groups),
            vec![
                vec!["second.parquet"],
                vec!["first.parquet", "fourth.parquet"],
                vec!["third.parquet", "fifth.parquet"],
            ]
        );
        assert_eq!(
            groups
                .iter()
                .map(|group| group.iter().map(|file| file.file_size).sum::<u64>())
                .collect::<Vec<_>>(),
            vec![10, 15, 15]
        );
    }

    #[test]
    fn manifest_sort_metadata_does_not_advertise_time_order() {
        for time_column in ["p_timestamp", "event_time"] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                time_column,
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            )]));
            let files = [(200, 299), (100, 199)]
                .into_iter()
                .map(|(min, max)| {
                    let mut file = time_sorted_file("test.parquet", 10, min, max);
                    file.columns[0].name = time_column.to_owned();
                    file.sort_order_id[0].0 = time_column.to_owned();
                    file
                })
                .collect();
            assert!(!super::partitioned_files(&schema, &None, files, 1, false, time_column).2);
        }
    }

    #[tokio::test]
    async fn writer_parquet_requires_sort_even_with_footer_ordering() {
        use crate::{
            cli::Options, metadata::LogStreamMetadata, parseable::Stream, storage::StreamType,
        };
        use arrow_array::{Int32Array, RecordBatch, TimestampMillisecondArray};
        use datafusion::{
            datasource::{
                file_format::parquet::ParquetFormat,
                listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
            },
            physical_plan::{ExecutionPlanProperties, collect},
        };

        #[derive(Debug)]
        struct WriterParquetProvider {
            provider: StandardTableProvider,
            file: File,
            listing: Option<ListingTable>,
        }

        #[async_trait::async_trait]
        impl TableProvider for WriterParquetProvider {
            fn schema(&self) -> Arc<Schema> {
                self.provider.schema.clone()
            }
            fn table_type(&self) -> TableType {
                TableType::Base
            }
            async fn scan(
                &self,
                state: &dyn datafusion::catalog::Session,
                projection: Option<&Vec<usize>>,
                filters: &[Expr],
                limit: Option<usize>,
            ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
                if let Some(listing) = &self.listing {
                    return Ok(super::without_output_ordering(
                        listing.scan(state, projection, filters, limit).await?,
                    ));
                }
                let (files, stats, ordered) = self.provider.partitioned_files(
                    vec![self.file.clone()],
                    1,
                    false,
                    "p_timestamp",
                );
                assert!(!ordered);
                let mut plans = Vec::new();
                self.provider
                    .create_parquet_physical_plan(
                        &mut plans,
                        ObjectStoreUrl::local_filesystem(),
                        files,
                        stats,
                        projection,
                        filters,
                        limit,
                        state,
                        None,
                        ordered,
                    )
                    .await?;
                assert!(plans[0].output_ordering().is_none());
                Ok(plans.remove(0))
            }
        }

        for (values, nullable) in [
            ([1000, 1002, 1001], false),
            ([1002, 1001, 1000], false),
            ([1000, 1002, 1001], true),
        ] {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let staging = Stream::new(
                Arc::new(Options {
                    local_staging_path: temp_dir.path().to_path_buf(),
                    row_group_size: 1048576,
                    ..Default::default()
                }),
                "sort_test",
                LogStreamMetadata::default(),
                None,
                &None,
            );
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "p_timestamp",
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                    nullable,
                ),
                Field::new("id", DataType::Int32, false),
            ]));
            let time = (Utc::now() - Duration::minutes(2)).naive_utc();
            for (index, value) in values.into_iter().enumerate() {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(vec![
                            Some(value),
                            if nullable { None } else { Some(value) },
                            Some(value),
                        ])),
                        Arc::new(Int32Array::from(vec![
                            index as i32 * 3,
                            index as i32 * 3 + 1,
                            index as i32 * 3 + 2,
                        ])),
                    ],
                )
                .unwrap();
                staging
                    .push(
                        "sort",
                        &batch,
                        time,
                        &std::collections::HashMap::new(),
                        StreamType::UserDefined,
                    )
                    .unwrap();
            }
            staging.flush(true).unwrap();
            assert_eq!(staging.arrow_files().len(), 1);
            staging
                .convert_disk_files_to_parquet(None, None, false, true, &None)
                .unwrap();
            let paths = staging.parquet_files();
            assert_eq!(paths.len(), 1);
            let file = crate::catalog::manifest::create_from_parquet_file(
                paths[0].display().to_string(),
                &paths[0],
            )
            .unwrap();
            assert_eq!(
                file.sort_order_id,
                vec![("p_timestamp".to_owned(), SortOrder::DescNullsLast)]
            );
            let mut config = SessionConfig::new();
            config.options_mut().optimizer.prefer_existing_sort = true;
            let context = SessionContext::new_with_config(config);
            context
                .register_table(
                    "logs",
                    Arc::new(WriterParquetProvider {
                        provider: StandardTableProvider {
                            schema: schema.clone(),
                            stream: "sort_test".to_owned(),
                            tenant_id: None,
                            hot_tier_guarded: false,
                        },
                        file: file.clone(),
                        listing: None,
                    }),
                )
                .unwrap();
            let listing = ListingTable::try_new(
                ListingTableConfig::new(
                    ListingTableUrl::parse(paths[0].display().to_string()).unwrap(),
                )
                .with_listing_options(
                    ListingOptions::new(Arc::new(
                        ParquetFormat::default().with_enable_pruning(true),
                    ))
                    .with_file_extension(".parquet"),
                )
                .with_schema(schema.clone()),
            )
            .unwrap();
            let raw = listing
                .scan(&context.state(), None, &[], None)
                .await
                .unwrap();
            assert!(raw.output_ordering().is_some());
            let stats = StatisticsContext::new();
            let raw_stats = stats.compute(raw.as_ref(), &StatisticsArgs::new()).unwrap();
            let stripped = super::without_output_ordering(raw);
            assert!(!stripped.supports_limit_pushdown());
            assert!(stripped.output_ordering().is_none());
            stats.reset_cache();
            assert_eq!(
                stats
                    .compute(stripped.as_ref(), &StatisticsArgs::new())
                    .unwrap(),
                raw_stats
            );
            context
                .register_table(
                    "legacy",
                    Arc::new(WriterParquetProvider {
                        provider: StandardTableProvider {
                            schema,
                            stream: "sort_test".to_owned(),
                            tenant_id: None,
                            hot_tier_guarded: false,
                        },
                        file,
                        listing: Some(listing),
                    }),
                )
                .unwrap();
            for table in ["logs", "legacy"] {
                for limit in ["", " LIMIT 2"] {
                    let plan = context.sql(&format!("SELECT p_timestamp FROM {table} ORDER BY p_timestamp DESC NULLS LAST{limit}")).await.unwrap().create_physical_plan().await.unwrap();
                    let display = displayable(plan.as_ref()).indent(true).to_string();
                    assert!(display.contains("SortExec"), "{display}");
                    if table == "logs" {
                        assert!(
                            !display.contains("output_ordering=[p_timestamp"),
                            "{display}"
                        );
                    }
                    let batches = collect(plan, context.task_ctx()).await.unwrap();
                    let actual: Vec<_> = batches
                        .iter()
                        .flat_map(|batch| {
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap()
                                .iter()
                        })
                        .collect();
                    let mut expected = vec![Some(1002); if nullable { 2 } else { 3 }];
                    expected.extend(vec![Some(1001); if nullable { 2 } else { 3 }]);
                    expected.extend(vec![Some(1000); if nullable { 2 } else { 3 }]);
                    if nullable {
                        expected.extend([None; 3]);
                    }
                    if !limit.is_empty() {
                        expected.truncate(2);
                    }
                    assert_eq!(actual, expected);
                }
            }
        }
    }

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
            right: Box::new(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(1672531200000), None),
                None,
            )),
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
            right: Box::new(Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(1672531200000000000), None),
                None,
            )),
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
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(timestamp.to_owned())),
                None,
            )),
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
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(timestamp.to_owned())),
                None,
            )),
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
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(42)), None)),
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
            right: Box::new(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(1672531200000), None),
                None,
            )),
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
            right: Box::new(Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(1672531200000000000), None),
                None,
            )),
        };
        let result = extract_timestamp_bound(&binexpr, &time_partition);

        assert_eq!(result, expected);
    }
}
