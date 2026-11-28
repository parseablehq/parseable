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

pub(crate) mod staging_parquet;
use staging_parquet::{lease_staging_parquet, validate_staging_parquet};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use chrono::{DateTime, NaiveDateTime, TimeDelta, Timelike, Utc};
use datafusion::{
    catalog::{SchemaProvider, Session},
    common::{
        Constraints, ToDFSchema,
        stats::Precision,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    datasource::{
        DefaultTableSource, MemTable, TableProvider,
        file_format::{FileFormat, parquet::ParquetFormat},
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource},
        provider_as_source,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::object_store::ObjectStoreUrl,
    logical_expr::{
        BinaryExpr, Operator, TableProviderFilterPushDown, TableSource, TableType,
        physical_planning_context::PhysicalPlanningContext, utils::conjunction,
    },
    physical_expr::create_physical_expr,
    physical_plan::{
        ExecutionPlan, Statistics, empty::EmptyExec, filter::FilterExecBuilder, union::UnionExec,
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
pub(super) const MANIFEST_FETCH_CONCURRENCY: usize = 64;

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
            let stream = PARSEABLE
                .get_stream(name, &self.tenant_id)
                .expect(STREAM_EXISTS);
            Ok(Some(Arc::new(StandardTableProvider {
                schema: stream.get_schema(),
                stream: name.to_owned(),
                tenant_id: self.tenant_id.clone(),
                time_partition: stream.get_time_partition(),
                hot_tier_guarded: false,
                planning_cache: None,
            })))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        PARSEABLE.get_stream(name, &self.tenant_id).is_ok()
    }
}

#[derive(Clone, Debug)]
struct StandardTableProvider {
    schema: SchemaRef,
    // prefix under which to find snapshot
    stream: String,
    tenant_id: Option<String>,
    // Column the stream's data files are partitioned by; `p_timestamp` when `None`.
    time_partition: Option<String>,
    hot_tier_guarded: bool,
    planning_cache: Option<Arc<PlanningCache>>,
}

// Installed on a cloned table source for one opted-in request only. Historical
// scans can share a metadata view; staging scans retain the publication capture
// and refresh sequence below and deliberately bypass this cache.
#[derive(Debug, Default)]
struct PlanningCache {
    format: tokio::sync::OnceCell<ObjectStoreFormat>,
    manifests: tokio::sync::Mutex<HashMap<String, Option<crate::catalog::manifest::Manifest>>>,
}

pub(super) fn with_planning_cache(
    plan: datafusion::logical_expr::LogicalPlan,
) -> DataFusionResult<datafusion::logical_expr::LogicalPlan> {
    plan.transform_up(|node| {
        if let datafusion::logical_expr::LogicalPlan::TableScan(mut scan) = node {
            if let Some(provider) = standard_table_provider(&scan.source) {
                let mut provider = provider.clone();
                provider.planning_cache = Some(Arc::new(PlanningCache::default()));
                scan.source = provider_as_source(Arc::new(provider));
            }
            Ok(Transformed::yes(
                datafusion::logical_expr::LogicalPlan::TableScan(scan),
            ))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|t| t.data)
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
        time_partition: provider.time_partition.clone(),
        hot_tier_guarded: true,
        planning_cache: provider.planning_cache.clone(),
    })))
}

pub fn exact_source_filters(filters: &[Expr]) -> Vec<Expr> {
    filters
        .iter()
        .filter(|filter| expr_in_boundary(filter))
        .cloned()
        .collect()
}

/// Scans `table`, whose scan ignores pushed filters, and enforces the filters
/// `StandardTableProvider` reports as `Exact` (`exact_source_filters` is a
/// superset of them: see `filter_pushdown`).
///
/// DataFusion removes the `FilterExec` above the scan for `Exact` filters. Parquet
/// scans enforce them through the source predicate (see `exact_source_filters`),
/// but `MemTable` and `ListingTable` do not, so their rows are filtered here. The
/// limit is applied after the filter, and the filter's columns are scanned even
/// when they are not projected.
async fn scan_enforcing_exact_filters(
    table: &dyn TableProvider,
    state: &dyn Session,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let Some(predicate) = conjunction(exact_source_filters(filters)) else {
        return table.scan(state, projection, filters, limit).await;
    };

    let schema = table.schema();
    let scan_projection = projection
        .map(|projection| {
            let mut columns = projection.clone();
            for column in predicate.column_refs() {
                columns.push(schema.index_of(&column.name)?);
            }
            columns.sort_unstable();
            columns.dedup();
            Ok::<_, DataFusionError>(columns)
        })
        .transpose()?;
    // Positions of the requested columns in the scan's output.
    let output_projection =
        projection
            .zip(scan_projection.as_ref())
            .map(|(projection, scanned)| {
                projection
                    .iter()
                    .map(|index| {
                        scanned
                            .binary_search(index)
                            .expect("projected column is scanned")
                    })
                    .collect_vec()
            });

    let input = table
        .scan(state, scan_projection.as_ref(), filters, None)
        .await?;
    let predicate = create_physical_expr(
        &predicate,
        &input.schema().to_dfschema()?,
        state.execution_props(),
        &PhysicalPlanningContext::default(),
    )?;
    let filter = FilterExecBuilder::new(predicate, input)
        .apply_projection(output_projection)?
        .with_fetch(limit)
        .build()?;
    Ok(Arc::new(filter))
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
    // leaving one scan partition as the tail. Keep each group's original (newest-first manifest)
    // file order after the assignment.
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

/// Builds the Parquet scan over manifest or staging files.
///
/// No output ordering is declared. A manifest's `sort_order_id` only copies the Parquet footer's
/// `sorting_columns`, and the writer declares time-descending order without sorting: concurrent
/// requests append in writer-lock order rather than `p_timestamp` order, and time-partitioned rows
/// keep the client's order. Declaring that unverified order lets DataFusion drop a sort or stop a
/// TopK scan early (for example `ORDER BY p_timestamp DESC NULLS LAST LIMIT n`) and return the
/// wrong rows.
#[allow(clippy::too_many_arguments)]
async fn parquet_scan_plan(
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    partitions: Vec<Vec<PartitionedFile>>,
    statistics: Statistics,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    state: &dyn Session,
    batch_size: usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let cache_options = if object_store_url.as_str() == "file:///" {
        (0, 0, false)
    } else {
        (
            Arc::as_ptr(&PARSEABLE.storage) as *const () as usize,
            PARSEABLE.options.small_file_cache_size,
            PARSEABLE.options.small_file_cache_enabled,
        )
    };
    parquet_scan_plan_with_cache_options(
        schema,
        object_store_url,
        partitions,
        statistics,
        projection,
        filters,
        limit,
        state,
        batch_size,
        cache_options,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn parquet_scan_plan_with_cache_options(
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    partitions: Vec<Vec<PartitionedFile>>,
    statistics: Statistics,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    state: &dyn Session,
    batch_size: usize,
    cache_options: (usize, usize, bool),
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let (file_format, file_source) = build_parquet_scan_components(schema, filters, state)?;
    let (cache_scope, cache_budget, cache_enabled) = cache_options;
    let cache_store =
        if object_store_url.as_str() != "file:///" && cache_enabled && cache_budget > 0 {
            Some(state.runtime_env().object_store(&object_store_url)?)
        } else {
            None
        };

    // create file groups from vec file partitions
    let file_groups = partitions.into_iter().map(FileGroup::new).collect_vec();

    let mut conf_builder = FileScanConfigBuilder::new(object_store_url, file_source.into())
        .with_statistics(statistics)
        .with_batch_size(Some(batch_size))
        .with_constraints(Constraints::default())
        .with_file_groups(file_groups);

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
    if let Some(store) = cache_store {
        super::small_file_cache::install(
            plan,
            store,
            cache_scope,
            cache_budget,
            cache_enabled,
            state
                .config()
                .get_extension::<super::small_file_cache::RequestView>()
                .unwrap_or_default(),
        )
    } else {
        Ok(plan)
    }
}

// A snapshot used with staging must be fetched after staging is leased. If
// remote settings newly require staging, discard the first snapshot and retry.
async fn capture_before_snapshot<T, M, C, L, F, I>(
    predicted: bool,
    mut capture: C,
    mut load: L,
    includes_staging: I,
) -> DataFusionResult<(M, Option<T>)>
where
    C: FnMut() -> DataFusionResult<T>,
    L: FnMut(bool) -> F,
    F: std::future::Future<Output = DataFusionResult<M>>,
    I: Fn(&M) -> bool,
{
    let pending = predicted.then(&mut capture);
    let mut captured = None;
    let mut metadata = load(false).await?;
    if predicted && includes_staging(&metadata) {
        captured = Some(pending.expect("predicted capture")?);
    }
    if !predicted && includes_staging(&metadata) {
        captured = Some(capture()?);
        metadata = load(true).await?;
    }
    Ok((metadata, captured))
}

impl StandardTableProvider {
    fn cache_for_filters(&self, filters: &[Expr]) -> Option<&PlanningCache> {
        self.planning_cache.as_deref().filter(|_| {
            !is_within_staging_window(&extract_primary_filter(filters, &self.time_partition))
        })
    }

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
    ) -> Result<(), DataFusionError> {
        let plan = parquet_scan_plan(
            self.schema.clone(),
            object_store_url,
            partitions,
            statistics,
            projection,
            filters,
            limit,
            state,
            PARSEABLE.options.execution_batch_size,
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

        let (partitioned_files, statistics, _) = self.partitioned_files(
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
        captured: (Vec<RecordBatch>, Vec<PartitionedFile>),
    ) -> Result<(), DataFusionError> {
        // Both sources were captured before the remote snapshot was read.
        let (records, partitioned_files) = captured;
        // Keep the reverse traversal within one partition when the scan is
        // asked to stop early. Without a limit, contiguous groups can run
        // independently and any SQL ordering is enforced by DataFusion.
        let target_partitions = if limit.is_some() {
            1
        } else {
            state
                .config_options()
                .execution
                .target_partitions
                .min(num_cpus::get())
        };
        let (mem_table, mem_projection) = projected_reversed_mem_table(
            records,
            self.schema.clone(),
            projection,
            filters,
            target_partitions,
        )?;
        let arrow_exec = scan_enforcing_exact_filters(
            &mem_table,
            state,
            mem_projection.as_ref(),
            filters,
            limit,
        )
        .await?;
        execution_plans.push(arrow_exec);

        // Keep the native scan and its file extensions: each extension owns the
        // hard-link lease through optimizer rewrites and execution streams.
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
                    let plan = scan_enforcing_exact_filters(
                        table.as_ref(),
                        state,
                        projection,
                        filters,
                        limit,
                    )
                    .await?;
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

/// Groups manifest files into scan partitions and merges their statistics.
///
/// The returned flag, whether the scan output is ordered by `time_column`, is always `false`: a
/// manifest's `sort_order_id` is copied from an unverified Parquet footer claim, so it cannot
/// prove that a file is sorted.
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

    (partitioned_files, statistics, false)
}

pub async fn collect_from_snapshot(
    snapshot: &Snapshot,
    time_filters: &[PartialTimeFilter],
    filters: &[Expr],
    limit: Option<usize>,
    stream_name: &str,
    tenant_id: &Option<String>,
) -> Result<Vec<File>, DataFusionError> {
    collect_from_snapshot_cached(
        snapshot,
        time_filters,
        filters,
        limit,
        stream_name,
        tenant_id,
        None,
    )
    .await
}

async fn collect_from_snapshot_cached(
    snapshot: &Snapshot,
    time_filters: &[PartialTimeFilter],
    filters: &[Expr],
    limit: Option<usize>,
    stream_name: &str,
    tenant_id: &Option<String>,
    cache: Option<&PlanningCache>,
) -> Result<Vec<File>, DataFusionError> {
    // Manifest fetches are one object store GET each and are pure round trip
    // latency, so run them concurrently instead of serially. `buffered` keeps
    // the snapshot ordering, which the `rev()` below depends on.
    let items = snapshot.manifests(time_filters);
    let mut uncached = Vec::new();
    for item in &items {
        if let Some(cache) = cache
            && cache
                .manifests
                .lock()
                .await
                .contains_key(&item.manifest_path)
        {
            continue;
        }
        uncached.push(item.manifest_path.clone());
    }
    let batch = PARSEABLE
        .metastore
        .manifest_batch(&uncached, tenant_id)
        .await
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    let manifest_files = futures_util::stream::iter(items)
        .map(|manifest_item| {
            let ManifestItem {
                time_lower_bound,
                time_upper_bound,
                manifest_path,
                ..
            } = manifest_item;
            let batch = batch.as_ref();
            async move {
                if let Some(cache) = cache
                    && let Some(manifest) =
                        cache.manifests.lock().await.get(&manifest_path).cloned()
                {
                    return Ok(manifest);
                }
                let manifest_opt = if let Some(batch) = batch {
                    batch.get(&manifest_path).cloned().flatten()
                } else {
                    PARSEABLE
                        .metastore
                        .get_manifest(
                            stream_name,
                            time_lower_bound,
                            time_upper_bound,
                            Some(manifest_path.clone()),
                            tenant_id,
                        )
                        .await
                        .map_err(|e| DataFusionError::Plan(e.to_string()))?
                };
                if manifest_opt.is_none() {
                    tracing::warn!(
                        "Manifest missing for stream={} [{:?} - {:?}]",
                        stream_name,
                        time_lower_bound,
                        time_upper_bound
                    );
                }
                if let Some(cache) = cache {
                    cache
                        .manifests
                        .lock()
                        .await
                        .insert(manifest_path, manifest_opt.clone());
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

        let cache = self.cache_for_filters(filters);
        let load_format = || async {
            serde_json::from_slice::<ObjectStoreFormat>(
                &PARSEABLE
                    .metastore
                    .get_stream_json(&self.stream, false, &self.tenant_id, false)
                    .await
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?,
            )
            .map_err(|e| DataFusionError::Plan(e.to_string()))
        };

        // Capture before the first remote read. Resident settings normally match
        // remote settings; if they disagree about staging, capture and re-read.
        // Publication always precedes staging unlink, so this order cannot hide
        // a file uploaded while the query is being planned.
        let capture = || -> DataFusionResult<_> {
            match PARSEABLE.get_stream(&self.stream, &self.tenant_id) {
                Ok(staging) => {
                    let (records, files) = staging
                        .capture_staging(&self.schema, lease_staging_parquet)
                        .map_err(|error| {
                            DataFusionError::Execution(format!(
                                "Cannot capture staging record batches: {error}"
                            ))
                        })?;
                    // The hard links retain the captured inodes after the
                    // publication lock is released.
                    Ok((records, validate_staging_parquet(files?)?))
                }
                Err(_) => Ok((Vec::new(), Vec::new())),
            }
        };
        let captured_before_read =
            is_within_staging_window(&extract_primary_filter(filters, &self.time_partition));
        let read_format = |refresh: bool| async move {
            if let Some(cache) = cache.filter(|_| !refresh) {
                cache.format.get_or_try_init(load_format).await.cloned()
            } else {
                load_format().await
            }
        };
        let (object_store_format, captured) =
            capture_before_snapshot(captured_before_read, capture, read_format, |format| {
                is_within_staging_window(&extract_primary_filter(filters, &format.time_partition))
            })
            .await?;
        let (staged_records, staged_files) = captured.unwrap_or_default();
        let time_partition = object_store_format.time_partition;
        let mut time_filters = extract_primary_filter(filters, &time_partition);
        let include_staging = is_within_staging_window(&time_filters);
        let mut merged_snapshot = Snapshot::default();
        if PARSEABLE.options.mode == Mode::Query || PARSEABLE.options.mode == Mode::Prism {
            let objects = PARSEABLE
                .metastore
                .get_all_stream_jsons(&self.stream, None, &self.tenant_id, false)
                .await
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            for object in objects {
                let format: ObjectStoreFormat = serde_json::from_slice(&object)
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?;
                merged_snapshot
                    .manifest_list
                    .extend(format.snapshot.manifest_list);
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

        let mut manifest_files = collect_from_snapshot_cached(
            &merged_snapshot,
            &time_filters,
            filters,
            limit,
            &self.stream,
            &self.tenant_id,
            cache,
        )
        .await?;

        // Compare only with files actually selected for the remote scan, before
        // hot-tier routing mutates that list. Pruned/limit-excluded files must not
        // hide a staging copy. Use exactly the uploader's path mapping.
        if include_staging {
            let selected_remote = manifest_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<std::collections::HashSet<_>>();
            let staged_files =
                staging_parquet::exclude_uploaded(staged_files, &selected_remote, |path| {
                    let filename =
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Invalid staging Parquet filename: {}",
                                    path.display()
                                ))
                            })?;
                    let relative = crate::storage::object_storage::stream_relative_path(
                        &self.stream,
                        filename,
                        &object_store_format.custom_partition,
                        &self.tenant_id,
                    );
                    Ok(glob_storage
                        .absolute_url(relative_path::RelativePath::new(&relative))
                        .to_string())
                })?;
            self.get_staging_execution_plan(
                &mut execution_plans,
                projection,
                filters,
                limit,
                state,
                (staged_records, staged_files),
            )
            .await?;
        }

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

        let (partitioned_files, statistics, _) = self.partitioned_files(
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
        Ok(filters
            .iter()
            .map(|filter| filter_pushdown(filter, &self.time_partition))
            .collect_vec())
    }
}

pub fn reversed_mem_table(
    records: Vec<RecordBatch>,
    schema: Arc<Schema>,
) -> Result<MemTable, DataFusionError> {
    partitioned_reversed_mem_table(records, schema, 1)
}

fn projected_reversed_mem_table(
    mut records: Vec<RecordBatch>,
    mut schema: SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    target_partitions: usize,
) -> DataFusionResult<(MemTable, Option<Vec<usize>>)> {
    let mut mem_projection = projection.cloned();
    if let Some(projection) = projection {
        let mut needed = projection.clone();
        for column in filters.iter().flat_map(Expr::column_refs) {
            needed.push(schema.index_of(&column.name)?);
        }
        needed.sort_unstable();
        needed.dedup();

        if !needed.iter().copied().eq(0..schema.fields().len()) {
            schema = Arc::new(schema.project(&needed)?);
            // Arrow projects by cloning column references. Only columns that
            // survive this step enter the reverse/take kernel.
            for batch in &mut records {
                *batch = batch.project(&needed)?;
            }
            mem_projection = Some(
                projection
                    .iter()
                    .map(|index| {
                        needed
                            .binary_search(index)
                            .expect("projected column is retained")
                    })
                    .collect(),
            );
        }
    }

    let table = partitioned_reversed_mem_table(records, schema, target_partitions)?;
    Ok((table, mem_projection))
}

fn partitioned_reversed_mem_table(
    records: Vec<RecordBatch>,
    schema: SchemaRef,
    target_partitions: usize,
) -> DataFusionResult<MemTable> {
    let partition_count = target_partitions.clamp(1, records.len().max(1));
    let batches_per_partition = records.len() / partition_count;
    let extra_batches = records.len() % partition_count;
    let mut reversed = records.into_iter().rev().map(|batch| {
        // A zero-column projection still represents all of its original rows.
        if batch.num_columns() == 0 {
            batch
        } else {
            crate::utils::arrow::reverse(&batch)
        }
    });
    let partitions = (0..partition_count)
        .map(|index| {
            reversed
                .by_ref()
                .take(batches_per_partition + usize::from(index < extra_batches))
                .collect()
        })
        .collect();
    MemTable::try_new(schema, partitions)
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

        // Manifests (and staging/listing prefixes) are bucketed by the stream's
        // time partition column, or `p_timestamp` when none is configured. A
        // bound on any other column, including `p_timestamp` on a stream with a
        // custom time partition, says nothing about which manifests hold
        // matching rows and must not be used to prune them.
        let partition_column = time_partition.as_deref().unwrap_or(DEFAULT_TIMESTAMP_KEY);
        let Expr::Column(column) = binexpr.left.as_ref() else {
            return None;
        };
        if column.name != partition_column {
            return None;
        }
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

/// How `StandardTableProvider` can handle `filter`.
///
/// Data files hold one minute of the stream's time partition column, so a
/// minute-aligned bound on that column either keeps or prunes each file whole.
/// Only then is the filter `Exact`: DataFusion drops the `FilterExec` above the
/// scan and may push `LIMIT` into it, and `collect_from_snapshot` truncates the
/// file list by row counts assuming every row of a surviving file matches. A
/// bound on any other column (including `p_timestamp` on a stream with a custom
/// time partition) only prunes files partially, so it stays `Inexact`.
fn filter_pushdown(filter: &Expr, time_partition: &Option<String>) -> TableProviderFilterPushDown {
    let partition_column = time_partition.as_deref().unwrap_or(DEFAULT_TIMESTAMP_KEY);
    let is_partition_column = matches!(
        filter,
        Expr::BinaryExpr(BinaryExpr { left, .. })
            if matches!(left.as_ref(), Expr::Column(column) if column.name == partition_column)
    );
    if is_partition_column && expr_in_boundary(filter) {
        TableProviderFilterPushDown::Exact
    } else {
        // File level mechanisms such as Parquet row group pruning may still apply.
        TableProviderFilterPushDown::Inexact
    }
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
        // Out-of-range literals (reachable from SQL, e.g.
        // `to_timestamp_millis(9223372036854775807)`) yield no bound.
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            DateTime::from_timestamp_millis(*value).map(|time| (binexpr.op, time.naive_utc()))
        }
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
        physical_plan::{ExecutionPlan, displayable},
        prelude::Expr,
        scalar::ScalarValue,
    };

    use crate::catalog::{
        Snapshot as _,
        column::{Column, Int64Type, TypedStatistics},
        manifest::{File, SortOrder},
        snapshot::{ManifestItem, Snapshot},
    };
    use crate::event::DEFAULT_TIMESTAMP_KEY;

    use super::{
        HotTierStreamKey, PartialTimeFilter, StandardTableProvider, balanced_file_groups,
        build_parquet_scan_components, build_parquet_scan_components_with_full_filters,
        exact_source_filters, extract_primary_filter, extract_timestamp_bound,
        guarded_hot_tier_table_source, hot_tier_stream_key, is_overlapping_query,
        projected_reversed_mem_table, scan_enforcing_exact_filters, standard_table_provider,
    };

    #[tokio::test]
    async fn staging_reverses_projected_and_filter_columns_in_contiguous_partitions() {
        use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
        use datafusion::{physical_plan::collect_partitioned, prelude::col};

        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, false),
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int64, false),
        ]));
        let large_value = "a".repeat(64 * 1024);
        let payload = Arc::new(StringArray::from(vec![large_value.as_str(); 3]));
        let records = (0..3)
            .map(|offset| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        payload.clone(),
                        Arc::new(StringArray::from(vec!["first", "second", "third"])),
                        Arc::new(TimestampMillisecondArray::from(vec![60_000, 0, 60_000])),
                        Arc::new(Int64Array::from(vec![
                            offset * 3,
                            offset * 3 + 1,
                            offset * 3 + 2,
                        ])),
                    ],
                )
                .unwrap()
            })
            .collect();
        let projection = vec![3, 1];
        let filters = [col(DEFAULT_TIMESTAMP_KEY).gt_eq(Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(60_000), None),
            None,
        ))];
        let (table, mapped_projection) =
            projected_reversed_mem_table(records, schema.clone(), Some(&projection), &filters, 2)
                .unwrap();

        assert_eq!(
            table.schema().as_ref(),
            &schema.project(&[1, 2, 3]).unwrap()
        );
        assert_eq!(Arc::strong_count(&payload), 1);
        assert_eq!(mapped_projection, Some(vec![2, 0]));
        assert_eq!(table.batches.len(), 2);

        let context = SessionContext::new();
        let plan = scan_enforcing_exact_filters(
            &table,
            &context.state(),
            mapped_projection.as_ref(),
            &filters,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            plan.schema().as_ref(),
            &schema.project(&projection).unwrap()
        );
        let partitions = collect_partitioned(plan, context.task_ctx()).await.unwrap();
        let ids: Vec<Vec<_>> = partitions
            .iter()
            .map(|batches| {
                batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                            .iter()
                            .copied()
                    })
                    .collect()
            })
            .collect();
        assert_eq!(ids, vec![vec![8, 6, 5, 3], vec![2, 0]]);
    }

    #[tokio::test]
    async fn staging_empty_projection_keeps_row_count() {
        use arrow_array::{ArrayRef, Int64Array, RecordBatch};

        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let (table, projection) =
            projected_reversed_mem_table(vec![batch], schema, Some(&vec![]), &[], 2).unwrap();
        assert_eq!(projection, Some(vec![]));
        let partition = table.batches[0].read().await;
        assert_eq!(partition[0].num_columns(), 0);
        assert_eq!(partition[0].num_rows(), 3);
    }

    #[test]
    fn prefilter_cache_is_request_scoped_and_bypasses_staging() {
        use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
        use datafusion::prelude::{col, lit};
        let source = provider_as_source(Arc::new(StandardTableProvider {
            schema: Arc::new(Schema::new(vec![Field::new(
                "p_timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            )])),
            stream: "logs".to_owned(),
            tenant_id: Some("tenant".to_owned()),
            time_partition: None,
            hot_tier_guarded: true,
            planning_cache: None,
        }));
        let original = LogicalPlanBuilder::scan("logs", source.clone(), None)
            .unwrap()
            .build()
            .unwrap();
        let first = super::with_planning_cache(original.clone()).unwrap();
        let second = super::with_planning_cache(original).unwrap();
        let provider = |plan: LogicalPlan| {
            let LogicalPlan::TableScan(scan) = plan else {
                panic!("expected scan")
            };
            standard_table_provider(&scan.source).unwrap().clone()
        };
        let a = provider(first.clone());
        let clone = provider(first);
        let b = provider(second);
        assert!(
            standard_table_provider(&source)
                .unwrap()
                .planning_cache
                .is_none()
        );
        assert!(Arc::ptr_eq(
            a.planning_cache.as_ref().unwrap(),
            clone.planning_cache.as_ref().unwrap()
        ));
        assert!(!Arc::ptr_eq(
            a.planning_cache.as_ref().unwrap(),
            b.planning_cache.as_ref().unwrap()
        ));
        assert_eq!(a.tenant_id.as_deref(), Some("tenant"));
        assert!(a.hot_tier_guarded);
        let bounds = |end| {
            vec![
                col("p_timestamp").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(0), None))),
                col("p_timestamp").lt_eq(lit(ScalarValue::TimestampMillisecond(Some(end), None))),
            ]
        };
        assert!(a.cache_for_filters(&bounds(60_000)).is_some());
        assert!(
            a.cache_for_filters(&bounds(Utc::now().timestamp_millis()))
                .is_none()
        );
    }

    #[test]
    fn guarded_hot_tier_source_preserves_provider_identity() {
        let source = provider_as_source(Arc::new(StandardTableProvider {
            schema: Arc::new(Schema::empty()),
            stream: "logs".to_owned(),
            tenant_id: Some("other-tenant".to_owned()),
            time_partition: Some("event_time".to_owned()),
            hot_tier_guarded: false,
            planning_cache: None,
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
        assert_eq!(
            guarded_provider.time_partition.as_deref(),
            Some("event_time")
        );
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
                .map(|filter| super::filter_pushdown(filter, &None))
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

    #[test]
    fn only_minute_aligned_partition_column_bounds_are_exact() {
        let minute = ScalarValue::TimestampMillisecond(Some(1_672_531_200_000), None);
        let mid_minute = ScalarValue::TimestampMillisecond(Some(1_672_531_230_000), None);
        let bound = |column: &str, value: &ScalarValue| {
            Expr::Column(column.into()).gt_eq(Expr::Literal(value.clone(), None))
        };
        let event_time = Some("event_time".to_owned());

        let cases = [
            (bound("p_timestamp", &minute), &None, true),
            (bound("p_timestamp", &mid_minute), &None, false),
            (bound("created_at", &minute), &None, false),
            (bound("event_time", &minute), &event_time, true),
            // Ingestion time on a stream partitioned by event time.
            (bound("p_timestamp", &minute), &event_time, false),
            (
                Expr::Literal(minute.clone(), None).lt(Expr::Column("p_timestamp".into())),
                &None,
                false,
            ),
        ];
        for (filter, time_partition, exact) in cases {
            assert_eq!(
                matches!(
                    super::filter_pushdown(&filter, time_partition),
                    TableProviderFilterPushDown::Exact
                ),
                exact,
                "{filter} with time partition {time_partition:?}"
            );
        }
    }

    #[tokio::test]
    async fn limit_is_pushed_into_scan_only_for_exact_partition_bounds() {
        // `collect_from_snapshot` truncates the file list by row counts when a
        // limit reaches the scan, so it must not see one when a filter only
        // partially matches the surviving files.
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "p_timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let context = SessionContext::new();
        context
            .register_table("logs", Arc::new(TestParquetProvider { schema }))
            .unwrap();

        for (column, limit_pushed) in [("p_timestamp", true), ("created_at", false)] {
            let plan = context
                .sql(&format!(
                    "SELECT * FROM logs WHERE {column} >= '2023-01-01T00:00:00' LIMIT 5"
                ))
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let plan = displayable(plan.as_ref()).indent(true).to_string();
            let source_line = plan
                .lines()
                .find(|line| line.contains("DataSourceExec"))
                .expect("parquet source line");

            assert_eq!(source_line.contains("limit=5"), limit_pushed, "{plan}");
            assert_eq!(
                plan.lines()
                    .any(|line| line.contains("FilterExec") && line.contains(column)),
                !limit_pushed,
                "{plan}"
            );
        }
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
    fn scan_order_is_never_advertised_from_footer_sort_order() {
        // Footer-derived `sort_order_id` plus non-overlapping statistics: a single file can still
        // be unsorted inside, so neither one file nor a statistics-chained group is advertised.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "p_timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            true,
        )]));
        for files in [
            vec![time_sorted_file("only.parquet", 10, 100, 199)],
            vec![
                time_sorted_file("new.parquet", 10, 200, 299),
                time_sorted_file("old.parquet", 10, 100, 199),
            ],
        ] {
            for target_partitions in [1, 4] {
                let (_, _, ordered) = super::partitioned_files(
                    &schema,
                    &None,
                    files.clone(),
                    target_partitions,
                    false,
                    "p_timestamp",
                );
                assert!(!ordered);
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
    fn out_of_range_millisecond_literal_yields_no_bound() {
        let binexpr = BinaryExpr {
            left: Box::new(Expr::Column("p_timestamp".into())),
            op: Operator::GtEq,
            right: Box::new(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(i64::MAX), None),
                None,
            )),
        };

        assert_eq!(extract_timestamp_bound(&binexpr, &None), None);
        assert!(!super::expr_in_boundary(&Expr::BinaryExpr(binexpr)));
    }

    #[tokio::test]
    async fn out_of_range_millisecond_literal_in_sql_does_not_panic() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "p_timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            false,
        )]));
        let context = SessionContext::new();
        context
            .register_table("logs", Arc::new(TestParquetProvider { schema }))
            .unwrap();

        context
            .sql("SELECT * FROM logs WHERE p_timestamp >= to_timestamp_millis(9223372036854775807)")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
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

    fn ts_millis(time: DateTime<Utc>) -> Expr {
        Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(time.timestamp_millis()), None),
            None,
        )
    }

    fn column(name: &str) -> Expr {
        Expr::Column(name.into())
    }

    fn pruned_manifest_paths(filters: &[Expr], time_partition: &Option<String>) -> Vec<String> {
        let time_filters = extract_primary_filter(filters, time_partition);
        manifest_snapshot()
            .manifests(&time_filters)
            .into_iter()
            .map(|item| item.manifest_path)
            .collect()
    }

    fn manifest_snapshot() -> Snapshot {
        Snapshot {
            manifest_list: manifest_items(),
            ..Snapshot::default()
        }
    }

    #[test]
    fn non_timestamp_column_filter_does_not_prune_manifests() {
        // `created_at` is unrelated to how manifests are bucketed: rows with a
        // late `created_at` can live in any day's manifest.
        let filters = [
            column("created_at").gt_eq(ts_millis(datetime_min(2023, 12, 17))),
            column("created_at").lt(ts_millis(datetime_max(2023, 12, 17))),
        ];

        assert!(extract_primary_filter(&filters, &None).is_empty());
        assert_eq!(pruned_manifest_paths(&filters, &None), ["1", "2", "3"]);
    }

    #[test]
    fn non_timestamp_string_literal_filter_does_not_prune_manifests() {
        let filters = [column("created_at").gt(Expr::Literal(
            ScalarValue::Utf8(Some("2023-12-17T00:00:00".to_owned())),
            None,
        ))];

        assert!(extract_primary_filter(&filters, &None).is_empty());
        assert_eq!(pruned_manifest_paths(&filters, &None), ["1", "2", "3"]);
    }

    #[test]
    fn p_timestamp_filter_prunes_manifests() {
        let filters = [
            column(DEFAULT_TIMESTAMP_KEY).gt_eq(ts_millis(datetime_min(2023, 12, 16))),
            column(DEFAULT_TIMESTAMP_KEY).lt(ts_millis(datetime_max(2023, 12, 16))),
        ];

        assert_eq!(extract_primary_filter(&filters, &None).len(), 2);
        assert_eq!(pruned_manifest_paths(&filters, &None), ["2"]);
    }

    #[test]
    fn qualified_p_timestamp_filter_prunes_manifests() {
        // The query handler injects table-qualified time filters.
        let filters = [Expr::Column(datafusion::common::Column::new(
            Some("app_logs"),
            DEFAULT_TIMESTAMP_KEY,
        ))
        .gt_eq(ts_millis(datetime_min(2023, 12, 17)))];

        assert_eq!(pruned_manifest_paths(&filters, &None), ["3"]);
    }

    #[test]
    fn literal_on_left_or_non_comparison_does_not_prune_manifests() {
        let filters = [
            ts_millis(datetime_min(2023, 12, 17)).lt(column(DEFAULT_TIMESTAMP_KEY)),
            column(DEFAULT_TIMESTAMP_KEY).not_eq(ts_millis(datetime_min(2023, 12, 17))),
        ];

        assert!(extract_primary_filter(&filters, &None).is_empty());
    }

    #[test]
    fn custom_time_partition_filter_prunes_manifests() {
        // Manifests of time-partitioned streams are bucketed by the partition
        // column, so bounds on it (timestamp or parseable string) prune.
        let time_partition = Some("event_time".to_owned());
        let filters = [
            column("event_time").gt_eq(ts_millis(datetime_min(2023, 12, 16))),
            column("event_time").lt(Expr::Literal(
                ScalarValue::Utf8(Some("2023-12-16T12:00:00".to_owned())),
                None,
            )),
        ];

        assert_eq!(extract_primary_filter(&filters, &time_partition).len(), 2);
        assert_eq!(pruned_manifest_paths(&filters, &time_partition), ["2"]);
    }

    #[test]
    fn p_timestamp_filter_does_not_prune_custom_time_partition_manifests() {
        // On a time-partitioned stream `p_timestamp` is ingestion time, while
        // manifests are bucketed by event time: back-filled events ingested on
        // the 17th sit in the 15th's manifest and must not be pruned away.
        let time_partition = Some("event_time".to_owned());
        let filters = [column(DEFAULT_TIMESTAMP_KEY).gt_eq(ts_millis(datetime_min(2023, 12, 17)))];

        assert!(extract_primary_filter(&filters, &time_partition).is_empty());
        assert_eq!(
            pruned_manifest_paths(&filters, &time_partition),
            ["1", "2", "3"]
        );
    }

    #[test]
    fn unrelated_column_does_not_prune_custom_time_partition_manifests() {
        let time_partition = Some("event_time".to_owned());
        let filters = [column("created_at").gt_eq(ts_millis(datetime_min(2023, 12, 17)))];

        assert!(extract_primary_filter(&filters, &time_partition).is_empty());
    }
}

#[cfg(test)]
mod ordering_tests;

#[cfg(test)]
mod exact_filter_tests;

#[cfg(test)]
mod snapshot_capture_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn publication_between_capture_and_read_cannot_hide_a_file() {
        for predicted in [true, false] {
            let published = AtomicUsize::new(0);
            let reads = AtomicUsize::new(0);
            let (snapshot, staging) = capture_before_snapshot(
                predicted,
                || {
                    // Capture a lease, then simulate publication and unlink.
                    let lease = "just-uploaded.parquet";
                    published.store(1, Ordering::SeqCst);
                    Ok(lease)
                },
                |_| async {
                    reads.fetch_add(1, Ordering::SeqCst);
                    Ok(published.load(Ordering::SeqCst))
                },
                |_| true,
            )
            .await
            .unwrap();
            assert_eq!(snapshot, 1);
            assert_eq!(staging, Some("just-uploaded.parquet"));
            assert_eq!(reads.load(Ordering::SeqCst), if predicted { 1 } else { 2 });
        }
    }

    #[tokio::test]
    async fn historical_queries_do_not_capture_staging() {
        let (snapshot, staging) = capture_before_snapshot(
            false,
            || -> DataFusionResult<()> { panic!("historical scan must not lease staging") },
            |_| async { Ok(42) },
            |_| false,
        )
        .await
        .unwrap();
        assert_eq!(snapshot, 42);
        assert_eq!(staging, None);
    }
    #[tokio::test]
    async fn speculative_capture_error_only_fails_when_staging_is_needed() {
        for needed in [false, true] {
            let result = capture_before_snapshot(
                true,
                || -> DataFusionResult<()> {
                    Err(DataFusionError::Execution("capture failed".into()))
                },
                |_| async { Ok(needed) },
                |needed| *needed,
            )
            .await;
            assert_eq!(result.is_err(), needed);
        }
    }
}
