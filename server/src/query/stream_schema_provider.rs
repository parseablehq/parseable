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

use std::{any::Any, collections::HashMap, ops::Bound, sync::Arc};

use arrow_schema::{Schema, SchemaRef, SortOptions};
use bytes::Bytes;
use chrono::{NaiveDateTime, Timelike, Utc};
use datafusion::{
    catalog::schema::SchemaProvider,
    common::{
        tree_node::{TreeNode, VisitRecursion},
        ToDFSchema,
    },
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::DataFusionError,
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, TableType},
    optimizer::utils::conjunction,
    physical_expr::{create_physical_expr, PhysicalSortExpr},
    physical_plan::{self, ExecutionPlan},
    prelude::{Column, Expr},
    scalar::ScalarValue,
};
use futures_util::{stream::FuturesOrdered, StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::{path::Path, ObjectStore};
use url::Url;

use crate::{
    catalog::{self, column::TypedStatistics, manifest::Manifest, ManifestFile, Snapshot},
    event::{self, DEFAULT_TIMESTAMP_KEY},
    metadata::STREAM_INFO,
    option::CONFIG,
    storage::ObjectStorage,
};

use super::{listing_table_builder::ListingTableBuilder, table_provider::QueryTableProvider};

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

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        if self.table_exist(name) {
            Some(Arc::new(StandardTableProvider {
                schema: STREAM_INFO.schema(name).unwrap(),
                stream: name.to_owned(),
                url: self.storage.store_url(),
            }))
        } else {
            None
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

impl StandardTableProvider {
    #[allow(clippy::too_many_arguments)]
    async fn remote_physical_plan(
        &self,
        glob_storage: Arc<dyn ObjectStorage + Send>,
        object_store: Arc<dyn ObjectStore>,
        snapshot: &catalog::snapshot::Snapshot,
        time_filters: &[PartialTimeFilter],
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
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

        if manifest_files.is_empty() {
            return Ok(None);
        }

        let (partitioned_files, statistics) = partitioned_files(manifest_files, &self.schema, 1);

        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            let table_df_schema = self.schema.as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(
                &expr,
                &table_df_schema,
                &self.schema,
                state.execution_props(),
            )?;
            Some(filters)
        } else {
            None
        };

        let sort_expr = PhysicalSortExpr {
            expr: physical_plan::expressions::col(DEFAULT_TIMESTAMP_KEY, &self.schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        };

        let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
        // create the execution plan
        let plan = file_format
            .create_physical_plan(
                state,
                FileScanConfig {
                    object_store_url: ObjectStoreUrl::parse(&glob_storage.store_url()).unwrap(),
                    file_schema: self.schema.clone(),
                    file_groups: partitioned_files,
                    statistics,
                    projection: projection.cloned(),
                    limit,
                    output_ordering: vec![vec![sort_expr]],
                    table_partition_cols: Vec::new(),
                    infinite_source: false,
                },
                filters.as_ref(),
            )
            .await?;

        Ok(Some(plan))
    }
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
                    null_count: None,
                    max_value: Some(max),
                    min_value: Some(min),
                    distinct_count: None,
                })
                .unwrap_or_default()
        })
        .collect();

    let statistics = datafusion::common::Statistics {
        num_rows: Some(count as usize),
        total_byte_size: None,
        column_statistics: Some(statistics),
        is_exact: true,
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let time_filters = extract_primary_filter(filters);
        if time_filters.is_empty() {
            return Err(DataFusionError::Plan("potentially unbounded query on time range. Table scanning requires atleast one time bound".to_string()));
        }

        let memtable = if include_now(filters) {
            event::STREAM_WRITERS.recordbatches_cloned(&self.stream, &self.schema)
        } else {
            None
        };

        let storage = state
            .runtime_env()
            .object_store_registry
            .get_store(&self.url)
            .unwrap();

        let glob_storage = CONFIG.storage().get_object_store();

        // Fetch snapshot
        let snapshot = glob_storage
            .get_snapshot(&self.stream)
            .await
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        let remote_table = if is_overlapping_query(&snapshot, &time_filters) {
            // Is query timerange is overlapping with older data.
            if let Some(table) = ListingTableBuilder::new(self.stream.clone())
                .populate_via_listing(glob_storage.clone(), storage, &time_filters)
                .await?
                .build(self.schema.clone(), |x| glob_storage.query_prefixes(x))?
            {
                Some(table.scan(state, projection, filters, limit).await?)
            } else {
                None
            }
        } else {
            self.remote_physical_plan(
                glob_storage,
                storage,
                &snapshot,
                &time_filters,
                projection,
                filters,
                limit,
                state,
            )
            .await?
        };

        QueryTableProvider::try_new(memtable, remote_table, self.schema.clone())?
            .scan(state, projection, filters, limit)
            .await
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        if expr_in_boundary(filter) {
            // if filter can be handled by time partiton pruning, it is exact
            Ok(TableProviderFilterPushDown::Exact)
        } else {
            // otherwise, we still might be able to handle the filter with file
            // level mechanisms such as Parquet row group pruning.
            Ok(TableProviderFilterPushDown::Inexact)
        }
    }
}

#[derive(Debug)]
pub enum PartialTimeFilter {
    Low(Bound<NaiveDateTime>),
    High(Bound<NaiveDateTime>),
    Eq(NaiveDateTime),
}

impl PartialTimeFilter {
    fn try_from_expr(expr: &Expr) -> Option<Self> {
        let Expr::BinaryExpr(binexpr) = expr else {
            return None;
        };
        let (op, time) = extract_timestamp_bound(binexpr)?;
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
                (Operator::Gt, time.timestamp_millis())
            }
            PartialTimeFilter::Low(Bound::Included(time)) => {
                (Operator::GtEq, time.timestamp_millis())
            }
            PartialTimeFilter::High(Bound::Excluded(time)) => {
                (Operator::Lt, time.timestamp_millis())
            }
            PartialTimeFilter::High(Bound::Included(time)) => {
                (Operator::LtEq, time.timestamp_millis())
            }
            PartialTimeFilter::Eq(time) => (Operator::Eq, time.timestamp_millis()),
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

    fn is_greater_than(&self, other: &NaiveDateTime) -> bool {
        match self {
            PartialTimeFilter::Low(Bound::Excluded(time)) => time >= other,
            PartialTimeFilter::Low(Bound::Included(time))
            | PartialTimeFilter::High(Bound::Excluded(time))
            | PartialTimeFilter::High(Bound::Included(time)) => time > other,
            PartialTimeFilter::Eq(time) => time > other,
            _ => unimplemented!(),
        }
    }
}

fn is_overlapping_query(
    snapshot: &catalog::snapshot::Snapshot,
    time_filters: &[PartialTimeFilter],
) -> bool {
    // This is for backwards compatiblity. Older table format relies on listing.
    // if time is lower than 2nd smallest time bound then we fall back to old listing table code for now.
    let Some(second_lowest) = snapshot
        .manifest_list
        .iter()
        .map(|file| file.time_lower_bound)
        .k_smallest(2)
        .nth(1)
    else {
        return true;
    };

    // Query is overlapping when no lower bound exists such that it is greater than second lowest time in snapshot
    !time_filters
        .iter()
        .all(|filter| filter.is_greater_than(&second_lowest.naive_utc()))
}

fn include_now(filters: &[Expr]) -> bool {
    let current_minute = Utc::now()
        .with_second(0)
        .and_then(|x| x.with_nanosecond(0))
        .expect("zeroed value is valid")
        .naive_utc();

    let time_filters = extract_primary_filter(filters);

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
    let Some((op, time)) = extract_timestamp_bound(binexpr) else {
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

fn extract_from_lit(expr: &Expr) -> Option<NaiveDateTime> {
    if let Expr::Literal(value) = expr {
        match value {
            ScalarValue::TimestampMillisecond(Some(value), _) => {
                Some(NaiveDateTime::from_timestamp_millis(*value).unwrap())
            }
            _ => None,
        }
    } else {
        None
    }
}

fn extract_timestamp_bound(binexpr: &BinaryExpr) -> Option<(Operator, NaiveDateTime)> {
    if matches!(&*binexpr.left, Expr::Column(Column { name, .. }) if name == DEFAULT_TIMESTAMP_KEY)
    {
        let time = extract_from_lit(&binexpr.right)?;
        Some((binexpr.op, time))
    } else {
        None
    }
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
fn extract_primary_filter(filters: &[Expr]) -> Vec<PartialTimeFilter> {
    let mut time_filters = Vec::new();
    filters.iter().for_each(|expr| {
        let _ = expr.apply(&mut |expr| {
            let time = PartialTimeFilter::try_from_expr(expr);
            if let Some(time) = time {
                time_filters.push(time);
                Ok(VisitRecursion::Stop)
            } else {
                Ok(VisitRecursion::Skip)
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
