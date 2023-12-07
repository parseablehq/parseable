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

use std::{any::Any, collections::HashMap, ops::Bound, pin::Pin, sync::Arc};

use arrow_schema::SchemaRef;
use bytes::Bytes;
use chrono::{NaiveDateTime, Timelike, Utc};
use datafusion::{
    catalog::schema::SchemaProvider,
    common::tree_node::{TreeNode, VisitRecursion},
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
    prelude::{col, Column, Expr},
    scalar::ScalarValue,
};
use futures_util::{
    future,
    stream::{FuturesOrdered, FuturesUnordered},
    Future, StreamExt, TryStreamExt,
};
use itertools::Itertools;
use object_store::{path::Path, ObjectMeta, ObjectStore};
use url::Url;

use crate::{
    catalog::{self, column::TypedStatistics, manifest::Manifest, ManifestFile, Snapshot},
    event::{self, DEFAULT_TIMESTAMP_KEY},
    metadata::STREAM_INFO,
    option::CONFIG,
    storage::{ObjectStorage, OBJECT_STORE_DATA_GRANULARITY},
    utils::TimePeriod,
};

use super::table_provider;

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
    async fn collect_remote_scan(
        &self,
        glob_storage: Arc<dyn ObjectStorage + Send>,
        object_store: Arc<dyn ObjectStore>,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Vec<ListingTableUrl>, DataFusionError> {
        let time_filters = extract_primary_filter(filters);
        if time_filters.is_empty() {
            return Err(DataFusionError::Plan("potentially unbounded query on time range. Table scanning requires atleast one time bound".to_string()));
        }

        // Fetch snapshot
        let snapshot = glob_storage
            .get_snapshot(&self.stream)
            .await
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        // Is query timerange is overlapping with older data.
        if is_overlapping_query(&snapshot, &time_filters) {
            return listing_scan(
                self.stream.clone(),
                glob_storage.clone(),
                object_store,
                &time_filters,
            )
            .await
            .map(|prefixes| glob_storage.query_prefixes(prefixes));
        }

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

        Ok(manifest_files
            .iter()
            .map(|x| glob_storage.store_url().join(&x.file_path).unwrap())
            .map(|x| ListingTableUrl::parse(x).unwrap())
            .collect())
    }

    fn remote_query(
        &self,
        prefixes: Vec<ListingTableUrl>,
    ) -> Result<Option<Arc<ListingTable>>, DataFusionError> {
        if prefixes.is_empty() {
            return Ok(None);
        }

        let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
        let file_sort_order = vec![vec![col(DEFAULT_TIMESTAMP_KEY).sort(true, false)]];
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_file_sort_order(file_sort_order)
            .with_collect_stat(true)
            .with_target_partitions(1);

        let config = ListingTableConfig::new_with_multi_paths(prefixes)
            .with_listing_options(listing_options)
            .with_schema(self.schema.clone());

        let listing_table = Arc::new(ListingTable::try_new(config)?);
        Ok(Some(listing_table))
    }
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

        let prefixes = self
            .collect_remote_scan(
                CONFIG.storage().get_object_store(),
                storage,
                projection,
                filters,
                limit,
            )
            .await?;

        table_provider::QueryTableProvider::try_new(
            memtable,
            self.remote_query(prefixes)?,
            self.schema(),
        )?
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

// accepts relative paths to resolve the narrative
// returns list of prefixes sorted in descending order
async fn listing_scan(
    stream: String,
    storage: Arc<dyn ObjectStorage + Send>,
    client: Arc<dyn ObjectStore>,
    time_filters: &[PartialTimeFilter],
) -> Result<Vec<String>, DataFusionError> {
    let start_time = time_filters
        .iter()
        .filter_map(|x| match x {
            PartialTimeFilter::Low(Bound::Excluded(x)) => Some(x),
            PartialTimeFilter::Low(Bound::Included(x)) => Some(x),
            _ => None,
        })
        .min()
        .cloned();

    let end_time = time_filters
        .iter()
        .filter_map(|x| match x {
            PartialTimeFilter::High(Bound::Excluded(x)) => Some(x),
            PartialTimeFilter::High(Bound::Included(x)) => Some(x),
            _ => None,
        })
        .max()
        .cloned();

    let Some((start_time, end_time)) = start_time.zip(end_time) else {
        return Err(DataFusionError::NotImplemented(
            "The time predicate is not supported because of possibly querying older data."
                .to_string(),
        ));
    };

    let prefixes = TimePeriod::new(
        start_time.and_utc(),
        end_time.and_utc(),
        OBJECT_STORE_DATA_GRANULARITY,
    )
    .generate_prefixes();

    let prefixes = prefixes
        .into_iter()
        .map(|entry| {
            let path = relative_path::RelativePathBuf::from(format!("{}/{}", stream, entry));
            storage.absolute_url(path.as_relative_path()).to_string()
        })
        .collect_vec();

    let mut minute_resolve: HashMap<String, Vec<String>> = HashMap::new();
    let mut all_resolve = Vec::new();

    for prefix in prefixes {
        let components = prefix.split_terminator('/');
        if components.last().is_some_and(|x| x.starts_with("minute")) {
            let hour_prefix = &prefix[0..prefix.rfind("minute").expect("minute exists")];
            minute_resolve
                .entry(hour_prefix.to_owned())
                .and_modify(|list| list.push(prefix))
                .or_default();
        } else {
            all_resolve.push(prefix)
        }
    }

    type ResolveFuture = Pin<
        Box<dyn Future<Output = Result<Vec<ObjectMeta>, object_store::Error>> + Send + 'static>,
    >;

    let tasks: FuturesUnordered<ResolveFuture> = FuturesUnordered::new();

    for (listing_prefix, prefix) in minute_resolve {
        let client = Arc::clone(&client);
        tasks.push(Box::pin(async move {
            let mut list = client
                .list(Some(&object_store::path::Path::from(listing_prefix)))
                .await?
                .try_collect::<Vec<_>>()
                .await?;

            list.retain(|object| {
                prefix.iter().any(|prefix| {
                    object
                        .location
                        .prefix_matches(&object_store::path::Path::from(prefix.as_ref()))
                })
            });

            Ok(list)
        }));
    }

    for prefix in all_resolve {
        let client = Arc::clone(&client);
        tasks.push(Box::pin(async move {
            client
                .list(Some(&object_store::path::Path::from(prefix)))
                .await?
                .try_collect::<Vec<_>>()
                .await
                .map_err(Into::into)
        }));
    }

    let res: Vec<Vec<String>> = tasks
        .and_then(|res| {
            future::ok(
                res.into_iter()
                    .map(|res| res.location.to_string())
                    .collect_vec(),
            )
        })
        .try_collect()
        .await?;

    let mut res = res.into_iter().flatten().collect_vec();
    res.sort();
    res.reverse();
    Ok(res)
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
