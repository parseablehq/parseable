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

use actix_web::Either;
use arrow_schema::SchemaRef;
use chrono::NaiveDateTime;
use chrono::{DateTime, Duration, Utc};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::Transformed;
use datafusion::execution::disk_manager::DiskManager;
use datafusion::execution::{
    RecordBatchStream, SendableRecordBatchStream, SessionState, SessionStateBuilder,
};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    Aggregate, Explain, Filter, LogicalPlan, PlanType, Projection, ToStringifiedPlan,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion::sql::parser::DFParser;
use datafusion::sql::resolve::resolve_table_references;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use futures::Stream;
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use sysinfo::System;
use tokio::runtime::Runtime;

use self::error::ExecuteError;
use self::stream_schema_provider::GlobalSchemaProvider;
pub use self::stream_schema_provider::PartialTimeFilter;
use crate::alerts::alert_structs::Conditions;
use crate::alerts::alerts_utils::get_filter_string;
use crate::catalog::Snapshot as CatalogSnapshot;
use crate::catalog::column::{Int64Type, TypedStatistics};
use crate::catalog::manifest::Manifest;
use crate::catalog::snapshot::Snapshot;
use crate::event::DEFAULT_TIMESTAMP_KEY;
use crate::handlers::http::query::QueryError;
use crate::metrics::increment_bytes_scanned_in_query_by_date;
use crate::option::Mode;
use crate::parseable::PARSEABLE;
use crate::storage::{ObjectStorageProvider, ObjectStoreFormat};
use crate::utils::time::TimeRange;

pub static QUERY_SESSION: Lazy<SessionContext> =
    Lazy::new(|| Query::create_session_context(PARSEABLE.storage()));

pub static QUERY_SESSION_STATE: Lazy<SessionState> =
    Lazy::new(|| Query::create_session_state(PARSEABLE.storage()));

/// Dedicated multi-threaded runtime to run all queries on
pub static QUERY_RUNTIME: Lazy<Runtime> =
    Lazy::new(|| Runtime::new().expect("Runtime should be constructible"));

/// This function executes a query on the dedicated runtime, ensuring that the query is not isolated to a single thread/CPU
/// at a time and has access to the entire thread pool, enabling better concurrent processing, and thus quicker results.
pub async fn execute(
    query: Query,
    is_streaming: bool,
) -> Result<
    (
        Either<Vec<RecordBatch>, Pin<Box<MetricMonitorStream>>>,
        Vec<String>,
    ),
    ExecuteError,
> {
    QUERY_RUNTIME
        .spawn(async move { query.execute(is_streaming).await })
        .await
        .expect("The Join should have been successful")
}

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
        let state = Self::create_session_state(storage.clone());

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

    fn create_session_state(storage: Arc<dyn ObjectStorageProvider>) -> SessionState {
        let runtime_config = storage
            .get_datafusion_runtime()
            .with_disk_manager_builder(DiskManager::builder());
        let (pool_size, fraction) = match PARSEABLE.options.query_memory_pool_size {
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

        // All the config options are explained here -
        // https://datafusion.apache.org/user-guide/configs.html
        let mut config = SessionConfig::default()
            .with_parquet_pruning(true)
            .with_prefer_existing_sort(true)
            //batch size has been made configurable via environment variable
            //default value is 20000
            .with_batch_size(PARSEABLE.options.execution_batch_size);

        // Pushdown filters allows DF to push the filters as far down in the plan as possible
        // and thus, reducing the number of rows decoded
        config.options_mut().execution.parquet.pushdown_filters = true;

        // Reorder filters allows DF to decide the order of filters minimizing the cost of filter evaluation
        config.options_mut().execution.parquet.reorder_filters = true;
        config.options_mut().execution.parquet.binary_as_string = true;
        config
            .options_mut()
            .execution
            .use_row_number_estimates_to_optimize_partitioning = true;

        //adding this config as it improves query performance as explained here -
        // https://github.com/apache/datafusion/pull/13101
        config
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = true;

        SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime)
            .build()
    }

    /// this function returns the result of the query
    /// if streaming is true, it returns a stream
    /// if streaming is false, it returns a vector of record batches
    pub async fn execute(
        &self,
        is_streaming: bool,
    ) -> Result<
        (
            Either<Vec<RecordBatch>, Pin<Box<MetricMonitorStream>>>,
            Vec<String>,
        ),
        ExecuteError,
    > {
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

        if fields.is_empty() && !is_streaming {
            return Ok((Either::Left(vec![]), fields));
        }

        let plan = QUERY_SESSION
            .state()
            .create_physical_plan(df.logical_plan())
            .await?;

        let results = if !is_streaming {
            let task_ctx = QUERY_SESSION.task_ctx();

            let stream = plan.execute(0, task_ctx)?;
            let batches = datafusion::physical_plan::common::collect(stream).await?;

            let actual_io_bytes = get_total_bytes_scanned(&plan);

            // Track billing metrics for query scan
            let current_date = chrono::Utc::now().date_naive().to_string();
            increment_bytes_scanned_in_query_by_date(actual_io_bytes, &current_date);

            Either::Left(batches)
        } else {
            let task_ctx = QUERY_SESSION.task_ctx();

            let stream = plan.execute(0, task_ctx)?;

            let monitored_stream = MetricMonitorStream::new(stream, plan.clone());

            Either::Right(Box::pin(monitored_stream))
        };

        Ok((results, fields))
    }

    pub async fn get_dataframe(&self) -> Result<DataFrame, ExecuteError> {
        let df = QUERY_SESSION
            .execute_logical_plan(self.final_logical_plan())
            .await?;

        Ok(df)
    }

    /// return logical plan with all time filters applied through
    fn final_logical_plan(&self) -> LogicalPlan {
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
                );
                LogicalPlan::Explain(Explain {
                    explain_format: plan.explain_format,
                    verbose: plan.verbose,
                    stringified_plans: vec![
                        transformed
                            .data
                            .to_stringified(PlanType::InitialLogicalPlan),
                    ],
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
                )
                .data
            }
        }
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
        if !matches!(&**input, LogicalPlan::TableScan { .. }) || expr.len() != 1 {
            return None;
        }

        // Check if the expression is a column or an alias for COUNT(*)
        match &expr[0] {
            // Direct column check
            Expr::Column(Column { name, .. }) if name.to_lowercase() == "count(*)" => Some(name),
            // Alias for COUNT(*)
            Expr::Alias(Alias {
                expr: inner_expr,
                name: alias_name,
                ..
            }) => {
                if let Expr::Column(Column { name, .. }) = &**inner_expr
                    && name.to_lowercase() == "count(*)"
                {
                    return Some(alias_name);
                }
                None
            }
            // Unsupported expression type
            _ => None,
        }
    }
}

/// Recursively sums up "bytes_scanned" from all nodes in the plan
fn get_total_bytes_scanned(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    let mut total_bytes = 0;

    if let Some(metrics) = plan.metrics() {
        // "bytes_scanned" is the standard key used by ParquetExec
        if let Some(scanned) = metrics.sum_by_name("bytes_scanned") {
            total_bytes += scanned.as_usize() as u64;
        }
    }

    for child in plan.children() {
        total_bytes += get_total_bytes_scanned(child);
    }

    total_bytes
}

/// Record of counts for a given time bin.
#[derive(Debug, Serialize, Clone, Deserialize)]
pub struct CountsRecord {
    #[serde(alias = "_bin_start_time_")]
    /// Start time of the bin
    pub start_time: String,
    #[serde(alias = "_bin_end_time_")]
    /// End time of the bin
    pub end_time: String,
    /// Number of logs in the bin
    pub count: u64,
}

struct TimeBounds {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CountConditions {
    /// Optional conditions for filters
    pub conditions: Option<Conditions>,
    /// GroupBy columns
    pub group_by: Option<Vec<String>>,
}

/// Request for counts, received from API/SQL query.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CountsRequest {
    /// Name of the stream to get counts for
    pub stream: String,
    /// Included start time for counts query
    pub start_time: String,
    /// Excluded end time for counts query
    pub end_time: String,
    /// optional number of bins to divide the time range into
    pub num_bins: Option<u64>,
    /// Conditions
    pub conditions: Option<CountConditions>,
}

impl CountsRequest {
    /// This function is supposed to read maninfest files for the given stream,
    /// get the sum of `num_rows` between the `startTime` and `endTime`,
    /// divide that by number of bins and return in a manner acceptable for the console
    pub async fn get_bin_density(&self) -> Result<Vec<CountsRecord>, QueryError> {
        let time_partition = PARSEABLE
            .get_stream(&self.stream)
            .map_err(|err| anyhow::Error::msg(err.to_string()))?
            .get_time_partition()
            .unwrap_or_else(|| DEFAULT_TIMESTAMP_KEY.to_owned());

        // get time range
        let time_range = TimeRange::parse_human_time(&self.start_time, &self.end_time)?;
        let all_manifest_files = get_manifest_list(&self.stream, &time_range).await?;
        // get bounds
        let counts = self.get_bounds(&time_range);

        // we have start and end times for each bin
        // we also have all the manifest files for the given time range
        // now we iterate over start and end times for each bin
        // then we iterate over the manifest files which are within that time range
        // we sum up the num_rows
        let mut counts_records = Vec::new();

        for bin in counts {
            // extract start and end time to compare
            // Sum up the number of rows that fall within the bin
            let count: u64 = all_manifest_files
                .iter()
                .flat_map(|m| &m.files)
                .filter_map(|f| {
                    if f.columns.iter().any(|c| {
                        c.name == time_partition
                            && c.stats.as_ref().is_some_and(|stats| match stats {
                                TypedStatistics::Int(Int64Type { min, .. }) => {
                                    let min = DateTime::from_timestamp_millis(*min).unwrap();
                                    bin.start <= min && bin.end >= min // Determines if a column matches the bin's time range.
                                }
                                _ => false,
                            })
                    }) {
                        Some(f.num_rows)
                    } else {
                        None
                    }
                })
                .sum();

            counts_records.push(CountsRecord {
                start_time: bin.start.to_rfc3339(),
                end_time: bin.end.to_rfc3339(),
                count,
            });
        }
        Ok(counts_records)
    }

    /// Calculate the end time for each bin based on the number of bins
    fn get_bounds(&self, time_range: &TimeRange) -> Vec<TimeBounds> {
        let total_minutes = time_range
            .end
            .signed_duration_since(time_range.start)
            .num_minutes() as u64;

        let num_bins = if let Some(num_bins) = self.num_bins {
            num_bins
        } else {
            // create number of bins based on total minutes
            if total_minutes <= 60 * 5 {
                // till 5 hours, 1 bin = 1 min
                total_minutes
            } else if total_minutes <= 60 * 24 {
                // till 1 day, 1 bin = 5 min
                total_minutes.div_ceil(5)
            } else if total_minutes <= 60 * 24 * 10 {
                // till 10 days, 1 bin = 1 hour
                total_minutes.div_ceil(60)
            } else {
                // > 10 days, 1 bin = 1 day
                total_minutes.div_ceil(1440)
            }
        };

        // divide minutes by num bins to get minutes per bin
        let quotient = total_minutes / num_bins;
        let remainder = total_minutes % num_bins;
        let have_remainder = remainder > 0;

        // now create multiple bounds [startTime, endTime)
        // Should we exclude the last one???
        let mut bounds = vec![];

        let mut start = time_range.start;

        let loop_end = if have_remainder {
            num_bins
        } else {
            num_bins - 1
        };

        // Create bins for all but the last date
        for _ in 0..loop_end {
            let end = start + Duration::minutes(quotient as i64);
            bounds.push(TimeBounds { start, end });
            start = end;
        }

        // Add the last bin, accounting for any remainder, should we include it?
        if have_remainder {
            bounds.push(TimeBounds {
                start,
                end: start + Duration::minutes(remainder as i64),
            });
        } else {
            bounds.push(TimeBounds {
                start,
                end: start + Duration::minutes(quotient as i64),
            });
        }

        bounds
    }

    /// This function will get executed only if self.conditions is some
    pub async fn get_df_sql(&self, time_column: String) -> Result<String, QueryError> {
        // unwrap because we have asserted that it is some
        let count_conditions = self.conditions.as_ref().unwrap();

        let time_range = TimeRange::parse_human_time(&self.start_time, &self.end_time)?;

        let dur = time_range.end.signed_duration_since(time_range.start);

        let table_name = &self.stream;
        let start_time_col_name = "_bin_start_time_";
        let end_time_col_name = "_bin_end_time_";
        let date_bin = if dur.num_minutes() <= 60 * 5 {
            // less than 5 hour = 1 min bin
            format!(
                "CAST(DATE_BIN('1m', \"{table_name}\".\"{time_column}\", TIMESTAMP '1970-01-01 00:00:00+00') AS TEXT) as {start_time_col_name}, DATE_BIN('1m', \"{table_name}\".\"{time_column}\", TIMESTAMP '1970-01-01 00:00:00+00') + INTERVAL '1m' as {end_time_col_name}"
            )
        } else if dur.num_minutes() <= 60 * 24 {
            // 1 day = 5 min bin
            format!(
                "CAST(DATE_BIN('5m', \"{table_name}\".\"{time_column}\", TIMESTAMP '1970-01-01 00:00:00+00') AS TEXT) as {start_time_col_name}, DATE_BIN('5m', \"{table_name}\".\"{time_column}\", TIMESTAMP '1970-01-01 00:00:00+00') + INTERVAL '5m' as {end_time_col_name}"
            )
        } else if dur.num_minutes() < 60 * 24 * 10 {
            // 10 days = 1 hour bin
            format!(
                "CAST(DATE_BIN('1h', \"{table_name}\".\"{time_column}\", TIMESTAMP '1970-01-01 00:00:00+00') AS TEXT) as {start_time_col_name}, DATE_BIN('1h', \"{table_name}\".\"{time_column}\", TIMESTAMP '1970-01-01 00:00:00+00') + INTERVAL '1h' as {end_time_col_name}"
            )
        } else {
            // 1 day
            format!(
                "CAST(DATE_BIN('1d', \"{table_name}\".\"{time_column}\", TIMESTAMP '1970-01-01 00:00:00+00') AS TEXT) as {start_time_col_name}, DATE_BIN('1d', \"{table_name}\".\"{time_column}\", TIMESTAMP '1970-01-01 00:00:00+00') + INTERVAL '1d' as {end_time_col_name}"
            )
        };

        let query = if let Some(conditions) = &count_conditions.conditions {
            let f = get_filter_string(conditions).map_err(QueryError::CustomError)?;
            format!(
                "SELECT {date_bin}, COUNT(*) as count FROM \"{table_name}\" WHERE {} GROUP BY {end_time_col_name},{start_time_col_name} ORDER BY {end_time_col_name}",
                f
            )
        } else {
            format!(
                "SELECT {date_bin}, COUNT(*) as count FROM \"{table_name}\" GROUP BY {end_time_col_name},{start_time_col_name} ORDER BY {end_time_col_name}",
            )
        };
        Ok(query)
    }
}

/// Response for the counts API
#[derive(Debug, Serialize, Clone, Deserialize)]
pub struct CountsResponse {
    /// Fields in the log stream
    pub fields: Vec<String>,
    /// Records in the response
    pub records: Vec<CountsRecord>,
}

pub fn resolve_stream_names(sql: &str) -> Result<Vec<String>, anyhow::Error> {
    let normalized_sql = sql.replace('`', "\"");
    let dialect = &PostgreSqlDialect {};
    let statement = DFParser::parse_sql_with_dialect(&normalized_sql, dialect)?
        .pop_back()
        .ok_or(anyhow::anyhow!("Failed to parse sql"))?;
    let (table_refs, _) = resolve_table_references(&statement, true)?;
    let mut tables = Vec::new();
    for table in table_refs {
        tables.push(table.table().to_string());
    }
    Ok(tables)
}

pub async fn get_manifest_list(
    stream_name: &str,
    time_range: &TimeRange,
) -> Result<Vec<Manifest>, QueryError> {
    // get object store
    let object_store_format: ObjectStoreFormat = serde_json::from_slice(
        &PARSEABLE
            .metastore
            .get_stream_json(stream_name, false)
            .await?,
    )?;

    // all the manifests will go here
    let mut merged_snapshot: Snapshot = Snapshot::default();

    // get a list of manifests
    if PARSEABLE.options.mode == Mode::Query || PARSEABLE.options.mode == Mode::Prism {
        let obs = PARSEABLE
            .metastore
            .get_all_stream_jsons(stream_name, None)
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

    let mut all_manifest_files = Vec::new();
    for manifest_item in merged_snapshot.manifests(&time_filter) {
        let manifest_opt = PARSEABLE
            .metastore
            .get_manifest(
                stream_name,
                manifest_item.time_lower_bound,
                manifest_item.time_upper_bound,
                Some(manifest_item.manifest_path.clone()),
            )
            .await?;
        let manifest = manifest_opt.ok_or_else(|| {
            QueryError::CustomError(format!(
                "Manifest not found for {stream_name} [{} - {}], path- {}",
                manifest_item.time_lower_bound,
                manifest_item.time_upper_bound,
                manifest_item.manifest_path
            ))
        })?;
        all_manifest_files.push(manifest);
    }

    Ok(all_manifest_files)
}

fn transform(
    plan: LogicalPlan,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
) -> Transformed<LogicalPlan> {
    plan.transform_up_with_subqueries(&|plan| {
        match plan {
            LogicalPlan::TableScan(table) => {
                // Get the specific time partition for this stream
                let time_partition = PARSEABLE
                    .get_stream(&table.table_name.to_string())
                    .ok()
                    .and_then(|stream| stream.get_time_partition());

                let mut new_filters = vec![];
                if !table_contains_any_time_filters(&table, time_partition.as_ref()) {
                    let default_timestamp = DEFAULT_TIMESTAMP_KEY.to_string();
                    let time_column = time_partition.as_ref().unwrap_or(&default_timestamp);

                    // Create time filters with table-qualified column names
                    let start_time_filter = PartialTimeFilter::Low(std::ops::Bound::Included(
                        start_time,
                    ))
                    .binary_expr(Expr::Column(Column::new(
                        Some(table.table_name.to_owned()),
                        time_column.clone(),
                    )));

                    let end_time_filter = PartialTimeFilter::High(std::ops::Bound::Excluded(
                        end_time,
                    ))
                    .binary_expr(Expr::Column(Column::new(
                        Some(table.table_name.to_owned()),
                        time_column.clone(),
                    )));

                    new_filters.push(start_time_filter);
                    new_filters.push(end_time_filter);
                }

                let new_filter = new_filters.into_iter().reduce(and);
                if let Some(new_filter) = new_filter {
                    let filter =
                        Filter::try_new(new_filter, Arc::new(LogicalPlan::TableScan(table)))
                            .unwrap();
                    Ok(Transformed::yes(LogicalPlan::Filter(filter)))
                } else {
                    Ok(Transformed::no(LogicalPlan::TableScan(table)))
                }
            }
            _ => {
                // For all other plan types, continue the transformation recursively
                // This ensures that subqueries and other nested plans are also transformed
                Ok(Transformed::no(plan))
            }
        }
    })
    .expect("transform processes all plan nodes")
}

fn table_contains_any_time_filters(
    table: &datafusion::logical_expr::TableScan,
    time_partition: Option<&String>,
) -> bool {
    let default_timestamp = DEFAULT_TIMESTAMP_KEY.to_string();
    let time_column = time_partition.unwrap_or(&default_timestamp);

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
            if name == &default_timestamp || name == time_column)
        })
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
    use crate::{parseable::StreamNotFound, storage::ObjectStorageError};
    use datafusion::error::DataFusionError;

    #[derive(Debug, thiserror::Error)]
    pub enum ExecuteError {
        #[error("Query Execution failed due to error in object storage: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
        #[error("Query Execution failed due to error in datafusion: {0}")]
        Datafusion(#[from] DataFusionError),
        #[error("{0}")]
        StreamNotFound(#[from] StreamNotFound),
    }
}

/// A wrapper that monitors the ExecutionPlan and logs metrics when the stream finishes.
pub struct MetricMonitorStream {
    // The actual stream doing the work
    inner: SendableRecordBatchStream,
    // We hold the plan so we can read metrics after execution
    plan: Arc<dyn ExecutionPlan>,
}

impl MetricMonitorStream {
    pub fn new(inner: SendableRecordBatchStream, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { inner, plan }
    }
}

impl Stream for MetricMonitorStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.as_mut().poll_next(cx);

        // Check if the stream just finished
        match &poll {
            Poll::Ready(None) => {
                // Stream is done. Now we can safely read the metrics.
                let bytes = get_total_bytes_scanned(&self.plan);
                let current_date = chrono::Utc::now().date_naive().to_string();
                increment_bytes_scanned_in_query_by_date(bytes, &current_date);
            }
            Poll::Ready(Some(Err(e))) => {
                let bytes = get_total_bytes_scanned(&self.plan);
                let current_date = chrono::Utc::now().date_naive().to_string();
                increment_bytes_scanned_in_query_by_date(bytes, &current_date);
                tracing::error!("Stream Failed with error: {}", e);
            }
            _ => {}
        }

        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl RecordBatchStream for MetricMonitorStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::query::flatten_objects_for_count;

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
