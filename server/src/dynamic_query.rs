use crate::handlers::http::query::QueryError;
use crate::query::QUERY_SESSION;
use crate::{query::Query, response::QueryResponse};
use anyhow::anyhow;
use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{Field, Fields, Schema};
use bytes::Bytes;
use chrono::Utc;
use clokwerk::AsyncScheduler;
use datafusion::arrow::datatypes::ToByteSlice;
use datafusion::common::{Column, DFSchema};
use datafusion::logical_expr::{
    Aggregate, CrossJoin, Join, Limit, LogicalPlan, Projection, RecursiveQuery, Subquery,
    SubqueryAlias, TableScan, Union, Unnest,
};
use datafusion::prelude::Expr;
use datafusion_proto::bytes::{logical_plan_from_bytes, Serializeable};
use futures::{TryFutureExt, TryStreamExt};
use itertools::Itertools;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::errors::ParquetError;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::io::Cursor;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{env, path::PathBuf, u32};
use std::{fs, io};
use tokio::fs as AsyncFs;
use tokio::io as AsyncIo;
use tokio::task;
use ulid::Ulid;

async fn write_opt_exprs<W>(expr: Option<&Expr>, writer: &mut W) -> io::Result<()>
where
    W: AsyncIo::AsyncWrite + AsyncIo::AsyncWriteExt + Unpin,
{
    match expr {
        None => writer.write_u8(0).await?,
        Some(v) => writer.write(&v.to_bytes().unwrap()).map_ok(|_| ()).await?,
    };
    Ok(())
}
async fn write_expr_refs<W>(exprs: &[&Expr], writer: &mut W) -> io::Result<()>
where
    W: AsyncIo::AsyncWrite + AsyncIo::AsyncWriteExt + Unpin,
{
    writer.write_u16(exprs.len() as u16).await?;
    for expr in exprs {
        writer.write(&expr.to_bytes().unwrap()).await?;
    }
    Ok(())
}
async fn write_exprs<W>(exprs: &[Expr], writer: &mut W) -> io::Result<()>
where
    W: AsyncIo::AsyncWrite + AsyncIo::AsyncWriteExt + Unpin,
{
    write_expr_refs(&exprs.iter().collect::<Vec<&Expr>>(), writer).await
}
async fn write_bool<W>(value: bool, writer: &mut W) -> io::Result<()>
where
    W: AsyncIo::AsyncWrite + AsyncIo::AsyncWriteExt + Unpin,
{
    writer.write_u8(if value { 1 } else { 0 }).await
}
async fn write_str<W>(value: &str, writer: &mut W) -> io::Result<()>
where
    W: AsyncIo::AsyncWrite + AsyncIo::AsyncWriteExt + Unpin,
{
    writer.write_u64(value.len() as u64).await?;
    writer.write_all(value.as_bytes()).await
}
async fn write_schema<W>(value: &DFSchema, writer: &mut W) -> io::Result<()>
where
    W: AsyncIo::AsyncWrite + AsyncIo::AsyncWriteExt + Unpin,
{
    let cols = value.columns();
    writer.write_u16(cols.len() as u16).await?;
    for Column { name, relation } in cols {
        write_str(&name, writer).await?;
    }

    Ok(())
}

async fn logical_plan_to_bytes(plan: &LogicalPlan) -> io::Result<Bytes> {
    let bytes = Vec::<u8>::with_capacity(8);
    let mut cursor = Cursor::new(bytes);
    write_logical_plan(plan, &mut cursor).await?;

    Ok(cursor.get_ref().to_vec().into())
}
fn write_logical_plan<'a, W>(
    plan: &'a LogicalPlan,
    writer: &'a mut W,
) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>>
where
    W: AsyncIo::AsyncWrite + AsyncIo::AsyncWriteExt + Unpin,
{
    Box::pin(async move {
        match plan {
            LogicalPlan::Aggregate(Aggregate {
                aggr_expr,
                group_expr,
                input,
                schema,
                ..
            }) => {
                writer.write_u8(0).await?;
                write_exprs(&aggr_expr, writer).await?;
                write_exprs(&group_expr, writer).await?;
                write_logical_plan(input, writer).await?;
                write_schema(&schema, writer).await?;
            }
            LogicalPlan::Analyze(_) => unimplemented!(),
            LogicalPlan::Copy(_) => unimplemented!(),
            LogicalPlan::CrossJoin(CrossJoin {
                left,
                right,
                schema,
            }) => {
                writer.write_u8(3).await?;
                write_logical_plan(&left, writer).await?;
                write_logical_plan(&right, writer).await?;
            }
            LogicalPlan::Ddl(_) => unimplemented!(),
            LogicalPlan::DescribeTable(_) => unimplemented!(),
            LogicalPlan::Distinct(d) => {
                writer.write_u8(6).await?;
                write_logical_plan(d.input(), writer).await?;
            }
            LogicalPlan::Dml(_) => unimplemented!(),
            LogicalPlan::EmptyRelation(v) => writer.write_u8(8).await?,
            LogicalPlan::Explain(_) => unimplemented!(),
            LogicalPlan::Extension(_) => unimplemented!(),
            LogicalPlan::Filter(f) => {
                writer.write_u8(11).await?;
                write_logical_plan(&f.input, writer).await?;
                writer.write(&f.predicate.to_bytes().unwrap()).await?;
            }
            LogicalPlan::Join(Join {
                filter,
                join_constraint,
                join_type,
                left,
                right,
                null_equals_null,
                on,
                schema,
                ..
            }) => {
                writer.write_u8(12).await?;
                write_schema(schema, writer).await?;
                write_opt_exprs(filter.as_ref(), writer).await?;
                write_bool(*null_equals_null, writer).await?;

                write_logical_plan(&left, writer).await?;
                write_logical_plan(&right, writer).await?;

                writer.write_u8(on.len() as u8).await?;
                for (a, b) in on {
                    write_expr_refs(&[a, b], writer).await?;
                }
            }
            LogicalPlan::Limit(Limit { fetch, input, skip }) => {
                writer.write_u8(13).await?;
                match fetch {
                    None => writer.write_u8(0).await?,
                    Some(v) => {
                        writer.write_u8(1).await?;
                        writer.write_u64(*v as u64).await?;
                    }
                }
                write_logical_plan(&input, writer).await?;
                writer.write_u64(*skip as u64).await?;
            }
            LogicalPlan::Prepare(_) => unimplemented!(),
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                ..
            }) => {
                writer.write_u8(14).await?;
                write_expr_refs(&expr.iter().collect::<Vec<&Expr>>(), writer).await?;
                write_logical_plan(&input, writer).await?;
                write_schema(&schema, writer).await?;
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                is_distinct,
                name,
                recursive_term,
                static_term,
            }) => {
                writer.write_u8(15).await?;

                write_bool(*is_distinct, writer).await?;
                write_str(&name, writer).await?;
                write_logical_plan(&recursive_term, writer).await?;
                write_logical_plan(&static_term, writer).await?;
            }
            LogicalPlan::Repartition(_) => unimplemented!(),
            LogicalPlan::Sort(s) => {
                writer.write_u8(17).await?;
                write_logical_plan(&s.input, writer).await?;
                match s.fetch {
                    None => writer.write_u8(0).await?,
                    Some(v) => {
                        writer.write_u8(1).await?;
                        writer.write_u64(v as u64).await?;
                    }
                };
            }
            LogicalPlan::Statement(stmt) => {
                writer.write_u8(18).await?;
                write_str(stmt.name(), writer).await?;
                write_schema(stmt.schema(), writer).await?;
            }
            LogicalPlan::Subquery(Subquery {
                outer_ref_columns,
                subquery,
            }) => {
                writer.write_u8(19).await?;
                write_logical_plan(&subquery, writer).await?;
                write_expr_refs(&outer_ref_columns.iter().collect::<Vec<&Expr>>(), writer).await?;
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                alias,
                input,
                schema,
                ..
            }) => {
                writer.write_u8(20).await?;
                write_logical_plan(&input, writer).await?;
                write_schema(&schema, writer).await?;
                write_str(alias.table(), writer).await?;
            }
            LogicalPlan::TableScan(TableScan {
                fetch,
                filters,
                projected_schema,
                projection,
                source,
                table_name,
            }) => {
                writer.write_u8(21).await?;
            }
            LogicalPlan::Union(Union { inputs, schema }) => {
                writer.write_u8(22).await?;
                writer.write_u32(inputs.len() as u32).await?;
                for input in inputs {
                    write_logical_plan(input, writer).await?;
                }
                write_schema(schema, writer).await?;
            }
            LogicalPlan::Unnest(Unnest {
                struct_type_columns,
                dependency_indices,
                exec_columns,
                input,
                list_type_columns,
                options,
                schema,
            }) => {
                writer.write_u8(23).await?;
                write_schema(schema, writer).await?;
            }
            LogicalPlan::Values(vals) => {
                writer.write_u8(24).await?;
                write_schema(&vals.schema, writer).await?;
            }
            LogicalPlan::Window(w) => {
                writer.write_u8(25).await?;
                write_logical_plan(&w.input, writer).await?;
                write_exprs(&w.window_expr, writer).await?;
                write_schema(&w.schema, writer).await?;
            }
        };
        Ok(())
    })
}
const MAX_SERVER_URL_STORES: usize = 10;
const DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV: &str = "DYNAMIC_QUERY_RESULTS_CACHE_PATH";

/// Query Request through http endpoint.
#[derive(Debug, Clone)]
pub struct DynamicQuery {
    pub plan: LogicalPlan,
    pub cache_duration: Duration,
}
fn load_env(name: &str) -> Result<String, QueryError> {
    env::var(name).map_err(|_| QueryError::Anyhow(anyhow!("Missing environment variable: {name}")))
}

fn parquet_to_err(err: ParquetError) -> QueryError {
    QueryError::Anyhow(anyhow!("Parquet error: {err}"))
}
fn io_to_err(err: io::Error) -> QueryError {
    QueryError::Anyhow(anyhow!("IO error: {err}"))
}
fn load_env_path(name: &str) -> Result<PathBuf, QueryError> {
    let txt = load_env(name)?;
    PathBuf::from_str(txt.as_str())
        .map_err(|path_err| QueryError::Anyhow(anyhow!("Path parse error: {path_err}")))
}

fn load_cache_path() -> Result<PathBuf, QueryError> {
    load_env_path(DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV)
}
fn load_plans_path() -> Result<PathBuf, QueryError> {
    let mut curr = load_cache_path()?;
    curr.push("queries");
    Ok(curr)
}

pub async fn clear() -> anyhow::Result<()> {
    if let Ok(cache_path) = load_cache_path() {
        if AsyncFs::try_exists(&cache_path).await? {
            log::info!("Clearing old dynamic cache files");
            let mut total: u32 = 0;
            let mut dirs = AsyncFs::read_dir(&cache_path).await?;
            while let Some(entry) = dirs.next_entry().await? {
                let path = entry.path();
                if path.extension() != Some(OsStr::new("parquet")) {
                    continue;
                }
                total += 1;
                AsyncFs::remove_file(path).await?;
            }
            log::info!("Cleared old dynamic cache files: {}", total);
        }
    } else {
        log::warn!("No env var found for {DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV}");
    }
    Ok(())
}

fn resolve_uuid_cache_path(uuid: Ulid) -> Result<PathBuf, QueryError> {
    let curr = load_cache_path()?;
    Ok(curr.join(format!("{}.parquet", uuid)))
}

fn resolve_uuid_query_path(uuid: Ulid) -> Result<PathBuf, QueryError> {
    let curr = load_plans_path()?;
    Ok(curr.join(format!("{}.parquet", uuid)))
}

pub async fn register_query(uuid: Ulid, query: DynamicQuery) -> Result<(), QueryError> {
    let plans_path: PathBuf = load_plans_path()?;
    let curr_plan_path = resolve_uuid_query_path(uuid)?;
    AsyncFs::create_dir_all(&plans_path)
        .await
        .map_err(io_to_err)?;
    let mut query_files = AsyncFs::read_dir(&plans_path).await.map_err(io_to_err)?;
    let mut total_queries = 0usize;

    while let Some(_) = query_files.next_entry().await.map_err(io_to_err)? {
        total_queries += 1;
    }
    if total_queries + 1 == MAX_SERVER_URL_STORES {
        return Err(QueryError::Anyhow(anyhow!(
            "Total dynamic queries would be over limit: {}",
            MAX_SERVER_URL_STORES
        )));
    }

    let plan_bytes = logical_plan_to_bytes(&query.plan)
        .await
        .map_err(io_to_err)?;
    let plan_parquet_file = AsyncFs::File::create(&curr_plan_path)
        .await
        .map_err(io_to_err)?;

    let fields_raw = [
        Arc::new(Field::new("bytes", arrow_schema::DataType::Binary, false)),
        Arc::new(Field::new(
            "cache_duration_mins",
            arrow_schema::DataType::UInt8,
            false,
        )),
    ];

    let sch = Arc::new(Schema {
        fields: Fields::from_iter(fields_raw),
        metadata: HashMap::new(),
    });

    let mut arrow_writer =
        AsyncArrowWriter::try_new(plan_parquet_file, sch, None).map_err(parquet_to_err)?;
    let a: ArrayRef = Arc::new(UInt32Array::from(vec![
        query.cache_duration.as_secs() as u32
    ]));
    let b: ArrayRef = Arc::new(StringArray::from(vec![Some(unsafe {
        String::from_utf8_unchecked(plan_bytes.to_vec())
    })]));

    let record_batch = RecordBatch::try_from_iter(vec![("bytes", b), ("cache_duration_mins", a)])
        .map_err(|err| QueryError::Anyhow(anyhow!("{err}")))?;
    arrow_writer.write(&record_batch).await.unwrap();

    task::spawn(async move {
        log::info!("Fetching initial dynamic query: {uuid}");
        process_dynamic_query(uuid, &query).await.unwrap();
        log::info!("Fetched initial dynamic query: {uuid}");
    });
    Ok(())
}

pub async fn load(uuid: Ulid) -> anyhow::Result<QueryResponse, QueryError> {
    let path = resolve_uuid_cache_path(uuid)?;

    let file = AsyncFs::File::open(path).await.map_err(io_to_err)?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(parquet_to_err)?;
    // Build a async parquet reader.
    let stream = builder.build().map_err(parquet_to_err)?;

    let records = stream
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(parquet_to_err)?;
    let fields = records.first().map_or_else(Vec::new, |record| {
        record
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .cloned()
            .collect_vec()
    });

    Ok(QueryResponse {
        fields,
        records,
        fill_null: false,
        with_fields: true,
    })
}
async fn load_query(cache_duration: chrono::Duration, plan: LogicalPlan) -> QueryResponse {
    let curr = Utc::now();

    let query = Query {
        start: curr - cache_duration,
        end: curr,
        raw_logical_plan: plan.clone(),
        filter_tag: None,
    };
    let table_name = query
        .first_table_name()
        .expect("No table name found in query");
    let (records, fields) = query.execute(table_name.clone()).await.unwrap();
    QueryResponse {
        records,
        fields,
        fill_null: false,
        with_fields: true,
    }
}

async fn process_dynamic_query(uuid: Ulid, query: &DynamicQuery) -> Result<(), QueryError> {
    log::info!("Reloading dynamic query {uuid}: {:?}", query);
    let curr = load_query(
        chrono::Duration::from_std(query.cache_duration).unwrap(),
        query.plan.clone(),
    )
    .await;

    let cached_path = resolve_uuid_cache_path(uuid)?;
    let parquet_file = AsyncFs::File::create(cached_path)
        .await
        .map_err(io_to_err)?;

    let sch = if let Some(record) = curr.records.first() {
        record.schema()
    } else {
        // the record batch is empty, do not cache and return early
        return Ok(());
    };

    let mut arrow_writer =
        AsyncArrowWriter::try_new(parquet_file, sch, None).map_err(parquet_to_err)?;

    for record in curr.records.iter().as_ref() {
        if let Err(e) = arrow_writer.write(record).await {
            log::error!("Error While Writing to Query Cache: {}", e);
        }
    }

    arrow_writer.close().await.map_err(parquet_to_err)?;
    log::info!("Reloaded dynamic query {uuid}: {}", curr.records.len());
    Ok(())
}
async fn load_parquet(path: &Path) -> Result<Vec<RecordBatch>, QueryError> {
    let file = AsyncFs::File::open(path).await.map_err(io_to_err)?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(parquet_to_err)?;
    // Build a async parquet reader.
    let stream = builder.build().map_err(parquet_to_err)?;

    stream
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(parquet_to_err)
}
pub fn init_dynamic_query_scheduler() -> anyhow::Result<()> {
    if let Ok(plans_path) = load_plans_path() {
        fs::create_dir_all(plans_path).unwrap();
    } else {
        return Ok(());
    }
    log::info!("Setting up schedular for dynamic query");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(clokwerk::Interval::Minutes(1))
        .run(move || async {
            let queries_path = load_plans_path().unwrap();

            let mut queries = AsyncFs::read_dir(&queries_path).await.unwrap();
            while let Some(entry) = queries.next_entry().await.unwrap() {
                let path = entry.path();
                let uuid = Ulid::from_string(&path.with_extension("").to_string_lossy()).unwrap();

                let rec = load_parquet(&path).await.unwrap().pop().unwrap();
                let bytes_arr = rec.column(0);
                let bytes_data = bytes_arr.to_data();
                let bytes = bytes_data.buffers()[0].to_byte_slice();

                let durations_arr = rec.column(1).to_data();
                let dur_mins = durations_arr.buffer::<u32>(0);
                let query = DynamicQuery {
                    cache_duration: Duration::from_secs((dur_mins[0] as u64) * 60),
                    plan: logical_plan_from_bytes(bytes, &QUERY_SESSION).unwrap(),
                };
                process_dynamic_query(uuid, &query).await.unwrap();
            }
        });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    Ok(())
}
