use crate::handlers::http::query::QueryError;
use crate::{query::Query, response::QueryResponse};
use anyhow::anyhow;
use arrow_array::RecordBatch;
use chrono::Utc;
use clokwerk::AsyncScheduler;
use datafusion::logical_expr::LogicalPlan;
use futures::TryStreamExt;
use itertools::Itertools;
use lazy_static::lazy_static;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::errors::ParquetError;
use std::ffi::OsStr;
use std::io;
use std::str::FromStr;
use std::time::Duration;
use std::{collections::BTreeMap, env, path::PathBuf, u32};
use tokio::fs as AsyncFs;
use tokio::sync::Mutex;
use tokio::task;
use ulid::Ulid;

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
lazy_static! {
    static ref QUERIES_BY_UUID: Mutex<BTreeMap<Ulid, DynamicQuery>> = Mutex::new(BTreeMap::new());
}

pub async fn clear() -> anyhow::Result<()> {
    if let Ok(cache_path) = load_env_path(DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV) {
        log::info!("Clearing old dynamic cache files");
        let mut total: u32 = 0;
        let mut dirs = AsyncFs::read_dir(cache_path).await?;
        while let Some(entry) = dirs.next_entry().await? {
            let path = entry.path();
            if path.extension() != Some(OsStr::new("parquet")) {
                continue;
            }
            total += 1;
            AsyncFs::remove_file(path).await?;
        }
        log::info!("Cleared old dynamic cache files: {}", total);
    } else {
        log::warn!("No env var found for {DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV}");
    }
    Ok(())
}

fn resolve_uuid_cache_path(uuid: Ulid) -> Result<PathBuf, QueryError> {
    let curr = load_env_path(DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV)?;
    Ok(curr.join(format!("{}.parquet", uuid)))
}

pub async fn register_query(uuid: Ulid, query: DynamicQuery) -> Result<(), QueryError> {
    let cache_path = load_env_path(DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV)?;
    AsyncFs::create_dir_all(cache_path)
        .await
        .map_err(io_to_err)?;
    let mut queries = QUERIES_BY_UUID.lock().await;
    if queries.len() == MAX_SERVER_URL_STORES {
        return Err(QueryError::Anyhow(anyhow!(
            "Total dynamic queries is over limit: {}",
            MAX_SERVER_URL_STORES
        )));
    }
    queries.insert(uuid, query.clone());

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

pub fn init_dynamic_query_scheduler() -> anyhow::Result<()> {
    log::info!("Setting up schedular for dynamic query");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(clokwerk::Interval::Minutes(1))
        .run(move || async {
            let queries = QUERIES_BY_UUID.lock().await;
            for (uuid, query) in queries.iter() {
                process_dynamic_query(*uuid, query).await.unwrap();
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
