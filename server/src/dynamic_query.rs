use crate::{
    handlers::http::dynamic_query::DynamicQuery, localcache::CacheError, query::Query,
    response::QueryResponse,
};
use arrow_array::RecordBatch;
use chrono::Utc;
use clokwerk::AsyncScheduler;
use datafusion::logical_expr::LogicalPlan;
use futures::TryStreamExt;
use itertools::Itertools;
use lazy_static::lazy_static;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::time::Duration;
use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    u32,
};
use tokio::fs as AsyncFs;
use tokio::sync::Mutex;
use ulid::Ulid;

const DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV: &str = "DYNAMIC_QUERY_RESULTS_CACHE_PATH";
lazy_static! {
    static ref QUERIES_BY_UUID: Mutex<BTreeMap<Ulid, DynamicQuery>> = Mutex::new(BTreeMap::new());
    static ref DYNAMIC_QUERY_RESULTS_CACHE_PATH: PathBuf = Path::new(
        &env::var(DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV).expect(&format!(
            "Missing environment variable: {DYNAMIC_QUERY_RESULTS_CACHE_PATH_ENV}"
        ))
    )
    .to_owned();
}

pub async fn clear() -> std::io::Result<()> {
    log::info!("Clearing old dynamic cache files");
    let mut total: u32 = 0;
    let mut dirs = AsyncFs::read_dir(DYNAMIC_QUERY_RESULTS_CACHE_PATH.as_path()).await?;
    while let Some(entry) = dirs.next_entry().await? {
        let path = entry.path();
        if !path.ends_with(".parquet") {
            continue;
        }
        total += 1;
        AsyncFs::remove_file(path).await?;
    }
    log::info!("Cleared old dynamic cache files: {}", total);
    Ok(())
}

fn resolve_uuid_cache_path(uuid: Ulid) -> PathBuf {
    DYNAMIC_QUERY_RESULTS_CACHE_PATH.join(format!("{}.parquet", uuid))
}

pub async fn register_query(uuid: Ulid, query: DynamicQuery) {
    let mut queries = QUERIES_BY_UUID.lock().await;
    queries.insert(uuid, query.clone());
    process_dynamic_query(uuid, &query).await
}

pub async fn load(uuid: Ulid) -> Result<QueryResponse, CacheError> {
    let path = resolve_uuid_cache_path(uuid);

    let file = AsyncFs::File::open(path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
    // Build a async parquet reader.
    let stream = builder.build()?;

    let records = stream.try_collect::<Vec<RecordBatch>>().await?;
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

async fn process_dynamic_query(uuid: Ulid, query: &DynamicQuery) {

    log::info!("Reloading dynamic query {uuid}: {:?}", query);
    let curr = load_query(
        chrono::Duration::from_std(query.cache_duration).unwrap(),
        query.plan.clone(),
    )
    .await;

    log::info!("Reloaded dynamic query {uuid}: {}", curr.records.len());
}

pub fn init_dynamic_query_scheduler() -> anyhow::Result<()> {
    log::info!("Setting up schedular for dynamic query");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(clokwerk::Interval::Minutes(1))
        .run(move || async {
            let queries = QUERIES_BY_UUID.lock().await;
            for (uuid, query) in queries.iter() {
                process_dynamic_query(*uuid, query)  .await;
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
