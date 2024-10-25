use chrono::Utc;
use clokwerk::AsyncScheduler;
use datafusion::logical_expr::LogicalPlan;
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::Mutex;
use ulid::Ulid;

use crate::{handlers::http::dynamic_query::DynamicQuery, query::Query, response::QueryResponse};

lazy_static! {
    static ref QUERIES_BY_UUID: Mutex<BTreeMap<Ulid, DynamicQuery>> = Mutex::new(BTreeMap::new());
}

pub async fn register_query(uuid: Ulid, query: DynamicQuery) {
    let mut queries = QUERIES_BY_UUID.lock().await;
    queries.insert(uuid, query);
}

pub async fn load(uuid: Ulid) -> QueryResponse {
    unimplemented!("TODO: ")
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

pub fn init_dynamic_query_scheduler() -> anyhow::Result<()> {
    log::info!("Setting up schedular for dynamic query");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(clokwerk::Interval::Minutes(1))
        .run(move || async {
            let queries = QUERIES_BY_UUID.lock().await;
            for (uuid, query) in queries.iter() {
                log::info!("Refreshing dynamic query {uuid}: {:?}", query);
                let curr = load_query(
                    chrono::Duration::from_std(query.cache_duration).unwrap(),
                    query.plan.clone(),
                )
                .await;

                log::info!("Reloaded dynamic query {uuid}: {:?}", query);
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
