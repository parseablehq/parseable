use crate::handlers::http::query::QueryError;
use crate::query::QUERY_SESSION;
use actix_web::web::Json;
use actix_web::{FromRequest, HttpRequest};
use clokwerk::AsyncScheduler;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;
/// Query Request through http endpoint.
#[derive(Debug)]
pub struct DynamicQuery {
    pub plan: LogicalPlan,
    pub cache_duration: Duration,
}
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct RawDynamicQuery {
    pub query: String,
    pub cache_duration: String,
}
lazy_static! {
    static ref DURATION_REGEX: Regex = Regex::new(r"^([0-9]+)([dhms])$").unwrap();
}
fn parse_duration(s: &str) -> Option<Duration> {
    DURATION_REGEX.captures(s).and_then(|cap| {
        let value = cap[1].parse::<u64>().unwrap();
        let unit = &cap[2];
        match unit {
            "s" => Duration::from_secs(value).into(),
            "m" => Duration::from_secs(value * 60).into(),
            "h" => Duration::from_secs(value * 60 * 60).into(),
            "d" => Duration::from_secs(value * 60 * 60 * 24).into(),
            _ => None,
        }
    })
}
async fn from_raw_query(raw: &RawDynamicQuery) -> DynamicQuery {
    let session_state = QUERY_SESSION.state();

    // get the logical plan and extract the table name
    let raw_logical_plan = session_state.create_logical_plan(&raw.query).await.unwrap();
    DynamicQuery {
        cache_duration: parse_duration(&raw.cache_duration).expect("Invalid duration"),
        plan: raw_logical_plan,
    }
}
impl FromRequest for DynamicQuery {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let query = Json::<RawDynamicQuery>::from_request(req, payload);
        let fut = async move {
            let query_res = query.await?.into_inner();
            Ok(from_raw_query(&query_res).await)
        };
        Box::pin(fut)
    }
}

pub fn background_run_scheduler() {
    tokio::spawn(async move {
        loop {
            let mut scheduler = ASYNC_SCHEDULER.lock().await;
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}
lazy_static! {
    static ref ASYNC_SCHEDULER: Mutex<AsyncScheduler> = Mutex::new(AsyncScheduler::new());
    static ref PLANS_BY_UUID: Mutex<BTreeMap<Uuid, LogicalPlan>> = Mutex::new(BTreeMap::new());
    static ref RESULTS_BY_UUID: Mutex<BTreeMap<Uuid, DataFrame>> = Mutex::new(BTreeMap::new());
}

pub async fn dynamic_query(_req: HttpRequest, res: DynamicQuery) -> Result<String, QueryError> {
    let duration = res.cache_duration;
    let uuid = Uuid::new_v4();
    let interval = clokwerk::Interval::Seconds(duration.as_secs() as u32);
    {
        let mut plans = PLANS_BY_UUID.lock().await;
        (&mut *plans).insert(uuid, res.plan);
    }
    let mut scheduler = ASYNC_SCHEDULER.lock().await;
    let my_id = uuid.clone();

    scheduler.every(interval).run( || async {
        let id = my_id.clone();
        let mut curr: LogicalPlan;
        {
            let plans = PLANS_BY_UUID.lock().await;
            curr = plans.get(&id).cloned().unwrap();
        }
        println!("TODO: Refresh cache for query: {}", curr.display());

        /*
        let session_ctx = &*QUERY_SESSION;
        let frame = session_ctx.execute_logical_plan(curr.clone()).await;
        println!("Result: {:?}", frame);

        let mut results = RESULTS_BY_UUID.lock().await;
        results.insert(uuid, frame.unwrap());*/
    });

    Ok(String::new())
}
