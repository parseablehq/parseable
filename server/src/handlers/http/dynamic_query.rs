use crate::handlers::http::query::QueryError;
use crate::query::{Query, QUERY_SESSION};
use crate::response::QueryResponse;
use actix_web::web::Json;
use actix_web::{FromRequest, HttpRequest};
use chrono::{DateTime, Utc};
use datafusion::logical_expr::LogicalPlan;
use lazy_static::lazy_static;
use regex::Regex;
use std::{collections::BTreeMap, future::Future, pin::Pin, time::Duration};
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

lazy_static! {
    static ref RESULTS_BY_UUID: Mutex<BTreeMap<Uuid, QueryResponse>> = Mutex::new(BTreeMap::new());
}

pub async fn dynamic_query(_req: HttpRequest, res: DynamicQuery) -> Result<String, QueryError> {
    let duration = res.cache_duration;
    let uuid = Uuid::new_v4();
    let plan = res.plan;
    let chrono_duration = chrono::Duration::from_std(duration)?;
    let mut last_query_time: DateTime<Utc> = Utc::now() - chrono_duration;
    tokio::spawn(async move {
        loop {
            let query = Query {
                start: last_query_time,
                end: last_query_time + chrono_duration,
                raw_logical_plan: plan.clone(),
                filter_tag: None,
            };
            last_query_time = Utc::now();
            let table_name = query
                .first_table_name()
                .expect("No table name found in query");

            let (records, fields) = query.execute(table_name.clone()).await?;

            println!("Results total: {:?}", records[0].num_rows());

            let response = QueryResponse {
                records,
                fields,
                fill_null: false,
                with_fields: true,
            }
            .to_http()?;

            let mut results = RESULTS_BY_UUID.lock().await;
            results.insert(uuid, response);
            tokio::time::sleep(duration).await;
        }
    });
    Ok(String::new())
}
