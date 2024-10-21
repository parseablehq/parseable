use crate::handlers::http::query::QueryError;
use crate::query::QUERY_SESSION;
use actix_web::web::Json;
use actix_web::{FromRequest, HttpRequest};
use datafusion::logical_expr::LogicalPlan;
use lazy_static::lazy_static;
use regex::Regex;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

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
    static ref DURATION_REGEX: Regex = Regex::new(r"^([0-9]+)(d|h|m|s|ms)$").unwrap();
}
fn parse_duration(s: &str) -> Option<Duration> {
    DURATION_REGEX.captures(s).and_then(|cap| {
        let value = cap[1].parse::<u64>().unwrap();
        let unit = &cap[2];
        match unit {
            "ms" => Duration::from_secs(value).into(),
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
            let query = query.await?.into_inner();
            let res = from_raw_query(&query).await;
            println!("query: {:?}", res);
            Ok(res)
        };

        Box::pin(fut)
    }
}

pub async fn dynamic_query(
    _req: HttpRequest,
    query_request: DynamicQuery,
) -> Result<String, QueryError> {
    Ok(format!("{:?}", query_request))
}
