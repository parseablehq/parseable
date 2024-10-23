use actix_web::{HttpResponse, Responder};

use crate::handlers::http::modal::{query_server::{QUERIER_META, QUERY_ROUTING}, LEADER};

pub mod querier_ingest;
pub mod querier_logstream;
pub mod querier_rbac;
pub mod querier_role;
pub mod querier_query;

pub async fn make_leader() -> impl Responder {
    LEADER.lock()
        .make_leader();
    log::warn!("made leader- {:?}\n{:?}", LEADER, QUERIER_META);

    // also modify QUERY_ROUTING for this query node
    QUERY_ROUTING.lock().await.new().await;
    HttpResponse::Ok().finish()
}