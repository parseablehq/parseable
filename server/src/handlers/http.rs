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

use std::sync::Arc;

use actix_cors::Cors;
use actix_web_prometheus::PrometheusMetrics;

use crate::option::CONFIG;

use crate::handlers::http::server::{
    ingest_server::IngestServer, parseable_server::ParseableServer, query_server::QueryServer,
    server::Server,
};
use crate::option::Mode;

pub(crate) mod about;
pub(crate) mod health_check;
pub(crate) mod ingest;
mod kinesis;
pub(crate) mod llm;
pub(crate) mod logstream;
pub(crate) mod middleware;
pub(crate) mod oidc;
mod otel;
pub(crate) mod query;
pub(crate) mod rbac;
pub(crate) mod role;
mod server;

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

pub const MAX_EVENT_PAYLOAD_SIZE: usize = 10485760;
pub const API_BASE_PATH: &str = "/api";
pub const API_VERSION: &str = "v1";

pub async fn run_http(
    prometheus: PrometheusMetrics,
    oidc_client: Option<crate::oidc::OpenidConfig>,
) -> anyhow::Result<()> {
    let server: Arc<dyn ParseableServer> = match CONFIG.parseable.mode {
        Mode::Query => Arc::new(QueryServer::default()),
        Mode::Ingest => Arc::new(IngestServer::default()),
        Mode::All => Arc::new(Server::default()),
    };

    server.start(prometheus, oidc_client).await?;
    Ok(())
}

pub(crate) fn base_path() -> String {
    format!("{API_BASE_PATH}/{API_VERSION}")
}

pub fn metrics_path() -> String {
    format!("{}/metrics", base_path())
}

pub(crate) fn cross_origin_config() -> Cors {
    if cfg!(feature = "debug") {
        Cors::permissive().block_on_origin_mismatch(false)
    } else {
        Cors::default().block_on_origin_mismatch(false)
    }
}
