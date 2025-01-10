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

pub mod about;
mod alerts;
pub mod analytics;
pub mod audit;
pub mod banner;
mod catalog;
mod cli;
pub mod correlation;
mod event;
pub mod handlers;
pub mod hottier;
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
pub mod kafka;
mod livetail;
mod metadata;
pub mod metrics;
pub mod migration;
mod oidc;
pub mod option;
pub mod otel;
mod query;
pub mod rbac;
mod response;
mod static_schema;
mod stats;
pub mod storage;
pub mod sync;
pub mod users;
mod utils;
mod validator;

use std::time::Duration;

pub use handlers::http::modal::{
    ingest_server::IngestServer, query_server::QueryServer, server::Server, ParseableServer,
};
use once_cell::sync::Lazy;
use reqwest::{Client, ClientBuilder};

pub const STORAGE_UPLOAD_INTERVAL: u32 = 60;

// A single HTTP client for all outgoing HTTP requests from the parseable server
static HTTP_CLIENT: Lazy<Client> = Lazy::new(|| {
    ClientBuilder::new()
        .connect_timeout(Duration::from_secs(3)) // set a timeout of 3s for each connection setup
        .timeout(Duration::from_secs(10)) // set a timeout of 10s for each request
        .pool_idle_timeout(Duration::from_secs(90)) // set a timeout of 90s for each idle connection
        .pool_max_idle_per_host(32) // max 32 idle connections per host
        .gzip(true) // gzip compress for all requests
        .brotli(true) // brotli compress for all requests
        .use_rustls_tls() // use only the rustls backend
        .http1_only() // use only http/1.1
        .build()
        .expect("Construction of client shouldn't fail")
});
