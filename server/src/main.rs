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

mod about;
mod alerts;
mod analytics;
mod banner;
mod catalog;
mod cli;
mod event;
mod handlers;
mod livetail;
mod localcache;
mod metadata;
mod metrics;
mod migration;
mod oidc;
mod option;
mod query;
mod rbac;
mod response;
mod static_schema;
mod stats;
mod storage;
mod sync;
mod utils;
mod validator;

use std::sync::Arc;

use handlers::http::modal::ParseableServer;
use option::{Mode, CONFIG};

use crate::{
    handlers::http::modal::{
        ingest_server::IngestServer, query_server::QueryServer, server::Server,
    },
};
pub const STORAGE_UPLOAD_INTERVAL: u32 = 60;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // these are empty ptrs so mem footprint should be minimal
    let server: Arc<dyn ParseableServer> = match CONFIG.parseable.mode {
        Mode::Query => Arc::new(QueryServer),

        Mode::Ingest => Arc::new(IngestServer),

        Mode::All => Arc::new(Server),
    };

    server.init().await?;

    Ok(())
}
