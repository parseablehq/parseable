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

use parseable::{
    banner,
    option::{Mode, CONFIG},
    rbac, storage, IngestServer, ParseableServer, QueryServer, Server,
};
use tracing_subscriber::EnvFilter;

#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
use parseable::kafka;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .init();

    // these are empty ptrs so mem footprint should be minimal
    let server: Box<dyn ParseableServer> = match CONFIG.options.mode {
        Mode::Query => Box::new(QueryServer),
        Mode::Ingest => Box::new(IngestServer),
        Mode::All => Box::new(Server),
    };

    // load metadata from persistence
    let parseable_json = server.load_metadata().await?;
    let metadata = storage::resolve_parseable_metadata(&parseable_json).await?;
    banner::print(&CONFIG, &metadata).await;
    // initialize the rbac map
    rbac::map::init(&metadata);
    // keep metadata info in mem
    metadata.set_global();

    #[cfg(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    // load kafka server
    if CONFIG.options.mode != Mode::Query {
        tokio::task::spawn(kafka::setup_integration());
    }

    server.init().await?;

    Ok(())
}
