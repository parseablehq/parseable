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
use tokio::signal::ctrl_c;
use tokio::sync::oneshot;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[cfg(unix)]
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

    // Spawn a task to trigger graceful shutdown on appropriate signal
    let (shutdown_trigger, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        block_until_shutdown_signal().await;

        // Trigger graceful shutdown
        println!("Received shutdown signal, notifying server to shut down...");
        shutdown_trigger.send(()).unwrap();
    });

    server.init(shutdown_rx).await?;

    Ok(())
}

#[cfg(windows)]
/// Asynchronously blocks until a shutdown signal is received
pub async fn block_until_shutdown_signal() {
    _ = ctrl_c().await;
    info!("Received a CTRL+C event");
}

#[cfg(unix)]
/// Asynchronously blocks until a shutdown signal is received
pub async fn block_until_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm =
        signal(SignalKind::terminate()).expect("Failed to create SIGTERM signal handler");

    tokio::select! {
        _ = ctrl_c() => info!("Received SIGINT signal"),
        _ = sigterm.recv() => info!("Received SIGTERM signal"),
    }
}
