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

#[cfg(any(
    feature = "rdkafka-ssl",
    feature = "rdkafka-ssl-vendored",
    feature = "rdkafka-sasl"
))]
use parseable::connectors;
use parseable::{
    banner, metrics,
    option::{Mode, CONFIG},
    rbac, storage, IngestServer, ParseableServer, QueryServer, Server,
};
use tokio::signal::ctrl_c;
use tokio::sync::oneshot;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    init_logger(LevelFilter::DEBUG);

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

    // Spawn a task to trigger graceful shutdown on appropriate signal
    let (shutdown_trigger, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        block_until_shutdown_signal().await;

        // Trigger graceful shutdown
        println!("Received shutdown signal, notifying server to shut down...");
        shutdown_trigger.send(()).unwrap();
    });

    let prometheus = metrics::build_metrics_handler();
    let parseable_server = server.init(&prometheus, shutdown_rx);

    #[cfg(any(
        feature = "rdkafka-ssl",
        feature = "rdkafka-ssl-vendored",
        feature = "rdkafka-sasl"
    ))]
    {
        // load kafka server
        if CONFIG.options.mode != Mode::Query {
            let connectors_task = connectors::init(&prometheus);
            tokio::try_join!(parseable_server, connectors_task)?;
        } else {
            parseable_server.await?;
        }
    }

    #[cfg(not(any(
        feature = "rdkafka-ssl",
        feature = "rdkafka-ssl-vendored",
        feature = "rdkafka-sasl"
    )))]
    {
        parseable_server.await?;
    }

    Ok(())
}

pub fn init_logger(default_level: LevelFilter) {
    let filter_layer = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level.to_string()));

    let fmt_layer = fmt::layer()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .compact();

    Registry::default()
        .with(filter_layer)
        .with(fmt_layer)
        .init();
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
