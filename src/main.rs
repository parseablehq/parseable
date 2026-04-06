use std::process::exit;

/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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
use opentelemetry::trace::TracerProvider as _;
#[cfg(feature = "kafka")]
use parseable::connectors;
use parseable::{
    IngestServer, ParseableServer, QueryServer, Server, banner, metrics, option::Mode,
    parseable::PARSEABLE, rbac, storage,
};
use tokio::signal::ctrl_c;
use tokio::sync::oneshot;
use tracing::Level;
use tracing::{info, warn};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry, fmt};

/// Env var to read the logging level of OTel traces. Defaults to `info`.
const OTEL_TRACE_LEVEL: &str = "OTEL_TRACE_LEVEL";

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let otel_provider = init_logger();
    // Install the rustls crypto provider before any TLS operations.
    // This is required for rustls 0.23+ which needs an explicit crypto provider.
    // If the installation fails, log a warning but continue execution.
    if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
        warn!("Failed to install rustls crypto provider: {:?}", e);
    }

    // these are empty ptrs so mem footprint should be minimal
    let server: Box<dyn ParseableServer> = match &PARSEABLE.options.mode {
        Mode::Query => Box::new(QueryServer),
        Mode::Ingest => Box::new(IngestServer),
        Mode::Index => {
            println!(
                "Indexing is an enterprise feature. Check out https://www.parseable.com/pricing to know more!"
            );
            exit(0)
        }
        Mode::Prism => {
            println!(
                "Prism is an enterprise feature. Check out https://www.parseable.com/pricing to know more!"
            );
            exit(0)
        }
        Mode::All => Box::new(Server),
    };

    // load metadata from persistence
    let parseable_json = server.load_metadata().await?;
    let metadata = storage::resolve_parseable_metadata(&parseable_json, &None).await?;
    banner::print(&PARSEABLE, &metadata).await;
    // initialize the rbac map
    rbac::map::init(&metadata);
    // keep metadata info in mem
    metadata.set_global();

    // Spawn a task to trigger graceful shutdown on appropriate signal
    let (shutdown_trigger, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        block_until_shutdown_signal().await;

        // Trigger graceful shutdown
        warn!("Received shutdown signal, notifying server to shut down...");
        shutdown_trigger.send(()).unwrap();
    });

    let prometheus = metrics::build_metrics_handler();
    // Start servers
    #[cfg(feature = "kafka")]
    {
        let parseable_server = server.init(&prometheus, shutdown_rx);
        let connectors = connectors::init(&prometheus);

        tokio::try_join!(parseable_server, connectors)?;
    }

    #[cfg(not(feature = "kafka"))]
    {
        let parseable_server = server.init(&prometheus, shutdown_rx);
        parseable_server.await?;
    }

    if let Some(provider) = otel_provider {
        if let Err(e) = provider.shutdown() {
            warn!("Failed to shutdown OTel tracer provider: {:?}", e);
        }
    }

    Ok(())
}

pub fn init_logger() -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    let filter_layer = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let default_level = if cfg!(debug_assertions) {
            Level::DEBUG
        } else {
            Level::WARN
        };
        EnvFilter::new(default_level.to_string())
    });

    let fmt_layer = fmt::layer()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_target(true)
        .compact();

    let otel_provider = parseable::telemetry::init_tracing();

    let otel_layer = otel_provider.as_ref().map(|provider| {
        let otel_filter =
            EnvFilter::try_from_env(OTEL_TRACE_LEVEL).unwrap_or_else(|_| EnvFilter::new("info"));
        let tracer = provider.tracer("parseable");
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(otel_filter)
    });

    Registry::default()
        .with(filter_layer)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    otel_provider
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
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm =
        signal(SignalKind::terminate()).expect("Failed to create SIGTERM signal handler");

    tokio::select! {
        _ = ctrl_c() => info!("Received SIGINT signal"),
        _ = sigterm.recv() => info!("Received SIGTERM signal"),
    }
}
