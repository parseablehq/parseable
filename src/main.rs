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
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    init_logger(LevelFilter::DEBUG);

    // these are empty ptrs so mem footprint should be minimal
    let server: Box<dyn ParseableServer> = match CONFIG.parseable.mode {
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

    let prometheus = metrics::build_metrics_handler();
    let parseable_server = server.init(&prometheus);

    #[cfg(any(
        feature = "rdkafka-ssl",
        feature = "rdkafka-ssl-vendored",
        feature = "rdkafka-sasl"
    ))]
    {
        let connectors_task = connectors::init(&prometheus);
        tokio::try_join!(parseable_server, connectors_task)?;
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
