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
    audit::AuditLayer,
    banner, kafka,
    option::{Mode, CONFIG},
    rbac, storage, IngestServer, ParseableServer, QueryServer, Server,
};
use tokio::runtime::Handle;
use tracing_subscriber::{
    layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer, Registry,
};

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let stdout_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(EnvFilter::from_default_env());
    let subscriber = Registry::default().with(stdout_layer);

    // Use audit logging endpoint if set, else only log to STDOUT
    if let Some(audit_layer) = AuditLayer::new(Handle::current()) {
        // All logs go through audit_layer before reaching the stdout_layer
        subscriber.with(audit_layer).init();
    } else {
        subscriber.init();
    };

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

    // load kafka server
    if CONFIG.parseable.mode != Mode::Query {
        tokio::task::spawn(kafka::setup_integration());
    }

    server.init().await?;

    Ok(())
}
