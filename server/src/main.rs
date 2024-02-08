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
mod stats;
mod storage;
mod sync;
mod utils;
mod validator;

use option::CONFIG;

use crate::localcache::LocalCacheManager;
pub const STORAGE_UPLOAD_INTERVAL: u32 = 60;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    CONFIG.validate().await?;
    migration::run_metadata_migration(&CONFIG).await?;
    let metadata = storage::resolve_parseable_metadata().await?;
    banner::print(&CONFIG, &metadata).await;
    rbac::map::init(&metadata);
    metadata.set_global();
    if let Some(cache_manager) = LocalCacheManager::global() {
        cache_manager
            .validate(CONFIG.parseable.local_cache_size)
            .await?;
    };
    let prometheus = metrics::build_metrics_handler();
    CONFIG.storage().register_store_metrics(&prometheus);

    migration::run_migration(&CONFIG).await?;

    let storage = CONFIG.storage().get_object_store();
    if let Err(e) = metadata::STREAM_INFO.load(&*storage).await {
        log::warn!("could not populate local metadata. {:?}", e);
    }

    // track all parquet files already in the data directory
    storage::retention::load_retention_from_global();
    // load data from stats back to prometheus metrics
    metrics::fetch_stats_from_storage().await;

    let (localsync_handler, mut localsync_outbox, localsync_inbox) = sync::run_local_sync();
    let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
        sync::object_store_sync();

    // all internal data structures populated now.
    // start the analytics scheduler if enabled
    if CONFIG.parseable.send_analytics {
        analytics::init_analytics_scheduler();
    }

    tokio::spawn(handlers::livetail::server());

    let app = handlers::http::run_http(prometheus, CONFIG.parseable.openid.clone());
    tokio::pin!(app);
    loop {
        tokio::select! {
            e = &mut app => {
                // actix server finished .. stop other threads and stop the server
                remote_sync_inbox.send(()).unwrap_or(());
                localsync_inbox.send(()).unwrap_or(());
                localsync_handler.join().unwrap_or(());
                remote_sync_handler.join().unwrap_or(());
                return e
            },
            _ = &mut localsync_outbox => {
                // crash the server if localsync fails for any reason
                // panic!("Local Sync thread died. Server will fail now!")
                return Err(anyhow::Error::msg("Failed to sync local data to drive. Please restart the Parseable server.\n\nJoin us on Parseable Slack if the issue persists after restart : https://launchpass.com/parseable"))
            },
            _ = &mut remote_sync_outbox => {
                // remote_sync failed, this is recoverable by just starting remote_sync thread again
                remote_sync_handler.join().unwrap_or(());
                (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = sync::object_store_sync();
            }

        };
    }
}
