/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use clokwerk::{AsyncScheduler, Scheduler, TimeUnits};
use thread_priority::{ThreadBuilder, ThreadPriority};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

#[cfg(feature = "debug")]
use pyroscope::PyroscopeAgent;
#[cfg(feature = "debug")]
use pyroscope_pprofrs::{pprof_backend, PprofConfig};

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::thread::{self, JoinHandle};
use std::time::Duration;

mod about;
mod alerts;
mod analytics;
mod banner;
mod event;
mod external_service;
mod handlers;
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
mod utils;
mod validator;

use option::CONFIG;

#[cfg(feature = "debug")]
const DEBUG_PYROSCOPE_URL: &str = "P_PROFILE_PYROSCOPE_URL";
#[cfg(feature = "debug")]
const DEBUG_PYROSCOPE_TOKEN: &str = "P_PROFILE_PYROSCOPE_AUTH_TOKEN";

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    CONFIG.validate();
    let storage = CONFIG.storage().get_object_store();
    CONFIG.validate_staging()?;
    migration::run_metadata_migration(&CONFIG).await?;
    let metadata = storage::resolve_parseable_metadata().await?;
    banner::print(&CONFIG, &metadata).await;
    rbac::map::init(&metadata);
    external_service::init(&metadata);
    metadata.set_global();
    let prometheus = metrics::build_metrics_handler();
    CONFIG.storage().register_store_metrics(&prometheus);

    migration::run_migration(&CONFIG).await?;

    #[cfg(feature = "debug")]
    {
        if std::env::var(DEBUG_PYROSCOPE_URL).is_ok() {
            let url = std::env::var(DEBUG_PYROSCOPE_URL).ok();
            start_profiling(url.unwrap());
        }
    }

    if let Err(e) = metadata::STREAM_INFO.load(&*storage).await {
        log::warn!("could not populate local metadata. {:?}", e);
    }

    // track all parquet files already in the data directory
    storage::retention::load_retention_from_global().await;
    // load data from stats back to prometheus metrics
    metrics::load_from_stats_from_storage().await;

    let (localsync_handler, mut localsync_outbox, localsync_inbox) = run_local_sync();
    let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
        object_store_sync();

    // all internal data structures populated now.
    // start the analytics scheduler if enabled
    if CONFIG.parseable.send_analytics {
        analytics::init_analytics_scheduler().await;
    }

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
                (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = object_store_sync();
            }
        };
    }
}

#[cfg(feature = "debug")]
fn start_profiling(url: String) {
    let auth_token = std::env::var(DEBUG_PYROSCOPE_TOKEN).unwrap_or_else(|_| "".to_string());

    // Configure Pyroscope client.
    let agent = PyroscopeAgent::builder(url, env!("CARGO_PKG_NAME").to_string())
        .auth_token(auth_token)
        .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
        .build()
        .unwrap();

    // Start the Pyroscope client.
    agent.start().unwrap();
}

fn object_store_sync() -> (JoinHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();
    let mut inbox_rx = AssertUnwindSafe(inbox_rx);
    let handle = thread::spawn(move || {
        let res = catch_unwind(move || {
            let rt = actix_web::rt::System::new();
            rt.block_on(async {
                let mut scheduler = AsyncScheduler::new();
                scheduler
                    .every((CONFIG.parseable.upload_interval as u32).seconds())
                    .run(|| async {
                        if let Err(e) = CONFIG.storage().get_object_store().sync().await {
                            log::warn!("failed to sync local data with object store. {:?}", e);
                        }
                    });

                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    scheduler.run_pending().await;
                    match AssertUnwindSafe(|| inbox_rx.try_recv())() {
                        Ok(_) => break,
                        Err(TryRecvError::Empty) => continue,
                        Err(TryRecvError::Closed) => {
                            // should be unreachable but breaking anyways
                            break;
                        }
                    }
                }
            })
        });

        if res.is_err() {
            outbox_tx.send(()).unwrap();
        }
    });

    (handle, outbox_rx, inbox_tx)
}

fn run_local_sync() -> (JoinHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();
    let mut inbox_rx = AssertUnwindSafe(inbox_rx);

    let handle = ThreadBuilder::default()
        .name("local-sync")
        .priority(ThreadPriority::Max)
        .spawn(move |priority_result| {
            if priority_result.is_err() {
                log::warn!("Max priority cannot be set for sync thread. Make sure that user/program is allowed to set thread priority.")
            }
            let res = catch_unwind(move || {
                let mut scheduler = Scheduler::new();
                scheduler
                    .every((storage::LOCAL_SYNC_INTERVAL as u32).seconds())
                    .run(move || crate::event::STREAM_WRITERS.unset_all());

                loop {
                    thread::sleep(Duration::from_millis(50));
                    scheduler.run_pending();
                    match AssertUnwindSafe(|| inbox_rx.try_recv())() {
                        Ok(_) => break,
                        Err(TryRecvError::Empty) => continue,
                        Err(TryRecvError::Closed) => {
                            // should be unreachable but breaking anyways
                            break;
                        }
                    }
                }
            });

            if res.is_err() {
                outbox_tx.send(()).unwrap();
            }
        })
        .unwrap();

    (handle, outbox_rx, inbox_tx)
}
