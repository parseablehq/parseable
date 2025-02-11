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

use chrono::{TimeDelta, Timelike};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use tokio::sync::oneshot;
use tokio::time::{interval_at, sleep, Duration, Instant};
use tokio::{select, task};
use tracing::{error, info, trace, warn};

use crate::alerts::{alerts_utils, AlertConfig, AlertError};
use crate::option::CONFIG;
use crate::staging::STAGING;
use crate::storage::LOCAL_SYNC_INTERVAL;
use crate::{STORAGE_CONVERSION_INTERVAL, STORAGE_UPLOAD_INTERVAL};

// Calculates the instant that is the start of the next minute
fn next_minute() -> Instant {
    let now = chrono::Utc::now();
    let time_till = now
        .with_second(0)
        .expect("Start of the minute")
        .signed_duration_since(now)
        + TimeDelta::minutes(1);

    Instant::now() + time_till.to_std().expect("Valid duration")
}

pub async fn monitor_task_duration<F, Fut, T>(task_name: &str, threshold: Duration, f: F) -> T
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send,
    T: Send + 'static,
{
    let mut future = tokio::spawn(async move { f().await });
    let mut warned_once = false;
    let start_time = Instant::now();

    loop {
        select! {
            _ = sleep(threshold), if !warned_once => {
                warn!(
                    "Task '{task_name}' is taking longer than expected: (threshold: {threshold:?})",
                );
                warned_once = true;
            },
            res = &mut future => {
                if warned_once {
                    warn!(
                        "Task '{task_name}' took longer than expected: {:?} (threshold: {threshold:?})",
                        start_time.elapsed() - threshold
                    );
                }
                break res.expect("Task handle shouldn't error");
            }
        }
    }
}

/// Flushes arrows onto disk every `LOCAL_SYNC_INTERVAL` seconds, packs arrows into parquet every
/// `STORAGE_CONVERSION_INTERVAL` secondsand uploads them every `STORAGE_UPLOAD_INTERVAL` seconds.
#[tokio::main(flavor = "current_thread")]
pub async fn handler(mut cancel_rx: oneshot::Receiver<()>) -> anyhow::Result<()> {
    let (localsync_handler, mut localsync_outbox, localsync_inbox) = run_local_sync();
    let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
        object_store_sync();
    let (mut remote_conversion_handler, mut remote_conversion_outbox, mut remote_conversion_inbox) =
        arrow_conversion();
    loop {
        select! {
            _ = &mut cancel_rx => {
                // actix server finished .. stop other threads and stop the server
                remote_sync_inbox.send(()).unwrap_or(());
                localsync_inbox.send(()).unwrap_or(());
                remote_conversion_inbox.send(()).unwrap_or(());
                if let Err(e) = localsync_handler.await {
                    error!("Error joining remote_sync_handler: {:?}", e);
                }
                if let Err(e) = remote_sync_handler.await {
                    error!("Error joining remote_sync_handler: {:?}", e);
                }
                if let Err(e) = remote_conversion_handler.await {
                    error!("Error joining remote_conversion_handler: {:?}", e);
                }
                return Ok(());
            },
            _ = &mut localsync_outbox => {
                // crash the server if localsync fails for any reason
                // panic!("Local Sync thread died. Server will fail now!")
                return Err(anyhow::Error::msg("Failed to sync local data to drive. Please restart the Parseable server.\n\nJoin us on Parseable Slack if the issue persists after restart : https://launchpass.com/parseable"))
            },
            _ = &mut remote_sync_outbox => {
                // remote_sync failed, this is recoverable by just starting remote_sync thread again
                if let Err(e) = remote_sync_handler.await {
                    error!("Error joining remote_sync_handler: {:?}", e);
                }
                (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = object_store_sync();
            },
            _ = &mut remote_conversion_outbox => {
                // remote_conversion failed, this is recoverable by just starting remote_conversion thread again
                if let Err(e) = remote_conversion_handler.await {
                    error!("Error joining remote_conversion_handler: {:?}", e);
                }
                (remote_conversion_handler, remote_conversion_outbox, remote_conversion_inbox) = arrow_conversion();
            },
        }
    }
}

pub fn object_store_sync() -> (
    task::JoinHandle<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();

    let handle = task::spawn(async move {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut sync_interval =
                interval_at(next_minute(), Duration::from_secs(STORAGE_UPLOAD_INTERVAL));

            let mut inbox_rx = AssertUnwindSafe(inbox_rx);

            loop {
                select! {
                    _ = sync_interval.tick() => {
                        trace!("Syncing Parquets to Object Store... ");
                        if let Err(e) = monitor_task_duration(
                            "object_store_sync",
                            Duration::from_secs(15),
                            || async {
                                CONFIG
                                    .storage()
                                    .get_object_store()
                                    .upload_files_from_staging()
                                    .await
                            },
                        )
                        .await
                        {
                            warn!("failed to upload local data with object store. {e:?}");
                        }
                    },
                    res = &mut inbox_rx => {match res{
                        Ok(_) => break,
                        Err(_) => {
                            warn!("Inbox channel closed unexpectedly");
                            break;
                        }}
                    }
                }
            }
        }));

        match result {
            Ok(future) => {
                future.await;
            }
            Err(panic_error) => {
                error!("Panic in object store sync task: {panic_error:?}");
                let _ = outbox_tx.send(());
            }
        }

        info!("Object store sync task ended");
    });

    (handle, outbox_rx, inbox_tx)
}

pub fn arrow_conversion() -> (
    task::JoinHandle<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();

    let handle = task::spawn(async move {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut sync_interval = interval_at(
                next_minute() + Duration::from_secs(5), // 5 second delay
                Duration::from_secs(STORAGE_CONVERSION_INTERVAL),
            );

            let mut inbox_rx = AssertUnwindSafe(inbox_rx);

            loop {
                select! {
                    _ = sync_interval.tick() => {
                        trace!("Converting Arrow to Parquet... ");
                        if let Err(e) = monitor_task_duration(
                            "arrow_conversion",
                            Duration::from_secs(30),
                            || async { STAGING.prepare_parquet(false) },
                        ).await
                        {
                            warn!("failed to convert local arrow data to parquet. {e:?}");
                        }
                    },
                    res = &mut inbox_rx => {match res{
                        Ok(_) => break,
                        Err(_) => {
                            warn!("Inbox channel closed unexpectedly");
                            break;
                        }}
                    }
                }
            }
        }));

        match result {
            Ok(future) => {
                future.await;
            }
            Err(panic_error) => {
                error!("Panic in object store sync task: {panic_error:?}");
                let _ = outbox_tx.send(());
            }
        }

        info!("Object store sync task ended");
    });

    (handle, outbox_rx, inbox_tx)
}

pub fn run_local_sync() -> (
    task::JoinHandle<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();

    let handle = task::spawn(async move {
        info!("Local sync task started");
        let mut inbox_rx = inbox_rx;

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut sync_interval =
                interval_at(next_minute(), Duration::from_secs(LOCAL_SYNC_INTERVAL));

            loop {
                select! {
                    _ = sync_interval.tick() => {
                        trace!("Flushing Arrows to disk...");
                        STAGING.flush_all();
                    },
                    res = &mut inbox_rx => {match res{
                        Ok(_) => break,
                        Err(_) => {
                            warn!("Inbox channel closed unexpectedly");
                            break;
                        }}
                    }
                }
            }
        }));

        match result {
            Ok(future) => {
                future.await;
            }
            Err(panic_error) => {
                error!("Panic in local sync task: {:?}", panic_error);
            }
        }

        let _ = outbox_tx.send(());
        info!("Local sync task ended");
    });

    (handle, outbox_rx, inbox_tx)
}

pub fn schedule_alert_task(
    eval_frequency: u64,
    alert: AlertConfig,
    inbox_rx: oneshot::Receiver<()>,
    outbox_tx: oneshot::Sender<()>,
) -> Result<task::JoinHandle<()>, AlertError> {
    let handle = tokio::task::spawn(async move {
        info!("new alert task started for {alert:?}");

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut sync_interval =
                interval_at(next_minute(), Duration::from_secs(eval_frequency * 60));
            let mut inbox_rx = AssertUnwindSafe(inbox_rx);

            loop {
                select! {
                    _ = sync_interval.tick() => {
                        trace!("Flushing stage to disk...");
                        match alerts_utils::evaluate_alert(&alert).await {
                            Ok(_) => {}
                            Err(err) => error!("Error while evaluation- {err}"),
                        }
                    },
                    res = &mut inbox_rx => {match res{
                        Ok(_) => break,
                        Err(_) => {
                            warn!("Inbox channel closed unexpectedly");
                            break;
                        }}
                    }
                }
            }
        }));

        match result {
            Ok(future) => {
                future.await;
            }
            Err(panic_error) => {
                error!("Panic in scheduled alert task: {panic_error:?}");
                let _ = outbox_tx.send(());
            }
        }
    });
    Ok(handle)
}
