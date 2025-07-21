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
use futures::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant, interval_at, sleep};
use tokio::{select, task};
use tracing::{error, info, trace, warn};

use crate::alerts::{AlertTask, alerts_utils};
use crate::parseable::PARSEABLE;
use crate::storage::object_storage::sync_all_streams;
use crate::{LOCAL_SYNC_INTERVAL, STORAGE_UPLOAD_INTERVAL};

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
                        start_time.elapsed()
                    );
                }
                break res.expect("Task handle shouldn't error");
            }
        }
    }
}

/// Flushes arrows onto disk every `LOCAL_SYNC_INTERVAL` seconds, packs arrows into parquet every
/// `STORAGE_CONVERSION_INTERVAL` secondsand uploads them every `STORAGE_UPLOAD_INTERVAL` seconds.
#[tokio::main(flavor = "multi_thread")]
pub async fn handler(mut cancel_rx: oneshot::Receiver<()>) -> anyhow::Result<()> {
    let (localsync_handler, mut localsync_outbox, localsync_inbox) = local_sync();
    let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
        object_store_sync();
    loop {
        select! {
            _ = &mut cancel_rx => {
                remote_sync_inbox.send(()).unwrap_or(());
                localsync_inbox.send(()).unwrap_or(());
                if let Err(e) = localsync_handler.await {
                    error!("Error joining localsync_handler: {e:?}");
                }
                if let Err(e) = remote_sync_handler.await {
                    error!("Error joining remote_sync_handler: {e:?}");
                }
                return Ok(());
            },
            _ = &mut localsync_outbox => {
                return Err(anyhow::Error::msg("Failed to sync local data to drive. Please restart the Parseable server.\n\nJoin us on Parseable Slack if the issue persists after restart : https://launchpass.com/parseable"))
            },
            _ = &mut remote_sync_outbox => {
                if let Err(e) = remote_sync_handler.await {
                    error!("Error joining remote_sync_handler: {e:?}");
                }
                (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = object_store_sync();
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
        info!("Object store sync task started");
        let mut inbox_rx = inbox_rx;

        let result = tokio::spawn(async move {
            let mut sync_interval = interval_at(next_minute(), STORAGE_UPLOAD_INTERVAL);

            loop {
                select! {
                    _ = sync_interval.tick() => {
                        trace!("Syncing Parquets to Object Store... ");

                        // Monitor the duration of sync_all_streams execution
                        monitor_task_duration(
                            "object_store_sync_all_streams",
                            Duration::from_secs(PARSEABLE.options.object_store_sync_threshold),
                            || async {
                                let mut joinset = JoinSet::new();
                                sync_all_streams(&mut joinset);

                                // Wait for all spawned tasks to complete
                                while let Some(res) = joinset.join_next().await {
                                    log_join_result(res, "object store sync");
                                }
                            }
                        ).await;
                    },
                    res = &mut inbox_rx => {
                        match res {
                            Ok(_) => break,
                            Err(_) => {
                                warn!("Inbox channel closed unexpectedly");
                                break;
                            }
                        }
                    }
                }
            }
        });

        match AssertUnwindSafe(result).catch_unwind().await {
            Ok(join_result) => {
                if let Err(join_err) = join_result {
                    error!("Panic in object store sync task: {join_err:?}");
                }
            }
            Err(panic_error) => {
                error!("Panic in object store sync task: {panic_error:?}");
            }
        }

        let _ = outbox_tx.send(());
        info!("Object store sync task ended");
    });

    (handle, outbox_rx, inbox_tx)
}

/// Flush arrows onto disk and convert them into parquet files
pub fn local_sync() -> (
    task::JoinHandle<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();

    let handle = task::spawn(async move {
        info!("Local sync task started");
        let mut inbox_rx = inbox_rx;

        let result = tokio::spawn(async move {
            let mut sync_interval = interval_at(next_minute(), LOCAL_SYNC_INTERVAL);

            loop {
                select! {
                    _ = sync_interval.tick() => {
                        // Monitor the duration of flush_and_convert execution
                        monitor_task_duration(
                            "local_sync_flush_and_convert",
                            Duration::from_secs(PARSEABLE.options.local_sync_threshold),
                            || async {
                                let mut joinset = JoinSet::new();
                                PARSEABLE.streams.flush_and_convert(&mut joinset, false, false);

                                // Wait for all spawned tasks to complete
                                while let Some(res) = joinset.join_next().await {
                                    log_join_result(res, "flush and convert");
                                }
                            }
                        ).await;
                    },
                    res = &mut inbox_rx => {
                        match res {
                            Ok(_) => break,
                            Err(_) => {
                                warn!("Inbox channel closed unexpectedly");
                                break;
                            }
                        }
                    }
                }
            }
        });

        match AssertUnwindSafe(result).catch_unwind().await {
            Ok(join_result) => {
                if let Err(join_err) = join_result {
                    error!("Panic in local sync task: {join_err:?}");
                }
            }
            Err(panic_error) => {
                error!("Panic in local sync task: {panic_error:?}");
            }
        }

        let _ = outbox_tx.send(());
        info!("Local sync task ended");
    });

    (handle, outbox_rx, inbox_tx)
}

// local and object store sync at the start of the server
pub async fn sync_start() -> anyhow::Result<()> {
    // Monitor local sync duration at startup
    monitor_task_duration(
        "startup_local_sync",
        Duration::from_secs(PARSEABLE.options.local_sync_threshold),
        || async {
            let mut local_sync_joinset = JoinSet::new();
            PARSEABLE
                .streams
                .flush_and_convert(&mut local_sync_joinset, true, false);
            while let Some(res) = local_sync_joinset.join_next().await {
                log_join_result(res, "flush and convert");
            }
        },
    )
    .await;

    // Monitor object store sync duration at startup
    monitor_task_duration(
        "startup_object_store_sync",
        Duration::from_secs(PARSEABLE.options.object_store_sync_threshold),
        || async {
            let mut object_store_joinset = JoinSet::new();
            sync_all_streams(&mut object_store_joinset);
            while let Some(res) = object_store_joinset.join_next().await {
                log_join_result(res, "object store sync");
            }
        },
    )
    .await;

    Ok(())
}

fn log_join_result<T, E>(res: Result<Result<T, E>, tokio::task::JoinError>, context: &str)
where
    E: std::fmt::Debug,
{
    match res {
        Ok(Ok(_)) => info!("Successfully completed {context}."),
        Ok(Err(err)) => warn!("Failed to complete {context}. {err:?}"),
        Err(err) => error!("Issue joining {context} task: {err}"),
    }
}

/// A separate runtime for running all alert tasks
#[tokio::main(flavor = "multi_thread")]
pub async fn alert_runtime(mut rx: mpsc::Receiver<AlertTask>) -> Result<(), anyhow::Error> {
    let mut alert_tasks = HashMap::new();

    // this is the select! loop which will keep waiting for the alert task to finish or get cancelled
    while let Some(task) = rx.recv().await {
        match task {
            AlertTask::Create(alert) => {
                // check if the alert already exists
                if alert_tasks.contains_key(&alert.id) {
                    error!("Alert with id {} already exists", alert.id);
                    continue;
                }

                let alert = alert.clone();
                let id = alert.id;
                let handle = tokio::spawn(async move {
                    let mut retry_counter = 0;
                    let mut sleep_duration = alert.get_eval_frequency();
                    loop {
                        match alerts_utils::evaluate_alert(&alert).await {
                            Ok(_) => {
                                retry_counter = 0;
                            }
                            Err(err) => {
                                warn!(
                                    "Error while evaluation- {}\nRetrying after sleeping for 1 minute",
                                    err
                                );
                                sleep_duration = 1;
                                retry_counter += 1;

                                if retry_counter > 3 {
                                    error!(
                                        "Alert with id {} failed to evaluate after 3 retries with err- {}",
                                        id, err
                                    );
                                    break;
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(sleep_duration * 60)).await;
                    }
                });

                // store the handle in the map, since it is not awaited, it will keep on running
                alert_tasks.insert(id, handle);
            }
            AlertTask::Delete(ulid) => {
                // check if the alert exists
                if let Some(handle) = alert_tasks.remove(&ulid) {
                    // cancel the task
                    handle.abort();
                    warn!("Alert with id {} deleted", ulid);
                } else {
                    error!("Alert with id {} does not exist", ulid);
                }
            }
        }
    }
    Ok(())
}
