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

use clokwerk::{AsyncScheduler, Job, TimeUnits};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Instant;
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use tokio::{select, task};
use tracing::{error, info, warn};

use crate::alerts::{alerts_utils, AlertConfig, AlertError};
use crate::option::CONFIG;
use crate::staging::STAGING;
use crate::{storage, STORAGE_CONVERSION_INTERVAL, STORAGE_UPLOAD_INTERVAL};

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
                    "Task '{task_name}' started at: {start_time:?} is taking longer than expected: (threshold: {threshold:?})",
                );
                warned_once = true;
            },
            res = &mut future => {
                if warned_once {
                    warn!(
                        "Task '{task_name}' started at: {start_time:?} took longer than expected: {:?} (threshold: {threshold:?})",
                        start_time.elapsed()
                    );
                }
                break res.expect("Task handle shouldn't error");
            }
        }
    }
}

pub async fn object_store_sync() -> (
    task::JoinHandle<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();

    let handle = task::spawn(async move {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut scheduler = AsyncScheduler::new();
            scheduler
                .every(STORAGE_UPLOAD_INTERVAL.seconds())
                // .plus(5u32.seconds())
                .run(|| async {
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
                });

            let mut inbox_rx = AssertUnwindSafe(inbox_rx);

            loop {
                select! {
                    _ = scheduler.run_pending() => {},
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

pub async fn arrow_conversion() -> (
    task::JoinHandle<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();

    let handle = task::spawn(async move {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut scheduler = AsyncScheduler::new();
            scheduler
                .every(STORAGE_CONVERSION_INTERVAL.seconds())
                .plus(5u32.seconds())
                .run(|| async {
                    if let Err(e) = monitor_task_duration(
                        "arrow_conversion",
                        Duration::from_secs(30),
                        || async { CONFIG.storage().get_object_store().conversion(false).await },
                    )
                    .await
                    {
                        warn!("failed to convert local arrow data to parquet. {e:?}");
                    }
                });

            let mut inbox_rx = AssertUnwindSafe(inbox_rx);

            loop {
                select! {
                    _ = scheduler.run_pending() => {},
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

pub async fn run_local_sync() -> (
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
            let mut scheduler = AsyncScheduler::new();
            scheduler
                .every((storage::LOCAL_SYNC_INTERVAL as u32).seconds())
                .run(|| async {
                    STAGING.flush_all();
                });

            loop {
                select! {
                    _ = scheduler.run_pending() => {},
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

pub async fn schedule_alert_task(
    eval_frequency: u32,
    alert: AlertConfig,
) -> Result<
    (
        task::JoinHandle<()>,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
    ),
    AlertError,
> {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();

    let handle = tokio::task::spawn(async move {
        info!("new alert task started for {alert:?}");

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut scheduler = AsyncScheduler::new();
            scheduler.every((eval_frequency).minutes()).run(move || {
                let alert_val = alert.clone();
                async move {
                    match alerts_utils::evaluate_alert(&alert_val).await {
                        Ok(_) => {}
                        Err(err) => error!("Error while evaluation- {err}"),
                    }
                }
            });
            let mut inbox_rx = AssertUnwindSafe(inbox_rx);

            loop {
                select! {
                    _ = scheduler.run_pending() => {},
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
    Ok((handle, outbox_rx, inbox_tx))
}
