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
use std::panic::AssertUnwindSafe;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::{interval, sleep, Duration};

use crate::option::CONFIG;
use crate::{storage, STORAGE_UPLOAD_INTERVAL};

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
                .plus(5u32.seconds())
                .run(|| async {
                    if let Err(e) = CONFIG.storage().get_object_store().sync(false).await {
                        log::warn!("failed to sync local data with object store. {:?}", e);
                    }
                });

            let mut inbox_rx = AssertUnwindSafe(inbox_rx);
            let mut check_interval = interval(Duration::from_secs(1));

            loop {
                check_interval.tick().await;
                scheduler.run_pending().await;

                match inbox_rx.try_recv() {
                    Ok(_) => break,
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => continue,
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        log::warn!("Inbox channel closed unexpectedly");
                        break;
                    }
                }
            }
        }));

        match result {
            Ok(future) => {
                future.await;
            }
            Err(panic_error) => {
                log::error!("Panic in object store sync task: {:?}", panic_error);
                let _ = outbox_tx.send(());
            }
        }

        log::info!("Object store sync task ended");
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
        log::info!("Local sync task started");
        let mut inbox_rx = inbox_rx;

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut scheduler = AsyncScheduler::new();
            scheduler
                .every((storage::LOCAL_SYNC_INTERVAL as u32).seconds())
                .run(|| async {
                    crate::event::STREAM_WRITERS.unset_all();
                });

            loop {
                // Sleep for 50ms
                sleep(Duration::from_millis(50)).await;

                // Run any pending scheduled tasks
                scheduler.run_pending().await;

                // Check inbox
                match inbox_rx.try_recv() {
                    Ok(_) => break,
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => continue,
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        log::warn!("Inbox channel closed unexpectedly");
                        break;
                    }
                }
            }
        }));

        match result {
            Ok(future) => {
                future.await;
            }
            Err(panic_error) => {
                log::error!("Panic in local sync task: {:?}", panic_error);
            }
        }

        let _ = outbox_tx.send(());
        log::info!("Local sync task ended");
    });

    (handle, outbox_rx, inbox_tx)
}
