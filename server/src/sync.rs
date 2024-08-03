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

use std::panic::{self, AssertUnwindSafe};
use tokio::select;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::{interval, Duration};

use crate::option::CONFIG;
use crate::{storage, STORAGE_UPLOAD_INTERVAL};

pub async fn object_store_sync() -> (
    task::JoinHandle<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, mut inbox_rx) = oneshot::channel::<()>();

    let handle = task::spawn(async move {
        let mut interval = interval(Duration::from_secs((STORAGE_UPLOAD_INTERVAL + 5).into()));

        loop {
            select! {
                _ = interval.tick() => {
                    match task::spawn(async {
                        CONFIG.storage().get_object_store().sync().await
                    }).await {
                        Ok(Ok(_)) => {
                            log::info!("Successfully synced local data with object store.");
                        }
                        Ok(Err(e)) => {
                            log::warn!("Failed to sync local data with object store: {:?}", e);
                        }
                        Err(e) => {
                            log::error!("Task panicked during sync: {:?}", e);
                            if let Err(send_err) = outbox_tx.send(()) {
                                log::error!("Failed to send outbox message: {:?}", send_err);
                            }
                            break;
                        }
                    }
                }
                _ = &mut inbox_rx => break,
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
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
        let result = panic::catch_unwind(AssertUnwindSafe(|| async move {
            let mut interval = interval(Duration::from_secs(storage::LOCAL_SYNC_INTERVAL));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        crate::event::STREAM_WRITERS.unset_all();
                    }
                    _ = &mut inbox_rx => {
                        log::info!("Received signal to stop local sync");
                        return;  // Exit the async block when signaled
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));

        match result {
            Ok(future) => {
                future.await; // We don't need to check the result here
            }
            Err(panic_error) => {
                log::error!("Panic in local sync task: {:?}", panic_error);
            }
        }

        // Signal that the task has ended, regardless of how it ended
        let _ = outbox_tx.send(());
        log::info!("Local sync task ended");
    });

    (handle, outbox_rx, inbox_tx)
}
