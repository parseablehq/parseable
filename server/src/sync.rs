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

use clokwerk::{AsyncScheduler, Job, Scheduler, TimeUnits};
use thread_priority::{ThreadBuilder, ThreadPriority};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::option::CONFIG;
use crate::{storage, STORAGE_UPLOAD_INTERVAL};

pub fn object_store_sync() -> (JoinHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();
    let mut inbox_rx = AssertUnwindSafe(inbox_rx);
    let handle = thread::spawn(move || {
        let res = catch_unwind(move || {
            let rt = actix_web::rt::System::new();
            rt.block_on(async {
                let mut scheduler = AsyncScheduler::new();
                scheduler
                    .every(STORAGE_UPLOAD_INTERVAL.seconds())
                    // Extra time interval is added so that this schedular does not race with local sync.
                    .plus(5u32.seconds())
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

pub fn run_local_sync() -> (JoinHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
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
