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

use crate::connectors::common::shutdown::Shutdown;
use crate::connectors::kafka::state::StreamState;
use crate::connectors::kafka::RebalanceEvent;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::{runtime::Handle, sync::mpsc::Receiver};
use tracing::{info, warn};

pub struct RebalanceListener {
    rebalance_rx: Receiver<RebalanceEvent>,
    stream_state: Arc<RwLock<StreamState>>,
    shutdown_handle: Shutdown,
}

impl RebalanceListener {
    pub fn new(
        rebalance_rx: Receiver<RebalanceEvent>,
        stream_state: Arc<RwLock<StreamState>>,
        shutdown_handle: Shutdown,
    ) -> Self {
        Self {
            rebalance_rx,
            stream_state,
            shutdown_handle,
        }
    }

    pub fn start(self) {
        let mut rebalance_receiver = self.rebalance_rx;
        let stream_state = self.stream_state.clone();
        let shutdown_handle = self.shutdown_handle.clone();
        let tokio_runtime_handle = Handle::current();

        std::thread::spawn(move || {
            tokio_runtime_handle.block_on(async move {
                loop {
                    tokio::select! {
                        rebalance = rebalance_receiver.recv() => {
                            match rebalance  {
                                Some(RebalanceEvent::Assign(tpl)) => info!("RebalanceEvent Assign: {:?}", tpl),
                                Some(RebalanceEvent::Revoke(tpl, callback)) => {
                                    info!("RebalanceEvent Revoke: {:?}", tpl);
                                    let mut stream_state = stream_state.write().await;
                                    stream_state.terminate_partition_streams(tpl).await;
                                    drop(stream_state);
                                    
                                    if let Err(err) = callback.send(()) {
                                        warn!("Error during sending response to context. Cause: {:?}", err);
                                    }
                                    info!("Finished Rebalance Revoke");
                                }
                                None => {
                                    info!("Rebalance event sender is closed!");
                                    break
                                }
                            }
                        },
                        _ = shutdown_handle.recv() => {
                            info!("Gracefully stopping rebalance listener!");   
                            break;
                        },
                    }
                }
            })
        });
    }
}
