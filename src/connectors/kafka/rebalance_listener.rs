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
