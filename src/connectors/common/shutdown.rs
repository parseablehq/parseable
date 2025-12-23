/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Debug)]
pub struct Shutdown {
    cancel_token: CancellationToken,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
}

impl Shutdown {
    pub fn start(&self) {
        self.cancel_token.cancel();
    }

    pub async fn recv(&self) {
        self.cancel_token.cancelled().await;
    }

    pub async fn signal_listener(&self) {
        let ctrl_c_signal = tokio::signal::ctrl_c();
        #[cfg(unix)]
        let mut sigterm_signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        #[cfg(unix)]
        tokio::select! {
            _ = ctrl_c_signal => {},
            _ = sigterm_signal.recv() => {}
        }
        #[cfg(windows)]
        let _ = ctrl_c_signal.await;

        warn!("Shutdown signal received!");
        self.start();
    }

    pub async fn complete(self) {
        drop(self.shutdown_complete_tx);
        self.shutdown_complete_rx.unwrap().recv().await;
        info!("Shutdown complete!")
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        let cancel_token = CancellationToken::new();
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        Self {
            cancel_token,
            shutdown_complete_tx,
            shutdown_complete_rx: Some(shutdown_complete_rx),
        }
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        Self {
            cancel_token: self.cancel_token.clone(),
            shutdown_complete_tx: self.shutdown_complete_tx.clone(),
            shutdown_complete_rx: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_shutdown_recv() {
        let shutdown = Shutdown::default();
        let shutdown_clone = shutdown.clone();
        // receive shutdown task
        let task = tokio::spawn(async move {
            shutdown_clone.recv().await;
            1
        });
        // start shutdown task after 200 ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            shutdown.start();
        });
        // if shutdown is not received within 5 seconds, fail test
        let check_value = tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("Shutdown not received within 5 seconds"),
            v = task => v.unwrap(),
        };
        assert_eq!(check_value, 1);
    }

    #[tokio::test]
    async fn test_shutdown_wait_for_complete() {
        let shutdown = Shutdown::default();
        let shutdown_clone = shutdown.clone();
        let check_value: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let check_value_clone = Arc::clone(&check_value);
        // receive shutdown task
        tokio::spawn(async move {
            shutdown_clone.recv().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let mut check: std::sync::MutexGuard<'_, bool> = check_value_clone.lock().unwrap();
            *check = true;
        });
        shutdown.start();
        shutdown.complete().await;
        let check = check_value.lock().unwrap();
        assert!(*check, "shutdown did not successfully wait for complete");
    }
}
