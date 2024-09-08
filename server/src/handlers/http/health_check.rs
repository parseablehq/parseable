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

use crate::option::CONFIG;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{oneshot, Mutex};
use tokio::time::{sleep, Duration};

// Create a global variable to store signal status
lazy_static! {
    static ref SIGNAL_RECEIVED: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
}

pub async fn liveness() -> HttpResponse {
    HttpResponse::new(StatusCode::OK)
}

pub async fn handle_signals(shutdown_signal: Arc<Mutex<Option<oneshot::Sender<()>>>>) {
    let signal_received = SIGNAL_RECEIVED.clone();

    let mut sigterm =
        signal(SignalKind::terminate()).expect("Failed to set up SIGTERM signal handler");
    log::info!("Signal handler task started");

    // Block until SIGTERM is received
    match sigterm.recv().await {
        Some(_) => {
            log::info!("Received SIGTERM signal at Readiness Probe Handler");

            // Set the shutdown flag to true
            let mut shutdown_flag = signal_received.lock().await;
            *shutdown_flag = true;

            // Trigger graceful shutdown
            if let Some(shutdown_sender) = shutdown_signal.lock().await.take() {
                let _ = shutdown_sender.send(());
            }

            // Delay to allow readiness probe to return SERVICE_UNAVAILABLE
            let _ = sleep(Duration::from_secs(20)).await;

            // Sync to local
            crate::event::STREAM_WRITERS.unset_all();

            // Sync to S3
            if let Err(e) = CONFIG.storage().get_object_store().sync().await {
                log::warn!("Failed to sync local data with object store. {:?}", e);
            }

            log::info!("Local and S3 Sync done, handler SIGTERM completed.");
        }
        None => {
            log::info!("Signal handler received None, indicating an error or end of stream");
        }
    }

    log::info!("Signal handler task completed");
}

pub async fn readiness() -> HttpResponse {
    // Check if the application has received a shutdown signal
    let shutdown_flag = SIGNAL_RECEIVED.lock().await;
    if *shutdown_flag {
        return HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE);
    }

    // Check the object store connection
    if CONFIG.storage().get_object_store().check().await.is_ok() {
        HttpResponse::new(StatusCode::OK)
    } else {
        HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE)
    }
}
