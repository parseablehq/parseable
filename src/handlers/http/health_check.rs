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

use std::sync::Arc;

use actix_web::{
    HttpResponse,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    error::Error,
    error::ErrorServiceUnavailable,
    middleware::Next,
};
use http::StatusCode;
use once_cell::sync::Lazy;
use tokio::{sync::Mutex, task::JoinSet};
use tracing::{error, info};

use crate::{parseable::PARSEABLE, storage::object_storage::sync_all_streams};

// Create a global variable to store signal status
pub static SIGNAL_RECEIVED: Lazy<Arc<Mutex<bool>>> = Lazy::new(|| Arc::new(Mutex::new(false)));

pub async fn liveness() -> HttpResponse {
    HttpResponse::new(StatusCode::OK)
}

pub async fn check_shutdown_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    // Acquire the shutdown flag to check if the server is shutting down.
    if *SIGNAL_RECEIVED.lock().await {
        // Return 503 Service Unavailable if the server is shutting down.
        Err(ErrorServiceUnavailable("Server is shutting down"))
    } else {
        // Continue processing the request if the server is not shutting down.
        next.call(req).await
    }
}

// This function is called when the server is shutting down
pub async fn shutdown() {
    // Set shutdown flag to true
    set_shutdown_flag().await;

    //sleep for 5 secs to allow any ongoing requests to finish
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Perform sync operations
    perform_sync_operations().await;

    // If collect_dataset_stats is enabled, perform sync operations
    // This is to ensure that all stats data is synced before the server shuts down
    if PARSEABLE.options.collect_dataset_stats {
        perform_sync_operations().await;
    }
}

async fn set_shutdown_flag() {
    let mut shutdown_flag = SIGNAL_RECEIVED.lock().await;
    *shutdown_flag = true;
}

async fn perform_sync_operations() {
    // Perform local sync
    perform_local_sync().await;
    // Perform object store sync
    perform_object_store_sync().await;
}

async fn perform_local_sync() {
    let mut local_sync_joinset = JoinSet::new();

    // Sync staging
    PARSEABLE
        .streams
        .flush_and_convert(&mut local_sync_joinset, false, true);

    while let Some(res) = local_sync_joinset.join_next().await {
        match res {
            Ok(Ok(_)) => info!("Successfully converted arrow files to parquet."),
            Ok(Err(err)) => error!("Failed to convert arrow files to parquet. {err:?}"),
            Err(err) => error!("Failed to join async task: {err}"),
        }
    }
}

async fn perform_object_store_sync() {
    // Sync object store
    let mut object_store_joinset = JoinSet::new();
    sync_all_streams(&mut object_store_joinset);

    while let Some(res) = object_store_joinset.join_next().await {
        match res {
            Ok(Ok(_)) => info!("Successfully synced all data to S3."),
            Ok(Err(err)) => error!("Failed to sync local data with object store. {err:?}"),
            Err(err) => error!("Failed to join async task: {err}"),
        }
    }
}

pub async fn readiness() -> HttpResponse {
    // Check the object store connection
    if PARSEABLE.storage.get_object_store().check().await.is_ok() {
        HttpResponse::new(StatusCode::OK)
    } else {
        HttpResponse::new(StatusCode::SERVICE_UNAVAILABLE)
    }
}
