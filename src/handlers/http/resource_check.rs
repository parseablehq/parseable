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

use std::sync::{Arc, LazyLock, atomic::AtomicBool};

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    error::Error,
    error::ErrorServiceUnavailable,
    middleware::Next,
};
use tokio::{
    select,
    time::{Duration, interval},
};
use tracing::{info, trace, warn};

use crate::analytics::{SYS_INFO, refresh_sys_info};
use crate::parseable::PARSEABLE;

static RESOURCE_CHECK_ENABLED: LazyLock<Arc<AtomicBool>> =
    LazyLock::new(|| Arc::new(AtomicBool::new(false)));

/// Spawn a background task to monitor system resources
pub fn spawn_resource_monitor(shutdown_rx: tokio::sync::oneshot::Receiver<()>) {
    tokio::spawn(async move {
        let resource_check_interval = PARSEABLE.options.resource_check_interval;
        let mut check_interval = interval(Duration::from_secs(resource_check_interval));
        let mut shutdown_rx = shutdown_rx;

        let cpu_threshold = PARSEABLE.options.cpu_utilization_threshold;
        let memory_threshold = PARSEABLE.options.memory_utilization_threshold;

        info!(
            "Resource monitor started with thresholds - CPU: {:.1}%, Memory: {:.1}%",
            cpu_threshold, memory_threshold
        );
        loop {
            select! {
                _ = check_interval.tick() => {
                    trace!("Checking system resource utilization...");

                    refresh_sys_info();
                    let (used_memory, total_memory, cpu_usage) = tokio::task::spawn_blocking(|| {
                        let sys = SYS_INFO.lock().unwrap();
                        let used_memory = sys.used_memory() as f32;
                        let total_memory = sys.total_memory() as f32;
                        let cpu_usage = sys.global_cpu_usage();
                        (used_memory, total_memory, cpu_usage)
                    }).await.unwrap();

                    let mut resource_ok = true;

                    // Calculate memory usage percentage
                    let memory_usage = if total_memory > 0.0 {
                        (used_memory / total_memory) * 100.0
                    } else {
                        0.0
                    };

                    // Log current resource usage every few checks for debugging
                    info!("Current resource usage - CPU: {:.1}%, Memory: {:.1}% ({:.1}GB/{:.1}GB)",
                          cpu_usage, memory_usage,
                          used_memory / 1024.0 / 1024.0 / 1024.0,
                          total_memory / 1024.0 / 1024.0 / 1024.0);

                    // Check memory utilization
                    if memory_usage > memory_threshold {
                        warn!("High memory usage detected: {:.1}% (threshold: {:.1}%)",
                              memory_usage, memory_threshold);
                        resource_ok = false;
                    }

                    // Check CPU utilization
                    if cpu_usage > cpu_threshold {
                        warn!("High CPU usage detected: {:.1}% (threshold: {:.1}%)",
                              cpu_usage, cpu_threshold);
                        resource_ok = false;
                    }

                    let previous_state = RESOURCE_CHECK_ENABLED.load(std::sync::atomic::Ordering::SeqCst);
                    RESOURCE_CHECK_ENABLED.store(resource_ok, std::sync::atomic::Ordering::SeqCst);

                    // Log state changes
                    if previous_state != resource_ok {
                        if resource_ok {
                            info!("Resource utilization back to normal - requests will be accepted");
                        } else {
                            warn!("Resource utilization too high - requests will be rejected");
                        }
                    }
                },
                _ = &mut shutdown_rx => {
                    trace!("Resource monitor shutting down");
                    break;
                }
            }
        }
    });
}

/// Middleware to check system resource utilization before processing requests
/// Returns 503 Service Unavailable if resources are over-utilized
pub async fn check_resource_utilization_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let resource_ok = RESOURCE_CHECK_ENABLED.load(std::sync::atomic::Ordering::SeqCst);

    if !resource_ok {
        let error_msg = "Server resources over-utilized";
        warn!(
            "Rejecting request to {} due to resource constraints",
            req.path()
        );
        return Err(ErrorServiceUnavailable(error_msg));
    }

    // Continue processing the request if resource utilization is within limits
    next.call(req).await
}
