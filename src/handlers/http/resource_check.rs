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

use std::sync::{Arc, LazyLock, atomic::AtomicBool};

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    error::Error,
    error::ErrorServiceUnavailable,
    middleware::Next,
};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use tokio::{
    select,
    time::{Duration, interval},
};
use tracing::{info, trace, warn};

use crate::analytics::{SYS_INFO, refresh_sys_info};
use crate::metrics::record_process_metrics_sample;
use crate::parseable::PARSEABLE;

const PROCESS_METRICS_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

static SERVER_OK: LazyLock<Arc<AtomicBool>> = LazyLock::new(|| Arc::new(AtomicBool::new(true)));

/// Spawn a background task to monitor system resources
pub fn spawn_resource_monitor(shutdown_rx: tokio::sync::oneshot::Receiver<()>) {
    tokio::spawn(async move {
        let resource_check_interval = PARSEABLE.options.resource_check_interval;
        let mut check_interval = interval(Duration::from_secs(resource_check_interval));
        let mut process_metrics_interval = interval(PROCESS_METRICS_SAMPLE_INTERVAL);
        let mut shutdown_rx = shutdown_rx;

        let memory_threshold = (PARSEABLE.options.memory_utilization_threshold / 100.0) as f64;

        loop {
            select! {
                _ = check_interval.tick() => {
                    if !PARSEABLE.options.resource_check_enabled {
                        continue;
                    }
                    refresh_sys_info();

                    let mut resource_ok = true;

                    let mut s = System::new_with_specifics(
                        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
                    );
                    if let Some(cgroup) = s.cgroup_limits() {
                        if (cgroup.rss as f64) > memory_threshold * (cgroup.total_memory as f64) {
                            resource_ok = false;
                        }
                    } else {
                        s.refresh_memory();
                        if (s.used_memory() as f64) > memory_threshold * (s.total_memory() as f64) {
                            resource_ok = false;
                        }
                    }
                    let previous_state = SERVER_OK.load(std::sync::atomic::Ordering::SeqCst);
                    SERVER_OK.store(resource_ok, std::sync::atomic::Ordering::SeqCst);

                    // Log state changes
                    if previous_state != resource_ok {
                        if resource_ok {
                            info!("Resource utilization back to normal - requests will be accepted");
                        } else {
                            warn!("Resource utilization too high - requests will be rejected");
                        }
                    }
                },
                _ = process_metrics_interval.tick() => {
                    refresh_sys_info();
                    let process_metrics = tokio::task::spawn_blocking(|| {
                        let sys = SYS_INFO.lock().unwrap();
                        let total_mem = if let Some(cgroup) = sys.cgroup_limits() {
                            cgroup.total_memory
                        } else {
                            sys.total_memory()
                        };
                        sysinfo::get_current_pid()
                            .ok()
                            .and_then(|pid| sys.process(pid))
                            .map(|process| (process.cpu_usage() as f64, process.memory(), total_mem))
                    }).await.unwrap();
                    if let Some((cpu_usage, memory_bytes, total_mem)) = process_metrics {
                        record_process_metrics_sample(cpu_usage, memory_bytes, total_mem);
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
    let resource_ok = SERVER_OK.load(std::sync::atomic::Ordering::SeqCst);

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
