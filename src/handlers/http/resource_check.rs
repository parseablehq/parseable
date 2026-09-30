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

#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
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
use crate::metrics::{record_disk_metrics, record_process_metrics_sample};
use crate::parseable::PARSEABLE;

const PROCESS_METRICS_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(target_os = "linux")]
const CGROUP_V2_CPU_MAX_FILE: &str = "cpu.max";
#[cfg(target_os = "linux")]
const CGROUP_V1_CPU_QUOTA_FILE: &str = "cpu.cfs_quota_us";
#[cfg(target_os = "linux")]
const CGROUP_V1_CPU_PERIOD_FILE: &str = "cpu.cfs_period_us";

static SERVER_OK: LazyLock<Arc<AtomicBool>> = LazyLock::new(|| Arc::new(AtomicBool::new(true)));

#[cfg(target_os = "linux")]
fn cpu_quota_cores(quota: &str, period: &str) -> Result<Option<f64>, ()> {
    let quota = quota.trim();
    if quota == "max" || quota == "-1" {
        return Ok(None);
    }

    let quota = quota.parse::<f64>().map_err(|_| ())?;
    let period = period.trim().parse::<f64>().map_err(|_| ())?;
    if quota <= 0.0 || period <= 0.0 {
        return Err(());
    }

    Ok(Some(quota / period))
}

#[cfg(target_os = "linux")]
fn cgroup_directory(pathname: &str, root: &str, mount_point: &Path) -> Option<PathBuf> {
    let pathname = Path::new(pathname);
    if pathname == Path::new("/") {
        return Some(mount_point.to_path_buf());
    }
    Some(mount_point.join(pathname.strip_prefix(root).ok()?))
}

#[cfg(target_os = "linux")]
fn cgroup_cpu_limit_cores() -> Result<Option<f64>, ()> {
    let process = procfs::process::Process::myself().map_err(|_| ())?;
    let cgroups = process.cgroups().map_err(|_| ())?.0;
    let mounts = process.mountinfo().map_err(|_| ())?.0;

    if let (Some(cgroup), Some(mount)) = (
        cgroups.iter().find(|group| group.hierarchy == 0),
        mounts.iter().find(|mount| mount.fs_type == "cgroup2"),
    ) {
        let directory =
            cgroup_directory(&cgroup.pathname, &mount.root, &mount.mount_point).ok_or(())?;
        let cpu_max =
            std::fs::read_to_string(directory.join(CGROUP_V2_CPU_MAX_FILE)).map_err(|_| ())?;
        let mut values = cpu_max.split_whitespace();
        return cpu_quota_cores(values.next().ok_or(())?, values.next().ok_or(())?);
    }

    let cgroup = cgroups
        .iter()
        .find(|group| group.controllers.iter().any(|item| item == "cpu"))
        .ok_or(())?;
    let mount = mounts
        .iter()
        .find(|mount| mount.fs_type == "cgroup" && mount.super_options.contains_key("cpu"))
        .ok_or(())?;
    let directory = cgroup_directory(&cgroup.pathname, &mount.root, &mount.mount_point).ok_or(())?;
    let quota =
        std::fs::read_to_string(directory.join(CGROUP_V1_CPU_QUOTA_FILE)).map_err(|_| ())?;
    let period =
        std::fs::read_to_string(directory.join(CGROUP_V1_CPU_PERIOD_FILE)).map_err(|_| ())?;
    cpu_quota_cores(&quota, &period)
}

#[cfg(not(target_os = "linux"))]
fn cgroup_cpu_limit_cores() -> Result<Option<f64>, ()> {
    Err(())
}

pub fn cpu_limit_cores() -> f64 {
    match cgroup_cpu_limit_cores() {
        Ok(Some(limit)) => limit,
        Ok(None) => num_cpus::get() as f64,
        Err(()) => 0.0,
    }
}

async fn sample_process_metrics() {
    refresh_sys_info();
    let process_metrics = tokio::task::spawn_blocking(|| {
        let process_metrics = {
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
        };

        process_metrics.map(|(cpu_usage, memory_bytes, total_mem)| {
            (cpu_usage, memory_bytes, total_mem, cpu_limit_cores())
        })
    })
    .await
    .unwrap();
    if let Some((cpu_usage, memory_bytes, total_mem, cpu_limit_cores)) = process_metrics {
        record_process_metrics_sample(cpu_usage, memory_bytes, total_mem, cpu_limit_cores);
    }

    let staging_path = PARSEABLE.options.staging_dir().clone();
    let hot_tier_path = PARSEABLE.hot_tier_dir().clone();
    tokio::task::spawn_blocking(move || {
        record_disk_metrics("staging", &staging_path);
        if let Some(hot_tier_path) = hot_tier_path {
            record_disk_metrics("hottier", &hot_tier_path);
        }
    })
    .await
    .unwrap();
}

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
                    SERVER_OK.store(resource_ok, std::sync::atomic::Ordering::SeqCst);

                    // Log state changes
                    if resource_ok {
                        info!("Resource utilization back to normal - requests will be accepted");
                    } else {
                        warn!("Resource utilization too high - requests will be rejected");

                        // sleep for rejection duration (+ the check interval)
                        let rejection_delay = tokio::time::sleep(Duration::from_secs(
                            PARSEABLE.options.rejection_duration,
                        ));
                        tokio::pin!(rejection_delay);

                        loop {
                            select! {
                                _ = &mut rejection_delay => break,
                                _ = process_metrics_interval.tick() => {
                                    sample_process_metrics().await;
                                },
                                _ = &mut shutdown_rx => {
                                    trace!("Resource monitor shutting down");
                                    return;
                                }
                            }
                        }
                    }
                },
                _ = process_metrics_interval.tick() => {
                    sample_process_metrics().await;
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::cpu_quota_cores;

    #[test]
    fn parses_limited_and_unlimited_cpu_quotas() {
        assert_eq!(cpu_quota_cores("50000", "100000"), Ok(Some(0.5)));
        assert_eq!(cpu_quota_cores("max", "100000"), Ok(None));
        assert_eq!(cpu_quota_cores("-1", "100000"), Ok(None));
    }

    #[test]
    fn rejects_invalid_cpu_quotas() {
        assert_eq!(cpu_quota_cores("invalid", "100000"), Err(()));
        assert_eq!(cpu_quota_cores("50000", "0"), Err(()));
    }
}
