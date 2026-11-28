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
#[cfg(target_os = "linux")]
use std::sync::OnceLock;
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

use crate::analytics::{SYS_INFO, refresh_process_info};
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

/// Where this process's CPU quota is read from. The cgroup a process is in,
/// and where its hierarchy is mounted, do not change while it runs, but the
/// quota in these files can (an in-place resize), so only the location is
/// resolved once and the files are read on every sample.
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq)]
enum CgroupCpuFiles {
    V2 { cpu_max: PathBuf },
    V1 { quota: PathBuf, period: PathBuf },
}

/// `None` when the process has no CPU cgroup this code can read, which is
/// as permanent as a resolved location.
#[cfg(target_os = "linux")]
static CGROUP_CPU_FILES: OnceLock<Option<CgroupCpuFiles>> = OnceLock::new();

/// Finds the CPU quota files from `/proc/self/cgroup` and
/// `/proc/self/mountinfo`. `Err` when those cannot be read, which may be
/// transient and is not remembered.
#[cfg(target_os = "linux")]
fn resolve_cgroup_cpu_files() -> Result<Option<CgroupCpuFiles>, ()> {
    let process = procfs::process::Process::myself().map_err(|_| ())?;
    let cgroups = process.cgroups().map_err(|_| ())?.0;
    let mounts = process.mountinfo().map_err(|_| ())?.0;

    if let (Some(cgroup), Some(mount)) = (
        cgroups.iter().find(|group| group.hierarchy == 0),
        mounts.iter().find(|mount| mount.fs_type == "cgroup2"),
    ) {
        return Ok(
            cgroup_directory(&cgroup.pathname, &mount.root, &mount.mount_point).map(|directory| {
                CgroupCpuFiles::V2 {
                    cpu_max: directory.join(CGROUP_V2_CPU_MAX_FILE),
                }
            }),
        );
    }

    let Some(cgroup) = cgroups
        .iter()
        .find(|group| group.controllers.iter().any(|item| item == "cpu"))
    else {
        return Ok(None);
    };
    let Some(mount) = mounts
        .iter()
        .find(|mount| mount.fs_type == "cgroup" && mount.super_options.contains_key("cpu"))
    else {
        return Ok(None);
    };
    Ok(
        cgroup_directory(&cgroup.pathname, &mount.root, &mount.mount_point).map(|directory| {
            CgroupCpuFiles::V1 {
                quota: directory.join(CGROUP_V1_CPU_QUOTA_FILE),
                period: directory.join(CGROUP_V1_CPU_PERIOD_FILE),
            }
        }),
    )
}

#[cfg(target_os = "linux")]
fn read_cgroup_cpu_limit(files: &CgroupCpuFiles) -> Result<Option<f64>, ()> {
    match files {
        CgroupCpuFiles::V2 { cpu_max } => {
            let cpu_max = std::fs::read_to_string(cpu_max).map_err(|_| ())?;
            let mut values = cpu_max.split_whitespace();
            cpu_quota_cores(values.next().ok_or(())?, values.next().ok_or(())?)
        }
        CgroupCpuFiles::V1 { quota, period } => {
            let quota = std::fs::read_to_string(quota).map_err(|_| ())?;
            let period = std::fs::read_to_string(period).map_err(|_| ())?;
            cpu_quota_cores(&quota, &period)
        }
    }
}

#[cfg(target_os = "linux")]
fn cgroup_cpu_limit_cores() -> Result<Option<f64>, ()> {
    let files = match CGROUP_CPU_FILES.get() {
        Some(files) => files,
        None => {
            let resolved = resolve_cgroup_cpu_files()?;
            CGROUP_CPU_FILES.get_or_init(|| resolved)
        }
    };
    read_cgroup_cpu_limit(files.as_ref().ok_or(())?)
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
    // Refreshing and reading `/proc` blocks, so all of it runs off the
    // async workers.
    let process_metrics = tokio::task::spawn_blocking(|| {
        let process_metrics = sysinfo::get_current_pid().ok().and_then(|pid| {
            let mut sys = SYS_INFO.lock().unwrap();
            refresh_process_info(&mut sys, pid);
            let total_mem = if let Some(cgroup) = sys.cgroup_limits() {
                cgroup.total_memory
            } else {
                sys.total_memory()
            };
            sys.process(pid)
                .map(|process| (process.cpu_usage() as f64, process.memory(), total_mem))
        });

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

/// Whether memory use is within `memory_threshold` (a fraction) of the
/// cgroup limit, or of host memory outside a cgroup. Blocking: it reads
/// `/proc` and the cgroup files.
fn memory_within_threshold(memory_threshold: f64) -> bool {
    // Created with memory already refreshed.
    let system = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
    );
    if let Some(cgroup) = system.cgroup_limits() {
        (cgroup.rss as f64) <= memory_threshold * (cgroup.total_memory as f64)
    } else {
        (system.used_memory() as f64) <= memory_threshold * (system.total_memory() as f64)
    }
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

                    // Off the async workers: it reads `/proc` and cgroup files.
                    let resource_ok = match tokio::task::spawn_blocking(move || {
                        memory_within_threshold(memory_threshold)
                    })
                    .await
                    {
                        Ok(resource_ok) => resource_ok,
                        Err(error) => {
                            warn!("Resource check failed, keeping the previous state: {error}");
                            continue;
                        }
                    };
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
    use super::{
        CGROUP_CPU_FILES, CgroupCpuFiles, cgroup_cpu_limit_cores, cpu_quota_cores,
        read_cgroup_cpu_limit, resolve_cgroup_cpu_files,
    };

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

    #[test]
    fn cached_cgroup_location_rereads_the_quota_files() {
        let directory = tempfile::tempdir().unwrap();
        let cpu_max = directory.path().join("cpu.max");
        let v2 = CgroupCpuFiles::V2 {
            cpu_max: cpu_max.clone(),
        };
        // A missing file is an error, as before.
        assert_eq!(read_cgroup_cpu_limit(&v2), Err(()));
        std::fs::write(&cpu_max, "50000 100000\n").unwrap();
        assert_eq!(read_cgroup_cpu_limit(&v2), Ok(Some(0.5)));
        // A changed quota is seen on the next read of the same location.
        std::fs::write(&cpu_max, "max 100000\n").unwrap();
        assert_eq!(read_cgroup_cpu_limit(&v2), Ok(None));
        std::fs::write(&cpu_max, "200000 100000\n").unwrap();
        assert_eq!(read_cgroup_cpu_limit(&v2), Ok(Some(2.0)));
        std::fs::write(&cpu_max, "50000\n").unwrap();
        assert_eq!(read_cgroup_cpu_limit(&v2), Err(()));

        let quota = directory.path().join("cpu.cfs_quota_us");
        let period = directory.path().join("cpu.cfs_period_us");
        let v1 = CgroupCpuFiles::V1 {
            quota: quota.clone(),
            period: period.clone(),
        };
        std::fs::write(&quota, "25000\n").unwrap();
        assert_eq!(read_cgroup_cpu_limit(&v1), Err(()));
        std::fs::write(&period, "100000\n").unwrap();
        assert_eq!(read_cgroup_cpu_limit(&v1), Ok(Some(0.25)));
        std::fs::write(&quota, "-1\n").unwrap();
        assert_eq!(read_cgroup_cpu_limit(&v1), Ok(None));
    }

    #[test]
    fn cgroup_location_is_resolved_once() {
        // Whatever this host has, repeated reads agree with a fresh resolution.
        let first = cgroup_cpu_limit_cores();
        let resolved = CGROUP_CPU_FILES.get().cloned();
        if let Ok(fresh) = resolve_cgroup_cpu_files() {
            assert_eq!(resolved, Some(fresh.clone()));
            let expected = fresh.as_ref().ok_or(()).and_then(read_cgroup_cpu_limit);
            assert_eq!(first, expected);
        }
        assert_eq!(cgroup_cpu_limit_cores(), first);
        assert_eq!(CGROUP_CPU_FILES.get().cloned(), resolved);
    }
}
