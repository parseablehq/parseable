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

use std::collections::VecDeque;
use std::sync::{Arc, LazyLock, Mutex, atomic::AtomicBool};

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

#[derive(Debug, Clone)]
struct ResourceSample {
    cpu_usage: f32,
    memory_usage: f32,
    timestamp: std::time::Instant,
}

/// Structure to maintain rolling average of resource utilization
struct ResourceHistory {
    samples: VecDeque<ResourceSample>,
    window_duration: Duration,
}

impl ResourceHistory {
    fn new(window_duration: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window_duration,
        }
    }

    fn add_sample(&mut self, cpu_usage: f32, memory_usage: f32) {
        let now = std::time::Instant::now();
        let sample = ResourceSample {
            cpu_usage,
            memory_usage,
            timestamp: now,
        };

        // Add new sample
        self.samples.push_back(sample);

        // Remove old samples outside the window
        let cutoff_time = now - self.window_duration;
        while let Some(front) = self.samples.front() {
            if front.timestamp < cutoff_time {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn get_average(&self) -> Option<(f32, f32)> {
        if self.samples.is_empty() {
            return None;
        }

        let count = self.samples.len() as f32;
        let (total_cpu, total_memory) =
            self.samples
                .iter()
                .fold((0.0, 0.0), |(cpu_acc, mem_acc), sample| {
                    (cpu_acc + sample.cpu_usage, mem_acc + sample.memory_usage)
                });

        Some((total_cpu / count, total_memory / count))
    }

    fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

static RESOURCE_CHECK_ENABLED: LazyLock<Arc<AtomicBool>> =
    LazyLock::new(|| Arc::new(AtomicBool::new(true)));

static RESOURCE_HISTORY: LazyLock<Arc<Mutex<ResourceHistory>>> =
    LazyLock::new(|| Arc::new(Mutex::new(ResourceHistory::new(Duration::from_secs(120)))));

/// Spawn a background task to monitor system resources
pub fn spawn_resource_monitor(shutdown_rx: tokio::sync::oneshot::Receiver<()>) {
    tokio::spawn(async move {
        let resource_check_interval = PARSEABLE.options.resource_check_interval;
        let mut check_interval = interval(Duration::from_secs(resource_check_interval));
        let mut shutdown_rx = shutdown_rx;

        let cpu_threshold = PARSEABLE.options.cpu_utilization_threshold;
        let memory_threshold = PARSEABLE.options.memory_utilization_threshold;

        info!(
            "Resource monitor started with thresholds - CPU: {:.1}%, Memory: {:.1}% (2-minute rolling average)",
            cpu_threshold, memory_threshold
        );

        // Calculate minimum samples needed for a reliable 2-minute average
        let min_samples_for_decision = std::cmp::max(1, 120 / resource_check_interval as usize);

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

                    // Calculate memory usage percentage
                    let memory_usage = if total_memory > 0.0 {
                        (used_memory / total_memory) * 100.0
                    } else {
                        0.0
                    };

                    // Add current sample to history
                    {
                        let mut history = RESOURCE_HISTORY.lock().unwrap();
                        history.add_sample(cpu_usage, memory_usage);
                    }

                    // Get rolling averages
                    let (avg_cpu, avg_memory, sample_count) = {
                        let history = RESOURCE_HISTORY.lock().unwrap();
                        if let Some((cpu_avg, mem_avg)) = history.get_average() {
                            (cpu_avg, mem_avg, history.sample_count())
                        } else {
                            (cpu_usage, memory_usage, 1) // Fallback to current values if no history
                        }
                    };

                    // Log current and average resource usage
                    info!(
                        "Resource usage - Current: CPU {:.1}%, Memory {:.1}% | 2-min avg: CPU {:.1}%, Memory {:.1}% (samples: {})",
                        cpu_usage, memory_usage, avg_cpu, avg_memory, sample_count
                    );

                    // Only make decisions based on rolling average if we have enough samples
                    let (decision_cpu, decision_memory) = if sample_count >= min_samples_for_decision {
                        (avg_cpu, avg_memory)
                    } else {
                        // For the first few minutes, use current values but be more conservative
                        info!("Still warming up resource history (need {} samples, have {})", min_samples_for_decision, sample_count);
                        (cpu_usage, memory_usage)
                    };

                    let mut resource_ok = true;

                    // Check memory utilization against rolling average
                    if decision_memory > memory_threshold {
                        warn!(
                            "High memory usage detected: 2-min avg {:.1}% (threshold: {:.1}%, current: {:.1}%)",
                            decision_memory, memory_threshold, memory_usage
                        );
                        resource_ok = false;
                    }

                    // Check CPU utilization against rolling average
                    if decision_cpu > cpu_threshold {
                        warn!(
                            "High CPU usage detected: 2-min avg {:.1}% (threshold: {:.1}%, current: {:.1}%)",
                            decision_cpu, cpu_threshold, cpu_usage
                        );
                        resource_ok = false;
                    }

                    let previous_state = RESOURCE_CHECK_ENABLED.load(std::sync::atomic::Ordering::SeqCst);
                    RESOURCE_CHECK_ENABLED.store(resource_ok, std::sync::atomic::Ordering::SeqCst);

                    // Log state changes
                    if previous_state != resource_ok {
                        if resource_ok {
                            info!("Resource utilization back to normal (2-min avg: CPU {:.1}%, Memory {:.1}%) - requests will be accepted", avg_cpu, avg_memory);
                        } else {
                            warn!("Resource utilization too high (2-min avg: CPU {:.1}%, Memory {:.1}%) - requests will be rejected", avg_cpu, avg_memory);
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
/// Returns 503 Service Unavailable if resources are over-utilized (based on 2-minute rolling average)
pub async fn check_resource_utilization_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let resource_ok = RESOURCE_CHECK_ENABLED.load(std::sync::atomic::Ordering::SeqCst);

    if !resource_ok {
        let error_msg = "Server resources over-utilized (based on 2-minute rolling average)";
        warn!(
            "Rejecting request to {} due to resource constraints (2-minute average above threshold)",
            req.path()
        );
        return Err(ErrorServiceUnavailable(error_msg));
    }

    // Continue processing the request if resource utilization is within limits
    next.call(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_resource_history_basic() {
        let mut history = ResourceHistory::new(Duration::from_secs(60));

        // Add some samples
        history.add_sample(50.0, 60.0);
        history.add_sample(70.0, 80.0);

        let (avg_cpu, avg_memory) = history.get_average().unwrap();
        assert_eq!(avg_cpu, 60.0); // (50 + 70) / 2
        assert_eq!(avg_memory, 70.0); // (60 + 80) / 2
        assert_eq!(history.sample_count(), 2);
    }

    #[test]
    fn test_resource_history_window_cleanup() {
        let mut history = ResourceHistory::new(Duration::from_millis(100));

        // Add samples
        history.add_sample(50.0, 60.0);
        std::thread::sleep(Duration::from_millis(50));
        history.add_sample(70.0, 80.0);

        // Both samples should be present
        assert_eq!(history.sample_count(), 2);

        // Wait for first sample to expire
        std::thread::sleep(Duration::from_millis(100));
        history.add_sample(90.0, 100.0);

        // Old samples should be cleaned up, only recent samples remain
        assert!(history.sample_count() <= 2);

        let (avg_cpu, avg_memory) = history.get_average().unwrap();
        // Should be average of recent samples only
        assert!(avg_cpu >= 70.0);
        assert!(avg_memory >= 80.0);
    }

    #[test]
    fn test_resource_history_empty() {
        let history = ResourceHistory::new(Duration::from_secs(60));
        assert!(history.get_average().is_none());
        assert_eq!(history.sample_count(), 0);
    }

    #[test]
    fn test_resource_history_single_sample() {
        let mut history = ResourceHistory::new(Duration::from_secs(60));
        history.add_sample(75.5, 85.3);

        let (avg_cpu, avg_memory) = history.get_average().unwrap();
        assert_eq!(avg_cpu, 75.5);
        assert_eq!(avg_memory, 85.3);
        assert_eq!(history.sample_count(), 1);
    }
}
