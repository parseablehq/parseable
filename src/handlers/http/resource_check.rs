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

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    error::Error,
    error::ErrorServiceUnavailable,
    middleware::Next,
};
use sysinfo::System;
use tracing::warn;

const CPU_UTILIZATION_THRESHOLD: f32 = 90.0;
const MEMORY_UTILIZATION_THRESHOLD: f32 = 90.0;

/// Middleware to check system resource utilization before processing requests
/// Returns 503 Service Unavailable if CPU or memory usage exceeds thresholds
pub async fn check_resource_utilization_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    
    let mut sys = System::new_all();
    sys.refresh_cpu_usage();
    sys.refresh_memory();
    
    let used_memory = sys.used_memory() as f32;
    let total_memory = sys.total_memory() as f32;
    
    // Check memory utilization
    if total_memory > 0.0 {
        let memory_usage = (used_memory / total_memory) * 100.0;
        if memory_usage > MEMORY_UTILIZATION_THRESHOLD {
            let error_msg = format!("Memory is over-utilized: {:.1}%", memory_usage);
            warn!(
                "Rejecting request to {} due to high memory usage: {:.1}% (threshold: {:.1}%)", 
                req.path(), 
                memory_usage,
                MEMORY_UTILIZATION_THRESHOLD
            );
            return Err(ErrorServiceUnavailable(error_msg));
        }
    }

    // Check CPU utilization
    let cpu_usage = sys.global_cpu_usage();
    if cpu_usage > CPU_UTILIZATION_THRESHOLD {
        let error_msg = format!("CPU is over-utilized: {:.1}%", cpu_usage);
        warn!(
            "Rejecting request to {} due to high CPU usage: {:.1}% (threshold: {:.1}%)", 
            req.path(), 
            cpu_usage,
            CPU_UTILIZATION_THRESHOLD
        );
        return Err(ErrorServiceUnavailable(error_msg));
    }

    // Continue processing the request if resource utilization is within limits
    next.call(req).await
}
