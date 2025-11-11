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

use std::ffi::CString;
use std::time::Duration;

use clokwerk::AsyncScheduler;
use tracing::{info, warn};

/// Force memory release using jemalloc
pub fn force_memory_release() {
    // Advance epoch to refresh statistics and trigger potential cleanup
    if let Err(e) = tikv_jemalloc_ctl::epoch::mib().and_then(|mib| mib.advance()) {
        warn!("Failed to advance jemalloc epoch: {:?}", e);
    }

    // Purge each initialized arena
    if let Ok(n) = tikv_jemalloc_ctl::arenas::narenas::read() {
        for i in 0..n {
            if let Ok(name) = CString::new(format!("arena.{i}.purge")) {
                unsafe {
                    let ret = tikv_jemalloc_sys::mallctl(
                        name.as_ptr(),
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        0,
                    );
                    if ret != 0 {
                        warn!("Arena purge failed for index {i} with code: {ret}");
                    }
                }
            }
        }
    } else {
        warn!("Failed to read jemalloc arenas.narenas");
    }
}

/// Initialize memory management scheduler
pub fn init_memory_release_scheduler() -> anyhow::Result<()> {
    info!("Setting up scheduler for memory release");

    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(clokwerk::Interval::Hours(1))
        .run(move || async {
            info!("Running scheduled memory release");
            force_memory_release();
        });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(60)).await; // Check every minute
        }
    });

    Ok(())
}
