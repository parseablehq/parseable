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
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::{handlers::http::query::QueryError, utils::arrow::record_batches_to_json};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{Value, json};
use tracing::{debug, info, warn};

pub struct QueryResponse {
    pub records: Vec<RecordBatch>,
    pub fields: Vec<String>,
    pub fill_null: bool,
    pub with_fields: bool,
}

impl QueryResponse {
    pub fn to_json(&self) -> Result<Value, QueryError> {
        info!("{}", "Returning query results");

        // Process in batches to avoid massive allocations
        const BATCH_SIZE: usize = 100; // Process 100 record batches at a time
        let mut all_values = Vec::new();

        for chunk in self.records.chunks(BATCH_SIZE) {
            let mut json_records = record_batches_to_json(chunk)?;

            if self.fill_null {
                for map in &mut json_records {
                    for field in &self.fields {
                        if !map.contains_key(field) {
                            map.insert(field.clone(), Value::Null);
                        }
                    }
                }
            }

            // Convert this batch to values and add to collection
            let batch_values: Vec<Value> = json_records.into_iter().map(Value::Object).collect();
            all_values.extend(batch_values);
        }

        let response = if self.with_fields {
            json!({
                "fields": self.fields,
                "records": all_values,
            })
        } else {
            Value::Array(all_values)
        };

        Ok(response)
    }
}

impl Drop for QueryResponse {
    fn drop(&mut self) {
        force_memory_release();
    }
}

// Rate-limited memory release with proper error handling
static LAST_PURGE: Mutex<Option<Instant>> = Mutex::new(None);
const PURGE_INTERVAL: Duration = Duration::from_secs(3600); // 1 hour
pub fn force_memory_release() {
    {
        let mut last_purge = LAST_PURGE.lock().unwrap();
        if let Some(last) = *last_purge {
            if last.elapsed() < PURGE_INTERVAL {
                return;
            }
        }
        *last_purge = Some(Instant::now());
    }

    // Advance epoch to refresh statistics and trigger potential cleanup
    if let Err(e) = tikv_jemalloc_ctl::epoch::mib().and_then(|mib| mib.advance()) {
        warn!("Failed to advance jemalloc epoch: {:?}", e);
    }

    // Purge all arenas using MALLCTL_ARENAS_ALL
    if let Ok(arena_purge) = CString::new("arena.4096.purge") {
        unsafe {
            let ret = tikv_jemalloc_sys::mallctl(
                arena_purge.as_ptr(),
                std::ptr::null_mut(), // oldp (not reading)
                std::ptr::null_mut(), // oldlenp (not reading)
                std::ptr::null_mut(), // newp (void operation)
                0,                    // newlen (void operation)
            );
            if ret != 0 {
                warn!("Arena purge failed with code: {}", ret);
            } else {
                debug!("Successfully purged all jemalloc arenas");
            }
        }
    } else {
        warn!("Failed to create CString for arena purge");
    }
}
