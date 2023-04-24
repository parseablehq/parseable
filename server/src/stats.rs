/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use crate::metrics::{EVENTS_INGESTED, EVENTS_INGESTED_SIZE, STORAGE_SIZE};

/// Helper struct type created by copying stats values from metadata
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub events: u64,
    pub ingestion: u64,
    pub storage: u64,
}

pub fn get_current_stats(stream_name: &str, format: &'static str) -> Option<Stats> {
    let events_ingested = EVENTS_INGESTED
        .get_metric_with_label_values(&[stream_name, format])
        .ok()?
        .get();
    let ingestion_size = EVENTS_INGESTED_SIZE
        .get_metric_with_label_values(&[stream_name, format])
        .ok()?
        .get();
    let storage_size = STORAGE_SIZE
        .get_metric_with_label_values(&["data", stream_name, "parquet"])
        .ok()?
        .get();
    // this should be valid for all cases given that gauge must never go negative
    let ingestion_size = ingestion_size as u64;
    let storage_size = storage_size as u64;

    Some(Stats {
        events: events_ingested,
        ingestion: ingestion_size,
        storage: storage_size,
    })
}
