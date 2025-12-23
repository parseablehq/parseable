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

use std::ops::Bound;

use chrono::{DateTime, Utc};

use crate::query::PartialTimeFilter;

pub const CURRENT_SNAPSHOT_VERSION: &str = "v2";
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub version: String,
    pub manifest_list: Vec<ManifestItem>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            version: CURRENT_SNAPSHOT_VERSION.to_string(),
            manifest_list: Vec::default(),
        }
    }
}

impl super::Snapshot for Snapshot {
    fn manifests(&self, time_predicates: &[PartialTimeFilter]) -> Vec<ManifestItem> {
        let mut manifests = self.manifest_list.clone();
        for predicate in time_predicates {
            match predicate {
                PartialTimeFilter::Low(Bound::Included(time)) => manifests.retain(|item| {
                    let time = time.and_utc();
                    item.time_upper_bound >= time
                }),
                PartialTimeFilter::Low(Bound::Excluded(time)) => manifests.retain(|item| {
                    let time = time.and_utc();
                    item.time_upper_bound > time
                }),
                PartialTimeFilter::High(Bound::Included(time)) => manifests.retain(|item| {
                    let time = time.and_utc();
                    item.time_lower_bound <= time
                }),
                PartialTimeFilter::High(Bound::Excluded(time)) => manifests.retain(|item| {
                    let time = time.and_utc();
                    item.time_lower_bound < time
                }),
                PartialTimeFilter::Eq(time) => manifests.retain(|item| {
                    let time = time.and_utc();
                    item.time_lower_bound <= time && time <= item.time_upper_bound
                }),
                _ => (),
            }
        }

        manifests
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ManifestItem {
    pub manifest_path: String,
    pub time_lower_bound: DateTime<Utc>,
    pub time_upper_bound: DateTime<Utc>,
    pub events_ingested: u64,
    pub ingestion_size: u64,
    pub storage_size: u64,
}
