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

use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;
use prometheus::IntGaugeVec;
use prometheus::core::Collector;
use prometheus::proto::MetricFamily;
use tracing::warn;

use crate::metrics::{
    DELETED_EVENTS_STORAGE_SIZE, EVENTS_DELETED, EVENTS_DELETED_SIZE, EVENTS_INGESTED,
    EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE, EVENTS_INGESTED_SIZE_DATE,
    EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_INGESTED, LIFETIME_EVENTS_INGESTED_SIZE,
    LIFETIME_EVENTS_STORAGE_SIZE, STORAGE_SIZE,
};
use crate::storage::{ObjectStorage, ObjectStorageError, ObjectStoreFormat};

/// Helper struct type created by copying stats values from metadata
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub events: u64,
    pub ingestion: u64,
    pub storage: u64,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct FullStats {
    pub lifetime_stats: Stats,
    pub current_stats: Stats,
    pub deleted_stats: Stats,
}

pub fn get_current_stats(stream_name: &str, format: &'static str) -> Option<FullStats> {
    let event_labels = event_labels(stream_name, format);
    let storage_size_labels = storage_size_labels(stream_name);

    let events_ingested = EVENTS_INGESTED
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let ingestion_size = EVENTS_INGESTED_SIZE
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let storage_size = STORAGE_SIZE
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get() as u64;
    let events_deleted = EVENTS_DELETED
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let events_deleted_size = EVENTS_DELETED_SIZE
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let deleted_events_storage_size = DELETED_EVENTS_STORAGE_SIZE
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get() as u64;
    let lifetime_events_ingested = LIFETIME_EVENTS_INGESTED
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let lifetime_ingestion_size = LIFETIME_EVENTS_INGESTED_SIZE
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let lifetime_events_storage_size = LIFETIME_EVENTS_STORAGE_SIZE
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get() as u64;

    Some(FullStats {
        lifetime_stats: Stats {
            events: lifetime_events_ingested,
            ingestion: lifetime_ingestion_size,
            storage: lifetime_events_storage_size,
        },
        current_stats: Stats {
            events: events_ingested,
            ingestion: ingestion_size,
            storage: storage_size,
        },
        deleted_stats: Stats {
            events: events_deleted,
            ingestion: events_deleted_size,
            storage: deleted_events_storage_size,
        },
    })
}

pub async fn update_deleted_stats(
    storage: Arc<dyn ObjectStorage>,
    stream_name: &str,
    meta: ObjectStoreFormat,
    dates: Vec<String>,
) -> Result<(), ObjectStorageError> {
    let mut num_row: i64 = 0;
    let mut storage_size: i64 = 0;
    let mut ingestion_size: i64 = 0;

    let mut manifests = meta.snapshot.manifest_list;
    manifests.retain(|item| dates.iter().any(|date| item.manifest_path.contains(date)));
    if !manifests.is_empty() {
        for manifest in manifests {
            let manifest_date = manifest.time_lower_bound.date_naive().to_string();
            let _ =
                EVENTS_INGESTED_DATE.remove_label_values(&[stream_name, "json", &manifest_date]);
            let _ = EVENTS_INGESTED_SIZE_DATE.remove_label_values(&[
                stream_name,
                "json",
                &manifest_date,
            ]);
            let _ = EVENTS_STORAGE_SIZE_DATE.remove_label_values(&[
                "data",
                stream_name,
                "parquet",
                &manifest_date,
            ]);
            num_row += manifest.events_ingested as i64;
            ingestion_size += manifest.ingestion_size as i64;
            storage_size += manifest.storage_size as i64;
        }
    }
    EVENTS_DELETED
        .with_label_values(&[stream_name, "json"])
        .add(num_row);
    EVENTS_DELETED_SIZE
        .with_label_values(&[stream_name, "json"])
        .add(ingestion_size);
    DELETED_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .add(storage_size);
    EVENTS_INGESTED
        .with_label_values(&[stream_name, "json"])
        .sub(num_row);
    EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, "json"])
        .sub(ingestion_size);
    STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .sub(storage_size);
    let stats = get_current_stats(stream_name, "json");
    if let Some(stats) = stats {
        if let Err(e) = storage.put_stats(stream_name, &stats).await {
            warn!("Error updating stats to objectstore due to error [{}]", e);
        }
    }

    Ok(())
}

pub fn delete_stats(stream_name: &str, format: &'static str) -> prometheus::Result<()> {
    let event_labels = event_labels(stream_name, format);
    let storage_size_labels = storage_size_labels(stream_name);

    remove_label_values(&EVENTS_INGESTED, &event_labels);
    remove_label_values(&EVENTS_INGESTED_SIZE, &event_labels);
    remove_label_values(&STORAGE_SIZE, &storage_size_labels);
    remove_label_values(&EVENTS_DELETED, &event_labels);
    remove_label_values(&EVENTS_DELETED_SIZE, &event_labels);
    remove_label_values(&DELETED_EVENTS_STORAGE_SIZE, &storage_size_labels);
    remove_label_values(&LIFETIME_EVENTS_INGESTED, &event_labels);
    remove_label_values(&LIFETIME_EVENTS_INGESTED_SIZE, &event_labels);
    remove_label_values(&LIFETIME_EVENTS_STORAGE_SIZE, &storage_size_labels);

    delete_with_label_prefix(&EVENTS_INGESTED_DATE, &event_labels);
    delete_with_label_prefix(&EVENTS_INGESTED_SIZE_DATE, &event_labels);
    delete_with_label_prefix(&EVENTS_STORAGE_SIZE_DATE, &storage_size_labels);

    Ok(())
}

#[inline]
fn remove_label_values(lazy_static: &Lazy<IntGaugeVec>, event_labels: &[&str]) {
    if let Err(e) = lazy_static.remove_label_values(event_labels) {
        warn!("Unable to delete labels- {event_labels:?}\nwith error- {e}");
    }
}

fn delete_with_label_prefix(metrics: &IntGaugeVec, prefix: &[&str]) {
    let families: Vec<MetricFamily> = metrics.collect().into_iter().collect();
    for metric in families.iter().flat_map(|m| m.get_metric()) {
        let label_map: HashMap<&str, &str> = metric
            .get_label()
            .iter()
            .map(|l| (l.get_name(), l.get_value()))
            .collect();

        // Check if all prefix elements are present in label values
        let all_prefixes_found = prefix.iter().all(|p| label_map.values().any(|v| v == p));

        if all_prefixes_found {
            if let Err(err) = metrics.remove(&label_map) {
                warn!("Error removing metric with labels {:?}: {err}", label_map);
            }
        }
    }
}

pub fn event_labels<'a>(stream_name: &'a str, format: &'static str) -> [&'a str; 2] {
    [stream_name, format]
}

pub fn storage_size_labels(stream_name: &str) -> [&str; 3] {
    ["data", stream_name, "parquet"]
}

pub fn event_labels_date<'a>(
    stream_name: &'a str,
    format: &'static str,
    date: &'a str,
) -> [&'a str; 3] {
    [stream_name, format, date]
}

pub fn storage_size_labels_date<'a>(stream_name: &'a str, date: &'a str) -> [&'a str; 4] {
    ["data", stream_name, "parquet", date]
}
