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
use crate::metrics::{
    DELETED_EVENTS_STORAGE_SIZE, EVENTS_DELETED, EVENTS_DELETED_SIZE, EVENTS_INGESTED,
    EVENTS_INGESTED_SIZE, EVENTS_INGESTED_SIZE_TODAY, LIFETIME_EVENTS_INGESTED,
    LIFETIME_EVENTS_INGESTED_SIZE, LIFETIME_EVENTS_STORAGE_SIZE, STORAGE_SIZE,
};

use crate::catalog::partition_path;
use crate::storage::{ObjectStorage, ObjectStorageError, ObjectStoreFormat};
use std::sync::Arc;

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
        .get();
    let ingestion_size = EVENTS_INGESTED_SIZE
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get();
    let storage_size = STORAGE_SIZE
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get();
    let events_deleted = EVENTS_DELETED
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get();
    let events_deleted_size = EVENTS_DELETED_SIZE
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get();
    let deleted_events_storage_size = DELETED_EVENTS_STORAGE_SIZE
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get();
    let lifetime_events_ingested = LIFETIME_EVENTS_INGESTED
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get();
    let lifetime_ingestion_size = LIFETIME_EVENTS_INGESTED_SIZE
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get();
    let lifetime_events_storage_size = LIFETIME_EVENTS_STORAGE_SIZE
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get();
    // this should be valid for all cases given that gauge must never go negative
    let events_ingested = events_ingested as u64;
    let ingestion_size = ingestion_size as u64;
    let storage_size = storage_size as u64;
    let events_deleted = events_deleted as u64;
    let events_deleted_size = events_deleted_size as u64;
    let deleted_events_storage_size = deleted_events_storage_size as u64;
    let lifetime_events_ingested = lifetime_events_ingested as u64;
    let lifetime_ingestion_size = lifetime_ingestion_size as u64;
    let lifetime_events_storage_size = lifetime_events_storage_size as u64;
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
    storage: Arc<dyn ObjectStorage + Send>,
    stream_name: &str,
    meta: ObjectStoreFormat,
    dates: Vec<String>,
) -> Result<(), ObjectStorageError> {
    let mut num_row: u64 = 0;
    let mut storage_size: u64 = 0;
    let mut ingestion_size: i64 = 0;

    let mut manifests = meta.snapshot.manifest_list;
    manifests.retain(|item| dates.iter().any(|date| item.manifest_path.contains(date)));

    if !manifests.is_empty() {
        for manifest in manifests {
            let path = partition_path(
                stream_name,
                manifest.time_lower_bound,
                manifest.time_upper_bound,
            );
            let Some(manifest) = storage.get_manifest(&path).await? else {
                return Err(ObjectStorageError::UnhandledError(
                    "Manifest found in snapshot but not in object-storage"
                        .to_string()
                        .into(),
                ));
            };
            manifest.files.iter().for_each(|file| {
                num_row += file.num_rows;
                storage_size += file.file_size;
            });
            ingestion_size += manifest.ingestion_size;
        }
    }
    EVENTS_DELETED
        .with_label_values(&[stream_name, "json"])
        .add(num_row as i64);
    EVENTS_DELETED_SIZE
        .with_label_values(&[stream_name, "json"])
        .add(ingestion_size);
    DELETED_EVENTS_STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .add(storage_size as i64);
    EVENTS_INGESTED
        .with_label_values(&[stream_name, "json"])
        .sub(num_row as i64);
    EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, "json"])
        .sub(ingestion_size);
    EVENTS_INGESTED_SIZE_TODAY
        .with_label_values(&[stream_name, "json"])
        .sub(ingestion_size);
    STORAGE_SIZE
        .with_label_values(&["data", stream_name, "parquet"])
        .sub(storage_size as i64);
    let stats = get_current_stats(stream_name, "json");
    if let Some(stats) = stats {
        if let Err(e) = storage.put_stats(stream_name, &stats).await {
            log::warn!("Error updating stats to objectstore due to error [{}]", e);
        }
    }

    Ok(())
}

pub fn delete_stats(stream_name: &str, format: &'static str) -> prometheus::Result<()> {
    let event_labels = event_labels(stream_name, format);
    let storage_size_labels = storage_size_labels(stream_name);

    EVENTS_INGESTED.remove_label_values(&event_labels)?;
    EVENTS_INGESTED_SIZE.remove_label_values(&event_labels)?;
    STORAGE_SIZE.remove_label_values(&storage_size_labels)?;
    EVENTS_DELETED.remove_label_values(&event_labels)?;
    EVENTS_DELETED_SIZE.remove_label_values(&event_labels)?;
    DELETED_EVENTS_STORAGE_SIZE.remove_label_values(&storage_size_labels)?;
    LIFETIME_EVENTS_INGESTED.remove_label_values(&event_labels)?;
    LIFETIME_EVENTS_INGESTED_SIZE.remove_label_values(&event_labels)?;
    LIFETIME_EVENTS_STORAGE_SIZE.remove_label_values(&storage_size_labels)?;

    Ok(())
}

pub fn event_labels<'a>(stream_name: &'a str, format: &'static str) -> [&'a str; 2] {
    [stream_name, format]
}

pub fn storage_size_labels(stream_name: &str) -> [&str; 3] {
    ["data", stream_name, "parquet"]
}
