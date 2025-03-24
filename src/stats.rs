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

use std::{collections::HashMap, sync::Arc};

use chrono::NaiveDate;
use prometheus::core::Collector;
use prometheus::proto::MetricFamily;
use prometheus::IntGaugeVec;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::warn;

use crate::{
    metrics::{
        DELETED_EVENTS_STORAGE_SIZE, EVENTS_DELETED, EVENTS_DELETED_SIZE, EVENTS_INGESTED,
        EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE, EVENTS_INGESTED_SIZE_DATE,
        EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_INGESTED, LIFETIME_EVENTS_INGESTED_SIZE,
        LIFETIME_EVENTS_STORAGE_SIZE, STORAGE_SIZE,
    },
    prism::home::PrismHomeError,
    storage::{ObjectStorage, ObjectStorageError, ObjectStoreFormat},
};

/// Helper struct type created by copying stats values from metadata
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub events: u64,
    pub ingestion: u64,
    pub storage: u64,
}

impl Stats {
    fn get_current(event_labels: &[&str], storage_size_labels: &[&str]) -> Option<Self> {
        let events = EVENTS_INGESTED
            .get_metric_with_label_values(event_labels)
            .ok()?
            .get() as u64;
        let ingestion = EVENTS_INGESTED_SIZE
            .get_metric_with_label_values(event_labels)
            .ok()?
            .get() as u64;
        let storage = STORAGE_SIZE
            .get_metric_with_label_values(storage_size_labels)
            .ok()?
            .get() as u64;

        Some(Self {
            events,
            ingestion,
            storage,
        })
    }

    fn get_lifetime(event_labels: &[&str], storage_size_labels: &[&str]) -> Option<Self> {
        let events = LIFETIME_EVENTS_INGESTED
            .get_metric_with_label_values(event_labels)
            .ok()?
            .get() as u64;
        let ingestion = LIFETIME_EVENTS_INGESTED_SIZE
            .get_metric_with_label_values(event_labels)
            .ok()?
            .get() as u64;
        let storage = LIFETIME_EVENTS_STORAGE_SIZE
            .get_metric_with_label_values(storage_size_labels)
            .ok()?
            .get() as u64;

        Some(Self {
            events,
            ingestion,
            storage,
        })
    }

    fn get_deleted(event_labels: &[&str], storage_size_labels: &[&str]) -> Option<Self> {
        let events = EVENTS_DELETED
            .get_metric_with_label_values(event_labels)
            .ok()?
            .get() as u64;
        let ingestion = EVENTS_DELETED_SIZE
            .get_metric_with_label_values(event_labels)
            .ok()?
            .get() as u64;
        let storage = DELETED_EVENTS_STORAGE_SIZE
            .get_metric_with_label_values(storage_size_labels)
            .ok()?
            .get() as u64;

        Some(Self {
            events,
            ingestion,
            storage,
        })
    }

    pub fn for_stream_on_date(date: NaiveDate, stream_name: &str) -> Stats {
        let date = date.to_string();
        let event_labels = event_labels_date(stream_name, "json", &date);
        let storage_size_labels = storage_size_labels_date(stream_name, &date);
        let events_ingested = EVENTS_INGESTED_DATE
            .get_metric_with_label_values(&event_labels)
            .unwrap()
            .get() as u64;
        let ingestion_size = EVENTS_INGESTED_SIZE_DATE
            .get_metric_with_label_values(&event_labels)
            .unwrap()
            .get() as u64;
        let storage_size = EVENTS_STORAGE_SIZE_DATE
            .get_metric_with_label_values(&storage_size_labels)
            .unwrap()
            .get() as u64;

        Stats {
            events: events_ingested,
            ingestion: ingestion_size,
            storage: storage_size,
        }
    }

    pub fn fetch_from_ingestors(
        date: NaiveDate,
        stream_meta_list: &[ObjectStoreFormat],
    ) -> Result<Stats, PrismHomeError> {
        // for the given date, get the stats from the ingestors
        let mut events_ingested = 0;
        let mut ingestion_size = 0;
        let mut storage_size = 0;

        for meta in stream_meta_list.iter() {
            for manifest in meta.snapshot.manifest_list.iter() {
                if manifest.time_lower_bound.date_naive() == date {
                    events_ingested += manifest.events_ingested;
                    ingestion_size += manifest.ingestion_size;
                    storage_size += manifest.storage_size;
                }
            }
        }

        let stats = Stats {
            events: events_ingested,
            ingestion: ingestion_size,
            storage: storage_size,
        };
        Ok(stats)
    }
}

#[derive(Debug, Serialize, Default)]
pub struct DatedStats {
    pub date: NaiveDate,
    pub events: u64,
    pub ingestion_size: u64,
    pub storage_size: u64,
}

impl DatedStats {
    pub fn for_all_streams(
        date: NaiveDate,
        stream_wise_meta: &HashMap<String, Vec<ObjectStoreFormat>>,
    ) -> Result<Option<DatedStats>, PrismHomeError> {
        // collect stats for all the streams for the given date
        let mut details = DatedStats {
            date,
            ..Default::default()
        };

        for (stream, meta) in stream_wise_meta {
            let querier_stats = Stats::for_stream_on_date(date, stream);
            let ingestor_stats = Stats::fetch_from_ingestors(date, meta)?;
            // collect date-wise stats for all streams
            details.events += querier_stats.events + ingestor_stats.events;
            details.ingestion_size += querier_stats.ingestion + ingestor_stats.ingestion;
            details.storage_size += querier_stats.storage + ingestor_stats.storage;
        }

        Ok(Some(details))
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct FullStats {
    pub lifetime_stats: Stats,
    pub current_stats: Stats,
    pub deleted_stats: Stats,
}

impl FullStats {
    pub fn get_current(stream_name: &str, format: &'static str) -> Option<FullStats> {
        let event_labels = event_labels(stream_name, format);
        let storage_size_labels = storage_size_labels(stream_name);

        Some(FullStats {
            lifetime_stats: Stats::get_lifetime(&event_labels, &storage_size_labels)?,
            current_stats: Stats::get_current(&event_labels, &storage_size_labels)?,
            deleted_stats: Stats::get_deleted(&event_labels, &storage_size_labels)?,
        })
    }
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
    let stats = FullStats::get_current(stream_name, "json");
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

    EVENTS_INGESTED.remove_label_values(&event_labels)?;
    EVENTS_INGESTED_SIZE.remove_label_values(&event_labels)?;
    STORAGE_SIZE.remove_label_values(&storage_size_labels)?;
    EVENTS_DELETED.remove_label_values(&event_labels)?;
    EVENTS_DELETED_SIZE.remove_label_values(&event_labels)?;
    DELETED_EVENTS_STORAGE_SIZE.remove_label_values(&storage_size_labels)?;
    LIFETIME_EVENTS_INGESTED.remove_label_values(&event_labels)?;
    LIFETIME_EVENTS_INGESTED_SIZE.remove_label_values(&event_labels)?;
    LIFETIME_EVENTS_STORAGE_SIZE.remove_label_values(&storage_size_labels)?;

    delete_with_label_prefix(&EVENTS_INGESTED_DATE, &event_labels);
    delete_with_label_prefix(&EVENTS_INGESTED_SIZE_DATE, &event_labels);
    delete_with_label_prefix(&EVENTS_STORAGE_SIZE_DATE, &storage_size_labels);

    Ok(())
}

fn delete_with_label_prefix(metrics: &IntGaugeVec, prefix: &[&str]) {
    let families: Vec<MetricFamily> = metrics.collect().into_iter().collect();
    for metric in families.iter().flat_map(|m| m.get_metric()) {
        let label: Vec<&str> = metric.get_label().iter().map(|l| l.get_value()).collect();
        if !label.starts_with(prefix) {
            continue;
        }
        if let Err(err) = metrics.remove_label_values(&label) {
            warn!("Error = {err}");
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

#[derive(Debug, Deserialize)]
pub struct StatsParams {
    #[serde(deserialize_with = "deserialize_date")]
    pub date: Option<NaiveDate>,
}

pub fn deserialize_date<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(s) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };

    NaiveDate::parse_from_str(&s, "%Y-%m-%d")
        .map(Some)
        .map_err(serde::de::Error::custom)
}
