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

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use crate::catalog::snapshot::ManifestItem;
use crate::event::format::LogSourceEntry;
use crate::metrics::{
    EVENTS_INGESTED, EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE, EVENTS_INGESTED_SIZE_DATE,
    EVENTS_STORAGE_SIZE_DATE, LIFETIME_EVENTS_INGESTED, LIFETIME_EVENTS_INGESTED_SIZE,
};
use crate::storage::StreamType;
use crate::storage::retention::Retention;

pub fn update_stats(
    stream_name: &str,
    origin: &'static str,
    size: u64,
    num_rows: usize,
    parsed_date: NaiveDate,
) {
    let parsed_date = parsed_date.to_string();
    EVENTS_INGESTED
        .with_label_values(&[stream_name, origin])
        .add(num_rows as i64);
    EVENTS_INGESTED_DATE
        .with_label_values(&[stream_name, origin, &parsed_date])
        .add(num_rows as i64);
    EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, origin])
        .add(size as i64);
    EVENTS_INGESTED_SIZE_DATE
        .with_label_values(&[stream_name, origin, &parsed_date])
        .add(size as i64);
    LIFETIME_EVENTS_INGESTED
        .with_label_values(&[stream_name, origin])
        .add(num_rows as i64);
    LIFETIME_EVENTS_INGESTED_SIZE
        .with_label_values(&[stream_name, origin])
        .add(size as i64);
}

/// In order to support backward compatability with streams created before v1.6.4,
/// we will consider past versions of stream schema to be v0. Streams created with
/// v1.6.4+ will be v1.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum SchemaVersion {
    #[default]
    V0,
    /// Applies generic JSON flattening, ignores null data, handles all numbers as
    /// float64 and uses the timestamp type to store compatible time information.
    V1,
}

#[derive(Debug, Default, Clone)]
pub struct LogStreamMetadata {
    pub schema_version: SchemaVersion,
    pub schema: HashMap<String, Arc<Field>>,
    pub retention: Option<Retention>,
    pub created_at: String,
    pub first_event_at: Option<String>,
    pub time_partition: Option<String>,
    pub time_partition_limit: Option<NonZeroU32>,
    pub custom_partition: Option<String>,
    pub static_schema_flag: bool,
    pub hot_tier_enabled: bool,
    pub stream_type: StreamType,
    pub log_source: Vec<LogSourceEntry>,
}

impl LogStreamMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        created_at: String,
        time_partition: String,
        time_partition_limit: Option<NonZeroU32>,
        custom_partition: Option<String>,
        static_schema_flag: bool,
        static_schema: HashMap<String, Arc<Field>>,
        stream_type: StreamType,
        schema_version: SchemaVersion,
        log_source: Vec<LogSourceEntry>,
    ) -> Self {
        LogStreamMetadata {
            created_at: if created_at.is_empty() {
                Utc::now().to_rfc3339()
            } else {
                created_at
            },
            time_partition: if time_partition.is_empty() {
                None
            } else {
                Some(time_partition)
            },
            time_partition_limit,
            custom_partition,
            static_schema_flag,
            schema: if static_schema.is_empty() {
                HashMap::new()
            } else {
                static_schema
            },
            stream_type,
            schema_version,
            log_source,
            ..Default::default()
        }
    }
}

///this function updates the data type of time partition field
/// from utf-8 to timestamp if it is not already timestamp
/// and updates the schema in the storage
/// required only when migrating from version 1.2.0 and below
/// this function will be removed in the future
pub async fn update_data_type_time_partition(
    schema: &mut Schema,
    time_partition: Option<&String>,
) -> anyhow::Result<()> {
    if let Some(time_partition) = time_partition {
        if let Ok(time_partition_field) = schema.field_with_name(time_partition) {
            if time_partition_field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
            {
                let mut fields = schema
                    .fields()
                    .iter()
                    .filter(|field| field.name() != time_partition)
                    .cloned()
                    .collect::<Vec<Arc<Field>>>();
                let time_partition_field = Arc::new(Field::new(
                    time_partition,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ));
                fields.push(time_partition_field);
                *schema = Schema::new(fields);
            }
        }
    }

    Ok(())
}

pub fn load_daily_metrics(manifests: &Vec<ManifestItem>, stream_name: &str) {
    for manifest in manifests {
        let manifest_date = manifest.time_lower_bound.date_naive().to_string();
        let events_ingested = manifest.events_ingested;
        let ingestion_size = manifest.ingestion_size;
        let storage_size = manifest.storage_size;
        EVENTS_INGESTED_DATE
            .with_label_values(&[stream_name, "json", &manifest_date])
            .set(events_ingested as i64);
        EVENTS_INGESTED_SIZE_DATE
            .with_label_values(&[stream_name, "json", &manifest_date])
            .set(ingestion_size as i64);
        EVENTS_STORAGE_SIZE_DATE
            .with_label_values(&["data", stream_name, "parquet", &manifest_date])
            .set(storage_size as i64);
    }
}
