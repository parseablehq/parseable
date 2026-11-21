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
 *
 */

pub mod format;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Fields, Schema};
use itertools::Itertools;
use std::sync::Arc;

use tracing::{info_span, instrument};

use self::error::EventError;
use crate::{
    LOCK_EXPECT,
    handlers::TelemetryType,
    metadata::update_stats,
    metrics::{increment_events_ingested_by_date, increment_events_ingested_size_by_date},
    parseable::{DEFAULT_TENANT, PARSEABLE, StagingError},
    storage::StreamType,
};
use chrono::NaiveDateTime;
use std::collections::HashMap;

pub const DEFAULT_TIMESTAMP_KEY: &str = "p_timestamp";
pub const USER_AGENT_KEY: &str = "p_user_agent";
pub const SOURCE_IP_KEY: &str = "p_src_ip";
pub const FORMAT_KEY: &str = "p_format";
pub const FORMAT_VERIFY_KEY: &str = "p_format_verified";

#[derive(Clone)]
pub struct Event {
    pub stream_name: String,
    pub rb: RecordBatch,
    pub origin_format: &'static str,
    pub origin_size: u64,
    pub is_first_event: bool,
    pub parsed_timestamp: NaiveDateTime,
    pub time_partition: Option<String>,
    pub custom_partition_values: HashMap<String, String>,
    pub stream_type: StreamType,
    pub telemetry_type: TelemetryType,
    pub tenant_id: Option<String>,
}

// Events holds the schema related to a each event for a single log stream
impl Event {
    #[instrument(
        name = "event_process",
        level = "info",
        skip_all,
        fields(
            stream_name = %self.stream_name,
            num_rows = self.rb.num_rows(),
            is_first_event = self.is_first_event
        )
    )]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn process(self) -> Result<(), EventError> {
        let mut key = get_schema_key(&self.rb.schema().fields);
        if self.time_partition.is_some() {
            let parsed_timestamp_to_min = self.parsed_timestamp.format("%Y%m%dT%H%M").to_string();
            key.push_str(&parsed_timestamp_to_min);
        }

        if !self.custom_partition_values.is_empty() {
            for (k, v) in self.custom_partition_values.iter().sorted_by_key(|v| v.0) {
                key.push_str(&format!("&{k}={v}"));
            }
        }

        let stream = PARSEABLE.get_stream(&self.stream_name, &self.tenant_id)?;
        let _ingestion_guard = stream.ingestion_guard()?;
        stream.check_schema_resolution().map_err(|reason| {
            EventError::SchemaResolutionBlocked(self.stream_name.clone(), reason)
        })?;

        if self.is_first_event {
            commit_schema(&self.stream_name, self.rb.schema(), &self.tenant_id)?;
            if !stream.get_static_schema_flag() {
                stream.stage_schema_file(stream.get_schema().as_ref().clone())?;
            }
        }

        stream.push(
            &key,
            &self.rb,
            self.parsed_timestamp,
            &self.custom_partition_values,
            self.stream_type,
        )?;

        let tenant = self.tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        update_stats(
            &self.stream_name,
            self.origin_format,
            self.origin_size,
            self.rb.num_rows(),
            self.parsed_timestamp.date(),
            tenant,
        );

        // Track billing metrics for event ingestion
        let date_string = self.parsed_timestamp.date().to_string();
        increment_events_ingested_by_date(self.rb.num_rows() as u64, &date_string, tenant);
        increment_events_ingested_size_by_date(
            self.origin_size,
            &date_string,
            self.telemetry_type,
            tenant,
        );

        crate::livetail::LIVETAIL.process(&self.stream_name, &self.rb);

        Ok(())
    }

    pub fn process_unchecked(&self) -> Result<(), EventError> {
        let key = get_schema_key(&self.rb.schema().fields);

        PARSEABLE
            .get_stream(&self.stream_name, &self.tenant_id)?
            .push(
                &key,
                &self.rb,
                self.parsed_timestamp,
                &self.custom_partition_values,
                self.stream_type,
            )?;

        Ok(())
    }
}

/// Key of the arrow file a batch is written to. It covers names *and* types: a batch whose
/// field changed type (after schema resolution) must never be appended to a file whose IPC
/// header declares the old type. Each name/type is framed so `["ab","c"]` != `["a","bc"]`.
pub fn get_schema_key(fields: &[Arc<Field>]) -> String {
    use std::hash::{Hash, Hasher};
    // Fields must be sorted
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for field in fields.iter().sorted_by_key(|v| v.name()) {
        hasher.update(field.name().as_bytes());
        hasher.update(&[0xFF]);
        field.data_type().hash(&mut hasher);
        hasher.update(&[0xFE]);
    }
    let hash = hasher.finish();
    format!("{hash:x}")
}

pub fn commit_schema(
    stream_name: &str,
    schema: Arc<Schema>,
    tenant_id: &Option<String>,
) -> Result<(), StagingError> {
    let _span = info_span!("commit_schema", stream_name).entered();
    // The streams map is only needed to clone the target stream. Holding its
    // global lock while waiting for the per-stream metadata lock can deadlock
    // concurrent ingestion that needs the map to finish processing an event.
    let stream = PARSEABLE.get_stream(stream_name, tenant_id)?;
    let mut metadata = stream.metadata.write().expect(LOCK_EXPECT);
    // Read under the schema lock: a resolution publishes its view before it changes the
    // schema, so a type the authority set is always in the view seen here.
    let view = stream.resolution.view();
    let map = &mut metadata.schema;
    // A batch built against the schema from before a resolved type change can still carry
    // the old type. Its arrow file is resolved at conversion, so it must not fail here.
    let incoming: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .filter(|f| {
            !map.get(f.name()).is_some_and(|cur| {
                view.predates_resolved_type(f.name(), cur.data_type(), f.data_type())
            })
        })
        .cloned()
        .collect();
    let current_schema = Schema::new(map.values().cloned().collect::<Fields>());
    let schema = Schema::try_merge(vec![current_schema, Schema::new(incoming)])?;
    map.clear();
    map.extend(schema.fields.iter().map(|f| (f.name().clone(), f.clone())));
    Ok(())
}

pub mod error {
    use crate::{
        parseable::{StagingError, StreamNotFound},
        storage::ObjectStorageError,
    };
    #[derive(Debug, thiserror::Error)]

    pub enum EventError {
        #[error("Staging Failed: {0}")]
        Staging(#[from] StagingError),
        #[error("ObjectStorage Error: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
        #[error(transparent)]
        StreamNotFound(#[from] StreamNotFound),
        #[error(
            "Stream {0} is temporarily not accepting data: its schema could not be reconciled with the cluster ({1}). Retry later"
        )]
        SchemaResolutionBlocked(String, String),
    }
}
