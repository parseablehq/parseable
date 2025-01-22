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
 *
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use anyhow::{anyhow, Error as AnyError};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    metadata::SchemaVersion,
    utils::arrow::{get_field, get_timestamp_array, replace_columns},
};

use super::DEFAULT_TIMESTAMP_KEY;

pub mod json;

static TIME_FIELD_NAME_PARTS: [&str; 2] = ["time", "date"];
type EventSchema = Vec<Arc<Field>>;

/// Source of the logs, used to perform special processing for certain sources
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogSource {
    // AWS Kinesis sends logs in the format of a json array
    Kinesis,
    // OpenTelemetry sends logs according to the specification as explained here
    // https://github.com/open-telemetry/opentelemetry-proto/tree/v1.0.0/opentelemetry/proto/logs/v1
    OtelLogs,
    // OpenTelemetry sends traces according to the specification as explained here
    // https://github.com/open-telemetry/opentelemetry-proto/blob/v1.0.0/opentelemetry/proto/trace/v1/trace.proto
    OtelMetrics,
    // OpenTelemetry sends traces according to the specification as explained here
    // https://github.com/open-telemetry/opentelemetry-proto/tree/v1.0.0/opentelemetry/proto/metrics/v1
    OtelTraces,
    // Internal Stream format
    Pmeta,
    #[default]
    // Json object or array
    Json,
    Custom(String),
}

impl From<&str> for LogSource {
    fn from(s: &str) -> Self {
        match s {
            "kinesis" => LogSource::Kinesis,
            "otel-logs" => LogSource::OtelLogs,
            "otel-metrics" => LogSource::OtelMetrics,
            "otel-traces" => LogSource::OtelTraces,
            "pmeta" => LogSource::Pmeta,
            _ => LogSource::Json,
        }
    }
}

impl Display for LogSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LogSource::Kinesis => "kinesis",
            LogSource::OtelLogs => "otel-logs",
            LogSource::OtelMetrics => "otel-metrics",
            LogSource::OtelTraces => "otel-traces",
            LogSource::Json => "json",
            LogSource::Pmeta => "pmeta",
            LogSource::Custom(custom) => custom,
        })
    }
}

// Global Trait for event format
// This trait is implemented by all the event formats
pub trait EventFormat: Sized {
    type Data;

    fn to_data(
        self,
        schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: bool,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
    ) -> Result<(Self::Data, EventSchema, bool), AnyError>;

    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, AnyError>;

    fn into_recordbatch(
        self,
        storage_schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: bool,
        time_partition: Option<&String>,
        schema_version: SchemaVersion,
    ) -> Result<(RecordBatch, bool), AnyError> {
        let (data, mut schema, is_first) = self.to_data(
            storage_schema,
            static_schema_flag,
            time_partition,
            schema_version,
        )?;

        if get_field(&schema, DEFAULT_TIMESTAMP_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_TIMESTAMP_KEY
            ));
        };

        // add the p_timestamp field to the event schema to the 0th index
        schema.insert(
            0,
            Arc::new(Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
        );

        // prepare the record batch and new fields to be added
        let mut new_schema = Arc::new(Schema::new(schema));
        if !Self::is_schema_matching(new_schema.clone(), storage_schema, static_schema_flag) {
            return Err(anyhow!("Schema mismatch"));
        }
        new_schema =
            update_field_type_in_schema(new_schema, None, time_partition, None, schema_version);

        let mut rb = Self::decode(data, new_schema.clone())?;
        rb = replace_columns(
            rb.schema(),
            &rb,
            &[0],
            &[Arc::new(get_timestamp_array(rb.num_rows()))],
        );

        Ok((rb, is_first))
    }

    fn is_schema_matching(
        new_schema: Arc<Schema>,
        storage_schema: &HashMap<String, Arc<Field>>,
        static_schema_flag: bool,
    ) -> bool {
        if !static_schema_flag {
            return true;
        }
        for field in new_schema.fields() {
            let Some(storage_field) = storage_schema.get(field.name()) else {
                return false;
            };
            if field.name() != storage_field.name() {
                return false;
            }
            if field.data_type() != storage_field.data_type() {
                return false;
            }
        }
        true
    }
}

pub fn get_existing_field_names(
    inferred_schema: Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
) -> HashSet<String> {
    let mut existing_field_names = HashSet::new();

    let Some(existing_schema) = existing_schema else {
        return existing_field_names;
    };
    for field in inferred_schema.fields.iter() {
        if existing_schema.contains_key(field.name()) {
            existing_field_names.insert(field.name().to_owned());
        }
    }

    existing_field_names
}

pub fn override_existing_timestamp_fields(
    existing_schema: &HashMap<String, Arc<Field>>,
    inferred_schema: Arc<Schema>,
) -> Arc<Schema> {
    let timestamp_field_names: HashSet<String> = existing_schema
        .values()
        .filter_map(|field| {
            if let DataType::Timestamp(TimeUnit::Millisecond, None) = field.data_type() {
                Some(field.name().to_owned())
            } else {
                None
            }
        })
        .collect();
    let updated_fields: Vec<Arc<Field>> = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            if timestamp_field_names.contains(field.name()) {
                Arc::new(Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    field.is_nullable(),
                ))
            } else {
                field.clone()
            }
        })
        .collect();

    Arc::new(Schema::new(updated_fields))
}

pub fn update_field_type_in_schema(
    inferred_schema: Arc<Schema>,
    existing_schema: Option<&HashMap<String, Arc<Field>>>,
    time_partition: Option<&String>,
    log_records: Option<&Vec<Value>>,
    schema_version: SchemaVersion,
) -> Arc<Schema> {
    let mut updated_schema = inferred_schema.clone();
    let existing_field_names = get_existing_field_names(inferred_schema.clone(), existing_schema);

    if let Some(existing_schema) = existing_schema {
        // overriding known timestamp fields which were inferred as string fields
        updated_schema = override_existing_timestamp_fields(existing_schema, updated_schema);
    }

    if let Some(log_records) = log_records {
        for log_record in log_records {
            updated_schema =
                override_data_type(updated_schema.clone(), log_record.clone(), schema_version);
        }
    }

    let Some(time_partition) = time_partition else {
        return updated_schema;
    };

    let new_schema: Vec<Field> = updated_schema
        .fields()
        .iter()
        .map(|field| {
            // time_partition field not present in existing schema with string type data as timestamp
            if field.name() == time_partition
                && !existing_field_names.contains(field.name())
                && field.data_type() == &DataType::Utf8
            {
                let new_data_type = DataType::Timestamp(TimeUnit::Millisecond, None);
                Field::new(field.name(), new_data_type, true)
            } else {
                Field::new(field.name(), field.data_type().clone(), true)
            }
        })
        .collect();
    Arc::new(Schema::new(new_schema))
}

// From Schema v1 onwards, convert json fields with name containig "date"/"time" and having
// a string value parseable into timestamp as timestamp type and all numbers as float64.
pub fn override_data_type(
    inferred_schema: Arc<Schema>,
    log_record: Value,
    schema_version: SchemaVersion,
) -> Arc<Schema> {
    let Value::Object(map) = log_record else {
        return inferred_schema;
    };
    let updated_schema: Vec<Field> = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            let field_name = field.name().as_str();
            match (schema_version, map.get(field.name())) {
                // in V1 for new fields in json named "time"/"date" or such and having inferred
                // type string, that can be parsed as timestamp, use the timestamp type.
                // NOTE: support even more datetime string formats
                (SchemaVersion::V1, Some(Value::String(s)))
                    if TIME_FIELD_NAME_PARTS
                        .iter()
                        .any(|part| field_name.to_lowercase().contains(part))
                        && field.data_type() == &DataType::Utf8
                        && (DateTime::parse_from_rfc3339(s).is_ok()
                            || DateTime::parse_from_rfc2822(s).is_ok()) =>
                {
                    // Update the field's data type to Timestamp
                    Field::new(
                        field_name,
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    )
                }
                // in V1 for new fields in json with inferred type number, cast as float64.
                (SchemaVersion::V1, Some(Value::Number(_))) if field.data_type().is_numeric() => {
                    // Update the field's data type to Float64
                    Field::new(field_name, DataType::Float64, true)
                }
                // Return the original field if no update is needed
                _ => Field::new(field_name, field.data_type().clone(), true),
            }
        })
        .collect();

    Arc::new(Schema::new(updated_schema))
}
