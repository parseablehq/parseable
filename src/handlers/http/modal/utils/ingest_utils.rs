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

use arrow_schema::Field;
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

use crate::{
    event::{
        format::{json, EventFormat, LogSource},
        Event,
    },
    handlers::http::{
        ingest::PostError,
        kinesis::{flatten_kinesis_logs, Message},
    },
    metadata::SchemaVersion,
    parseable::{StreamNotFound, PARSEABLE},
    storage::StreamType,
    utils::json::{convert_array_to_object, flatten::convert_to_array},
    LOCK_EXPECT,
};

pub async fn flatten_and_push_logs(
    json: Value,
    stream_name: &str,
    log_source: &LogSource,
) -> Result<(), PostError> {
    match log_source {
        LogSource::Kinesis => {
            let message: Message = serde_json::from_value(json)?;
            let json = flatten_kinesis_logs(message);
            for record in json {
                push_logs(stream_name, record, &LogSource::default()).await?;
            }
        }
        LogSource::OtelLogs | LogSource::OtelMetrics | LogSource::OtelTraces => {
            return Err(PostError::OtelNotSupported);
        }
        _ => {
            push_logs(stream_name, json, log_source).await?;
        }
    }
    Ok(())
}

pub async fn push_logs(
    stream_name: &str,
    json: Value,
    log_source: &LogSource,
) -> Result<(), PostError> {
    let time_partition = PARSEABLE.streams.get_time_partition(stream_name)?;
    let time_partition_limit = PARSEABLE.streams.get_time_partition_limit(stream_name)?;
    let static_schema_flag = PARSEABLE.streams.get_static_schema_flag(stream_name)?;
    let custom_partition = PARSEABLE.streams.get_custom_partition(stream_name)?;
    let schema_version = PARSEABLE.streams.get_schema_version(stream_name)?;

    let data = if time_partition.is_some() || custom_partition.is_some() {
        convert_array_to_object(
            json,
            time_partition.as_ref(),
            time_partition_limit,
            custom_partition.as_ref(),
            schema_version,
            log_source,
        )?
    } else {
        vec![convert_to_array(convert_array_to_object(
            json,
            None,
            None,
            None,
            schema_version,
            log_source,
        )?)?]
    };

    for value in data {
        let origin_size = serde_json::to_vec(&value).unwrap().len() as u64; // string length need not be the same as byte length
        let parsed_timestamp = match time_partition.as_ref() {
            Some(time_partition) => get_parsed_timestamp(&value, time_partition)?,
            _ => Utc::now().naive_utc(),
        };
        let custom_partition_values = match custom_partition.as_ref() {
            Some(custom_partition) => {
                let custom_partitions = custom_partition.split(',').collect_vec();
                get_custom_partition_values(&value, &custom_partitions)
            }
            None => HashMap::new(),
        };
        let schema = PARSEABLE
            .streams
            .read()
            .unwrap()
            .get(stream_name)
            .ok_or_else(|| StreamNotFound(stream_name.to_owned()))?
            .metadata
            .read()
            .expect(LOCK_EXPECT)
            .schema
            .clone();
        let (rb, is_first_event) = into_event_batch(
            value,
            schema,
            static_schema_flag,
            time_partition.as_ref(),
            schema_version,
        )?;

        Event {
            rb,
            stream_name: stream_name.to_owned(),
            origin_format: "json",
            origin_size,
            is_first_event,
            parsed_timestamp,
            time_partition: time_partition.clone(),
            custom_partition_values,
            stream_type: StreamType::UserDefined,
        }
        .process()
        .await?;
    }
    Ok(())
}

pub fn into_event_batch(
    data: Value,
    schema: HashMap<String, Arc<Field>>,
    static_schema_flag: bool,
    time_partition: Option<&String>,
    schema_version: SchemaVersion,
) -> Result<(arrow_array::RecordBatch, bool), PostError> {
    let (rb, is_first) = json::Event { data }.into_recordbatch(
        &schema,
        static_schema_flag,
        time_partition,
        schema_version,
    )?;
    Ok((rb, is_first))
}

pub fn get_custom_partition_values(
    json: &Value,
    custom_partition_list: &[&str],
) -> HashMap<String, String> {
    let mut custom_partition_values: HashMap<String, String> = HashMap::new();
    for custom_partition_field in custom_partition_list {
        let custom_partition_value = json.get(custom_partition_field.trim()).unwrap().to_owned();
        let custom_partition_value = match custom_partition_value {
            e @ Value::Number(_) | e @ Value::Bool(_) => e.to_string(),
            Value::String(s) => s,
            _ => "".to_string(),
        };
        custom_partition_values.insert(
            custom_partition_field.trim().to_string(),
            custom_partition_value,
        );
    }
    custom_partition_values
}

fn get_parsed_timestamp(json: &Value, time_partition: &str) -> Result<NaiveDateTime, PostError> {
    let current_time = json
        .get(time_partition)
        .ok_or_else(|| PostError::MissingTimePartition(time_partition.to_string()))?;
    let parsed_time: DateTime<Utc> = serde_json::from_value(current_time.clone())?;

    Ok(parsed_time.naive_utc())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;

    use super::*;

    #[test]
    fn parse_time_parition_from_value() {
        let json = json!({"timestamp": "2025-05-15T15:30:00Z"});
        let parsed = get_parsed_timestamp(&json, "timestamp");

        let expected = NaiveDateTime::from_str("2025-05-15T15:30:00").unwrap();
        assert_eq!(parsed.unwrap(), expected);
    }

    #[test]
    fn time_parition_not_in_json() {
        let json = json!({"timestamp": "2025-05-15T15:30:00Z"});
        let parsed = get_parsed_timestamp(&json, "timestamp");

        matches!(parsed, Err(PostError::MissingTimePartition(_)));
    }

    #[test]
    fn time_parition_not_parseable_as_datetime() {
        let json = json!({"timestamp": "2025-05-15T15:30:00Z"});
        let parsed = get_parsed_timestamp(&json, "timestamp");

        matches!(parsed, Err(PostError::SerdeError(_)));
    }
}
