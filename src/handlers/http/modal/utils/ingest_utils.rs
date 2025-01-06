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

use actix_web::HttpRequest;
use anyhow::anyhow;
use arrow_schema::Field;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

use crate::{
    event::{
        self,
        format::{self, EventFormat, LogSource},
    },
    handlers::{
        http::{ingest::PostError, kinesis},
        LOG_SOURCE_KEY, PREFIX_META, PREFIX_TAGS, SEPARATOR,
    },
    metadata::{SchemaVersion, STREAM_INFO},
    storage::StreamType,
    utils::{header_parsing::collect_labelled_headers, json::convert_array_to_object},
};

pub async fn flatten_and_push_logs(
    req: HttpRequest,
    body: Bytes,
    stream_name: &str,
) -> Result<(), PostError> {
    let log_source = req
        .headers()
        .get(LOG_SOURCE_KEY)
        .map(|h| h.to_str().unwrap_or(""))
        .map(LogSource::from)
        .unwrap_or_default();

    match log_source {
        LogSource::Kinesis => {
            let json = kinesis::flatten_kinesis_logs(&body);
            for record in json.iter() {
                let body: Bytes = serde_json::to_vec(record).unwrap().into();
                push_logs(stream_name, &req, &body, &LogSource::default()).await?;
            }
        }
        LogSource::OtelLogs | LogSource::OtelMetrics | LogSource::OtelTraces => {
            return Err(PostError::Invalid(anyhow!(
                "Please use endpoints `/v1/logs` for otel logs, `/v1/metrics` for otel metrics and `/v1/traces` for otel traces"
            )));
        }
        _ => {
            push_logs(stream_name, &req, &body, &log_source).await?;
        }
    }
    Ok(())
}

pub async fn push_logs(
    stream_name: &str,
    req: &HttpRequest,
    body: &Bytes,
    log_source: &LogSource,
) -> Result<(), PostError> {
    let time_partition = STREAM_INFO.get_time_partition(stream_name)?;
    let time_partition_limit = STREAM_INFO.get_time_partition_limit(stream_name)?;
    let static_schema_flag = STREAM_INFO.get_static_schema_flag(stream_name)?;
    let custom_partition = STREAM_INFO.get_custom_partition(stream_name)?;
    let schema_version = STREAM_INFO.get_schema_version(stream_name)?;
    let body_val: Value = serde_json::from_slice(body)?;

    let size: usize = body.len();
    let mut parsed_timestamp = Utc::now().naive_utc();
    if time_partition.is_none() {
        if custom_partition.is_none() {
            let size = size as u64;
            create_process_record_batch(
                stream_name,
                req,
                body_val,
                static_schema_flag.as_ref(),
                None,
                parsed_timestamp,
                &HashMap::new(),
                size,
                schema_version,
                log_source,
            )
            .await?;
        } else {
            let data = convert_array_to_object(
                body_val,
                None,
                None,
                custom_partition.as_ref(),
                schema_version,
                log_source,
            )?;
            let custom_partition = custom_partition.unwrap();
            let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();

            for value in data {
                let custom_partition_values =
                    get_custom_partition_values(&value, &custom_partition_list);

                let size = value.to_string().into_bytes().len() as u64;
                create_process_record_batch(
                    stream_name,
                    req,
                    value,
                    static_schema_flag.as_ref(),
                    None,
                    parsed_timestamp,
                    &custom_partition_values,
                    size,
                    schema_version,
                    log_source,
                )
                .await?;
            }
        }
    } else if custom_partition.is_none() {
        let data = convert_array_to_object(
            body_val,
            time_partition.as_ref(),
            time_partition_limit,
            None,
            schema_version,
            log_source,
        )?;
        for value in data {
            parsed_timestamp = get_parsed_timestamp(&value, time_partition.as_ref().unwrap())?;
            let size = value.to_string().into_bytes().len() as u64;
            create_process_record_batch(
                stream_name,
                req,
                value,
                static_schema_flag.as_ref(),
                time_partition.as_ref(),
                parsed_timestamp,
                &HashMap::new(),
                size,
                schema_version,
                log_source,
            )
            .await?;
        }
    } else {
        let data = convert_array_to_object(
            body_val,
            time_partition.as_ref(),
            time_partition_limit,
            custom_partition.as_ref(),
            schema_version,
            log_source,
        )?;
        let custom_partition = custom_partition.unwrap();
        let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();

        for value in data {
            let custom_partition_values =
                get_custom_partition_values(&value, &custom_partition_list);

            parsed_timestamp = get_parsed_timestamp(&value, time_partition.as_ref().unwrap())?;
            let size = value.to_string().into_bytes().len() as u64;
            create_process_record_batch(
                stream_name,
                req,
                value,
                static_schema_flag.as_ref(),
                time_partition.as_ref(),
                parsed_timestamp,
                &custom_partition_values,
                size,
                schema_version,
                log_source,
            )
            .await?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn create_process_record_batch(
    stream_name: &str,
    req: &HttpRequest,
    value: Value,
    static_schema_flag: Option<&String>,
    time_partition: Option<&String>,
    parsed_timestamp: NaiveDateTime,
    custom_partition_values: &HashMap<String, String>,
    origin_size: u64,
    schema_version: SchemaVersion,
    log_source: &LogSource,
) -> Result<(), PostError> {
    let (rb, is_first_event) = get_stream_schema(
        stream_name,
        req,
        &value,
        static_schema_flag,
        time_partition,
        schema_version,
        log_source,
    )?;
    event::Event {
        rb,
        stream_name: stream_name.to_owned(),
        origin_format: "json",
        origin_size,
        is_first_event,
        parsed_timestamp,
        time_partition: time_partition.cloned(),
        custom_partition_values: custom_partition_values.clone(),
        stream_type: StreamType::UserDefined,
    }
    .process()
    .await?;

    Ok(())
}

pub fn get_stream_schema(
    stream_name: &str,
    req: &HttpRequest,
    body: &Value,
    static_schema_flag: Option<&String>,
    time_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
) -> Result<(arrow_array::RecordBatch, bool), PostError> {
    let hash_map = STREAM_INFO.read().unwrap();
    let schema = hash_map
        .get(stream_name)
        .ok_or(PostError::StreamNotFound(stream_name.to_owned()))?
        .schema
        .clone();
    into_event_batch(
        req,
        body,
        schema,
        static_schema_flag,
        time_partition,
        schema_version,
        log_source,
    )
}

pub fn into_event_batch(
    req: &HttpRequest,
    body: &Value,
    schema: HashMap<String, Arc<Field>>,
    static_schema_flag: Option<&String>,
    time_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
) -> Result<(arrow_array::RecordBatch, bool), PostError> {
    let tags = collect_labelled_headers(req, PREFIX_TAGS, SEPARATOR)?;
    let metadata = collect_labelled_headers(req, PREFIX_META, SEPARATOR)?;
    let event = format::json::Event {
        data: body.to_owned(),
        tags,
        metadata,
    };
    let (rb, is_first) = event.into_recordbatch(
        &schema,
        static_schema_flag,
        time_partition,
        schema_version,
        log_source,
    )?;
    Ok((rb, is_first))
}

pub fn get_custom_partition_values(
    body: &Value,
    custom_partition_list: &[&str],
) -> HashMap<String, String> {
    let mut custom_partition_values: HashMap<String, String> = HashMap::new();
    for custom_partition_field in custom_partition_list {
        let custom_partition_value = body.get(custom_partition_field.trim()).unwrap().to_owned();
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

fn get_parsed_timestamp(body: &Value, time_partition: &str) -> Result<NaiveDateTime, PostError> {
    let current_time = body.get(time_partition).ok_or_else(|| {
        anyhow!(
            "Missing field for time partition from json: {:?}",
            time_partition
        )
    })?;
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

        matches!(parsed, Err(PostError::Invalid(_)));
    }

    #[test]
    fn time_parition_not_parseable_as_datetime() {
        let json = json!({"timestamp": "2025-05-15T15:30:00Z"});
        let parsed = get_parsed_timestamp(&json, "timestamp");

        matches!(parsed, Err(PostError::SerdeError(_)));
    }
}
