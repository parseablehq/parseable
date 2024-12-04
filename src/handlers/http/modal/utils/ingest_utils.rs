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

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use actix_web::HttpRequest;
use arrow_schema::Field;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;

use crate::{
    event::{
        self,
        format::{self, EventFormat},
    },
    handlers::{
        http::{ingest::PostError, kinesis},
        LOG_SOURCE_KEY, LOG_SOURCE_KINESIS, PREFIX_META, PREFIX_TAGS, SEPARATOR,
    },
    metadata::STREAM_INFO,
    storage::StreamType,
    utils::{header_parsing::collect_labelled_headers, json::convert_array_to_object},
};

pub async fn flatten_and_push_logs(
    req: HttpRequest,
    body: Bytes,
    stream_name: String,
) -> Result<(), PostError> {
    //flatten logs
    if let Some((_, log_source)) = req.headers().iter().find(|&(key, _)| key == LOG_SOURCE_KEY) {
        let mut json: Vec<BTreeMap<String, Value>> = Vec::new();
        let log_source: String = log_source.to_str().unwrap().to_owned();
        match log_source.as_str() {
            LOG_SOURCE_KINESIS => json = kinesis::flatten_kinesis_logs(&body),
            _ => {
                log::warn!("Unknown log source: {}", log_source);
                push_logs(stream_name.to_string(), req.clone(), body).await?;
            }
        }
        for record in json.iter_mut() {
            let body: Bytes = serde_json::to_vec(record).unwrap().into();
            push_logs(stream_name.to_string(), req.clone(), body).await?;
        }
    } else {
        push_logs(stream_name.to_string(), req, body).await?;
    }
    Ok(())
}

pub async fn push_logs(
    stream_name: String,
    req: HttpRequest,
    body: Bytes,
) -> Result<(), PostError> {
    let time_partition = STREAM_INFO.get_time_partition(&stream_name)?;
    let time_partition_limit = STREAM_INFO.get_time_partition_limit(&stream_name)?;
    let static_schema_flag = STREAM_INFO.get_static_schema_flag(&stream_name)?;
    let custom_partition = STREAM_INFO.get_custom_partition(&stream_name)?;
    let body_val: Value = serde_json::from_slice(&body)?;
    let size: usize = body.len();
    let mut parsed_timestamp = Utc::now().naive_utc();
    if time_partition.is_none() {
        if custom_partition.is_none() {
            let size = size as u64;
            create_process_record_batch(
                stream_name.clone(),
                req.clone(),
                body_val.clone(),
                static_schema_flag.clone(),
                None,
                parsed_timestamp,
                HashMap::new(),
                size,
            )
            .await?;
        } else {
            let data =
                convert_array_to_object(body_val.clone(), None, None, custom_partition.clone())?;
            let custom_partition = custom_partition.unwrap();
            let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();

            for value in data {
                let custom_partition_values =
                    get_custom_partition_values(&value, &custom_partition_list);

                let size = value.to_string().into_bytes().len() as u64;
                create_process_record_batch(
                    stream_name.clone(),
                    req.clone(),
                    value.clone(),
                    static_schema_flag.clone(),
                    None,
                    parsed_timestamp,
                    custom_partition_values.clone(),
                    size,
                )
                .await?;
            }
        }
    } else if custom_partition.is_none() {
        let data = convert_array_to_object(
            body_val.clone(),
            time_partition.clone(),
            time_partition_limit,
            None,
        )?;
        for value in data {
            parsed_timestamp = get_parsed_timestamp(&value, &time_partition);
            let size = value.to_string().into_bytes().len() as u64;
            create_process_record_batch(
                stream_name.clone(),
                req.clone(),
                value.clone(),
                static_schema_flag.clone(),
                time_partition.clone(),
                parsed_timestamp,
                HashMap::new(),
                size,
            )
            .await?;
        }
    } else {
        let data = convert_array_to_object(
            body_val.clone(),
            time_partition.clone(),
            time_partition_limit,
            custom_partition.clone(),
        )?;
        let custom_partition = custom_partition.unwrap();
        let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();

        for value in data {
            let custom_partition_values =
                get_custom_partition_values(&value, &custom_partition_list);

            parsed_timestamp = get_parsed_timestamp(&value, &time_partition);
            let size = value.to_string().into_bytes().len() as u64;
            create_process_record_batch(
                stream_name.clone(),
                req.clone(),
                value.clone(),
                static_schema_flag.clone(),
                time_partition.clone(),
                parsed_timestamp,
                custom_partition_values.clone(),
                size,
            )
            .await?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn create_process_record_batch(
    stream_name: String,
    req: HttpRequest,
    value: Value,
    static_schema_flag: Option<String>,
    time_partition: Option<String>,
    parsed_timestamp: NaiveDateTime,
    custom_partition_values: HashMap<String, String>,
    origin_size: u64,
) -> Result<(), PostError> {
    let (rb, is_first_event) = get_stream_schema(
        stream_name.clone(),
        req.clone(),
        value.clone(),
        static_schema_flag.clone(),
        time_partition.clone(),
    )?;
    event::Event {
        rb,
        stream_name: stream_name.clone(),
        origin_format: "json",
        origin_size,
        is_first_event,
        parsed_timestamp,
        time_partition: time_partition.clone(),
        custom_partition_values: custom_partition_values.clone(),
        stream_type: StreamType::UserDefined,
    }
    .process()
    .await?;

    Ok(())
}

pub fn get_stream_schema(
    stream_name: String,
    req: HttpRequest,
    body: Value,
    static_schema_flag: Option<String>,
    time_partition: Option<String>,
) -> Result<(arrow_array::RecordBatch, bool), PostError> {
    let hash_map = STREAM_INFO.read().unwrap();
    let schema = hash_map
        .get(&stream_name)
        .ok_or(PostError::StreamNotFound(stream_name))?
        .schema
        .clone();
    into_event_batch(req, body, schema, static_schema_flag, time_partition)
}

pub fn into_event_batch(
    req: HttpRequest,
    body: Value,
    schema: HashMap<String, Arc<Field>>,
    static_schema_flag: Option<String>,
    time_partition: Option<String>,
) -> Result<(arrow_array::RecordBatch, bool), PostError> {
    let tags = collect_labelled_headers(&req, PREFIX_TAGS, SEPARATOR)?;
    let metadata = collect_labelled_headers(&req, PREFIX_META, SEPARATOR)?;
    let event = format::json::Event {
        data: body,
        tags,
        metadata,
    };
    let (rb, is_first) = event.into_recordbatch(schema, static_schema_flag, time_partition)?;
    Ok((rb, is_first))
}

pub fn get_custom_partition_values(
    body: &Value,
    custom_partition_list: &[&str],
) -> HashMap<String, String> {
    let mut custom_partition_values: HashMap<String, String> = HashMap::new();
    for custom_partition_field in custom_partition_list {
        let custom_partition_value = body.get(custom_partition_field.trim()).unwrap().to_owned();
        let custom_partition_value = match custom_partition_value.clone() {
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

pub fn get_parsed_timestamp(body: &Value, time_partition: &Option<String>) -> NaiveDateTime {
    let body_timestamp = body.get(time_partition.clone().unwrap().to_string());
    let parsed_timestamp = body_timestamp
        .unwrap()
        .to_owned()
        .as_str()
        .unwrap()
        .parse::<DateTime<Utc>>()
        .unwrap()
        .naive_utc();
    parsed_timestamp
}
