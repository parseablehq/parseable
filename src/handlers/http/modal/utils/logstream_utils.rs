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

use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use actix_web::http::header::HeaderMap;
use arrow_schema::{Field, Schema};
use bytes::Bytes;
use http::StatusCode;

use crate::{
    event::format::LogSource,
    handlers::{
        http::logstream::error::{CreateStreamError, StreamError},
        CUSTOM_PARTITION_KEY, LOG_SOURCE_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY,
        TIME_PARTITION_KEY, TIME_PARTITION_LIMIT_KEY, UPDATE_STREAM_KEY,
    },
    metadata::{self, SchemaVersion, STREAM_INFO},
    option::{Mode, CONFIG},
    static_schema::{convert_static_schema_to_arrow_schema, StaticSchema},
    storage::{LogStream, ObjectStoreFormat, StreamType},
    validator,
};

pub async fn create_update_stream(
    headers: &HeaderMap,
    body: &Bytes,
    stream_name: &str,
) -> Result<HeaderMap, StreamError> {
    let PutStreamHeaders {
        time_partition,
        time_partition_limit,
        custom_partition,
        static_schema_flag,
        update_stream_flag,
        stream_type,
        log_source,
    } = headers.into();

    if metadata::STREAM_INFO.stream_exists(stream_name) && !update_stream_flag {
        return Err(StreamError::Custom {
            msg: format!(
                "Logstream {stream_name} already exists, please create a new log stream with unique name"
            ),
            status: StatusCode::BAD_REQUEST,
        });
    }

    if !metadata::STREAM_INFO.stream_exists(stream_name)
        && CONFIG.options.mode == Mode::Query
        && create_stream_and_schema_from_storage(stream_name).await?
    {
        return Err(StreamError::Custom {
            msg: format!(
                "Logstream {stream_name} already exists, please create a new log stream with unique name"
            ),
            status: StatusCode::BAD_REQUEST,
        });
    }

    if update_stream_flag {
        return update_stream(
            headers,
            stream_name,
            &time_partition,
            static_schema_flag,
            &time_partition_limit,
            &custom_partition,
        )
        .await;
    }

    let time_partition_in_days = if !time_partition_limit.is_empty() {
        Some(validate_time_partition_limit(&time_partition_limit)?)
    } else {
        None
    };

    if !custom_partition.is_empty() {
        validate_custom_partition(&custom_partition)?;
    }

    if !time_partition.is_empty() && !custom_partition.is_empty() {
        validate_time_with_custom_partition(&time_partition, &custom_partition)?;
    }

    let schema = validate_static_schema(
        body,
        stream_name,
        &time_partition,
        &custom_partition,
        static_schema_flag,
    )?;

    create_stream(
        stream_name.to_string(),
        &time_partition,
        time_partition_in_days,
        &custom_partition,
        static_schema_flag,
        schema,
        stream_type,
        log_source,
    )
    .await?;

    Ok(headers.clone())
}

async fn update_stream(
    headers: &HeaderMap,
    stream_name: &str,
    time_partition: &str,
    static_schema_flag: bool,
    time_partition_limit: &str,
    custom_partition: &str,
) -> Result<HeaderMap, StreamError> {
    if !STREAM_INFO.stream_exists(stream_name) {
        return Err(StreamError::StreamNotFound(stream_name.to_string()));
    }
    if !time_partition.is_empty() {
        return Err(StreamError::Custom {
            msg: "Altering the time partition of an existing stream is restricted.".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    if static_schema_flag {
        return Err(StreamError::Custom {
            msg: "Altering the schema of an existing stream is restricted.".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    if !time_partition_limit.is_empty() {
        let time_partition_days = validate_time_partition_limit(time_partition_limit)?;
        update_time_partition_limit_in_stream(stream_name.to_string(), time_partition_days).await?;
        return Ok(headers.clone());
    }
    validate_and_update_custom_partition(stream_name, custom_partition).await?;

    Ok(headers.clone())
}

async fn validate_and_update_custom_partition(
    stream_name: &str,
    custom_partition: &str,
) -> Result<(), StreamError> {
    if !custom_partition.is_empty() {
        validate_custom_partition(custom_partition)?;
        update_custom_partition_in_stream(stream_name.to_string(), custom_partition).await?;
    } else {
        update_custom_partition_in_stream(stream_name.to_string(), "").await?;
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct PutStreamHeaders {
    pub time_partition: String,
    pub time_partition_limit: String,
    pub custom_partition: String,
    pub static_schema_flag: bool,
    pub update_stream_flag: bool,
    pub stream_type: StreamType,
    pub log_source: LogSource,
}

impl From<&HeaderMap> for PutStreamHeaders {
    fn from(headers: &HeaderMap) -> Self {
        PutStreamHeaders {
            time_partition: headers
                .get(TIME_PARTITION_KEY)
                .map_or("", |v| v.to_str().unwrap())
                .to_string(),
            time_partition_limit: headers
                .get(TIME_PARTITION_LIMIT_KEY)
                .map_or("", |v| v.to_str().unwrap())
                .to_string(),
            custom_partition: headers
                .get(CUSTOM_PARTITION_KEY)
                .map_or("", |v| v.to_str().unwrap())
                .to_string(),
            static_schema_flag: headers
                .get(STATIC_SCHEMA_FLAG)
                .is_some_and(|v| v.to_str().unwrap() == "true"),
            update_stream_flag: headers
                .get(UPDATE_STREAM_KEY)
                .is_some_and(|v| v.to_str().unwrap() == "true"),
            stream_type: headers
                .get(STREAM_TYPE_KEY)
                .map(|v| StreamType::from(v.to_str().unwrap()))
                .unwrap_or_default(),
            log_source: headers
                .get(LOG_SOURCE_KEY)
                .map_or(LogSource::default(), |v| v.to_str().unwrap().into()),
        }
    }
}

pub fn validate_time_partition_limit(
    time_partition_limit: &str,
) -> Result<NonZeroU32, CreateStreamError> {
    if !time_partition_limit.ends_with('d') {
        return Err(CreateStreamError::Custom {
            msg: "Missing 'd' suffix for duration value".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    let days = &time_partition_limit[0..time_partition_limit.len() - 1];
    let Ok(days) = days.parse::<NonZeroU32>() else {
        return Err(CreateStreamError::Custom {
            msg: "Could not convert duration to an unsigned number".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    };

    Ok(days)
}

pub fn validate_custom_partition(custom_partition: &str) -> Result<(), CreateStreamError> {
    let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
    if custom_partition_list.len() > 3 {
        return Err(CreateStreamError::Custom {
            msg: "Maximum 3 custom partition keys are supported".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    Ok(())
}

pub fn validate_time_with_custom_partition(
    time_partition: &str,
    custom_partition: &str,
) -> Result<(), CreateStreamError> {
    let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
    if custom_partition_list.contains(&time_partition) {
        return Err(CreateStreamError::Custom {
            msg: format!(
                "time partition {} cannot be set as custom partition",
                time_partition
            ),
            status: StatusCode::BAD_REQUEST,
        });
    }
    Ok(())
}

pub fn validate_static_schema(
    body: &Bytes,
    stream_name: &str,
    time_partition: &str,
    custom_partition: &str,
    static_schema_flag: bool,
) -> Result<Arc<Schema>, CreateStreamError> {
    if static_schema_flag {
        if body.is_empty() {
            return Err(CreateStreamError::Custom {
                msg: format!(
                    "Please provide schema in the request body for static schema logstream {stream_name}"
                ),
                status: StatusCode::BAD_REQUEST,
            });
        }

        let static_schema: StaticSchema = serde_json::from_slice(body)?;
        let parsed_schema =
            convert_static_schema_to_arrow_schema(static_schema, time_partition, custom_partition)
                .map_err(|_| CreateStreamError::Custom {
                    msg: format!(
                        "Unable to commit static schema, logstream {stream_name} not created"
                    ),
                    status: StatusCode::BAD_REQUEST,
                })?;

        return Ok(parsed_schema);
    }

    Ok(Arc::new(Schema::empty()))
}

pub async fn update_time_partition_limit_in_stream(
    stream_name: String,
    time_partition_limit: NonZeroU32,
) -> Result<(), CreateStreamError> {
    let storage = CONFIG.storage().get_object_store();
    if let Err(err) = storage
        .update_time_partition_limit_in_stream(&stream_name, time_partition_limit)
        .await
    {
        return Err(CreateStreamError::Storage { stream_name, err });
    }

    if metadata::STREAM_INFO
        .update_time_partition_limit(&stream_name, time_partition_limit)
        .is_err()
    {
        return Err(CreateStreamError::Custom {
            msg: "failed to update time partition limit in metadata".to_string(),
            status: StatusCode::EXPECTATION_FAILED,
        });
    }

    Ok(())
}

pub async fn update_custom_partition_in_stream(
    stream_name: String,
    custom_partition: &str,
) -> Result<(), CreateStreamError> {
    let static_schema_flag = STREAM_INFO.get_static_schema_flag(&stream_name).unwrap();
    let time_partition = STREAM_INFO.get_time_partition(&stream_name).unwrap();
    if static_schema_flag {
        let schema = STREAM_INFO.schema(&stream_name).unwrap();

        if !custom_partition.is_empty() {
            let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();
            let custom_partition_exists: HashMap<_, _> = custom_partition_list
                .iter()
                .map(|&partition| {
                    (
                        partition.to_string(),
                        schema
                            .fields()
                            .iter()
                            .any(|field| field.name() == partition),
                    )
                })
                .collect();

            for partition in &custom_partition_list {
                if !custom_partition_exists[*partition] {
                    return Err(CreateStreamError::Custom {
                        msg: format!("custom partition field {} does not exist in the schema for the stream {}", partition, stream_name),
                        status: StatusCode::BAD_REQUEST,
                    });
                }

                if let Some(time_partition) = time_partition.clone() {
                    if time_partition == *partition {
                        return Err(CreateStreamError::Custom {
                            msg: format!(
                                "time partition {} cannot be set as custom partition",
                                partition
                            ),
                            status: StatusCode::BAD_REQUEST,
                        });
                    }
                }
            }
        }
    }
    let storage = CONFIG.storage().get_object_store();
    if let Err(err) = storage
        .update_custom_partition_in_stream(&stream_name, custom_partition)
        .await
    {
        return Err(CreateStreamError::Storage { stream_name, err });
    }

    if metadata::STREAM_INFO
        .update_custom_partition(&stream_name, custom_partition.to_string())
        .is_err()
    {
        return Err(CreateStreamError::Custom {
            msg: "failed to update custom partition in metadata".to_string(),
            status: StatusCode::EXPECTATION_FAILED,
        });
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn create_stream(
    stream_name: String,
    time_partition: &str,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: &str,
    static_schema_flag: bool,
    schema: Arc<Schema>,
    stream_type: StreamType,
    log_source: LogSource,
) -> Result<(), CreateStreamError> {
    // fail to proceed if invalid stream name
    if stream_type != StreamType::Internal {
        validator::stream_name(&stream_name, stream_type)?;
    }
    // Proceed to create log stream if it doesn't exist
    let storage = CONFIG.storage().get_object_store();

    match storage
        .create_stream(
            &stream_name,
            time_partition,
            time_partition_limit,
            custom_partition,
            static_schema_flag,
            schema.clone(),
            stream_type,
            log_source.clone(),
        )
        .await
    {
        Ok(created_at) => {
            let mut static_schema: HashMap<String, Arc<Field>> = HashMap::new();

            for (field_name, field) in schema
                .fields()
                .iter()
                .map(|field| (field.name().to_string(), field.clone()))
            {
                static_schema.insert(field_name, field);
            }

            metadata::STREAM_INFO.add_stream(
                stream_name.to_string(),
                created_at,
                time_partition.to_string(),
                time_partition_limit,
                custom_partition.to_string(),
                static_schema_flag,
                static_schema,
                stream_type,
                SchemaVersion::V1, // New stream
                log_source,
            );
        }
        Err(err) => {
            return Err(CreateStreamError::Storage { stream_name, err });
        }
    }
    Ok(())
}

/// list all streams from storage
/// if stream exists in storage, create stream and schema from storage
/// and add it to the memory map
pub async fn create_stream_and_schema_from_storage(stream_name: &str) -> Result<bool, StreamError> {
    // Proceed to create log stream if it doesn't exist
    let storage = CONFIG.storage().get_object_store();
    let streams = storage.list_streams().await?;
    if streams.contains(&LogStream {
        name: stream_name.to_owned(),
    }) {
        let mut stream_metadata = ObjectStoreFormat::default();
        let stream_metadata_bytes = storage.create_stream_from_ingestor(stream_name).await?;
        if !stream_metadata_bytes.is_empty() {
            stream_metadata = serde_json::from_slice::<ObjectStoreFormat>(&stream_metadata_bytes)?;
        }

        let mut schema = Arc::new(Schema::empty());
        let schema_bytes = storage.create_schema_from_ingestor(stream_name).await?;
        if !schema_bytes.is_empty() {
            schema = serde_json::from_slice::<Arc<Schema>>(&schema_bytes)?;
        }

        let mut static_schema: HashMap<String, Arc<Field>> = HashMap::new();

        for (field_name, field) in schema
            .fields()
            .iter()
            .map(|field| (field.name().to_string(), field.clone()))
        {
            static_schema.insert(field_name, field);
        }

        let time_partition = stream_metadata.time_partition.as_deref().unwrap_or("");
        let time_partition_limit = stream_metadata
            .time_partition_limit
            .and_then(|limit| limit.parse().ok());
        let custom_partition = stream_metadata.custom_partition.as_deref().unwrap_or("");
        let static_schema_flag = stream_metadata.static_schema_flag;
        let stream_type = stream_metadata
            .stream_type
            .map(|s| StreamType::from(s.as_str()))
            .unwrap_or_default();
        let schema_version = stream_metadata.schema_version;
        let log_source = stream_metadata.log_source;
        metadata::STREAM_INFO.add_stream(
            stream_name.to_string(),
            stream_metadata.created_at,
            time_partition.to_string(),
            time_partition_limit,
            custom_partition.to_string(),
            static_schema_flag,
            static_schema,
            stream_type,
            schema_version,
            log_source,
        );
    } else {
        return Ok(false);
    }

    Ok(true)
}
