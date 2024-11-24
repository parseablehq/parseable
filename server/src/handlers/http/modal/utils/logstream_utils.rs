use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use actix_web::{http::header::HeaderMap, HttpRequest};
use arrow_schema::{Field, Schema};
use bytes::Bytes;
use http::StatusCode;
use relative_path::RelativePathBuf;

use crate::{
    catalog::snapshot::Snapshot,
    handlers::{
        http::logstream::error::{CreateStreamError, StreamError},
        CUSTOM_PARTITION_KEY, STATIC_SCHEMA_FLAG, STREAM_TYPE_KEY, TIME_PARTITION_KEY,
        TIME_PARTITION_LIMIT_KEY, UPDATE_STREAM_KEY,
    },
    metadata::{self, STREAM_INFO},
    option::{Mode, CONFIG},
    static_schema::{convert_static_schema_to_arrow_schema, StaticSchema},
    stats::FullStats,
    storage::{
        object_storage::{schema_path, stream_json_path},
        LogStream, ObjectStoreFormat, StreamType, STREAM_ROOT_DIRECTORY,
    },
    validator,
};

pub async fn create_update_stream(
    req: &HttpRequest,
    body: &Bytes,
    stream_name: &str,
) -> Result<HeaderMap, StreamError> {
    let (
        time_partition,
        time_partition_limit,
        custom_partition,
        static_schema_flag,
        update_stream,
        stream_type,
    ) = fetch_headers_from_put_stream_request(req);

    if metadata::STREAM_INFO.stream_exists(stream_name) && update_stream != "true" {
        return Err(StreamError::Custom {
            msg: format!(
                "Logstream {stream_name} already exists, please create a new log stream with unique name"
            ),
            status: StatusCode::BAD_REQUEST,
        });
    }

    if CONFIG.parseable.mode == Mode::Query {
        create_stream_and_schema_from_storage(stream_name).await?;
        return Err(StreamError::Custom {
                    msg: format!(
                        "Logstream {stream_name} already exists, please create a new log stream with unique name"
                    ),
                    status: StatusCode::BAD_REQUEST,
                });
    }

    if update_stream == "true" {
        if !STREAM_INFO.stream_exists(stream_name) {
            return Err(StreamError::StreamNotFound(stream_name.to_string()));
        }
        if !time_partition.is_empty() {
            return Err(StreamError::Custom {
                msg: "Altering the time partition of an existing stream is restricted.".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }

        if !static_schema_flag.is_empty() {
            return Err(StreamError::Custom {
                msg: "Altering the schema of an existing stream is restricted.".to_string(),
                status: StatusCode::BAD_REQUEST,
            });
        }

        if !time_partition_limit.is_empty() {
            let time_partition_days = validate_time_partition_limit(&time_partition_limit)?;
            update_time_partition_limit_in_stream(stream_name.to_string(), time_partition_days)
                .await?;
            return Ok(req.headers().clone());
        }

        if !custom_partition.is_empty() {
            validate_custom_partition(&custom_partition)?;
            update_custom_partition_in_stream(stream_name.to_string(), &custom_partition).await?;
        } else {
            update_custom_partition_in_stream(stream_name.to_string(), "").await?;
        }
        return Ok(req.headers().clone());
    }
    let mut time_partition_in_days = "";
    if !time_partition_limit.is_empty() {
        time_partition_in_days = validate_time_partition_limit(&time_partition_limit)?;
    }
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
        &static_schema_flag,
    )?;

    create_stream(
        stream_name.to_string(),
        &time_partition,
        time_partition_in_days,
        &custom_partition,
        &static_schema_flag,
        schema,
        &stream_type,
    )
    .await?;

    Ok(req.headers().clone())
}

pub fn fetch_headers_from_put_stream_request(
    req: &HttpRequest,
) -> (String, String, String, String, String, String) {
    let mut time_partition = String::default();
    let mut time_partition_limit = String::default();
    let mut custom_partition = String::default();
    let mut static_schema_flag = String::default();
    let mut update_stream = String::default();
    let mut stream_type = StreamType::UserDefined.to_string();
    req.headers().iter().for_each(|(key, value)| {
        if key == TIME_PARTITION_KEY {
            time_partition = value.to_str().unwrap().to_string();
        }
        if key == TIME_PARTITION_LIMIT_KEY {
            time_partition_limit = value.to_str().unwrap().to_string();
        }
        if key == CUSTOM_PARTITION_KEY {
            custom_partition = value.to_str().unwrap().to_string();
        }
        if key == STATIC_SCHEMA_FLAG {
            static_schema_flag = value.to_str().unwrap().to_string();
        }
        if key == UPDATE_STREAM_KEY {
            update_stream = value.to_str().unwrap().to_string();
        }
        if key == STREAM_TYPE_KEY {
            stream_type = value.to_str().unwrap().to_string();
        }
    });

    (
        time_partition,
        time_partition_limit,
        custom_partition,
        static_schema_flag,
        update_stream,
        stream_type,
    )
}

pub fn validate_time_partition_limit(
    time_partition_limit: &str,
) -> Result<&str, CreateStreamError> {
    if !time_partition_limit.ends_with('d') {
        return Err(CreateStreamError::Custom {
            msg: "Missing 'd' suffix for duration value".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }
    let days = &time_partition_limit[0..time_partition_limit.len() - 1];
    if days.parse::<NonZeroU32>().is_err() {
        return Err(CreateStreamError::Custom {
            msg: "Could not convert duration to an unsigned number".to_string(),
            status: StatusCode::BAD_REQUEST,
        });
    }

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
    static_schema_flag: &str,
) -> Result<Arc<Schema>, CreateStreamError> {
    if static_schema_flag == "true" {
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
    time_partition_limit: &str,
) -> Result<(), CreateStreamError> {
    let storage = CONFIG.storage().get_object_store();
    if let Err(err) = storage
        .update_time_partition_limit_in_stream(&stream_name, time_partition_limit)
        .await
    {
        return Err(CreateStreamError::Storage { stream_name, err });
    }

    if metadata::STREAM_INFO
        .update_time_partition_limit(&stream_name, time_partition_limit.to_string())
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
    if static_schema_flag.is_some() {
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

pub async fn create_stream(
    stream_name: String,
    time_partition: &str,
    time_partition_limit: &str,
    custom_partition: &str,
    static_schema_flag: &str,
    schema: Arc<Schema>,
    stream_type: &str,
) -> Result<(), CreateStreamError> {
    // fail to proceed if invalid stream name
    if stream_type != StreamType::Internal.to_string() {
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
                time_partition_limit.to_string(),
                custom_partition.to_string(),
                static_schema_flag.to_string(),
                static_schema,
                stream_type,
            );
        }
        Err(err) => {
            return Err(CreateStreamError::Storage { stream_name, err });
        }
    }
    Ok(())
}

pub async fn create_stream_and_schema_from_storage(stream_name: &str) -> Result<(), StreamError> {
    // Proceed to create log stream if it doesn't exist
    let storage = CONFIG.storage().get_object_store();
    let streams = storage.list_streams().await?;
    if streams.contains(&LogStream {
        name: stream_name.to_owned(),
    }) {
        let mut stream_metadata = ObjectStoreFormat::default();
        let path = RelativePathBuf::from_iter([stream_name, STREAM_ROOT_DIRECTORY]);
        let stream_obs = storage
            .get_objects(
                Some(&path),
                Box::new(|file_name| {
                    file_name.starts_with(".ingestor") && file_name.ends_with("stream.json")
                }),
            )
            .await
            .into_iter()
            .next();
        if let Some(stream_obs) = stream_obs {
            let stream_ob = &stream_obs[0];
            let stream_ob_metdata = serde_json::from_slice::<ObjectStoreFormat>(stream_ob)?;
            stream_metadata = ObjectStoreFormat {
                stats: FullStats::default(),
                snapshot: Snapshot::default(),
                ..stream_ob_metdata
            };

            let stream_metadata_bytes = serde_json::to_vec(&stream_metadata)?.into();
            storage
                .put_object(&stream_json_path(stream_name), stream_metadata_bytes)
                .await?;
        }

        let mut schema = Arc::new(Schema::empty());
        let schema_obs = storage
            .get_objects(
                Some(&path),
                Box::new(|file_name| {
                    file_name.starts_with(".ingestor") && file_name.ends_with("schema")
                }),
            )
            .await
            .into_iter()
            .next();
        if let Some(schema_obs) = schema_obs {
            let schema_ob = &schema_obs[0];
            storage
                .put_object(&schema_path(stream_name), schema_ob.clone())
                .await?;
            schema = serde_json::from_slice::<Arc<Schema>>(schema_ob)?;
        }

        let mut static_schema: HashMap<String, Arc<Field>> = HashMap::new();

        for (field_name, field) in schema
            .fields()
            .iter()
            .map(|field| (field.name().to_string(), field.clone()))
        {
            static_schema.insert(field_name, field);
        }

        let time_partition = if stream_metadata.time_partition.is_some() {
            stream_metadata.time_partition.as_ref().unwrap()
        } else {
            ""
        };
        let time_partition_limit = if stream_metadata.time_partition_limit.is_some() {
            stream_metadata.time_partition_limit.as_ref().unwrap()
        } else {
            ""
        };
        let custom_partition = if stream_metadata.custom_partition.is_some() {
            stream_metadata.custom_partition.as_ref().unwrap()
        } else {
            ""
        };
        let static_schema_flag = if stream_metadata.static_schema_flag.is_some() {
            stream_metadata.static_schema_flag.as_ref().unwrap()
        } else {
            ""
        };
        let stream_type = if stream_metadata.stream_type.is_some() {
            stream_metadata.stream_type.as_ref().unwrap()
        } else {
            ""
        };

        metadata::STREAM_INFO.add_stream(
            stream_name.to_string(),
            stream_metadata.created_at,
            time_partition.to_string(),
            time_partition_limit.to_string(),
            custom_partition.to_string(),
            static_schema_flag.to_string(),
            static_schema,
            stream_type,
        );
    } else {
        return Err(StreamError::StreamNotFound(stream_name.to_string()));
    }

    Ok(())
}
