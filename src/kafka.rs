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
use chrono::Utc;
use futures_util::StreamExt;
use itertools::Itertools;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError as NativeKafkaError, RDKafkaError};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::num::ParseIntError;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};
use tracing::{debug, error, info, warn};

use crate::option::CONFIG;
use crate::{
    event::{
        self,
        error::EventError,
        format::{self, EventFormat},
    },
    handlers::http::ingest::{create_stream_if_not_exists, PostError},
    metadata::{error::stream_info::MetadataError, STREAM_INFO},
    storage::StreamType,
};

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub enum SslProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Please set env var {0} (To use Kafka integration env vars P_KAFKA_TOPICS, P_KAFKA_HOST, and P_KAFKA_GROUP are mandatory)")]
    NoVarError(&'static str),

    #[error("Kafka error {0}")]
    NativeError(#[from] NativeKafkaError),
    #[error("RDKafka error {0}")]
    RDKError(#[from] RDKafkaError),

    #[error("Error parsing int {1} for environment variable {0}")]
    ParseIntError(&'static str, ParseIntError),
    #[error("Error parsing duration int {1} for environment variable {0}")]
    ParseDurationError(&'static str, ParseIntError),

    #[error("Stream not found: #{0}")]
    StreamNotFound(String),
    #[error("Post error: #{0}")]
    PostError(#[from] PostError),
    #[error("Metadata error: #{0}")]
    MetadataError(#[from] MetadataError),
    #[error("Event error: #{0}")]
    EventError(#[from] EventError),
    #[error("JSON error: #{0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Invalid group offset storage: #{0}")]
    InvalidGroupOffsetStorage(String),

    #[error("Invalid SSL protocol: #{0}")]
    InvalidSslProtocolError(String),
    #[error("Invalid unicode for environment variable {0}")]
    EnvNotUnicode(&'static str),
    #[error("")]
    DoNotPrintError,
}

fn setup_consumer() -> Result<(StreamConsumer, Vec<String>), KafkaError> {
    if let Some(topics) = &CONFIG.parseable.kafka_topics {
        // topics can be a comma separated list of topics to subscribe to
        let topics = topics.split(',').map(|v| v.to_owned()).collect_vec();

        let host = if CONFIG.parseable.kafka_host.is_some() {
            CONFIG.parseable.kafka_host.as_ref()
        } else {
            return Err(KafkaError::NoVarError("P_KAKFA_HOST"));
        };

        let group = if CONFIG.parseable.kafka_group.is_some() {
            CONFIG.parseable.kafka_group.as_ref()
        } else {
            return Err(KafkaError::NoVarError("P_KAKFA_GROUP"));
        };

        let mut conf = ClientConfig::new();
        conf.set("bootstrap.servers", host.unwrap());
        conf.set("group.id", group.unwrap());

        if let Some(val) = CONFIG.parseable.kafka_client_id.as_ref() {
            conf.set("client.id", val);
        }

        if let Some(ssl_protocol) = CONFIG.parseable.kafka_security_protocol.as_ref() {
            conf.set("security.protocol", serde_json::to_string(&ssl_protocol)?);
        }

        let consumer: StreamConsumer = conf.create()?;
        consumer.subscribe(&topics.iter().map(|v| v.as_str()).collect_vec())?;

        if let Some(vals_raw) = CONFIG.parseable.kafka_partitions.as_ref() {
            // partitions is a comma separated pairs of topic:partitions
            let mut topic_partition_pairs = Vec::new();
            let mut set = true;
            for vals in vals_raw.split(',') {
                let intermediate = vals.split(':').collect_vec();
                if intermediate.len() != 2 {
                    warn!(
                        "Value for P_KAFKA_PARTITIONS is incorrect! Skipping setting partitions!"
                    );
                    set = false;
                    break;
                }
                topic_partition_pairs.push(intermediate);
            }

            if set {
                let mut parts = TopicPartitionList::new();
                for pair in topic_partition_pairs {
                    let topic = pair[0];
                    match pair[1].parse::<i32>() {
                        Ok(partition) => {
                            parts.add_partition(topic, partition);
                        }
                        Err(_) => warn!("Skipping setting partition for topic- {topic}"),
                    }
                }
                consumer.seek_partitions(parts, Timeout::Never)?;
            }
        }
        Ok((consumer, topics.clone()))
    } else {
        // if the user hasn't even set KAFKA_TOPICS
        // then they probably don't want to use the integration
        // send back the DoNotPrint error
        Err(KafkaError::DoNotPrintError)
    }
}

fn resolve_schema(stream_name: &str) -> Result<HashMap<String, Arc<Field>>, KafkaError> {
    let hash_map = STREAM_INFO.read().unwrap();
    let raw = hash_map
        .get(stream_name)
        .ok_or_else(|| KafkaError::StreamNotFound(stream_name.to_owned()))?;
    Ok(raw.schema.clone())
}

async fn ingest_message(msg: BorrowedMessage<'_>) -> Result<(), KafkaError> {
    let Some(payload) = msg.payload() else {
        debug!("No payload received");
        return Ok(());
    };

    let msg = msg.detach();
    let stream_name = msg.topic();

    // stream should get created only if there is an incoming event, not before that
    create_stream_if_not_exists(stream_name, &StreamType::UserDefined.to_string()).await?;

    let schema = resolve_schema(stream_name)?;
    let event = format::json::Event {
        data: serde_json::from_slice(payload)?,
        tags: String::default(),
        metadata: String::default(),
    };

    let time_partition = STREAM_INFO.get_time_partition(stream_name)?;
    let static_schema_flag = STREAM_INFO.get_static_schema_flag(stream_name)?;
    let schema_version = STREAM_INFO.get_schema_version(stream_name)?;

    let (rb, is_first) = event
        .into_recordbatch(
            &schema,
            static_schema_flag.as_ref(),
            time_partition.as_ref(),
            schema_version,
            "",
        )
        .map_err(|err| KafkaError::PostError(PostError::CustomError(err.to_string())))?;

    event::Event {
        rb,
        stream_name: stream_name.to_string(),
        origin_format: "json",
        origin_size: payload.len() as u64,
        is_first_event: is_first,
        parsed_timestamp: Utc::now().naive_utc(),
        time_partition: None,
        custom_partition_values: HashMap::new(),
        stream_type: StreamType::UserDefined,
    }
    .process()
    .await?;

    Ok(())
}

pub async fn setup_integration() {
    let (consumer, stream_names) = match setup_consumer() {
        Ok(c) => c,
        Err(err) => {
            match err {
                KafkaError::DoNotPrintError => {
                    debug!("P_KAFKA_TOPICS not set, skipping kafka integration");
                }
                _ => {
                    error!("{err}");
                }
            }
            return;
        }
    };

    info!("Setup kafka integration for {stream_names:?}");
    let mut stream = consumer.stream();

    while let Ok(curr) = stream.next().await.unwrap() {
        if let Err(err) = ingest_message(curr).await {
            error!("Unable to ingest incoming kafka message- {err}")
        }
    }
}
