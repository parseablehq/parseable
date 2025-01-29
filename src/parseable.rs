use std::{collections::HashMap, sync::Arc};

use arrow_schema::{Field, Schema};
use once_cell::sync::Lazy;

use crate::{
    handlers::http::logstream::error::StreamError,
    metadata::{StreamInfo, STREAM_INFO},
    option::{Config, CONFIG},
    storage::{LogStream, ObjectStoreFormat, StreamType},
};

pub static PARSEABLE: Lazy<Parseable> = Lazy::new(|| Parseable {
    config: &CONFIG,
    streams: &STREAM_INFO,
});

pub struct Parseable {
    pub config: &'static Config,
    pub streams: &'static StreamInfo,
}

impl Parseable {
    /// list all streams from storage
    /// if stream exists in storage, create stream and schema from storage
    /// and add it to the memory map
    pub async fn create_stream_and_schema_from_storage(
        &self,
        stream_name: &str,
    ) -> Result<bool, StreamError> {
        // Proceed to create log stream if it doesn't exist
        let storage = self.config.storage().get_object_store();
        let streams = storage.list_streams().await?;
        if !streams.contains(&LogStream {
            name: stream_name.to_owned(),
        }) {
            return Ok(false);
        }

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
        self.streams.add_stream(
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

        Ok(true)
    }
}
