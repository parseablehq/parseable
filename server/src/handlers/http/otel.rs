use super::proto::{
    common::v1::{any_value::Value as PBValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, SeverityNumber},
    resource::v1::Resource,
};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
use serde_json::Value;
use bytes::Bytes;
use serde::{Deserialize, Serialize, Serializer};
use chrono::{DateTime, TimeZone, Utc};
use crate::handlers::http::proto::collector::logs::v1::ExportLogsServiceRequest;

impl ResourceLogs {
    pub fn into_event_iter(self){
        let resource = self.resource;

        self.scope_logs
            .into_iter()
            .flat_map(|scope_log| scope_log.log_records)
            .map(move |log_record| {
                ResourceLog {
                    resource: resource.clone(),
                    log_record,
                }
                .into_event()
            })
    }
}

impl From<PBValue> for Value {
    fn from(av: PBValue) -> Self {
        match av {
            PBValue::StringValue(v) => Value::String(String::from(v)),
            PBValue::BoolValue(v) => Value::Bool(v),
            PBValue::IntValue(v) => Value::Number(v.into()),
            PBValue::DoubleValue(v) => Value::String(v.to_string()),
            PBValue::BytesValue(v) => Value::String(String::from_utf8(v).unwrap()),
            PBValue::ArrayValue(arr) => Value::Array(
                arr.values
                    .into_iter()
                    .map(|av| av.value.map(Into::into).unwrap_or(Value::Null))
                    .collect::<Vec<Value>>(),
            ),
            PBValue::KvlistValue(arr) => kv_list_into_value(arr.values),
        }
    }
}

pub fn flatten_otel_logs(body: Bytes) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let request = ExportLogsServiceRequest::decode(body).map_err(|error| {
        format!("Could not decode request: {}", error)
        });
        println!("request: {:?}", request);

        let events: Vec<Event> = request
        .resource_logs
        .into_iter()
        .flat_map(|v| v.into_event_iter(log_namespace))
        .collect();
    vec_otel_json
}

struct ResourceLog {
    resource: Option<Resource>,
    log_record: LogRecord,
}

fn kv_list_into_value(arr: Vec<KeyValue>) -> Value {
    Value::Object(
        arr.into_iter()
            .filter_map(|kv| {
                kv.value.map(|av| {
                    (
                        kv.key.into(),
                        av.value.map(Into::into).unwrap_or(Value::Null),
                    )
                })
            })
            .collect(),
    )
}

impl ResourceLog {
    fn into_event(self) {
        let mut log = if let Some(v) = self.log_record.body.and_then(|av| av.value) {
            LogEvent::from(<PBValue as Into<Value>>::into(v))
        } else {
            LogEvent::from(Value::Null)
        };

        // Optional fields
        if let Some(resource) = self.resource {
            if !resource.attributes.is_empty() {
                println!("resource.attributes: {:?}", resource.attributes);
                
            }
        }
        if !self.log_record.attributes.is_empty() {
            println!("self.log_record.attributes: {:?}", self.log_record.attributes);
        }
        if !self.log_record.trace_id.is_empty() {
            println!("self.log_record.trace_id: {:?}", self.log_record.trace_id);
        }
        if !self.log_record.span_id.is_empty() {
            println!("self.log_record.span_id: {:?}", self.log_record.span_id);
        }
        if !self.log_record.severity_text.is_empty() {
            println!("self.log_record.severity_text: {:?}", self.log_record.severity_text);
        }
        if self.log_record.severity_number != SeverityNumber::Unspecified as i32 {
            println!("self.log_record.severity_number: {:?}", self.log_record.severity_number);
        }
        if self.log_record.flags > 0 {
            println!("self.log_record.flags: {:?}", self.log_record.flags);
        }

        println!("self.log_record_drop_count: {:?}", self.log_record.dropped_attributes_count);
        

        // According to log data model spec, if observed_time_unix_nano is missing, the collector
        // should set it to the current time.
        // let observed_timestamp = if self.log_record.observed_time_unix_nano > 0 {
        //     Utc.timestamp_nanos(self.log_record.observed_time_unix_nano as i64)
        //         .into()
        // } else {
        //     Value::String(Utc::now().to_string())
        // };
        println!("self.log_record.observed_time_unix_nano: {:?}", self.log_record.observed_time_unix_nano);

        // If time_unix_nano is not present (0 represents missing or unknown timestamp) use observed time
        // let timestamp = if self.log_record.time_unix_nano > 0 {
        //     Utc.timestamp_nanos(self.log_record.time_unix_nano as i64)
        //         .into()
        // } else {
        //     observed_timestamp
        // };
        // log_namespace.insert_source_metadata(
        //     SOURCE_NAME,
        //     &mut log,
        //     log_schema().timestamp_key().map(LegacyKey::Overwrite),
        //     path!("timestamp"),
        //     timestamp,
        // );

        // log_namespace.insert_vector_metadata(
        //     &mut log,
        //     log_schema().source_type_key(),
        //     path!("source_type"),
        //     Bytes::from_static(SOURCE_NAME.as_bytes()),
        // );
        // if log_namespace == LogNamespace::Vector {
        //     log.metadata_mut()
        //         .value_mut()
        //         .insert(path!("vector", "ingest_timestamp"), now);
        // }

    }
}

#[derive(Debug, Deserialize)]
pub struct LogEvent {
    #[serde(flatten)]
    inner: Arc<Inner>
}

impl From<Value> for LogEvent {
    fn from(value: Value) -> Self {
        Self::from_parts(value)
    }
}
impl LogEvent {

    pub fn from_parts(value: Value) -> Self {
        Self {
            inner: Arc::new(value.into()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Inner {
    #[serde(flatten)]
    fields: Value
}

impl Inner {
    fn as_value(&self) -> &Value {
        &self.fields
    }
}

impl From<Value> for Inner {
    fn from(fields: Value) -> Self {
        Self {
            fields
        }
    }
}