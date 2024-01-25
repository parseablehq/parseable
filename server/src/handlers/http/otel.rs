use serde_json::Value;
use bytes::Bytes;
mod log;
mod common;
mod resource;
use std::collections::BTreeMap;



pub fn flatten_otel_logs(body: Bytes) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let body_str = std::str::from_utf8(&body).unwrap();
    println!("body_str: {:?}", body_str);

    let logsData: log::LogsData = serde_json::from_str(body_str).unwrap();
    println!("logsData: {:?}", logsData.resource_logs);
        // let events: Vec<Event> = request
        // .resource_logs
        // .into_iter()
        // .flat_map(|v| v.into_event_iter(log_namespace))
        // .collect();
    vec_otel_json
}