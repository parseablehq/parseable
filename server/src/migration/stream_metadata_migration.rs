use serde_json::{json, Value};

pub fn v1_v2(mut stream_metadata: Value) -> Value {
    let default_stats = json!({
        "events": 0,
        "ingestion": 0,
        "storage": 0
    });
    let stream_metadata_map = stream_metadata.as_object_mut().unwrap();
    stream_metadata_map.entry("stats").or_insert(default_stats);
    stream_metadata_map.insert("version".to_owned(), Value::String("v2".into()));
    stream_metadata_map.insert("objectstore-format".to_owned(), Value::String("v2".into()));
    stream_metadata
}
