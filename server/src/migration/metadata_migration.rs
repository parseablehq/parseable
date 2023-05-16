use std::vec;

use serde_json::Value;

pub fn v1_v2(mut storage_metadata: serde_json::Value) -> Value {
    let metadata = storage_metadata.as_object_mut().unwrap();
    *metadata.get_mut("version").unwrap() = Value::String("v2".to_string());
    metadata.remove("user");
    metadata.remove("stream");
    metadata.insert("users".to_string(), Value::Array(vec![]));
    metadata.insert("streams".to_string(), Value::Array(vec![]));
    storage_metadata
}
