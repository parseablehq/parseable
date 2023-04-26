use serde_json::Value;

use crate::storage::CURRENT_ENCRYPTION_ALGORITHM;

pub fn v1_v2(mut meta: Value) -> Value {
    let meta_obj = meta.as_object_mut().unwrap();
    let version = meta_obj.get_mut("version").unwrap();
    *version = Value::String("v2".to_string());

    meta_obj.insert(
        "encryption_algorithm".to_string(),
        Value::String(CURRENT_ENCRYPTION_ALGORITHM.to_string()),
    );

    meta
}
