/// AnyValue is used to represent any type of attribute value. AnyValue may contain a
/// primitive value such as a string or integer or it may contain an arbitrary nested
/// object containing arrays, key-value lists and primitives.
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
pub struct AnyValue {
    /// The value is one of the listed fields. It is valid for all values to be unspecified
    /// in which case this AnyValue is considered to be "empty".
    pub value: Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Value {
    #[serde(rename = "stringValue")]
    pub str_val: Option<String>,
    #[serde(rename = "boolValue")]
    pub bool_val: Option<String>,
    #[serde(rename = "intValue")]
    pub int_val: Option<String>,
    #[serde(rename = "doubleValue")]
    pub double_val: Option<String>,
    #[serde(rename = "arrayValue")]
    pub array_val: Option<ArrayValue>,
    #[serde(rename = "keyVauleList")]
    pub kv_list_val: Option<KeyValueList>,
    #[serde(rename = "bytesValue")]
    pub bytes_val: Option<String>,
}

/// ArrayValue is a list of AnyValue messages. We need ArrayValue as a message
/// since oneof in AnyValue does not allow repeated fields.
#[derive(Serialize, Deserialize, Debug)]
pub struct ArrayValue {
    /// Array of values. The array may be empty (contain 0 elements).
    pub values: Vec<AnyValue>,
}
/// KeyValueList is a list of KeyValue messages. We need KeyValueList as a message
/// since `oneof` in AnyValue does not allow repeated fields. Everywhere else where we need
/// a list of KeyValue messages (e.g. in Span) we use `repeated KeyValue` directly to
/// avoid unnecessary extra wrapping (which slows down the protocol). The 2 approaches
/// are semantically equivalent.
#[derive(Serialize, Deserialize, Debug)]
pub struct KeyValueList {
    /// A collection of key/value pairs of key-value pairs. The list may be empty (may
    /// contain 0 elements).
    /// The keys MUST be unique (it is not allowed to have more than one
    /// value with the same key).
    pub values: Vec<KeyValue>,
}
/// KeyValue is a key-value pair that is used to store Span attributes, Link
/// attributes, etc.
#[derive(Serialize, Deserialize, Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: Option<Value>,
}
/// InstrumentationScope is a message representing the instrumentation scope information
/// such as the fully qualified name and version.
#[derive(Serialize, Deserialize, Debug)]
pub struct InstrumentationScope {
    /// An empty instrumentation scope name means the name is unknown.
    pub name: Option<String>,
    pub version: Option<String>,
    /// Additional attributes that describe the scope. \[Optional\].
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    pub attributes: Option<Vec<KeyValue>>,
    #[serde(rename = "droppedAttributesCount")]
    pub dropped_attributes_count: Option<u32>,
}
