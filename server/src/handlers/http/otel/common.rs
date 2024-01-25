/// AnyValue is used to represent any type of attribute value. AnyValue may contain a
/// primitive value such as a string or integer or it may contain an arbitrary nested
/// object containing arrays, key-value lists and primitives.
use serde::{Serialize, Deserialize};
#[derive(Serialize, Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyValue {
    /// The value is one of the listed fields. It is valid for all values to be unspecified
    /// in which case this AnyValue is considered to be "empty".
    #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub value: ::core::option::Option<any_value::Value>,
}
/// Nested message and enum types in `AnyValue`.
pub mod any_value {
    /// The value is one of the listed fields. It is valid for all values to be unspecified
    /// in which case this AnyValue is considered to be "empty".
    use serde::{Serialize, Deserialize};
    #[derive(Serialize, Deserialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        #[serde(rename = "stringValue")]
        StringValue(::prost::alloc::string::String),
        #[prost(bool, tag = "2")]
        #[serde(rename = "boolValue")]
        BoolValue(bool),
        #[prost(int64, tag = "3")]
        #[serde(rename = "stringValue")]
        IntValue(i64),
        #[prost(double, tag = "4")]
        #[serde(rename = "doubleValue")]
        DoubleValue(f64),
        #[prost(message, tag = "5")]
        #[serde(rename = "arrayValue")]
        ArrayValue(super::ArrayValue),
        #[prost(message, tag = "6")]
        #[serde(rename = "keyVauleList")]
        KvlistValue(super::KeyValueList),
        #[prost(bytes, tag = "7")]
        #[serde(rename = "bytesValue")]
        BytesValue(::prost::alloc::vec::Vec<u8>),
    }
}
/// ArrayValue is a list of AnyValue messages. We need ArrayValue as a message
/// since oneof in AnyValue does not allow repeated fields.
#[derive(Serialize, Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrayValue {
    /// Array of values. The array may be empty (contain 0 elements).
    #[prost(message, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<AnyValue>,
}
/// KeyValueList is a list of KeyValue messages. We need KeyValueList as a message
/// since `oneof` in AnyValue does not allow repeated fields. Everywhere else where we need
/// a list of KeyValue messages (e.g. in Span) we use `repeated KeyValue` directly to
/// avoid unnecessary extra wrapping (which slows down the protocol). The 2 approaches
/// are semantically equivalent.
#[derive(Serialize, Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValueList {
    /// A collection of key/value pairs of key-value pairs. The list may be empty (may
    /// contain 0 elements).
    /// The keys MUST be unique (it is not allowed to have more than one
    /// value with the same key).
    #[prost(message, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<KeyValue>,
}
/// KeyValue is a key-value pair that is used to store Span attributes, Link
/// attributes, etc.
#[derive(Serialize, Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<AnyValue>,
}
/// InstrumentationScope is a message representing the instrumentation scope information
/// such as the fully qualified name and version.
#[derive(Serialize, Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstrumentationScope {
    /// An empty instrumentation scope name means the name is unknown.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub version: ::prost::alloc::string::String,
    /// Additional attributes that describe the scope. \[Optional\].
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag = "3")]
    pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(uint32, tag = "4")]
    #[serde(rename = "droppedAttributesCount")]
    pub dropped_attributes_count: u32,
}
