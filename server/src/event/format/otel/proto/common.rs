use prost::alloc::string::String;
use prost::alloc::vec::Vec;

/// AnyValue is used to represent any type of attribute value. AnyValue may contain a
/// primitive value such as a string or integer or it may contain an arbitrary nested
/// object containing arrays, key-value lists and primitives.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, prost::Message, serde::Serialize, serde::Deserialize)]
pub struct AnyValue {
    /// The value is one of the listed fields. It is valid for all values to be unspecified
    /// in which case this AnyValue is considered to be "empty".
    #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub value: Option<any_value::Value>,
}

impl ToString for AnyValue {
    fn to_string(&self) -> String {
        self.value
            .as_ref()
            .map(|value| value.to_string())
            .unwrap_or("null".to_string())
    }
}

/// Nested message and enum types in `AnyValue`.
pub mod any_value {
    use prost::alloc::{string::String, vec::Vec};

    /// The value is one of the listed fields. It is valid for all values to be unspecified
    /// in which case this AnyValue is considered to be "empty".
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, prost::Oneof, serde::Serialize, serde::Deserialize)]
    pub enum Value {
        #[prost(string, tag = "1")]
        StringValue(String),
        #[prost(bool, tag = "2")]
        BoolValue(bool),
        #[prost(int64, tag = "3")]
        IntValue(i64),
        #[prost(double, tag = "4")]
        DoubleValue(f64),
        #[prost(message, tag = "5")]
        ArrayValue(super::ArrayValue),
        #[prost(message, tag = "6")]
        KvlistValue(super::KeyValueList),
        #[prost(bytes, tag = "7")]
        BytesValue(Vec<u8>),
    }

    impl ToString for Value {
        fn to_string(&self) -> String {
            match self {
                Value::StringValue(value) => value.to_owned(),
                Value::BoolValue(value) => value.to_string(),
                Value::IntValue(value) => value.to_string(),
                Value::DoubleValue(value) => value.to_string(),
                Value::ArrayValue(value) => value.to_string(),
                Value::KvlistValue(value) => value.to_string(),
                Value::BytesValue(value) => format!("{:?}", value),
            }
        }
    }
}

/// ArrayValue is a list of AnyValue messages. We need ArrayValue as a message
/// since oneof in AnyValue does not allow repeated fields.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, prost::Message, serde::Serialize, serde::Deserialize)]
pub struct ArrayValue {
    /// Array of values. The array may be empty (contain 0 elements).
    #[prost(message, repeated, tag = "1")]
    pub values: Vec<AnyValue>,
}

impl std::fmt::Display for ArrayValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for value in &self.values {
            write!(f, "{}", value.to_string())?;
        }
        write!(f, "]")?;

        Ok(())
    }
}

/// KeyValueList is a list of KeyValue messages. We need KeyValueList as a message
/// since `oneof` in AnyValue does not allow repeated fields. Everywhere else where we need
/// a list of KeyValue messages (e.g. in Span) we use `repeated KeyValue` directly to
/// avoid unnecessary extra wrapping (which slows down the protocol). The 2 approaches
/// are semantically equivalent.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, prost::Message, serde::Serialize, serde::Deserialize)]
pub struct KeyValueList {
    /// A collection of key/value pairs of key-value pairs. The list may be empty (may
    /// contain 0 elements).
    /// The keys MUST be unique (it is not allowed to have more than one
    /// value with the same key).
    #[prost(message, repeated, tag = "1")]
    pub values: Vec<KeyValue>,
}

impl std::fmt::Display for KeyValueList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        for value in &self.values {
            write!(f, "{}", value.to_string())?;
        }
        write!(f, "}}")?;

        Ok(())
    }
}

/// KeyValue is a key-value pair that is used to store Span attributes, Link
/// attributes, etc.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, prost::Message, serde::Serialize, serde::Deserialize)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

impl ToString for KeyValue {
    fn to_string(&self) -> String {
        format!(
            "{}={}",
            self.key,
            self.value
                .as_ref()
                .map(|x| x.to_string())
                .unwrap_or("null".to_string())
        )
    }
}

/// InstrumentationScope is a message representing the instrumentation scope information
/// such as the fully qualified name and version.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, prost::Message, serde::Serialize, serde::Deserialize)]
pub struct InstrumentationScope {
    /// An empty instrumentation scope name means the name is unknown.
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub version: String,
    /// Additional attributes that describe the scope. \[Optional\].
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag = "3")]
    pub attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "4")]
    pub dropped_attributes_count: u32,
}
