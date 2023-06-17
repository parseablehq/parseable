use prost::alloc::vec::Vec;

use super::common::KeyValue;

/// Resource information.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, prost::Message, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    /// Set of attributes that describe the resource.
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
    /// dropped_attributes_count is the number of dropped attributes. If the value is 0, then
    /// no attributes were dropped.
    #[prost(uint32, tag = "2")]
    pub dropped_attributes_count: u32,
}
