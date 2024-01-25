use crate::handlers::http::proto::common::v1::KeyValue;
/// Resource information.
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
pub struct Resource {
    /// Set of attributes that describe the resource.
    /// Attribute keys MUST be unique (it is not allowed to have more than one
    /// attribute with the same key).
    #[serde(rename = "attributes")]
    pub attributes: Option<Vec<KeyValue>>,
    /// dropped_attributes_count is the number of dropped attributes. If the value is 0, then
    /// no attributes were dropped.

    #[serde(rename = "droppedAttributesCount")]
    pub dropped_attributes_count: Option<u32>,
}
