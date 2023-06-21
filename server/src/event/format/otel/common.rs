use itertools::Itertools;

use super::proto::{common, resource};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    pub attributes: Vec<KeyValue>,
}

impl From<resource::Resource> for Resource {
    fn from(value: resource::Resource) -> Self {
        Resource {
            attributes: value.attributes.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Scope {
    pub name: String,
    pub version: String,
    pub attributes: Vec<KeyValue>,
}

impl From<common::InstrumentationScope> for Scope {
    fn from(value: common::InstrumentationScope) -> Self {
        let common::InstrumentationScope {
            name,
            version,
            attributes,
            ..
        } = value;
        Scope {
            name,
            version,
            attributes: attributes.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyValue {
    key: String,
    value: String,
}

impl From<common::KeyValue> for KeyValue {
    fn from(value: common::KeyValue) -> Self {
        KeyValue {
            key: value.key,
            value: value.value.map(|x| x.to_string()).unwrap_or_default(),
        }
    }
}

impl ToString for KeyValue {
    fn to_string(&self) -> String {
        format!("{}={}", self.key, self.value)
    }
}

impl From<common::KeyValueList> for Vec<KeyValue> {
    fn from(value: common::KeyValueList) -> Self {
        value.values.into_iter().map(Into::into).collect()
    }
}

pub fn attributes_to_string(values: &[KeyValue]) -> String {
    values.iter().map(|x| x.to_string()).join("^")
}

pub fn id_to_string(trace_id: &[u8]) -> String {
    String::from_utf8_lossy(&trace_id).to_string()
}
