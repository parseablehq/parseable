use super::proto::{common, resource};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    pub attributes: Vec<KeyValue>,
    dropped_attributes_count: u32,
}

impl From<resource::Resource> for Resource {
    fn from(value: resource::Resource) -> Self {
        let resource::Resource {
            attributes,
            dropped_attributes_count,
        } = value;
        Resource {
            attributes: attributes.into_iter().map(Into::into).collect(),
            dropped_attributes_count,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Scope {
    pub name: String,
    pub version: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

impl From<common::InstrumentationScope> for Scope {
    fn from(value: common::InstrumentationScope) -> Self {
        let common::InstrumentationScope {
            name,
            version,
            attributes,
            dropped_attributes_count,
        } = value;
        Scope {
            name,
            version,
            attributes: attributes.into_iter().map(Into::into).collect(),
            dropped_attributes_count,
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

impl From<common::KeyValueList> for Vec<KeyValue> {
    fn from(value: common::KeyValueList) -> Self {
        value.values.into_iter().map(Into::into).collect()
    }
}
