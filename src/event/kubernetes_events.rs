use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubernetesEvents {
    api_version: Option<String>,
    items: Vec<Items>,
    kind: Option<String>,
    metadata: Option<Metadata>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Items {
    api_version: Option<String>,
    count: Option<i64>,
    event_time: Option<String>,
    first_timestamp: Option<String>,
    involved_object: Option<InvolvedObject>,
    kind: Option<String>,
    last_timestamp: Option<String>,
    message: Option<String>,
    metadata: Option<Metadata>,
    reason: Option<String>,
    reporting_component: Option<String>,
    reporting_instance: Option<String>,
    source: Option<Source>,
    type_: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvolvedObject {
    api_version: Option<String>,
    field_path: Option<String>,
    kind: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
    resource_version: Option<String>,
    uid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    creation_timestamp: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
    resource_version: Option<String>,
    uid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    component: Option<String>,
    host: Option<String>,
}

pub async fn flatten_kubernetes_events_log(body: &Bytes) -> Result<Vec<Value>, anyhow::Error> {
    let body_str = std::str::from_utf8(body)?;
    let message: KubernetesEvents = serde_json::from_str(body_str)?;

    let mut vec_kubernetes_events_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let key_items = "items";
    let key_involved_object = "involved_object";
    let key_metadata = "metadata";
    let key_source = "source";

    for record in message.items.iter() {
        let mut kubernetes_events_json: BTreeMap<String, Value> = BTreeMap::new();

        insert_if_some(
            &mut kubernetes_events_json,
            "api_version",
            &message.api_version,
        );
        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_api_version", key_items),
            &record.api_version,
        );
        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_count", key_items),
            &record.count,
        );
        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_event_time", key_items),
            &record.event_time,
        );
        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_first_timestamp", key_items),
            &record.first_timestamp,
        );

        if let Some(involved_object) = &record.involved_object {
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_api_version", key_items, key_involved_object),
                &involved_object.api_version,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_field_path", key_items, key_involved_object),
                &involved_object.field_path,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_kind", key_items, key_involved_object),
                &involved_object.kind,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_name", key_items, key_involved_object),
                &involved_object.name,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_namespace", key_items, key_involved_object),
                &involved_object.namespace,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_resource_version", key_items, key_involved_object),
                &involved_object.resource_version,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_uid", key_items, key_involved_object),
                &involved_object.uid,
            );
        }

        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_kind", key_items),
            &record.kind,
        );
        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_last_timestamp", key_items),
            &record.last_timestamp,
        );
        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_message", key_items),
            &record.message,
        );

        if let Some(metadata) = &record.metadata {
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_creation_timestamp", key_items, key_metadata),
                &metadata.creation_timestamp,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_name", key_items, key_metadata),
                &metadata.name,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_namespace", key_items, key_metadata),
                &metadata.namespace,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_resource_version", key_items, key_metadata),
                &metadata.resource_version,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_uid", key_items, key_metadata),
                &metadata.uid,
            );
        }

        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_reason", key_items),
            &record.reason,
        );
        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_reporting_component", key_items),
            &record.reporting_component,
        );
        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_reporting_instance", key_items),
            &record.reporting_instance,
        );

        if let Some(source) = &record.source {
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_component", key_items, key_source),
                &source.component,
            );
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_{}_host", key_items, key_source),
                &source.host,
            );
        }

        insert_if_some(
            &mut kubernetes_events_json,
            &format!("{}_type", key_items),
            &record.type_,
        );

        insert_if_some(&mut kubernetes_events_json, "kind", &message.kind);

        if let Some(metadata) = &message.metadata {
            insert_if_some(
                &mut kubernetes_events_json,
                &format!("{}_resource_version", key_metadata),
                &metadata.resource_version,
            );
        }

        vec_kubernetes_events_json.push(kubernetes_events_json);
    }

    Ok(vec_kubernetes_events_json
        .into_iter()
        .map(|btree_map| Value::Object(btree_map.into_iter().collect()))
        .collect())
}

fn insert_if_some<T: ToString>(map: &mut BTreeMap<String, Value>, key: &str, option: &Option<T>) {
    if let Some(value) = option {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
}
