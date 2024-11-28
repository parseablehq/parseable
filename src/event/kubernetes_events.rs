/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    reporting_nstance: Option<String>,
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
    let body_str = std::str::from_utf8(body).unwrap();
    if let Ok(message) = serde_json::from_str::<KubernetesEvents>(body_str) {
        let mut vec_kubernetes_events_json: Vec<BTreeMap<String, Value>> = Vec::new();
        let key_items = "items";
        let key_involved_object = "involved_object";
        let key_metadata = "metadata";
        let key_source = "source";
        for record in message.items.iter() {
            let mut kubernetes_events_json: BTreeMap<String, Value> = BTreeMap::new();

            if let Some(api_version) = &message.api_version {
                kubernetes_events_json
                    .insert("api_version".to_owned(), Value::String(api_version.clone()));
            }

            if record.api_version.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_api_version", key_items),
                    Value::String(record.api_version.clone().unwrap()),
                );
            }

            if record.count.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_count", key_items),
                    Value::Number(serde_json::Number::from(record.count.unwrap())),
                );
            }

            if record.event_time.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_event_time", key_items),
                    Value::String(record.event_time.clone().unwrap()),
                );
            }

            if record.first_timestamp.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_first_timestamp", key_items),
                    Value::String(record.first_timestamp.clone().unwrap()),
                );
            }

            if record.involved_object.is_some() {
                if record
                    .involved_object
                    .as_ref()
                    .unwrap()
                    .api_version
                    .is_some()
                {
                    kubernetes_events_json.insert(
                        format!("{}_{}_api_version", key_items, key_involved_object),
                        Value::String(
                            record
                                .involved_object
                                .as_ref()
                                .unwrap()
                                .api_version
                                .clone()
                                .unwrap(),
                        ),
                    );
                }

                if record
                    .involved_object
                    .as_ref()
                    .unwrap()
                    .field_path
                    .is_some()
                {
                    kubernetes_events_json.insert(
                        format!("{}_{}_field_path", key_items, key_involved_object),
                        Value::String(
                            record
                                .involved_object
                                .as_ref()
                                .unwrap()
                                .field_path
                                .clone()
                                .unwrap(),
                        ),
                    );
                }

                if record.involved_object.as_ref().unwrap().kind.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_kind", key_items, key_involved_object),
                        Value::String(
                            record
                                .involved_object
                                .as_ref()
                                .unwrap()
                                .kind
                                .clone()
                                .unwrap(),
                        ),
                    );
                }

                if record.involved_object.as_ref().unwrap().name.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_name", key_items, key_involved_object),
                        Value::String(
                            record
                                .involved_object
                                .as_ref()
                                .unwrap()
                                .name
                                .clone()
                                .unwrap(),
                        ),
                    );
                }

                if record.involved_object.as_ref().unwrap().namespace.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_namespace", key_items, key_involved_object),
                        Value::String(
                            record
                                .involved_object
                                .as_ref()
                                .unwrap()
                                .namespace
                                .clone()
                                .unwrap(),
                        ),
                    );
                }

                if record
                    .involved_object
                    .as_ref()
                    .unwrap()
                    .resource_version
                    .is_some()
                {
                    kubernetes_events_json.insert(
                        format!("{}_{}_resource_version", key_items, key_involved_object),
                        Value::String(
                            record
                                .involved_object
                                .as_ref()
                                .unwrap()
                                .resource_version
                                .clone()
                                .unwrap(),
                        ),
                    );
                }

                if record.involved_object.as_ref().unwrap().uid.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_uid", key_items, key_involved_object),
                        Value::String(
                            record
                                .involved_object
                                .as_ref()
                                .unwrap()
                                .uid
                                .clone()
                                .unwrap(),
                        ),
                    );
                }
            }

            if record.kind.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_kind", key_items),
                    Value::String(record.kind.clone().unwrap()),
                );
            }

            if record.last_timestamp.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_last_timestamp", key_items),
                    Value::String(record.last_timestamp.clone().unwrap()),
                );
            }

            if record.message.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_message", key_items),
                    Value::String(record.message.clone().unwrap()),
                );
            }

            if record.metadata.is_some() {
                if record
                    .metadata
                    .as_ref()
                    .unwrap()
                    .creation_timestamp
                    .is_some()
                {
                    kubernetes_events_json.insert(
                        format!("{}_{}_creation_timestamp", key_items, key_metadata),
                        Value::String(
                            record
                                .metadata
                                .as_ref()
                                .unwrap()
                                .creation_timestamp
                                .clone()
                                .unwrap(),
                        ),
                    );
                }

                if record.metadata.as_ref().unwrap().name.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_name", key_items, key_metadata),
                        Value::String(record.metadata.as_ref().unwrap().name.clone().unwrap()),
                    );
                }

                if record.metadata.as_ref().unwrap().namespace.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_namespace", key_items, key_metadata),
                        Value::String(record.metadata.as_ref().unwrap().namespace.clone().unwrap()),
                    );
                }

                if record.metadata.as_ref().unwrap().resource_version.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_resource_version", key_items, key_metadata),
                        Value::String(
                            record
                                .metadata
                                .as_ref()
                                .unwrap()
                                .resource_version
                                .clone()
                                .unwrap(),
                        ),
                    );
                }

                if record.metadata.as_ref().unwrap().uid.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_uid", key_items, key_metadata),
                        Value::String(record.metadata.as_ref().unwrap().uid.clone().unwrap()),
                    );
                }
            }

            if record.reason.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_reason", key_items),
                    Value::String(record.reason.clone().unwrap()),
                );
            }

            if record.reporting_component.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_reporting_component", key_items),
                    Value::String(record.reporting_component.clone().unwrap()),
                );
            }

            if record.reporting_nstance.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_reporting_instance", key_items),
                    Value::String(record.reporting_nstance.clone().unwrap()),
                );
            }

            if record.source.is_some() {
                if record.source.as_ref().unwrap().component.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_component", key_items, key_source),
                        Value::String(record.source.as_ref().unwrap().component.clone().unwrap()),
                    );
                }

                if record.source.as_ref().unwrap().host.is_some() {
                    kubernetes_events_json.insert(
                        format!("{}_{}_host", key_items, key_source),
                        Value::String(record.source.as_ref().unwrap().host.clone().unwrap()),
                    );
                }
            }

            if record.type_.is_some() {
                kubernetes_events_json.insert(
                    format!("{}_type", key_items),
                    Value::String(record.type_.clone().unwrap()),
                );
            }

            if message.kind.is_some() {
                kubernetes_events_json.insert(
                    "kind".to_owned(),
                    Value::String(message.kind.clone().unwrap()),
                );
            }

            if message.metadata.is_some()
                && message
                    .metadata
                    .as_ref()
                    .unwrap()
                    .resource_version
                    .is_some()
            {
                kubernetes_events_json.insert(
                    format!("{}_resource_version", key_metadata),
                    Value::String(
                        message
                            .metadata
                            .as_ref()
                            .unwrap()
                            .resource_version
                            .clone()
                            .unwrap(),
                    ),
                );
            }

            vec_kubernetes_events_json.push(kubernetes_events_json);
        }
        Ok(vec_kubernetes_events_json
            .into_iter()
            .map(|btree_map| Value::Object(btree_map.into_iter().collect()))
            .collect())
    } else {
        Err(anyhow::anyhow!("Could not parse the kubernetes events log"))
    }
}
