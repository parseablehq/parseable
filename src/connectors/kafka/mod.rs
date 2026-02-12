/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use crate::connectors::kafka::config::KafkaConfig;
use derive_more::Constructor;
use rdkafka::client::OAuthToken;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::producer::ProducerContext;
use rdkafka::topic_partition_list::TopicPartitionListElem;
use rdkafka::{ClientContext, Message, Offset, Statistics};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

pub mod config;
pub mod consumer;
pub mod metrics;
mod partition_stream;
pub mod processor;
pub mod rebalance_listener;
pub mod sink;
pub mod state;
#[allow(dead_code)]
type BaseConsumer = rdkafka::consumer::BaseConsumer<KafkaContext>;
#[allow(dead_code)]
type FutureProducer = rdkafka::producer::FutureProducer<KafkaContext>;
type StreamConsumer = rdkafka::consumer::StreamConsumer<KafkaContext>;

#[derive(Clone, Debug)]
pub struct KafkaContext {
    config: Arc<KafkaConfig>,
    statistics: Arc<RwLock<Statistics>>,
    rebalance_tx: mpsc::Sender<RebalanceEvent>,
}

impl KafkaContext {
    pub fn new(config: Arc<KafkaConfig>) -> (Self, Receiver<RebalanceEvent>) {
        let (rebalance_tx, rebalance_rx) = mpsc::channel(10);
        let statistics = Arc::new(RwLock::new(Statistics::default()));
        (
            Self {
                config,
                statistics,
                rebalance_tx,
            },
            rebalance_rx,
        )
    }

    pub fn notify(&self, rebalance_event: RebalanceEvent) {
        let rebalance_sender = self.rebalance_tx.clone();
        std::thread::spawn(move || {
            info!("Sending RebalanceEvent to listener...");
            if let Err(e) = rebalance_sender.blocking_send(rebalance_event) {
                warn!("Rebalance event receiver is closed! {:?}", e);
            } else {
                info!("RebalanceEvent sent successfully!");
            }
        });
    }

    pub fn config(&self) -> Arc<KafkaConfig> {
        Arc::clone(&self.config)
    }
}

#[derive(Debug, Clone)]
pub enum RebalanceEvent {
    Assign(TopicPartitionList),
    Revoke(TopicPartitionList, std::sync::mpsc::Sender<()>),
}

impl RebalanceEvent {
    pub fn get_assignment(&self) -> &TopicPartitionList {
        match self {
            RebalanceEvent::Assign(tpl) => tpl,
            RebalanceEvent::Revoke(tpl, _) => tpl,
        }
    }
}

#[derive(Constructor, Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn from_kafka_msg(msg: &BorrowedMessage) -> Self {
        Self::new(msg.topic().to_owned(), msg.partition())
    }

    pub fn from_tp_elem(elem: &TopicPartitionListElem<'_>) -> Self {
        Self::new(elem.topic().to_owned(), elem.partition())
    }
}

#[derive(Constructor, Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartitionList {
    pub tpl: Vec<TopicPartition>,
}

impl TopicPartitionList {
    pub fn from_rdkafka_tpl(tpl: &rdkafka::topic_partition_list::TopicPartitionList) -> Self {
        let elements = tpl.elements();
        let mut tp_vec = Vec::with_capacity(elements.len());
        for ref element in elements {
            let tp = TopicPartition::from_tp_elem(element);
            tp_vec.push(tp);
        }
        Self::new(tp_vec)
    }

    pub fn is_empty(&self) -> bool {
        self.tpl.is_empty()
    }
}

#[derive(Constructor, Debug, Hash, Eq, PartialEq)]
pub struct ConsumerRecord {
    pub payload: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub tenant_id: Option<String>, // pub headers: Option<HashMap<String, Option<String>>>,
}

impl ConsumerRecord {
    pub fn from_borrowed_msg(msg: BorrowedMessage) -> Self {
        let tenant_id = if let Some(headers) = extract_headers(&msg)
            && let Some(tenant_id) = headers.get("tenant")
        {
            tenant_id.clone()
        } else {
            None
        };

        Self {
            key: msg.key().map(|k| k.to_vec()),
            payload: msg.payload().map(|p| p.to_vec()),
            topic: msg.topic().to_owned(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp: msg.timestamp().to_millis(),
            tenant_id, // headers: extract_headers(&msg),
        }
    }

    pub fn key_str(&self) -> String {
        self.key.clone().map_or_else(
            || String::from("null"),
            |k| String::from_utf8_lossy(k.as_ref()).to_string(),
        )
    }

    pub fn offset_to_commit(&self) -> KafkaResult<rdkafka::TopicPartitionList> {
        let mut offset_to_commit = rdkafka::TopicPartitionList::new();
        offset_to_commit.add_partition_offset(
            &self.topic,
            self.partition,
            Offset::Offset(self.offset + 1),
        )?;
        Ok(offset_to_commit)
    }
}

#[allow(unused)]
fn extract_headers(msg: &BorrowedMessage<'_>) -> Option<HashMap<String, Option<String>>> {
    msg.headers().map(|headers| {
        headers
            .iter()
            .map(|header| {
                (
                    header.key.to_string(),
                    header.value.map(|v| String::from_utf8_lossy(v).to_string()),
                )
            })
            .collect()
    })
}

impl ConsumerContext for KafkaContext {
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &Rebalance<'_>,
    ) {
        info!("Running pre-rebalance with {:?}", rebalance);
        match rebalance {
            Rebalance::Revoke(tpl) => {
                let (pq_waiter_tx, pq_waiter_rx) = std::sync::mpsc::channel();

                let tpl = TopicPartitionList::from_rdkafka_tpl(tpl);
                self.notify(RebalanceEvent::Revoke(tpl, pq_waiter_tx));

                if pq_waiter_rx.recv().is_err() {
                    warn!("Queue termination sender dropped");
                }
                info!("Rebalance Revoke started");
            }
            Rebalance::Assign(tpl) => {
                let tpl = TopicPartitionList::from_rdkafka_tpl(tpl);
                self.notify(RebalanceEvent::Assign(tpl));
            }

            Rebalance::Error(err) => error!("Error occurred during rebalance {:?}", err),
        };
    }

    fn post_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &Rebalance<'_>,
    ) {
        info!("Running post-rebalance with {:?}", rebalance);
    }
}

impl ProducerContext for KafkaContext {
    type DeliveryOpaque = ();
    fn delivery(
        &self,
        _delivery_result: &rdkafka::message::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
    }
}

impl ClientContext for KafkaContext {
    // TODO: when implementing OAuth, set this to true
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = false;

    fn stats(&self, new_stats: Statistics) {
        match self.statistics.write() {
            Ok(mut stats) => {
                *stats = new_stats;
            }
            Err(e) => {
                error!("Cannot write to kafka statistics from RwLock. Error: {}", e)
            }
        };
    }

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        // TODO Implement OAuth token generation when needed
        Err("OAuth token generation is not implemented".into())
    }
}
