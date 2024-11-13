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

use std::collections::{HashMap, HashSet};

use actix_web::http::header::{self, HeaderMap};
use bytes::Bytes;
use reqwest::{Client, RequestBuilder, Response};

use crate::handlers::http::{
    base_path,
    cluster::{get_querier_info_storage, sync_with_queriers, utils::check_liveness},
};

use super::{
    query_server::{QUERIER_META, QUERY_COORDINATION},
    QuerierMetadata, LEADER,
};

pub mod querier_cluster;
pub mod querier_dashboards;
pub mod querier_filters;
pub mod querier_hottier;
pub mod querier_ingest;
pub mod querier_leader;
pub mod querier_logstream;
pub mod querier_query;
pub mod querier_rbac;
pub mod querier_role;

pub struct LeaderRequest<'a> {
    pub body: Option<Bytes>,
    pub api: &'a str,
    pub resource: Option<&'a str>,
    pub method: Method,
}

impl LeaderRequest<'_> {
    pub async fn request(&self) -> anyhow::Result<Response> {
        let request_builder = self.get_request_builder().await?;

        let res = match request_builder.send().await {
            Ok(r) => r,
            Err(e) => {
                log::error!("error- {e:?}");
                // this error signifies inability to send request
                // check if leader is alive
                // if dead, select another leader and send request
                if QUERY_COORDINATION.lock().await.leader_liveness().await {
                    // leader is alive
                } else {
                    // leader is dead, this function call will automatically select a new one
                    QUERY_COORDINATION.lock().await.get_leader().await?;
                }

                let request_builder = self.get_request_builder().await?;
                request_builder.send().await?
            }
        };

        Ok(res)
    }

    async fn get_request_builder(&self) -> anyhow::Result<RequestBuilder> {
        let leader = QUERY_COORDINATION.lock().await.get_leader().await.unwrap();

        let token = leader.token;

        let domain = if let Some(domain) = leader.domain_name.strip_suffix("/") {
            domain.to_string()
        } else {
            leader.domain_name.to_string()
        };

        let target_url = if let Some(res) = self.resource {
            format!("{domain}{}/{}/{res}", base_path(), self.api)
        } else {
            format!("{domain}{}/{}", base_path(), self.api)
        };

        // make a client to forward the request
        let client = Client::new();

        let req_builder = match self.method {
            Method::Post => client.post(target_url),
            Method::Put => client.put(target_url),
            Method::Delete => client.delete(target_url),
        };

        let req_builder = match &self.body {
            Some(body) => req_builder.body(body.clone()),
            None => req_builder,
        };

        Ok(req_builder
            .header(header::AUTHORIZATION, token)
            .header(header::CONTENT_TYPE, "application/json"))
    }
}

pub enum Method {
    Post,
    Put,
    Delete,
}

/// Struct will be used when we do intelligent query routing
#[allow(unused)]
#[derive(Debug, Clone, Default)]
pub struct QueryNodeStats {
    pub ticket: String,
    pub start_time: u128,
    pub hottier_info: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryRouting {
    pub available_nodes: HashSet<String>,
    pub stats: HashMap<String, QueryNodeStats>,
    pub info: HashMap<String, QuerierMetadata>,
}

impl QueryRouting {
    /// this function will be called when a query node is made the leader
    /// for now it will start without any information about what the other nodes are doing
    pub async fn reset(&mut self) {
        let querier_metas = get_querier_info_storage().await.unwrap();
        let mut available_nodes = HashSet::new();
        let mut stats: HashMap<String, QueryNodeStats> = HashMap::new();
        let mut info: HashMap<String, QuerierMetadata> = HashMap::new();
        for qm in querier_metas {
            if qm.eq(&QUERIER_META) {
                // don't append self to the list
                // using self is an edge case
                continue;
            }

            if !check_liveness(&qm.domain_name).await {
                // only append if node is live
                continue;
            }

            available_nodes.insert(qm.querier_id.clone());

            stats.insert(
                qm.querier_id.clone(),
                QueryNodeStats {
                    start_time: qm.start_time,
                    ..Default::default()
                },
            );

            info.insert(qm.querier_id.clone(), qm);
        }
        self.available_nodes = available_nodes;
        self.info = info;
        self.stats = stats;
    }

    /// This function is supposed to look at all available query nodes
    /// in `available_nodes` and return one.
    /// If none is available, it will return one at random from `query_map`.
    /// if info is also empty, it will re-read metas from storage and try to recreate itself
    /// as a last resort, it will answer the query itself
    /// It can later be augmented to accept the stream name(s)
    /// to figure out which Query Nodes have those streams in their hottier
    pub async fn get_query_node(&mut self) -> QuerierMetadata {
        // get node from query coodinator
        if !self.available_nodes.is_empty() {
            let mut drain = self.available_nodes.drain();
            let node_id = drain.next().unwrap();
            self.available_nodes = HashSet::from_iter(drain);
            self.info.get(&node_id).unwrap().to_owned()
        } else if !self.info.is_empty() {
            self.info.values().next().unwrap().to_owned()
        } else {
            // no nodes available, send query request to self?
            // first check if any new query nodes are available
            self.reset().await;

            if !self.available_nodes.is_empty() {
                let mut drain = self.available_nodes.drain();
                let node_id = drain.next().unwrap();
                self.available_nodes = HashSet::from_iter(drain);
                self.info.get(&node_id).unwrap().to_owned()
            } else {
                QUERIER_META.clone()
            }
        }
    }

    pub fn reinstate_node(&mut self, node: QuerierMetadata) {
        // make this node available again
        self.available_nodes.insert(node.querier_id);
    }

    pub async fn check_liveness(&mut self) {
        let mut to_remove: Vec<String> = Vec::new();
        for (node_id, node) in self.info.iter() {
            if !check_liveness(&node.domain_name).await {
                to_remove.push(node_id.clone());
            }
        }

        for node_id in to_remove {
            self.info.remove(&node_id);
            self.available_nodes.remove(&node_id);
            self.stats.remove(&node_id);
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryCoordination {
    pub leader: Option<QuerierMetadata>,
}

impl QueryCoordination {
    /// selects a new leader from the given list of nodes
    /// if neither of the nodes in the list is alive, make self the leader
    async fn select_leader(&mut self, mut qmetas: Vec<QuerierMetadata>) {
        let mut leader_selected = false;

        qmetas.sort_by_key(|item| item.start_time);

        // iterate over querier_metas to see which node is alive
        // if alive, make leader and break
        for (i, meta) in qmetas.iter().enumerate() {
            if meta.eq(&QUERIER_META) {
                if i.eq(&0) {
                    // this is the OG leader
                    // as per our leader selection logic, this should be leader again
                    LEADER.lock().await.make_leader();
                    self.leader = Some(QUERIER_META.clone());
                    leader_selected = true;

                    // now remove the other leader
                    let _ =
                        sync_with_queriers(HeaderMap::new(), None, "leader", Method::Delete).await;
                    break;
                } else {
                    continue;
                }
            }

            if check_liveness(&meta.domain_name).await {
                self.make_leader(meta.clone()).await;
                leader_selected = true;
                break;
            }
        }

        if !leader_selected {
            // this means the current node is the leader!
            LEADER.lock().await.make_leader();
            self.leader = Some(QUERIER_META.clone());
        }
    }

    async fn make_leader(&mut self, leader: QuerierMetadata) {
        self.leader = Some(leader.clone());

        // send request to this leader letting it know of its promotion
        let client = Client::new();
        let token = leader.token;

        let domain = if let Some(domain) = leader.domain_name.strip_suffix("/") {
            domain.to_string()
        } else {
            leader.domain_name.to_string()
        };

        let target_url = format!("{domain}{}/leader", base_path());
        client
            .put(target_url)
            .header(header::AUTHORIZATION, token)
            .header(header::CONTENT_TYPE, "application/json")
            .send()
            .await
            .unwrap();
    }

    /// checks if the leader is alive
    /// if not, then makes self.leader = None
    pub async fn leader_liveness(&mut self) -> bool {
        if let Some(leader) = &self.leader {
            if check_liveness(&leader.domain_name).await {
                true
            } else {
                self.leader = None;
                false
            }
        } else {
            false
        }
    }

    /// gets the leader if present
    /// otherwise selects a new leader
    pub async fn get_leader(&mut self) -> anyhow::Result<QuerierMetadata> {
        if self.leader.is_some() {
            Ok(self.leader.clone().unwrap())
        } else {
            self.update().await;
            match self.leader.clone() {
                Some(l) => Ok(l),
                None => Err(anyhow::Error::msg("Please start a Query server.")),
            }
        }
    }

    pub async fn reset_leader(&mut self) -> anyhow::Result<()> {
        self.leader = None;
        self.get_leader().await?;
        Ok(())
    }

    /// reads node metadata from storage
    /// and selects a leader if not present
    async fn update(&mut self) {
        let qmetas = get_querier_info_storage().await.unwrap();
        if !qmetas.is_empty() && self.leader.is_none() {
            self.select_leader(qmetas.clone()).await;
        }
    }
}
