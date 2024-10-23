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

use std::fs;

use actix_web::{http::header::HeaderMap, HttpRequest, Responder};
use bytes::Bytes;
use http::{header, StatusCode};
use itertools::Itertools;
use reqwest::{Client, RequestBuilder, Response};

use crate::{event, handlers::http::{base_path, base_path_without_preceding_slash, cluster::{self, get_ingestor_info_storage, get_querier_info_storage, utils::check_liveness}, logstream::error::StreamError, modal::coordinator_server::QUERY_COORDINATION}, hottier::HotTierManager, metadata, option::CONFIG, stats, storage::StorageDir};

use super::{IngestorMetadata, QuerierMetadata};

pub mod coordinator_query;
// pub mod coordinator_register;
pub mod coordinator_logstream;
pub mod coordinator_dashboards;
pub mod coordinator_filters;
pub mod coordinator_llm;
pub mod coordinator_metrics;
pub mod coordinator_rbac;
pub mod coordinator_role;
pub mod coordinator_cluster;

pub enum Method {
    GET,
    POST,
    PUT,
    DELETE
}

pub struct CoordinatorRequest<'a> {
    pub body: Option<Bytes>,
    pub api: &'a str,
    pub resource: Option<&'a str>,
    pub method: Method
}

impl CoordinatorRequest<'_> {

    pub async fn request(&self) -> anyhow::Result<Response> {
        let request_builder = self.get_request_builder().await?;

        log::warn!("request_builder- {request_builder:?}");
        let res = match request_builder.send().await {
                Ok(r) => {
                    log::warn!("response- {r:?}");
                    r
                },
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
        let leader = QUERY_COORDINATION.lock()
            .await
            .get_leader()
            .await
            .unwrap();
        
        let token = leader.token;
        
        let domain = if let Some(domain) = leader.domain_name.strip_suffix("/") {
            domain.to_string()
        } else {
            leader.domain_name.to_string()
        };

        let target_url = if let Some(res) = self.resource {
            format!("{domain}{}/{}/{res}",base_path(),self.api)
        } else {
            format!("{domain}{}/{}",base_path(),self.api)
        };

        // make a client to forward the request
        let client = Client::new();

        let req_builder = match self.method {
            Method::GET => {
                client.get(target_url)
            },
            Method::POST => {
                client.post(target_url)
            },
            Method::PUT => {
                client.put(target_url)
            },
            Method::DELETE => {
                client.delete(target_url)
            }
        };

        let req_builder = match &self.body {
            Some(body) => {
                req_builder.body(body.clone())
            },
            None => req_builder,
        };

        Ok(req_builder
            .header(header::AUTHORIZATION, token)
            .header(header::CONTENT_TYPE, "application/json")
        )
    }
}