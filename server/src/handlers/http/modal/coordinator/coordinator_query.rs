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

use std::{hash::{DefaultHasher, Hash, Hasher}, time::Instant};

use actix_web::{web, HttpRequest, HttpResponse, Responder};
use arrow_array::RecordBatch;
use arrow_flight::{FlightClient, FlightDescriptor, Ticket};
use bytes::Bytes;
use datafusion::common::tree_node::TreeNode;
use futures::TryStreamExt;
use http::{header, StatusCode};
use itertools::Itertools;
use reqwest::Client;
use serde_json::{json, Value};
use tonic::{transport::{Channel, Uri}, Status};

use crate::{handlers::{http::{base_path, cluster::get_querier_info_storage, modal::{coordinator_server::QUERY_COORDINATION, QuerierMetadata}, query::{authorize_and_set_filter_tags, get_results_from_cache, into_query, put_results_in_cache, update_schema_when_distributed, Query, QueryError}}, CACHE_RESULTS_HEADER_KEY, CACHE_VIEW_HEADER_KEY, USER_ID_HEADER_KEY}, metrics::QUERY_EXECUTE_TIME, option::CONFIG, query::{TableScanVisitor, QUERY_SESSION}, querycache::QueryCacheManager, rbac::Users, response::QueryResponse, utils::actix::extract_session_key_from_req};

use super::{CoordinatorRequest, Method};


pub async fn query(req: HttpRequest, body: Bytes) -> Result<impl Responder, QueryError> {
    
    let request = CoordinatorRequest{
        body: Some(body),
        api: "query_coordinator",
        resource: None,
        method: Method::POST
    };

    let res = request.request().await?;

    match res.status() {
        StatusCode::OK => {
            Ok(web::Json(res.json::<Value>().await.unwrap()))
        },
        _ => {
            let err = res.text().await.unwrap();
            Err(QueryError::Anyhow(anyhow::Error::msg(err)))
        }
    }
    
}