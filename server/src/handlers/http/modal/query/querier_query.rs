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

use actix_web::{web, HttpRequest, Responder};
use arrow_array::RecordBatch;
use arrow_flight::{FlightClient, Ticket};
use bytes::Bytes;
use futures::TryStreamExt;
use http::StatusCode;
use itertools::Itertools;
use serde_json::{json, Value};
use tonic::{
    transport::{Channel, Uri},
    Status,
};

use crate::{
    handlers::http::{
        modal::{query_server::QUERY_ROUTING, QuerierMetadata, LEADER},
        query::{Query, QueryError},
    },
    utils::arrow::record_batches_to_json,
};

use super::{LeaderRequest, Method};

/// If leader, decide which querier will respond to the query
/// if no querier is free or if only leader is alive, leader processes query
pub async fn query(_req: HttpRequest, body: Bytes) -> Result<impl Responder, QueryError> {
    let query_request: Query = serde_json::from_slice(&body).map_err(|err| {
        log::error!("Error while converting bytes to Query");
        QueryError::Anyhow(anyhow::Error::msg(err.to_string()))
    })?;

    if LEADER.lock().await.is_leader() {
        // if leader, then fetch a query node to reroute the query to
        QUERY_ROUTING.lock().await.check_liveness().await;
        let node = QUERY_ROUTING.lock().await.get_query_node().await;
        let fill_null = query_request.send_null;
        let with_fields = query_request.fields;

        match query_helper(query_request, node.clone()).await {
            Ok(records) => match records.first() {
                Some(record) => {
                    let fields = record
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .cloned()
                        .collect_vec();

                    QUERY_ROUTING.lock().await.reinstate_node(node);

                    let r: Vec<&RecordBatch> = records.iter().collect();
                    let mut json_records = record_batches_to_json(&r)?;

                    if fill_null {
                        for map in &mut json_records {
                            for field in fields.clone() {
                                if !map.contains_key(&field) {
                                    map.insert(field.clone(), Value::Null);
                                }
                            }
                        }
                    }
                    let values = json_records.into_iter().map(Value::Object).collect_vec();

                    let response = if with_fields {
                        json!({
                            "fields": fields,
                            "records": values
                        })
                    } else {
                        Value::Array(values)
                    };

                    Ok(web::Json(response))
                }
                None => {
                    QUERY_ROUTING.lock().await.reinstate_node(node);
                    Err(QueryError::Anyhow(anyhow::Error::msg("no records")))
                }
            },
            Err(e) => {
                QUERY_ROUTING.lock().await.reinstate_node(node);
                Err(QueryError::Anyhow(anyhow::Error::msg(e.to_string())))
            }
        }
    } else {
        // follower will forward request to leader
        let request = LeaderRequest {
            body: Some(body),
            api: "query",
            resource: None,
            method: Method::Post,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => {
                let data: Value = res.json().await?;
                Ok(web::Json(data))
            }
            _ => {
                let err_msg = res.text().await?;
                Err(QueryError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

async fn query_helper(
    query_request: Query,
    node: QuerierMetadata,
) -> Result<Vec<RecordBatch>, Status> {
    let sql = query_request.query;
    let start_time = query_request.start_time;
    let end_time = query_request.end_time;
    let out_ticket = json!({
        "query": sql,
        "startTime": start_time,
        "endTime": end_time
    })
    .to_string();

    // send a poll_info request
    let url = node
        .domain_name
        .rsplit_once(':')
        .ok_or(Status::failed_precondition("Querier metadata is courupted"))
        .unwrap()
        .0;
    let url = format!("{}:{}", url, node.flight_port);

    let url = url
        .parse::<Uri>()
        .map_err(|_| Status::failed_precondition("Querier metadata is courupted"))
        .unwrap();

    let channel = Channel::builder(url)
        .connect()
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))
        .unwrap();

    let client = FlightClient::new(channel);

    let inn = client
        .into_inner()
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    let mut client = FlightClient::new_from_inner(inn);

    client.add_header("authorization", &node.token).unwrap();

    Ok(client
        .do_get(Ticket {
            ticket: out_ticket.into(),
        })
        .await?
        .try_collect()
        .await?)

    // // Leave this here in case we want to look into implementing PollFlightInfo later
    // let mut hasher = DefaultHasher::new();
    // out_ticket.hash(&mut hasher);
    // let hashed_query = hasher.finish();

    // let mut descriptor = FlightDescriptor{
    //     cmd: out_ticket.into(),
    //     path: vec![hashed_query.to_string()],
    //     ..Default::default()
    // };
    // let mut response = Vec::new();

    // loop {
    //     match client.poll_flight_info(descriptor.clone()).await {
    //         Ok(poll_info) => {
    //             // in case we got no error, we either have the result or the server is still working on the result
    //             match poll_info.info {
    //                 Some(info) => {
    //                     // this means query is complete, expect FlightEndpoint
    //                     let endpoints = info.endpoint;

    //                     for endpoint in endpoints {
    //                         match client.do_get(endpoint.ticket.unwrap()).await {
    //                             Ok(batch_stream) => response.push(batch_stream),
    //                             Err(err) => {
    //                                 // there was an error, depending upon its type decide whether we need to re-try the request
    //                                 log::error!("do_get error- {err:?}");
    //                             },
    //                         }
    //                     }
    //                 },
    //                 None => {
    //                     // this means query is still running, expect FlightDescriptor
    //                     descriptor = poll_info.flight_descriptor.unwrap();
    //                 },
    //             }
    //         },
    //         Err(err) => {
    //             log::error!("poll_info error- {err:?}");
    //         },
    //     }
    // }
}
