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

use arrow_array::RecordBatch;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::PollInfo;
use arrow_schema::ArrowError;

use serde_json::json;
use std::net::SocketAddr;
use std::time::Instant;
use tonic::codec::CompressionEncoding;
use tracing::{error, info};

use futures_util::{Future, TryFutureExt};

use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_web::GrpcWebLayer;

use crate::handlers::http::cluster::get_ingestor_info;
use crate::handlers::livetail::cross_origin_config;
use crate::metrics::QUERY_EXECUTE_TIME;
use crate::option::CONFIG;
use crate::utils::arrow::flight::{
    append_temporary_events, get_query_from_ticket, into_flight_data, run_do_get_rpc,
    send_to_ingester,
};
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use futures::stream;
use tonic::{Request, Response, Status, Streaming};

use crate::handlers::livetail::extract_session_key;
use crate::metadata::STREAM_INFO;
use crate::rbac;
use crate::rbac::Users;

#[derive(Clone, Debug)]
pub struct AirServiceImpl {}

#[tonic::async_trait]
impl FlightService for AirServiceImpl {
    type HandshakeStream = stream::BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = stream::BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = stream::BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = stream::BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = stream::BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = stream::BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = stream::BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "handshake is disabled in favour of direct authentication and authorization",
        ))
    }

    /// list_flights is an operation that allows a client
    /// to query a Flight server for information
    /// about available datasets or "flights" that the server can provide.
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let table_name = request.into_inner().path;
        let table_name = table_name[0].clone();

        let schema = STREAM_INFO
            .schema(&table_name)
            .map_err(|err| Status::failed_precondition(err.to_string()))?;

        let options = IpcWriteOptions::default();
        let schema_result = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|err: ArrowError| Status::internal(err.to_string()))?;

        Ok(Response::new(schema_result))
    }

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let key = extract_session_key(req.metadata())?;

        // try authorize
        match Users.authorize(key.clone(), rbac::role::Action::Query, None, None) {
            rbac::Response::Authorized => (),
            rbac::Response::UnAuthorized => {
                return Err(Status::permission_denied(
                    "user is not authorized to access this resource",
                ))
            }
            rbac::Response::ReloadRequired => {
                return Err(Status::unauthenticated("reload required"))
            }
        }

        let query_request = get_query_from_ticket(&req)?;
        info!("query requested to airplane: {:?}", query_request);

        // map payload to query
        let query = query_request.into_query(key).await.map_err(|e| {
            error!("Error faced while constructing logical query: {e}");
            Status::internal(format!("Failed to process query: {e}"))
        })?;

        let stream_name = query
            .first_stream_name()
            .ok_or_else(|| Status::internal("Failed to get stream name from query"))?;

        let event = if send_to_ingester(
            query.time_range.start.timestamp_millis(),
            query.time_range.end.timestamp_millis(),
        ) {
            let out_ticket = json!({
                "query": format!("select * from {stream_name}"),
                "startTime": query_request.start_time,
                "endTime":  query_request.end_time,
            })
            .to_string();

            let ingester_metadatas = get_ingestor_info()
                .await
                .map_err(|err| Status::failed_precondition(err.to_string()))?;
            let mut minute_result: Vec<RecordBatch> = vec![];

            for im in ingester_metadatas {
                if let Ok(mut batches) = run_do_get_rpc(im, out_ticket.clone()).await {
                    minute_result.append(&mut batches);
                }
            }
            let mr = minute_result.iter().collect::<Vec<_>>();
            let event = append_temporary_events(stream_name, mr).await?;
            Some(event)
        } else {
            None
        };

        let time = Instant::now();
        let (records, _) = query
            .execute()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        /*
        * INFO: No returning the schema with the data.
        * kept it in case it needs to be sent in the future.

        let schemas = results
            .iter()
            .map(|batch| batch.schema())
            .map(|s| s.as_ref().clone())
            .collect::<Vec<_>>();
        let schema = Schema::try_merge(schemas).map_err(|err| Status::internal(err.to_string()))?;
         */
        let out = into_flight_data(records);

        if let Some(event) = event {
            event.clear(stream_name);
        }

        let time = time.elapsed().as_secs_f64();
        QUERY_EXECUTE_TIME
            .with_label_values(&[&format!("flight-query-{}", stream_name)])
            .observe(time);

        out
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "do_put not implemented because we are only using flight for querying",
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(
            "do_action not implemented because we are only using flight for querying",
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(
            "list_actions not implemented because we are only using flight for querying",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "do_exchange not implemented because we are only using flight for querying",
        ))
    }
}

pub fn server() -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send>>> + Send {
    let mut addr: SocketAddr = CONFIG
        .parseable
        .address
        .parse()
        .unwrap_or_else(|err| panic!("{}, failed to parse `{}` as a socket address. Please set the environment variable `P_ADDR` to `<ip address>:<port>` without the scheme (e.g., 192.168.1.1:8000). Please refer to the documentation: https://logg.ing/env for more details.",
CONFIG.parseable.address, err));
    addr.set_port(CONFIG.parseable.flight_port);

    let service = AirServiceImpl {};

    let svc = FlightServiceServer::new(service)
        .max_encoding_message_size(usize::MAX)
        .max_decoding_message_size(usize::MAX)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);

    let cors = cross_origin_config();

    let identity = match (
        &CONFIG.parseable.tls_cert_path,
        &CONFIG.parseable.tls_key_path,
    ) {
        (Some(cert), Some(key)) => {
            match (std::fs::read_to_string(cert), std::fs::read_to_string(key)) {
                (Ok(cert_file), Ok(key_file)) => {
                    let identity = Identity::from_pem(cert_file, key_file);
                    Some(identity)
                }
                _ => None,
            }
        }
        (_, _) => None,
    };

    let config = identity.map(|id| ServerTlsConfig::new().identity(id));

    // rust is treating closures as different types
    let err_map_fn = |err| Box::new(err) as Box<dyn std::error::Error + Send>;

    // match on config to decide if we want to use tls or not
    match config {
        Some(config) => {
            let server = match Server::builder().tls_config(config) {
                Ok(server) => server,
                Err(_) => Server::builder(),
            };

            server
                .max_frame_size(16 * 1024 * 1024 - 2)
                .accept_http1(true)
                .layer(cors)
                .layer(GrpcWebLayer::new())
                .add_service(svc)
                .serve(addr)
                .map_err(err_map_fn)
        }
        None => Server::builder()
            .max_frame_size(16 * 1024 * 1024 - 2)
            .accept_http1(true)
            .layer(cors)
            .layer(GrpcWebLayer::new())
            .add_service(svc)
            .serve(addr)
            .map_err(err_map_fn),
    }
}
