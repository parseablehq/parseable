use arrow_array::RecordBatch;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{FlightClient, PollInfo};
use arrow_schema::ArrowError;
use chrono::Utc;
use datafusion::common::tree_node::TreeNode;
use futures::TryStreamExt;
use http::Uri;
use itertools::Itertools;
use serde_json::Value as JsonValue;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{Future, TryFutureExt};

use tonic::transport::{Channel, Identity, Server, ServerTlsConfig};
use tonic_web::GrpcWebLayer;

use crate::event::commit_schema;
use crate::handlers::http::cluster::get_ingestor_info;
use crate::handlers::http::fetch_schema;
use crate::option::{Mode, CONFIG};

use crate::handlers::livetail::cross_origin_config;

use crate::handlers::http::query::{authorize_and_set_filter_tags, into_query, Query as QueryJson};
use crate::query::{TableScanVisitor, QUERY_SESSION};
use crate::storage::object_storage::commit_schema_to_storage;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use futures::stream::BoxStream;

use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc,
    SchemaResult, Ticket,
};

use crate::handlers::livetail::extract_session_key;

use crate::metadata::STREAM_INFO;

use crate::rbac::Users;

#[derive(Clone)]
pub struct AirServiceImpl {}

#[tonic::async_trait]
impl FlightService for AirServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

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

        let ticket = if CONFIG.parseable.mode == Mode::Ingest {
            let query = serde_json::from_slice::<JsonValue>(&req.into_inner().ticket)
                .map_err(|_| Status::failed_precondition("Ticket is not valid json"))?["query"]
                .as_str()
                .ok_or_else(|| Status::failed_precondition("query is not valid string"))?
                .to_owned();
            QueryJson {
                query,
                send_null: false,
                fields: false,
                filter_tags: None,
                // we can use humantime because into_query handle parseing
                end_time: String::from("now"),
                start_time: String::from("1min"),
            }
        } else {
            serde_json::from_slice::<QueryJson>(&req.into_inner().ticket)
                .map_err(|err| Status::internal(err.to_string()))?
        };

        log::info!("airplane requested for query {:?}", ticket);

        // get the query session_state
        let session_state = QUERY_SESSION.state();

        // get the logical plan and extract the table name
        let raw_logical_plan = session_state
            .create_logical_plan(&ticket.query)
            .await
            .map_err(|err| {
                log::error!("Failed to create logical plan: {}", err);
                Status::internal("Failed to create logical plan")
            })?;

        // create a visitor to extract the table name
        let mut visitor = TableScanVisitor::default();
        let _ = raw_logical_plan.visit(&mut visitor);

        let tables = visitor.into_inner();

        if CONFIG.parseable.mode == Mode::Query {
            // using http to get the schema. may update to use flight later

            for table in tables {
                if let Ok(new_schema) = fetch_schema(&table).await {
                    // commit schema merges the schema internally and updates the schema in storage.
                    commit_schema_to_storage(&table, new_schema.clone())
                        .await
                        .map_err(|err| Status::internal(err.to_string()))?;
                    commit_schema(&table, Arc::new(new_schema))
                        .map_err(|err| Status::internal(err.to_string()))?;
                }
            }
        }

        // map payload to query
        let mut query = into_query(&ticket, &session_state)
            .await
            .map_err(|_| Status::internal("Failed to parse query"))?;
        let time_delta = query.end - Utc::now();

        let minute_result = if CONFIG.parseable.mode == Mode::Query && time_delta.num_seconds() < 2
        {
            let sql = ticket.query.clone();
            let ingester_metadatas = get_ingestor_info()
                .await
                .map_err(|err| Status::failed_precondition(err.to_string()))?;
            let mut minute_result: Vec<RecordBatch> = vec![];

            for im in ingester_metadatas.iter() {
                let mut url = im.domain_name.rsplit(":").collect_vec();
                let _ = url.pop();
                url.reverse();
                url.push(&im.flight_port);
                let url = url.join("");
                let url = url
                    .parse::<Uri>()
                    .map_err(|_| Status::failed_precondition("Ingester metadata is courupted"))?;

                let channel = Channel::builder(url)
                    .connect()
                    .await
                    .map_err(|err| Status::failed_precondition(err.to_string()))?;

                let mut client = FlightClient::new(channel);
                client.add_header("authorization", &im.token)?;

                let response = client
                    .do_get(Ticket {
                        ticket: sql.clone().into(),
                    })
                    .await?;

                let mut batches: Vec<RecordBatch> = response.try_collect().await?;

                minute_result.append(&mut batches);
            }

            Some(minute_result)
        } else {
            None
        };

        // if table name is not present it is a Malformed Query
        let stream_name = query
            .first_table_name()
            .ok_or_else(|| Status::invalid_argument("Malformed Query"))?;

        let permissions = Users.get_permissions(&key);

        authorize_and_set_filter_tags(&mut query, permissions, &stream_name).map_err(|_| {
            Status::permission_denied("User Does not have permission to access this")
        })?;

        let schema = STREAM_INFO
            .schema(&stream_name)
            .map_err(|err| Status::failed_precondition(err.to_string()))?;

        let (mut results, _) = query
            .execute(stream_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        if let Some(mut minute_result) = minute_result {
            results.append(&mut minute_result);
        }

        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let schema_flight_data = SchemaAsIpc::new(&schema, &options);

        let mut flights = vec![FlightData::from(schema_flight_data)];
        let encoder = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        for batch in &results {
            let (flight_dictionaries, flight_batch) = encoder
                .encoded_batch(batch, &mut tracker, &options)
                .map_err(|e| Status::internal(e.to_string()))?;
            flights.extend(flight_dictionaries.into_iter().map(Into::into));
            flights.push(flight_batch.into());
        }
        let output = futures::stream::iter(flights.into_iter().map(Ok));
        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
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
        .expect("valid socket address");
    addr.set_port(CONFIG.parseable.flight_port);

    let service = AirServiceImpl {};

    let svc = FlightServiceServer::new(service);

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
                .accept_http1(true)
                .layer(cors)
                .layer(GrpcWebLayer::new())
                .add_service(svc)
                .serve(addr)
                .map_err(err_map_fn)
        }
        None => Server::builder()
            .accept_http1(true)
            .layer(cors)
            .layer(GrpcWebLayer::new())
            .add_service(svc)
            .serve(addr)
            .map_err(err_map_fn),
    }
}
