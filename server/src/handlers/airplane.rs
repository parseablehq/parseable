use arrow_array::RecordBatch;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::PollInfo;
use arrow_schema::{ArrowError, Schema};
use chrono::Utc;
use datafusion::common::tree_node::TreeNode;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use futures_util::{Future, TryFutureExt};

use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_web::GrpcWebLayer;

use crate::event::commit_schema;
use crate::handlers::http::cluster::get_ingestor_info;
use crate::handlers::http::fetch_schema;
use crate::handlers::http::ingest::push_logs_unchecked;
use crate::metrics::QUERY_EXECUTE_TIME;
use crate::option::{Mode, CONFIG};

use crate::handlers::livetail::cross_origin_config;

use crate::handlers::http::query::{authorize_and_set_filter_tags, into_query};
use crate::query::{TableScanVisitor, QUERY_SESSION};
use crate::storage::object_storage::commit_schema_to_storage;
use crate::utils::arrow::flight::{get_query_from_ticket, run_do_get_rpc};
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use futures::stream::BoxStream;
use tonic::{Request, Response, Status, Streaming};

use crate::handlers::livetail::extract_session_key;
use crate::metadata::STREAM_INFO;
use crate::rbac::Users;

const L_CURLY: char = '{';
const R_CURLY: char = '}';

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

        let ticket = get_query_from_ticket(req)?;

        log::info!("query requested to airplane: {:?}", ticket);

        // get the query session_state
        let session_state = QUERY_SESSION.state();

        // get the logical plan and extract the table name
        let raw_logical_plan = session_state
            .create_logical_plan(&ticket.query)
            .await
            .map_err(|err| {
                log::error!("Datafusion Error: Failed to create logical plan: {}", err);
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

        // if table name is not present it is a Malformed Query
        let stream_name = query
            .first_table_name()
            .ok_or_else(|| Status::invalid_argument("Malformed Query"))?;

        let time_delta = query.end - Utc::now();

        let events = if CONFIG.parseable.mode == Mode::Query && time_delta.num_seconds() < 1 {
            let sql = format!(
                "{}\"query\": \"select * from {}\"{}",
                L_CURLY, &stream_name, R_CURLY
            );
            let ingester_metadatas = get_ingestor_info()
                .await
                .map_err(|err| Status::failed_precondition(err.to_string()))?;
            let mut minute_result: Vec<RecordBatch> = vec![];

            for im in ingester_metadatas {
                let mut batches = run_do_get_rpc(im, sql.clone()).await?;
                minute_result.append(&mut batches);
            }
            let mut events = vec![];
            for batch in minute_result {
                events.push(
                    push_logs_unchecked(batch, &stream_name)
                        .await
                        .map_err(|err| Status::internal(err.to_string()))?,
                );
            }
            Some(events)
        } else {
            None
        };

        let permissions = Users.get_permissions(&key);

        authorize_and_set_filter_tags(&mut query, permissions, &stream_name).map_err(|_| {
            Status::permission_denied("User Does not have permission to access this")
        })?;

        let time = Instant::now();
        let (results, _) = query
            .execute(stream_name.clone())
            .await
            .map_err(|err| Status::internal(err.to_string()))
            .unwrap();

        let schemas = results
            .iter()
            .map(|batch| batch.schema())
            .map(|s| s.as_ref().clone())
            .collect::<Vec<_>>();

        let schema = Schema::try_merge(schemas).map_err(|err| Status::internal(err.to_string()))?;
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
        if let Some(events) = events {
            for event in events {
                event.clear(&stream_name);
            }
        }

        let time = time.elapsed().as_secs_f64();
        QUERY_EXECUTE_TIME
            .with_label_values(&[&format!("flight-query-{}", stream_name)])
            .observe(time);

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
