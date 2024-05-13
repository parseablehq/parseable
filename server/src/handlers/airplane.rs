use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::PollInfo;
use arrow_schema::{ArrowError, Schema};

use chrono::Utc;
use datafusion::common::tree_node::TreeNode;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tonic::codec::CompressionEncoding;

use futures_util::{Future, TryFutureExt};

use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_web::GrpcWebLayer;

use crate::event::commit_schema;
use crate::handlers::http::cluster::get_ingestor_info;
use crate::handlers::http::fetch_schema;

use crate::metrics::QUERY_EXECUTE_TIME;
use crate::option::{Mode, CONFIG};

use crate::handlers::livetail::cross_origin_config;

use crate::handlers::http::query::{authorize_and_set_filter_tags, into_query};
use crate::query::{TableScanVisitor, QUERY_SESSION};
use crate::storage::object_storage::commit_schema_to_storage;
use crate::utils::arrow::flight::{append_temporary_events, get_query_from_ticket, run_do_get_rpc};
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use futures::{stream, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::handlers::livetail::extract_session_key;
use crate::metadata::STREAM_INFO;
use crate::rbac::Users;

const L_CURLY: char = '{';
const R_CURLY: char = '}';

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

        let event = if CONFIG.parseable.mode == Mode::Query && time_delta.num_seconds() < 2 {
            let sql = format!(
                "{}\"query\": \"select * from {}\"{}",
                L_CURLY, &stream_name, R_CURLY
            );
            let ingester_metadatas = get_ingestor_info()
                .await
                .map_err(|err| Status::failed_precondition(err.to_string()))?;
            let mut minute_result: Vec<RecordBatch> = vec![];

            for im in ingester_metadatas {
                if let Ok(mut batches) = run_do_get_rpc(im, sql.clone()).await {
                    minute_result.append(&mut batches);
                }
            }
            let mr = minute_result.iter().collect::<Vec<_>>();
            let event = append_temporary_events(&stream_name, mr).await?;

            Some(event)
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
        let _schema =
            Schema::try_merge(schemas).map_err(|err| Status::internal(err.to_string()))?;
        let input_stream = futures::stream::iter(results.into_iter().map(Ok));
        let write_options = IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType(1)))
            .map_err(|err| Status::failed_precondition(err.to_string()))?;

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_max_flight_data_size(usize::MAX)
            .with_options(write_options)
            // .with_schema(schema.into())
            .build(input_stream);

        let flight_data_stream = flight_data_stream.map_err(|err| Status::unknown(err.to_string()));

        if let Some(event) = event {
            event.clear(&stream_name);
        }

        let time = time.elapsed().as_secs_f64();
        QUERY_EXECUTE_TIME
            .with_label_values(&[&format!("flight-query-{}", stream_name)])
            .observe(time);

        Ok(Response::new(
            Box::pin(flight_data_stream) as Self::DoGetStream
        ))
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
