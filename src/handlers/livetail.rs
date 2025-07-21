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

use std::net::SocketAddr;

use arrow_array::RecordBatch;
use arrow_flight::PollInfo;
use arrow_flight::encode::FlightDataEncoderBuilder;
use cookie::Cookie;
use futures::stream::BoxStream;
use futures_util::{Future, StreamExt, TryFutureExt, TryStreamExt};
use http_auth_basic::Credentials;
use rand::distributions::{Alphanumeric, DistString};
use tonic::metadata::MetadataMap;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
};
use tonic_web::GrpcWebLayer;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use crate::livetail::{LIVETAIL, Message};
use crate::parseable::PARSEABLE;
use crate::rbac::map::SessionKey;
use crate::rbac::{self, Users};
use crate::utils;

use super::SESSION_COOKIE_NAME;

#[derive(Clone)]
pub struct FlightServiceImpl {}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
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

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let key = extract_session_key(req.metadata()).map_err(|e| *e)?;
        let ticket: serde_json::Value = serde_json::from_slice(&req.into_inner().ticket)
            .map_err(|err| Status::internal(err.to_string()))?;
        let stream = extract_stream(&ticket).map_err(|e| *e)?;
        info!("livetail requested for stream {}", stream);
        match Users.authorize(key, rbac::role::Action::Query, Some(stream), None) {
            rbac::Response::Authorized => (),
            rbac::Response::UnAuthorized => {
                return Err(Status::permission_denied(
                    "user is not authenticated to access this resource",
                ));
            }
            rbac::Response::ReloadRequired => {
                return Err(Status::unauthenticated("reload required"));
            }
        }

        let schema = PARSEABLE
            .get_stream(stream)
            .map_err(|err| Status::failed_precondition(err.to_string()))?
            .get_schema();

        let rx = LIVETAIL.new_pipe(
            Alphanumeric.sample_string(&mut rand::thread_rng(), 32),
            stream.to_string(),
        );

        let adapter_schema = schema.clone();
        let rx = rx.map(move |x| match x {
            Message::Record(t) => Ok(utils::arrow::adapt_batch(&adapter_schema, &t)),
            Message::Skipped(_) => {
                warn!("livetail channel capacity is full.");
                Ok(RecordBatch::new_empty(adapter_schema.clone()))
            }
        });

        let rb_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(rx);

        let rb_stream = rb_stream.map_err(|err| Status::unknown(err.to_string()));
        Ok(Response::new(Box::pin(rb_stream)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

pub fn server() -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send>>> + Send {
    let mut addr: SocketAddr = PARSEABLE
        .options
        .address
        .parse()
        .expect("valid socket address");
    addr.set_port(PARSEABLE.options.grpc_port);

    let service = FlightServiceImpl {};

    let svc = FlightServiceServer::new(service);

    let cors = cross_origin_config();

    let identity = match (
        &PARSEABLE.options.tls_cert_path,
        &PARSEABLE.options.tls_key_path,
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

pub fn extract_stream(body: &serde_json::Value) -> Result<&str, Box<Status>> {
    body.as_object()
        .ok_or_else(|| Box::new(Status::invalid_argument("expected object in request body")))?
        .get("stream")
        .ok_or_else(|| Box::new(Status::invalid_argument("stream key value is not provided")))?
        .as_str()
        .ok_or_else(|| Box::new(Status::invalid_argument("stream key value is invalid")))
}

pub fn extract_session_key(headers: &MetadataMap) -> Result<SessionKey, Box<Status>> {
    // Extract username and password from the request using basic auth extractor.
    let basic = extract_basic_auth(headers).map(|creds| SessionKey::BasicAuth {
        username: creds.user_id,
        password: creds.password,
    });

    if let Some(basic) = basic {
        return Ok(basic);
    }

    let session = extract_cookie(headers)
        .map(|cookie| ulid::Ulid::from_string(cookie.value()))
        .transpose()
        .map_err(|_| Status::invalid_argument("Cookie is tampered with or invalid"))?;

    if let Some(session) = session {
        return Ok(SessionKey::SessionId(session));
    }

    Err(Box::new(Status::unauthenticated(
        "No authentication method supplied",
    )))
}

fn extract_basic_auth(header: &MetadataMap) -> Option<Credentials> {
    header
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Credentials::from_header(value.to_string()).ok())
}

fn extract_cookie(header: &MetadataMap) -> Option<Cookie> {
    // extract the cookie from the request
    let cookies = header.get_all("cookie");
    let cookies: Vec<_> = cookies
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(Cookie::split_parse)
        .map(|value| value.expect("cookie is parseable"))
        .collect();

    cookies
        .into_iter()
        .find(|cookie| cookie.name() == SESSION_COOKIE_NAME)
}

#[inline(always)]
pub fn cross_origin_config() -> CorsLayer {
    CorsLayer::very_permissive().allow_credentials(true)
}
