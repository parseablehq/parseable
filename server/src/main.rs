/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

use actix_cors::Cors;
use actix_files::{Files, NamedFile};
use actix_web::dev::{fn_service, ServiceRequest, ServiceResponse};
use actix_web::{middleware, web, App, HttpServer};
use actix_web_httpauth::extractors::basic::BasicAuth;
use actix_web_httpauth::middleware::HttpAuthentication;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

use std::thread;
use std::time::Duration;
extern crate ticker;

mod banner;
mod error;
mod event;
mod handler;
mod metadata;
mod option;
mod query;
mod response;
mod s3;
mod storage;
mod utils;
mod validator;

use error::Error;
use option::CONFIG;
use s3::S3;
use storage::ObjectStorage;

// Global configurations
const MAX_EVENT_PAYLOAD_SIZE: usize = 102400;
const API_BASE_PATH: &str = "/api";
const API_VERSION: &str = "/v1";

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    CONFIG.print();
    let storage = S3::new();
    if let Err(e) = metadata::STREAM_INFO.load(&storage).await {
        println!("{}", e);
    }
    thread::spawn(move || wrap(storage));
    run_http().await?;

    Ok(())
}

#[actix_web::main]
async fn wrap(storage: impl ObjectStorage) {
    let ticker = ticker::Ticker::new(0.., Duration::from_secs(1));
    for _ in ticker {
        if let Err(e) = storage.sync().await {
            println!("{}", e)
        }
    }
}

async fn validator(
    req: ServiceRequest,
    credentials: BasicAuth,
) -> Result<ServiceRequest, actix_web::Error> {
    if credentials.user_id().trim() == CONFIG.parseable.username
        && credentials.password().unwrap().trim() == CONFIG.parseable.password
    {
        return Ok(req);
    }

    Err(actix_web::error::ErrorUnauthorized("Unauthorized"))
}

async fn run_http() -> anyhow::Result<()> {
    let ssl_acceptor = match (
        &CONFIG.parseable.tls_cert_path,
        &CONFIG.parseable.tls_key_path,
    ) {
        (Some(cert), Some(key)) => {
            let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
            builder.set_private_key_file(key, SslFiletype::PEM)?;
            builder.set_certificate_chain_file(cert)?;
            Some(builder)
        }
        (_, _) => None,
    };

    let http_server =
        HttpServer::new(move || create_app!().wrap(HttpAuthentication::basic(validator)));
    if let Some(builder) = ssl_acceptor {
        http_server
            .bind_openssl(&CONFIG.parseable.address, builder)?
            .run()
            .await?;
    } else {
        http_server.bind(&CONFIG.parseable.address)?.run().await?;
    }

    Ok(())
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    // Base path "{url}/api/v1"
    // POST "/query" ==> Get results of the SQL query passed in request body
    cfg.service(web::resource(query_path()).route(web::post().to(handler::query)))
        .service(
            // logstream API
            web::resource(logstream_path("{logstream}"))
                // PUT "/logstream/{logstream}" ==> Create log stream
                .route(web::put().to(handler::put_stream))
                // POST "/logstream/{logstream}" ==> Post logs to given log stream
                .route(web::post().to(handler::post_event))
                // DELETE "/logstream/{logstream}" ==> Delete log stream
                .route(web::delete().to(handler::delete_stream))
                .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
        )
        .service(
            web::resource(alert_path("{logstream}"))
                // PUT "/logstream/{logstream}/alert" ==> Set alert for given log stream
                .route(web::put().to(handler::put_alert))
                // GET "/logstream/{logstream}/alert" ==> Get alert for given log stream
                .route(web::get().to(handler::get_alert)),
        )
        // GET "/logstream" ==> Get list of all Log Streams on the server
        .service(web::resource(logstream_path("")).route(web::get().to(handler::list_streams)))
        .service(
            // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
            web::resource(schema_path("{logstream}")).route(web::get().to(handler::get_schema)),
        )
        // GET "/liveness" ==> Livenss check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
        .service(web::resource(liveness_path()).route(web::get().to(handler::liveness)))
        // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
        .service(web::resource(readiness_path()).route(web::get().to(handler::readiness)));
}

pub fn configure_static_files(cfg: &mut web::ServiceConfig) {
    cfg.service(
        Files::new("/", "./ui/build/")
            .index_file("index.html")
            .show_files_listing()
            .default_handler(fn_service(|req: ServiceRequest| async {
                let (req, _) = req.into_parts();
                let file = NamedFile::open_async("./ui/build/index.html").await?;
                let res = file.into_response(&req);
                Ok(ServiceResponse::new(req, res))
            })),
    );
}

#[macro_export]
macro_rules! create_app {
    () => {
        App::new()
            .configure(|cfg| configure_routes(cfg))
            .configure(|cfg| configure_static_files(cfg))
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(
                Cors::default()
                    .allow_any_header()
                    .allow_any_method()
                    .allow_any_origin(),
            )
    };
}

fn base_path() -> String {
    format!("{}{}", API_BASE_PATH, API_VERSION)
}

fn logstream_path(stream_name: &str) -> String {
    if stream_name.is_empty() {
        return format!("{}/logstream", base_path());
    }
    format!("{}/logstream/{}", base_path(), stream_name)
}

fn readiness_path() -> String {
    format!("{}/readiness", base_path())
}

fn liveness_path() -> String {
    format!("{}/liveness", base_path())
}

fn query_path() -> String {
    format!("{}/query", base_path())
}

fn alert_path(stream_name: &str) -> String {
    format!("{}/alert", logstream_path(stream_name))
}

fn schema_path(stream_name: &str) -> String {
    format!("{}/schema", logstream_path(stream_name))
}
