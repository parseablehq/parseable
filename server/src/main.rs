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
use actix_web::dev::ServiceRequest;
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
mod storage;
mod sync_s3;
mod utils;
mod validator;

use error::Error;

// Global configurations
const MAX_EVENT_PAYLOAD_SIZE: usize = 102400;
const API_BASE_PATH: &str = "/api";
const API_VERSION: &str = "/v1";

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    banner::print();
    if let Err(e) = metadata::STREAM_INFO.load() {
        println!("{}", e);
    }
    let opt = option::get_opts();
    wrap(opt.clone()).await?;
    run_http(opt).await?;

    Ok(())
}

async fn wrap(opt: option::Opt) -> anyhow::Result<()> {
    thread::spawn(move || {
        let ticker = ticker::Ticker::new(0.., Duration::from_secs(1));
        for _ in ticker {
            if let Err(e) = sync_s3::syncer(opt.clone()) {
                println!("{}", e)
            }
        }
    });

    Ok(())
}

async fn validator(
    req: ServiceRequest,
    credentials: BasicAuth,
) -> Result<ServiceRequest, actix_web::Error> {
    let opt = option::get_opts();
    if credentials.user_id().trim() == opt.username
        && credentials.password().unwrap().trim() == opt.password
    {
        return Ok(req);
    }

    Err(actix_web::error::ErrorUnauthorized("Unauthorized"))
}

async fn run_http(opt: option::Opt) -> anyhow::Result<()> {
    let opt_clone = opt.clone();
    let ssl_acceptor = match (opt_clone.tls_cert_path, opt_clone.tls_key_path) {
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
            .bind_openssl(opt_clone.address, builder)?
            .run()
            .await?;
    } else {
        http_server.bind(opt_clone.address)?.run().await?;
    }

    Ok(())
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    // Base path "{url}/api/v1"
    cfg.service(
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
    // GET "/query" ==> Get results of the SQL query passed in request body
    .service(web::resource(query_path()).route(web::get().to(handler::cache_query)))
    // GET "/liveness" ==> Livenss check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
    .service(web::resource(liveness_path()).route(web::get().to(handler::liveness)))
    // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
    .service(web::resource(readiness_path()).route(web::get().to(handler::readiness)));
}

#[macro_export]
macro_rules! create_app {
    () => {
        App::new()
            .configure(|cfg| configure_routes(cfg))
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
