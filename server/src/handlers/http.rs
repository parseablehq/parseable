/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::fs::File;
use std::io::BufReader;

use actix_cors::Cors;
use actix_web::dev::{Service, ServiceRequest};
use actix_web::error::ErrorBadRequest;
use actix_web::{middleware, web, App, HttpServer};
use actix_web_httpauth::extractors::basic::BasicAuth;
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_prometheus::PrometheusMetrics;
use actix_web_static_files::ResourceFiles;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};

use crate::option::CONFIG;
use crate::rbac::user_map;

mod health_check;
mod ingest;
mod logstream;
mod query;
mod rbac;

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

const MAX_EVENT_PAYLOAD_SIZE: usize = 10485760;
const API_BASE_PATH: &str = "/api";
const API_VERSION: &str = "v1";

#[macro_export]
macro_rules! create_app {
    ($prometheus: expr) => {
        App::new()
            .wrap($prometheus.clone())
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

async fn authenticate(
    req: ServiceRequest,
    credentials: BasicAuth,
) -> Result<ServiceRequest, (actix_web::Error, ServiceRequest)> {
    let username = credentials.user_id().trim();
    let password = credentials.password().unwrap().trim();

    if let Some(user) = user_map().read().unwrap().get(username) {
        if user.verify(password) {
            return Ok(req);
        }
    }

    Err((actix_web::error::ErrorUnauthorized("Unauthorized"), req))
}

pub async fn run_http(prometheus: PrometheusMetrics) -> anyhow::Result<()> {
    let ssl_acceptor = match (
        &CONFIG.parseable.tls_cert_path,
        &CONFIG.parseable.tls_key_path,
    ) {
        (Some(cert), Some(key)) => {
            // init server config builder with safe defaults
            let config = ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth();

            // load TLS key/cert files
            let cert_file = &mut BufReader::new(File::open(cert)?);
            let key_file = &mut BufReader::new(File::open(key)?);

            // convert files to key/cert objects
            let cert_chain = certs(cert_file)?.into_iter().map(Certificate).collect();

            let mut keys: Vec<PrivateKey> = pkcs8_private_keys(key_file)?
                .into_iter()
                .map(PrivateKey)
                .collect();

            // exit if no keys could be parsed
            if keys.is_empty() {
                anyhow::bail!("Could not locate PKCS 8 private keys.");
            }

            let server_config = config.with_single_cert(cert_chain, keys.remove(0))?;

            Some(server_config)
        }
        (_, _) => None,
    };

    // concurrent workers equal to number of cores on the cpu
    let http_server = HttpServer::new(move || create_app!(prometheus)).workers(num_cpus::get());
    if let Some(config) = ssl_acceptor {
        http_server
            .bind_rustls(&CONFIG.parseable.address, config)?
            .run()
            .await?;
    } else {
        http_server.bind(&CONFIG.parseable.address)?.run().await?;
    }

    Ok(())
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    let generated = generate();

    //log stream API
    let logstream_api = web::scope("/{logstream}")
        .service(
            web::resource("")
                // PUT "/logstream/{logstream}" ==> Create log stream
                .route(web::put().to(logstream::put_stream))
                // POST "/logstream/{logstream}" ==> Post logs to given log stream
                .route(web::post().to(ingest::post_event))
                // DELETE "/logstream/{logstream}" ==> Delete log stream
                .route(web::delete().to(logstream::delete))
                .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
        )
        .service(
            web::resource("/alert")
                // PUT "/logstream/{logstream}/alert" ==> Set alert for given log stream
                .route(web::put().to(logstream::put_alert))
                // GET "/logstream/{logstream}/alert" ==> Get alert for given log stream
                .route(web::get().to(logstream::get_alert)),
        )
        .service(
            // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
            web::resource("/schema").route(web::get().to(logstream::schema)),
        )
        .service(
            // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
            web::resource("/stats").route(web::get().to(logstream::get_stats)),
        )
        .service(
            web::resource("/retention")
                // PUT "/logstream/{logstream}/retention" ==> Set retention for given logstream
                .route(web::put().to(logstream::put_retention))
                // GET "/logstream/{logstream}/retention" ==> Get retention for given logstream
                .route(web::get().to(logstream::get_retention)),
        );

    // User API
    let user_api = web::scope("/user").service(
        web::resource("/{username}")
            // POST /user/{username} => Create a new user
            .route(web::put().to(rbac::put_user))
            // DELETE /user/{username} => Delete a user
            .route(web::delete().to(rbac::delete_user))
            .wrap_fn(|req, srv| {
                // The credentials set in the env vars (P_USERNAME & P_PASSWORD) are treated
                // as root credentials. Any other user is not allowed to modify / delete
                // the root user. Deny request if username is same as username 
                // from env variable P_USERNAME.
                let username = req.match_info().get("username").unwrap_or("");
                let is_root = username == CONFIG.parseable.username;
                let call = srv.call(req);
                async move {
                    if is_root {
                        return Err(ErrorBadRequest("Cannot call this API for root admin user"));
                    }
                    call.await
                }
            }),
    );

    cfg.service(
        // Base path "{url}/api/v1"
        web::scope(&base_path())
            // POST "/query" ==> Get results of the SQL query passed in request body
            .service(web::resource("/query").route(web::post().to(query::query)))
            // POST "/ingest" ==> Post logs to given log stream based on header
            .service(
                web::resource("/ingest")
                    .route(web::post().to(ingest::ingest))
                    .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            // GET "/liveness" ==> Liveness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
            .service(web::resource("/liveness").route(web::get().to(health_check::liveness)))
            // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
            .service(web::resource("/readiness").route(web::get().to(health_check::readiness)))
            .service(
                web::scope("/logstream")
                    .service(
                        // GET "/logstream" ==> Get list of all Log Streams on the server
                        web::resource("").route(web::get().to(logstream::list)),
                    )
                    .service(
                        // logstream API
                        logstream_api,
                    ),
            )
            .service(user_api)
            .wrap(HttpAuthentication::basic(authenticate)),
    )
    // GET "/" ==> Serve the static frontend directory
    .service(ResourceFiles::new("/", generated));
}

fn base_path() -> String {
    format!("{API_BASE_PATH}/{API_VERSION}")
}

pub fn metrics_path() -> String {
    format!("{}/metrics", base_path())
}
