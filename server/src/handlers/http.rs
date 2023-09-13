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
use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetrics;
use actix_web_static_files::ResourceFiles;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};

use crate::option::CONFIG;
use crate::rbac::role::Action;

use self::middleware::{DisAllowRootUser, RouteExt};

mod about;
mod health_check;
mod ingest;
mod llm;
mod logstream;
mod middleware;
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
            .wrap(actix_web::middleware::Logger::default())
            .wrap(actix_web::middleware::Compress::default())
            .wrap(
                Cors::default()
                    .allow_any_header()
                    .allow_any_method()
                    .allow_any_origin(),
            )
    };
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
                .route(
                    web::put()
                        .to(logstream::put_stream)
                        .authorize_for_stream(Action::CreateStream),
                )
                // POST "/logstream/{logstream}" ==> Post logs to given log stream
                .route(
                    web::post()
                        .to(ingest::post_event)
                        .authorize_for_stream(Action::Ingest),
                )
                // DELETE "/logstream/{logstream}" ==> Delete log stream
                .route(
                    web::delete()
                        .to(logstream::delete)
                        .authorize_for_stream(Action::DeleteStream),
                )
                .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
        )
        .service(
            web::resource("/alert")
                // PUT "/logstream/{logstream}/alert" ==> Set alert for given log stream
                .route(
                    web::put()
                        .to(logstream::put_alert)
                        .authorize_for_stream(Action::PutAlert),
                )
                // GET "/logstream/{logstream}/alert" ==> Get alert for given log stream
                .route(
                    web::get()
                        .to(logstream::get_alert)
                        .authorize_for_stream(Action::GetAlert),
                ),
        )
        .service(
            // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
            web::resource("/schema").route(
                web::get()
                    .to(logstream::schema)
                    .authorize_for_stream(Action::GetSchema),
            ),
        )
        .service(
            // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
            web::resource("/stats").route(
                web::get()
                    .to(logstream::get_stats)
                    .authorize_for_stream(Action::GetStats),
            ),
        )
        .service(
            web::resource("/retention")
                // PUT "/logstream/{logstream}/retention" ==> Set retention for given logstream
                .route(
                    web::put()
                        .to(logstream::put_retention)
                        .authorize_for_stream(Action::PutRetention),
                )
                // GET "/logstream/{logstream}/retention" ==> Get retention for given logstream
                .route(
                    web::get()
                        .to(logstream::get_retention)
                        .authorize_for_stream(Action::GetRetention),
                ),
        );

    // User API
    let user_api = web::scope("/user")
        .service(
            web::resource("")
                // GET /user => List all users
                .route(web::get().to(rbac::list_users).authorize(Action::ListUser)),
        )
        .service(
            web::resource("/{username}")
                // PUT /user/{username} => Create a new user
                .route(web::post().to(rbac::post_user).authorize(Action::PutUser))
                // DELETE /user/{username} => Delete a user
                .route(
                    web::delete()
                        .to(rbac::delete_user)
                        .authorize(Action::DeleteUser),
                )
                .wrap(DisAllowRootUser),
        )
        .service(
            web::resource("/{username}/role")
                // PUT /user/{username}/roles => Put roles for user
                .route(
                    web::put()
                        .to(rbac::put_role)
                        .authorize(Action::PutRoles)
                        .wrap(DisAllowRootUser),
                )
                .route(
                    web::get()
                        .to(rbac::get_role)
                        .authorize_for_user(Action::GetRole),
                ),
        )
        .service(
            web::resource("/{username}/generate-new-password")
                // POST /user/{username}/generate-new-password => reset password for this user
                .route(
                    web::post()
                        .to(rbac::post_gen_password)
                        .authorize(Action::PutUser)
                        .wrap(DisAllowRootUser),
                ),
        );

    let llm_query_api = web::scope("/llm").service(
        web::resource("").route(
            web::post()
                .to(llm::make_llm_request)
                .authorize(Action::QueryLLM),
        ),
    );

    // Deny request if username is same as the env variable P_USERNAME.
    cfg.service(
        // Base path "{url}/api/v1"
        web::scope(&base_path())
            // POST "/query" ==> Get results of the SQL query passed in request body
            .service(
                web::resource("/query")
                    .route(web::post().to(query::query).authorize(Action::Query)),
            )
            // POST "/ingest" ==> Post logs to given log stream based on header
            .service(
                web::resource("/ingest")
                    .route(
                        web::post()
                            .to(ingest::ingest)
                            .authorize_for_stream(Action::Ingest),
                    )
                    .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            // GET "/liveness" ==> Liveness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
            .service(web::resource("/liveness").route(web::get().to(health_check::liveness)))
            // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
            .service(web::resource("/readiness").route(web::get().to(health_check::readiness)))
            // GET "/about" ==> Returns information about instance
            .service(
                web::resource("/about")
                    .route(web::get().to(about::about).authorize(Action::GetAbout)),
            )
            .service(
                web::scope("/logstream")
                    .service(
                        // GET "/logstream" ==> Get list of all Log Streams on the server
                        web::resource("")
                            .route(web::get().to(logstream::list).authorize(Action::ListStream)),
                    )
                    .service(
                        // logstream API
                        logstream_api,
                    ),
            )
            .service(user_api)
            .service(llm_query_api),
    )
    // GET "/" ==> Serve the static frontend directory
    .service(ResourceFiles::new("/", generated).resolve_not_found_to_root());
}

fn base_path() -> String {
    format!("{API_BASE_PATH}/{API_VERSION}")
}

pub fn metrics_path() -> String {
    format!("{}/metrics", base_path())
}
