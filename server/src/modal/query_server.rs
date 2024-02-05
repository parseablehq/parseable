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


use actix_web::web;
use std::sync::Arc;

use crate::handlers::http::middleware::RouteExt;
use crate::handlers::http::{
    base_path, cross_origin_config, logstream, API_BASE_PATH, API_VERSION,
};
use crate::rbac::role::Action;
use actix_web::web::ServiceConfig;
use actix_web::{App, HttpServer};
use actix_web_static_files::ResourceFiles;
use async_trait::async_trait;

use crate::option::CONFIG;

use super::parseable_server::{OpenIdClient, ParseableServer};
use super::server::SuperServer;
use super::ssl_acceptor::get_ssl_acceptor;

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

#[derive(Default)]
pub struct QueryServer;

#[async_trait(?Send)]
impl ParseableServer for QueryServer {
    async fn start(
        &self,
        prometheus: actix_web_prometheus::PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()> {
        let oidc_client = match oidc_client {
            Some(config) => {
                let client = config
                    .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
                    .await?;
                Some(Arc::new(client))
            }

            None => None,
        };

        let ssl = get_ssl_acceptor(
            &CONFIG.parseable.tls_cert_path,
            &CONFIG.parseable.tls_key_path,
        )?;

        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| QueryServer::configure_routes(config, oidc_client.clone()))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

        // concurrent workers equal to number of cores on the cpu
        let http_server = HttpServer::new(create_app_fn).workers(num_cpus::get());
        if let Some(config) = ssl {
            http_server
                .bind_rustls(&CONFIG.parseable.address, config)?
                .run()
                .await?;
        } else {
            http_server.bind(&CONFIG.parseable.address)?.run().await?;
        }

        Ok(())
    }
}

impl QueryServer {
    // configure the api routes
    pub fn configure_routes(config: &mut ServiceConfig, oidc_client: Option<OpenIdClient>) {
        let generated = generate();

        let user_scope = SuperServer::get_user_webscope();
        let llm_scope = SuperServer::get_llm_webscope();
        let role_scope = SuperServer::get_user_role_webscope();
        let oauth_scope = SuperServer::get_oauth_webscope(oidc_client);

        config
            .service(
                web::scope(&base_path())
                    // POST "/query" ==> Get results of the SQL query passed in request body
                    .service(SuperServer::get_query_factory())
                    // GET "/liveness" ==> Liveness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
                    .service(SuperServer::get_liveness_factory())
                    // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
                    .service(SuperServer::get_readiness_factory())
                    // GET "/about" ==> Returns information about instance
                    .service(SuperServer::get_about_factory())
                    .service(
                        web::scope("/logstream").service(
                            // GET "/logstream" ==> Get list of all Log Streams on the server
                            web::resource("").route(
                                web::get().to(logstream::list).authorize(Action::ListStream),
                            ),
                        ),
                    )
                    .service(user_scope)
                    .service(llm_scope)
                    .service(oauth_scope)
                    .service(role_scope),
            )
            .service(ResourceFiles::new("/", generated).resolve_not_found_to_root());
    }
}
