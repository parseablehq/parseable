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


use crate::handlers::http::API_BASE_PATH;
use crate::handlers::http::API_VERSION;

use std::sync::Arc;

use super::parseable_server::OpenIdClient;
use super::parseable_server::ParseableServer;
use super::server::SuperServer;
use super::ssl_acceptor::get_ssl_acceptor;

use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;

use crate::{
    handlers::http::{base_path, cross_origin_config},
    option::CONFIG,
};

pub struct IngestServer;

#[async_trait(?Send)]
impl ParseableServer for IngestServer {
    async fn start(
        &self,
        prometheus: PrometheusMetrics,
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
                .configure(|config| IngestServer::configure_routes(config, oidc_client.clone()))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

        // concurrent workers equal to number of logical cores
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

impl IngestServer {
    // configure the api routes
    fn configure_routes(config: &mut web::ServiceConfig, _odic_client: Option<OpenIdClient>) {
        let logstream_scope = SuperServer::get_logstream_webscope();
        let ingest_factory = SuperServer::get_ingest_factory();

        config
            .service(
                // Base path "{url}/api/v1"
                web::scope(&base_path()).service(ingest_factory),
            )
            // GET "/liveness" ==> Liveness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
            .service(SuperServer::get_liveness_factory())
            // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
            .service(SuperServer::get_readiness_factory())
            .service(logstream_scope);
    }
}
