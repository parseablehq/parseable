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
use crate::utils::hostname_unchecked;

use std::sync::Arc;

use super::parseable_server::OpenIdClient;
use super::parseable_server::ParseableServer;
use super::server::Server;
use super::server::DEFAULT_VERSION;
use super::ssl_acceptor::get_ssl_acceptor;

use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use itertools::Itertools;
use relative_path::RelativePathBuf;
use serde::Deserialize;
use serde::Serialize;

use crate::{
    handlers::http::{base_path, cross_origin_config},
    option::CONFIG,
};

#[derive(Default)]
pub struct IngestServer;

#[async_trait(?Send)]
impl ParseableServer for IngestServer {
    async fn start(
        &self,
        prometheus: PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()> {
        // get the oidc client
        let oidc_client = match oidc_client {
            Some(config) => {
                let client = config
                    .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
                    .await?;
                Some(Arc::new(client))
            }

            None => None,
        };

        // get the ssl stuff
        let ssl = get_ssl_acceptor(
            &CONFIG.parseable.tls_cert_path,
            &CONFIG.parseable.tls_key_path,
        )?;

        // fn that creates the app
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

        let store = CONFIG.storage().get_object_store();

        let (address, port) = self
            .get_ingestor_address()
            .unwrap_or(("0.0.0.0".to_string(), "8000".to_string()));
        let path =
            RelativePathBuf::from(format!(".ingestor.{}.{}.json", hostname_unchecked(), port));

        let resource = IngesterMetadata::new(
            address,
            port,
            CONFIG.parseable.domain_address.clone().unwrap().to_string(),
            DEFAULT_VERSION.to_string(),
            store.get_bucket_name(),
        );

        store.put_object(&path, resource);

        Ok(())
    }
}

impl IngestServer {
    // configure the api routes
    // odic_client is not used
    fn configure_routes(config: &mut web::ServiceConfig, _oidc_client: Option<OpenIdClient>) {
        let logstream_scope = Server::get_logstream_webscope();
        let ingest_factory = Server::get_ingest_factory();

        config
            .service(
                // Base path "{url}/api/v1"
                web::scope(&base_path()).service(ingest_factory),
            )
            .service(Server::get_liveness_factory())
            .service(Server::get_readiness_factory())
            .service(logstream_scope);
    }

    #[inline(always)]
    fn get_ingestor_address(&self) -> Option<(String, String)> {
        // this might cause an issue down the line
        // best is to make the Cli Struct better, but thats a chore
        CONFIG
            .parseable
            .address
            .split(":")
            .map(|string| string.to_owned())
            .collect_tuple()
    }
}

#[derive(Serialize, Debug, Deserialize)]
struct IngesterMetadata {
    version: String,
    address: String,
    port: String,
    origin: String,
    bucket_name: String,
}

impl IngesterMetadata {
    pub fn new(
        address: String,
        port: String,
        origin: String,
        version: String,
        bucket_name: String,
    ) -> Self {
        Self {
            address,
            port,
            origin,
            version,
            bucket_name,
        }
    }
}
