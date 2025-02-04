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

pub mod ingest;
pub mod ingest_server;
pub mod query;
pub mod query_server;
pub mod server;
pub mod ssl_acceptor;
pub mod utils;

use std::path::Path;
use std::sync::Arc;

use actix_web::middleware::from_fn;
use actix_web::web::ServiceConfig;
use actix_web::App;
use actix_web::HttpServer;
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use openid::Discovered;
use serde::Deserialize;
use serde::Serialize;
use ssl_acceptor::get_ssl_acceptor;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use super::audit;
use super::cross_origin_config;
use super::API_BASE_PATH;
use super::API_VERSION;
use crate::handlers::http::health_check;
use crate::oidc;
use crate::option::CONFIG;

pub type OpenIdClient = Arc<openid::Client<Discovered, oidc::Claims>>;

// to be decided on what the Default version should be
pub const DEFAULT_VERSION: &str = "v3";

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

#[async_trait]
pub trait ParseableServer {
    /// configure the router
    fn configure_routes(config: &mut ServiceConfig, oidc_client: Option<OpenIdClient>)
    where
        Self: Sized;

    /// load metadata/configuration from persistence for previous sessions of parseable
    async fn load_metadata(&self) -> anyhow::Result<Option<Bytes>>;

    /// code that describes starting and setup procedures for each type of server
    async fn init(
        &self,
        prometheus: &PrometheusMetrics,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> anyhow::Result<()>;

    /// configure the server
    async fn start(
        &self,
        shutdown_rx: oneshot::Receiver<()>,
        prometheus: PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
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
            &CONFIG.options.tls_cert_path,
            &CONFIG.options.tls_key_path,
            &CONFIG.options.trusted_ca_certs_path,
        )?;

        // fn that creates the app
        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| Self::configure_routes(config, oidc_client.clone()))
                .wrap(from_fn(health_check::check_shutdown_middleware))
                .wrap(from_fn(audit::audit_log_middleware))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

        // Create the HTTP server
        let http_server = HttpServer::new(create_app_fn)
            .workers(num_cpus::get())
            .shutdown_timeout(60);

        // Start the server with or without TLS
        let srv = if let Some(config) = ssl {
            http_server
                .bind_rustls_0_22(&CONFIG.options.address, config)?
                .run()
        } else {
            http_server.bind(&CONFIG.options.address)?.run()
        };

        // Graceful shutdown handling
        let srv_handle = srv.handle();

        let sync_task = tokio::spawn(async move {
            // Wait for the shutdown signal
            let _ = shutdown_rx.await;

            health_check::shutdown().await;

            // Perform S3 sync and wait for completion
            info!("Starting data sync to S3...");
            if let Err(e) = CONFIG.storage().get_object_store().sync(true).await {
                warn!("Failed to sync local data with object store. {:?}", e);
            } else {
                info!("Successfully synced all data to S3.");
            }

            // Initiate graceful shutdown
            info!("Graceful shutdown of HTTP server triggered");
            srv_handle.stop(true).await;
        });

        // Await the HTTP server to run
        let server_result = srv.await;

        // Wait for the sync task to complete before exiting
        if let Err(e) = sync_task.await {
            error!("Error in sync task: {:?}", e);
        } else {
            info!("Sync task completed successfully.");
        }

        // Return the result of the server
        server_result?;

        Ok(())
    }
}

#[derive(Serialize, Debug, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct IngestorMetadata {
    pub version: String,
    pub port: String,
    pub domain_name: String,
    pub bucket_name: String,
    pub token: String,
    pub ingestor_id: String,
    pub flight_port: String,
}

impl IngestorMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        port: String,
        domain_name: String,
        version: String,
        bucket_name: String,
        username: &str,
        password: &str,
        ingestor_id: String,
        flight_port: String,
    ) -> Self {
        let token = base64::prelude::BASE64_STANDARD.encode(format!("{}:{}", username, password));

        let token = format!("Basic {}", token);

        Self {
            port,
            domain_name,
            version,
            bucket_name,
            token,
            ingestor_id,
            flight_port,
        }
    }

    pub fn get_ingestor_id(&self) -> String {
        self.ingestor_id.clone()
    }

    /// Puts the ingestor info into the staging.
    ///
    /// This function takes the ingestor info as a parameter and stores it in staging.
    /// # Parameters
    ///
    /// * `staging_path`: Staging root directory.
    pub fn put_on_disk(&self, staging_path: &Path) -> anyhow::Result<()> {
        let file_name = format!("ingestor.{}.json", self.ingestor_id);
        let file_path = staging_path.join(file_name);

        std::fs::write(file_path, serde_json::to_vec(&self)?)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use actix_web::body::MessageBody;
    use bytes::Bytes;
    use rstest::rstest;

    use super::{IngestorMetadata, DEFAULT_VERSION};

    #[rstest]
    fn test_deserialize_resource() {
        let lhs: IngestorMetadata = IngestorMetadata::new(
            "8000".to_string(),
            "https://localhost:8000".to_string(),
            DEFAULT_VERSION.to_string(),
            "somebucket".to_string(),
            "admin",
            "admin",
            "ingestor_id".to_string(),
            "8002".to_string(),
        );

        let rhs = serde_json::from_slice::<IngestorMetadata>(br#"{"version":"v3","port":"8000","domain_name":"https://localhost:8000","bucket_name":"somebucket","token":"Basic YWRtaW46YWRtaW4=", "ingestor_id": "ingestor_id","flight_port": "8002"}"#).unwrap();

        assert_eq!(rhs, lhs);
    }

    #[rstest]
    fn test_serialize_resource() {
        let im = IngestorMetadata::new(
            "8000".to_string(),
            "https://localhost:8000".to_string(),
            DEFAULT_VERSION.to_string(),
            "somebucket".to_string(),
            "admin",
            "admin",
            "ingestor_id".to_string(),
            "8002".to_string(),
        );

        let lhs = Bytes::from(serde_json::to_vec(&im).unwrap());
        let rhs = br#"{"version":"v3","port":"8000","domain_name":"https://localhost:8000","bucket_name":"somebucket","token":"Basic YWRtaW46YWRtaW4=","ingestor_id":"ingestor_id","flight_port":"8002"}"#
                .try_into_bytes()
                .unwrap();

        assert_eq!(lhs, rhs);
    }
}
